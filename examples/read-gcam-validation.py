from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd


def find_latest_report(default_dir: Path) -> Path:
    if not default_dir.exists():
        raise FileNotFoundError(f"Report directory not found: {default_dir}")

    candidates = [
        p
        for p in default_dir.glob("*.xlsx")
        if p.is_file() and not p.name.startswith("~$")
    ]

    if not candidates:
        raise FileNotFoundError(f"No .xlsx reports found in {default_dir}")

    return max(candidates, key=lambda p: p.stat().st_mtime)


def pick_column(df: pd.DataFrame, options: list[str]) -> Optional[str]:
    normalized = {str(c).strip().lower(): c for c in df.columns}

    for option in options:
        key = option.strip().lower()
        if key in normalized:
            return normalized[key]

    for option in options:
        key = option.strip().lower()
        for norm_name, original in normalized.items():
            if key in norm_name:
                return original

    return None


def print_value_counts(title: str, series: pd.Series, top_n: int) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    counts = series.fillna("<missing>").astype(str).value_counts().head(top_n)
    if counts.empty:
        print("No rows")
        return

    for key, value in counts.items():
        print(f"{value:>8}  {key}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read a premise change report and summarize Validation anomalies."
    )
    parser.add_argument(
        "--report",
        type=str,
        default=None,
        help="Path to a specific change report .xlsx. If omitted, uses latest in examples/export/change reports.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="How many top entries to print for reasons, regions, and datasets.",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    report_dir = script_dir / "export" / "change reports"

    report_path = Path(args.report).resolve() if args.report else find_latest_report(report_dir)
    if not report_path.exists():
        raise FileNotFoundError(f"Report not found: {report_path}")

    print(f"Using report: {report_path}")

    # Validation sheet uses row 1 as column labels and often has a blank spacer row.
    df = pd.read_excel(report_path, sheet_name="Validation", header=0)
    df = df.dropna(how="all")

    sev_col = pick_column(
        df,
        [
            "Severity of anomaly",
            "severity",
        ],
    )
    reason_col = pick_column(
        df,
        [
            "Reason for anomaly",
            "reason",
        ],
    )
    location_col = pick_column(
        df,
        [
            "location",
            "Location",
        ],
    )
    dataset_col = pick_column(
        df,
        [
            "Name of the dataset",
            "name of the dataset",
            "name",
            "dataset",
        ],
    )

    print(f"Validation rows: {len(df)}")

    if sev_col:
        print_value_counts("Severity counts", df[sev_col], top_n=50)
    else:
        print("\nSeverity column not found")

    if reason_col:
        print_value_counts("Top anomaly reasons", df[reason_col], top_n=args.top)
    else:
        print("\nReason column not found")

    if location_col:
        print_value_counts("Top affected locations", df[location_col], top_n=args.top)
    else:
        print("\nLocation column not found")

    if dataset_col:
        print_value_counts("Top affected datasets", df[dataset_col], top_n=args.top)
    else:
        print("\nDataset column not found")


if __name__ == "__main__":
    main()
