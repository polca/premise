"""Audit traceability and review priority of legacy TRAILS lifetime records.

The canonical legacy registry preserves historical behavior, but this script
keeps evidence review separate from compatibility. It reports which fixed
lifetimes lack a recognizable source reference and optionally writes the
record-level audit table used to plan reviewed YAML replacements.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path

from premise.core.filesystem_constants import DATA_DIR
from premise.temporal_legacy import compile_legacy_rows

SOURCE_REFERENCE = re.compile(
    r"(?:https?://|\bdoi\b|\bISBN\b|\bNREL\b|\bEIA\b|\bEurostat\b|"
    r"\bEuropean Commission\b|\bJRC\b|\bNRC\b)",
    re.IGNORECASE,
)
FAMILIES = (
    ("passenger_car", re.compile(r"\bpassenger car\b", re.IGNORECASE)),
    ("bus", re.compile(r"\b(?:passenger )?bus\b|\bcoach\b", re.IGNORECASE)),
    ("lorry", re.compile(r"\blorry\b|\btruck\b", re.IGNORECASE)),
    (
        "stationary_battery",
        re.compile(
            r"storage battery|electricity storage system|battery 50kW", re.IGNORECASE
        ),
    ),
    ("wind", re.compile(r"\bwind power plant\b", re.IGNORECASE)),
    ("solar_pv", re.compile(r"\bphotovoltaic\b", re.IGNORECASE)),
    ("hydropower", re.compile(r"\bhydropower plant\b", re.IGNORECASE)),
    ("nuclear", re.compile(r"\bnuclear power plant\b", re.IGNORECASE)),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv", type=Path, help="Optionally write the record-level audit."
    )
    return parser.parse_args()


def family(name: str, product: str) -> str:
    identity = f"{name} {product}"
    return next(
        (label for label, pattern in FAMILIES if pattern.search(identity)), "other"
    )


def audit_rows() -> list[dict[str, object]]:
    records = []
    for row in compile_legacy_rows(DATA_DIR / "trails" / "temporal"):
        if not row.get("lifetime"):
            continue
        notes = row.get("param_notes", "")
        records.append(
            {
                "name": row["name"],
                "reference_product": row["reference product"],
                "temporal_tag": row["temporal_tag"],
                "lifetime_years": row["lifetime"],
                "family": family(row["name"], row["reference product"]),
                "has_source_reference": bool(SOURCE_REFERENCE.search(notes)),
                "parameter_notes": notes,
            }
        )
    return records


def main() -> int:
    args = parse_args()
    records = audit_rows()
    stock_assets = [row for row in records if row["temporal_tag"] == "stock_asset"]
    print(f"lifetime records: {len(records)}")
    print(f"stock-asset lifetime records: {len(stock_assets)}")
    print(
        "stock-asset records with recognizable source references: "
        f"{sum(bool(row['has_source_reference']) for row in stock_assets)}"
    )
    print("stock-asset review families:")
    for label, count in sorted(Counter(row["family"] for row in stock_assets).items()):
        print(f"  {label}: {count}")

    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with args.csv.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
