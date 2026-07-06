"""Plot hydrogen demand per node and demand node counts over time.

The script reads ``dev/hydrogen_demand_nodes_shares.csv`` and creates one line
chart per demand node type. Each chart includes all non-WORLD regions from 2025
onward:

* top panel: hydrogen used per demand node per year
* bottom panel: rounded-up number of demand nodes
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "dev" / "hydrogen_demand_nodes_shares.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "dev" / "hydrogen_demand_node_charts"
START_YEAR = 2025


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create line charts from hydrogen_demand_nodes_shares.csv, excluding "
            "the WORLD region."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input CSV path. Default: {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for PNG charts. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser.parse_args()


def load_data(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    required_columns = {
        "year",
        "region",
        "demand_node_type",
        "hydrogen_demand_t_per_year",
        "hydrogen_demand_t_per_node_per_year",
        "demand_nodes_rounded_up",
    }
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns in {csv_path}: {missing}")

    df = df[df["region"].astype(str).str.upper() != "WORLD"].copy()
    df = df.dropna(subset=["demand_node_type"])

    numeric_columns = [
        "year",
        "hydrogen_demand_t_per_year",
        "hydrogen_demand_t_per_node_per_year",
        "demand_nodes_rounded_up",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["year", "region", "demand_node_type"])
    df["year"] = df["year"].astype(int)
    df = df[df["year"] >= START_YEAR]
    return df


def aggregate_by_region_and_type(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(["year", "region", "demand_node_type"], as_index=False)
        .agg(
            hydrogen_demand_t_per_year=("hydrogen_demand_t_per_year", "sum"),
            demand_nodes_rounded_up=("demand_nodes_rounded_up", "sum"),
        )
        .sort_values(["demand_node_type", "region", "year"])
    )

    grouped["hydrogen_demand_t_per_node_per_year"] = (
        grouped["hydrogen_demand_t_per_year"]
        / grouped["demand_nodes_rounded_up"].where(
            grouped["demand_nodes_rounded_up"] > 0
        )
    )

    return grouped


def safe_filename(value: str) -> str:
    clean = "".join(
        character.lower() if character.isalnum() else "_"
        for character in str(value).strip()
    )
    return "_".join(part for part in clean.split("_") if part)


DEMAND_NODE_LABELS = {
    "cement_plants": ("cement plant", "cement plants"),
    "chemical_plants": ("chemical plant", "chemical plants"),
    "fueling_stations": ("fueling station", "fueling stations"),
    "other_demand_nodes": ("other demand node", "other demand nodes"),
    "steel_plants": ("steel plant", "steel plants"),
}


def plot_demand_node_type(
    df: pd.DataFrame,
    demand_node_type: str,
    output_dir: Path,
    available_years: list[int],
) -> Path:
    subset = df[df["demand_node_type"] == demand_node_type]
    regions = sorted(subset["region"].unique())
    singular_label, plural_label = DEMAND_NODE_LABELS.get(
        demand_node_type,
        (
            str(demand_node_type).replace("_", " "),
            f"{str(demand_node_type).replace('_', ' ')}s",
        ),
    )

    fig, (ax_hydrogen, ax_nodes) = plt.subplots(
        nrows=2,
        ncols=1,
        figsize=(8.7, 9),
        sharex=True,
        gridspec_kw={"height_ratios": [2, 1.4]},
    )

    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for index, region in enumerate(regions):
        region_data = subset[subset["region"] == region].sort_values("year")
        color = color_cycle[index % len(color_cycle)]

        ax_hydrogen.plot(
            region_data["year"],
            region_data["hydrogen_demand_t_per_node_per_year"],
            color=color,
            linewidth=2,
            label=region,
        )
        ax_nodes.plot(
            region_data["year"],
            region_data["demand_nodes_rounded_up"],
            color=color,
            linewidth=2,
            label=f"{region} demand nodes",
        )

    ax_hydrogen.set_title(
        f"Hydrogen demand per {singular_label} and number of "
        f"{plural_label} per region"
    )
    ax_hydrogen.set_ylabel(f"Hydrogen demand per {singular_label} (t H2/year)")
    ax_nodes.set_ylabel(f"Number of {plural_label}")
    ax_hydrogen.grid(True, axis="both", linestyle=":", linewidth=0.8, alpha=0.6)
    ax_nodes.grid(True, axis="both", linestyle=":", linewidth=0.8, alpha=0.6)
    ax_hydrogen.set_ylim(bottom=0)
    ax_nodes.set_ylim(bottom=0)
    ax_nodes.set_xlabel("Year")

    ax_nodes.set_xticks(available_years)
    ax_nodes.set_xlim(left=START_YEAR)
    ax_nodes.tick_params(axis="x", rotation=45)

    hydrogen_lines, hydrogen_labels = ax_hydrogen.get_legend_handles_labels()
    ax_hydrogen.legend(
        hydrogen_lines,
        hydrogen_labels,
        loc="upper left",
        bbox_to_anchor=(1.08, 1.0),
        fontsize="small",
    )

    fig.tight_layout()

    output_path = output_dir / f"{safe_filename(demand_node_type)}.png"
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_data(args.input)
    aggregated = aggregate_by_region_and_type(df)
    available_years = sorted(aggregated["year"].unique())

    output_paths = [
        plot_demand_node_type(
            aggregated,
            demand_node_type,
            output_dir,
            available_years,
        )
        for demand_node_type in sorted(aggregated["demand_node_type"].unique())
    ]

    print(f"Created {len(output_paths)} chart(s):")
    for output_path in output_paths:
        print(f"- {output_path}")


if __name__ == "__main__":
    main()
