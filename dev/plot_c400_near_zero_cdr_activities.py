#!/usr/bin/env python3
"""Plot 100 end-use activities with near-zero positive GWP under CDR allocation."""

from __future__ import annotations

import argparse
import math
import re
import textwrap
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import pandas as pd

DEFAULT_SCORES = Path(
    "results/c400_common_non_market_lca_scores_with_primary_energy.csv"
)
DEFAULT_METHODS = Path(
    "results/c400_common_non_market_lca_methods_with_primary_energy.csv"
)
DEFAULT_OUTPUT = Path(
    "results/c400_common_non_market_near_zero_cdr_end_use_precdr_nonnegative_nonclimate_increase_100_cdr_markers_diverse_by_cpc_scores_with_primary_energy.pdf"
)
DEFAULT_SELECTION = Path(
    "results/c400_common_non_market_near_zero_cdr_end_use_precdr_nonnegative_nonclimate_increase_100_cdr_markers_diverse_by_cpc_activities_with_primary_energy.csv"
)
DEFAULT_PROJECT = "ecoinvent-3.12-cutoff"

CDR_MARKET_DATABASE_ORDER = 8
PRE_CDR_DATABASE_ORDER = 7
CPC_DATABASE_ORDER = 1
MIN_NONCLIMATE_INCREASE = 0.15
MAX_PER_MAIN_CPC = 35
MAX_PER_ACTIVITY_NAME = 2
NO_CPC = "No CPC"
IDENTITY_FIELDS = ["name", "reference_product", "location", "unit"]

INDICATORS = [
    ("gwp100_incl_bio_co2", "GWP100 incl. bio CO2"),
    ("water_extraction", "Water extraction"),
    ("land_occupation", "Land occupation"),
    ("metals_minerals_depletion", "Metals/minerals depletion"),
    ("primary_energy_demand", "Primary energy demand"),
]
NONCLIMATE_COLUMNS = [
    "water_extraction",
    "land_occupation",
    "metals_minerals_depletion",
    "primary_energy_demand",
]

COLORS = [
    "#4b5563",
    "#64748b",
    "#0f766e",
    "#2563eb",
    "#7c3aed",
    "#db2777",
    "#111827",
    "#dc2626",
    "#f97316",
    "#f59e0b",
    "#16a34a",
    "#0891b2",
    "#65a30d",
    "#84cc16",
]

CDR_MARKERS = ["o", "s", "D", "^", "v", "P"]

END_USE_INCLUDE_TERMS = [
    "tap water production",
    "transport, passenger",
    "transport, bicycle",
    "energy use and operation emissions, electric bicycle",
    "operation, computer",
    "consumer electronics production, mobile device",
    "power adapter production, for smartphone",
    "building operation",
    "photovoltaic installation",
    "photovoltaic slanted-roof installation",
    "photovoltaic flat-roof installation",
    "photovoltaics, electric installation",
    "slanted-roof construction",
    "flat-roof construction",
    "kick scooter",
    "motorbike, battery electric",
    "bicycle, conventional",
    "bicycle, electric",
    "ev charger",
    "forklift operation",
    "smartphone",
    "washing machine",
    "dishwasher",
    "refrigerator",
    "freezer",
    "television",
    "restaurant",
    "hotel",
    "cow milk",
    "milk",
    "bread",
    "tomato",
    "apple",
    "potato",
    "rice",
    "pasta",
    "coffee",
    "beer",
    "wine",
    "cheese",
]

END_USE_EXCLUDE_TERMS = [
    "electricity",
    "heat",
    "steam",
    "fuel",
    "diesel",
    "petrol",
    "gasoline",
    "natural gas",
    "hydrogen",
    "carbon dioxide",
    "co2",
    "captured",
    "coal",
    "lignite",
    "crude oil",
    "chemical",
    "chlor",
    "acid",
    "sodium",
    "ammonia",
    "methanol",
    "ethanol",
    "ethylene",
    "propylene",
    "benzene",
    "xylene",
    "toluene",
    "steel",
    "aluminium",
    "copper",
    "zinc",
    "nickel",
    "cement",
    "clinker",
    "concrete",
    "brick",
    "glass",
    "plastic",
    "poly",
    "resin",
    "ore",
    "mine",
    "mining",
    "sawlog",
    "pulp",
    "paper pulp",
    "treatment of",
    "waste",
    "sewage",
    "sludge",
    "decarbonised",
    "deionised",
    "ultrapure",
    "ultra pure",
    "pure, via",
    "ion exchanger",
    "softened",
    "rainwater",
    "irrigation",
    "evaporation",
    "spray-drying",
    "drying of",
    "field application",
    "fertilis",
    "harrowing",
    "harvesting",
    "haulm",
    "maintenance",
    "pesticide",
    "planting",
    "ploughing",
    "plowing",
    "sowing",
    "tillage",
    "hard disk drive",
    "disk drive",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scores", type=Path, default=DEFAULT_SCORES)
    parser.add_argument("--methods", type=Path, default=DEFAULT_METHODS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--activities-per-page", type=int, default=4)
    parser.add_argument(
        "--cdr-market-order", type=int, default=CDR_MARKET_DATABASE_ORDER
    )
    parser.add_argument("--pre-cdr-order", type=int, default=PRE_CDR_DATABASE_ORDER)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--cpc-database-order", type=int, default=CPC_DATABASE_ORDER)
    parser.add_argument(
        "--min-nonclimate-increase", type=float, default=MIN_NONCLIMATE_INCREASE
    )
    parser.add_argument("--max-per-main-cpc", type=int, default=MAX_PER_MAIN_CPC)
    parser.add_argument(
        "--max-per-activity-name", type=int, default=MAX_PER_ACTIVITY_NAME
    )
    return parser.parse_args()


def wrap_label(label: str, width: int = 30) -> str:
    return "\n".join(textwrap.wrap(label, width=width, break_long_words=False))


def finite_limits(values: pd.Series, force_zero_floor: bool) -> tuple[float, float]:
    arr = np.asarray([float(value) for value in values if math.isfinite(float(value))])
    if arr.size == 0:
        return (0.0, 1.0) if force_zero_floor else (-1.0, 1.0)

    lo = float(arr.min())
    hi = float(arr.max())
    if force_zero_floor and lo >= 0:
        return 0.0, hi * 1.08 if hi else 1.0

    if lo == hi:
        pad = abs(lo) * 0.1 or 1.0
        return lo - pad, hi + pad
    pad = (hi - lo) * 0.08
    return lo - pad, hi + pad


def activity_label(row: pd.Series) -> str:
    return (
        f"{row['name']} | {row['reference_product']} | "
        f"{row['location']} | {row['unit']}"
    )


def end_use_text(row: pd.Series) -> str:
    return f"{row['name']} {row['reference_product']}".lower()


def is_end_use_activity(row: pd.Series) -> bool:
    text = end_use_text(row)
    return any(term in text for term in END_USE_INCLUDE_TERMS) and not any(
        term in text for term in END_USE_EXCLUDE_TERMS
    )


def load_units(methods_path: Path) -> dict[str, str]:
    methods = pd.read_csv(methods_path)
    return dict(zip(methods["column"], methods["unit"], strict=True))


def identity_from_activity(activity) -> tuple[str, str, str, str]:
    return (
        activity.get("name") or "",
        activity.get("reference product") or "",
        activity.get("location") or "",
        activity.get("unit") or "",
    )


def extract_cpc(classifications: object) -> str:
    for item in classifications or []:
        if len(item) >= 2 and item[0] == "CPC":
            return str(item[1] or "")
    return ""


def main_cpc_code(cpc: str) -> str:
    match = re.match(r"\s*(\d{2})", str(cpc or ""))
    return match.group(1) if match else NO_CPC


def cpc_group_label(code: str) -> str:
    return f"CPC {code}" if code != NO_CPC else "No CPC classification"


def cpc_sort_value(code: str) -> int:
    return int(code) if str(code).isdigit() else 999


def load_cpc_metadata(
    scores: pd.DataFrame, project: str, cpc_database_order: int
) -> pd.DataFrame:
    database_names = (
        scores.loc[scores["database_order"] == cpc_database_order, "database"]
        .dropna()
        .unique()
        .tolist()
    )
    if len(database_names) != 1:
        raise ValueError(
            f"Expected one database for CPC order {cpc_database_order}, "
            f"found {database_names!r}"
        )

    import bw2data as bd

    bd.projects.set_current(project)
    rows = []
    seen = set()
    for activity in bd.Database(database_names[0]):
        cpc = extract_cpc(activity.get("classifications"))
        if not cpc:
            continue
        key = identity_from_activity(activity)
        if key in seen:
            continue
        seen.add(key)
        code = main_cpc_code(cpc)
        rows.append(
            {
                "name": key[0],
                "reference_product": key[1],
                "location": key[2],
                "unit": key[3],
                "cpc": cpc,
                "main_cpc_code": code,
                "main_cpc_group": cpc_group_label(code),
            }
        )

    return pd.DataFrame(
        rows, columns=[*IDENTITY_FIELDS, "cpc", "main_cpc_code", "main_cpc_group"]
    )


def add_cpc_metadata(scores: pd.DataFrame, cpc_metadata: pd.DataFrame) -> pd.DataFrame:
    if cpc_metadata.empty:
        scores = scores.copy()
        scores["cpc"] = ""
        scores["main_cpc_code"] = NO_CPC
        scores["main_cpc_group"] = cpc_group_label(NO_CPC)
        return scores

    scores = scores.merge(cpc_metadata, on=IDENTITY_FIELDS, how="left")
    scores["cpc"] = scores["cpc"].fillna("")
    scores["main_cpc_code"] = scores["main_cpc_code"].fillna(NO_CPC)
    scores["main_cpc_group"] = scores["main_cpc_code"].map(cpc_group_label)
    return scores


def diversified_head(
    candidates: pd.DataFrame,
    count: int,
    max_per_main_cpc: int,
    max_per_activity_name: int,
) -> pd.DataFrame:
    candidates = candidates.sort_values(
        ["gwp100_incl_bio_co2", "activity_index"], ascending=[True, True]
    ).copy()
    group_order = (
        candidates.groupby("main_cpc_code")["gwp100_incl_bio_co2"]
        .min()
        .sort_values()
        .index.tolist()
    )
    selected_indices: list[int] = []
    selected_set = set()
    group_counts: Counter[str] = Counter()
    name_counts: Counter[str] = Counter()

    def take_candidates(use_group_cap: bool, use_name_cap: bool) -> None:
        made_progress = True
        while len(selected_indices) < count and made_progress:
            made_progress = False
            for group in group_order:
                if len(selected_indices) >= count:
                    break
                if use_group_cap and max_per_main_cpc > 0:
                    if group_counts[group] >= max_per_main_cpc:
                        continue
                group_candidates = candidates[candidates["main_cpc_code"] == group]
                for idx, row in group_candidates.iterrows():
                    if idx in selected_set:
                        continue
                    name = str(row["name"])
                    if use_name_cap and max_per_activity_name > 0:
                        if name_counts[name] >= max_per_activity_name:
                            continue
                    selected_indices.append(idx)
                    selected_set.add(idx)
                    group_counts[group] += 1
                    name_counts[name] += 1
                    made_progress = True
                    break

    take_candidates(use_group_cap=True, use_name_cap=True)
    if len(selected_indices) < count:
        take_candidates(use_group_cap=True, use_name_cap=False)
    if len(selected_indices) < count:
        take_candidates(use_group_cap=False, use_name_cap=False)

    selected = candidates.loc[selected_indices].copy()
    selected["_main_cpc_sort"] = selected["main_cpc_code"].map(cpc_sort_value)
    selected = selected.sort_values(
        ["_main_cpc_sort", "gwp100_incl_bio_co2", "activity_index"],
        ascending=[True, True, True],
    ).drop(columns="_main_cpc_sort")
    return selected.head(count)


def select_activities(
    scores: pd.DataFrame,
    cpc_metadata: pd.DataFrame,
    cdr_market_order: int,
    pre_cdr_order: int,
    min_nonclimate_increase: float,
    max_per_main_cpc: int,
    max_per_activity_name: int,
    count: int,
) -> pd.DataFrame:
    cdr_market = scores[
        (scores["database_order"] == cdr_market_order)
        & (scores["gwp100_incl_bio_co2"] > 0)
    ].copy()
    cdr_market = cdr_market[cdr_market.apply(is_end_use_activity, axis=1)].copy()
    pre_cdr = scores[scores["database_order"] == pre_cdr_order][
        ["activity_index", "gwp100_incl_bio_co2", *NONCLIMATE_COLUMNS]
    ].rename(
        columns={
            "gwp100_incl_bio_co2": "pre_cdr_gwp100_incl_bio_co2",
            **{column: f"pre_cdr_{column}" for column in NONCLIMATE_COLUMNS},
        }
    )
    cdr_market = cdr_market.merge(pre_cdr, on="activity_index", how="inner")
    cdr_market = cdr_market[cdr_market["pre_cdr_gwp100_incl_bio_co2"] >= 0].copy()

    ratio_columns = []
    for column in NONCLIMATE_COLUMNS:
        ratio_column = f"{column}_increase_ratio"
        abs_column = f"{column}_increase_abs"
        pre_column = f"pre_cdr_{column}"
        denominator = cdr_market[pre_column].where(cdr_market[pre_column].abs() > 1e-30)
        cdr_market[ratio_column] = (
            cdr_market[column] - cdr_market[pre_column]
        ) / denominator
        cdr_market[abs_column] = cdr_market[column] - cdr_market[pre_column]
        ratio_columns.append(ratio_column)

    cdr_market["max_nonclimate_increase_ratio"] = cdr_market[ratio_columns].max(axis=1)
    cdr_market["nonclimate_indicators_increased"] = (
        cdr_market[ratio_columns] >= min_nonclimate_increase
    ).sum(axis=1)
    cdr_market["max_nonclimate_increase_indicator"] = (
        cdr_market[ratio_columns].idxmax(axis=1).str.removesuffix("_increase_ratio")
    )
    cdr_market = cdr_market[
        cdr_market["max_nonclimate_increase_ratio"] >= min_nonclimate_increase
    ].copy()
    cdr_market = add_cpc_metadata(cdr_market, cpc_metadata)
    selected = diversified_head(
        cdr_market,
        count=count,
        max_per_main_cpc=max_per_main_cpc,
        max_per_activity_name=max_per_activity_name,
    )
    selected = selected[
        [
            "activity_index",
            "name",
            "reference_product",
            "location",
            "unit",
            "database_order",
            "scenario",
            "database",
            "cpc",
            "main_cpc_code",
            "main_cpc_group",
            "pre_cdr_gwp100_incl_bio_co2",
            "gwp100_incl_bio_co2",
            "max_nonclimate_increase_ratio",
            "max_nonclimate_increase_indicator",
            "nonclimate_indicators_increased",
            *[f"pre_cdr_{column}" for column in NONCLIMATE_COLUMNS],
            *NONCLIMATE_COLUMNS,
            *[f"{column}_increase_ratio" for column in NONCLIMATE_COLUMNS],
            *[f"{column}_increase_abs" for column in NONCLIMATE_COLUMNS],
        ]
    ]
    selected = selected.rename(
        columns={"gwp100_incl_bio_co2": "cdr_market_gwp100_incl_bio_co2"}
    )
    return selected


def plot_pdf(
    scores: pd.DataFrame,
    selected: pd.DataFrame,
    units: dict[str, str],
    output: Path,
    activities_per_page: int,
    min_nonclimate_increase: float,
) -> None:
    scenario_rows = (
        scores[["database_order", "scenario"]]
        .drop_duplicates()
        .sort_values("database_order")
    )
    bar_scenario_rows = scenario_rows[
        scenario_rows["database_order"] <= CDR_MARKET_DATABASE_ORDER
    ]
    marker_scenario_rows = scenario_rows[
        scenario_rows["database_order"] > CDR_MARKET_DATABASE_ORDER
    ]
    bar_scenario_order = bar_scenario_rows["scenario"].tolist()
    marker_scenario_order = marker_scenario_rows["scenario"].tolist()
    color_by_scenario = dict(zip(scenario_rows["scenario"], COLORS, strict=True))
    marker_by_scenario = dict(zip(marker_scenario_order, CDR_MARKERS, strict=True))
    selected_ids = selected["activity_index"].tolist()
    selected_scores = scores[scores["activity_index"].isin(selected_ids)].copy()
    cdr_market_x = bar_scenario_order.index("2060 all + CDR market")
    marker_offsets = np.linspace(-0.26, 0.26, len(marker_scenario_order))
    pages = []
    group_sizes = selected["main_cpc_group"].value_counts(sort=False).to_dict()
    for group_label, group_selected in selected.groupby("main_cpc_group", sort=False):
        for start in range(0, len(group_selected), activities_per_page):
            pages.append(
                (
                    group_label,
                    group_selected.iloc[start : start + activities_per_page],
                )
            )

    output.parent.mkdir(parents=True, exist_ok=True)
    with PdfPages(output) as pdf:
        total_pages = len(pages)
        for page_number, (group_label, page_selected) in enumerate(pages, start=1):
            nrows = len(page_selected)
            fig, axes = plt.subplots(
                nrows=nrows,
                ncols=len(INDICATORS),
                figsize=(6.0 * len(INDICATORS), 3.5 * nrows + 1.6),
                constrained_layout=True,
                sharex=True,
            )
            if nrows == 1:
                axes = np.asarray([axes])

            for row_idx, (_, activity) in enumerate(page_selected.iterrows()):
                activity_scores = selected_scores[
                    selected_scores["activity_index"] == activity["activity_index"]
                ]
                label = activity_label(activity)
                label += (
                    f"\nCDR market GWP: "
                    f"{activity['cdr_market_gwp100_incl_bio_co2']:.3g} kg CO2-Eq; "
                    f"pre-CDR: {activity['pre_cdr_gwp100_incl_bio_co2']:.3g}; "
                    f"max non-clim. +{activity['max_nonclimate_increase_ratio']:.0%}"
                )

                for col_idx, (column, indicator) in enumerate(INDICATORS):
                    ax = axes[row_idx][col_idx]
                    bar_subset = (
                        activity_scores.set_index("scenario")
                        .reindex(bar_scenario_order)
                        .reset_index()
                    )
                    marker_subset = (
                        activity_scores.set_index("scenario")
                        .reindex(marker_scenario_order)
                        .reset_index()
                    )
                    x = np.arange(len(bar_scenario_order))
                    values = bar_subset[column].to_numpy(dtype=float)
                    ax.bar(
                        x,
                        values,
                        color=[color_by_scenario[item] for item in bar_scenario_order],
                        width=0.76,
                    )
                    for marker_idx, marker_row in marker_subset.iterrows():
                        scenario = marker_row["scenario"]
                        ax.scatter(
                            cdr_market_x + marker_offsets[marker_idx],
                            float(marker_row[column]),
                            color=color_by_scenario[scenario],
                            edgecolor="#111827",
                            linewidth=0.45,
                            marker=marker_by_scenario[scenario],
                            s=34,
                            zorder=4,
                        )
                    ax.axhline(0, color="#111827", linewidth=0.8)
                    limit_values = pd.concat(
                        [bar_subset[column], marker_subset[column]], ignore_index=True
                    )
                    lo, hi = finite_limits(
                        limit_values,
                        force_zero_floor=column != "gwp100_incl_bio_co2",
                    )
                    ax.set_ylim(lo, hi)
                    ax.grid(axis="y", color="#e5e7eb", linewidth=0.8)
                    ax.set_axisbelow(True)
                    ax.tick_params(axis="y", labelsize=8)

                    if row_idx == 0:
                        ax.set_title(f"{indicator}\n[{units[column]}]", fontsize=11)
                    if col_idx == 0:
                        ax.set_ylabel(wrap_label(label, 32), fontsize=8)

                    ax.set_xticks(x)
                    if row_idx == nrows - 1:
                        ax.set_xticklabels(
                            [wrap_label(item, 18) for item in bar_scenario_order],
                            rotation=55,
                            ha="right",
                            fontsize=8,
                        )
                    else:
                        ax.set_xticklabels([])

            fig.suptitle(
                "C400 common non-market activities with near-zero positive GWP "
                "under CDR market allocation, grouped by main CPC code; "
                f"{group_label} (n={group_sizes[group_label]}); "
                f">= {min_nonclimate_increase:.0%} non-climate increase "
                f"(page {page_number}/{total_pages})",
                fontsize=15,
                y=1.01,
            )
            legend_handles = [
                plt.Line2D(
                    [0],
                    [0],
                    marker=marker_by_scenario[scenario],
                    color="none",
                    markerfacecolor=color_by_scenario[scenario],
                    markeredgecolor="#111827",
                    markeredgewidth=0.45,
                    markersize=7,
                    label=scenario.removeprefix("CDR "),
                )
                for scenario in marker_scenario_order
            ]
            fig.legend(
                handles=legend_handles,
                loc="lower center",
                ncol=3,
                fontsize=8,
                frameon=False,
                title="Technology-specific CDR scores shown as markers on the CDR market bar",
                title_fontsize=9,
                bbox_to_anchor=(0.5, -0.02),
            )
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)


def main() -> None:
    args = parse_args()
    if args.count <= 0:
        raise ValueError("--count must be positive")
    if args.activities_per_page <= 0:
        raise ValueError("--activities-per-page must be positive")

    scores = pd.read_csv(args.scores)
    units = load_units(args.methods)
    cpc_metadata = load_cpc_metadata(scores, args.project, args.cpc_database_order)
    selected = select_activities(
        scores,
        cpc_metadata,
        args.cdr_market_order,
        args.pre_cdr_order,
        args.min_nonclimate_increase,
        args.max_per_main_cpc,
        args.max_per_activity_name,
        args.count,
    )
    if len(selected) < args.count:
        raise ValueError(
            f"Only found {len(selected)} activities with positive CDR-market GWP"
        )

    args.selection.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(args.selection, index=False)
    plot_pdf(
        scores=scores,
        selected=selected,
        units=units,
        output=args.output,
        activities_per_page=args.activities_per_page,
        min_nonclimate_increase=args.min_nonclimate_increase,
    )
    print(f"Wrote {args.selection}")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
