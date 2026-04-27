#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Update temporal_distributions.csv with reviewed stock-asset lifetime parameters.

If a reviewed row is no longer considered a plausible stock asset, the script
does NOT automatically convert it to throughput_process. Instead, it tries to
assign one of the valid temporal tags:

- biomass_growth
- end_of_life
- maintenance
- market
- stock_asset
- throughput_process

Priority:
1. Use reviewed column codex_suggested_temporal_tag if available and valid.
2. Otherwise infer temporal_tag from name + reference product using heuristics.
3. Final fallback is throughput_process.
"""

from pathlib import Path
from collections import Counter

import numpy as np
import pandas as pd


# =============================================================================
# USER SETTINGS
# =============================================================================

REPLACE_FILE_FOR_TRAILS = True

ORIGINAL_FP = r"temporal_distributions.csv"
REVIEWED_FP = r"stock_asset_review_bw25_with_iedc.csv"
OUT_FP = r"temporal_distributions_stock_asset_updated.csv"

VALID_TEMPORAL_TAGS = {
    "biomass_growth",
    "end_of_life",
    "maintenance",
    "market",
    "stock_asset",
    "throughput_process",
}


# =============================================================================
# PATH HELPERS
# =============================================================================

def find_repo_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "premise").exists():
            return p
    raise RuntimeError("Could not find repo root: no 'premise' folder found.")


BASE_DIR = find_repo_root(Path(__file__).resolve())
OUT_FP_TRAILS = BASE_DIR / "premise" / "data" / "trails" / "temporal_distributions.csv"


# =============================================================================
# HELPERS
# =============================================================================

def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def normalize_tag(x) -> str:
    return normalize_text(x).lower().replace(" ", "_").replace("-", "_")


def build_key(df: pd.DataFrame) -> pd.Series:
    required = ["name", "reference product", "CPC", "temporal_tag"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns for key construction: {missing}")

    return (
        df["name"].map(normalize_text)
        + " || " + df["reference product"].map(normalize_text)
        + " || " + df["CPC"].map(normalize_text)
        + " || " + df["temporal_tag"].map(normalize_text)
    )


def is_false_like(x) -> bool:
    if pd.isna(x):
        return False
    return str(x).strip().lower() in {"false", "0", "no", "n"}


def infer_temporal_tag(name, ref_product) -> str:
    """
    Conservative fallback classifier for rows that are not plausible stock assets.
    """

    name = normalize_text(name).lower()
    ref = normalize_text(ref_product).lower()
    text = f"{name} {ref}"

    # Markets
    if (
        name.startswith("market for ")
        or name.startswith("market group for ")
        or " market for " in text
        or "market group for" in text
    ):
        return "market"

    # Maintenance / repair / replacement services
    if any(
        k in text
        for k in [
            "maintenance",
            "repair",
            "servicing",
            "service life repair",
            "replacement",
            "overhaul",
            "inspection",
        ]
    ):
        return "maintenance"

    # End-of-life / waste handling
    if any(
        k in text
        for k in [
            "waste",
            "disposal",
            "treatment of",
            "scrap",
            "recycling",
            "incineration",
            "landfill",
            "end of life",
            "end-of-life",
            "dismantling",
            "decommissioning",
        ]
    ):
        return "end_of_life"

    # Biomass growth
    if any(
        k in text
        for k in [
            "biomass growth",
            "forest growth",
            "tree growth",
            "wood growth",
            "crop growth",
            "cultivation",
            "forestry",
            "plantation",
            "harvesting",
            "standing biomass",
        ]
    ):
        return "biomass_growth"

    # Default process-like activity
    return "throughput_process"


def clear_distribution_columns(df: pd.DataFrame, idx) -> None:
    cols_to_clear = [
        "age distribution type",
        "loc",
        "scale",
        "offsets",
        "weights",
        "minimum",
        "maximum",
    ]

    for col in cols_to_clear:
        if col in df.columns:
            df.loc[idx, col] = np.nan


# =============================================================================
# LOAD DATA
# =============================================================================

df_orig = pd.read_csv(ORIGINAL_FP, encoding="utf-8-sig", low_memory=False)
df_rev = pd.read_csv(REVIEWED_FP, encoding="utf-8-sig", low_memory=False)

df_orig["_key"] = build_key(df_orig)
df_rev["_key"] = build_key(df_rev)


# =============================================================================
# FILTER REVIEWED STOCK ASSET ROWS
# =============================================================================

df_rev = df_rev[df_rev["temporal_tag"] == "stock_asset"].copy()


# =============================================================================
# DUPLICATE HANDLING
# =============================================================================

dups = df_rev["_key"].duplicated(keep=False)

if dups.any():
    print("Found duplicate reviewed keys. Showing first 20 duplicates:")
    print(
        df_rev.loc[dups, ["name", "reference product", "CPC", "temporal_tag", "_key"]]
        .head(20)
        .to_string(index=False)
    )

    df_rev = df_rev.drop_duplicates(subset="_key", keep="last").copy()
    print(f"After dropping duplicates, reviewed rows = {len(df_rev)}")


# =============================================================================
# CREATE LOOKUP
# =============================================================================

rev_map = df_rev.set_index("_key")


# =============================================================================
# SELECT MATCHING STOCK ASSET ROWS
# =============================================================================

stock_mask = df_orig["temporal_tag"] == "stock_asset"
match_mask = stock_mask & df_orig["_key"].isin(rev_map.index)

print(f"Total rows: {len(df_orig)}")
print(f"Original stock_asset rows: {stock_mask.sum()}")
print(f"Matched reviewed stock_asset rows: {match_mask.sum()}")


# =============================================================================
# UPDATE STOCK ASSET PARAMETERS
# =============================================================================

keys = df_orig.loc[match_mask, "_key"]

column_mapping = {
    "lifetime": "codex_suggested_lifetime",
    "age distribution type": "codex_suggested_distribution_type",
    "loc": "codex_suggested_loc",
    "scale": "codex_suggested_scale",
    "minimum": "codex_suggested_minimum",
    "maximum": "codex_suggested_maximum",
}

for target_col, source_col in column_mapping.items():
    if source_col not in rev_map.columns:
        print(f"WARNING: Reviewed file has no column '{source_col}'. Skipping.")
        continue

    if target_col not in df_orig.columns:
        print(f"WARNING: Original file has no column '{target_col}'. Skipping.")
        continue

    df_orig.loc[match_mask, target_col] = keys.map(rev_map[source_col])


# =============================================================================
# HANDLE ROWS THAT ARE NOT PLAUSIBLE STOCK ASSETS
# =============================================================================

if "codex_unit_plausible_for_stock_asset" in rev_map.columns:
    plausible = keys.map(rev_map["codex_unit_plausible_for_stock_asset"])
    implausible_mask = plausible.map(is_false_like)

    implausible_idx = df_orig.loc[match_mask].index[implausible_mask]

    print(f"Rows no longer considered plausible stock assets: {len(implausible_idx)}")

    reclassified = []

    for idx in implausible_idx:
        key = df_orig.at[idx, "_key"]

        suggested_tag = None

        if "codex_suggested_temporal_tag" in rev_map.columns:
            raw_tag = rev_map.at[key, "codex_suggested_temporal_tag"]
            suggested_tag = normalize_tag(raw_tag)

        if suggested_tag in VALID_TEMPORAL_TAGS and suggested_tag != "stock_asset":
            new_tag = suggested_tag
            source = "codex_suggested_temporal_tag"
        else:
            new_tag = infer_temporal_tag(
                df_orig.at[idx, "name"],
                df_orig.at[idx, "reference product"],
            )
            source = "heuristic"

        df_orig.at[idx, "temporal_tag"] = new_tag
        clear_distribution_columns(df_orig, idx)

        reclassified.append(
            {
                "idx": idx,
                "name": df_orig.at[idx, "name"],
                "reference product": df_orig.at[idx, "reference product"],
                "new_temporal_tag": new_tag,
                "source": source,
            }
        )

    if reclassified:
        df_reclassified = pd.DataFrame(reclassified)

        print("\nReclassified non-stock assets by temporal_tag:")
        print(Counter(df_reclassified["new_temporal_tag"]))

        print("\nReclassified non-stock assets by source:")
        print(Counter(df_reclassified["source"]))

        print("\nFirst 30 reclassified rows:")
        print(
            df_reclassified[
                ["idx", "name", "reference product", "new_temporal_tag", "source"]
            ]
            .head(30)
            .to_string(index=False)
        )

else:
    print(
        "WARNING: Column 'codex_unit_plausible_for_stock_asset' not found. "
        "No rows were reclassified."
    )


# =============================================================================
# CLEAN NUMERIC COLUMNS
# =============================================================================

for col in ["lifetime", "age distribution type", "loc", "scale", "minimum", "maximum"]:
    if col in df_orig.columns:
        df_orig[col] = pd.to_numeric(df_orig[col], errors="coerce")


# =============================================================================
# FINAL CHECKS
# =============================================================================

invalid_tags = sorted(set(df_orig["temporal_tag"].dropna().astype(str)) - VALID_TEMPORAL_TAGS)

if invalid_tags:
    print("\nWARNING: Invalid temporal_tag values found:")
    for tag in invalid_tags:
        print(f"  - {tag}")
else:
    print("\nAll temporal_tag values are valid.")


# =============================================================================
# SAVE
# =============================================================================

df_orig = df_orig.drop(columns=["_key"])

df_orig.to_csv(OUT_FP, index=False, encoding="utf-8-sig")

print("\nDone. File written to:")
print(OUT_FP)

if REPLACE_FILE_FOR_TRAILS:
    df_orig.to_csv(OUT_FP_TRAILS, index=False, encoding="utf-8-sig")
    print(OUT_FP_TRAILS)