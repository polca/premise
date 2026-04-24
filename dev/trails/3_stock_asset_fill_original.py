"""
Update `temporal_distributions.csv` with reviewed stock-asset lifetime parameters.

Purpose
-------
This script takes:
1. the original TRAILS temporal distributions file, and
2. a reviewed stock-asset file containing updated Codex suggestions,

and writes a new CSV in which matching `stock_asset` rows are updated with the
reviewed temporal parameters.

The update is performed only for rows with:
- `temporal_tag == "stock_asset"` in the original file, and
- a matching composite key in the reviewed file.

Matching logic
--------------
Rows are matched using a composite key built from:

- `name`
- `reference product`
- `CPC`
- `temporal_tag`

This is used to map reviewed values back onto the original temporal distributions
table.

Updated columns
---------------
For matched stock-asset rows, the following columns are overwritten using the
reviewed Codex suggestions:

- `lifetime`                    <- `codex_suggested_lifetime`
- `age distribution type`       <- `codex_suggested_distribution_type`
- `loc`                         <- `codex_suggested_loc`
- `scale`                       <- `codex_suggested_scale`
- `minimum`                     <- `codex_suggested_minimum`
- `maximum`                     <- `codex_suggested_maximum`

The following are intentionally left unchanged:
- offsets
- weights

Duplicate handling
------------------
If duplicate composite keys are found in the reviewed stock-asset file, the script:
1. prints the first 20 duplicate rows for inspection
2. keeps only the last occurrence of each duplicate key

Additional plausibility handling
--------------------------------
If the reviewed file contains `codex_unit_plausible_for_stock_asset` and this
value is False for a matched stock-asset row, the row is no longer treated as a
stock asset. In the output file:

- `temporal_tag` is changed to `throughput_process`
- `age distribution type`
- `loc`
- `scale`
- `offsets`
- `weights`
- `minimum`
- `maximum`

are cleared.

Inputs
------
- `original_fp`: path to the original `temporal_distributions.csv`
- `reviewed_fp`: path to the reviewed stock-asset file
- `out_fp`: path to the updated output CSV

Output
------
A new CSV file is written to `out_fp` containing the updated temporal
distribution parameters for matched stock assets.

Typical use case
----------------
Use this script after manually or semi-automatically reviewing stock-asset
lifetimes and distribution parameters, in order to write the accepted values
back into the main TRAILS temporal distributions file.

Notes
-----
- Only rows tagged as `stock_asset` are updated.
- Non-matching rows are left unchanged.
- All updated numeric columns are coerced to numeric using `errors="coerce"`.
- The helper `_key` column is removed before saving.
"""
from pathlib import Path

# to decide whether to use and overwrite original temporal_distributions.csv for trails, or write to a separate file
REPLACE_FILE_FOR_TRAILS = True

# === USER INPUT ===
ORIGINAL_FP = r"temporal_distributions.csv"
REVIEWED_FP = r"stock_asset_review_bw25_with_iedc.csv"
OUT_FP = r"temporal_distributions_stock_asset_updated.csv"

def find_repo_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "premise").exists():
            return p
    raise RuntimeError("Could not find repo root (no 'premise' folder found)")

BASE_DIR = find_repo_root(Path(__file__).resolve())
OUT_FP_TRAILS = BASE_DIR / "premise" / "data" / "trails" / "temporal_distributions.csv"

# === IMPORTS ===
import pandas as pd
import numpy as np

# === HELPERS ===
def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def build_key(df):
    return (
        df["name"].map(normalize_text)
        + " || " + df["reference product"].map(normalize_text)
        + " || " + df["CPC"].map(normalize_text)
        + " || " + df["temporal_tag"].map(normalize_text)
    )

# === LOAD DATA ===
df_orig = pd.read_csv(ORIGINAL_FP, encoding="utf-8-sig", low_memory=False)
df_rev = pd.read_csv(REVIEWED_FP, encoding="utf-8-sig", low_memory=False)

# === BUILD MATCH KEY ===
df_orig["_key"] = build_key(df_orig)
df_rev["_key"] = build_key(df_rev)

# === FILTER STOCK ASSETS ===
df_rev = df_rev[df_rev["temporal_tag"] == "stock_asset"].copy()

# === CHECK DUPLICATES IN REVIEWED FILE ===
dups = df_rev["_key"].duplicated(keep=False)
if dups.any():
    print("Found duplicate reviewed keys. Showing first 20 duplicates:")
    print(
        df_rev.loc[dups, ["name", "reference product", "CPC", "temporal_tag", "_key"]]
        .head(20)
        .to_string(index=False)
    )

    # keep the last occurrence
    df_rev = df_rev.drop_duplicates(subset="_key", keep="last").copy()
    print(f"After dropping duplicates, reviewed rows = {len(df_rev)}")

# === CREATE LOOKUP ===
rev_map = df_rev.set_index("_key")

# === SELECT MATCHING ROWS ===
stock_mask = df_orig["temporal_tag"] == "stock_asset"
match_mask = stock_mask & df_orig["_key"].isin(rev_map.index)

print(f"Total rows: {len(df_orig)}")
print(f"Stock asset rows: {stock_mask.sum()}")
print(f"Matched rows: {match_mask.sum()}")

# === UPDATE ORIGINAL COLUMNS ===
keys = df_orig.loc[match_mask, "_key"]

df_orig.loc[match_mask, "lifetime"] = keys.map(rev_map["codex_suggested_lifetime"])
df_orig.loc[match_mask, "age distribution type"] = keys.map(rev_map["codex_suggested_distribution_type"])
df_orig.loc[match_mask, "loc"] = keys.map(rev_map["codex_suggested_loc"])
df_orig.loc[match_mask, "scale"] = keys.map(rev_map["codex_suggested_scale"])
df_orig.loc[match_mask, "minimum"] = keys.map(rev_map["codex_suggested_minimum"])
df_orig.loc[match_mask, "maximum"] = keys.map(rev_map["codex_suggested_maximum"])

# === HANDLE ROWS THAT ARE NOT PLAUSIBLE STOCK ASSETS ===
# If Codex says the unit is not plausible for a stock asset:
# - convert temporal_tag to throughput_process
# - clear all temporal distribution columns

if "codex_unit_plausible_for_stock_asset" in rev_map.columns:
    plausible = keys.map(rev_map["codex_unit_plausible_for_stock_asset"])

    implausible_mask = plausible.astype(str).str.strip().str.lower().isin(
        ["false", "0", "no", "n"]
    )

    implausible_idx = df_orig.loc[match_mask].index[implausible_mask]

    print(f"Rows converted from stock_asset to throughput_process: {len(implausible_idx)}")

    df_orig.loc[implausible_idx, "temporal_tag"] = "throughput_process"

    cols_to_clear = [
        #"lifetime",
        "age distribution type",
        "loc",
        "scale",
        "offsets",
        "weights",
        "minimum",
        "maximum",
    ]

    for col in cols_to_clear:
        if col in df_orig.columns:
            df_orig.loc[implausible_idx, col] = np.nan

else:
    print(
        "WARNING: Column 'codex_unit_plausible_for_stock_asset' not found. "
        "No rows converted to throughput_process."
    )

# offsets and weights are left unchanged

# === CLEAN NUMERICS ===
for col in ["lifetime", "age distribution type", "loc", "scale", "minimum", "maximum"]:
    df_orig[col] = pd.to_numeric(df_orig[col], errors="coerce")

# === DROP HELPER COLUMN ===
df_orig = df_orig.drop(columns=["_key"])

# === SAVE ===
df_orig.to_csv(OUT_FP, index=False, encoding="utf-8-sig")

print("Done. File written to, locations:")
print(OUT_FP)

if REPLACE_FILE_FOR_TRAILS:
    df_orig.to_csv(OUT_FP_TRAILS, index=False, encoding="utf-8-sig")
    print(OUT_FP_TRAILS)