"""
Compare two temporal_distributions files and create an Excel comparison output.

Adds:
- absolute differences
- percentage absolute lifetime difference
- ecoinvent_evidence_mode
- ecoinvent_direct_evidence_snippet
"""

import pandas as pd
import numpy as np
from pathlib import Path


# =========================
# USER INPUT
# =========================
BASE_FILE = Path(r"temporal_distributions.csv")
REVIEW_FILE = Path(r"stock_asset_review_bw25_with_iedc.csv")
OUTPUT_FILE = Path(r"comparison_output.xlsx")

ONLY_STOCK_ASSETS = True  # set to False to export everything

def find_repo_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "premise").exists():
            return p
    raise RuntimeError("Could not find repo root: no 'premise' folder found.")


BASE_DIR = find_repo_root(Path(__file__).resolve())
HEAD_FILE = BASE_DIR / "premise" / "data" / "trails" / "temporal_distributions.csv"


# =========================
# SETTINGS
# =========================
KEY_COLS = ["name", "reference product"]

COMPARE_COLS = {
    "temporal_tag": ("old_temporal_tag", "new_temporal_tag"),
    "lifetime": ("old_lifetime", "new_lifetime"),
    "age distribution type": ("old_age_distribution_type", "new_age_distribution_type"),
    "loc": ("old_loc", "new_loc"),
    "scale": ("old_scale", "new_scale"),
    "minimum": ("old_minimum", "new_minimum"),
    "maximum": ("old_maximum", "new_maximum"),
}

EVIDENCE_COLS = [
    "ecoinvent_evidence_mode",
    "ecoinvent_direct_evidence_snippet",
]


# =========================
# HELPERS
# =========================
def read_any(fp: Path) -> pd.DataFrame:
    fp = Path(fp)
    suffix = fp.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(fp, encoding="utf-8-sig", low_memory=False)

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(fp, engine="openpyxl")

    raise ValueError(f"Unsupported file format: {fp}")


def clean(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def col_letter(idx: int) -> str:
    """Convert 0-based column index to Excel column letter."""
    letters = ""
    while idx >= 0:
        letters = chr(idx % 26 + 65) + letters
        idx = idx // 26 - 1
    return letters


# =========================
# LOAD DATA
# =========================
df_base = read_any(BASE_FILE)
df_head = read_any(HEAD_FILE)

df_base["base_line"] = df_base.index + 2
df_head["head_line"] = df_head.index + 2

for col in KEY_COLS:
    df_base[col] = df_base[col].map(clean)
    df_head[col] = df_head[col].map(clean)


# =========================
# MERGE BASE AND HEAD
# =========================
df = df_base.merge(
    df_head,
    on=KEY_COLS,
    how="outer",
    suffixes=("_base", "_head"),
    indicator=True,
)


# =========================
# BUILD OUTPUT
# =========================
rows = []

for _, r in df.iterrows():

    if r["_merge"] == "both":
        change_type = "modified"
    elif r["_merge"] == "left_only":
        change_type = "removed"
    else:
        change_type = "added"

    out = {
        "change_type": change_type,
        "base_line": r.get("base_line", ""),
        "head_line": r.get("head_line", ""),
        "name": r["name"],
        "reference product": r["reference product"],
    }

    for meta_col in ["ISIC rev.4 ecoinvent", "CPC", "EcoSpold01Categories"]:
        base_val = r.get(f"{meta_col}_base", np.nan)
        head_val = r.get(f"{meta_col}_head", np.nan)
        out[meta_col] = head_val if pd.notna(head_val) else base_val

    changed_fields = []

    for col, (old_col, new_col) in COMPARE_COLS.items():
        old_val = r.get(f"{col}_base", np.nan)
        new_val = r.get(f"{col}_head", np.nan)

        out[old_col] = old_val
        out[new_col] = new_val

        if pd.notna(old_val) and pd.notna(new_val):
            if old_val != new_val:
                changed_fields.append(col)
        elif pd.isna(old_val) != pd.isna(new_val):
            changed_fields.append(col)

    out["changed_fields"] = "; ".join(changed_fields)
    rows.append(out)

df_out = pd.DataFrame(rows)


# =========================
# ADD EVIDENCE COLUMNS FROM REVIEW FILE
# =========================
df_review = read_any(REVIEW_FILE)

for col in KEY_COLS:
    df_review[col] = df_review[col].map(clean)

missing_evidence_cols = [c for c in EVIDENCE_COLS if c not in df_review.columns]
if missing_evidence_cols:
    raise KeyError(f"Review file is missing evidence columns: {missing_evidence_cols}")

df_evidence = df_review[KEY_COLS + EVIDENCE_COLS].copy()
df_evidence = df_evidence.drop_duplicates(subset=KEY_COLS, keep="last")

df_out = df_out.merge(
    df_evidence,
    on=KEY_COLS,
    how="left",
)


# =========================
# SORT BY LIFETIME DIFFERENCE
# =========================
df_out["_sort_lifetime_diff"] = (
    pd.to_numeric(df_out["new_lifetime"], errors="coerce")
    - pd.to_numeric(df_out["old_lifetime"], errors="coerce")
).abs()

df_out = df_out.sort_values(
    by="_sort_lifetime_diff",
    ascending=False,
    na_position="last",
).drop(columns=["_sort_lifetime_diff"])


# =========================
# OPTIONAL FILTER: ONLY STOCK ASSETS
# =========================
if ONLY_STOCK_ASSETS:
    before = len(df_out)

    df_out = df_out[
        df_out["old_temporal_tag"].astype(str).str.strip().str.lower() == "stock_asset"
    ].copy()

    after = len(df_out)

    print(f"Filtered to stock_asset only: {after} rows (removed {before - after})")
    
# =========================
# WRITE TO EXCEL WITH FORMULAS
# =========================
with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
    df_out.to_excel(writer, index=False, sheet_name="comparison")

    wb = writer.book
    ws = writer.sheets["comparison"]

    percent_fmt = wb.add_format({"num_format": "0%"})
    text_wrap_fmt = wb.add_format({"text_wrap": True, "valign": "top"})

    col_map = {col: i for i, col in enumerate(df_out.columns)}

    formula_cols = [
        ("diff lifetime", "old_lifetime", "new_lifetime", "abs"),
        ("diff lifetime pct", "old_lifetime", "new_lifetime", "pct"),
        ("diff age", "old_age_distribution_type", "new_age_distribution_type", "abs"),
        ("diff min", "old_minimum", "new_minimum", "abs"),
        ("diff max", "old_maximum", "new_maximum", "abs"),
    ]

    start_col = len(df_out.columns)

    for i, (name, old_c, new_c, mode) in enumerate(formula_cols):
        col_idx = start_col + i
        ws.write(0, col_idx, name)

        old_idx = col_map.get(old_c)
        new_idx = col_map.get(new_c)

        if old_idx is None or new_idx is None:
            continue

        old_letter = col_letter(old_idx)
        new_letter = col_letter(new_idx)

        for row in range(1, len(df_out) + 1):
            excel_row = row + 1

            if mode == "abs":
                formula = (
                    f'=IF(OR({old_letter}{excel_row}="",'
                    f'{new_letter}{excel_row}=""),"",'
                    f'ABS({new_letter}{excel_row}-{old_letter}{excel_row}))'
                )
                ws.write_formula(row, col_idx, formula)

            elif mode == "pct":
                formula = (
                    f'=IF(OR({old_letter}{excel_row}="",'
                    f'{new_letter}{excel_row}="",'
                    f'{old_letter}{excel_row}=0),"",'
                    f'ABS(({new_letter}{excel_row}-{old_letter}{excel_row})/'
                    f'{old_letter}{excel_row}))'
                )
                ws.write_formula(row, col_idx, formula, percent_fmt)

    # Formatting
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, len(df_out), len(df_out.columns) + len(formula_cols) - 1)

    for col_name, width in {
        "name": 45,
        "reference product": 35,
        "changed_fields": 35,
        "ecoinvent_evidence_mode": 25,
        "ecoinvent_direct_evidence_snippet": 80,
    }.items():
        if col_name in col_map:
            ws.set_column(col_map[col_name], col_map[col_name], width, text_wrap_fmt)

    ws.set_column(0, len(df_out.columns) + len(formula_cols), 16)


print(f"Comparison file written to: {OUTPUT_FILE}")