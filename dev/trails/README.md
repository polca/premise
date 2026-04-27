# TRAILS Stock-Asset Temporal Review Pipeline

This inner repository contains a structured workflow to review, diagnose, and update stock-asset temporal distributions in TRAILS using ecoinvent-based evidence, IEDC lifetimes, and Codex-supported validation.

---

## 📁 Repository Structure

```
.
├── lt_data/                                      # External lifetime data (from IEDC), credits to: https://www.database.industrialecology.uni-freiburg.de/
├── 0_plot_group_distributions.py                  # Visualization of grouped distributions
├── 1_stock_asset_review_with_codex.py             # Main review of temporal distributions of infrastructure + Codex integration
├── 2_stock_asset_dashboard.py                     # Interactive Dash dashboard
├── 3_stock_asset_fill_original.py                 # Writes reviewed values back to original file
├── 4_validate_differences_files                   # Validates the two temporal distributions files and shows main differences
│
├── stock_asset_grouped_temporal_defaults.xlsx     # Group-based default lifetimes and temporal distributions
├── stock_asset_review_bw25_with_iedc.csv          # Reviewed dataset (main output of step 1)
├── temporal_distributions.csv                     # Original TRAILS input file
├── temporal_distributions_stock_asset_updated.csv # Final updated file (output of step 3)
├── comparison_output.csv                          # Excel file to show differences between the two temporal distributions files 
```

---

## 🚀 Workflow Overview

### Step 0 — Data sanity checks
Run:
```
python 0_general_temporal_tag_check.py
```
- Validates temporal tags
- Identifies missing or inconsistent fields

Optional:
```
python 0_plot_group_distributions.py
```
- Visual inspection of grouped distributions

---

### Step 1 — Stock-asset review (core step)
```
python 1_stock_asset_review_with_codex.py
```

This step:
- Extracts stock-asset rows
- Matches lifetimes using:
  - ecoinvent direct values
  - inferred lifetimes
  - IEDC dataset
  - group-level defaults
- Applies Codex suggestions (if enabled)
- Flags suspicious or inconsistent entries

Output:
```
stock_asset_review_bw25_with_iedc.csv and potentially writing back temporal_distributions.csv back to premise to be used for new temporal distributions.
```

---

### Step 2 — Interactive dashboard
```
python 2_stock_asset_dashboard.py --input stock_asset_review_bw25_with_iedc.csv
```

Then open:
```
http://127.0.0.1:8050/
```

Features:
- Filtering (CPC, group, flags, etc.)
- Outlier detection (IQR-based)
- Distribution preview
- Lifetime comparison plots
- Diagnostics for:
  - suspicious ecoinvent values
  - codex gaps
  - extreme lifetimes

---

### Step 3 — Write back to TRAILS
```
python 3_stock_asset_fill_original.py
```

This step:
- Matches reviewed rows back to original dataset
- Updates:
  - lifetime
  - distribution type
  - loc / scale / min / max
- Keeps non-stock rows unchanged

Output:
```
temporal_distributions_stock_asset_updated.csv
```

---

## 🔑 Matching Logic

Rows are matched using a composite key:

```
name || reference product || CPC || temporal_tag
```

Only rows with:
```
temporal_tag == "stock_asset"
```
are updated.

---

## ⚠️ Key Assumptions

- Matching is strict (no fuzzy matching)
- Duplicate reviewed keys → last entry is used
- Numeric columns are coerced (invalid → NaN)
- Offsets and weights are not modified

---

## 📦 Requirements

Create a `requirements.txt`:

```
numpy>=1.24
pandas>=2.0
plotly>=5.18
dash>=2.16
openpyxl>=3.1
```

Install:
```
pip install -r requirements.txt
```

---

## 💡 Typical Usage

```
# Step 1: review
python 1_stock_asset_review_with_codex.py

# Step 2: inspect
python 2_stock_asset_dashboard.py --input stock_asset_review_bw25_with_iedc.csv

# Step 3: write back
python 3_stock_asset_fill_original.py

# Step 4: validate differences
python 4_validate_differences_files.py
```

---

## 🧠 Notes

- Designed for Brightway / ecoinvent workflows
- Focuses on robust lifetime consistency
- Supports hybrid evidence (ecoinvent + IEDC + Codex)

---

## 📬 Contact

Maintained within the TRAILS / premise workflow environment.