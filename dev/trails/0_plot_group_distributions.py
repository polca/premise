#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Plot grouped stock-asset fleet-vintage distributions from the Excel workbook.

Interpretation used here
-----------------------
The distribution parameters are interpreted as the timing of when the existing
fleet/stock was installed before the commissioning/start year:

- x-axis is "years before commissioning", shown as negative values
  e.g. -40 means "installed 40 years before commissioning"

Distribution types:
- 2 = lognormal  -> loc = median lag before commissioning (negative years),
                    scale = geometric standard deviation (GSD)
- 3 = normal     -> loc = mean lag before commissioning, scale = std. dev.
- 4 = uniform    -> minimum and maximum define a flat in-service vintage range
- 5 = triangular -> minimum, loc (mode), and maximum define the vintage shape
- 6 = discrete   -> optional support if offsets/weights columns are present
                    in the same sheet (not common in your current workbook)

The script:
1) reads the Group_Assumptions sheet from the Excel file
2) builds the corresponding distribution for each group
3) normalizes the density / mass for visual comparison
4) saves one PNG per group
5) saves one combined PDF with all plots
6) writes a quick summary CSV of plotting inputs

Usage example
-------------
python plot_group_distributions.py ^

Requirements
------------
pip install pandas numpy matplotlib openpyxl
"""

from __future__ import annotations

import argparse
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import sys
import argparse


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
TEMPORAL_DISTRIBUTION_TYPES: Dict[int, str] = {
    2: "lognormal",
    3: "normal",
    4: "uniform",
    5: "triangular",
    6: "discrete",
}


def normalize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def safe_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, float) and np.isnan(x):
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def sanitize_filename(x: str) -> str:
    s = re.sub(r"[^\w\-\.]+", "_", str(x).strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "group"


def normalize_area(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    area = np.trapz(y, x)
    if not np.isfinite(area) or area <= 0:
        return y
    return y / area


# ------------------------------------------------------------
# Density builders
# ------------------------------------------------------------
def triangular_pdf(x: np.ndarray, a: float, c: float, b: float) -> np.ndarray:
    """
    Piecewise triangular PDF on [a, b] with mode c, where a <= c <= b.
    """
    y = np.zeros_like(x, dtype=float)

    if not (a < b):
        return y

    c = min(max(c, a), b)

    left = (x >= a) & (x <= c)
    right = (x >= c) & (x <= b)

    if c > a:
        y[left] = 2.0 * (x[left] - a) / ((b - a) * (c - a))
    else:
        y[left] = 0.0

    if b > c:
        y[right] = 2.0 * (b - x[right]) / ((b - a) * (b - c))
    else:
        y[right] = 0.0

    # if c equals a or b exactly, the formula becomes degenerate;
    # the curve remains visually fine on the non-degenerate side.
    y[~np.isfinite(y)] = 0.0
    y[y < 0] = 0.0
    return y


def uniform_pdf(x: np.ndarray, a: float, b: float) -> np.ndarray:
    y = np.zeros_like(x, dtype=float)
    if not (a < b):
        return y
    mask = (x >= a) & (x <= b)
    y[mask] = 1.0 / (b - a)
    return y


def normal_pdf(x: np.ndarray, mu: float, sigma: float) -> np.ndarray:
    y = np.zeros_like(x, dtype=float)
    if sigma is None or not np.isfinite(sigma) or sigma <= 0:
        return y
    coeff = 1.0 / (sigma * math.sqrt(2.0 * math.pi))
    y = coeff * np.exp(-0.5 * ((x - mu) / sigma) ** 2)
    y[~np.isfinite(y)] = 0.0
    return y


def lognormal_pdf_negative_axis(
    x: np.ndarray,
    median_negative: float,
    gsd: float,
) -> np.ndarray:
    """
    Interpret loc as median lag before commissioning (negative years).
    Convert to positive age y = -x, then evaluate lognormal on y > 0.
    """
    y = np.zeros_like(x, dtype=float)

    if median_negative is None or not np.isfinite(median_negative):
        return y
    if gsd is None or not np.isfinite(gsd) or gsd <= 1.0:
        return y

    median_age = abs(median_negative)
    if median_age <= 0:
        return y

    sigma_ln = math.log(gsd)
    ages = -x  # positive years before commissioning

    mask = ages > 0
    ages_valid = ages[mask]

    y_valid = (
        1.0
        / (ages_valid * sigma_ln * math.sqrt(2.0 * math.pi))
        * np.exp(-((np.log(ages_valid) - math.log(median_age)) ** 2) / (2.0 * sigma_ln**2))
    )

    y[mask] = y_valid
    y[~np.isfinite(y)] = 0.0
    y[y < 0] = 0.0
    return y


def discrete_mass_series(
    offsets: List[float],
    weights: List[float],
) -> Tuple[np.ndarray, np.ndarray]:
    x = np.array(offsets, dtype=float)
    y = np.array(weights, dtype=float)

    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]

    if len(x) == 0:
        return np.array([]), np.array([])

    order = np.argsort(x)
    x = x[order]
    y = y[order]

    total = y.sum()
    if total > 0:
        y = y / total

    return x, y


# ------------------------------------------------------------
# Reading workbook
# ------------------------------------------------------------
def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {normalize_text(c): c for c in df.columns}
    for cand in candidates:
        key = normalize_text(cand)
        if key in cols:
            return cols[key]
    return None


def load_group_assumptions(xlsx_path: Path, sheet_name: str = "Group_Assumptions") -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    if df.empty:
        raise ValueError(f"Sheet '{sheet_name}' is empty in {xlsx_path}")
    return df


# ------------------------------------------------------------
# Build one distribution from one row
# ------------------------------------------------------------
def build_distribution_from_row(row: pd.Series) -> Dict[str, Any]:
    group = str(row.get("group", "") or "").strip()
    lifetime = safe_float_or_none(row.get("lifetime"))
    dist_type_raw = safe_float_or_none(row.get("age distribution type"))
    loc = safe_float_or_none(row.get("loc"))
    scale = safe_float_or_none(row.get("scale"))
    minimum = safe_float_or_none(row.get("minimum"))
    maximum = safe_float_or_none(row.get("maximum"))

    dist_type = int(dist_type_raw) if dist_type_raw is not None else None
    dist_name = TEMPORAL_DISTRIBUTION_TYPES.get(dist_type, "unknown")

    if lifetime is None or lifetime <= 0:
        lifetime = 20.0

    # Default sensible range if some parameters are missing
    if minimum is None:
        minimum = -lifetime
    if maximum is None:
        maximum = -1.0

    if minimum > maximum:
        minimum, maximum = maximum, minimum

    # Make plotting window a bit wider than support / relevant range
    plot_min = minimum
    plot_max = maximum

    if dist_type == 2:
        # lognormal: extend plotting window a bit
        median_neg = loc if (loc is not None and loc < 0) else -0.5 * lifetime
        gsd = scale if (scale is not None and scale > 1.0) else 1.8

        plot_min = min(minimum, -lifetime * 1.5, median_neg * 2.5)
        plot_max = min(-0.25, maximum)

        x = np.linspace(plot_min, plot_max, 1200)
        y = lognormal_pdf_negative_axis(x, median_negative=median_neg, gsd=gsd)
        y = normalize_area(x, y)

        return {
            "group": group,
            "distribution_type": dist_type,
            "distribution_name": dist_name,
            "x": x,
            "y": y,
            "plot_kind": "line",
            "lifetime": lifetime,
            "loc": median_neg,
            "scale": gsd,
            "minimum": minimum,
            "maximum": maximum,
        }

    if dist_type == 3:
        mu = loc if loc is not None else -0.5 * lifetime
        sigma = scale if (scale is not None and scale > 0) else max(1.0, 0.2 * lifetime)

        plot_min = min(minimum, mu - 4.0 * sigma)
        plot_max = max(maximum, mu + 4.0 * sigma)
        plot_max = min(plot_max, -0.1)

        x = np.linspace(plot_min, plot_max, 1200)
        y = normal_pdf(x, mu=mu, sigma=sigma)
        y = normalize_area(x, y)

        return {
            "group": group,
            "distribution_type": dist_type,
            "distribution_name": dist_name,
            "x": x,
            "y": y,
            "plot_kind": "line",
            "lifetime": lifetime,
            "loc": mu,
            "scale": sigma,
            "minimum": minimum,
            "maximum": maximum,
        }

    if dist_type == 4:
        a = minimum
        b = maximum

        x = np.linspace(a - 0.05 * lifetime, min(-0.1, b + 0.05 * lifetime), 1000)
        y = uniform_pdf(x, a=a, b=b)
        y = normalize_area(x, y)

        return {
            "group": group,
            "distribution_type": dist_type,
            "distribution_name": dist_name,
            "x": x,
            "y": y,
            "plot_kind": "line",
            "lifetime": lifetime,
            "loc": None,
            "scale": None,
            "minimum": a,
            "maximum": b,
        }

    if dist_type == 5:
        a = minimum
        c = loc if loc is not None else -0.5 * lifetime
        b = maximum

        c = min(max(c, a), b)

        x = np.linspace(a - 0.05 * lifetime, min(-0.1, b + 0.05 * lifetime), 1000)
        y = triangular_pdf(x, a=a, c=c, b=b)
        y = normalize_area(x, y)

        return {
            "group": group,
            "distribution_type": dist_type,
            "distribution_name": dist_name,
            "x": x,
            "y": y,
            "plot_kind": "line",
            "lifetime": lifetime,
            "loc": c,
            "scale": None,
            "minimum": a,
            "maximum": b,
        }

    if dist_type == 6:
        # Optional support: try to read offsets / weights from same row if present
        offsets_col = find_column(pd.DataFrame([row]), ["offsets", "weights_offsets", "discrete_offsets"])
        weights_col = find_column(pd.DataFrame([row]), ["weights", "discrete_weights"])

        if offsets_col and weights_col:
            try:
                offsets = [float(v) for v in str(row[offsets_col]).split(",")]
                weights = [float(v) for v in str(row[weights_col]).split(",")]
                x, y = discrete_mass_series(offsets, weights)
            except Exception:
                x, y = np.array([]), np.array([])
        else:
            x, y = np.array([]), np.array([])

        return {
            "group": group,
            "distribution_type": dist_type,
            "distribution_name": dist_name,
            "x": x,
            "y": y,
            "plot_kind": "stem",
            "lifetime": lifetime,
            "loc": loc,
            "scale": scale,
            "minimum": minimum,
            "maximum": maximum,
        }

    # Unknown / unsupported
    x = np.linspace(minimum, maximum, 200)
    y = np.zeros_like(x)

    return {
        "group": group,
        "distribution_type": dist_type,
        "distribution_name": dist_name,
        "x": x,
        "y": y,
        "plot_kind": "line",
        "lifetime": lifetime,
        "loc": loc,
        "scale": scale,
        "minimum": minimum,
        "maximum": maximum,
    }


# ------------------------------------------------------------
# Plotting
# ------------------------------------------------------------
def make_title(rec: Dict[str, Any]) -> str:
    return f"{rec['group']} | type {rec['distribution_type']} ({rec['distribution_name']})"


def make_annotation_text(rec: Dict[str, Any]) -> str:
    parts = [
        f"lifetime = {rec['lifetime']:.3g} yr" if rec["lifetime"] is not None else "lifetime = NA",
        f"loc = {rec['loc']:.3g}" if rec["loc"] is not None else "loc = blank",
        f"scale = {rec['scale']:.3g}" if rec["scale"] is not None else "scale = blank",
        f"minimum = {rec['minimum']:.3g}" if rec["minimum"] is not None else "minimum = blank",
        f"maximum = {rec['maximum']:.3g}" if rec["maximum"] is not None else "maximum = blank",
    ]
    return "\n".join(parts)


def plot_one_distribution(rec: Dict[str, Any], out_png: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.6))

    x = rec["x"]
    y = rec["y"]

    if rec["plot_kind"] == "stem":
        if len(x) > 0:
            markerline, stemlines, baseline = ax.stem(x, y)
            plt.setp(markerline, markersize=5)
            plt.setp(stemlines, linewidth=1.5)
            plt.setp(baseline, linewidth=0.8)
        else:
            ax.text(
                0.5, 0.5,
                "Discrete distribution selected,\nbut no explicit offsets/weights were found.",
                transform=ax.transAxes,
                ha="center", va="center", fontsize=11
            )
    else:
        ax.plot(x, y, linewidth=2)

    # Vertical reference lines
    if rec["minimum"] is not None:
        ax.axvline(rec["minimum"], linestyle="--", linewidth=1)
    if rec["maximum"] is not None:
        ax.axvline(rec["maximum"], linestyle="--", linewidth=1)
    if rec["loc"] is not None and rec["distribution_type"] in {2, 3, 5}:
        ax.axvline(rec["loc"], linestyle=":", linewidth=1.2)

    ax.set_title(make_title(rec))
    ax.set_xlabel("Years before commissioning (negative = installed before start year)")
    ax.set_ylabel("Normalized density / mass")
    ax.grid(True, alpha=0.3)

    annotation = make_annotation_text(rec)
    ax.text(
        0.985,
        0.97,
        annotation,
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.9),
    )

    fig.tight_layout()
    fig.savefig(out_png, dpi=220, bbox_inches="tight")
    plt.close(fig)


def add_distribution_to_pdf(rec: Dict[str, Any], pdf: PdfPages) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.6))

    x = rec["x"]
    y = rec["y"]

    if rec["plot_kind"] == "stem":
        if len(x) > 0:
            markerline, stemlines, baseline = ax.stem(x, y)
            plt.setp(markerline, markersize=5)
            plt.setp(stemlines, linewidth=1.5)
            plt.setp(baseline, linewidth=0.8)
        else:
            ax.text(
                0.5, 0.5,
                "Discrete distribution selected,\nbut no explicit offsets/weights were found.",
                transform=ax.transAxes,
                ha="center", va="center", fontsize=11
            )
    else:
        ax.plot(x, y, linewidth=2)

    if rec["minimum"] is not None:
        ax.axvline(rec["minimum"], linestyle="--", linewidth=1)
    if rec["maximum"] is not None:
        ax.axvline(rec["maximum"], linestyle="--", linewidth=1)
    if rec["loc"] is not None and rec["distribution_type"] in {2, 3, 5}:
        ax.axvline(rec["loc"], linestyle=":", linewidth=1.2)

    ax.set_title(make_title(rec))
    ax.set_xlabel("Years before commissioning (negative = installed before start year)")
    ax.set_ylabel("Normalized density / mass")
    ax.grid(True, alpha=0.3)

    annotation = make_annotation_text(rec)
    ax.text(
        0.985,
        0.97,
        annotation,
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.9),
    )

    fig.tight_layout()
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ------------------------------------------------------------
# Main runner
# ------------------------------------------------------------
def run(xlsx_path: Path, outdir: Path, sheet_name: str = "Group_Assumptions") -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_group_assumptions(xlsx_path, sheet_name=sheet_name)

    required_cols = {"group", "lifetime", "age distribution type", "loc", "scale", "minimum", "maximum"}
    missing = required_cols.difference(set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns in '{sheet_name}': {sorted(missing)}")

    summary_rows: List[Dict[str, Any]] = []
    pdf_path = outdir / "all_group_distributions.pdf"

    with PdfPages(pdf_path) as pdf:
        for _, row in df.iterrows():
            group = str(row.get("group", "") or "").strip()
            if not group:
                continue

            rec = build_distribution_from_row(row)

            out_png = outdir / f"{sanitize_filename(group)}.png"
            plot_one_distribution(rec, out_png)
            add_distribution_to_pdf(rec, pdf)

            summary_rows.append(
                {
                    "group": group,
                    "distribution_type": rec["distribution_type"],
                    "distribution_name": rec["distribution_name"],
                    "lifetime": rec["lifetime"],
                    "loc": rec["loc"],
                    "scale": rec["scale"],
                    "minimum": rec["minimum"],
                    "maximum": rec["maximum"],
                    "png_file": out_png.name,
                }
            )

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(outdir / "group_distribution_plot_summary.csv", index=False, encoding="utf-8-sig")

    print(f"Done. Wrote plots to: {outdir}")
    print(f"Combined PDF: {pdf_path}")
    print(f"Summary CSV: {outdir / 'group_distribution_plot_summary.csv'}")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=None)
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--sheet", default="Group_Assumptions")

    if "ipykernel_launcher" in Path(sys.argv[0]).name:
        args, _ = ap.parse_known_args()
    else:
        args = ap.parse_args()

    xlsx_path = Path(args.xlsx or r"stock_asset_grouped_temporal_defaults.xlsx")
    outdir = Path(args.outdir or r"lt_data/figs_distributions")

    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    run(xlsx_path=xlsx_path, outdir=outdir, sheet_name=args.sheet)


if __name__ == "__main__":
    main()