#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# conda activate tools_dev
# cd C:\Users\terlouw_t\Documents\Projects\premise_trails\dev\trails
# python 2_stock_asset_dashboard.py --input "stock_asset_review_bw25_with_iedc.csv"

"""
Interactive Dash dashboard for reviewing and diagnosing stock-asset lifetime data.

Purpose
-------
This app helps inspect outputs from the stock-asset review workflow
(e.g. `stock_asset_review_bw25_with_iedc.csv`) by combining:

- filtering by CPC, group, distribution type, verdict, flags, and free text
- outlier screening using IQR-based diagnostics
- summary statistics per CPC
- row-level inspection of reviewed assets
- visual comparison of lifetime and timing parameters
- quick diagnostics for suspicious ecoinvent direct lifetimes
- preview plots of selected temporal distributions

Supported input
---------------
The input file can be:
- CSV
- Excel (.xlsx, .xls, .xlsm)

Typical usage
-------------
Activate the environment and run:

    conda activate tools_dev
    python 2_stock_asset_dashboard.py --input "stock_asset_review_bw25_with_iedc.csv"

Then open in your browser:

    http://127.0.0.1:8050/

Main dashboard features
-----------------------
Overview tab:
- lifetime frequency chart
- preview of selected temporal distributions
- CPC-level summary table
- filtered row table for detailed inspection

Diagnostics tab:
- lifetime vs timing-ratio scatter plot
- comparison of ecoinvent direct lifetime against current, preliminary, and codex values
- CPC ranking by outlier burden / review load
- tables for suspicious direct lifetimes, large direct-vs-codex gaps, and extreme rows

Command-line arguments
----------------------
--input   Path to the CSV or Excel review file (required)
--host    Host address for the local Dash server (default: 127.0.0.1)
--port    Port for the local Dash server (default: 8050)
--debug   Run Dash in debug mode

Requirements:
numpy>=1.24
pandas>=2.0
plotly>=5.18
dash>=2.16
openpyxl>=3.1

Example:
    python 2_stock_asset_dashboard.py --input "stock_asset_review_bw25_with_iedc.csv" --port 8051 --debug
"""
from __future__ import annotations

import argparse
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dash import Dash, dcc, html, dash_table, Input, Output
import plotly.graph_objects as go


TEMPORAL_DISTRIBUTION_TYPES = {
    2: "lognormal",
    3: "normal",
    4: "uniform",
    5: "triangular",
    6: "discrete",
}


# ---------------------------------------------------------------------
# I/O and preprocessing
# ---------------------------------------------------------------------
def read_table(fp: Path) -> pd.DataFrame:
    if not fp.exists():
        raise FileNotFoundError(f"Input file not found: {fp}")
    if fp.suffix.lower() in [".xlsx", ".xls", ".xlsm"]:
        return pd.read_excel(fp)
    return pd.read_csv(fp, encoding="utf-8-sig", low_memory=False)


def normalize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def to_bool_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin(["true", "1", "yes"])


def split_flags(x: Any) -> list[str]:
    s = normalize_text(x)
    if not s:
        return []
    return [part.strip() for part in s.split(";") if part.strip()]


def coalesce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    out = pd.Series(np.nan, index=df.index, dtype=float)
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            out = out.where(out.notna(), s)
    return out


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "needs_check" in df.columns:
        df["needs_check"] = to_bool_series(df["needs_check"])
    else:
        df["needs_check"] = False

    if "codex_realistic" in df.columns:
        df["codex_realistic"] = to_bool_series(df["codex_realistic"])
    else:
        df["codex_realistic"] = False

    text_cols = [
        "name",
        "reference product",
        "CPC",
        "group_name",
        "flags",
        "codex_verdict",
        "ecoinvent_lifetime_status",
        "needs_check_reasons",
        "group_match_basis",
        "preliminary_lifetime_basis",
        "preliminary_distribution_basis",
    ]
    for c in text_cols:
        if c not in df.columns:
            df[c] = ""

    df["current_lifetime_num"] = pd.to_numeric(df["lifetime"], errors="coerce") if "lifetime" in df.columns else np.nan
    df["current_dist_type_num"] = pd.to_numeric(df["age distribution type"], errors="coerce") if "age distribution type" in df.columns else np.nan

    df["ecoinvent_direct_lifetime_num"] = pd.to_numeric(df["ecoinvent_direct_lifetime_years"], errors="coerce") if "ecoinvent_direct_lifetime_years" in df.columns else np.nan
    df["ecoinvent_inferred_lifetime_num"] = pd.to_numeric(df["ecoinvent_inferred_median_years"], errors="coerce") if "ecoinvent_inferred_median_years" in df.columns else np.nan
    df["iedc_lifetime_num"] = pd.to_numeric(df["iedc_lifetime_median_years"], errors="coerce") if "iedc_lifetime_median_years" in df.columns else np.nan
    df["group_lifetime_num"] = pd.to_numeric(df["group_lifetime"], errors="coerce") if "group_lifetime" in df.columns else np.nan
    df["preliminary_lifetime_num"] = pd.to_numeric(df["preliminary_lifetime"], errors="coerce") if "preliminary_lifetime" in df.columns else np.nan
    df["codex_lifetime_num"] = pd.to_numeric(df["codex_suggested_lifetime"], errors="coerce") if "codex_suggested_lifetime" in df.columns else np.nan

    df["display_lifetime"] = coalesce_numeric(df, ["codex_suggested_lifetime", "preliminary_lifetime", "lifetime"])
    df["display_dist_type"] = coalesce_numeric(df, ["codex_suggested_distribution_type", "preliminary_distribution_type", "age distribution type"])
    df["display_loc"] = coalesce_numeric(df, ["codex_suggested_loc", "preliminary_loc", "loc"])
    df["display_scale"] = coalesce_numeric(df, ["codex_suggested_scale", "preliminary_scale", "scale"])
    df["display_minimum"] = coalesce_numeric(df, ["codex_suggested_minimum", "preliminary_minimum", "minimum"])
    df["display_maximum"] = coalesce_numeric(df, ["codex_suggested_maximum", "preliminary_maximum", "maximum"])
    df["match_score_num"] = pd.to_numeric(df["match_score"], errors="coerce") if "match_score" in df.columns else np.nan

    df["display_dist_name"] = df["display_dist_type"].map(TEMPORAL_DISTRIBUTION_TYPES).fillna("unknown")

    df["timing_ratio"] = np.where(
        (df["display_lifetime"] > 0) & np.isfinite(df["display_loc"]),
        np.abs(df["display_loc"]) / df["display_lifetime"],
        np.nan,
    )
    df["maximum_ratio"] = np.where(
        (df["display_lifetime"] > 0) & np.isfinite(df["display_maximum"]),
        np.abs(df["display_maximum"]) / df["display_lifetime"],
        np.nan,
    )

    df["row_id"] = np.arange(len(df), dtype=int)

    all_flags = []
    for _, row in df.iterrows():
        fs = set(split_flags(row.get("flags", "")))

        lt = row.get("display_lifetime")
        tr = row.get("timing_ratio")
        ei_status = normalize_text(row.get("ecoinvent_lifetime_status", "")).upper()
        direct_lt = row.get("ecoinvent_direct_lifetime_num")

        if pd.notna(lt):
            if lt < 2:
                fs.add("VERY_SHORT_LIFETIME")
            if lt > 120:
                fs.add("VERY_LONG_LIFETIME")

        if pd.notna(tr):
            if tr > 0.95:
                fs.add("TIMING_RATIO_GT_0.95")
            if tr < 0.05:
                fs.add("TIMING_RATIO_LT_0.05")

        if ei_status == "SUSPICIOUS":
            fs.add("ECOINVENT_SUSPICIOUS")

        if pd.notna(direct_lt) and direct_lt <= 1.5:
            fs.add("ECOINVENT_DIRECT_LE_1_5Y")

        if pd.notna(direct_lt) and pd.notna(row.get("codex_lifetime_num")):
            if abs(direct_lt - row["codex_lifetime_num"]) >= 5:
                fs.add("DIRECT_VS_CODEX_GAP_GE_5Y")

        a = row.get("display_minimum")
        b = row.get("display_maximum")
        c = row.get("display_loc")
        d = row.get("display_dist_type")
        if d == 5 and pd.notna(a) and pd.notna(b) and pd.notna(c):
            if not (a <= c <= b):
                fs.add("TRI_LOC_OUTSIDE_RANGE")

        all_flags.append(";".join(sorted(fs)))

    df["all_flags"] = all_flags
    return df


def iqr_bounds(s: pd.Series) -> tuple[float, float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) < 4:
        return -np.inf, np.inf
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr


def add_outlier_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for metric in ["display_lifetime", "timing_ratio", "display_loc", "match_score_num"]:
        lo, hi = iqr_bounds(out[metric])
        out[f"{metric}_outlier"] = (out[metric] < lo) | (out[metric] > hi)
    out["any_iqr_outlier"] = out[[c for c in out.columns if c.endswith("_outlier")]].any(axis=1)
    return out


# ---------------------------------------------------------------------
# Filtering and safe table conversion
# ---------------------------------------------------------------------
def filter_df(
    df: pd.DataFrame,
    cpcs: list[str],
    groups: list[str],
    dist_names: list[str],
    only_outliers: bool,
    only_needs_check: bool,
    flag_contains: str,
    verdicts: list[str],
    text_search: str,
) -> pd.DataFrame:
    sub = df.copy()

    if cpcs:
        sub = sub[sub["CPC"].astype(str).isin(cpcs)]
    if groups:
        sub = sub[sub["group_name"].astype(str).isin(groups)]
    if dist_names:
        sub = sub[sub["display_dist_name"].astype(str).isin(dist_names)]
    if only_outliers:
        sub = sub[sub["any_iqr_outlier"]]
    if only_needs_check:
        sub = sub[sub["needs_check"]]
    if verdicts:
        sub = sub[sub["codex_verdict"].astype(str).isin(verdicts)]
    if flag_contains:
        patt = re.escape(normalize_text(flag_contains))
        sub = sub[sub["all_flags"].astype(str).str.lower().str.contains(patt.lower(), na=False)]
    if text_search:
        q = re.escape(normalize_text(text_search))
        hay = (
            sub["name"].astype(str) + " | " +
            sub["reference product"].astype(str) + " | " +
            sub["CPC"].astype(str) + " | " +
            sub["group_name"].astype(str) + " | " +
            sub["all_flags"].astype(str)
        ).str.lower()
        sub = sub[hay.str.contains(q.lower(), na=False)]

    return sub


def safe_sort(df: pd.DataFrame, by: list[str], ascending: list[bool] | bool) -> pd.DataFrame:
    valid_by = [c for c in by if c in df.columns]
    if not valid_by:
        return df.copy()

    if isinstance(ascending, list):
        valid_ascending = [a for c, a in zip(by, ascending) if c in df.columns]
    else:
        valid_ascending = ascending
    return df.sort_values(valid_by, ascending=valid_ascending)


def prepare_table_df(df: pd.DataFrame, round_digits: int = 3) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_bool_dtype(out[col]):
            out[col] = out[col].map(lambda x: "True" if bool(x) else "False")
        elif pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else round(float(x), round_digits))
        else:
            out[col] = out[col].fillna("").astype(str)
    out.columns = [str(c) for c in out.columns]
    return out


def df_to_records_and_columns(df: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    safe_df = prepare_table_df(df)
    return safe_df.to_dict("records"), [{"name": c, "id": c} for c in safe_df.columns]


# ---------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------
def empty_figure(title: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=title,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(size=16, color="#555"),
    )
    fig.update_layout(
        template="plotly_white",
        height=430,
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def fig_lifetime_frequency(sub: pd.DataFrame) -> go.Figure:
    plot_df = sub.dropna(subset=["display_lifetime"]).copy()
    if plot_df.empty:
        return empty_figure("No lifetime data after filtering")

    vals = pd.to_numeric(plot_df["display_lifetime"], errors="coerce").dropna()
    if vals.empty:
        return empty_figure("No lifetime data after filtering")

    counts = vals.round(4).value_counts().sort_index()
    freq_df = pd.DataFrame({"lifetime": counts.index.astype(float), "count": counts.values.astype(int)})

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=freq_df["lifetime"],
            y=freq_df["count"],
            text=freq_df["count"],
            textposition="outside",
            hovertemplate="lifetime=%{x}<br>count=%{y}<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        title="Lifetime frequency",
        xaxis_title="Lifetime [years]",
        yaxis_title="Count",
        height=430,
        margin=dict(l=60, r=20, t=60, b=55),
    )
    return fig


def fig_scatter(sub: pd.DataFrame) -> go.Figure:
    plot_df = sub.dropna(subset=["display_lifetime", "timing_ratio"]).copy()
    if plot_df.empty:
        return empty_figure("No lifetime/timing-ratio pairs after filtering")

    fig = go.Figure()
    for dist_name in sorted(plot_df["display_dist_name"].astype(str).unique()):
        part = plot_df[plot_df["display_dist_name"].astype(str) == dist_name]
        if part.empty:
            continue
        fig.add_trace(
            go.Scattergl(
                x=part["display_lifetime"],
                y=part["timing_ratio"],
                mode="markers",
                name=dist_name,
                text=part["name"],
                customdata=np.stack([
                    part["reference product"].astype(str),
                    part["CPC"].astype(str),
                    part["group_name"].astype(str),
                    part["all_flags"].astype(str),
                ], axis=1),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "ref product=%{customdata[0]}<br>"
                    "CPC=%{customdata[1]}<br>"
                    "group=%{customdata[2]}<br>"
                    "lifetime=%{x}<br>"
                    "timing ratio=%{y:.3f}<br>"
                    "flags=%{customdata[3]}<extra></extra>"
                ),
                marker=dict(size=7, opacity=0.75),
            )
        )

    fig.update_layout(
        template="plotly_white",
        title="Lifetime vs timing ratio",
        xaxis_title="Lifetime [years]",
        yaxis_title="|loc| / lifetime",
        height=430,
        margin=dict(l=60, r=20, t=60, b=55),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    return fig


def triangular_pdf(x: np.ndarray, a: float, c: float, b: float) -> np.ndarray:
    y = np.zeros_like(x, dtype=float)
    if not (np.isfinite(a) and np.isfinite(b) and np.isfinite(c) and a < b):
        return y
    c = min(max(c, a), b)
    left = (x >= a) & (x <= c)
    right = (x >= c) & (x <= b)
    if c > a:
        y[left] = 2.0 * (x[left] - a) / ((b - a) * (c - a))
    if b > c:
        y[right] = 2.0 * (b - x[right]) / ((b - a) * (b - c))
    y[~np.isfinite(y)] = 0.0
    y[y < 0] = 0.0
    return y


def uniform_pdf(x: np.ndarray, a: float, b: float) -> np.ndarray:
    y = np.zeros_like(x, dtype=float)
    if not (np.isfinite(a) and np.isfinite(b) and a < b):
        return y
    mask = (x >= a) & (x <= b)
    y[mask] = 1.0 / (b - a)
    return y


def normal_pdf(x: np.ndarray, mu: float, sigma: float) -> np.ndarray:
    y = np.zeros_like(x, dtype=float)
    if not (np.isfinite(mu) and np.isfinite(sigma) and sigma > 0):
        return y
    return (1.0 / (sigma * math.sqrt(2.0 * math.pi))) * np.exp(-0.5 * ((x - mu) / sigma) ** 2)


def lognormal_pdf_negative_axis(x: np.ndarray, median_negative: float, gsd: float) -> np.ndarray:
    y = np.zeros_like(x, dtype=float)
    if not (np.isfinite(median_negative) and np.isfinite(gsd) and gsd > 1.0):
        return y
    median_age = abs(median_negative)
    if median_age <= 0:
        return y
    sigma_ln = math.log(gsd)
    ages = -x
    mask = ages > 0
    vals = ages[mask]
    y[mask] = (1.0 / (vals * sigma_ln * math.sqrt(2.0 * math.pi))) * np.exp(
        -((np.log(vals) - math.log(median_age)) ** 2) / (2 * sigma_ln ** 2)
    )
    y[~np.isfinite(y)] = 0.0
    y[y < 0] = 0.0
    return y


def fig_distribution_preview(sub: pd.DataFrame) -> go.Figure:
    if sub.empty:
        return empty_figure("No rows selected")

    sub = sub.head(min(20, len(sub)))
    fig = go.Figure()

    for _, row in sub.iterrows():
        lt = row.get("display_lifetime")
        dt = row.get("display_dist_type")
        loc = row.get("display_loc")
        scale = row.get("display_scale")
        a = row.get("display_minimum")
        b = row.get("display_maximum")

        if pd.isna(lt):
            continue

        plot_min = a if pd.notna(a) else -lt
        plot_max = b if pd.notna(b) else -1.0
        if pd.isna(plot_min):
            plot_min = -lt
        if pd.isna(plot_max):
            plot_max = -1.0
        if plot_min >= plot_max:
            plot_min, plot_max = -lt, -1.0

        x = np.linspace(plot_min, plot_max, 300)

        if dt == 2:
            y = lognormal_pdf_negative_axis(x, loc, scale if pd.notna(scale) else 1.8)
        elif dt == 3:
            y = normal_pdf(x, loc if pd.notna(loc) else -0.5 * lt, scale if pd.notna(scale) else max(1.0, 0.2 * lt))
        elif dt == 4:
            y = uniform_pdf(x, a if pd.notna(a) else -lt, b if pd.notna(b) else -1.0)
        elif dt == 5:
            y = triangular_pdf(x, a if pd.notna(a) else -lt, loc if pd.notna(loc) else -0.5 * lt, b if pd.notna(b) else -1.0)
        else:
            continue

        area = np.trapz(y, x)
        if np.isfinite(area) and area > 0:
            y = y / area

        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines",
                name=str(row.get("name", f"row_{int(row['row_id'])}"))[:42],
                opacity=0.75,
                line=dict(width=1.5),
                hovertemplate=(
                    f"row_id={int(row['row_id'])}<br>"
                    f"name={row.get('name','')}<br>"
                    f"CPC={row.get('CPC','')}<br>"
                    f"lifetime={row.get('display_lifetime')}<br>"
                    f"type={row.get('display_dist_name')}<extra></extra>"
                ),
            )
        )

    if len(fig.data) == 0:
        return empty_figure("No supported distributions in selected rows")

    fig.update_layout(
        template="plotly_white",
        title="Distribution preview for selected / top rows",
        xaxis_title="Years before commissioning",
        yaxis_title="Normalized density",
        height=430,
        margin=dict(l=60, r=20, t=60, b=50),
        legend=dict(font=dict(size=10)),
    )
    return fig


def fig_direct_vs_other(sub: pd.DataFrame) -> go.Figure:
    plot_df = sub.dropna(subset=["ecoinvent_direct_lifetime_num"]).copy()
    if plot_df.empty:
        return empty_figure("No ecoinvent direct lifetime rows after filtering")

    fig = go.Figure()

    if plot_df["current_lifetime_num"].notna().any():
        part = plot_df.dropna(subset=["current_lifetime_num"])
        fig.add_trace(go.Scattergl(
            x=part["ecoinvent_direct_lifetime_num"],
            y=part["current_lifetime_num"],
            mode="markers",
            name="Current lifetime",
            text=part["name"],
            hovertemplate="<b>%{text}</b><br>ecoinvent direct=%{x}<br>current=%{y}<extra></extra>"
        ))

    if plot_df["preliminary_lifetime_num"].notna().any():
        part = plot_df.dropna(subset=["preliminary_lifetime_num"])
        fig.add_trace(go.Scattergl(
            x=part["ecoinvent_direct_lifetime_num"],
            y=part["preliminary_lifetime_num"],
            mode="markers",
            name="Preliminary lifetime",
            text=part["name"],
            hovertemplate="<b>%{text}</b><br>ecoinvent direct=%{x}<br>preliminary=%{y}<extra></extra>"
        ))

    if plot_df["codex_lifetime_num"].notna().any():
        part = plot_df.dropna(subset=["codex_lifetime_num"])
        fig.add_trace(go.Scattergl(
            x=part["ecoinvent_direct_lifetime_num"],
            y=part["codex_lifetime_num"],
            mode="markers",
            name="Codex lifetime",
            text=part["name"],
            hovertemplate="<b>%{text}</b><br>ecoinvent direct=%{x}<br>codex=%{y}<extra></extra>"
        ))

    max_val = pd.concat(
        [
            plot_df["ecoinvent_direct_lifetime_num"],
            plot_df["current_lifetime_num"],
            plot_df["preliminary_lifetime_num"],
            plot_df["codex_lifetime_num"],
        ],
        axis=0,
    ).dropna()

    if not max_val.empty:
        m = float(max_val.max()) * 1.05
        fig.add_trace(go.Scatter(x=[0, m], y=[0, m], mode="lines", name="1:1 line"))

    fig.update_layout(
        template="plotly_white",
        title="Ecoinvent direct lifetime vs current/preliminary/codex",
        xaxis_title="Ecoinvent direct lifetime [years]",
        yaxis_title="Compared lifetime [years]",
        height=430,
        margin=dict(l=60, r=20, t=60, b=55),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    return fig


def fig_cpc_outlier_ranking(sub: pd.DataFrame, top_n: int = 20) -> go.Figure:
    if sub.empty:
        return empty_figure("No data for CPC ranking")

    grp = (
        sub.groupby("CPC", dropna=False)
        .agg(
            rows=("row_id", "count"),
            outliers=("any_iqr_outlier", "sum"),
            needs_check=("needs_check", "sum"),
        )
        .reset_index()
    )

    if grp.empty:
        return empty_figure("No CPC ranking data after filtering")

    grp["outlier_share"] = np.where(grp["rows"] > 0, grp["outliers"] / grp["rows"], np.nan)
    grp = grp.sort_values(["outliers", "outlier_share", "rows"], ascending=[False, False, False]).head(top_n)
    grp["CPC_short"] = grp["CPC"].astype(str).str.slice(0, 32)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=grp["CPC_short"], y=grp["outliers"], name="Outliers"))
    fig.add_trace(go.Bar(x=grp["CPC_short"], y=grp["needs_check"], name="Needs check"))

    fig.update_layout(
        template="plotly_white",
        title=f"CPC ranking by outliers / review load (top {len(grp)})",
        xaxis_title="CPC",
        yaxis_title="Count",
        barmode="group",
        height=430,
        margin=dict(l=60, r=20, t=60, b=150),
        xaxis=dict(tickangle=-45, automargin=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    return fig


# ---------------------------------------------------------------------
# Summary / diagnostics
# ---------------------------------------------------------------------
def build_summary_table(sub: pd.DataFrame) -> pd.DataFrame:
    if sub.empty:
        return pd.DataFrame(columns=[
            "CPC", "n", "median_lifetime", "median_timing_ratio", "median_loc", "outliers", "needs_check"
        ])

    return (
        sub.groupby("CPC", dropna=False)
        .agg(
            n=("row_id", "count"),
            median_lifetime=("display_lifetime", "median"),
            median_timing_ratio=("timing_ratio", "median"),
            median_loc=("display_loc", "median"),
            outliers=("any_iqr_outlier", "sum"),
            needs_check=("needs_check", "sum"),
        )
        .reset_index()
        .sort_values(["outliers", "n"], ascending=[False, False])
    )


def diagnostic_rows(sub: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    direct_bad = sub[
        (sub["ecoinvent_direct_lifetime_num"].notna()) &
        (
            (sub["ecoinvent_direct_lifetime_num"] <= 1.5) |
            (sub["ecoinvent_lifetime_status"].astype(str).str.upper() == "SUSPICIOUS")
        )
    ].copy()

    codex_gap = sub[
        sub["ecoinvent_direct_lifetime_num"].notna() &
        sub["codex_lifetime_num"].notna() &
        (np.abs(sub["ecoinvent_direct_lifetime_num"] - sub["codex_lifetime_num"]) >= 5)
    ].copy()

    extreme = sub[
        (
            (sub["display_lifetime"].notna()) &
            ((sub["display_lifetime"] <= 2) | (sub["display_lifetime"] >= 100))
        ) | sub["needs_check"]
    ].copy()

    return direct_bad, codex_gap, extreme


def make_card(title: str, value: str) -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "12px", "color": "#555", "marginBottom": "4px"}),
            html.Div(value, style={"fontSize": "22px", "fontWeight": "700"}),
        ],
        style={
            "border": "1px solid #ddd",
            "padding": "10px 14px",
            "borderRadius": "10px",
            "minWidth": "130px",
            "backgroundColor": "white",
            "boxShadow": "0 1px 4px rgba(0,0,0,0.04)",
        },
    )


# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------
def make_app(df: pd.DataFrame) -> Dash:
    df = add_outlier_columns(df)

    cpc_options = [{"label": x, "value": x} for x in sorted(df["CPC"].astype(str).dropna().unique())]
    group_options = [{"label": x, "value": x} for x in sorted(df["group_name"].astype(str).dropna().unique()) if x]
    dist_options = [{"label": x, "value": x} for x in sorted(df["display_dist_name"].astype(str).dropna().unique())]
    verdict_options = [{"label": x, "value": x} for x in sorted(df["codex_verdict"].astype(str).dropna().unique()) if x]

    app = Dash(__name__)
    app.title = "Stock Asset Dashboard"

    panel_style = {
        "border": "1px solid #e5e7eb",
        "borderRadius": "12px",
        "padding": "6px",
        "backgroundColor": "white",
        "boxShadow": "0 1px 4px rgba(0,0,0,0.05)",
    }

    table_style = {
        "overflowX": "auto",
        "minHeight": "220px",
        "maxHeight": "550px",
        "overflowY": "auto",
        "border": "1px solid #ddd",
        "borderRadius": "8px",
        "backgroundColor": "white",
    }

    app.layout = html.Div(
        style={"fontFamily": "Arial, sans-serif", "padding": "14px", "backgroundColor": "#f8fafc"},
        children=[
            html.H2("Stock asset dashboard", style={"marginBottom": "10px"}),

            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "1fr 1fr 1fr",
                    "gap": "10px",
                    "marginBottom": "10px",
                },
                children=[
                    html.Div([html.Label("CPC"), dcc.Dropdown(id="cpc-filter", options=cpc_options, multi=True)]),
                    html.Div([html.Label("Group"), dcc.Dropdown(id="group-filter", options=group_options, multi=True)]),
                    html.Div([html.Label("Distribution type"), dcc.Dropdown(id="dist-filter", options=dist_options, multi=True)]),
                    html.Div([html.Label("Codex verdict"), dcc.Dropdown(id="verdict-filter", options=verdict_options, multi=True)]),
                    html.Div([html.Label("Flag contains"), dcc.Input(id="flag-filter", type="text", style={"width": "100%"}, placeholder="e.g. suspicious")]),
                    html.Div([html.Label("Text search"), dcc.Input(id="text-search", type="text", style={"width": "100%"}, placeholder="Search name / product / CPC")]),
                ],
            ),

            dcc.Checklist(
                id="toggle-filter",
                options=[
                    {"label": "Only IQR outliers", "value": "outliers"},
                    {"label": "Only needs_check", "value": "needs_check"},
                ],
                value=[],
                inline=True,
                style={"marginBottom": "12px"},
            ),

            html.Div(
                id="summary-cards",
                style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "14px"},
            ),

            dcc.Tabs(
                value="overview",
                children=[
                    dcc.Tab(
                        label="Overview",
                        children=[
                            html.Br(),
                            html.Div(
                                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "14px"},
                                children=[
                                    html.Div(dcc.Graph(id="fig-lifetime", config={"responsive": True}), style=panel_style),
                                    html.Div(dcc.Graph(id="fig-preview", config={"responsive": True}), style=panel_style),
                                ],
                            ),
                            html.Br(),
                            html.H4("CPC summary"),
                            dash_table.DataTable(
                                id="summary-table",
                                page_size=12,
                                sort_action="native",
                                filter_action="native",
                                style_table=table_style,
                                style_cell={
                                    "textAlign": "left",
                                    "fontSize": 12,
                                    "padding": "6px",
                                    "whiteSpace": "normal",
                                    "height": "auto",
                                    "minWidth": "100px",
                                    "maxWidth": "260px",
                                },
                                style_header={"fontWeight": "bold", "backgroundColor": "#f3f4f6"},
                            ),
                            html.Br(),
                            html.H4("Filtered rows"),
                            dash_table.DataTable(
                                id="rows-table",
                                page_size=20,
                                row_selectable="multi",
                                selected_rows=[],
                                sort_action="native",
                                filter_action="native",
                                style_table={**table_style, "minHeight": "320px", "maxHeight": "800px"},
                                style_cell={
                                    "textAlign": "left",
                                    "fontSize": 12,
                                    "padding": "6px",
                                    "whiteSpace": "normal",
                                    "height": "auto",
                                    "minWidth": "120px",
                                    "maxWidth": "340px",
                                },
                                style_header={"fontWeight": "bold", "backgroundColor": "#f3f4f6"},
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Diagnostics",
                        children=[
                            html.Br(),
                            html.Div(
                                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "14px"},
                                children=[
                                    html.Div(dcc.Graph(id="fig-scatter", config={"responsive": True}), style=panel_style),
                                    html.Div(dcc.Graph(id="fig-direct-compare", config={"responsive": True}), style=panel_style),
                                    html.Div(dcc.Graph(id="fig-cpc-ranking", config={"responsive": True}), style=panel_style),
                                ],
                            ),
                            html.Br(),
                            html.H4("Suspicious ecoinvent direct lifetime rows"),
                            dash_table.DataTable(
                                id="diag-direct-table",
                                page_size=10,
                                sort_action="native",
                                filter_action="native",
                                style_table=table_style,
                                style_cell={"textAlign": "left", "fontSize": 12, "padding": "6px", "whiteSpace": "normal", "height": "auto", "maxWidth": "300px"},
                                style_header={"fontWeight": "bold", "backgroundColor": "#f3f4f6"},
                            ),
                            html.Br(),
                            html.H4("Large direct vs codex lifetime gaps"),
                            dash_table.DataTable(
                                id="diag-gap-table",
                                page_size=10,
                                sort_action="native",
                                filter_action="native",
                                style_table=table_style,
                                style_cell={"textAlign": "left", "fontSize": 12, "padding": "6px", "whiteSpace": "normal", "height": "auto", "maxWidth": "300px"},
                                style_header={"fontWeight": "bold", "backgroundColor": "#f3f4f6"},
                            ),
                            html.Br(),
                            html.H4("Extreme / review-priority rows"),
                            dash_table.DataTable(
                                id="diag-extreme-table",
                                page_size=12,
                                sort_action="native",
                                filter_action="native",
                                style_table=table_style,
                                style_cell={"textAlign": "left", "fontSize": 12, "padding": "6px", "whiteSpace": "normal", "height": "auto", "maxWidth": "300px"},
                                style_header={"fontWeight": "bold", "backgroundColor": "#f3f4f6"},
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    @app.callback(
        Output("summary-cards", "children"),
        Output("fig-lifetime", "figure"),
        Output("summary-table", "data"),
        Output("summary-table", "columns"),
        Output("rows-table", "data"),
        Output("rows-table", "columns"),
        Output("fig-scatter", "figure"),
        Output("fig-direct-compare", "figure"),
        Output("fig-cpc-ranking", "figure"),
        Output("diag-direct-table", "data"),
        Output("diag-direct-table", "columns"),
        Output("diag-gap-table", "data"),
        Output("diag-gap-table", "columns"),
        Output("diag-extreme-table", "data"),
        Output("diag-extreme-table", "columns"),
        Input("cpc-filter", "value"),
        Input("group-filter", "value"),
        Input("dist-filter", "value"),
        Input("toggle-filter", "value"),
        Input("flag-filter", "value"),
        Input("verdict-filter", "value"),
        Input("text-search", "value"),
    )
    def update_main(cpcs, groups, dist_names, toggles, flag_contains, verdicts, text_search):
        sub = filter_df(
            df=df,
            cpcs=cpcs or [],
            groups=groups or [],
            dist_names=dist_names or [],
            only_outliers="outliers" in (toggles or []),
            only_needs_check="needs_check" in (toggles or []),
            flag_contains=flag_contains or "",
            verdicts=verdicts or [],
            text_search=text_search or "",
        )

        suspicious_direct_n = int(
            (
                sub["ecoinvent_direct_lifetime_num"].notna() &
                (
                    (sub["ecoinvent_direct_lifetime_num"] <= 1.5) |
                    (sub["ecoinvent_lifetime_status"].astype(str).str.upper() == "SUSPICIOUS")
                )
            ).sum()
        )

        cards = [
            make_card("Rows", f"{len(sub):,}"),
            make_card("CPCs", f"{sub['CPC'].nunique():,}"),
            make_card("Needs check", f"{int(sub['needs_check'].sum()):,}"),
            make_card("IQR outliers", f"{int(sub['any_iqr_outlier'].sum()):,}"),
            make_card("Suspicious direct", f"{suspicious_direct_n:,}"),
        ]

        summary = build_summary_table(sub)
        summary_data, summary_cols = df_to_records_and_columns(summary)

        row_cols = [
            "row_id",
            "name",
            "reference product",
            "CPC",
            "group_name",
            "current_lifetime_num",
            "preliminary_lifetime_num",
            "codex_lifetime_num",
            "display_lifetime",
            "display_dist_name",
            "display_loc",
            "display_minimum",
            "display_maximum",
            "timing_ratio",
            "match_score_num",
            "needs_check",
            "all_flags",
            "codex_verdict",
            "ecoinvent_lifetime_status",
        ]
        row_cols = [c for c in row_cols if c in sub.columns]

        # FIX: sort on the full dataframe first, then select visible columns
        rows_sorted = safe_sort(
            sub.copy(),
            by=["any_iqr_outlier", "needs_check", "row_id"],
            ascending=[False, False, True],
        )
        rows_view = rows_sorted[row_cols].copy()
        rows_data, rows_cols = df_to_records_and_columns(rows_view)

        direct_bad, codex_gap, extreme = diagnostic_rows(sub)

        direct_cols = [c for c in [
            "row_id", "name", "reference product", "CPC", "group_name",
            "ecoinvent_direct_lifetime_num", "ecoinvent_lifetime_status",
            "current_lifetime_num", "preliminary_lifetime_num", "codex_lifetime_num",
            "all_flags", "needs_check_reasons",
        ] if c in direct_bad.columns]

        gap_cols = [c for c in [
            "row_id", "name", "reference product", "CPC", "group_name",
            "ecoinvent_direct_lifetime_num", "codex_lifetime_num",
            "current_lifetime_num", "preliminary_lifetime_num",
            "codex_verdict", "all_flags",
        ] if c in codex_gap.columns]

        extreme_cols = [c for c in [
            "row_id", "name", "reference product", "CPC", "group_name",
            "display_lifetime", "display_dist_name", "timing_ratio",
            "needs_check", "codex_verdict", "all_flags", "needs_check_reasons",
        ] if c in extreme.columns]

        direct_data, direct_cols_out = df_to_records_and_columns(direct_bad[direct_cols].copy())
        gap_data, gap_cols_out = df_to_records_and_columns(codex_gap[gap_cols].copy())
        extreme_data, extreme_cols_out = df_to_records_and_columns(extreme[extreme_cols].copy())

        return (
            cards,
            fig_lifetime_frequency(sub),
            summary_data,
            summary_cols,
            rows_data,
            rows_cols,
            fig_scatter(sub),
            fig_direct_vs_other(sub),
            fig_cpc_outlier_ranking(sub),
            direct_data,
            direct_cols_out,
            gap_data,
            gap_cols_out,
            extreme_data,
            extreme_cols_out,
        )

    @app.callback(
        Output("fig-preview", "figure"),
        Input("rows-table", "derived_virtual_data"),
        Input("rows-table", "derived_virtual_selected_rows"),
        Input("cpc-filter", "value"),
        Input("group-filter", "value"),
        Input("dist-filter", "value"),
        Input("toggle-filter", "value"),
        Input("flag-filter", "value"),
        Input("verdict-filter", "value"),
        Input("text-search", "value"),
    )
    def update_preview(table_data, selected_rows, cpcs, groups, dist_names, toggles, flag_contains, verdicts, text_search):
        sub = filter_df(
            df=df,
            cpcs=cpcs or [],
            groups=groups or [],
            dist_names=dist_names or [],
            only_outliers="outliers" in (toggles or []),
            only_needs_check="needs_check" in (toggles or []),
            flag_contains=flag_contains or "",
            verdicts=verdicts or [],
            text_search=text_search or "",
        )

        if table_data is not None and len(table_data) > 0:
            table_df = pd.DataFrame(table_data)
            if "row_id" in table_df.columns:
                table_df["row_id"] = pd.to_numeric(table_df["row_id"], errors="coerce")
        else:
            table_df = sub.copy()

        if selected_rows:
            preview_df = table_df.iloc[selected_rows].copy()
            if "row_id" in preview_df.columns:
                row_ids = pd.to_numeric(preview_df["row_id"], errors="coerce").dropna().astype(int).tolist()
                preview_df = sub[sub["row_id"].isin(row_ids)].copy()
            else:
                preview_df = sub.head(12).copy()
        else:
            preview_df = safe_sort(
                sub.copy(),
                by=["any_iqr_outlier", "needs_check", "row_id"],
                ascending=[False, False, True],
            ).head(12).copy()

        return fig_distribution_preview(preview_df)

    return app


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to stock_asset_review_bw25_with_iedc CSV or Excel file.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8050)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    fp = Path(args.input)
    df = prepare_dataframe(read_table(fp))
    app = make_app(df)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()