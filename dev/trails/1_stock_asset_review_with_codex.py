#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# conda activate trails
# cd C:\Users\terlouw_t\Documents\Projects\premise_trails\dev\trails
# Codex CLI first, then API fallback:
#   python 1_stock_asset_review_with_codex.py --use-codex --prefer-codex-cli --codex-review-scope all --codex-batch-size 20 --lt-data-dir "C:\Users\terlouw_t\Documents\Projects\premise_trails\dev\trails\lt_data"
# Before API use in PowerShell:
# $env:OPENAI_API_KEY=""

"""
TRAILS stock-asset lifetime + temporal-distribution review
(Brightway 2.5 / bw2data) with:
- ecoinvent-first evidence
- local IEDC lifetime Excel benchmark files from lt_data
- group-based preliminary temporal-distribution defaults from an external Excel file
- optional Codex CLI validation
- optional OpenAI API validation with automatic model fallback

STEP-BY-STEP WHAT THIS SCRIPT DOES
1) Loads the TRAILS temporal_distributions.csv file (or xlsx).
2) Keeps only rows with the selected temporal tag (default: stock_asset).
3) Loads the ecoinvent database from the configured Brightway project.
4) Matches each TRAILS row to the best ecoinvent activity using:
   - activity name
   - reference product
5) Tries to extract a direct lifetime from the matched ecoinvent activity text.
6) If no direct lifetime is found, infers a plausible lifetime from similar
   ecoinvent neighbors that do have strong lifetime evidence.
7) Loads all local IEDC lifetime Excel files from lt_data\\ and extracts plausible
   lifetime values as a second benchmark layer.
8) Loads grouped temporal-distribution defaults from an external Excel workbook,
   maps each stock asset to a group, and creates a pre-Codex preliminary
   parameter set in separate columns.
9) Compares the TRAILS lifetime against:
   - ecoinvent evidence first (binding / first benchmark)
   - local IEDC benchmark files second
   - group-default lifetime third
10) Checks whether the chosen temporal-emission distribution parameters are internally coherent.
11) Marks rows that likely need extra review.
12) Optionally sends rows to a model for an additional AI review:
    - either only flagged rows
    - or every row
    - optionally using Codex CLI first
    - with optional OpenAI API fallback across multiple models
    - model sees ecoinvent, IEDC, and group-preliminary parameters
13) Writes the final review table to CSV.

Requirements:
  pip install bw2data rapidfuzz numpy pandas openpyxl

Optional for OpenAI API validation:
  pip install openai
  set OPENAI_API_KEY

Optional for Codex CLI validation:
  - Install Codex CLI separately
  - Make sure the executable is on PATH, or pass --codex-exe
  - Be signed in locally before running with --prefer-codex-cli
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process


# ----------------------------
# CONFIG (your BW2.5 inputs)
# ----------------------------
BW_PROJECT = "ecoinvent-3.12-cutoff"
EI_DB_NAME = "ecoinvent_312_reference"

DEFAULT_CSV = r"temporal_distributions.csv"
DEFAULT_OUT = r"stock_asset_review_bw25_with_iedc.csv"
DEFAULT_LT_DATA_DIR = r"/lt_data/"
DEFAULT_GROUP_DEFAULTS_XLSX = r"stock_asset_grouped_temporal_defaults.xlsx"

IEDC_BAD_SHEET_NAMES = {
    "dataset description",
    "description",
    "metadata",
    "readme",
    "cover",
    "summary",
}

IEDC_MIN_MATCH_SCORE = 80.0
IEDC_TOP_K = 12
IEDC_MIN_SHARED_KEYWORDS = 1

DEFAULT_CODEX_MODEL = "gpt-5.4"
DEFAULT_API_MODELS = ["gpt-5.4", "gpt-5-mini"]
DEFAULT_API_TIMEOUT = 180

STRICT_SUGGESTION_DISTRIBUTION_TYPES = {2, 3, 4, 5}
DEFAULT_ENABLE_WEB_REVIEW = True
DEFAULT_WEB_SEARCH_CONTEXT = "medium"

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None


# ----------------------------
# age distribution definitions
# ----------------------------
TEMPORAL_DISTRIBUTION_TYPES: Dict[int, Dict[str, str]] = {
    2: {
        "distribution": "lognormal",
        "description": "Uses loc and scale.",
        "interpretation": "Skewed emission profile over the asset lifetime; often front-loaded, with most emissions occurring earlier and a smaller tail later.",
    },
    3: {
        "distribution": "normal",
        "description": "Uses loc and scale.",
        "interpretation": "Symmetric emission profile over time, with emissions centered around the mean timing.",
    },
    4: {
        "distribution": "uniform",
        "description": "Uses minimum and maximum.",
        "interpretation": "Emissions are spread evenly over the specified time interval.",
    },
    5: {
        "distribution": "triangular",
        "description": "Uses loc, minimum, and maximum.",
        "interpretation": "Emissions are concentrated around the mode (loc) within the specified time interval.",
    },
    6: {
        "distribution": "discrete",
        "description": "Uses weights and offsets columns.",
        "interpretation": "Emissions occur at explicit time offsets rather than following one continuous distribution.",
    },
}


# ----------------------------
# Dataclasses
# ----------------------------
@dataclass(frozen=True)
class ActRec:
    key: Tuple[str, str]
    name: str
    ref_product: str
    location: str
    unit: str
    category_path: str
    comment: str


@dataclass
class IEDCLifetimeRec:
    source_file: str
    sheet_name: str
    row_number: int
    label_text: str
    lifetime_years: float
    raw_row_json: str
    asset_class: str
    geography: Optional[str] = None
    commodity: Optional[str] = None


@dataclass(frozen=True)
class GroupDefaultRec:
    group: str
    lifetime: Optional[float]
    age_distribution_type: Optional[int]
    age_distribution_description: str
    loc: Optional[float]
    scale: Optional[float]
    minimum: Optional[float]
    maximum: Optional[float]
    param_confidence: str
    basis_type: str
    confidence: str
    source_short: str
    source_url: str
    notes: str


@dataclass(frozen=True)
class GroupKeywordRec:
    keyword: str
    group: str
    priority: int


# ----------------------------
# Helpers
# ----------------------------
STOPWORDS_FOR_MATCH = {
    "production", "market", "for", "from", "and", "of", "in", "on", "at",
    "unit", "row", "data", "dataset", "description", "product", "commodity",
    "technology", "technologies", "system", "systems", "manufacture",
    "equipment", "machinery", "apparatus", "supply", "components", "component",
    "installed", "replaceable", "stock", "asset"
}

ASSET_CLASS_KEYWORDS: Dict[str, List[str]] = {
    "filter": ["filter", "air filter", "purifying", "filtration"],
    "uv_lamp": ["uv", "ultraviolet", "lamp", "uv lamp", "disinfection"],
    "building": ["building", "buildings", "residential", "commercial"],
    "vehicle": ["vehicle", "vehicles", "car", "cars", "truck", "trucks", "bus", "buses"],
    "machinery": ["machinery", "machine", "machines", "industrial machine"],
    "electricity_technology": ["electricity", "power plant", "pv", "photovoltaic", "wind", "grid", "nuclear", "hydro", "transformer", "substation"],
    "sand_gravel": ["sand", "gravel", "aggregate", "mineral"],
    "pipe": ["pipe", "pipes", "piping", "pipeline", "canal", "waterway"],
    "generic": [],
}

ASSET_CLASS_TO_GROUP_CANDIDATES: Dict[str, List[str]] = {
    "building": [
        "buildings",
        "roads_pavements",
        "rail_infrastructure",
        "bridges_tunnels_civil_structures",
    ],
    "vehicle": [
        "vehicles_cars_vans_buses",
        "vehicles_trucks_heavy_duty",
        "ships_vessels",
        "aircraft",
    ],
    "machinery": [
        "compressed_air_equipment_small",
        "pumps_small_medium",
        "electrical_cabinet_control_equipment",
        "industrial_machinery_plant",
        "water_wastewater_treatment_equipment",
        "hvac_systems",
    ],
    "electricity_technology": [
        "charging_infrastructure_equipment",
        "electrical_cabinet_control_equipment",
        "electricity_grids_substations",
        "power_plants_general",
        "wind_farms_turbines",
        "pv_systems",
    ],
    "pipe": [
        "pipelines_buried_pipe_networks",
        "waterways_canals_harbours",
    ],
    "filter": [
        "filters_replaceable",
        "hvac_systems",
    ],
    "uv_lamp": [
        "uv_lamps_equipment",
        "water_wastewater_treatment_equipment",
    ],
    "sand_gravel": [
        "roads_pavements",
        "bridges_tunnels_civil_structures",
        "buildings",
    ],
    "generic": [
        "industrial_machinery_plant",
        "electrical_cabinet_control_equipment",
        "buildings",
    ],
}


def sanitize_text_for_csv(x: Any) -> Any:
    if x is None:
        return None

    if isinstance(x, (int, float, bool, np.integer, np.floating)):
        if isinstance(x, float) and np.isnan(x):
            return None
        return x

    s = str(x)
    s = s.replace("\r\n", " | ").replace("\n", " | ").replace("\r", " | ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_iedc_commodity_for_matching(x: Any) -> str:
    s = normalize_text_for_match(x)

    replacements = {
        "passenger car": "private car",
        "passenger cars": "private car",
        "car": "private car",
        "cars": "private car",
        "lorry": "truck",
        "truck": "truck",
        "trucks": "truck",
        "bus": "bus",
        "buses": "bus",
        "motorcycle": "motorcycle",
        "motorcycles": "motorcycle",
        "bicycle": "bicycle",
        "bicycles": "bicycle",
        "van": "light commercial vehicle",
        "vans": "light commercial vehicle",
    }

    for k, v in replacements.items():
        if k in s:
            return v

    return s


def to_jsonable(x: Any) -> Any:
    """
    Recursively convert pandas/numpy/scalar objects to plain Python JSON-safe types.
    """
    if x is None:
        return None

    if isinstance(x, (np.integer,)):
        return int(x)

    if isinstance(x, (np.floating,)):
        if np.isnan(x):
            return None
        return float(x)

    if isinstance(x, (np.bool_,)):
        return bool(x)

    if isinstance(x, float):
        if np.isnan(x):
            return None
        return x

    if isinstance(x, tuple):
        return [to_jsonable(v) for v in x]

    if isinstance(x, list):
        return [to_jsonable(v) for v in x]

    if isinstance(x, dict):
        return {str(k): to_jsonable(v) for k, v in x.items()}

    return x


def sanitize_dataframe_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].map(sanitize_text_for_csv)
    return out


def tokenize_for_match(text: Any) -> List[str]:
    s = normalize_text_for_match(text)
    toks = re.findall(r"[a-z0-9\-\+\.]+", s)
    return [t for t in toks if len(t) >= 3 and t not in STOPWORDS_FOR_MATCH]


def detect_asset_class(text: Any) -> str:
    s = normalize_text_for_match(text)

    if any(k in s for k in ASSET_CLASS_KEYWORDS["filter"]):
        return "filter"
    if any(k in s for k in ASSET_CLASS_KEYWORDS["uv_lamp"]):
        return "uv_lamp"
    if any(k in s for k in ASSET_CLASS_KEYWORDS["building"]):
        return "building"
    if any(k in s for k in ASSET_CLASS_KEYWORDS["vehicle"]):
        return "vehicle"
    if any(k in s for k in ASSET_CLASS_KEYWORDS["machinery"]):
        return "machinery"
    if any(k in s for k in ASSET_CLASS_KEYWORDS["electricity_technology"]):
        return "electricity_technology"
    if any(k in s for k in ASSET_CLASS_KEYWORDS["sand_gravel"]):
        return "sand_gravel"
    if any(k in s for k in ASSET_CLASS_KEYWORDS["pipe"]):
        return "pipe"
    return "generic"


def row_has_meaningful_description(row_dict: Dict[str, Any]) -> bool:
    txt = row_to_searchable_text(row_dict)
    toks = tokenize_for_match(txt)
    return len(toks) >= 2


def should_skip_iedc_sheet(sheet_name: str) -> bool:
    s = normalize_text_for_match(sheet_name)
    return s in IEDC_BAD_SHEET_NAMES


def shared_keywords_count(q: str, target: str) -> int:
    qset = set(tokenize_for_match(q))
    tset = set(tokenize_for_match(target))
    return len(qset.intersection(tset))


def asset_classes_compatible(query_class: str, rec_class: str) -> bool:
    if query_class == "generic" or rec_class == "generic":
        return False
    return query_class == rec_class


def source_file_asset_class(source_file: str) -> str:
    return detect_asset_class(Path(source_file).stem)


def row_asset_class_from_context(row: pd.Series) -> str:
    txt = " | ".join([
        str(row.get("name", "") or ""),
        str(row.get("reference product", "") or ""),
        str(row.get("CPC", "") or ""),
    ])
    return detect_asset_class(txt)


def safe_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, float) and np.isnan(x):
            return None
        return float(x)
    except Exception:
        return None


def clamp_positive(x: Optional[float], fallback: float) -> float:
    if x is None or not np.isfinite(x) or x <= 0:
        return float(fallback)
    return float(x)


def fmt_time(sec: float) -> str:
    sec = max(0.0, float(sec))
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:d}h{m:02d}m{s:02d}s"
    if m:
        return f"{m:d}m{s:02d}s"
    return f"{s:d}s"


def to_float(x: Any) -> Optional[float]:
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


def temporal_distribution_type_full_text(dist_type: Optional[int]) -> str:
    if dist_type is None:
        return ""
    info = TEMPORAL_DISTRIBUTION_TYPES.get(dist_type)
    if not info:
        return f"{dist_type}: Unknown temporal distribution type"
    return (
        f"{dist_type}: {info['distribution']} | "
        f"{info['description']} | {info['interpretation']}"
    )


def _norm(s: Any) -> str:
    if not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", s).strip()


def _lower(s: Any) -> str:
    return _norm(s).lower()


def normalize_text_for_match(x: Any) -> str:
    s = _lower(x)
    s = re.sub(r"[_/|]+", " ", s)
    s = re.sub(r"[^a-z0-9\-\+\.\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def row_to_searchable_text(row_dict: Dict[str, Any]) -> str:
    vals: List[str] = []
    for _, v in row_dict.items():
        if v is None:
            continue
        if isinstance(v, float) and np.isnan(v):
            continue
        s = str(v).strip()
        if s:
            vals.append(s)
    return normalize_text_for_match(" | ".join(vals))


def normalize_group_name(x: Any) -> str:
    return normalize_text_for_match(x)


# ----------------------------
# Group defaults loading and mapping
# ----------------------------
def load_group_defaults(group_defaults_xlsx: str) -> Tuple[Dict[str, GroupDefaultRec], List[GroupKeywordRec]]:
    fp = Path(group_defaults_xlsx)
    if not fp.exists():
        return {}, []

    try:
        group_df = pd.read_excel(fp, sheet_name="Group_Assumptions")
    except Exception:
        group_df = pd.DataFrame()

    try:
        kw_df = pd.read_excel(fp, sheet_name="Keyword_Mapping")
    except Exception:
        kw_df = pd.DataFrame()

    group_defaults: Dict[str, GroupDefaultRec] = {}

    if not group_df.empty:
        for _, row in group_df.iterrows():
            group = normalize_group_name(row.get("group"))
            if not group:
                continue

            dist_type_val = safe_float_or_none(row.get("age distribution type"))
            dist_type = int(dist_type_val) if dist_type_val is not None else None

            group_defaults[group] = GroupDefaultRec(
                group=group,
                lifetime=safe_float_or_none(row.get("lifetime")),
                age_distribution_type=dist_type,
                age_distribution_description=str(row.get("age distribution description", "") or ""),
                loc=safe_float_or_none(row.get("loc")),
                scale=safe_float_or_none(row.get("scale")),
                minimum=safe_float_or_none(row.get("minimum")),
                maximum=safe_float_or_none(row.get("maximum")),
                param_confidence=str(row.get("param_confidence", "") or ""),
                basis_type=str(row.get("basis_type", "") or ""),
                confidence=str(row.get("confidence", "") or ""),
                source_short=str(row.get("source_short", "") or ""),
                source_url=str(row.get("source_url", "") or ""),
                notes=str(row.get("notes", "") or ""),
            )

    keyword_recs: List[GroupKeywordRec] = []

    if not kw_df.empty:
        for _, row in kw_df.iterrows():
            kw = normalize_text_for_match(row.get("keyword", ""))
            grp = normalize_group_name(row.get("group", ""))
            if not kw or not grp:
                continue

            prio = safe_float_or_none(row.get("priority"))
            keyword_recs.append(
                GroupKeywordRec(
                    keyword=kw,
                    group=grp,
                    priority=int(prio) if prio is not None else 1,
                )
            )

    keyword_recs = sorted(keyword_recs, key=lambda x: (-x.priority, -len(x.keyword)))
    return group_defaults, keyword_recs


def detect_group_for_row(
    row: pd.Series,
    group_defaults: Dict[str, GroupDefaultRec],
    keyword_recs: List[GroupKeywordRec],
) -> Tuple[Optional[str], str]:
    fields = [
        str(row.get("name", "") or ""),
        str(row.get("reference product", "") or ""),
        str(row.get("CPC", "") or ""),
        str(row.get("ISIC rev.4 ecoinvent", "") or ""),
        str(row.get("EcoSpold01Categories", "") or ""),
        str(row.get("tag_notes", "") or ""),
        str(row.get("param_notes", "") or ""),
    ]
    haystack = " | ".join(fields)
    haystack_norm = normalize_text_for_match(haystack)
    tokens = set(tokenize_for_match(haystack_norm))

    charger_patterns = [
        r"\bev charger\b",
        r"\bcharger\b",
        r"\bcharging station\b",
        r"\bfast charger\b",
        r"\blevel 3\b",
        r"\bpantograph\b",
        r"\bplugin\b",
        r"\bplug in\b",
    ]
    if any(re.search(p, haystack_norm, re.IGNORECASE) for p in charger_patterns):
        for candidate_group in [
            "industrial_machinery_plant",
            "electricity_grids_substations",
            "power_plants_general",
        ]:
            if candidate_group in group_defaults:
                return candidate_group, "HARD_RULE:charger_equipment"

    hvac_patterns = [
        r"\bhvac\b",
        r"\bheat pump\b",
        r"\bair conditioner\b",
        r"\bventilation\b",
        r"\bboiler\b",
        r"\bchiller\b",
    ]
    if any(re.search(p, haystack_norm, re.IGNORECASE) for p in hvac_patterns):
        if "hvac_systems" in group_defaults:
            return "hvac_systems", "HARD_RULE:hvac"

    filter_patterns = [
        r"\bfilter\b",
        r"\bair filter\b",
        r"\bfiltration\b",
    ]
    if any(re.search(p, haystack_norm, re.IGNORECASE) for p in filter_patterns):
        if "filters_replaceable" in group_defaults:
            return "filters_replaceable", "HARD_RULE:filter"

    uv_patterns = [
        r"\buv\b",
        r"\bultraviolet\b",
        r"\buv lamp\b",
        r"\blamp\b",
    ]
    if any(re.search(p, haystack_norm, re.IGNORECASE) for p in uv_patterns):
        if "uv_lamps_equipment" in group_defaults:
            return "uv_lamps_equipment", "HARD_RULE:uv"

    pv_patterns = [
        r"\bpv\b",
        r"\bphotovoltaic\b",
        r"\bsolar panel\b",
        r"\bsolar module\b",
        r"\binverter\b",
    ]
    if any(re.search(p, haystack_norm, re.IGNORECASE) for p in pv_patterns):
        if "pv_systems" in group_defaults:
            return "pv_systems", "HARD_RULE:pv"

    wind_patterns = [
        r"\bwind\b",
        r"\bwind turbine\b",
        r"\bwind farm\b",
    ]
    if any(re.search(p, haystack_norm, re.IGNORECASE) for p in wind_patterns):
        if "wind_farms_turbines" in group_defaults:
            return "wind_farms_turbines", "HARD_RULE:wind"

    blocked_loose_keywords = {
        "port",
        "plant",
        "station",
        "grid",
        "network",
        "system",
        "unit",
        "construction",
        "infrastructure",
    }

    keyword_hits: List[Tuple[int, int, str, str]] = []

    for kw_rec in keyword_recs:
        if not kw_rec.keyword:
            continue
        if kw_rec.group not in group_defaults:
            continue

        kw = normalize_text_for_match(kw_rec.keyword)
        if not kw:
            continue

        if " " not in kw:
            matched = kw in tokens
        else:
            matched = re.search(rf"(?<![a-z0-9]){re.escape(kw)}(?![a-z0-9])", haystack_norm) is not None

        if kw in blocked_loose_keywords and " " not in kw:
            matched = kw in tokens

        if matched:
            keyword_hits.append((kw_rec.priority, len(kw), kw_rec.group, kw))

    if keyword_hits:
        keyword_hits.sort(key=lambda x: (-x[0], -x[1], x[2], x[3]))
        best = keyword_hits[0]
        return best[2], f"KEYWORD:{best[3]}"

    asset_class = row_asset_class_from_context(row)
    for candidate_group in ASSET_CLASS_TO_GROUP_CANDIDATES.get(asset_class, []):
        if candidate_group in group_defaults:
            return candidate_group, f"ASSET_CLASS:{asset_class}"

    for candidate_group in ASSET_CLASS_TO_GROUP_CANDIDATES.get("generic", []):
        if candidate_group in group_defaults:
            return candidate_group, "GENERIC_FALLBACK"

    return None, "NO_GROUP_MATCH"


def choose_preliminary_lifetime(
    current_lifetime: Optional[float],
    ecoinvent_direct: Optional[float],
    ecoinvent_inferred: Optional[float],
    iedc_median: Optional[float],
    group_lifetime: Optional[float],
) -> Tuple[float, str]:
    ordered = [
        ("ecoinvent_direct", ecoinvent_direct),
        ("ecoinvent_inferred", ecoinvent_inferred),
        ("iedc_median", iedc_median),
        ("group_default", group_lifetime),
        ("current_trails", current_lifetime),
    ]
    for label, val in ordered:
        if val is not None and np.isfinite(val) and val > 0:
            return float(val), label
    return 20.0, "hard_fallback"


def build_group_preliminary_parameters(
    row: pd.Series,
    group_rec: Optional[GroupDefaultRec],
    *,
    ecoinvent_direct: Optional[float],
    ecoinvent_inferred: Optional[float],
    iedc_median: Optional[float],
) -> Dict[str, Any]:
    current_lifetime = to_float(row.get("lifetime"))
    lifetime, lifetime_basis = choose_preliminary_lifetime(
        current_lifetime=current_lifetime,
        ecoinvent_direct=ecoinvent_direct,
        ecoinvent_inferred=ecoinvent_inferred,
        iedc_median=iedc_median,
        group_lifetime=(group_rec.lifetime if group_rec else None),
    )

    if group_rec and group_rec.age_distribution_type in STRICT_SUGGESTION_DISTRIBUTION_TYPES:
        dist_type = int(group_rec.age_distribution_type)
        dist_type_basis = "group_default"
    else:
        current_dist = to_float(row.get("age distribution type"))
        if current_dist is not None and int(current_dist) in STRICT_SUGGESTION_DISTRIBUTION_TYPES:
            dist_type = int(current_dist)
            dist_type_basis = "current_trails"
        else:
            dist_type = 5
            dist_type_basis = "hard_fallback"

    loc = group_rec.loc if group_rec else None
    scale = group_rec.scale if group_rec else None
    minimum = group_rec.minimum if group_rec else None
    maximum = group_rec.maximum if group_rec else None

    completed = derive_complete_parameters(
        lifetime=lifetime,
        dist_type=dist_type,
        loc=loc,
        scale=scale,
        minimum=minimum,
        maximum=maximum,
    )

    return {
        "preliminary_lifetime": completed["suggested_lifetime"],
        "preliminary_distribution_type": completed["suggested_distribution_type"],
        "preliminary_age_distribution_description": temporal_distribution_type_full_text(
            completed["suggested_distribution_type"]
        ),
        "preliminary_loc": completed["suggested_loc"],
        "preliminary_scale": completed["suggested_scale"],
        "preliminary_minimum": completed["suggested_minimum"],
        "preliminary_maximum": completed["suggested_maximum"],
        "preliminary_lifetime_basis": lifetime_basis,
        "preliminary_distribution_basis": dist_type_basis,
    }


# ----------------------------
# Final recommendation helpers
# ----------------------------
def choose_final_lifetime(row_payload: Dict[str, Any], result: Dict[str, Any]) -> float:
    ei_direct = safe_float_or_none(row_payload.get("ecoinvent_direct_lifetime_years"))
    ei_inferred = safe_float_or_none(row_payload.get("ecoinvent_inferred_median_years"))
    iedc = safe_float_or_none(row_payload.get("iedc_lifetime_median_years"))
    group_lt = safe_float_or_none(row_payload.get("group_lifetime"))
    codex_lt = safe_float_or_none(result.get("suggested_lifetime"))
    current_lt = safe_float_or_none(row_payload.get("current_lifetime"))

    ei_status = str(row_payload.get("ecoinvent_lifetime_status", "") or "").strip().upper()
    iedc_status = str(row_payload.get("iedc_lifetime_status", "") or "").strip().upper()

    if ei_direct is not None and np.isfinite(ei_direct) and ei_direct > 0 and ei_status != "SUSPICIOUS":
        return float(ei_direct)

    if ei_inferred is not None and np.isfinite(ei_inferred) and ei_inferred > 0:
        return float(ei_inferred)

    if codex_lt is not None and np.isfinite(codex_lt) and codex_lt > 0:
        return float(codex_lt)

    if iedc is not None and np.isfinite(iedc) and iedc > 0 and iedc_status != "SUSPICIOUS":
        return float(iedc)

    if group_lt is not None and np.isfinite(group_lt) and group_lt > 0:
        return float(group_lt)

    if current_lt is not None and np.isfinite(current_lt) and current_lt > 0:
        return float(current_lt)

    return 20.0


def choose_final_distribution_type(row_payload: Dict[str, Any], result: Dict[str, Any]) -> int:
    candidates = [
        result.get("suggested_distribution_type"),
        row_payload.get("preliminary_distribution_type"),
        row_payload.get("group_age_distribution_type"),
        row_payload.get("current_age_distribution_type"),
    ]
    for c in candidates:
        try:
            ci = int(float(c))
            if ci in STRICT_SUGGESTION_DISTRIBUTION_TYPES:
                return ci
        except Exception:
            pass
    return 5


def derive_complete_parameters(
    lifetime: float,
    dist_type: int,
    loc: Optional[float],
    scale: Optional[float],
    minimum: Optional[float],
    maximum: Optional[float],
) -> Dict[str, float]:
    lifetime = clamp_positive(lifetime, 20.0)

    if minimum is None or not np.isfinite(minimum):
        minimum = -lifetime
    if maximum is None or not np.isfinite(maximum):
        maximum = 0.0

    if minimum > maximum:
        minimum, maximum = maximum, minimum

    if dist_type in (2, 3):
        if loc is None or not np.isfinite(loc):
            loc = -0.5 * lifetime
        if scale is None or not np.isfinite(scale) or scale <= 0:
            scale = max(1.0, 0.25 * lifetime)

        minimum = min(minimum, loc - 2.0 * scale)
        maximum = max(maximum, min(0.0, loc + 2.0 * scale))

    elif dist_type == 4:
        if minimum is None or maximum is None:
            minimum = -lifetime
            maximum = 0.0
        if minimum > maximum:
            minimum, maximum = maximum, minimum
        if loc is None or not np.isfinite(loc):
            loc = 0.5 * (minimum + maximum)
        if scale is None or not np.isfinite(scale) or scale <= 0:
            scale = max(1.0, (maximum - minimum) / np.sqrt(12.0))

    elif dist_type == 5:
        if minimum is None or maximum is None:
            minimum = -lifetime
            maximum = 0.0
        if minimum > maximum:
            minimum, maximum = maximum, minimum
        if loc is None or not np.isfinite(loc):
            loc = -0.5 * lifetime
        loc = min(max(loc, minimum), maximum)
        if scale is None or not np.isfinite(scale) or scale <= 0:
            scale = max(1.0, (maximum - minimum) / 6.0)

    else:
        dist_type = 5
        minimum = -lifetime
        maximum = 0.0
        loc = -0.5 * lifetime if loc is None or not np.isfinite(loc) else loc
        loc = min(max(loc, minimum), maximum)
        scale = max(1.0, 0.2 * lifetime) if scale is None or not np.isfinite(scale) or scale <= 0 else scale

    return {
        "suggested_lifetime": float(lifetime),
        "suggested_distribution_type": int(dist_type),
        "suggested_loc": float(loc),
        "suggested_scale": float(scale),
        "suggested_minimum": float(minimum),
        "suggested_maximum": float(maximum),
    }


def coerce_result_to_complete_suggestions(
    row_payload: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    lifetime = choose_final_lifetime(row_payload, result)
    dist_type = choose_final_distribution_type(row_payload, result)

    loc = safe_float_or_none(result.get("suggested_loc"))
    scale = safe_float_or_none(result.get("suggested_scale"))
    minimum = safe_float_or_none(result.get("suggested_minimum"))
    maximum = safe_float_or_none(result.get("suggested_maximum"))

    if loc is None:
        loc = safe_float_or_none(row_payload.get("preliminary_loc"))
    if scale is None:
        scale = safe_float_or_none(row_payload.get("preliminary_scale"))
    if minimum is None:
        minimum = safe_float_or_none(row_payload.get("preliminary_minimum"))
    if maximum is None:
        maximum = safe_float_or_none(row_payload.get("preliminary_maximum"))

    completed = derive_complete_parameters(
        lifetime=lifetime,
        dist_type=dist_type,
        loc=loc,
        scale=scale,
        minimum=minimum,
        maximum=maximum,
    )

    result = dict(result)
    result.update(completed)

    if not result.get("verdict"):
        result["verdict"] = "realistic" if bool(result.get("realistic")) else "not_realistic"
    if not result.get("confidence"):
        result["confidence"] = "medium"
    if not result.get("main_reason"):
        result["main_reason"] = "Final recommendation completed from evidence and deterministic fallback rules."
    if not result.get("parameter_assessment"):
        result["parameter_assessment"] = "All six recommendation fields were completed to produce a final parameter set."

    return result


# ----------------------------
# 1) Lifetime extraction from text
# ----------------------------
LIFE_PATTERNS: List[re.Pattern] = [
    re.compile(
        r"(?:reference\s+)?(?:service\s*life|useful\s*life|life\s*time|technical\s*life|operational\s*life)"
        r"\s*(?:is|of|=|:|assumed\s+to\s+be)?\s*(?P<years>\d+(?:\.\d+)?)\s*(?:years?|yrs?|yr|y|a)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"assumed\s+(?:technical\s+|service\s+)?life\s*time\s*(?:is|of|=|:)?\s*"
        r"(?P<years>\d+(?:\.\d+)?)\s*(?:years?|yrs?|yr|y|a)?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:plant|equipment|infrastructure|installation)\s+life\s*time\s*(?:is|of|=|:)?\s*"
        r"(?P<years>\d+(?:\.\d+)?)\s*(?:years?|yrs?|yr|y|a)?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\blife\s*time\b\s*(?:is|of|=|:|assumed\s+to\s+be)?\s*(?P<years>\d+(?:\.\d+)?)\s*(?:years?|yrs?|yr|y|a)?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:life|lifetime|service\s*life|useful\s*life|technical\s*life)\b"
        r"[^0-9]{0,50}(?P<years>\d+(?:\.\d+)?)\s*(?:years?|yrs?|yr|y|a)\b",
        re.IGNORECASE,
    ),
]

ANY_YEAR_TOKEN = re.compile(
    r"\b(?P<years>\d+(?:\.\d+)?)\s*(?:years?|yrs?)\b",
    re.IGNORECASE,
)

LIFE_CONTEXT = re.compile(
    r"\b(life|lifetime|service\s*life|useful\s*life|technical\s*life|replacement|replace|durab|design\s*life)\b",
    re.IGNORECASE,
)


def extract_lifetime_with_evidence(
    text: str,
    *,
    context_chars: int = 120,
    min_years: float = 0.5,
    max_years: float = 200.0,
) -> Tuple[Optional[float], Optional[str], Optional[str], bool]:
    if not text:
        return None, None, None, False

    strong_matches: List[Tuple[float, str, Any]] = []

    for i, pat in enumerate(LIFE_PATTERNS, start=1):
        for m in pat.finditer(text):
            try:
                years = float(m.group("years"))
            except Exception:
                continue

            if years < min_years or years > max_years:
                continue

            strong_matches.append((years, f"EI_PATTERN_{i}", m))

    if strong_matches:
        max_val = max(y for y, _, _ in strong_matches)
        filtered = [(y, pat_name, m) for y, pat_name, m in strong_matches if y >= 0.5 * max_val]
        years, pat_name, m = max(filtered, key=lambda x: x[0])

        start = max(0, m.start() - context_chars)
        end = min(len(text), m.end() + context_chars)
        snippet = text[start:end].replace("\n", " ").strip()

        return years, pat_name, snippet, False

    weak_matches: List[Tuple[float, Any]] = []

    for m in ANY_YEAR_TOKEN.finditer(text):
        try:
            years = float(m.group("years"))
        except Exception:
            continue

        if years < min_years or years > max_years:
            continue

        w0 = max(0, m.start() - 60)
        w1 = min(len(text), m.end() + 60)
        window = text[w0:w1]

        if not LIFE_CONTEXT.search(window):
            continue

        weak_matches.append((years, m))

    if weak_matches:
        max_val = max(y for y, _ in weak_matches)
        filtered = [(y, m) for y, m in weak_matches if y >= 0.5 * max_val]
        years, m = max(filtered, key=lambda x: x[0])

        start = max(0, m.start() - context_chars)
        end = min(len(text), m.end() + context_chars)
        snippet = text[start:end].replace("\n", " ").strip()

        return years, "WEAK_ANY_YEAR_TOKEN", snippet, True

    return None, None, None, False


# ----------------------------
# 2) Brightway2.5 access + indexing
# ----------------------------
def load_ecoinvent_index(bw_project: str, ei_db_name: str) -> List[ActRec]:
    import bw2data as bd

    bd.projects.set_current(bw_project)

    if ei_db_name not in bd.databases:
        raise ValueError(f"Database {ei_db_name!r} not found. Available: {list(bd.databases)}")

    db = bd.Database(ei_db_name)
    out: List[ActRec] = []

    for a in db:
        cats = a.get("categories", ()) or ()
        out.append(
            ActRec(
                key=a.key,
                name=a.get("name", "") or "",
                ref_product=a.get("reference product", "") or "",
                location=a.get("location", "") or "",
                unit=a.get("unit", "") or "",
                category_path="/".join(cats) if cats else "",
                comment=a.get("comment", "") or "",
            )
        )
    return out


def act_index_strings(acts: List[ActRec]) -> List[str]:
    return [f"{_norm(a.name)} | {_norm(a.ref_product)}" for a in acts]


def trails_query_string(row: pd.Series) -> str:
    name = _norm(row.get("name", ""))
    refp = _norm(row.get("reference product", ""))
    if name and refp:
        return f"{name} | {refp}"
    if name:
        return name
    if refp:
        return refp
    return ""


def best_match_activity(
    row: pd.Series,
    acts: List[ActRec],
    act_strs: List[str],
    *,
    min_score: float = 95.0,
    min_ref_product_score: float = 95.0,
) -> Tuple[Optional[ActRec], Optional[float]]:
    q = trails_query_string(row)
    if not q:
        return None, None

    m = process.extractOne(q, act_strs, scorer=fuzz.WRatio)
    if not m:
        return None, None

    _, score, idx = m
    score_f = float(score)
    if score_f < float(min_score):
        return None, score_f

    cand = acts[idx]
    rp_row = _norm(row.get("reference product", ""))

    if rp_row:
        rp_score = float(fuzz.WRatio(_lower(rp_row), _lower(cand.ref_product)))
        if rp_score < float(min_ref_product_score):
            return None, score_f

    return cand, score_f

def infer_target_iedc_commodity(row: pd.Series) -> Optional[str]:
    txt = " | ".join([
        str(row.get("name", "") or ""),
        str(row.get("reference product", "") or ""),
        str(row.get("CPC", "") or ""),
    ])
    txt_norm = normalize_text_for_match(txt)

    if "passenger car" in txt_norm or re.search(r"\bcar\b", txt_norm):
        return "private car"
    if "truck" in txt_norm or "lorry" in txt_norm:
        return "truck"
    if re.search(r"\bbus\b", txt_norm):
        return "bus"
    if "motorcycle" in txt_norm:
        return "motorcycle"
    if "bicycle" in txt_norm:
        return "bicycle"
    if "van" in txt_norm:
        return "light commercial vehicle"

    return None


def build_direct_lifetime_table(acts: List[ActRec], *, context_chars: int) -> pd.DataFrame:
    rows = []
    for a in acts:
        haystack = " ".join([a.comment, a.name, a.ref_product])
        lt, pat, snip, is_weak = extract_lifetime_with_evidence(haystack, context_chars=context_chars)
        if lt is None:
            continue
        rows.append(
            {
                "db": a.key[0],
                "code": a.key[1],
                "key": a.key,
                "lifetime_years": lt,
                "pattern": pat,
                "snippet": snip,
                "is_weak": bool(is_weak),
            }
        )
    return pd.DataFrame(rows)


def infer_lifetime_from_neighbors(
    target: ActRec,
    acts: List[ActRec],
    act_strs: List[str],
    direct_lt_strong: pd.DataFrame,
    *,
    top_k: int = 40,
    min_score: float = 88.0,
) -> Dict[str, Any]:
    q = f"{_norm(target.name)} | {_norm(target.ref_product)}"
    candidates = process.extract(q, act_strs, scorer=fuzz.WRatio, limit=max(top_k, 50))

    keys: List[Tuple[str, str]] = []
    for _, sc, idx in candidates:
        if float(sc) >= float(min_score):
            keys.append(acts[idx].key)

    if not keys:
        return {}

    keyset = set(keys)
    sub = direct_lt_strong.copy()
    if "db_code" not in sub.columns:
        sub["db_code"] = list(zip(sub["db"], sub["code"]))
    sub = sub[sub["db_code"].isin(keyset)]

    if len(sub) < 3:
        return {}

    vals = sub["lifetime_years"].astype(float).values
    med = float(np.median(vals))
    q25 = float(np.quantile(vals, 0.25))
    q75 = float(np.quantile(vals, 0.75))

    examples = sub.sort_values("lifetime_years").head(5)[
        ["db", "code", "lifetime_years", "pattern", "snippet"]
    ].to_dict(orient="records")

    return {
        "inferred_median": med,
        "inferred_q25": q25,
        "inferred_q75": q75,
        "n_neighbors": int(len(sub)),
        "examples": examples,
    }


def format_examples(examples: List[Dict[str, Any]], max_snip: int = 220) -> str:
    parts = []
    for ex in examples:
        sn = (ex.get("snippet") or "").replace("\n", " ").strip()
        if len(sn) > max_snip:
            sn = sn[:max_snip] + "…"
        parts.append(f"{ex.get('db')}:{ex.get('code')} | {ex.get('lifetime_years')} | {ex.get('pattern')} | {sn}")
    return " || ".join(parts)


# ----------------------------
# 3) Local IEDC lifetime benchmark files
# ----------------------------
IEDC_REQUIRED_SHEET_NAME = "Data"
IEDC_YEAR_UNIT_CODES = {"yr", "year", "years", "a"}


def find_iedc_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    norm_to_original = {normalize_text_for_match(c): c for c in df.columns}
    for cand in candidates:
        cand_norm = normalize_text_for_match(cand)
        if cand_norm in norm_to_original:
            return norm_to_original[cand_norm]
    return None


def parse_iedc_lifetime_value(value: Any, unitcode: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None

    if unitcode is not None and not (isinstance(unitcode, float) and np.isnan(unitcode)):
        u = normalize_text_for_match(unitcode)
        if u and u not in IEDC_YEAR_UNIT_CODES:
            return None

    vf = safe_float_or_none(value)
    if vf is not None and 0.1 <= vf <= 2000:
        return float(vf)

    s = str(value).strip()
    if not s:
        return None

    m = re.search(
        r"(?P<years>\d+(?:\.\d+)?)\s*(?:yr|year|years|a)?\b",
        s,
        re.IGNORECASE,
    )
    if not m:
        return None

    try:
        years = float(m.group("years"))
    except Exception:
        return None

    if 0.1 <= years <= 2000:
        return years
    return None


def get_first_matching_column(row_dict: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    col_lookup = {normalize_text_for_match(k): k for k in row_dict.keys()}
    for cand in candidates:
        k = col_lookup.get(normalize_text_for_match(cand))
        if k is not None:
            return k
    return None


def build_iedc_label_text_from_data_row(row_dict: Dict[str, Any], source_file: str) -> str:
    field_candidates = [
        ["dataset", "dataset_name"],
        ["aspect 1 : commodity", "aspect 1 commodity"],
        ["aspect 2", "aspect 2 : region", "aspect 2 region", "region", "geography", "location"],
        ["aspect 3", "aspect 3 : age-cohort", "aspect 3 age-cohort", "age-cohort", "cohort"],
        ["aspect 4 : process", "aspect 4 process", "aspect 4 : end-use sector", "aspect 4 end-use sector"],
        ["aspect 5", "aspect 5 : "],
        ["aspect 6", "aspect 6 : "],
        ["aspect 7", "aspect 7 : "],
        ["aspect 8", "aspect 8 : "],
        ["aspect 9", "aspect 9 : "],
        ["aspect 10", "aspect 10 : "],
        ["aspect 11", "aspect 11 : "],
        ["aspect 12", "aspect 12 : "],
    ]

    vals: List[str] = [Path(source_file).stem]

    for candidates in field_candidates:
        k = get_first_matching_column(row_dict, candidates)
        if k is None:
            continue
        v = row_dict.get(k)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        s = str(v).strip()
        if s:
            vals.append(s)

    return normalize_text_for_match(" | ".join(vals))


def load_iedc_lifetime_records(lt_data_dir: str) -> List[IEDCLifetimeRec]:
    base = Path(lt_data_dir)
    if not base.exists():
        return []

    files = sorted([p for p in base.glob("*") if p.suffix.lower() in [".xlsx", ".xls", ".xlsm"]])
    records: List[IEDCLifetimeRec] = []

    for fp in files:
        file_asset_class = source_file_asset_class(fp.name)

        try:
            xl = pd.ExcelFile(fp)
        except Exception:
            continue

        sheet_name = None
        for s in xl.sheet_names:
            if normalize_text_for_match(s) == normalize_text_for_match(IEDC_REQUIRED_SHEET_NAME):
                sheet_name = s
                break

        if sheet_name is None:
            continue

        try:
            df = pd.read_excel(fp, sheet_name=sheet_name)
        except Exception:
            continue

        if df.empty:
            continue

        commodity_col = find_iedc_column(df, ["aspect 1 : commodity", "aspect 1 commodity"])
        value_col = find_iedc_column(df, ["value"])
        unit_col = find_iedc_column(df, ["unitcode", "unit code", "unit"])

        dataset_col = find_iedc_column(df, ["dataset", "dataset_name"])
        process_col = find_iedc_column(
            df,
            [
                "aspect 4 : process",
                "aspect 4 process",
                "aspect 4 : end-use sector",
                "aspect 4 end-use sector",
            ],
        )
        geography_col = find_iedc_column(
            df,
            [
                "aspect 2",
                "aspect 2 : region",
                "aspect 2 region",
                "region",
                "geography",
                "location",
            ],
        )
        years_col = find_iedc_column(
            df,
            [
                "aspect 3",
                "aspect 3 : age-cohort",
                "aspect 3 age-cohort",
                "age-cohort",
                "cohort",
            ],
        )

        if commodity_col is None or value_col is None:
            continue

        for i, row in df.iterrows():
            commodity = row.get(commodity_col)
            if commodity is None or (isinstance(commodity, float) and np.isnan(commodity)):
                continue

            commodity_s = str(commodity).strip()
            if not commodity_s:
                continue
            commodity_norm = normalize_iedc_commodity_for_matching(commodity_s)

            unit_val = row.get(unit_col) if unit_col else None
            lifetime_years = parse_iedc_lifetime_value(row.get(value_col), unit_val)
            if lifetime_years is None:
                continue

            row_dict = row.to_dict()

            compact_row_dict: Dict[str, Any] = {}
            for col in [dataset_col, commodity_col, geography_col, years_col, process_col, value_col, unit_col]:
                if col is None:
                    continue
                compact_row_dict[col] = row.get(col)

            label_text = build_iedc_label_text_from_data_row(row_dict, fp.name)

            record_asset_class = detect_asset_class(label_text)
            if record_asset_class == "generic":
                record_asset_class = file_asset_class

            raw_row_json = json.dumps(
                {k: (None if pd.isna(v) else str(v)) for k, v in compact_row_dict.items()},
                ensure_ascii=False,
            )

            geography_val = row.get(geography_col) if geography_col else None
            geography_s = None
            if geography_val is not None and not (isinstance(geography_val, float) and np.isnan(geography_val)):
                geography_s = str(geography_val).strip()

            records.append(
                IEDCLifetimeRec(
                    source_file=fp.name,
                    sheet_name=str(sheet_name),
                    row_number=int(i) + 2,
                    label_text=label_text,
                    lifetime_years=float(lifetime_years),
                    raw_row_json=raw_row_json,
                    asset_class=record_asset_class,
                    geography=geography_s,
                    commodity=commodity_norm,
                )
            )
    return records


def build_iedc_index_strings(recs: List[IEDCLifetimeRec]) -> List[str]:
    return [r.label_text for r in recs]


def trails_iedc_query_string(row: pd.Series) -> str:
    parts = [
        row.get("reference product", ""),
        row.get("name", ""),
    ]
    return normalize_text_for_match(
        " | ".join([str(p) for p in parts if p is not None and str(p).strip()])
    )


def select_iedc_records_with_global_preference(
    recs: List[IEDCLifetimeRec],
) -> Tuple[List[IEDCLifetimeRec], str]:
    if not recs:
        return [], "NO_SELECTED_RECORDS"

    global_recs = [
                r for r in recs
                if r.geography is not None
                and "global" in normalize_text_for_match(r.geography)
            ]

    if global_recs:
        return global_recs, "GLOBAL_PREFERRED"

    return recs, "AVERAGE_ALL_SELECTED"


def summarize_iedc_selected_records(
    recs: List[IEDCLifetimeRec],
) -> Dict[str, Any]:
    vals = [float(r.lifetime_years) for r in recs if r.lifetime_years is not None and 0.1 <= float(r.lifetime_years) <= 200]
    if not vals:
        return {
            "iedc_values_years": [],
            "iedc_lifetime_mean_years": None,
            "iedc_lifetime_median_years": None,
            "iedc_lifetime_q25_years": None,
            "iedc_lifetime_q75_years": None,
            "iedc_band": None,
        }

    arr = np.array(vals, dtype=float)
    mean_val = float(np.mean(arr))
    med_val = float(np.median(arr))
    q25_val = float(np.quantile(arr, 0.25))
    q75_val = float(np.quantile(arr, 0.75))

    if len(vals) == 1:
        y = float(vals[0])
        band = (0.5 * y, 1.5 * y)
    else:
        band = (q25_val, q75_val)

    return {
        "iedc_values_years": vals,
        "iedc_lifetime_mean_years": mean_val,
        "iedc_lifetime_median_years": med_val,
        "iedc_lifetime_q25_years": q25_val,
        "iedc_lifetime_q75_years": q75_val,
        "iedc_band": band,
    }


def fetch_iedc_evidence_for_row(
    row: pd.Series,
    iedc_recs: List[IEDCLifetimeRec],
    iedc_strs: List[str],
    *,
    top_k: int = IEDC_TOP_K,
    min_score: float = IEDC_MIN_MATCH_SCORE,
    min_shared_keywords: int = IEDC_MIN_SHARED_KEYWORDS,
) -> Dict[str, Any]:
    if not iedc_recs:
        return {
            "iedc_values_years": [],
            "iedc_selected_values_years": [],
            "iedc_sources": "",
            "iedc_selected_sources": "",
            "iedc_notes": "NO_IEDC_FILES_OR_RECORDS",
            "iedc_match_score": None,
            "iedc_examples": "",
            "iedc_selection_mode": "NO_IEDC_FILES_OR_RECORDS",
            "iedc_lifetime_mean_years": None,
            "iedc_lifetime_median_years": None,
            "iedc_lifetime_q25_years": None,
            "iedc_lifetime_q75_years": None,
            "iedc_band": None,
        }

    q = trails_iedc_query_string(row)
    query_asset_class = row_asset_class_from_context(row)
    target_commodity = infer_target_iedc_commodity(row)

    # First: commodity-based filtering if possible
    commodity_filtered: List[IEDCLifetimeRec] = []
    if target_commodity is not None:
        commodity_filtered = [
            rec for rec in iedc_recs
            if rec.commodity is not None and rec.commodity == target_commodity
        ]

    # If commodity filter worked, use it directly
    if commodity_filtered:
        selected_recs_only = [
            rec for rec in commodity_filtered
            if asset_classes_compatible(query_asset_class, rec.asset_class)
               or query_asset_class == "vehicle"
        ]
        selection_pre_note = f"COMMODITY_MATCH:{target_commodity}"
    else:
        # fallback to fuzzy matching
        if not q:
            return {
                "iedc_values_years": [],
                "iedc_selected_values_years": [],
                "iedc_sources": "",
                "iedc_selected_sources": "",
                "iedc_notes": "EMPTY_QUERY",
                "iedc_match_score": None,
                "iedc_examples": "",
                "iedc_selection_mode": "EMPTY_QUERY",
                "iedc_lifetime_mean_years": None,
                "iedc_lifetime_median_years": None,
                "iedc_lifetime_q25_years": None,
                "iedc_lifetime_q75_years": None,
                "iedc_band": None,
            }

        candidates = process.extract(q, iedc_strs, scorer=fuzz.WRatio, limit=max(top_k, 50))
        selected_pairs: List[Tuple[IEDCLifetimeRec, float]] = []

        for _, sc, idx in candidates:
            rec = iedc_recs[idx]
            scf = float(sc)

            if scf < float(min_score):
                continue
            if not asset_classes_compatible(query_asset_class, rec.asset_class):
                continue
            if shared_keywords_count(q, rec.label_text) < int(min_shared_keywords):
                continue

            selected_pairs.append((rec, scf))

        selected_recs_only = [rec for rec, _ in selected_pairs]
        selection_pre_note = "FUZZY_MATCH"

    if not selected_recs_only:
        return {
            "iedc_values_years": [],
            "iedc_selected_values_years": [],
            "iedc_sources": "",
            "iedc_selected_sources": "",
            "iedc_notes": "NO_GOOD_IEDC_MATCH",
            "iedc_match_score": None,
            "iedc_examples": "",
            "iedc_selection_mode": "NO_GOOD_IEDC_MATCH",
            "iedc_lifetime_mean_years": None,
            "iedc_lifetime_median_years": None,
            "iedc_lifetime_q25_years": None,
            "iedc_lifetime_q75_years": None,
            "iedc_band": None,
        }

    preferred_recs, selection_mode = select_iedc_records_with_global_preference(selected_recs_only)
    preferred_summary = summarize_iedc_selected_records(preferred_recs)

    all_vals = [float(rec.lifetime_years) for rec in selected_recs_only if 0.1 <= float(rec.lifetime_years) <= 200]
    all_srcs = [f"{rec.source_file}::{rec.sheet_name}::row{rec.row_number}" for rec in selected_recs_only]
    preferred_srcs = [f"{rec.source_file}::{rec.sheet_name}::row{rec.row_number}" for rec in preferred_recs]

    examples = " || ".join(
        [
            f"{rec.source_file}::{rec.sheet_name}::row{rec.row_number} | {rec.lifetime_years:g} y | aspect2={rec.geography or ''} | {rec.label_text[:180]}"
            for rec in preferred_recs[:5]
        ]
    )

    return {
        "iedc_values_years": all_vals,
        "iedc_selected_values_years": preferred_summary["iedc_values_years"],
        "iedc_sources": " | ".join(all_srcs),
        "iedc_selected_sources": " | ".join(preferred_srcs),
        "iedc_notes": f"{selection_pre_note};{selection_mode}",
        "iedc_match_score": None,
        "iedc_examples": examples,
        "iedc_selection_mode": selection_mode,
        "iedc_lifetime_mean_years": preferred_summary["iedc_lifetime_mean_years"],
        "iedc_lifetime_median_years": preferred_summary["iedc_lifetime_median_years"],
        "iedc_lifetime_q25_years": preferred_summary["iedc_lifetime_q25_years"],
        "iedc_lifetime_q75_years": preferred_summary["iedc_lifetime_q75_years"],
        "iedc_band": preferred_summary["iedc_band"],
    }

# ----------------------------
# 4) Review logic
# ----------------------------
def evidence_band_from_direct(years: float) -> Tuple[float, float]:
    return 0.5 * years, 1.5 * years


def evidence_band_from_iqr(q25: float, q75: float) -> Tuple[float, float]:
    return q25, q75


def compare_against_band(val: float, band: Tuple[float, float]) -> str:
    lo, hi = band
    if lo <= val <= hi:
        return "OK"
    exp_lo = 0.7 * lo
    exp_hi = 1.3 * hi
    if exp_lo <= val <= exp_hi:
        return "REVIEW"
    return "SUSPICIOUS"


def distribution_flags(
    lifetime_ref: Optional[float],
    dist_type: Optional[int],
    loc: Optional[float],
    scale: Optional[float],
    vmin: Optional[float],
    vmax: Optional[float],
) -> List[str]:
    flags: List[str] = []

    if lifetime_ref is None or not np.isfinite(lifetime_ref) or lifetime_ref <= 0:
        flags.append("NO_VALID_LIFETIME_REF_FOR_DISTRIBUTION")
        return flags

    if dist_type is None:
        flags.append("DIST_TYPE_MISSING")
        return flags

    if vmin is not None and vmax is not None and vmin >= vmax:
        flags.append("MIN_GE_MAX")

    if vmin is not None and vmin > 0:
        flags.append("MIN_POSITIVE_UNEXPECTED")

    if vmax is not None and vmax > 0:
        flags.append("MAX_POSITIVE_UNEXPECTED")

    if vmin is not None and abs(vmin) > 5 * lifetime_ref:
        flags.append("MIN_TOO_OLD_VS_LIFETIME")

    if vmax is not None and abs(vmax) > 2 * lifetime_ref:
        flags.append("MAX_TOO_OLD_VS_LIFETIME")

    if dist_type in (2, 3):
        if loc is None:
            flags.append("LOC_MISSING_FOR_NORMAL_LOGNORMAL")
        if scale is None:
            flags.append("SCALE_MISSING_FOR_NORMAL_LOGNORMAL")
        elif scale <= 0:
            flags.append("SCALE_NONPOSITIVE")

    if dist_type == 4:
        if vmin is None or vmax is None:
            flags.append("UNIFORM_NEEDS_MIN_MAX")

    if dist_type == 5:
        if vmin is None or vmax is None or loc is None:
            flags.append("TRI_NEEDS_MIN_MAX_LOC")
        else:
            if not (vmin <= loc <= vmax):
                flags.append("TRI_LOC_NOT_IN_RANGE")
            if loc < -1.2 * lifetime_ref:
                flags.append("TRI_MODE_VERY_EARLY")
            if loc > -0.05 * lifetime_ref:
                flags.append("TRI_MODE_VERY_LATE")

    return flags


def decide_needs_check(
    *,
    match_score: Optional[float],
    ei_status: str,
    iedc_status: str,
    dist_flags: List[str],
    weak_lifetime_hit: bool,
    has_any_numeric_evidence: bool,
) -> Tuple[bool, str]:
    reasons: List[str] = []

    if match_score is None or float(match_score) < 95.0:
        reasons.append("WEAK_OR_NO_ECOINVENT_MATCH")

    if not has_any_numeric_evidence:
        reasons.append("NO_NUMERIC_LIFETIME_EVIDENCE")

    if weak_lifetime_hit:
        reasons.append("WEAK_LIFETIME_MATCH_ANY_YEAR_TOKEN")

    if ei_status in ("SUSPICIOUS", "TRAILS_LIFETIME_INVALID"):
        reasons.append(f"ECoinvent_{ei_status}")

    if iedc_status == "SUSPICIOUS":
        reasons.append("IEDC_SUSPICIOUS")

    severe = {
        "MIN_GE_MAX",
        "SCALE_NONPOSITIVE",
        "TRI_LOC_NOT_IN_RANGE",
        "MIN_POSITIVE_UNEXPECTED",
        "MAX_POSITIVE_UNEXPECTED",
        "NO_VALID_LIFETIME_REF_FOR_DISTRIBUTION",
    }
    severe_hit = [f for f in dist_flags if f in severe]
    if severe_hit:
        reasons.append("DIST_SEVERE:" + ",".join(severe_hit))

    needs = len(reasons) > 0
    return needs, ";".join(reasons)


# ----------------------------
# 5) Model validation
# ----------------------------
CODEX_REVIEW_OUTPUT_COLUMNS = [
    "codex_is_stock_asset",
    "codex_stock_asset_confidence",
    "codex_stock_asset_reason",
    "codex_unit_plausible_for_stock_asset",
    "codex_unit_check_reason",
    "codex_realistic",
    "codex_verdict",
    "codex_confidence",
    "codex_main_reason",
    "codex_parameter_assessment",
    "codex_suggested_lifetime",
    "codex_suggested_distribution_type",
    "codex_suggested_loc",
    "codex_suggested_scale",
    "codex_suggested_minimum",
    "codex_suggested_maximum",
    "codex_batch_error",
]


def resolve_codex_executable(user_value: Optional[str] = None) -> Optional[str]:
    candidates: List[str] = []

    if user_value:
        candidates.append(user_value)

    env_value = os.environ.get("CODEX_EXE")
    if env_value:
        candidates.append(env_value)

    candidates.extend(["codex", "codex.cmd", "codex.exe"])

    for cand in candidates:
        if not cand:
            continue

        p = Path(cand)
        if p.exists():
            return str(p)

        resolved = shutil.which(cand)
        if resolved:
            return resolved

    return None


def codex_cli_available(codex_exe: Optional[str]) -> bool:
    return codex_exe is not None


def openai_api_available() -> bool:
    return OpenAI is not None and bool(os.environ.get("OPENAI_API_KEY"))


def initialize_codex_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in CODEX_REVIEW_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df


def is_row_for_codex(row: pd.Series, review_scope: str) -> bool:
    if review_scope == "all":
        return True

    flags = str(row.get("flags", "") or "")
    needs_check = bool(row.get("needs_check"))
    ei_status = str(row.get("ecoinvent_lifetime_status", "") or "")
    iedc_status = str(row.get("iedc_lifetime_status", "") or "")

    severe_markers = [
        "MIN_GE_MAX",
        "SCALE_NONPOSITIVE",
        "TRI_LOC_NOT_IN_RANGE",
        "NO_VALID_LIFETIME_REF_FOR_DISTRIBUTION",
        "MIN_POSITIVE_UNEXPECTED",
        "MAX_POSITIVE_UNEXPECTED",
    ]

    return (
        needs_check
        or ei_status == "SUSPICIOUS"
        or iedc_status == "SUSPICIOUS"
        or any(m in flags for m in severe_markers)
    )


def build_codex_payload_for_row(row: pd.Series, row_index: int) -> Dict[str, Any]:
    payload = {
        "row_index": int(row_index),
        "name": row.get("name", ""),
        "reference_product": row.get("reference product", ""),
        "CPC": row.get("CPC", ""),
        "ISIC_rev4_ecoinvent": row.get("ISIC rev.4 ecoinvent", ""),
        "current_lifetime": row.get("lifetime", None),
        "current_age_distribution_type": row.get("age distribution type", None),
        "current_loc": row.get("loc", None),
        "current_scale": row.get("scale", None),
        "current_minimum": row.get("minimum", None),
        "current_maximum": row.get("maximum", None),
        "matched_ei_name": row.get("matched_ei_name", ""),
        "matched_ei_ref_product": row.get("matched_ei_ref_product", ""),
        "matched_ei_location": row.get("matched_ei_location", ""),
        "ecoinvent_evidence_mode": row.get("ecoinvent_evidence_mode", ""),
        "ecoinvent_direct_lifetime_years": row.get("ecoinvent_direct_lifetime_years", None),
        "ecoinvent_lifetime_status": row.get("ecoinvent_lifetime_status", ""),
        "iedc_numeric_sources": row.get("iedc_numeric_sources", ""),
        "iedc_lifetime_values_years": row.get("iedc_lifetime_values_years", ""),
        "iedc_lifetime_status": row.get("iedc_lifetime_status", ""),
        "iedc_lifetime_median_years": row.get("iedc_lifetime_median_years", None),
        "group_name": row.get("group_name", ""),
        "group_match_basis": row.get("group_match_basis", ""),
        "group_lifetime": row.get("group_lifetime", None),
        "group_age_distribution_type": row.get("group_age_distribution_type", None),
        "group_age_distribution_description": row.get("group_age_distribution_description", ""),
        "group_loc": row.get("group_loc", None),
        "group_scale": row.get("group_scale", None),
        "group_minimum": row.get("group_minimum", None),
        "group_maximum": row.get("group_maximum", None),
        "group_param_confidence": row.get("group_param_confidence", ""),
        "group_basis_type": row.get("group_basis_type", ""),
        "group_confidence": row.get("group_confidence", ""),
        "group_source_short": row.get("group_source_short", ""),
        "preliminary_lifetime": row.get("preliminary_lifetime", None),
        "preliminary_distribution_type": row.get("preliminary_distribution_type", None),
        "preliminary_age_distribution_description": row.get("preliminary_age_distribution_description", ""),
        "preliminary_loc": row.get("preliminary_loc", None),
        "preliminary_scale": row.get("preliminary_scale", None),
        "preliminary_minimum": row.get("preliminary_minimum", None),
        "preliminary_maximum": row.get("preliminary_maximum", None),
        "preliminary_lifetime_basis": row.get("preliminary_lifetime_basis", ""),
        "preliminary_distribution_basis": row.get("preliminary_distribution_basis", ""),
        "distribution_lifetime_ref_used": row.get("distribution_lifetime_ref_used", None),
        "flags": row.get("flags", ""),
        "needs_check": bool(row.get("needs_check", False)),
        "needs_check_reasons": row.get("needs_check_reasons", ""),
    }
    return to_jsonable(payload)


def build_codex_batch_prompt(batch_payload: List[Dict[str, Any]]) -> str:
    schema_example = {
        "results": [
            {
                "row_index": 0,
                "is_stock_asset": True,
                "stock_asset_confidence": "high",
                "stock_asset_reason": "The row describes a durable technical asset or infrastructure element rather than an operating input or service flow.",
                "unit_plausible_for_stock_asset": True,
                "unit_check_reason": "The unit is consistent with a durable asset stock representation.",
                "realistic": True,
                "verdict": "realistic",
                "confidence": "medium",
                "main_reason": "short sentence",
                "parameter_assessment": "short sentence",
                "suggested_lifetime": 25.0,
                "suggested_distribution_type": 5,
                "suggested_loc": -12.5,
                "suggested_scale": 4.0,
                "suggested_minimum": -25.0,
                "suggested_maximum": 0.0,
            }
        ]
    }

    safe_batch_payload = to_jsonable(batch_payload)

    return f"""
You are reviewing TRAILS stock-asset rows.

Temporal distribution type definitions:

2 = lognormal, with loc and scale
3 = normal, with loc and scale
4 = uniform, with minimum and maximum
5 = triangular, with loc, minimum, and maximum
6 = discrete, using weights and offsets

Interpretation of grouped defaults:

Group-based defaults represent the approximate fleet-vintage structure of stock assets at the start of the analysis period.
Negative years mean the asset cohort was installed before commissioning/start year.
For example, a loc of -45 means the modal installed cohort is about 45 years old at the start year.
For stock assets, maximum should normally remain earlier than -1 year, because infrastructure and capital equipment are generally already in place before the modeled system starts operating.

Important preliminary checks before recommending parameters:

For each row, first assess whether the row is actually a stock asset.
A stock asset is a durable capital good, infrastructure element, technical installation, machinery item, vehicle, building, plant, network element, or replaceable durable equipment component that belongs to an asset stock and can meaningfully be represented by an age distribution over time.

Rows are less likely to be true stock assets if they mainly describe:
- operating inputs or consumables
- fuels, electricity, heat, water, chemicals, lubricants, or feedstocks
- services
- transport service outputs
- waste treatment flows
- bulk materials that are used up during operation rather than existing as a durable stock

Also assess whether the unit is plausible for stock-asset treatment.
Units more consistent with stock assets include durable-item or capacity-style units such as:
- unit, item, piece, vehicle, plant, building
- kW, MW, m2, km, infrastructure segment
- other units clearly representing installed durable capacity or physical stock

Units can be suspicious for stock-asset treatment if they mainly represent:
- mass, volume, or energy flows (e.g. kg, tonne, liter, MJ, kWh)
- service outputs (e.g. person-km, ton-km, treatment service units)
- operating inputs rather than installed durable stock

A suspicious unit does not automatically invalidate stock-asset treatment, but it should trigger stricter reasoning.

Lifetime source hierarchy with plausibility screening:

If ecoinvent_direct_lifetime_years exists and is plausible for the stock asset, use it as the chosen lifetime.
Else if ecoinvent_inferred_median_years exists and is plausible, use it as the chosen lifetime.
Else if iedc_lifetime_median_years exists and is plausible (i.e., not flagged suspicious or clearly mismatched), use it as the chosen lifetime.
Else use the grouped lifetime from Group_Assumptions, unless it is clearly mismatched for the asset.
If any benchmark is flagged as suspicious, implausible, or clearly mismatched, it must be discarded and replaced with a more realistic lifetime.

Distribution type default:

Use the grouped age distribution type as the default pre-Codex fleet-vintage shape for stock assets only.

If you judge that the row is probably not a real stock asset, you should still return a complete parameter set, but:
- clearly indicate that in is_stock_asset = false
- explain the problem in stock_asset_reason
- be conservative in the recommendation
- do not force a strong stock-style interpretation unless the evidence supports it

Parameter rescaling rules:

If the chosen lifetime differs from the grouped lifetime, rescale grouped defaults instead of copying them directly.
For grouped triangular defaults, preserve both the loc share and the maximum share of lifetime:
loc_share = ABS(group_loc / group_lifetime)
max_share = ABS(group_maximum / group_lifetime)
suggested_loc = - suggested_lifetime * loc_share
suggested_minimum = - suggested_lifetime
suggested_maximum = MIN(-1, - suggested_lifetime * max_share)

Uniform-group rules:

For grouped uniform defaults, spread in-service vintages approximately evenly across the replacement cycle.
Keep suggested_maximum earlier than -1 year.
loc and scale should remain unused for uniform distributions, but still return structurally valid placeholder values if required.

Lognormal-group rules:

For grouped lognormal defaults, interpret loc as the median lag before commissioning (negative years).
Interpret scale as the geometric spread factor (GSD).
Preserve the loc share of lifetime when chosen_lifetime differs from grouped lifetime:
loc_share = ABS(group_loc / group_lifetime)
suggested_loc = - suggested_lifetime * loc_share
Keep suggested_scale = group_scale.

Short-life replaceables:

For short-life replaceables such as filters and UV lamps, triangular is preferred over uniform when replacement timing is already modeled separately, because burdens are concentrated at each production or replacement event.

Evidence hierarchy:

ecoinvent evidence is the first and strongest benchmark when plausible.
local IEDC Excel lifetime benchmarks are the second benchmark layer, but must be screened for plausibility.
group-based preliminary temporal defaults are the third benchmark layer.
current TRAILS parameters are the fourth layer.

Task:
For each input row, produce:

1) a judgment on whether the row is actually a stock asset
2) a judgment on whether the unit is plausible for stock-asset treatment
3) a FINAL recommended parameter set for:

suggested_lifetime
suggested_distribution_type
suggested_loc
suggested_scale
suggested_minimum
suggested_maximum

Critical rules:

Every input row must appear exactly once in the results list.
You must return a COMPLETE recommendation for every row.
Do NOT return null for any of the suggested_* parameter fields.
Do NOT return null for the stock-asset or unit-check fields.
If a distribution does not use a parameter, still return a structurally valid placeholder value.
Base the recommendation on the row evidence provided.
If web search is available, use it only as a secondary support check, not as a primary benchmark.
Prefer realistic system or asset lifetime assumptions over component-only lifetimes.
Be conservative and internally consistent.
Respect the lifetime hierarchy, but apply plausibility screening before selecting any value.
Do not preserve any benchmark value that is flagged as suspicious or clearly implausible for the stock asset.
If ecoinvent direct lifetime exists and is credible, it is the strongest benchmark and should normally not be overridden.
If the ecoinvent direct lifetime is flagged suspicious or clearly implausible, you may treat it as invalid and replace it.
Use group-based mappings as structured support for distribution type and parameter shape.
Group defaults are coarse priors and may be overridden when the asset is a smaller standalone equipment unit (e.g., compressors, chargers, pumps, electrical cabinets) or when clearly mismatched.
The suggested parameters must be coherent with the selected distribution type and the fleet-vintage interpretation.

Additional override rule:

If a lower-tier benchmark (especially IEDC or grouped lifetime) is flagged as suspicious, implausible, or clearly mismatched, it must NOT be preserved.
In such cases, explicitly replace it with a corrected suggested_lifetime that reflects a realistic system-level lifetime.

Numeric output rule:

suggested_lifetime must represent your final accepted lifetime after plausibility screening.
Do NOT propagate a numeric value that you have judged invalid.
If, for example, IEDC = 30 years is flagged as suspicious and you judge 100 years more realistic, you must return suggested_lifetime = 100.

Critical post-processing intent:

If you recommend changing the lifetime because an existing benchmark is invalid, your suggested_lifetime is intended to replace that benchmark in the final output.
Do not only explain the correction in text; encode it directly in suggested_lifetime.

Output format:

Return EXACTLY one valid JSON object.
Do not use markdown fences.
Do not include any text before or after the JSON.

Required JSON shape:
{json.dumps(schema_example, ensure_ascii=False, indent=2)}

Rows:
{json.dumps(safe_batch_payload, ensure_ascii=False)}
""".strip()

def parse_codex_batch_output(text: str) -> Dict[int, Dict[str, Any]]:
    raw = (text or "").strip()

    if not raw:
        raise ValueError("Model returned empty text.")

    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(f"Model output is not valid JSON.\nRaw output:\n{raw[:4000]}")
        data = json.loads(raw[start:end + 1])

    if not isinstance(data, dict) or "results" not in data or not isinstance(data["results"], list):
        raise ValueError(f"Output must be a JSON object with a 'results' list.\nParsed output:\n{data}")

    out: Dict[int, Dict[str, Any]] = {}
    for item in data["results"]:
        if not isinstance(item, dict):
            continue
        idx = item.get("row_index")
        if idx is None:
            continue
        out[int(idx)] = item
    return out


def run_codex_batch(
    batch_payload: List[Dict[str, Any]],
    *,
    codex_exe: str,
    codex_model: str,
    codex_timeout: int,
) -> Dict[int, Dict[str, Any]]:
    prompt = build_codex_batch_prompt(batch_payload)

    env = os.environ.copy()
    env.setdefault("NO_COLOR", "1")
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    cmd = [codex_exe, "exec", "--model", codex_model, "-"]

    try:
        completed = subprocess.run(
            cmd,
            input=prompt,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            timeout=codex_timeout,
            shell=False,
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"Codex executable not found: {codex_exe!r}. Pass --codex-exe explicitly or add Codex to PATH."
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(
            f"Codex timed out after {codex_timeout}s.\nSTDOUT:\n{e.stdout or ''}\n\nSTDERR:\n{e.stderr or ''}"
        )

    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()

    if completed.returncode != 0:
        raise RuntimeError(
            "Codex failed.\n"
            f"Return code: {completed.returncode}\n"
            f"STDOUT:\n{stdout or '[empty]'}\n\n"
            f"STDERR:\n{stderr or '[empty]'}"
        )

    if not stdout:
        raise RuntimeError(f"Codex returned empty stdout.\nSTDERR:\n{stderr or '[empty]'}")

    return parse_codex_batch_output(stdout)


def extract_text_from_response(resp: Any) -> str:
    output_text = getattr(resp, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    text_parts: List[str] = []
    output = getattr(resp, "output", None)
    if output:
        for item in output:
            content = getattr(item, "content", None) or []
            for c in content:
                txt = getattr(c, "text", None)
                if isinstance(txt, str):
                    text_parts.append(txt)

    if text_parts:
        return "\n".join(text_parts).strip()

    return str(resp).strip()


def run_openai_api_batch(
    batch_payload: List[Dict[str, Any]],
    *,
    api_models: Sequence[str],
    api_timeout: int,
    enable_web_review: bool = True,
    web_search_context: str = "medium",
) -> Dict[int, Dict[str, Any]]:
    if not openai_api_available():
        raise RuntimeError("OpenAI API not available. Install `openai` and set OPENAI_API_KEY.")

    client = OpenAI(timeout=api_timeout)
    prompt = build_codex_batch_prompt(batch_payload)
    last_err: Optional[Exception] = None

    tools = []
    if enable_web_review:
        tools = [{
            "type": "web_search_preview_2025_03_11",
            "search_context_size": web_search_context,
        }]

    for model_name in api_models:
        try:
            resp = client.responses.create(
                model=model_name,
                input=prompt,
                tools=tools,
            )
            text = extract_text_from_response(resp)
            return parse_codex_batch_output(text)
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"All API fallback models failed. Last error: {last_err}")


def run_llm_batch_with_fallback(
    batch_payload: List[Dict[str, Any]],
    *,
    prefer_codex_cli: bool,
    codex_exe: Optional[str],
    codex_model: str,
    codex_timeout: int,
    api_models: Sequence[str],
    api_timeout: int,
    enable_web_review: bool,
    web_search_context: str,
) -> Dict[int, Dict[str, Any]]:
    cli_error: Optional[Exception] = None

    if prefer_codex_cli and codex_exe:
        try:
            return run_codex_batch(
                batch_payload,
                codex_exe=codex_exe,
                codex_model=codex_model,
                codex_timeout=codex_timeout,
            )
        except Exception as e:
            cli_error = e

    try:
        return run_openai_api_batch(
            batch_payload,
            api_models=api_models,
            api_timeout=api_timeout,
            enable_web_review=enable_web_review,
            web_search_context=web_search_context,
        )
    except Exception as api_err:
        if cli_error is not None:
            raise RuntimeError(
                f"Both Codex CLI and OpenAI API fallback failed.\nCLI error: {cli_error}\nAPI error: {api_err}"
            )
        raise


def apply_codex_results(
    df: pd.DataFrame,
    results_by_idx: Dict[int, Dict[str, Any]],
    *,
    batch_payload_by_idx: Dict[int, Dict[str, Any]],
    batch_error: Optional[str] = None,
) -> None:
    for idx, result in results_by_idx.items():
        if idx not in df.index:
            continue

        row_payload = batch_payload_by_idx[idx]
        result = coerce_result_to_complete_suggestions(row_payload, result)

        df.at[idx, "codex_is_stock_asset"] = result.get("is_stock_asset")
        df.at[idx, "codex_stock_asset_confidence"] = result.get("stock_asset_confidence")
        df.at[idx, "codex_stock_asset_reason"] = result.get("stock_asset_reason")
        df.at[idx, "codex_unit_plausible_for_stock_asset"] = result.get("unit_plausible_for_stock_asset")
        df.at[idx, "codex_unit_check_reason"] = result.get("unit_check_reason")
        df.at[idx, "codex_realistic"] = result.get("realistic")
        df.at[idx, "codex_verdict"] = result.get("verdict")
        df.at[idx, "codex_confidence"] = result.get("confidence")
        df.at[idx, "codex_main_reason"] = result.get("main_reason")
        df.at[idx, "codex_parameter_assessment"] = result.get("parameter_assessment")
        df.at[idx, "codex_suggested_lifetime"] = result.get("suggested_lifetime")
        df.at[idx, "codex_suggested_distribution_type"] = result.get("suggested_distribution_type")
        df.at[idx, "codex_suggested_loc"] = result.get("suggested_loc")
        df.at[idx, "codex_suggested_scale"] = result.get("suggested_scale")
        df.at[idx, "codex_suggested_minimum"] = result.get("suggested_minimum")
        df.at[idx, "codex_suggested_maximum"] = result.get("suggested_maximum")
        df.at[idx, "codex_batch_error"] = batch_error


def run_optional_codex_validation(
    out_df: pd.DataFrame,
    *,
    use_codex: bool,
    codex_exe: Optional[str],
    codex_review_scope: str,
    codex_batch_size: int,
    codex_max_rows: Optional[int],
    codex_model: str,
    codex_timeout: int,
    codex_fail_fast: bool,
    print_every_seconds: float,
    prefer_codex_cli: bool = False,
    api_models: Optional[Sequence[str]] = None,
    api_timeout: int = DEFAULT_API_TIMEOUT,
    enable_web_review: bool = DEFAULT_ENABLE_WEB_REVIEW,
    web_search_context: str = DEFAULT_WEB_SEARCH_CONTEXT,
) -> pd.DataFrame:
    out_df = initialize_codex_columns(out_df)

    if not use_codex:
        print("Model validation disabled.", flush=True)
        return out_df

    if api_models is None:
        api_models = DEFAULT_API_MODELS

    if prefer_codex_cli and not codex_cli_available(codex_exe):
        print("WARNING: Codex CLI requested but executable not found. Falling back to API only.", flush=True)

    if not prefer_codex_cli and not openai_api_available():
        msg = (
            "Model validation skipped: OpenAI API not available. "
            "Install `openai` and set OPENAI_API_KEY, or use --prefer-codex-cli with a working Codex CLI."
        )
        print(msg, flush=True)
        out_df["codex_batch_error"] = msg
        return out_df

    target_indices = [idx for idx, row in out_df.iterrows() if is_row_for_codex(row, codex_review_scope)]

    if codex_max_rows is not None:
        target_indices = target_indices[: max(0, int(codex_max_rows))]

    if not target_indices:
        print("Validation skipped: no rows selected for review.", flush=True)
        return out_df

    print(
        f"Starting model validation for {len(target_indices)} rows "
        f"(scope={codex_review_scope}, batch_size={codex_batch_size}, "
        f"prefer_codex_cli={prefer_codex_cli}, codex_exe={codex_exe}, api_models={list(api_models)})...",
        flush=True,
    )

    t0 = time.time()
    last_print = t0
    done = 0
    batch_size = max(1, codex_batch_size)

    for start in range(0, len(target_indices), batch_size):
        batch_indices = target_indices[start:start + batch_size]
        batch_payload = [build_codex_payload_for_row(out_df.loc[idx], idx) for idx in batch_indices]
        batch_payload_by_idx = {p["row_index"]: p for p in batch_payload}

        try:
            results_by_idx = run_llm_batch_with_fallback(
                batch_payload,
                prefer_codex_cli=prefer_codex_cli,
                codex_exe=codex_exe,
                codex_model=codex_model,
                codex_timeout=codex_timeout,
                api_models=api_models,
                api_timeout=api_timeout,
                enable_web_review=enable_web_review,
                web_search_context=web_search_context,
            )

            apply_codex_results(
                out_df,
                results_by_idx,
                batch_payload_by_idx=batch_payload_by_idx,
                batch_error=None,
            )

            missing = [idx for idx in batch_indices if idx not in results_by_idx]
            for idx in missing:
                fallback_result = coerce_result_to_complete_suggestions(
                    batch_payload_by_idx[idx],
                    {
                        "row_index": idx,
                        "realistic": None,
                        "verdict": "possibly_realistic_but_uncertain",
                        "confidence": "low",
                        "main_reason": "Model returned no explicit result; final recommendation completed from available evidence.",
                        "parameter_assessment": "Completed deterministically from evidence hierarchy and fallback parameter rules.",
                    },
                )
                apply_codex_results(
                    out_df,
                    {idx: fallback_result},
                    batch_payload_by_idx=batch_payload_by_idx,
                    batch_error="Model returned no result for this row; completed with deterministic fallback.",
                )

        except Exception as e:
            err = sanitize_text_for_csv(str(e))[:2000]
            for idx in batch_indices:
                out_df.at[idx, "codex_batch_error"] = err

            if "insufficient_quota" in err or "Error code: 429" in err:
                remaining = [
                    idx for idx in target_indices
                    if idx not in batch_indices and pd.isna(out_df.at[idx, "codex_batch_error"])
                ]
                for idx in remaining:
                    out_df.at[idx, "codex_batch_error"] = "Codex skipped after quota failure: " + err
                print("\nStopping Codex validation due to API quota exhaustion.", flush=True)
                break

            if codex_fail_fast:
                raise

        done += len(batch_indices)
        now = time.time()
        if (now - last_print >= print_every_seconds) or (done == len(target_indices)):
            elapsed = now - t0
            rate = elapsed / max(done, 1)
            eta = (len(target_indices) - done) * rate
            pct = 100.0 * done / max(len(target_indices), 1)
            msg = (
                f"[Model review {done}/{len(target_indices)} | {pct:5.1f}%] "
                f"elapsed={fmt_time(elapsed)} avg={rate:.3f}s/row ETA={fmt_time(eta)}"
            )
            print("\r" + msg.ljust(130), end="", flush=True)
            last_print = now

    print()
    return out_df


# ----------------------------
# 6) Runner
# ----------------------------
def run(
    csv_path: str,
    out_path: str,
    temporal_tag: str = "stock_asset",
    *,
    lt_data_dir: str = DEFAULT_LT_DATA_DIR,
    group_defaults_xlsx: str = DEFAULT_GROUP_DEFAULTS_XLSX,
    context_chars: int = 120,
    min_match_score: float = 95.0,
    min_ref_product_score: float = 70.0,
    neighbor_top_k: int = 40,
    neighbor_min_score: float = 88.0,
    print_every_seconds: float = 2.0,
    use_codex: bool = False,
    codex_exe: Optional[str] = None,
    codex_review_scope: str = "flagged",
    codex_batch_size: int = 10,
    codex_max_rows: Optional[int] = None,
    codex_model: str = DEFAULT_CODEX_MODEL,
    codex_timeout: int = 300,
    codex_fail_fast: bool = False,
    prefer_codex_cli: bool = False,
    api_models: Optional[Sequence[str]] = None,
    api_timeout: int = DEFAULT_API_TIMEOUT,
    enable_web_review: bool = DEFAULT_ENABLE_WEB_REVIEW,
    web_search_context: str = DEFAULT_WEB_SEARCH_CONTEXT,
) -> pd.DataFrame:
    csv_path_p = Path(csv_path)
    if not csv_path_p.exists():
        raise FileNotFoundError(csv_path)

    if csv_path_p.suffix.lower() in [".xlsx", ".xls", ".xlsm"]:
        df = pd.read_excel(csv_path_p)
    else:
        try:
            df = pd.read_csv(csv_path_p, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path_p, encoding="latin1")

    sub = df[df["temporal_tag"].astype(str).str.strip() == temporal_tag].copy()
    if sub.empty:
        raise ValueError(f"No rows with temporal_tag == {temporal_tag!r}")

    t_phase = time.time()
    acts = load_ecoinvent_index(BW_PROJECT, EI_DB_NAME)
    act_strs = act_index_strings(acts)
    print(f"Loaded {len(acts):,} ecoinvent activities in {fmt_time(time.time() - t_phase)}", flush=True)

    t_phase = time.time()
    direct_lt = build_direct_lifetime_table(acts, context_chars=context_chars)
    print(f"Extracted {len(direct_lt):,} direct lifetimes (incl. weak) in {fmt_time(time.time() - t_phase)}", flush=True)

    direct_lt_strong = direct_lt[direct_lt["is_weak"] == False].copy()
    print(f"Using {len(direct_lt_strong):,} STRONG lifetimes for neighbor inference", flush=True)

    t_phase = time.time()
    iedc_recs = load_iedc_lifetime_records(lt_data_dir)
    iedc_strs = build_iedc_index_strings(iedc_recs)
    print(f"Loaded {len(iedc_recs):,} IEDC local lifetime records from {lt_data_dir} in {fmt_time(time.time() - t_phase)}", flush=True)

    t_phase = time.time()
    group_defaults, keyword_recs = load_group_defaults(group_defaults_xlsx)
    print(
        f"Loaded {len(group_defaults):,} group defaults and {len(keyword_recs):,} keyword mappings "
        f"from {group_defaults_xlsx} in {fmt_time(time.time() - t_phase)}",
        flush=True,
    )

    report_rows: List[Dict[str, Any]] = []
    n = len(sub)
    t0 = time.time()
    last_print = t0
    print_every = max(1, min(50, n // 50 if n > 0 else 1))

    for i, (_, r) in enumerate(sub.iterrows(), start=1):
        flags: List[str] = []

        trails_lt = to_float(r.get("lifetime"))
        dist_type = to_float(r.get("age distribution type"))
        dist_type_i = int(dist_type) if dist_type is not None else None
        loc = to_float(r.get("loc"))
        scale = to_float(r.get("scale"))
        vmin = to_float(r.get("minimum"))
        vmax = to_float(r.get("maximum"))

        act, score = best_match_activity(
            r,
            acts,
            act_strs,
            min_score=min_match_score,
            min_ref_product_score=min_ref_product_score,
        )

        evidence: Dict[str, Any] = {}
        weak_lifetime_hit = False

        if act is not None:
            haystack = " ".join([act.comment, act.name, act.ref_product])
            direct, pat_used, snippet, is_weak = extract_lifetime_with_evidence(haystack, context_chars=context_chars)

            if direct is not None:
                evidence["mode"] = "DIRECT_FROM_MATCH"
                evidence["direct_years"] = direct
                evidence["direct_pattern"] = pat_used
                evidence["direct_snippet"] = snippet
                evidence["direct_is_weak"] = bool(is_weak)
                if is_weak:
                    weak_lifetime_hit = True
                    flags.append("WEAK_LIFETIME_MATCH_ANY_YEAR_TOKEN")
            else:
                inf = infer_lifetime_from_neighbors(
                    act,
                    acts,
                    act_strs,
                    direct_lt_strong,
                    top_k=neighbor_top_k,
                    min_score=neighbor_min_score,
                )
                if inf:
                    evidence["mode"] = "INFERRED_FROM_NEIGHBORS"
                    evidence.update(inf)
                else:
                    evidence["mode"] = "NO_ECOINVENT_LIFETIME_FOUND"
        else:
            evidence["mode"] = "NO_MATCH"

        ei_band = None
        if evidence.get("direct_years") is not None:
            ei_band = evidence_band_from_direct(float(evidence["direct_years"]))
        elif evidence.get("inferred_q25") is not None and evidence.get("inferred_q75") is not None:
            ei_band = evidence_band_from_iqr(float(evidence["inferred_q25"]), float(evidence["inferred_q75"]))

        if trails_lt is None or not np.isfinite(trails_lt):
            ei_status = "TRAILS_LIFETIME_INVALID"
        elif ei_band is None:
            ei_status = "NO_EVIDENCE"
        else:
            ei_status = compare_against_band(trails_lt, ei_band)

        iedc = fetch_iedc_evidence_for_row(r, iedc_recs, iedc_strs)
        iedc_vals = iedc["iedc_selected_values_years"]
        iedc_mean = iedc["iedc_lifetime_mean_years"]
        iedc_med = iedc["iedc_lifetime_median_years"]
        iedc_q25 = iedc["iedc_lifetime_q25_years"]
        iedc_q75 = iedc["iedc_lifetime_q75_years"]
        iedc_band = iedc["iedc_band"]

        if trails_lt is None or not np.isfinite(trails_lt):
            iedc_status = "TRAILS_LIFETIME_INVALID"
        elif iedc_band is not None:
            iedc_status = compare_against_band(trails_lt, iedc_band)
        else:
            iedc_status = "NO_EVIDENCE"

        group_name, group_match_basis = detect_group_for_row(r, group_defaults, keyword_recs)
        group_rec = group_defaults.get(group_name) if group_name else None

        if group_rec and trails_lt is not None and np.isfinite(trails_lt) and group_rec.lifetime is not None and group_rec.lifetime > 0:
            group_band = (0.5 * float(group_rec.lifetime), 1.5 * float(group_rec.lifetime))
            group_lifetime_status = compare_against_band(trails_lt, group_band)
        else:
            group_band = None
            group_lifetime_status = "NO_EVIDENCE"

        prelim = build_group_preliminary_parameters(
            r,
            group_rec,
            ecoinvent_direct=safe_float_or_none(evidence.get("direct_years")),
            ecoinvent_inferred=safe_float_or_none(evidence.get("inferred_median")),
            iedc_median=iedc_med,
        )

        if evidence.get("direct_years") is not None:
            lifetime_ref = float(evidence["direct_years"])
        elif evidence.get("inferred_median") is not None:
            lifetime_ref = float(evidence["inferred_median"])
        elif iedc_med is not None:
            lifetime_ref = float(iedc_med)
        elif group_rec and group_rec.lifetime is not None:
            lifetime_ref = float(group_rec.lifetime)
        else:
            lifetime_ref = trails_lt

        dist_flags = distribution_flags(lifetime_ref, dist_type_i, loc, scale, vmin, vmax)
        flags.extend(dist_flags)

        if score is not None and score < 90.0:
            flags.append(f"LOW_MATCH_SCORE_{score:.1f}")

        has_any_numeric = (ei_band is not None) or bool(iedc_vals) or (group_rec is not None and group_rec.lifetime is not None)

        needs_check, reasons = decide_needs_check(
            match_score=score,
            ei_status=ei_status,
            iedc_status=iedc_status,
            dist_flags=dist_flags,
            weak_lifetime_hit=weak_lifetime_hit,
            has_any_numeric_evidence=has_any_numeric,
        )

        neighbor_examples = sanitize_text_for_csv(format_examples(evidence["examples"])) if evidence.get("examples") else ""

        report_rows.append(
            {
                "name": r.get("name", ""),
                "reference product": r.get("reference product", ""),
                "ISIC rev.4 ecoinvent": r.get("ISIC rev.4 ecoinvent", ""),
                "CPC": r.get("CPC", ""),
                "EcoSpold01Categories": r.get("EcoSpold01Categories", ""),
                "temporal_tag": r.get("temporal_tag", ""),
                "tag_confidence": r.get("tag_confidence", ""),
                "tag_notes": r.get("tag_notes", ""),
                "lifetime": r.get("lifetime", ""),
                "age distribution type": r.get("age distribution type", ""),
                "age distribution description": temporal_distribution_type_full_text(dist_type_i),
                "loc": r.get("loc", ""),
                "scale": r.get("scale", ""),
                "minimum": r.get("minimum", ""),
                "maximum": r.get("maximum", ""),
                "param_confidence": r.get("param_confidence", ""),
                "param_notes": r.get("param_notes", ""),
                "match_score": score,
                "matched_ei_key": act.key if act else None,
                "matched_ei_name": act.name if act else None,
                "matched_ei_ref_product": act.ref_product if act else None,
                "matched_ei_location": act.location if act else None,
                "matched_ei_unit": act.unit if act else None,
                "matched_ei_category_path": act.category_path if act else None,
                "ecoinvent_evidence_mode": evidence.get("mode"),
                "ecoinvent_direct_lifetime_years": evidence.get("direct_years"),
                "ecoinvent_direct_pattern": evidence.get("direct_pattern"),
                "ecoinvent_direct_is_weak": evidence.get("direct_is_weak"),
                "ecoinvent_direct_evidence_snippet": sanitize_text_for_csv(evidence.get("direct_snippet")),
                "ecoinvent_inferred_median_years": evidence.get("inferred_median"),
                "ecoinvent_inferred_n": evidence.get("n_neighbors"),
                "ecoinvent_neighbor_examples": sanitize_text_for_csv(neighbor_examples),
                "ecoinvent_lifetime_status": ei_status,
                "iedc_numeric_sources": iedc["iedc_selected_sources"],
                "iedc_all_numeric_sources": iedc["iedc_sources"],
                "iedc_lifetime_values_years": ",".join([f"{v:.4g}" for v in iedc_vals]) if iedc_vals else "",
                "iedc_all_lifetime_values_years": ",".join([f"{v:.4g}" for v in iedc["iedc_values_years"]]) if iedc["iedc_values_years"] else "",
                "iedc_lifetime_status": iedc_status,
                "iedc_lifetime_mean_years": iedc_mean,
                "iedc_lifetime_median_years": iedc_med,
                "iedc_lifetime_q25_years": iedc_q25,
                "iedc_lifetime_q75_years": iedc_q75,
                "iedc_match_score": iedc["iedc_match_score"],
                "iedc_examples": sanitize_text_for_csv(iedc["iedc_examples"]),
                "iedc_notes": sanitize_text_for_csv(iedc["iedc_notes"]),
                "iedc_selection_mode": iedc["iedc_selection_mode"],
                "group_name": group_name,
                "group_match_basis": group_match_basis,
                "group_lifetime": (group_rec.lifetime if group_rec else None),
                "group_age_distribution_type": (group_rec.age_distribution_type if group_rec else None),
                "group_age_distribution_description": (group_rec.age_distribution_description if group_rec else ""),
                "group_loc": (group_rec.loc if group_rec else None),
                "group_scale": (group_rec.scale if group_rec else None),
                "group_minimum": (group_rec.minimum if group_rec else None),
                "group_maximum": (group_rec.maximum if group_rec else None),
                "group_param_confidence": (group_rec.param_confidence if group_rec else ""),
                "group_basis_type": (group_rec.basis_type if group_rec else ""),
                "group_confidence": (group_rec.confidence if group_rec else ""),
                "group_source_short": (group_rec.source_short if group_rec else ""),
                "group_lifetime_status": group_lifetime_status,
                "group_lifetime_band_min": (group_band[0] if group_band else None),
                "group_lifetime_band_max": (group_band[1] if group_band else None),
                "preliminary_lifetime": prelim["preliminary_lifetime"],
                "preliminary_distribution_type": prelim["preliminary_distribution_type"],
                "preliminary_age_distribution_description": prelim["preliminary_age_distribution_description"],
                "preliminary_loc": prelim["preliminary_loc"],
                "preliminary_scale": prelim["preliminary_scale"],
                "preliminary_minimum": prelim["preliminary_minimum"],
                "preliminary_maximum": prelim["preliminary_maximum"],
                "preliminary_lifetime_basis": prelim["preliminary_lifetime_basis"],
                "preliminary_distribution_basis": prelim["preliminary_distribution_basis"],
                "distribution_lifetime_ref_used": lifetime_ref,
                "flags": ";".join(sorted(set(flags))) if flags else "",
                "needs_check": bool(needs_check),
                "needs_check_reasons": reasons,
            }
        )

        now = time.time()
        do_print = (i % print_every == 0) or (now - last_print >= print_every_seconds) or (i == n)
        if do_print:
            elapsed = now - t0
            rate = elapsed / i
            eta = (n - i) * rate
            pct = 100.0 * i / n
            msg = f"[{i}/{n} | {pct:5.1f}%] elapsed={fmt_time(elapsed)} avg={rate:.3f}s/row ETA={fmt_time(eta)}"
            print("\r" + msg.ljust(140), end="", flush=True)
            last_print = now

    print()

    out_df = pd.DataFrame(report_rows)

    out_df = run_optional_codex_validation(
        out_df,
        use_codex=use_codex,
        codex_exe=codex_exe,
        codex_review_scope=codex_review_scope,
        codex_batch_size=codex_batch_size,
        codex_max_rows=codex_max_rows,
        codex_model=codex_model,
        codex_timeout=codex_timeout,
        codex_fail_fast=codex_fail_fast,
        print_every_seconds=print_every_seconds,
        prefer_codex_cli=prefer_codex_cli,
        api_models=api_models,
        api_timeout=api_timeout,
        enable_web_review=enable_web_review,
        web_search_context=web_search_context,
    )

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    out_df_export = sanitize_dataframe_for_csv(out_df)
    out_df_export.to_csv(
        out_path,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
    )
    return out_df_export


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--csv", default=DEFAULT_CSV)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--tag", default="stock_asset")
    ap.add_argument("--lt-data-dir", default=DEFAULT_LT_DATA_DIR, help="Directory containing local IEDC lifetime Excel files.")
    ap.add_argument(
        "--group-defaults-xlsx",
        default=DEFAULT_GROUP_DEFAULTS_XLSX,
        help="Excel workbook with sheets Group_Assumptions and Keyword_Mapping for grouped preliminary temporal defaults.",
    )

    ap.add_argument("--context-chars", type=int, default=120)
    ap.add_argument("--min-match-score", type=float, default=95.0)
    ap.add_argument("--min-ref-product-score", type=float, default=70.0)
    ap.add_argument("--neighbor-top-k", type=int, default=40)
    ap.add_argument("--neighbor-min-score", type=float, default=88.0)
    ap.add_argument("--print-every-seconds", type=float, default=2.0)

    ap.add_argument("--use-codex", action="store_true", help="Enable model review stage.")
    ap.add_argument(
        "--codex-exe",
        default=None,
        help="Explicit path to Codex executable, e.g. C:\\Users\\...\\codex.cmd",
    )
    ap.add_argument(
        "--prefer-codex-cli",
        action="store_true",
        help="Try Codex CLI first, then fall back to the OpenAI API.",
    )
    ap.add_argument(
        "--codex-review-scope",
        choices=["flagged", "all"],
        default="flagged",
        help="Send only flagged rows to the model, or every row.",
    )
    ap.add_argument(
        "--codex-batch-size",
        type=int,
        default=10,
        help="Number of rows per model batch.",
    )
    ap.add_argument(
        "--codex-max-rows",
        type=int,
        default=None,
        help="Maximum number of rows to send to the model after applying the review scope.",
    )
    ap.add_argument(
        "--codex-model",
        default=DEFAULT_CODEX_MODEL,
        help="Codex CLI model to use via codex exec --model.",
    )
    ap.add_argument(
        "--codex-timeout",
        type=int,
        default=300,
        help="Timeout in seconds per Codex CLI batch.",
    )
    ap.add_argument(
        "--codex-fail-fast",
        action="store_true",
        help="Stop immediately if one model batch fails.",
    )
    ap.add_argument(
        "--api-models",
        default="gpt-5.4,gpt-5-mini",
        help="Comma-separated fallback OpenAI API models.",
    )
    ap.add_argument(
        "--api-timeout",
        type=int,
        default=DEFAULT_API_TIMEOUT,
        help="Timeout in seconds for OpenAI API requests.",
    )
    ap.add_argument(
        "--enable-web-review",
        action="store_true",
        help="Enable web search during OpenAI API review so the model can check literature online.",
    )
    ap.add_argument(
        "--web-search-context",
        choices=["low", "medium", "high"],
        default=DEFAULT_WEB_SEARCH_CONTEXT,
        help="Web search context size for API-based review.",
    )

    args, _ = ap.parse_known_args()

    if OpenAI is None:
        print("WARNING: openai SDK not installed -> API fallback unavailable. pip install openai", flush=True)

    if not Path(args.group_defaults_xlsx).exists():
        print(
            f"WARNING: group defaults workbook not found: {args.group_defaults_xlsx}. "
            "Group-based preliminary defaults will be empty.",
            flush=True,
        )

    codex_exe = resolve_codex_executable(args.codex_exe)
    api_models = [m.strip() for m in str(args.api_models).split(",") if m.strip()]

    if args.use_codex:
        if args.prefer_codex_cli:
            if codex_exe is None:
                print("WARNING: Codex executable not found. Will rely on API fallback only.", flush=True)
            else:
                print(f"Resolved Codex executable: {codex_exe}", flush=True)

        if openai_api_available():
            print(f"OpenAI API available. Fallback models: {api_models}", flush=True)
        else:
            print("WARNING: OPENAI_API_KEY not set or openai SDK missing -> API fallback unavailable.", flush=True)

    df = run(
        args.csv,
        args.out,
        temporal_tag=args.tag,
        lt_data_dir=args.lt_data_dir,
        group_defaults_xlsx=args.group_defaults_xlsx,
        context_chars=args.context_chars,
        min_match_score=args.min_match_score,
        min_ref_product_score=args.min_ref_product_score,
        neighbor_top_k=args.neighbor_top_k,
        neighbor_min_score=args.neighbor_min_score,
        print_every_seconds=args.print_every_seconds,
        use_codex=args.use_codex,
        codex_exe=codex_exe,
        codex_review_scope=args.codex_review_scope,
        codex_batch_size=args.codex_batch_size,
        codex_max_rows=args.codex_max_rows,
        codex_model=args.codex_model,
        codex_timeout=args.codex_timeout,
        codex_fail_fast=args.codex_fail_fast,
        prefer_codex_cli=args.prefer_codex_cli,
        api_models=api_models,
        api_timeout=args.api_timeout,
        enable_web_review=args.enable_web_review,
        web_search_context=args.web_search_context,
    )

    print(f"Wrote: {args.out} ({len(df)} rows)", flush=True)


if __name__ == "__main__":
    main()