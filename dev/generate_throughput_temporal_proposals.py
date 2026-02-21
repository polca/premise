#!/usr/bin/env python3
"""Generate reasoned temporal-parameter proposals for throughput_process rows.

This script reads the first worksheet of the temporal-params Excel file directly
(via XLSX XML), infers missing temporal parameters for rows tagged
`throughput_process`, and writes a CSV proposal table with:
- inferred lifetime and average age
- distribution parameters consistent with the chosen distribution ID
- confidence level and rationale
- validation flags / repairs applied

It does not modify the Excel file.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import statistics
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile

NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

FIELDS = [
    "lifetime",
    "age distribution type",
    "loc",
    "scale",
    "minimum",
    "maximum",
]


@dataclass
class Archetype:
    name: str
    pattern: re.Pattern
    lifetime_years: float
    dist_id: int
    mean_age_share: float
    min_share: float
    max_share: float
    sigma_share: Optional[float] = None


ARCHETYPES: List[Archetype] = [
    Archetype(
        name="road_vehicle_operation",
        pattern=re.compile(r"\b(passenger car|heavy[ -]?duty vehicle|truck)\b"),
        lifetime_years=15.0,
        dist_id=5,
        mean_age_share=0.60,
        min_share=1.00,
        max_share=0.07,
    ),
    Archetype(
        name="bus_operation",
        pattern=re.compile(r"\b(single deck coach|urban bus|trolleybus|bus)\b"),
        lifetime_years=20.0,
        dist_id=5,
        mean_age_share=0.60,
        min_share=1.00,
        max_share=0.07,
    ),
    Archetype(
        name="rail_operation",
        pattern=re.compile(r"\b(passenger train|freight train|train)\b"),
        lifetime_years=40.0,
        dist_id=5,
        mean_age_share=0.55,
        min_share=1.00,
        max_share=0.05,
    ),
    Archetype(
        name="marine_operation",
        pattern=re.compile(r"\b(container ship|ship)\b"),
        lifetime_years=25.0,
        dist_id=5,
        mean_age_share=0.55,
        min_share=1.00,
        max_share=0.05,
    ),
    Archetype(
        name="tram_operation",
        pattern=re.compile(r"\btram\b"),
        lifetime_years=35.0,
        dist_id=5,
        mean_age_share=0.55,
        min_share=1.00,
        max_share=0.05,
    ),
    Archetype(
        name="aircraft_operation",
        pattern=re.compile(r"\b(aircraft|aviation)\b"),
        lifetime_years=20.0,
        dist_id=5,
        mean_age_share=0.55,
        min_share=1.00,
        max_share=0.07,
    ),
    Archetype(
        name="grid_asset_operation",
        pattern=re.compile(r"\b(voltage transformation|transmission|distribution)\b"),
        lifetime_years=30.0,
        dist_id=5,
        mean_age_share=0.55,
        min_share=1.00,
        max_share=0.05,
    ),
    Archetype(
        name="power_plant_operation",
        pattern=re.compile(r"\b(heat and power co-generation|electricity production|power plant)\b"),
        lifetime_years=40.0,
        dist_id=5,
        mean_age_share=0.55,
        min_share=1.00,
        max_share=0.05,
    ),
    Archetype(
        name="building_heat_asset_operation",
        pattern=re.compile(r"\b(heat pump|boiler|stove|furnace)\b"),
        lifetime_years=15.0,
        dist_id=5,
        mean_age_share=0.60,
        min_share=1.00,
        max_share=0.07,
    ),
    Archetype(
        name="desktop_ict_operation",
        pattern=re.compile(r"\b(desktop|cathode ray tube|liquid crystal display)\b"),
        lifetime_years=6.0,
        dist_id=3,
        mean_age_share=0.50,
        min_share=1.00,
        max_share=0.10,
        sigma_share=0.20,
    ),
    Archetype(
        name="laptop_ict_operation",
        pattern=re.compile(r"\blaptop\b"),
        lifetime_years=4.0,
        dist_id=3,
        mean_age_share=0.50,
        min_share=1.00,
        max_share=0.10,
        sigma_share=0.20,
    ),
    Archetype(
        name="internet_access_operation",
        pattern=re.compile(r"\b(internet access equipment|internet access)\b"),
        lifetime_years=5.0,
        dist_id=3,
        mean_age_share=0.50,
        min_share=1.00,
        max_share=0.10,
        sigma_share=0.20,
    ),
    Archetype(
        name="fuel_throughput_consumption",
        pattern=re.compile(r"\b(burned|combustion|used in a fuel cell|burning)\b"),
        lifetime_years=1.0,
        dist_id=5,
        mean_age_share=0.70,
        min_share=1.00,
        max_share=0.10,
    ),
]


def _col_idx(cell_ref: str) -> int:
    m = re.match(r"([A-Z]+)", cell_ref)
    if not m:
        return 0
    s = m.group(1)
    v = 0
    for ch in s:
        v = v * 26 + (ord(ch) - 64)
    return v - 1


def _to_float(v) -> Optional[float]:
    try:
        return float(str(v).strip())
    except Exception:
        return None


def _to_int(v) -> Optional[int]:
    f = _to_float(v)
    return int(round(f)) if f is not None else None


def _fmt(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{v:.12g}"


def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def _cpc_code(cpc: str) -> str:
    return (cpc or "").split(":")[0].strip()


def load_sheet1_rows(xlsx_path: Path) -> List[Dict[str, str]]:
    with ZipFile(xlsx_path) as z:
        shared_strings = []
        if "xl/sharedStrings.xml" in z.namelist():
            sst_root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in sst_root.findall("a:si", NS):
                shared_strings.append(
                    "".join(t.text or "" for t in si.findall(".//a:t", NS))
                )

        sh = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
        rows: List[List[str]] = []

        for r in sh.findall(".//a:sheetData/a:row", NS):
            vals: Dict[int, str] = {}
            for c in r.findall("a:c", NS):
                i = _col_idx(c.attrib.get("r", "A1"))
                t = c.attrib.get("t")
                v = c.find("a:v", NS)
                val = ""
                if v is not None:
                    raw = v.text or ""
                    if t == "s" and raw.isdigit() and int(raw) < len(shared_strings):
                        val = shared_strings[int(raw)]
                    else:
                        val = raw
                vals[i] = val

            max_i = max(vals) if vals else -1
            arr = [""] * (max_i + 1)
            for i, vv in vals.items():
                arr[i] = vv
            rows.append(arr)

    header = rows[0]
    out: List[Dict[str, str]] = []
    for ridx, rr in enumerate(rows[1:], start=2):
        rr = rr + [""] * (len(header) - len(rr))
        d = {header[i]: rr[i] for i in range(len(header))}
        d["_row"] = str(ridx)
        out.append(d)
    return out


def implied_mean_age(dist_id: Optional[int], loc, scale, mn, mx) -> Optional[float]:
    if dist_id is None:
        return None
    if dist_id == 2:
        if loc is None or scale is None:
            return None
        return math.exp(loc + 0.5 * scale * scale)
    if dist_id == 3:
        return -loc if loc is not None else None
    if dist_id == 4:
        if mn is None or mx is None:
            return None
        return -((mn + mx) / 2.0)
    if dist_id == 5:
        if loc is None or mn is None or mx is None:
            return None
        return -((mn + mx + loc) / 3.0)
    return None


def choose_archetype(name: str) -> Optional[Archetype]:
    n = _norm(name)
    for a in ARCHETYPES:
        if a.pattern.search(n):
            return a
    return None


def build_priors(rows: List[Dict[str, str]]):
    known = []
    for r in rows:
        if (r.get("temporal_tag") or "").strip() != "throughput_process":
            continue
        lt = _to_float(r.get("lifetime"))
        dt = _to_int(r.get("age distribution type"))
        loc = _to_float(r.get("loc"))
        sc = _to_float(r.get("scale"))
        mn = _to_float(r.get("minimum"))
        mx = _to_float(r.get("maximum"))
        if lt is None or dt is None:
            continue
        mean_age = implied_mean_age(dt, loc, sc, mn, mx)
        if mean_age is None:
            continue
        known.append((r, lt, dt, mean_age))

    by_cpc_lifetime: Dict[str, List[float]] = defaultdict(list)
    by_cpc_dist: Dict[str, List[int]] = defaultdict(list)
    by_cpc_mean_share: Dict[str, List[float]] = defaultdict(list)

    for r, lt, dt, mean_age in known:
        code = _cpc_code(r.get("CPC", ""))
        if lt > 0:
            by_cpc_lifetime[code].append(lt)
            by_cpc_mean_share[code].append(min(max(mean_age / lt, 0.0), 1.0))
        by_cpc_dist[code].append(dt)

    median_lifetime_by_cpc = {
        c: statistics.median(v) for c, v in by_cpc_lifetime.items() if v
    }
    mode_dist_by_cpc = {
        c: Counter(v).most_common(1)[0][0] for c, v in by_cpc_dist.items() if v
    }
    median_mean_share_by_cpc = {
        c: statistics.median(v) for c, v in by_cpc_mean_share.items() if v
    }

    all_lt = [lt for _, lt, _, _ in known]
    all_dt = [dt for _, _, dt, _ in known]
    all_share = [
        min(max(mean_age / lt, 0.0), 1.0)
        for _, lt, _, mean_age in known
        if lt and lt > 0
    ]

    global_lifetime = statistics.median(all_lt) if all_lt else 15.0
    global_dist = Counter(all_dt).most_common(1)[0][0] if all_dt else 5
    global_mean_share = statistics.median(all_share) if all_share else 0.55

    return {
        "median_lifetime_by_cpc": median_lifetime_by_cpc,
        "mode_dist_by_cpc": mode_dist_by_cpc,
        "median_mean_share_by_cpc": median_mean_share_by_cpc,
        "global_lifetime": global_lifetime,
        "global_dist": global_dist,
        "global_mean_share": global_mean_share,
    }


def infer_base(
    row: Dict[str, str],
    priors,
) -> Tuple[float, int, float, str, str]:
    """Infer lifetime, dist, mean_age and return confidence+reason."""

    lt = _to_float(row.get("lifetime"))
    dt = _to_int(row.get("age distribution type"))

    if lt is not None and dt is not None:
        loc = _to_float(row.get("loc"))
        sc = _to_float(row.get("scale"))
        mn = _to_float(row.get("minimum"))
        mx = _to_float(row.get("maximum"))
        mean_age = implied_mean_age(dt, loc, sc, mn, mx)
        if mean_age is None:
            mean_age = min(max(0.5 * lt, 0.0), lt)
        return lt, dt, min(max(mean_age, 0.0), lt), "high", "existing lifetime+dist"

    arch = choose_archetype(row.get("name", ""))
    if arch is not None:
        lt = lt if lt is not None else arch.lifetime_years
        dt = dt if dt is not None else arch.dist_id
        mean_age = min(max(arch.mean_age_share * lt, 0.0), lt)
        return lt, dt, mean_age, "high", f"archetype:{arch.name}"

    code = _cpc_code(row.get("CPC", ""))
    cpc_lt = priors["median_lifetime_by_cpc"].get(code)
    cpc_dt = priors["mode_dist_by_cpc"].get(code)
    cpc_share = priors["median_mean_share_by_cpc"].get(code)

    if cpc_lt is not None or cpc_dt is not None:
        lt = lt if lt is not None else (cpc_lt if cpc_lt is not None else priors["global_lifetime"])
        dt = dt if dt is not None else (cpc_dt if cpc_dt is not None else priors["global_dist"])
        share = cpc_share if cpc_share is not None else priors["global_mean_share"]
        mean_age = min(max(share * lt, 0.0), lt)
        return lt, dt, mean_age, "medium", f"cpc:{code}"

    lt = lt if lt is not None else priors["global_lifetime"]
    dt = dt if dt is not None else priors["global_dist"]
    mean_age = min(max(priors["global_mean_share"] * lt, 0.0), lt)
    return lt, dt, mean_age, "low", "global prior"


def propose_params(row: Dict[str, str], priors) -> Dict[str, str]:
    lt, dt, mean_age, conf, base_reason = infer_base(row, priors)

    loc = _to_float(row.get("loc"))
    scale = _to_float(row.get("scale"))
    mn = _to_float(row.get("minimum"))
    mx = _to_float(row.get("maximum"))

    notes: List[str] = [f"base={base_reason}"]
    flags: List[str] = []

    # Default bounds from inferred lifetime if missing.
    if mn is None:
        mn = -lt
        notes.append("min=-lifetime")
    if mx is None:
        # Keep a non-degenerate support even for short-lived processes.
        mx = -max(0.05 * lt, 0.1)
        notes.append("max=-max(5% lifetime, 0.1y)")

    # Ensure bound order.
    if mn > mx:
        mn, mx = mx, mn
        flags.append("swapped_bounds")

    if dt == 5:
        # Triangular: set mode from target mean age and clamp in bounds.
        if loc is None:
            loc = -3.0 * mean_age - mn - mx
            notes.append("triangular mode solved from target mean age")
        if loc < mn:
            loc = mn
            flags.append("loc_clamped_min")
        if loc > mx:
            loc = mx
            flags.append("loc_clamped_max")
        scale_out = ""

    elif dt == 3:
        # Normal: loc is mean, must be inside truncation interval.
        if loc is None:
            loc = -mean_age
            notes.append("normal loc=-mean_age")
        if loc < mn:
            loc = mn
            flags.append("loc_clamped_min")
        if loc > mx:
            loc = mx
            flags.append("loc_clamped_max")
        if scale is None:
            scale = max((mx - mn) / 6.0, 1e-6)
            notes.append("normal scale=(max-min)/6")
        scale_out = _fmt(scale)

    elif dt == 2:
        # Lognormal on age: keep bounded support but with positive mean-age intent.
        if loc is None:
            # Use conservative median-age proxy.
            loc = math.log(max(mean_age, 1e-6))
            notes.append("lognormal loc=ln(mean_age) proxy")
        if scale is None:
            scale = 0.5
            notes.append("lognormal scale default=0.5")
        scale_out = _fmt(scale)

    elif dt == 4:
        # Uniform: no loc/scale needed.
        if loc is None:
            loc = ""
        scale_out = ""

    else:
        # Unknown distribution: keep what exists, use conservative static fallback.
        if dt is None:
            dt = 0
        if loc is None:
            loc = ""
        scale_out = "" if scale is None else _fmt(scale)
        conf = "low"
        flags.append("unknown_dist_fallback")

    # Final consistency check: implied mean age <= lifetime where computable.
    mean_check = implied_mean_age(
        dt,
        _to_float(loc),
        _to_float(scale_out),
        mn,
        mx,
    )
    if mean_check is not None and mean_check > lt + 1e-9:
        flags.append("mean_age_gt_lifetime")

    # Preserve existing values when present.
    proposed = {
        "proposed_lifetime": _fmt(lt),
        "proposed_age_distribution_type": str(int(dt)),
        "proposed_loc": _fmt(_to_float(loc)) if loc != "" else "",
        "proposed_scale": scale_out,
        "proposed_minimum": _fmt(mn),
        "proposed_maximum": _fmt(mx),
        "proposed_average_age": _fmt(mean_age),
        "confidence": conf,
        "reasoning": " | ".join(notes),
        "validation_flags": ";".join(flags),
    }

    # If field already filled in Excel, keep it.
    for f in FIELDS:
        cur = row.get(f, "")
        if str(cur).strip() != "":
            proposed_key = f"proposed_{f.replace(' ', '_')}"
            if f == "age distribution type":
                proposed["proposed_age_distribution_type"] = cur
            elif f == "lifetime":
                proposed["proposed_lifetime"] = cur
            elif f == "loc":
                proposed["proposed_loc"] = cur
            elif f == "scale":
                proposed["proposed_scale"] = cur
            elif f == "minimum":
                proposed["proposed_minimum"] = cur
            elif f == "maximum":
                proposed["proposed_maximum"] = cur

    # Enforce triangular semantics after preserving existing values.
    if _to_int(proposed["proposed_age_distribution_type"]) == 5:
        proposed["proposed_scale"] = ""

    return proposed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("premise/data/trails/classifications_temporal_params_copy.xlsx"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("dev/throughput_process_param_proposals_reasoned.csv"),
    )
    args = parser.parse_args()

    rows = load_sheet1_rows(args.input)
    priors = build_priors(rows)

    proposals: List[Dict[str, str]] = []

    for r in rows:
        if (r.get("temporal_tag") or "").strip() != "throughput_process":
            continue
        if not any(str(r.get(f, "")).strip() == "" for f in FIELDS):
            continue

        p = propose_params(r, priors)
        proposals.append(
            {
                "row": r["_row"],
                "name": r.get("name", ""),
                "reference product": r.get("reference product", ""),
                "CPC": r.get("CPC", ""),
                "current_lifetime": r.get("lifetime", ""),
                "current_age_distribution_type": r.get("age distribution type", ""),
                "current_loc": r.get("loc", ""),
                "current_scale": r.get("scale", ""),
                "current_minimum": r.get("minimum", ""),
                "current_maximum": r.get("maximum", ""),
                **p,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(proposals[0].keys()))
        w.writeheader()
        w.writerows(proposals)

    # Quick QA summary.
    invalid_loc_bounds = 0
    invalid_mean_vs_lifetime = 0
    for p in proposals:
        dt = _to_int(p["proposed_age_distribution_type"])
        loc = _to_float(p["proposed_loc"])
        sc = _to_float(p["proposed_scale"])
        mn = _to_float(p["proposed_minimum"])
        mx = _to_float(p["proposed_maximum"])
        lt = _to_float(p["proposed_lifetime"])

        if dt in (2, 3, 5) and None not in (loc, mn, mx):
            if not (mn <= loc <= mx):
                invalid_loc_bounds += 1

        mu = implied_mean_age(dt, loc, sc, mn, mx)
        if mu is not None and lt is not None and mu > lt + 1e-9:
            invalid_mean_vs_lifetime += 1

    print(f"Wrote {len(proposals)} proposals to {args.output}")
    print(f"invalid_loc_bounds={invalid_loc_bounds}")
    print(f"invalid_mean_vs_lifetime={invalid_mean_vs_lifetime}")


if __name__ == "__main__":
    main()
