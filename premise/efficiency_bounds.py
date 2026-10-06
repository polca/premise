"""Explicit, auditable absolute efficiency bounds for external scenarios only.

Bounds do not require electricity or heat production series. Optional output
data support a separate diagnostic, never a correction to the target. The caller
defines the efficiency basis; bounds use the same units as the target.
"""

import math


def validate_policy(policy):
    """Validate explicit limits; never infer a technology or a default range."""
    bounds = policy.get("bounds")
    if bounds is not None:
        if not isinstance(bounds, dict) or set(bounds) != {"min", "max"}:
            raise ValueError("Efficiency bounds must contain min and max")
        lower, upper = bounds["min"], bounds["max"]
        if (
            isinstance(lower, bool)
            or isinstance(upper, bool)
            or not all(
                isinstance(v, (int, float)) and math.isfinite(v) for v in (lower, upper)
            )
            or not 0 < lower <= upper
        ):
            raise ValueError("Efficiency bounds require finite 0 < min <= max")


def output_data(scenario_data, year, electricity_pathway, heat_pathway):
    """Fetch optional annual energy outputs, retaining missing-data information."""
    if not heat_pathway:
        return {}
    data = scenario_data.get("production volume")
    if data is None:
        return {}
    records = {}
    units = data.attrs.get("unit", {})
    for region in scenario_data.get("regions", data.region.values.tolist()):
        record = {}
        for kind, pathway in (
            ("electricity", electricity_pathway),
            ("heat", heat_pathway),
        ):
            if pathway is None or pathway not in data.variables.values:
                continue
            series = data.sel(region=region, variables=pathway)
            if year in series.year.values:
                value = float(series.sel(year=year))
            elif min(series.year.values) <= year <= max(series.year.values):
                value = float(series.interp(year=year))
            else:
                continue
            record[kind] = value
            record[kind + " unit"] = units.get(pathway)
        records[region] = record
    return records


def combined_efficiency_check(requested, applied, outputs=None):
    """Use E/H only when provided in convertible annual energy units.

    This infers (E+H)/F from the electrical target E/F and H/E; it is not an
    independent verification of F or the scenario's heating-value convention.
    """
    outputs = outputs or {}
    factors = {
        "EJ": 1e12,
        "PJ": 1e9,
        "TJ": 1e6,
        "GJ": 1e3,
        "MJ": 1,
        "TWh": 3.6e9,
        "GWh": 3.6e6,
        "MWh": 3.6e3,
        "kWh": 3.6,
    }
    converted = {}
    for kind in ("electricity", "heat"):
        if kind not in outputs or not math.isfinite(outputs[kind]):
            return {"status": "not_available", "reason": f"missing {kind} output"}
        unit = outputs.get(kind + " unit")
        # Require an explicit annual basis on both outputs.
        if not isinstance(unit, str) or not unit.endswith(("/yr", "/yr.", "/year")):
            return {
                "status": "not_available",
                "reason": f"unsupported {kind} unit: {unit}",
            }
        factor = factors.get(unit.split("/")[0].strip())
        if factor is None:
            return {
                "status": "not_available",
                "reason": f"unsupported {kind} unit: {unit}",
            }
        converted[kind] = outputs[kind] * factor
    if converted["electricity"] <= 0 or converted["heat"] < 0:
        return {
            "status": "not_available",
            "reason": "nonpositive electricity or negative heat",
        }
    ratio = 1 + converted["heat"] / converted["electricity"]
    return {
        "status": "checked",
        "basis": "electrical target multiplied by (1 + heat/electricity); source heating-value basis",
        "requested_combined_efficiency": requested * ratio,
        "applied_combined_efficiency": applied * ratio,
        "requested_above_100_percent": requested * ratio > 1 + 1e-10,
        "applied_above_100_percent": applied * ratio > 1 + 1e-10,
        "action": "diagnostic only; heating-value basis requires interpretation",
    }


def bound_external_efficiency(dataset, target, policy=None, outputs=None):
    """Apply only caller-supplied limits to an absolute external target.

    The datapackage author chooses the eligible pathways, efficiency basis and
    limits. No inventory-name, technology, scenario or product inference occurs.
    Zero remains a missing/inactive target. Optional E/H outputs are diagnostic.
    """
    policy = policy or {}
    validate_policy(policy)
    target = float(target)
    if not math.isfinite(target) or target < 0:
        raise ValueError(f"Invalid external efficiency: {target}")
    bounds = policy.get("bounds")
    if target == 0:
        applied, status = target, "missing_or_inactive_target"
    elif bounds is None:
        applied, status = target, "not_configured"
    else:
        applied = min(max(target, bounds["min"]), bounds["max"])
        status = "clipped" if applied != target else "within_bounds"
    record = {
        "requested_efficiency": target,
        "applied_efficiency": applied,
        "bounds": dict(bounds) if bounds is not None else None,
        "clipped": applied != target,
        "status": status,
        "combined_efficiency_check": combined_efficiency_check(
            target, applied, outputs
        ),
    }
    dataset.setdefault("efficiency bounds audit", []).append(record)
    if record["clipped"]:
        dataset["comment"] = dataset.get("comment", "") + (
            f" External-scenario efficiency bounded: requested {target:.12g}, "
            f"applied {applied:.12g}, configured bounds {bounds}. "
            "This modelling assumption does not modify the source scenario data."
        )
    return applied, record
