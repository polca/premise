"""Read opt-in, precomputed cohort variables without a flodym runtime dependency.

No extrapolation, normalization, geographic fallback or automatic inventory
binding is performed. Stocks and service contributions are different arrays.
"""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re

import numpy as np

PREFIX = "Stock Vintage|v1|"


def verified_report(report_path, scenario_path):
    """Require a report bound to the exact plain or encrypted scenario file."""
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    digest = hashlib.sha256(Path(scenario_path).read_bytes()).hexdigest()
    if digest not in (
        report.get("output_sha256"),
        report.get("encrypted_output_sha256"),
    ):
        raise ValueError("Stock-vintage report does not match the IAM file")
    metadata = report.get("assumptions", {})
    if (
        metadata.get("stock_vintage_schema_version") != 1
        or metadata.get("origin") != "reconstructed_not_native"
        or metadata.get("allocation_basis") != "common_amortisation"
        or metadata.get("exchange_amount_changed") is not False
    ):
        raise ValueError("Unsupported stock-vintage report semantics")
    return report


def stock_vintage_weights(data, asset_id, region, year, *, basis="service"):
    """Read an exact annual stock/service profile from IAMDataCollection.data.

    Return calendar cohort years and shares. Missing years, zero service,
    malformed shares or missing variables raise rather than falling back to a
    parametric distribution. Future cohorts must be present with zero weight.
    """
    if not isinstance(asset_id, str) or not re.fullmatch(
        r"[a-z0-9][a-z0-9_-]*", asset_id
    ):
        raise ValueError("Invalid stock-vintage asset id")
    if basis not in ("stock", "service"):
        raise ValueError("Stock-vintage basis must be stock or service")
    if isinstance(year, bool) or not isinstance(year, (int, np.integer)):
        raise ValueError("Stock-vintage year must be an integer")
    if region not in data.region.values or year not in data.year.values:
        raise ValueError("No exact stock-vintage region/year; no fallback allowed")
    prefix = PREFIX + asset_id + "|"
    variables = list(data.variables.values)
    if len(set(variables)) != len(variables):
        raise ValueError("Duplicate IAM variables")
    units = data.attrs.get("unit", {})

    def scalar(suffix, expected_unit=None):
        variable = prefix + suffix
        if variable not in variables:
            raise ValueError(f"Missing stock-vintage variable: {variable}")
        value = float(data.sel(region=region, variables=variable, year=year))
        if not np.isfinite(value):
            raise ValueError(f"No finite stock-vintage value: {variable}/{year}")
        if expected_unit is not None and units.get(variable) != expected_unit:
            raise ValueError(f"Unexpected stock-vintage unit: {variable}")
        return value

    if scalar("Coverage", "1") != 1:
        raise ValueError("Stock-vintage year is outside declared coverage")
    family = prefix + ("Stock Share" if basis == "stock" else "Service Share")
    cohort_variables = [v for v in variables if v.startswith(family + "|Vintage|")]
    if not cohort_variables:
        raise ValueError("Stock-vintage cohort variables are absent")
    cohorts = []
    for variable in cohort_variables:
        tail = variable.removeprefix(family + "|Vintage|")
        if not re.fullmatch(r"[12][0-9]{3}", tail):
            raise ValueError("Malformed cohort calendar year")
        cohorts.append(int(tail))
        if units.get(variable) != "1":
            raise ValueError("Cohort shares must have unit 1")
    weights = data.sel(
        region=region, variables=cohort_variables, year=year
    ).values.astype(float)
    if (
        not np.isfinite(weights).all()
        or (weights < 0).any()
        or not np.isclose(weights.sum(), 1, rtol=0, atol=1e-10)
    ):
        raise ValueError("Cohort shares must be finite, nonnegative and sum to one")
    cohorts = np.asarray(cohorts)
    if ((cohorts > year) & (weights != 0)).any():
        raise ValueError("Positive stock-vintage weight after service year")
    if scalar("Stock") <= 0:
        raise ValueError("Cannot select a stock-vintage profile for zero stock")
    stock_variables = [prefix + f"Stock|Vintage|{c}" for c in cohorts]
    if not set(stock_variables).issubset(variables):
        raise ValueError("Cohort shares lack matching cohort quantities")
    stock = data.sel(region=region, variables=stock_variables, year=year).values.astype(
        float
    )
    stock_unit = units.get(prefix + "Stock")
    if not stock_unit or any(units.get(v) != stock_unit for v in stock_variables):
        raise ValueError("Cohort quantities and total stock have different units")
    if (
        not np.isfinite(stock).all()
        or (stock < 0).any()
        or ((cohorts > year) & (stock != 0)).any()
        or not np.isclose(stock.sum(), scalar("Stock"), rtol=1e-10, atol=1e-12)
        or ((stock == 0) & (weights > 0)).any()
    ):
        raise ValueError("Cohort quantities do not reconcile with total stock/shares")
    if basis == "stock" and not np.allclose(
        weights, stock / stock.sum(), rtol=0, atol=1e-10
    ):
        raise ValueError("Stock shares disagree with cohort quantities")
    if basis == "service" and prefix + "Service" in variables:
        if scalar("Service") <= 0:
            raise ValueError("Cannot select a service profile for zero service")
    order = np.argsort(cohorts)
    keep = order[weights[order] > 0]
    return {
        "service_year": int(year),
        "event_years": cohorts[keep].tolist(),
        "weights": weights[keep].tolist(),
    }


def stock_vintage_profile(
    data,
    *,
    asset_id,
    region,
    years,
    report_path,
    scenario_path,
    asset_unit,
    service_unit,
    profile_id=None,
):
    """Build a service-weighted profile for StockVintageExport's explicit bindings.

    Inventory units are caller-supplied: normalized cohort weights do not convert
    IAM regional quantities into an inventory coefficient or lifetime allocation.
    """
    report = verified_report(report_path, scenario_path)
    metadata = report["assumptions"]
    matches = [
        a
        for a in metadata["assets"]
        if a["asset"] == asset_id and a["region"] == region
    ]
    if len(matches) != 1:
        raise ValueError("No unique stock-vintage asset/region in the report")
    years = list(years)
    if (
        not years
        or any(type(y) is not int for y in years)
        or years != list(range(years[0], years[-1] + 1))
        or years[0] < metadata["reference_year"]
        or years[-1] > metadata["end_year"]
    ):
        raise ValueError("Profile years must be consecutive and within report coverage")
    for unit in (asset_unit, service_unit):
        if not isinstance(unit, str) or not unit.strip():
            raise ValueError("Explicit inventory asset and service units are required")
    return {
        "id": profile_id or asset_id,
        "event_role": "existing_asset_service",
        "allocation_basis": "common_amortisation",
        "asset_unit": asset_unit,
        "service_unit": service_unit,
        "years": [stock_vintage_weights(data, asset_id, region, y) for y in years],
        "provenance": {
            "origin": metadata["origin"],
            "iam_input_sha256": report["input_sha256"],
            "enriched_iam_sha256": report["output_sha256"],
            "report_sha256": hashlib.sha256(Path(report_path).read_bytes()).hexdigest(),
            "model": report["model"],
            "scenario": metadata["scenario"],
            "flodym_version": metadata["flodym_version"],
            **{
                key: deepcopy(metadata[key])
                for key in (
                    "software_versions",
                    "implementation_sha256",
                    "stock_timing",
                    "entry_timing",
                    "service_timing",
                )
                if key in metadata
            },
            "asset": deepcopy(matches[0]),
            "service_target_effect": metadata["service_target_effect"],
            "exchange_amount_changed": False,
        },
    }
