"""Reproducible derivations for empirical TRAILS timing evidence.

The helpers preserve source resolution.  In particular, Eurostat age bands are
not silently converted to annual point ages, and EIA generator vintages retain
annual operating years with explicit capacity weighting.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

EU27_2020 = frozenset(
    [
        "AT",
        "BE",
        "BG",
        "HR",
        "CY",
        "CZ",
        "DK",
        "EE",
        "FI",
        "FR",
        "DE",
        "EL",
        "HU",
        "IE",
        "IT",
        "LV",
        "LT",
        "LU",
        "MT",
        "NL",
        "PL",
        "PT",
        "RO",
        "SK",
        "SI",
        "ES",
        "SE",
    ]
)
EUROSTAT_AGE_BANDS = {
    "Y_LT2": {"minimum_years": 0, "maximum_years": 2},
    "Y2-5": {"minimum_years": 2, "maximum_years": 5},
    "Y5-10": {"minimum_years": 5, "maximum_years": 10},
    "Y10-20": {"minimum_years": 10, "maximum_years": 20},
    "Y_GT20": {"minimum_years": 20, "maximum_years": None},
}

# Closed Eurostat bands use their arithmetic midpoint.  The open-ended oldest
# band follows the EEA's published stock-age convention of representing
# vehicles older than 20 years at age 22.5.  Keeping this conversion separate
# from ``eurostat_age_band_profile`` preserves the original source resolution
# for evidence-only profiles while making the additional modelling assumption
# explicit for operational construction-vintage profiles.
EUROSTAT_REPRESENTATIVE_AGES = {
    "Y_LT2": 1.0,
    "Y2-5": 3.5,
    "Y5-10": 7.5,
    "Y10-20": 15.0,
    "Y_GT20": 22.5,
}

EUROSTAT_AIRCRAFT_AGE_BANDS = {
    "Y_LT5": {"minimum_years": 0, "maximum_years": 5},
    "Y5-9": {"minimum_years": 5, "maximum_years": 10},
    "Y10-14": {"minimum_years": 10, "maximum_years": 15},
    "Y15-19": {"minimum_years": 15, "maximum_years": 20},
    "Y_GE20": {"minimum_years": 20, "maximum_years": None},
}
EUROSTAT_AIRCRAFT_REPRESENTATIVE_AGES = {
    "Y_LT5": 2.0,
    "Y5-9": 7.0,
    "Y10-14": 12.0,
    "Y15-19": 17.0,
    "Y_GE20": 22.5,
}


def decode_jsonstat(document: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Decode the sparse JSON-stat representation returned by Eurostat."""

    dimensions = document["id"]
    sizes = document["size"]
    if len(dimensions) != len(sizes):
        raise ValueError("JSON-stat dimensions and sizes are not aligned.")
    categories = {}
    for dimension in dimensions:
        index = document["dimension"][dimension]["category"]["index"]
        categories[dimension] = [
            code for code, _ in sorted(index.items(), key=lambda item: item[1])
        ]
    strides = [math.prod(sizes[index + 1 :]) for index in range(len(sizes))]
    status = document.get("status", {})
    records = []
    for flat_index, value in document.get("value", {}).items():
        flat_index = int(flat_index)
        record = {
            dimension: categories[dimension][
                (flat_index // strides[index]) % sizes[index]
            ]
            for index, dimension in enumerate(dimensions)
        }
        record["value"] = float(value)
        record["status"] = status.get(str(flat_index), "")
        records.append(record)
    return records


def eurostat_age_band_profile(
    document: Mapping[str, Any],
    *,
    countries: Iterable[str] = EU27_2020,
    vehicle: str | None = None,
) -> dict[str, Any]:
    """Aggregate age shares over countries reporting every requested age band.

    Restricting all bands to the same country set avoids bias from different
    geographical coverage in different age classes.
    """

    records = decode_jsonstat(document)
    countries = set(countries)
    if vehicle is not None:
        records = [record for record in records if record.get("vehicle") == vehicle]
    age_codes = [
        code
        for code in document["dimension"]["age"]["category"]["index"]
        if code in EUROSTAT_AGE_BANDS
    ]
    if not age_codes:
        raise ValueError("No supported Eurostat age bands were found.")
    complete_countries = sorted(
        country
        for country in countries
        if all(
            any(
                record.get("geo") == country and record.get("age") == age
                for record in records
            )
            for age in age_codes
        )
    )
    if not complete_countries:
        raise ValueError("No country reports every requested Eurostat age band.")
    totals = {
        age: sum(
            record["value"]
            for record in records
            if record.get("geo") in complete_countries and record.get("age") == age
        )
        for age in age_codes
    }
    total = sum(totals.values())
    if total <= 0:
        raise ValueError("Eurostat age-band observations have no positive mass.")
    weights = [totals[age] / total for age in age_codes]
    weights[-1] += 1.0 - sum(weights)
    return {
        "countries": complete_countries,
        "country_count": len(complete_countries),
        "observations": int(total),
        "bands": [
            {
                "code": age,
                **EUROSTAT_AGE_BANDS[age],
                "count": int(totals[age]),
                "weight": weights[index],
            }
            for index, age in enumerate(age_codes)
        ],
    }


def eurostat_representative_age_profile(
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert Eurostat age bands to an explicit construction-vintage proxy.

    This function does not estimate a lifetime distribution.  It places each
    observed stock-age band at one documented representative age so TRAILS can
    attach the band mass to a discrete construction year.
    """

    bands = profile.get("bands")
    if not isinstance(bands, list) or not bands:
        raise ValueError("A nonempty Eurostat age-band profile is required.")
    unknown = [
        band.get("code")
        for band in bands
        if band.get("code") not in EUROSTAT_REPRESENTATIVE_AGES
    ]
    if unknown:
        raise ValueError(f"Unsupported Eurostat age bands: {unknown}")

    points = sorted(
        (
            EUROSTAT_REPRESENTATIVE_AGES[band["code"]],
            float(band["weight"]),
        )
        for band in bands
    )
    if any(not math.isfinite(weight) or weight < 0 for _, weight in points):
        raise ValueError("Eurostat age-band weights must be finite and nonnegative.")
    total = sum(weight for _, weight in points)
    if not math.isclose(total, 1.0, rel_tol=1e-9, abs_tol=1e-12):
        raise ValueError("Eurostat age-band weights must sum to one.")

    # TRAILS convention lists the oldest (most negative) construction offset
    # first.  Correct the final weight for harmless floating-point drift.
    points.reverse()
    weights = [weight for _, weight in points]
    weights[-1] += 1.0 - sum(weights)
    return {
        "offsets_years": [-age for age, _ in points],
        "weights": weights,
        "mean_age_years": sum(age * weight for age, weight in points),
    }


def eurostat_aircraft_age_band_profile(
    document: Mapping[str, Any], *, countries: Iterable[str] = EU27_2020
) -> dict[str, Any]:
    """Aggregate EU aircraft ages over countries reporting every age band."""

    records = decode_jsonstat(document)
    countries = set(countries)
    age_codes = [
        code
        for code in document["dimension"]["age"]["category"]["index"]
        if code in EUROSTAT_AIRCRAFT_AGE_BANDS
    ]
    if not age_codes:
        raise ValueError("No supported Eurostat aircraft age bands were found.")
    complete_countries = sorted(
        country
        for country in countries
        if all(
            any(
                record.get("geo") == country and record.get("age") == age
                for record in records
            )
            for age in age_codes
        )
    )
    if not complete_countries:
        raise ValueError("No country reports every requested aircraft age band.")
    totals = {
        age: sum(
            record["value"]
            for record in records
            if record.get("geo") in complete_countries and record.get("age") == age
        )
        for age in age_codes
    }
    total = sum(totals.values())
    if total <= 0:
        raise ValueError(
            "Eurostat aircraft age-band observations have no positive mass."
        )
    weights = [totals[age] / total for age in age_codes]
    weights[-1] += 1.0 - sum(weights)
    return {
        "countries": complete_countries,
        "country_count": len(complete_countries),
        "observations": int(total),
        "bands": [
            {
                "code": age,
                **EUROSTAT_AIRCRAFT_AGE_BANDS[age],
                "count": int(totals[age]),
                "weight": weights[index],
            }
            for index, age in enumerate(age_codes)
        ],
    }


def eurostat_aircraft_representative_age_profile(
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert observed aircraft age bands to a discrete fleet-vintage proxy."""

    bands = profile.get("bands")
    if not isinstance(bands, list) or not bands:
        raise ValueError("A nonempty Eurostat aircraft age-band profile is required.")
    unknown = [
        band.get("code")
        for band in bands
        if band.get("code") not in EUROSTAT_AIRCRAFT_REPRESENTATIVE_AGES
    ]
    if unknown:
        raise ValueError(f"Unsupported Eurostat aircraft age bands: {unknown}")
    points = sorted(
        (
            EUROSTAT_AIRCRAFT_REPRESENTATIVE_AGES[band["code"]],
            float(band["weight"]),
        )
        for band in bands
    )
    total = sum(weight for _, weight in points)
    if not math.isclose(total, 1.0, rel_tol=1e-9, abs_tol=1e-12):
        raise ValueError("Eurostat aircraft age-band weights must sum to one.")
    points.reverse()
    weights = [weight for _, weight in points]
    weights[-1] += 1.0 - sum(weights)
    return {
        "offsets_years": [-age for age, _ in points],
        "weights": weights,
        "mean_age_years": sum(age * weight for age, weight in points),
    }


def _weighted_annual_profile(
    frame: pd.DataFrame,
    *,
    age_column: str,
    weight_column: str,
) -> dict[str, Any]:
    annual = defaultdict(float)
    for age, weight in frame[[age_column, weight_column]].itertuples(index=False):
        annual[int(age)] += float(weight)
    total = sum(annual.values())
    if total <= 0:
        raise ValueError("Weighted age observations have no positive mass.")
    # Oldest vintages first gives monotonically increasing (negative) offsets.
    ages = sorted(annual, reverse=True)
    weights = [annual[age] / total for age in ages]
    weights[-1] += 1.0 - sum(weights)
    return {
        "offsets_years": [-age for age in ages],
        "weights": weights,
        "mean_age_years": sum(age * weight for age, weight in zip(ages, weights)),
        "minimum_age_years": min(ages),
        "maximum_age_years": max(ages),
        "weight_total_mw": total,
        "unit_count": len(frame),
    }


def eia_operating_fleet_profiles(
    frame: pd.DataFrame,
    *,
    reference_year: int,
    technologies: Iterable[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return annual, summer-capacity-weighted ages of operating generators."""

    required = {
        "Status",
        "Technology",
        "Operating Year",
        "Summer Capacity (MW)",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"EIA operating data are missing columns: {sorted(missing)}")
    data = frame.loc[frame["Status"].eq("OP"), list(required)].copy()
    data["Operating Year"] = pd.to_numeric(data["Operating Year"], errors="coerce")
    data["Summer Capacity (MW)"] = pd.to_numeric(
        data["Summer Capacity (MW)"], errors="coerce"
    )
    data = data.loc[
        data["Operating Year"].between(1800, reference_year)
        & data["Summer Capacity (MW)"].gt(0)
    ]
    data["age"] = reference_year - data["Operating Year"].astype(int)
    if technologies is not None:
        data = data.loc[data["Technology"].isin(set(technologies))]
    return {
        technology: _weighted_annual_profile(
            group, age_column="age", weight_column="Summer Capacity (MW)"
        )
        for technology, group in data.groupby("Technology", sort=True)
    }


def eia_retirement_age_profiles(
    frame: pd.DataFrame,
    *,
    technologies: Iterable[str] | None = None,
    earliest_retirement_year: int = 2002,
) -> dict[str, dict[str, Any]]:
    """Return observed, capacity-weighted generator ages at retirement.

    The default start year reflects the EIA-860M comprehensive retired-generator
    series.  These are observed retirement ages, not uncensored survival curves.
    """

    required = {
        "Status",
        "Technology",
        "Operating Year",
        "Retirement Year",
        "Summer Capacity (MW)",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"EIA retirement data are missing columns: {sorted(missing)}")
    data = frame.loc[frame["Status"].eq("RE"), list(required)].copy()
    for column in ("Operating Year", "Retirement Year", "Summer Capacity (MW)"):
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.loc[
        data["Retirement Year"].ge(earliest_retirement_year)
        & data["Retirement Year"].ge(data["Operating Year"])
        & data["Summer Capacity (MW)"].gt(0)
    ]
    data["age"] = data["Retirement Year"].astype(int) - data["Operating Year"].astype(
        int
    )
    if technologies is not None:
        data = data.loc[data["Technology"].isin(set(technologies))]
    return {
        technology: _weighted_annual_profile(
            group, age_column="age", weight_column="Summer Capacity (MW)"
        )
        for technology, group in data.groupby("Technology", sort=True)
    }
