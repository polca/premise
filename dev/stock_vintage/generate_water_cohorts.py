"""Reconstruct annual Quebec pipe and storage cohorts from reported age bins.

Observed quantities are preserved. Annual dates, utilisation, future stock and
survival are explicit scenario assumptions, never fitted age observations.
"""

import argparse
from collections import defaultdict
from dataclasses import asdict
import json
import math
from pathlib import Path

try:
    from .curate_observations import canadian_water_assets, source
except ImportError:
    from curate_observations import canadian_water_assets, source
from premise.stock_cohorts import (
    SurvivalLaw,
    evolve_stock,
    retirement_record,
    service_weights,
)

REFERENCE_YEAR = 2022
ASSETS = {
    "pipes": "Total linear potable water assets",
    "storage": "Water storage assets",
}
BIN_RANGES = {
    "Completed construction 2020 to reference period": (2020, 2022),
    "Completed construction 2010 to 2019": (2010, 2019),
    "Completed construction 2000 to 2009": (2000, 2009),
    "Completed construction 1970 to 1999": (1970, 1999),
    "Completed construction 1940 to 1969": (1940, 1969),
}
OPEN_BIN = "Completed construction prior to 1940"
ASSUMPTIONS = [
    "Observed end-2022 survivors; no second unconditional survival weighting",
    "Annual dates inside reported bins are imputed; construction completion is the manufacture-date proxy",
    "Unknown dates retained in total and explicitly assigned under each sensitivity",
    "Equal service per pipe kilometre and per storage asset in the primary case; no cohort hydraulic output or storage-volume data",
    "Storage-asset counts are a proxy for the generic concrete-tank portion of the inventory; capacity, type and materials are not observed",
    "Primary future closing stocks remain at observed totals; inferred replacement additions close conditional-survival balances",
    "No IAM water-stock variables used; future years are labelled maintenance scenarios, not forecasts",
    "Weibull mean 70 years borrows the inventory design life as a modelling prior; shape 3 is assumed, not estimated",
    "Physical retirement and source inventory disposal are assumed to coincide; abandonment in place and demolition delay are not observed",
    "Preserved common amortisation; no new lifetime denominator and no recalculation of disposal quantities",
    "End-year service portfolio; within-year construction and failure exposure not reconstructed",
]


def annualise(
    observation, *, within_bin="uniform", tail_start=1850, unknown="proportional"
):
    """Disaggregate surviving stock, keeping independent audit totals per bin."""
    if observation["observation_year"] != REFERENCE_YEAR:
        raise ValueError("This pilot is pinned to the 2022 observation")
    if within_bin not in {"uniform", "oldest", "newest"}:
        raise ValueError("Unknown within-bin policy")
    if not isinstance(tail_start, int) or not 1700 <= tail_start < 1940:
        raise ValueError("Declare a pre-1940 tail lower bound between 1700 and 1939")
    if unknown not in {"proportional", "oldest", "newest"}:
        raise ValueError("Unknown missing-date policy")
    expected = set(BIN_RANGES) | {OPEN_BIN}
    rows = observation["cohorts"]
    if len(rows) != len(expected) or {r["construction_bin"] for r in rows} != expected:
        raise ValueError("Construction bins changed or overlap")
    result, audit = defaultdict(float), []
    for row in rows:
        label, value = row["construction_bin"], float(row["stock"])
        if not math.isfinite(value) or value < 0:
            raise ValueError("Invalid observed bin quantity")
        first, last = (tail_start, 1939) if label == OPEN_BIN else BIN_RANGES[label]
        years = (
            list(range(first, last + 1))
            if within_bin == "uniform"
            else [first if within_bin == "oldest" else last]
        )
        amounts = dict.fromkeys(years, value / len(years))
        # Preserve the source bin even if division incurred floating point error.
        amounts[years[-1]] += value - math.fsum(amounts.values())
        for year, amount in amounts.items():
            result[year] += amount
        audit.append(
            {
                "source_bin": label,
                "source_stock": value,
                "assigned_stock": math.fsum(amounts.values()),
                "first_year": min(years),
                "last_year": max(years),
                "annual_policy": within_bin,
            }
        )
    known = math.fsum(result.values())
    missing = float(observation["unknown_stock"])
    total = float(observation["total_stock"])
    if (
        known <= 0
        or not math.isfinite(missing)
        or missing < 0
        or not math.isclose(known + missing, total, rel_tol=1e-12)
    ):
        raise ValueError("Known and unknown stock do not reconcile")
    if unknown == "proportional":
        assigned = {y: v * missing / known for y, v in result.items()}
    else:
        assigned = {tail_start if unknown == "oldest" else REFERENCE_YEAR: missing}
    for year, amount in assigned.items():
        result[year] += amount
    result[max(result)] += total - math.fsum(result.values())
    return dict(sorted((y, v) for y, v in result.items() if v > 0)), {
        "bin_reconciliation": audit,
        "known_stock": known,
        "unknown_stock": missing,
        "unknown_share": missing / total,
        "unknown_policy": unknown,
        "unknown_assignment": assigned,
        "assumed_open_bin_start": tail_start,
        "total_stock": total,
        "total_definition": observation["total_definition"],
    }


def project(
    stock, *, end_year=2030, lifetime=70, shape=3, growth=0, utilisation_age_scale=None
):
    """Replacement-only or explicitly growing stock; service weights and EOL."""
    if not REFERENCE_YEAR < end_year <= 2050:
        raise ValueError("Future end year must be between 2023 and 2050")
    if not 0 <= growth <= 0.03:
        raise ValueError("Only nonnegative bounded stock-growth scenarios supported")
    if utilisation_age_scale is not None and utilisation_age_scale <= 0:
        raise ValueError("Utilisation age scale must be positive")
    total = math.fsum(stock.values())
    law = SurvivalLaw("weibull", lifetime, shape=shape)
    projection = evolve_stock(
        stock,
        REFERENCE_YEAR,
        {
            y: total * (1 + growth) ** (y - REFERENCE_YEAR)
            for y in range(REFERENCE_YEAR + 1, end_year + 1)
        },
        law,
        mode="stock_target",
    )
    annual, retirement = [], []
    for year, cohorts in sorted(projection.stocks.items()):
        utilisation = (
            None
            if utilisation_age_scale is None
            else {c: math.exp(-(year - c) / utilisation_age_scale) for c in cohorts}
        )
        service = service_weights(cohorts, utilisation)
        weights = service["weights"]
        annual.append(
            {
                "service_year": year,
                "event_years": list(weights),
                "weights": list(weights.values()),
                "stock_by_cohort": cohorts,
                "total_stock": math.fsum(cohorts.values()),
                "service_proxy_by_cohort": service["service_by_cohort"],
                "service_proxy_total": service["total_service"],
                "service_unit": "relative service (not measured water volume)",
                "mean_age_years": math.fsum((year - c) * w for c, w in weights.items()),
            }
        )
        retirement.append(retirement_record(projection, year, weights))
    return {
        "annual": annual,
        "retirement": retirement,
        "balances": projection.balances,
        "survival": asdict(law),
        "annual_stock_growth": growth,
        "utilisation_age_scale": utilisation_age_scale,
        "allocation_basis": "common_amortisation",
    }


def build(path, end_year=2030):
    manifest = source(
        path, "S14", "https://www150.statcan.gc.ca/n1/tbl/csv/34100289-eng.zip"
    )
    observations = {}
    for key, asset in ASSETS.items():
        rows = [
            r
            for r in canadian_water_assets(path, asset)
            if r["observation_year"] == REFERENCE_YEAR
        ]
        if len(rows) != 1:
            raise ValueError("Expected one 2022 Quebec observation per asset")
        observations[key] = rows[0]
    cases = {
        "primary": {},
        "bins_oldest": {"within_bin": "oldest"},
        "bins_newest": {"within_bin": "newest"},
        "tail_1800": {"tail_start": 1800},
        "tail_1900": {"tail_start": 1900},
        "unknown_oldest": {"unknown": "oldest"},
        "unknown_newest": {"unknown": "newest"},
        "life_50": {"lifetime": 50},
        "life_100": {"lifetime": 100},
        "shape_2": {"shape": 2},
        "shape_4": {"shape": 4},
        "growth_1pct": {"growth": 0.01},
        "age_service_50": {"utilisation_age_scale": 50},
    }
    output = {}
    for case, parameters in cases.items():
        output[case] = {"parameters": parameters, "assets": {}}
        for key, observed in observations.items():
            initial_options = {
                k: v
                for k, v in parameters.items()
                if k in {"within_bin", "tail_start", "unknown"}
            }
            future_options = {
                k: v for k, v in parameters.items() if k not in initial_options
            }
            stock, reconciliation = annualise(observed, **initial_options)
            record = project(stock, end_year=end_year, **future_options)
            record["initial_reconciliation"] = reconciliation
            output[case]["assets"][key] = record
    return {
        "reference_year": REFERENCE_YEAR,
        "last_service_year": end_year,
        "geography": "CA-QC",
        "observations": observations,
        "sources": [manifest],
        "assumptions": ASSUMPTIONS,
        "cases": output,
        "rights": "Public observations: Statistics Canada Open Licence, with attribution; no licensed inventory included",
        "evidence_tier": "observed_binned_initial_stock_with_explicit_annual_and_future_assumptions",
        "future_scenario": "constant_stock_replacement_primary_not_IAM_forecast",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--end-year", type=int, default=2030)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = build(args.input, args.end_year)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            {
                "cases": len(result["cases"]),
                "assets": list(ASSETS),
                "output": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()
