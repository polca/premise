"""Generate local CCGT pilot cohorts from EIA observations and REMIND trajectories.

This is a labelled regional reconstruction, not native REMIND vintage output.
Source IAM values and derived records remain local. The script does not change
an inventory coefficient, bind an inventory, or approve the empirical pilot.
"""

import argparse
from collections import defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path

from premise.stock_cohorts import SurvivalLaw, evolve_stock, service_weights

if __package__:
    from .acquire_sources import verify
    from .audit_power_service import audit
else:
    from acquire_sources import verify
    from audit_power_service import audit


VARIABLES = {
    "capacity": ("Cap|Electricity|Gas|CC|+|w/o CC", "GW"),
    "additions": ("New Cap|Electricity|Gas|CC|+|w/o CC", "GW/yr"),
    "generation": ("SE|Electricity|Gas|++|Combined Cycle w/o CC", "EJ/yr"),
    "lifetime": ("Tech|Electricity|Gas|Combined Cycle w/o CC|Lifetime", "years"),
}
REFERENCE = 2022


def read_iam(path, expected_hash, scenario, end_year):
    """Require exact source identity, unique leaf rows, units and finite values."""
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
        raise ValueError("IAM file differs from the reviewed manifest")
    required_years = list(range(2020, end_year + 6, 5))
    inverse = {variable: (key, unit) for key, (variable, unit) in VARIABLES.items()}
    values = {}
    with path.open(newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream, delimiter=";"):
            if row["Region"] != "USA" or row["Variable"] not in inverse:
                continue
            key, unit = inverse[row["Variable"]]
            if key in values:
                raise ValueError(f"Duplicate selected IAM variable: {key}")
            if (
                row["Model"] != "REMIND"
                or row["Scenario"] != scenario
                or row["Unit"] != unit
            ):
                raise ValueError(f"Unexpected IAM context or unit for {key}")
            values[key] = {}
            for year in required_years:
                value = float(row[str(year)])
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"Invalid IAM value for {key}/{year}")
                values[key][year] = value
    if set(values) != set(VARIABLES):
        raise ValueError(
            "Missing CCGT leaf variables; aggregate gas variables are not substitutes"
        )
    if set(values["lifetime"].values()) != {35.0}:
        raise ValueError("This pilot requires the reviewed constant 35-year lifetime")
    return values


def linear(series, year):
    """Interpolate stock/output levels, without extrapolation."""
    if year in series:
        return series[year]
    before = [y for y in series if y < year]
    after = [y for y in series if y > year]
    if not before or not after:
        raise ValueError(f"Cannot extrapolate IAM level to {year}")
    lo, hi = max(before), min(after)
    return series[lo] + (series[hi] - series[lo]) * (year - lo) / (hi - lo)


def centred_additions(series, years, scale):
    """Expand a reported five-year annual rate over its centred calendar period.

    For example, 2025 covers 2023--2027. Never interpolate a rate and silently
    change its period integral. This bounded pilot does not handle the later
    switch to ten-year reporting periods.
    """
    years = list(years)
    expanded, periods = {}, []
    for centre, rate in sorted(series.items()):
        period_years = list(range(centre - 2, centre + 3))
        if not set(period_years) & set(years):
            continue
        for year in period_years:
            if year in expanded:
                raise ValueError("Overlapping addition periods")
            expanded[year] = rate * scale
        periods.append(
            {
                "reported_year": centre,
                "period_years": period_years,
                "scaled_period_additions_mw": rate * scale * 5,
                "expanded_period_sum_mw": math.fsum(expanded[y] for y in period_years),
                "selected_period_years": sorted(set(period_years) & set(years)),
            }
        )
    if set(years) - set(expanded):
        raise ValueError("Reported addition periods do not cover every requested year")
    return {year: expanded[year] for year in years}, periods


def initial_cohorts(blocks):
    capacity, generation = defaultdict(float), defaultdict(float)
    seen = set()
    for row in blocks:
        key = (row["plant_code"], row["unit_code"])
        if key in seen:
            raise ValueError("Duplicate observed block")
        seen.add(key)
        cohort = row["cohort_year"]
        stock, output = row["capacity_mw_ac"], row["net_generation_mwh"]
        if int(cohort) != cohort or cohort >= REFERENCE:
            raise ValueError(
                "Partial commissioning-year service needs a separate model"
            )
        if not all(math.isfinite(v) and v > 0 for v in (stock, output)):
            raise ValueError("Observed block stock/output must be positive")
        if output > stock * 8760 * (1 + 1e-10):
            raise ValueError(
                "Observed block output exceeds full-year nameplate production"
            )
        capacity[cohort] += stock
        generation[cohort] += output
    if not seen:
        raise ValueError("No complete observed CCGT blocks")
    return dict(sorted(capacity.items())), dict(sorted(generation.items()))


def project_case(initial, generation, iam, *, end_year, survival, mode, weighting):
    """Preserve physical capacity balance and report both incompatible IAM constraints."""
    if not REFERENCE < end_year <= 2050:
        raise ValueError("This pilot supports annual service years through 2050 only")
    years = list(range(REFERENCE + 1, end_year + 1))
    initial_capacity, initial_output = math.fsum(initial.values()), math.fsum(
        generation.values()
    )
    capacity_scale = initial_capacity / linear(iam["capacity"], REFERENCE)
    output_scale = initial_output / linear(iam["generation"], REFERENCE)
    targets = {y: linear(iam["capacity"], y) * capacity_scale for y in years}
    additions, periods = centred_additions(iam["additions"], years, capacity_scale)
    if mode not in {"stock_target", "gross_additions"}:
        raise ValueError("Unknown reconstruction mode")
    projection = evolve_stock(
        initial,
        REFERENCE,
        targets if mode == "stock_target" else additions,
        survival,
        mode=mode,
        early_exit_kind="service_exit",
    )
    baseline_hours = initial_output / initial_capacity
    annual = []
    for year, stock in projection.stocks.items():
        if weighting == "observed_output":
            hours = {
                c: generation[c] / initial[c] if c in initial else baseline_hours
                for c in stock
            }
        elif weighting == "equal_capacity":
            hours = {c: baseline_hours for c in stock}
        else:
            raise ValueError("Unknown service weighting")
        raw_output = math.fsum(stock[c] * hours[c] for c in stock)
        output_target = linear(iam["generation"], year) * output_scale
        if raw_output <= 0 or output_target <= 0:
            raise ValueError("No positive service in the requested year")
        service_scale = output_target / raw_output
        hours = {c: value * service_scale for c, value in hours.items()}
        # The common factor affects the output accounting, not normalized
        # vintage shares. Do not invent dispatch when the allocation is infeasible.
        if max(hours.values()) > 8760 * (1 + 1e-10):
            raise ValueError(f"Proposed service allocation exceeds 8760 h in {year}")
        service = service_weights(stock, hours)
        if not math.isclose(service["total_service"], output_target, rel_tol=1e-12):
            raise ValueError("Annual cohort service does not reconcile")
        weights = service["weights"]
        annual.append(
            {
                "service_year": year,
                "event_years": list(weights),
                "weights": list(weights.values()),
                "cohort_capacity_mw_ac": stock,
                "cohort_generation_mwh": service["service_by_cohort"],
                "cohort_full_load_hours": hours,
                "total_capacity_mw_ac": math.fsum(stock.values()),
                "total_generation_mwh": service["total_service"],
                "generation_target_mwh": output_target,
                "mean_age_years": math.fsum((year - c) * w for c, w in weights.items()),
                "output_rescaling_factor": service_scale,
            }
        )
    balances = [
        {
            **row,
            "iam_scaled_capacity_target_mw_ac": targets[row["year"]],
            "capacity_target_residual_mw_ac": row["closing_stock"]
            - targets[row["year"]],
            "iam_scaled_reported_additions_mw_ac": additions[row["year"]],
            "additions_residual_mw_ac": row["gross_additions"] - additions[row["year"]],
        }
        for row in projection.balances
    ]
    return {
        "mode": mode,
        "weighting": weighting,
        "survival": {
            "family": survival.family,
            "mean_years": survival.mean_years,
            "overage_remaining_years": survival.overage_remaining_years,
        },
        "capacity_scale_mw_per_iam_gw": capacity_scale,
        "generation_scale_mwh_per_iam_ej": output_scale,
        "early_exits_mean": "removal from operating service, not physical disposal",
        "annual": annual,
        "balances": balances,
        "reported_addition_periods": periods,
    }


def generate(blocks, iam, end_year):
    initial, generation = initial_cohorts(blocks)
    definitions = [
        ("primary", 35, 5, "stock_target", "observed_output"),
        ("capacity_weights", 35, 5, "stock_target", "equal_capacity"),
        ("short_old_tail", 35, 3, "stock_target", "observed_output"),
        ("long_old_tail", 35, 10, "stock_target", "observed_output"),
        ("shorter_depreciation", 25, 5, "stock_target", "observed_output"),
        ("longer_depreciation", 40, 5, "stock_target", "observed_output"),
        ("reported_additions", 35, 5, "gross_additions", "observed_output"),
    ]
    cases = {}
    for name, mean, tail, mode, weighting in definitions:
        cases[name] = project_case(
            initial,
            generation,
            iam,
            end_year=end_year,
            survival=SurvivalLaw(
                "quartic_capacity", mean, overage_remaining_years=tail
            ),
            mode=mode,
            weighting=weighting,
        )
    return {
        "stage": "annual_ccgt_reconstruction_pending_real_export_validation",
        "reference_year": REFERENCE,
        "last_service_year": end_year,
        "geography": "US-WECC observed subset; USA relative IAM trajectory proxy",
        "initial_capacity_mw_ac": initial,
        "initial_generation_mwh": generation,
        "initial_overage_capacity_mw_ac": math.fsum(
            v for c, v in initial.items() if REFERENCE - c >= 1.25 * 35
        ),
        "cases": cases,
        "assumptions": [
            "Initial end-year survivors are never multiplied by unconditional survival again",
            "Initial service uses calendar-year net electricity; all selected blocks operated before the reference year",
            "Block initial commercial-operation year proxies whole-plant construction; refurbishment is not identified",
            "USA CCGT capacity and output relative changes proxy the selected WECC subfleet, not all WECC",
            "Linear annual interpolation of capacity/output; five-year centred expansion of reported gross addition rates",
            "Primary capacity-constrained reconstruction infers additions and pro-rata operating-service exits",
            "Quartic continuous capacity analogue is not REMIND's native discrete vintage or reporting-period implementation",
            "Observed cohorts outside the finite lifetime support retain an explicitly assumed exponential residual life",
            "Initial cohort-specific utilisation persists; new cohorts use initial fleet-average utilisation; annual common rescaling reconciles output",
            "Future service represents the end-year operating portfolio; within-year commissioning, exit exposure and dispatch are not reconstructed",
            "Reporting both capacity and additions residuals exposes underdetermination rather than asserting simultaneous agreement",
            "Operating capacity exits do not determine physical disposal; no absent inventory end-of-life quantity is invented",
            "Service timing weights preserve the original common-amortisation capital exchange coefficient",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--iam-file", type=Path, required=True)
    parser.add_argument(
        "--scenario", choices=["SSP2-NPi2025", "SSP2-PkBudg650"], required=True
    )
    parser.add_argument("--end-year", type=int, default=2030)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    docs = Path(__file__).resolve().parents[2] / "docs/development/stock-asset-temporal"
    with (docs / "acquisition-manifest.csv").open() as stream:
        records = {r["local_research_filename"]: r for r in csv.DictReader(stream)}
    sources = []
    for filename in ("eia8602022.zip", "f923_2022.zip"):
        record = records[filename]
        if not verify(args.input_dir / filename, record):
            raise ValueError(f"Changed public source: {filename}")
        sources.append(record)
    with (docs / "iam-file-inventory.csv").open() as stream:
        matches = [
            r
            for r in csv.DictReader(stream)
            if r["file"] == f"remind 3.5.2/{args.iam_file.name}"
        ]
    if len(matches) != 1:
        raise ValueError("IAM file is absent or ambiguous in the reviewed manifest")
    iam = read_iam(args.iam_file, matches[0]["sha256"], args.scenario, args.end_year)
    power = audit(args.input_dir)["ccgt"]
    report = generate(power["candidate_blocks"], iam, args.end_year)
    report.update(
        {
            "public_sources": sources,
            "iam_source": {
                "file": matches[0]["file"],
                "sha256": matches[0]["sha256"],
                "scenario": args.scenario,
                "variables": VARIABLES,
                "rights": "restricted local input; do not redistribute raw data or derived series",
            },
            "observation_audit": power,
        }
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "blocks": len(power["candidate_blocks"]),
                "cases": list(report["cases"]),
                "years": [REFERENCE, args.end_year],
            }
        )
    )


if __name__ == "__main__":
    main()
