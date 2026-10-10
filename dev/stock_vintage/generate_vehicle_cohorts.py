"""Reconstruct bounded UK vehicle vintages with declared EUR scenario proxies.

Restricted IAM inputs and resulting series stay local. Neither the annual EDGE
kernel nor the reported macroseries identify native vehicle/component cohorts.
All cases retain common amortisation and the source inventory coefficients.
"""

import argparse
from collections import defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path

from premise.stock_cohorts import (
    AnnualVehicleSurvival,
    StockProjection,
    active_component_records,
    evolve_stock,
    retirement_record,
    service_weights,
)

if __package__:
    from .acquire_sources import verify
    from .curate_observations import uk_articulated_trucks, uk_vehicles
    from .generate_ccgt_cohorts import linear
else:
    from acquire_sources import verify
    from curate_observations import uk_articulated_trucks, uk_vehicles
    from generate_ccgt_cohorts import linear


REFERENCE = 2022
GROUPS = {
    "passenger_bev": {
        "suffix": "Transport|Pass|Road|LDV|Four Wheelers|BEV",
        "service_unit": "billion pkm/yr",
        "source_file": "veh1111.ods",
        "service_life": 20,
    },
    "heavy_trucks_32_40t": {
        "suffix": "Transport|Freight|Road|Heavy|Truck(40t)|Liquids",
        "service_unit": "billion tkm/yr",
        "source_file": "df_VEH0520.csv",
        "service_life": 15,
    },
}
CODE_SOURCES = (
    "edge-genParAnnuityCalc.csv",
    "edge-toolCalculateVehicleDepreciationFactors.R",
    "edge-toolCalculateFleetComposition.R",
    "reporttransport-reportTransportVarSet.R",
    "reporttransport-reportEdgeTransport.R",
)


def variables(group):
    definition = GROUPS[group]
    return {
        "stock": (f"Stock|{definition['suffix']}", "million veh"),
        "sales": (f"Sales|{definition['suffix']}", "million veh"),
        "service": (f"ES|{definition['suffix']}", definition["service_unit"]),
    }


def read_iam(path, expected_hash, scenario, group, end_year):
    """Select unique EUR leaves with exact identity and units; never fill gaps."""
    if not REFERENCE < end_year <= 2050:
        raise ValueError("Vehicle pilot supports 2023 through 2050")
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
        raise ValueError("IAM file differs from the reviewed manifest")
    inverse = {var: (key, unit) for key, (var, unit) in variables(group).items()}
    values = {}
    last_anchor = 5 * math.ceil(end_year / 5)
    required_years = range(2020, last_anchor + 1, 5)
    with path.open(newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream, delimiter=";"):
            if row["Region"] != "EUR" or row["Variable"] not in inverse:
                continue
            key, unit = inverse[row["Variable"]]
            if key in values:
                raise ValueError(f"Duplicate selected vehicle IAM leaf: {key}")
            if (row["Model"], row["Scenario"], row["Unit"]) != (
                "REMIND",
                scenario,
                unit,
            ):
                raise ValueError(f"Unexpected IAM context or unit: {key}")
            values[key] = {}
            for year in required_years:
                raw = row.get(str(year))
                if raw is None or not raw.strip():
                    raise ValueError(f"Missing vehicle IAM value: {key}/{year}")
                value = float(raw)
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"Invalid vehicle IAM value: {key}/{year}")
                values[key][year] = value
    if set(values) != set(variables(group)):
        raise ValueError("Missing exact vehicle IAM leaf; no aggregate substitution")
    return values


def initial_cohorts(observation, *, unknown_policy=None, open_start=1950):
    """Reconcile first-use counts, retaining exclusions and open-bin assumptions."""
    group = observation["group"]
    if group not in GROUPS or observation["observation_year"] != REFERENCE:
        raise ValueError("Require a reviewed vehicle group observed in 2022")
    if observation["unit"] != "vehicle":
        raise ValueError("Observed stock must be in vehicles")
    if unknown_policy is None:
        unknown_policy = "exclude" if group == "passenger_bev" else "proportional"
    if unknown_policy not in {"exclude", "proportional", "oldest", "newest"}:
        raise ValueError("Unknown missing-cohort policy")
    stock, seen, excluded = defaultdict(float), set(), 0.0
    known = 0.0
    for row in observation["cohorts"]:
        year, value = row["cohort_year"], row["stock"]
        if isinstance(year, bool) or int(year) != year or not 1800 <= year <= REFERENCE:
            raise ValueError("Invalid observed vehicle cohort year")
        if year in seen or not math.isfinite(value) or value < 0:
            raise ValueError("Duplicate or invalid observed vehicle cohort")
        seen.add(year)
        known += value
        if group == "passenger_bev" and year < 2010:
            excluded += value  # first use can predate conversion to BEV
        elif value:
            stock[year] += value
    bins = observation.get("unresolved_bins", [])
    open_mass = 0.0
    for row in bins:
        if group != "heavy_trucks_32_40t" or (
            row["label"],
            row["start_year"],
            row["end_year"],
        ) != ("Before 1980", None, 1979):
            raise ValueError("Unreviewed open vehicle-age bin")
        if not isinstance(open_start, int) or not 1800 <= open_start <= 1979:
            raise ValueError("Invalid declared pre-1980 lower bound")
        value = row["stock"]
        if not math.isfinite(value) or value < 0:
            raise ValueError("Invalid open-bin vehicle count")
        open_mass += value
        if value:
            for year in range(open_start, 1980):
                stock[year] += value / (1980 - open_start)
    unknown, total = observation["unknown_stock"], observation["total_stock"]
    if not all(math.isfinite(v) and v >= 0 for v in (unknown, total)):
        raise ValueError("Invalid vehicle total or unknown count")
    if not math.isclose(known + open_mass + unknown, total, abs_tol=1e-9, rel_tol=0):
        raise ValueError("Observed vehicle counts do not reconcile")
    before_unknown = math.fsum(stock.values())
    if before_unknown <= 0:
        raise ValueError("No positive stock in the bounded observed population")
    if unknown_policy == "proportional":
        stock = {c: n * (1 + unknown / before_unknown) for c, n in stock.items()}
    elif unknown_policy != "exclude":
        cohort = min(stock) if unknown_policy == "oldest" else REFERENCE
        stock[cohort] = stock.get(cohort, 0) + unknown
    selected = math.fsum(stock.values())
    expected = total - excluded - (unknown if unknown_policy == "exclude" else 0)
    if not math.isclose(selected, expected, rel_tol=1e-12, abs_tol=1e-9):
        raise ValueError("Bounded cohort selection failed reconciliation")
    return dict(sorted(stock.items())), {
        "source_total_vehicles": total,
        "selected_vehicles": selected,
        "selected_share": selected / total,
        "excluded_pre2010_bev_vehicles": excluded,
        "source_unknown_vehicles": unknown,
        "unknown_policy": unknown_policy,
        "open_bin_vehicles": open_mass,
        "open_bin_uniform_years": [open_start, 1979] if open_mass else None,
        "date_semantics": "first-use year proxies parent manufacture; no conversion or component history",
    }


def project_case(
    initial,
    iam,
    *,
    end_year,
    survival,
    mode="stock_target",
    weighting="equal_vehicle",
    battery_interval=None,
    disposal_delay=0,
):
    """Separate regional balance, service index and conditional physical events."""
    if not REFERENCE < end_year <= 2050:
        raise ValueError("Vehicle pilot supports 2023 through 2050")
    if not isinstance(disposal_delay, int) or disposal_delay < 0:
        raise ValueError("Disposal delay must be a nonnegative integer")
    if mode not in {"stock_target", "gross_additions"}:
        raise ValueError("Unknown stock reconstruction mode")
    initial_total = math.fsum(initial.values())
    ref_stock, ref_service = linear(iam["stock"], REFERENCE), linear(
        iam["service"], REFERENCE
    )
    if min(initial_total, ref_stock, ref_service) <= 0:
        raise ValueError("Reference stock and service must be positive")
    stock_scale = initial_total / ref_stock
    years = range(REFERENCE + 1, end_year + 1)
    targets = {y: linear(iam["stock"], y) * stock_scale for y in years}
    # Sales is the calendar-year entrant cohort, not a centred five-year rate.
    additions = {y: linear(iam["sales"], y) * stock_scale for y in years}
    projection = evolve_stock(
        initial,
        REFERENCE,
        targets if mode == "stock_target" else additions,
        survival,
        mode=mode,
        early_exit_kind="territorial_exit",
    )
    annual, parent_end, manufacture, component_end, joint = [], [], [], [], []

    def delayed(record):
        return {
            **record,
            "event_years": [y + disposal_delay for y in record["event_years"]],
            "assumed_post_exit_disposal_delay_years": disposal_delay,
        }

    for year, stock in projection.stocks.items():
        if weighting == "equal_vehicle":
            utilisation = dict.fromkeys(stock, 1.0)
        elif weighting == "declining_with_age":
            utilisation = {c: math.exp(-(year - c) / 10) for c in stock}
        elif weighting == "half_year_entrants":
            utilisation = {c: 0.5 if c == year else 1.0 for c in stock}
        else:
            raise ValueError("Unknown vehicle service weighting")
        raw_service = math.fsum(n * utilisation[c] for c, n in stock.items())
        target = initial_total * linear(iam["service"], year) / ref_service
        if min(raw_service, target) <= 0:
            raise ValueError("No positive service in requested vehicle year")
        factor = target / raw_service
        utilisation = {c: u * factor for c, u in utilisation.items()}
        service = service_weights(stock, utilisation)
        if not math.isclose(service["total_service"], target, rel_tol=1e-12):
            raise ValueError("Vehicle service index does not reconcile")
        weights = service["weights"]
        annual.append(
            {
                "service_year": year,
                "event_years": list(weights),
                "weights": list(weights.values()),
                "cohort_stock_vehicles": stock,
                "total_stock_vehicles": math.fsum(stock.values()),
                "cohort_service_index": service["service_by_cohort"],
                "cohort_utilisation_index": utilisation,
                "total_service_index": service["total_service"],
                "service_target_index": target,
                "service_rescaling_factor": factor,
                "mean_age_years": math.fsum((year - c) * w for c, w in weights.items()),
            }
        )
        # Observe the serving stock anew at t. Future territorial targets cannot
        # establish when an exported or parked vehicle is physically scrapped.
        physical = StockProjection(year, {year: stock}, [], survival, None)
        parent_end.append(delayed(retirement_record(physical, year, weights)))
        if battery_interval is not None:
            records = active_component_records(
                physical, year, weights, interval_years=battery_interval
            )
            manufacture.append(records["manufacture"])
            component_end.append(delayed(records["retirement"]))
            joint.append(
                {
                    "service_year": year,
                    "events": [
                        {
                            **event,
                            "disposal_year": event["retirement_year"] + disposal_delay,
                        }
                        for event in records["joint_events"]
                    ],
                }
            )
    result = {
        "mode": mode,
        "weighting": weighting,
        "survival": {
            "family": survival.family,
            "maximum_included_age": survival.service_life,
            "overage_remaining_years": survival.overage_remaining_years,
        },
        "stock_scale_vehicles_per_iam_million_vehicles": stock_scale,
        "sales_semantics": "calendar-year entrants; linear interpolation between reported years",
        "annual": annual,
        "parent_retirement": parent_end,
        "balances": [
            {
                **row,
                "iam_scaled_stock_target_vehicles": targets[row["year"]],
                "stock_target_residual_vehicles": row["closing_stock"]
                - targets[row["year"]],
                "iam_scaled_sales_vehicles": additions[row["year"]],
                "sales_residual_vehicles": row["gross_additions"]
                - additions[row["year"]],
            }
            for row in projection.balances
        ],
        "physical_retirement_basis": "separate conditional kernel proxy; no future territorial exits",
        "disposal_delay_years": disposal_delay,
        "allocation_basis": "common_amortisation",
        "exchange_amount_changed": False,
    }
    if battery_interval is not None:
        result.update(
            battery_manufacture=manufacture,
            battery_retirement=component_end,
            battery_joint_events=joint,
            battery_interval_years=battery_interval,
            battery_quantity_basis="original 1.5 equivalent 262 kg packs per 150000 km retained; timing proxy is not a pack-count forecast",
        )
    return result


def generate(observation, iam, end_year):
    group = observation["group"]
    life = GROUPS[group]["service_life"]
    # A declared timing proxy converts the inventory's 100000/150000 km ratio
    # using a separate nominal 20-year horizon; this is not EDGE's mean lifetime.
    interval = 20 * 100000 / 150000 if group == "passenger_bev" else None
    definitions = [
        ("primary", {}),
        ("declining_utilisation", {"weighting": "declining_with_age"}),
        ("half_year_entrants", {"weighting": "half_year_entrants"}),
        ("reported_sales", {"mode": "gross_additions"}),
        ("shorter_survival", {"service_life": life - 5}),
        ("longer_survival", {"service_life": life + 5}),
        ("short_overage_tail", {"overage": 1}),
        ("long_overage_tail", {"overage": 7}),
        ("unknown_oldest", {"unknown_policy": "oldest"}),
        ("unknown_newest", {"unknown_policy": "newest"}),
        ("disposal_delay_five_years", {"disposal_delay": 5}),
    ]
    if interval is not None:
        definitions += [
            ("unknown_proportional", {"unknown_policy": "proportional"}),
            ("battery_ten_years", {"battery_interval": 10}),
            ("battery_twenty_years", {"battery_interval": 20}),
        ]
    else:
        definitions += [("pre1980_lower_bound_1900", {"open_start": 1900})]
    cases = {}
    for name, overrides in definitions:
        options = dict(overrides)
        initial, selection = initial_cohorts(
            observation,
            unknown_policy=options.pop("unknown_policy", None),
            open_start=options.pop("open_start", 1950),
        )
        survival = AnnualVehicleSurvival(
            options.pop("service_life", life), options.pop("overage", 3)
        )
        case = project_case(
            initial,
            iam,
            end_year=end_year,
            survival=survival,
            battery_interval=options.pop("battery_interval", interval),
            **options,
        )
        cases[name] = {**case, "observation_selection": selection}
    return {
        "stage": "annual_vehicle_reconstruction_pending_real_export_validation",
        "reference_year": REFERENCE,
        "last_service_year": end_year,
        "group": group,
        "geography": "bounded UK stock; EUR relative scenario trajectory proxy",
        "cases": cases,
        "assumptions": [
            "First-use years proxy manufacture; imported, converted and refurbished vehicles are not identified",
            "Primary BEV population contains known first-use years from 2010; earlier and unknown dates remain explicit exclusions",
            "Truck population contains articulated road-using diesel vehicles with 32 < maximum gross weight <= 40 tonnes; no observed EURO class",
            "Observed end-year survivors receive conditional survival only once",
            "End-year serving-stock snapshot, not a complete calendar-year exposure reconstruction; half-year entrant case is a sensitivity",
            "Primary equal service per vehicle; age-dependent mileage, occupancy and loads are unobserved",
            "ES relative trend rescales a service index; pkm is not converted to vehicle-km and tkm is not a measured subgroup total",
            "Reported service can be harmonised separately from stock in reporttransport; no exact native fleet reproduction claim",
            "Annual survival borrows pinned EDGE defaults: maximum included age 20 for LDV 4W and 15 for heavy trucks, not mean lives",
            "Exact local-run EDGE parameters and native construction-year output are unavailable",
            "Reported Sales is a calendar-year entry count, interpolated linearly; stock and sales constraints have separate residuals",
            "Proportional territorial exits reconcile declining targets; physical disposal uses a separate conditional-kernel assumption",
            "Observed survivors beyond kernel support use a declared residual exponential life; no initial cohort is removed solely for being old",
            "BEV battery dates use deterministic active-component timing with an assumed nominal 20-year horizon and source mileage ratio; histories are unobserved",
            "Original common-amortised quantities are preserved; timing profiles do not forecast physical pack counts or material demand",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--iam-file", type=Path, required=True)
    parser.add_argument(
        "--scenario", choices=["SSP2-NPi2025", "SSP2-PkBudg650"], required=True
    )
    parser.add_argument("--group", choices=list(GROUPS), required=True)
    parser.add_argument("--end-year", type=int, default=2030)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    docs = root / "docs/development/stock-asset-temporal"
    with (docs / "acquisition-manifest.csv").open() as stream:
        records = {r["local_research_filename"]: r for r in csv.DictReader(stream)}
    sources = []
    for name in (GROUPS[args.group]["source_file"], *CODE_SOURCES):
        if not verify(args.input_dir / name, records[name]):
            raise ValueError(f"Changed reviewed public source: {name}")
        sources.append(records[name])
    topology_path = (
        root / "premise/iam_variables_mapping/topologies/remind-topology.json"
    )
    topology = json.loads(topology_path.read_text())
    if [r for r, countries in topology.items() if "GB" in countries] != ["EUR"]:
        raise ValueError("Reviewed GB to EUR mapping changed")
    with (docs / "iam-file-inventory.csv").open() as stream:
        matches = [
            r
            for r in csv.DictReader(stream)
            if r["file"] == f"remind 3.5.2/{args.iam_file.name}"
        ]
    if len(matches) != 1:
        raise ValueError("IAM file is absent or ambiguous in reviewed manifest")
    iam = read_iam(
        args.iam_file, matches[0]["sha256"], args.scenario, args.group, args.end_year
    )
    observations = (
        uk_vehicles(args.input_dir / "veh1111.ods")
        if args.group == "passenger_bev"
        else uk_articulated_trucks(args.input_dir / "df_VEH0520.csv", [REFERENCE])
    )
    selected = [
        r
        for r in observations
        if r["group"] == args.group and r["observation_year"] == REFERENCE
    ]
    if len(selected) != 1:
        raise ValueError("Require exactly one selected observed population")
    report = generate(selected[0], iam, args.end_year)
    report.update(
        public_sources=sources,
        observation=selected[0],
        iam_series=iam,
        iam_source={
            "file": matches[0]["file"],
            "sha256": matches[0]["sha256"],
            "scenario": args.scenario,
            "region": "EUR",
            "variables": variables(args.group),
            "rights": "Restricted local input and derived series; do not redistribute",
            "mapping_file": str(topology_path.relative_to(root)),
            "mapping_sha256": hashlib.sha256(topology_path.read_bytes()).hexdigest(),
        },
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "group": args.group,
                "cases": len(report["cases"]),
                "years": [REFERENCE, args.end_year],
                "primary_selection": report["cases"]["primary"][
                    "observation_selection"
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
