"""Generate bounded PV parent and active-inverter cohorts from audited sources.

Keep IAM-derived outputs local. This is a US-WECC observed subfleet with USA
relative scenario growth, not an exact REMIND vintage or technology reproduction.
Original inventory coefficients remain unchanged in every timing sensitivity.
"""

import argparse
from collections import defaultdict
import csv
import json
import math
from pathlib import Path

from premise.stock_cohorts import (
    StockProjection,
    SurvivalLaw,
    active_component_records,
    retirement_record,
)

if __package__:
    from .acquire_sources import verify
    from .audit_power_service import audit
    from .generate_ccgt_cohorts import REFERENCE, project_case, read_iam
else:
    from acquire_sources import verify
    from audit_power_service import audit
    from generate_ccgt_cohorts import REFERENCE, project_case, read_iam


VARIABLES = {
    "capacity": ("Cap|Electricity|Solar|+|PV", "GW"),
    "additions": ("New Cap|Electricity|Solar|+|PV", "GW/yr"),
    "generation": ("SE|Electricity|Solar|+|PV", "EJ/yr"),
    "curtailment": ("SE|Electricity|Curtailment|Solar|+|PV", "EJ/yr"),
    "lifetime": ("Tech|Electricity|Solar|PV|Lifetime", "years"),
}


def initial_cohorts(plants):
    """Retain zero-output installed stock and independently reconcile AC/DC/output."""
    ac, dc, generation = (defaultdict(float) for _ in range(3))
    seen = set()
    for row in plants:
        plant = row["plant_code"]
        if plant in seen:
            raise ValueError("Duplicate observed PV plant")
        seen.add(plant)
        year = row["cohort_year"]
        if isinstance(year, bool) or int(year) != year or year >= REFERENCE:
            raise ValueError("PV plants must share a pre-reference commissioning year")
        stock_ac, stock_dc, output = (
            row["selected_capacity_mw_ac"],
            row["capacity_mw_dc"],
            row["net_pv_generation_mwh"],
        )
        if not all(math.isfinite(v) and v > 0 for v in (stock_ac, stock_dc)):
            raise ValueError("Observed PV AC and DC capacity must be positive")
        if not math.isfinite(output) or not 0 <= output <= stock_ac * 8760 * (
            1 + 1e-10
        ):
            raise ValueError("Observed PV generation violates the annual AC bound")
        ac[year] += stock_ac
        dc[year] += stock_dc
        generation[year] += output
    if not seen or math.fsum(generation.values()) <= 0:
        raise ValueError("PV subfleet requires positive aggregate output")
    return tuple(dict(sorted(v.items())) for v in (ac, dc, generation))


def component_case(
    initial,
    generation,
    iam,
    *,
    end_year,
    survival,
    interval_years=15,
    mode="stock_target",
    weighting="observed_output",
):
    result = project_case(
        initial,
        generation,
        iam,
        end_year=end_year,
        survival=survival,
        mode=mode,
        weighting=weighting,
    )
    # Operating-service exit is not automatically physical disposal. The shared
    # conditional-retirement routine fails if a scenario requires forced exits.
    # Natural retirement is a declared physical end-of-life timing assumption.
    projection = StockProjection(
        REFERENCE,
        {row["service_year"]: row["cohort_capacity_mw_ac"] for row in result["annual"]},
        result["balances"],
        survival,
        "service_exit",
    )
    retirements, manufacture, disposal, joint = [], [], [], []
    for row in result["annual"]:
        year = row["service_year"]
        weights = dict(zip(row["event_years"], row["weights"]))
        retirements.append(retirement_record(projection, year, weights))
        component = active_component_records(
            projection,
            year,
            weights,
            interval_years=interval_years,
        )
        manufacture.append(component["manufacture"])
        disposal.append(component["retirement"])
        joint.append({"service_year": year, "events": component["joint_events"]})
    result.update(
        parent_retirement=retirements,
        inverter_manufacture=manufacture,
        inverter_retirement=disposal,
        inverter_joint_events=joint,
        inverter_interval_years=interval_years,
        allocation_basis="common_amortisation",
        exchange_amount_changed=False,
        natural_retirement_meaning="Assumed physical end of life; dismantling delay not observed",
    )
    return result


def generate(plants, iam, end_year):
    initial, dc, generation = initial_cohorts(plants)
    definitions = [
        ("primary", "fixed", 30, 15, "stock_target", "observed_output"),
        ("capacity_weights", "fixed", 30, 15, "stock_target", "equal_capacity"),
        ("shorter_parent", "fixed", 25, 15, "stock_target", "observed_output"),
        ("longer_parent", "fixed", 40, 15, "stock_target", "observed_output"),
        (
            "quartic_parent",
            "quartic_capacity",
            30,
            15,
            "stock_target",
            "observed_output",
        ),
        ("shorter_inverter", "fixed", 30, 10, "stock_target", "observed_output"),
        ("longer_inverter", "fixed", 30, 20, "stock_target", "observed_output"),
        ("reported_additions", "fixed", 30, 15, "gross_additions", "observed_output"),
    ]
    cases = {
        name: component_case(
            initial,
            generation,
            iam,
            end_year=end_year,
            survival=SurvivalLaw(family, years),
            interval_years=interval,
            mode=mode,
            weighting=weighting,
        )
        for name, family, years, interval, mode, weighting in definitions
    }
    return {
        "stage": "annual_pv_reconstruction_pending_component_export_validation",
        "reference_year": REFERENCE,
        "last_service_year": end_year,
        "geography": "US-WECC bounded observed subfleet; USA relative IAM trajectory proxy",
        "evidence_tier": "observed_parent_cohorts_and_service_with_declared_component_and_future_assumptions",
        "initial_capacity_mw_ac": initial,
        "initial_capacity_mw_dc": dc,
        "initial_generation_mwh": generation,
        "cases": cases,
        "assumptions": [
            "Complete fixed-crystalline EIA plants with one commissioning year before 2022; zero-output stocks retained",
            "EIA crystalline and fixed mounting do not prove multi-Si or ground mounting; source inventory is a technology proxy",
            "AC capacity balances and observed net generation weights; DC capacity is recorded independently, not silently substituted",
            "USA all-PV relative capacity and secondary-electricity trends proxy this bounded WECC subfleet",
            "Relative scaling does not identify the absolute IAM AC/DC capacity basis or exactly reproduce IAM vintages",
            "Initial survivors are not survival-weighted twice; future stock-target additions close the annual balance",
            "Primary physical parent life is a declared fixed 30 years based on the inventory design life, not REMIND's native depreciation kernel",
            "Primary inverter life is 15 years, with regular renewal until parent retirement; observed inverter histories are unavailable",
            "Active inverter manufacture is never after service; its end is the earlier of next replacement and conditional parent retirement",
            "Common original amortisation is preserved; survival and replacement sensitivities change timing only, not lifetime material totals",
            "Observed cohort-specific utilisation persists; new cohorts use fleet-average utilisation, with common output rescaling",
            "Future output is a scaled IAM trend applied to end-year stock, not an observed commissioning-year exposure or dispatch forecast",
            "Reported additions use the reviewed five-year centred expansion; both stock and addition residuals are reported",
            "Forced service exits cannot be used as physical disposal; generation fails if such a date would be required",
            "Module handling losses and occasional module replacements require separate installation/maintenance roles in the inventory exporter",
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
    for name in ("eia8602022.zip", "f923_2022.zip"):
        if not verify(args.input_dir / name, records[name]):
            raise ValueError(f"Changed public source: {name}")
        sources.append(records[name])
    with (docs / "iam-file-inventory.csv").open() as stream:
        matches = [
            r
            for r in csv.DictReader(stream)
            if r["file"] == f"remind 3.5.2/{args.iam_file.name}"
        ]
    if len(matches) != 1:
        raise ValueError("IAM file is absent or ambiguous in reviewed manifest")
    iam = read_iam(
        args.iam_file,
        matches[0]["sha256"],
        args.scenario,
        args.end_year,
        variables=VARIABLES,
        required_lifetime=30,
    )
    pv = audit(args.input_dir)["pv"]
    report = generate(pv["complete_plant_candidates"], iam, args.end_year)
    report.update(
        public_sources=sources,
        observation_audit=pv,
        iam_source={
            "file": matches[0]["file"],
            "sha256": matches[0]["sha256"],
            "scenario": args.scenario,
            "variables": VARIABLES,
            "rights": "Restricted local input and derived series; do not redistribute",
        },
        iam_series=iam,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "plants": len(pv["complete_plant_candidates"]),
                "cases": list(report["cases"]),
                "years": [REFERENCE, args.end_year],
            }
        )
    )


if __name__ == "__main__":
    main()
