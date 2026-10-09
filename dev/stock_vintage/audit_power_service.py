"""Audit EIA stock/output joins and bounded power-pilot candidates.

This reports coverage and unresolved allocation choices. It does not promote a
service profile, infer a block construction date, or divide a plant's output
equally among generators. All source files are checked against the manifest.
"""

import argparse
from collections import Counter, defaultdict
import csv
from io import BytesIO
import json
import math
from pathlib import Path
from zipfile import ZipFile

import openpyxl

if __package__:
    from .acquire_sources import verify
    from .curate_observations import eia_generators, xlsx_records
else:
    from acquire_sources import verify
    from curate_observations import eia_generators, xlsx_records


def generator_id(value):
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value).strip()


def numeric_alias(value):
    value = generator_id(value)
    return str(int(value)) if value.isdigit() else value


def match_generation(stock, generation):
    """Allow numeric padding only after proving one-to-one plant identities."""
    source_groups, target_groups = defaultdict(list), defaultdict(list)
    for key in generation:
        source_groups[(key[0], numeric_alias(key[1]))].append(key)
    for row in stock:
        key = (row["plant_code"], generator_id(row["generator_id"]))
        target_groups[(key[0], numeric_alias(key[1]))].append(key)
    matched, aliases, missing = {}, [], []
    for row in stock:
        key = (row["plant_code"], generator_id(row["generator_id"]))
        if key in generation:
            matched[key] = generation[key]
            continue
        alias = (key[0], numeric_alias(key[1]))
        candidates = source_groups.get(alias, [])
        if not candidates:
            missing.append(key)
            continue
        if len(candidates) != 1 or len(target_groups[alias]) != 1:
            raise ValueError(f"Ambiguous generator alias: {alias}")
        matched[key] = generation[candidates[0]]
        aliases.append({"stock_identity": key, "generation_identity": candidates[0]})
    return matched, aliases, missing


def records_923(workbook, sheet):
    rows = workbook[sheet].iter_rows(values_only=True)
    for _ in range(5):
        next(rows)
    header = next(rows)
    for values in rows:
        if values[0] is not None:
            yield dict(zip(header, values))


def number(value):
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    return None


def complete_block_membership(candidates, members, reference_year):
    """Reject hidden same-unit members and partial commissioning-year output.

    Membership must include every generator sheet, not just the operating
    technology subset. This deliberately excludes mixed and rebuilt blocks;
    it does not infer a whole-block construction date from component dates.
    """
    accepted, rejected = [], []
    for block in candidates:
        key = (block["plant_code"], block["unit_code"])
        rows = members.get(key, [])
        ids = [generator_id(r["Generator ID"]) for r in rows]
        reasons = []
        if len(ids) != len(set(ids)) or set(ids) != set(block["generator_ids"]):
            reasons.append("unexpected_or_duplicate_unit_members")
        for row in rows:
            if (
                row["sheet"] != "Operable"
                or row["Status"] != "OP"
                or row["Technology"] != "Natural Gas Fired Combined Cycle"
                or row["Energy Source 1"] != "NG"
                or row["Associated with Combined Heat and Power System"] != "N"
                or row.get("Operating Year") != block["cohort_year"]
                or row["Prime Mover"] not in {"CT", "CA", "CS"}
            ):
                reasons.append("incompatible_unit_member")
        if block["cohort_year"] >= reference_year:
            reasons.append("partial_initial_year_output")
        if reasons:
            rejected.append({**block, "membership_exclusions": sorted(set(reasons))})
        else:
            accepted.append(block)
    return accepted, rejected


def audit(root, year=2022):
    observations = {
        row["group"]: row for row in eia_generators(root / f"eia860{year}.zip", year)
    }
    gas = observations["gas_power"]["generator_records"]
    pv = observations["pv"]["generator_records"]
    plants = {row["plant_code"] for row in gas + pv}
    generation, pv_generation = {}, {}
    with ZipFile(root / f"f923_{year}.zip") as archive:
        name = next(n for n in archive.namelist() if "Schedules_2_3_4_5" in n)
        workbook = openpyxl.load_workbook(
            BytesIO(archive.read(name)), read_only=True, data_only=True
        )
        for row in records_923(workbook, "Page 4 Generator Data"):
            if row["Plant Id"] not in plants:
                continue
            key = (int(row["Plant Id"]), generator_id(row["Generator Id"]))
            if key in generation:
                raise ValueError(f"Duplicate scoped generation identity: {key}")
            generation[key] = number(row["Net Generation\nYear To Date"])
        for row in records_923(workbook, "Page 1 Generation and Fuel Data"):
            if row["Plant Id"] not in plants or row["Reported\nPrime Mover"] != "PV":
                continue
            key = int(row["Plant Id"])
            if key in pv_generation:
                raise ValueError(f"Duplicate scoped PV plant output: {key}")
            pv_generation[key] = number(row["Net Generation\n(Megawatthours)"])
        workbook.close()
    matched, aliases, missing = match_generation(gas, generation)
    blocks = defaultdict(list)
    for row in gas:
        blocks[(row["plant_code"], str(row["unit_code"]).strip())].append(row)
    eligible, excluded = [], Counter()
    for (plant, unit), rows in sorted(blocks.items()):
        capacity = math.fsum(r["capacity_mw"] for r in rows)
        movers = {r["prime_mover"] for r in rows}
        cohorts = {r["cohort_year"] for r in rows}
        values = [matched.get((plant, generator_id(r["generator_id"]))) for r in rows]
        reason = (
            "unknown_unit_code"
            if not unit
            else (
                "incomplete_combined_cycle"
                if movers not in ({"CA", "CT"}, {"CS"})
                else (
                    "mixed_commissioning_years"
                    if len(cohorts) != 1
                    else (
                        "missing_generation"
                        if any(v is None for v in values)
                        else (
                            "nonpositive_block_generation"
                            if math.fsum(values) <= 0
                            else None
                        )
                    )
                )
            )
        )
        if reason:
            excluded[reason] += capacity
            continue
        eligible.append(
            {
                "plant_code": plant,
                "unit_code": unit,
                "cohort_year": next(iter(cohorts)),
                "capacity_mw_ac": capacity,
                "net_generation_mwh": math.fsum(values),
                "generator_count": len(rows),
                "generator_ids": sorted(generator_id(r["generator_id"]) for r in rows),
                "zero_or_negative_component_outputs": sum(v <= 0 for v in values),
            }
        )
    members = defaultdict(list)
    candidate_keys = {(b["plant_code"], b["unit_code"]) for b in eligible}
    with ZipFile(root / f"eia860{year}.zip") as archive:
        for sheet in ("Operable", "Retired and Canceled", "Proposed"):
            for _, row in xlsx_records(archive, f"3_1_Generator_Y{year}.xlsx", sheet):
                key = (row["Plant Code"], str(row["Unit Code"]).strip())
                if key in candidate_keys:
                    members[key].append({**row, "sheet": sheet})
    eligible, membership_exclusions = complete_block_membership(eligible, members, year)
    for block in membership_exclusions:
        excluded["failed_full_membership_or_year_check"] += block["capacity_mw_ac"]
    # PV material/mounting limits are explicit. EIA's crystalline category does
    # not separate mono-Si and multi-Si, or prove ground versus rooftop mounting.
    pv_lookup = {(r["plant_code"], generator_id(r["generator_id"])): r for r in pv}
    selected = []
    with ZipFile(root / f"eia860{year}.zip") as archive:
        for _, row in xlsx_records(archive, f"3_3_Solar_Y{year}.xlsx", "Operable"):
            key = (row["Plant Code"], generator_id(row["Generator ID"]))
            if key not in pv_lookup:
                continue
            if row["Crystalline Silicon?"] != "Y" or row["Fixed Tilt?"] != "Y":
                continue
            if any(
                row[k] == "Y"
                for k in (
                    "Single-Axis Tracking?",
                    "Dual-Axis Tracking?",
                    "Thin-Film (CdTe)?",
                    "Thin-Film (A-Si)?",
                    "Thin-Film (CIGS)?",
                    "Thin-Film (Other)?",
                    "Other Materials?",
                )
            ):
                continue
            selected.append(
                {
                    **pv_lookup[key],
                    "dc_capacity_mw": number(row["DC Net Capacity (MW)"]),
                }
            )
    by_plant = defaultdict(list)
    for row in selected:
        by_plant[row["plant_code"]].append(row)
    all_pv = defaultdict(set)
    for row in pv:
        all_pv[row["plant_code"]].add(generator_id(row["generator_id"]))
    plant_candidates = []
    for plant, rows in sorted(by_plant.items()):
        plant_candidates.append(
            {
                "plant_code": plant,
                "selected_capacity_mw_ac": math.fsum(r["capacity_mw"] for r in rows),
                "net_pv_generation_mwh": pv_generation.get(plant),
                "all_operating_pv_matches_selected_technology": all_pv[plant]
                == {generator_id(r["generator_id"]) for r in rows},
                "commissioning_years": sorted({r["cohort_year"] for r in rows}),
            }
        )
    return {
        "stage": "join_and_boundary_audit_not_approved_profiles",
        "year": year,
        "geography": "US-WECC",
        "ccgt": {
            "initial_capacity_mw_ac": observations["gas_power"]["total_stock"],
            "numeric_identity_aliases": aliases,
            "unmatched_generators": missing,
            "candidate_blocks": eligible,
            "membership_exclusions": membership_exclusions,
            "membership_sheets_checked": [
                "Operable",
                "Retired and Canceled",
                "Proposed",
            ],
            "excluded_capacity_mw_ac": dict(excluded),
            "candidate_capacity_mw_ac": math.fsum(
                r["capacity_mw_ac"] for r in eligible
            ),
            "candidate_net_generation_mwh": math.fsum(
                r["net_generation_mwh"] for r in eligible
            ),
            "remaining_checks": [
                "Shared initial operation year is a construction-date proxy; later refurbishments are not identified",
                "Calendar-year generation weights end-year operating stock; within-year availability is not reconstructed",
                "Compare component-capacity and complete-block cohort sensitivities",
            ],
        },
        "pv": {
            "initial_capacity_mw_ac": observations["pv"]["total_stock"],
            "fixed_crystalline_capacity_mw_ac": math.fsum(
                r["capacity_mw"] for r in selected
            ),
            "candidate_generators": selected,
            "plant_candidates": plant_candidates,
            "remaining_checks": [
                "Retired and nonoperating same-plant PV can contribute to annual output",
                "Mixed commissioning years need within-plant output allocation and part-year exposure",
                "Crystalline silicon does not establish mono versus multi-Si",
                "Fixed tilt does not establish open-ground mounting",
                "Reconcile DC kWp inventory basis with AC stock and future capacity inputs",
            ],
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = (
        Path(__file__).resolve().parents[2]
        / "docs/development/stock-asset-temporal/acquisition-manifest.csv"
    )
    with manifest.open(newline="") as stream:
        records = {r["local_research_filename"]: r for r in csv.DictReader(stream)}
    sources = []
    for filename in ("eia8602022.zip", "f923_2022.zip"):
        record = records[filename]
        if not verify(args.input_dir / filename, record):
            raise ValueError(f"Changed source: {filename}")
        sources.append(record)
    report = {"sources": sources, **audit(args.input_dir)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            {
                "ccgt_candidate_capacity_mw_ac": report["ccgt"][
                    "candidate_capacity_mw_ac"
                ],
                "pv_fixed_crystalline_capacity_mw_ac": report["pv"][
                    "fixed_crystalline_capacity_mw_ac"
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
