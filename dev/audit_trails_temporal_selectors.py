"""Audit active TRAILS temporal selectors against installed ecoinvent databases.

This developer check requires local Brightway projects containing licensed
ecoinvent databases; it is therefore not part of the normal test suite. It
compares active crop selectors with an independent candidate set and audits
relationship-aware construction-timing selectors against actual caller-to-
infrastructure exchanges.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import bw2data

from premise.core.filesystem_constants import DATA_DIR
from premise.temporal_legacy import compile_legacy_rows
from premise.temporal_specs import TemporalRegistry

DEFAULT_VERSIONS = ("3.8", "3.9.1", "3.10", "3.11", "3.12")
CEREALS = ("barley", "maize", "oat", "rye", "spelt", "triticale", "wheat")
CROP_ISIC_PREFIXES = ("0111:", "0119:")
CEREAL_PATTERN = re.compile(r"\b(?:" + "|".join(CEREALS) + r")\b", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--versions",
        nargs="+",
        default=DEFAULT_VERSIONS,
        help="Ecoinvent versions to audit (default: %(default)s).",
    )
    parser.add_argument(
        "--project-template",
        default="ecoinvent-{version}-cutoff",
        help="Brightway project name template.",
    )
    parser.add_argument(
        "--database-template",
        default="ecoinvent-{version}-cutoff",
        help="Brightway database name template.",
    )
    parser.add_argument(
        "--json",
        type=Path,
        help="Optionally write the complete audit report as JSON.",
    )
    return parser.parse_args()


def activity_record(activity: Any) -> dict[str, Any]:
    record = dict(activity)
    record["classifications"] = record.get("classifications") or []
    return record


def exchange_record(
    exchange: Any, suppliers: dict[tuple[str, str], dict[str, Any]]
) -> dict[str, Any]:
    """Return an exchange record enriched with its supplier identity."""

    record = dict(exchange)
    supplier = suppliers[exchange["input"]]
    product = (
        record.get("product")
        or record.get("reference product")
        or supplier.get("reference product", "")
    )
    record.update(
        name=record.get("name") or supplier.get("name", ""),
        reference_product=product,
        product=product,
        location=supplier.get("location", ""),
        unit=record.get("unit") or supplier.get("unit", ""),
    )
    return record


def identity(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        record.get("name", ""),
        record.get("reference product", record.get("reference_product", "")),
        record.get("location", ""),
    )


def display_identity(record: dict[str, Any]) -> dict[str, str]:
    name, product, location = identity(record)
    return {"name": name, "reference_product": product, "location": location}


def direct_biogenic_uptake(activity: Any) -> list[float]:
    """Return positive ``Carbon dioxide, in air`` natural-resource inputs."""
    return [
        float(exchange.get("amount") or 0)
        for exchange in activity.biosphere()
        if exchange.get("name") == "Carbon dioxide, in air"
        and float(exchange.get("amount") or 0) > 0
    ]


def is_cereal_crop_production(record: dict[str, Any]) -> bool:
    name = record.get("name", "")
    if "production" not in name.casefold() or not CEREAL_PATTERN.search(name):
        return False
    return any(
        system == "ISIC rev.4 ecoinvent" and value.startswith(CROP_ISIC_PREFIXES)
        for system, value in record.get("classifications", [])
    )


def legacy_biomass_identities() -> set[tuple[str, str]]:
    rows = compile_legacy_rows(DATA_DIR / "trails" / "temporal")
    return {
        (row["name"], row["reference product"])
        for row in rows
        if row["temporal_tag"] == "biomass_growth"
    }


def is_independent_candidate(
    record: dict[str, Any],
    uptake: list[float],
    legacy_biomass: set[tuple[str, str]],
) -> bool:
    if not uptake:
        return False
    name, product, _ = identity(record)
    return (
        (name, product) in legacy_biomass and bool(CEREAL_PATTERN.search(name))
    ) or is_cereal_crop_production(record)


def audit_version(
    version: str,
    project: str,
    database_name: str,
    registry: TemporalRegistry,
    legacy_biomass: set[tuple[str, str]],
) -> dict[str, Any]:
    bw2data.projects.set_current(project)
    database = bw2data.Database(database_name)
    activities = list(database)
    records_by_key = {
        activity.key: activity_record(activity) for activity in activities
    }
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    candidates: dict[tuple[str, str, str], dict[str, str]] = {}
    counts: Counter[str] = Counter()
    relationship_counts: Counter[str] = Counter()

    for activity in activities:
        record = records_by_key[activity.key]
        uptake = direct_biogenic_uptake(activity)
        resolution = registry.resolve(
            record,
            target="activity",
            context={"ecoinvent_version": version},
        )
        key = identity(record)
        if resolution is not None:
            counts[resolution.assignment_id] += 1
            if registry.profiles[resolution.profile_id]["quantity"] == "biomass_growth":
                selected[key] = {
                    **display_identity(record),
                    "assignment": resolution.assignment_id,
                    "uptake_amounts": uptake,
                }
        if is_independent_candidate(record, uptake, legacy_biomass):
            candidates[key] = display_identity(record)
        caller_assignments = registry.matching_caller_assignments(
            record,
            target="supplier",
            context={"ecoinvent_version": version},
        )
        if not caller_assignments:
            continue
        for exchange in activity.technosphere():
            supplier_record = exchange_record(exchange, records_by_key)
            relationship = registry.resolve(
                supplier_record,
                target="supplier",
                context={"ecoinvent_version": version},
                caller_record=record,
            )
            if relationship is not None:
                counts[relationship.assignment_id] += 1
                relationship_counts[relationship.assignment_id] += 1

    missing = [candidates[key] for key in sorted(candidates.keys() - selected.keys())]
    extra = [selected[key] for key in sorted(selected.keys() - candidates.keys())]
    without_uptake = [item for item in selected.values() if not item["uptake_amounts"]]
    expected_failures = []
    for assignment in registry.assignments:
        applicability = assignment.get("applicability", {})
        if version not in applicability.get("ecoinvent_versions", []):
            continue
        minimum = assignment.get("expected_matches", {}).get("minimum")
        actual = counts[assignment["id"]]
        if minimum is not None and actual < minimum:
            expected_failures.append(
                {"assignment": assignment["id"], "minimum": minimum, "actual": actual}
            )

    return {
        "version": version,
        "project": project,
        "database": database_name,
        "database_activities": len(database),
        "selector_matches": len(selected),
        "all_matches_by_assignment": dict(sorted(counts.items())),
        "relationship_matches_by_assignment": dict(sorted(relationship_counts.items())),
        "independent_candidates": len(candidates),
        "matches_by_assignment": dict(sorted(counts.items())),
        "candidate_misses": missing,
        "selected_outside_candidate_set": extra,
        "selected_without_uptake": without_uptake,
        "expected_match_failures": expected_failures,
    }


def print_report(reports: list[dict[str, Any]]) -> None:
    print("version  activities  selected  candidates  misses  extras  no-uptake")
    for report in reports:
        print(
            f"{report['version']:<8}"
            f"{report['database_activities']:>11}"
            f"{report['selector_matches']:>10}"
            f"{report['independent_candidates']:>12}"
            f"{len(report['candidate_misses']):>8}"
            f"{len(report['selected_outside_candidate_set']):>8}"
            f"{len(report['selected_without_uptake']):>11}"
        )
        electricity_counts = {
            assignment: count
            for assignment, count in report["all_matches_by_assignment"].items()
            if "service-life" in assignment
        }
        if electricity_counts:
            print(f"  service_life_matches: {electricity_counts}")
        relationship_counts = report["relationship_matches_by_assignment"]
        if relationship_counts:
            print(f"  construction_timing_matches: {relationship_counts}")
        for field in (
            "candidate_misses",
            "selected_outside_candidate_set",
            "selected_without_uptake",
            "expected_match_failures",
        ):
            if report[field]:
                print(f"  {field}: {report[field]}")


def main() -> int:
    args = parse_args()
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    legacy_biomass = legacy_biomass_identities()
    available_projects = {project.name for project in bw2data.projects}
    reports = []
    missing_projects = []

    for version in args.versions:
        project = args.project_template.format(version=version)
        if project not in available_projects:
            missing_projects.append(project)
            continue
        reports.append(
            audit_version(
                version,
                project,
                args.database_template.format(version=version),
                registry,
                legacy_biomass,
            )
        )

    print_report(reports)
    if missing_projects:
        print(f"Missing Brightway projects: {', '.join(missing_projects)}")
    if args.json:
        args.json.write_text(json.dumps(reports, indent=2) + "\n", encoding="utf-8")

    failures = missing_projects or any(
        report["candidate_misses"]
        or report["selected_outside_candidate_set"]
        or report["selected_without_uptake"]
        or report["expected_match_failures"]
        for report in reports
    )
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
