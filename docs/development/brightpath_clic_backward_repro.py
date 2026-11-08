"""Reproduce all observed CLIC migration blockers without Premise or licensed data.

Keep brightpath-clic-target-blockers.json beside this script. All exchange amounts
are synthetic unit quantities. This script never opens a Brightway project or
needs a CLIC token/archive. Exit 1 means at least one selected check failed.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import importlib.metadata
import json
from pathlib import Path
import sys

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background import (
    catalog_provider_from_environment,
    migration_resource_fingerprint,
    plan_background_migration,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction


def context(version, biosphere=None, *, family="ecoinvent", model="cutoff"):
    return BackgroundContext(
        technosphere=TechnosphereProfile(family, version, model),
        biosphere=BiosphereProfile("ecoinvent", biosphere or version),
    )


def policy():
    return replace(
        MigrationPolicy.strict(),
        on_inferred_reverse=PolicyAction.WARN,
        on_information_loss=PolicyAction.WARN,
        on_ambiguous_rule=PolicyAction.WARN,
    )


def run_link(case, *, target_biosphere=None):
    transition = case["first_transition"]
    source = context(transition["from"])
    target = context(transition["to"], target_biosphere)
    identity = case["fixture_source_identity"]
    exchange = dict(identity, amount=1.0)
    if "categories" in identity:
        exchange["type"] = "biosphere"
        exchange["categories"] = tuple(exchange["categories"])
    else:
        exchange["type"] = "technosphere"
        exchange["product"] = exchange.pop("reference_product")
        exchange["reference product"] = exchange["product"]
    data = [
        {
            "name": "compatibility test",
            "reference product": "test output",
            "location": "GLO",
            "unit": "kilogram",
            "database": "diagnostic",
            "code": "test",
            "type": "process",
            "exchanges": [
                {
                    "name": "compatibility test",
                    "product": "test output",
                    "reference product": "test output",
                    "location": "GLO",
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1.0,
                },
                exchange,
            ],
        }
    ]
    inventory = BrightwayInventory.from_data(
        data,
        database_name="diagnostic",
        context=InventoryContext(
            format=FormatProfile("brightway_excel", dialect="bw2io"), background=source
        ),
    ).normalize()
    result = {
        "case": case["id"],
        "source_technosphere": transition["from"],
        "source_biosphere": transition["from"],
        "target_technosphere": transition["to"],
        "target_biosphere": target_biosphere or transition["to"],
    }
    try:
        migrated = inventory.migrate_background(target, policy=policy())
        report = migrated.last_migration_report
        result["status"] = "passed"
        result["migrated_exchanges"] = migrated.data[0]["exchanges"]
    except Exception as error:
        report = getattr(error, "report", None)
        if report is None:
            raise
        result["status"] = "failed"
        result["message"] = str(error)
    serialized = json.loads(report.to_json())
    result["errors"] = [
        issue
        for stage in serialized["stages"]
        for issue in stage.get("issues", [])
        if issue["severity"] == "error"
    ]
    result["source_validation_errors"] = [
        issue
        for stage in serialized["stages"]
        if stage.get("metrics", {}).get("role") == "source"
        for issue in stage.get("issues", [])
        if issue["severity"] == "error"
    ]
    result["issue_codes"] = sorted(
        {
            issue["code"]
            for stage in serialized["stages"]
            for issue in stage.get("issues", [])
        }
    )
    return result


def run_routes(case, versions):
    results = []
    for version in versions:
        source = (
            context("2025", "3.10", family="uvek")
            if case == "ROUTE-01"
            else context("3.12")
        )
        plan = plan_background_migration(
            source, context(version, model="consequential"), policy=policy()
        )
        results.append(
            {
                "case": case,
                "target_version": version,
                "status": "passed" if plan.executable else "failed",
                "report": plan.report.to_dict(),
            }
        )
    return results


def run_catalog():
    result = {"case": "CATALOG-01", "target_version": "3.10.1"}
    try:
        catalog_provider_from_environment().load_technosphere(
            TechnosphereProfile("ecoinvent", "3.10.1", "consequential")
        )
        result["status"] = "passed"
    except FileNotFoundError as error:
        result.update(
            status="failed", exception_type=type(error).__name__, message=str(error)
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--issues",
        type=Path,
        default=Path(__file__).with_name("brightpath-clic-target-blockers.json"),
    )
    parser.add_argument(
        "--case",
        action="append",
        help="LINK-01 through LINK-38, ROUTE-01/02, CATALOG-01, or modern_biosphere_control",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    catalog = json.loads(args.issues.read_text(encoding="utf-8"))
    cases = {case["id"]: case for case in catalog["link_cases"]}
    others = {case["id"]: case for case in catalog["other_blockers"]}
    selected = args.case or [*cases, *others, "modern_biosphere_control"]
    results = []
    for name in selected:
        if name in cases:
            results.append(run_link(cases[name]))
        elif name.startswith("ROUTE-") and name in others:
            results.extend(run_routes(name, others[name]["target_versions"]))
        elif name == "CATALOG-01":
            results.append(run_catalog())
        elif name == "modern_biosphere_control":
            nitric = next(
                c
                for c in cases.values()
                if c["unresolved_identity"]["name"] == "Nitric acid"
            )
            result = run_link(nitric, target_biosphere="3.12")
            result["case"] = name
            results.append(result)
        else:
            parser.error(f"Unknown case: {name}")
    payload = {
        "brightpath_version": importlib.metadata.version("brightpath"),
        "migration_resource_fingerprint": migration_resource_fingerprint(),
        "python": sys.version.split()[0],
        "synthetic_quantities": True,
        "results": results,
    }
    text = json.dumps(payload, indent=2)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return int(any(row["status"] == "failed" for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
