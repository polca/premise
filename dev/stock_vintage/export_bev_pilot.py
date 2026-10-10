"""Export bounded passenger-BEV parent, battery and lifecycle timing.

The historical GLO vehicle technology proxies the observed UK stock's capital
timing. Preserve its original 1.5 equivalent battery packs and all signed ports.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path

from premise.stock_lifecycle import (
    scope_event_market,
    split_embedded_lifecycle,
    wrap_scoped_exchange,
)
from premise.stock_vintage import identity

if __package__:
    from .export_ccgt_pilot import export, load_source_inventory
else:
    from export_ccgt_pilot import export, load_source_inventory

CODES = {
    "caller": "a02324de7a448dd7b3a111f44bbe43f7",
    "market": "95cb62e15c32b1b3ab69f1baf8ab581a",
    "constructor": "7da6cdfd32c431ed2ba85292f4245601",
    "glider_market": "a08a7ecc4e634478f1f462dbc779153b",
    "glider_producer": "29a48be7895bd8dc309d8038e0550d2f",
    "powertrain_market": "fb1af8fe49d50543e1c4eafa1e50ef98",
    "powertrain_producer": "e97752f4497434685e1b3564f30167b9",
    "dismantling": "b0fd1b4c1f6ae4e070b3aab88f515752",
    "used_glider": "994f3b0ac8d9523e1a04f873414aea3c",
    "used_powertrain": "f8e6ec04b455187c9f2eb0ff5606fb96",
    "battery": "e1d755b861af4b3480d82c8e2d83ed26",
    "used_battery": "809a4e0018b2e9537c1e60908266285d",
    "maintenance": "a4a0486b7b56a2dabc996c2afee952dd",
}


def rewrite_bev(inventory):
    lookup = {d["code"]: d for d in inventory}
    by_identity = {identity(d): d["code"] for d in inventory}
    caller, market, constructor = [
        lookup[CODES[k]] for k in ("caller", "market", "constructor")
    ]
    if (caller["location"], caller["unit"], market["unit"]) != (
        "GLO",
        "kilometer",
        "kilogram",
    ):
        raise ValueError("Reviewed passenger-BEV identity or unit changed")
    boundary_path = Path(__file__).with_name("bev_boundary.json")
    boundary = json.loads(boundary_path.read_text())
    prefixes = {CODES["constructor"]: [market]}
    for part in ("glider", "powertrain"):
        prefixes[CODES[part + "_producer"]] = [
            market,
            constructor,
            lookup[CODES[part + "_market"]],
        ]
    paths = []
    for code, prefix in prefixes.items():
        producer, review = lookup[code], boundary["producers"][code]
        if (producer["name"], producer["location"]) != (
            review["name"],
            review["location"],
        ):
            raise ValueError("Reviewed BEV producer changed")
        negative = {
            e["input"][1]: e
            for e in producer["exchanges"]
            if e["type"] == "technosphere" and e["amount"] < 0
        }
        if negative.keys() != review["negative_ports"].keys():
            raise ValueError("Reviewed BEV factory/disposal boundary changed")
        for supplier, port in review["negative_ports"].items():
            if negative[supplier]["name"] != port["name"]:
                raise ValueError("Reviewed BEV disposal identity changed")
            if port["role"] == "parent_disposal":
                paths.append(prefix + [producer, lookup[supplier]])
            elif port["role"] != "retain_manufacturing_scrap":
                raise ValueError("Unreviewed BEV waste role")
    paths.append([market, constructor, lookup[CODES["dismantling"]]])
    updated, audit = split_embedded_lifecycle(
        inventory,
        caller=caller,
        paths=paths,
        context_id="uk-bev-pilot",
        uncertainty_mode="deterministic",
    )
    if not math.isclose(
        audit["preserved_capital_amount"], 0.00612146666666667, rel_tol=1e-12
    ):
        raise ValueError("Reviewed passenger-car amortisation changed")

    def event_market(data, review_audit, owner, code):
        node, review = lookup[code], boundary["event_markets"][code]
        if node["name"] != review["name"]:
            raise ValueError("Reviewed BEV event market changed")
        actual = {
            e["input"][1]
            for e in node["exchanges"]
            if e["type"] == "technosphere"
            and (
                lookup[e["input"][1]]["reference product"],
                lookup[e["input"][1]]["unit"],
            )
            == (node["reference product"], node["unit"])
        }
        if actual != {r["code"] for r in review["providers"]}:
            raise ValueError("Reviewed BEV event providers changed")
        providers = []
        for record in review["providers"]:
            provider = lookup[record["code"]]
            if (provider["name"], provider["location"]) != (
                record["name"],
                record["location"],
            ):
                raise ValueError("Reviewed BEV provider identity changed")
            providers.append(provider)
        return scope_event_market(
            data, review_audit, caller=owner, supplier=node, event_suppliers=providers
        )

    for row in list(audit["lifted_exchanges"]):
        code = by_identity[identity(row["original_supplier"])]
        if code in {
            CODES[k] for k in ("dismantling", "used_glider", "used_powertrain")
        }:
            updated, audit = event_market(updated, audit, row["supplier"], code)
    direct = []
    for name, physical_role, expected in [
        ("battery", "battery_manufacture", 0.00262),
        ("used_battery", "battery_disposal", -0.00262),
        ("maintenance", "maintenance", 1 / 150000),
    ]:
        updated, audit = event_market(updated, audit, audit["caller"], CODES[name])
        row = audit["event_markets"][-1]
        original_supplier = row["original_supplier"]
        if name == "used_battery":
            updated, audit = wrap_scoped_exchange(
                updated, audit, supplier=row["supplier"]
            )
            row = audit["direct_lifecycle"][-1]
        if not math.isclose(row["amount_per_calling_dataset"], expected, rel_tol=1e-12):
            raise ValueError("Reviewed BEV battery or maintenance quantity changed")
        direct.append(
            {
                "caller": row["caller"],
                "supplier": row["supplier"],
                "original_supplier": original_supplier,
                "physical_role": physical_role,
                "amount_per_calling_dataset": row["amount_per_calling_dataset"],
            }
        )
    audit["direct_ports"] = direct
    audit["boundary_manifest_sha256"] = hashlib.sha256(
        boundary_path.read_bytes()
    ).hexdigest()
    audit["boundaries"] = {
        "population": "Known UK BEV first-use cohorts from 2010, with explicit excluded early/unknown dates",
        "technology_proxy": "Historical GLO compact BEV with LiMn2O4 battery proxies timing; not contemporary fleet technology",
        "parent": "Glider and drivetrain manufacture follows first-use composition; their material subassemblies are new at assembly",
        "dismantling": "All 14 glass/oil/rubber ports at car assembly plus manual dismantling follow parent retirement; source unit is one dismantling unit per kg vehicle",
        "module_disposal": "Used-glider and used-powertrain outputs are also lifted; four glider manufacturing-scrap ports stay at manufacture",
        "battery": "Existing 1.5 equivalent 262 kg packs per 150000 km retained; active-generation dates are an assumed timing allocation, not physical pack counts",
        "maintenance": "Lifetime maintenance package without battery attributed at requested service year, including its maintenance waste",
        "event_markets": "Reviewed manufacture/maintenance/treatment provider links are instantaneous at the assigned event; source signed production retained",
        "manufacture_background": "Battery freight, factory capital and other independent background assets retain their original roles",
        "physical_retirement": "Separate conditional lifetime proxy; regional stock exits do not establish disposal",
        "uncertainty": "Deterministic coefficients; no Monte Carlo correlation equivalence claim",
    }
    return updated, audit


def profiles(report, audit, case):
    if report["group"] != "passenger_bev":
        raise ValueError("Require the bounded passenger-BEV cohort report")
    selected = report["cases"][case]
    parent = selected["annual"]
    service = [
        {
            "service_year": r["service_year"],
            "event_years": [r["service_year"]],
            "weights": [1.0],
        }
        for r in parent
    ]
    calendars = {
        "parent_construction": parent,
        "parent_disposal": selected["parent_retirement"],
        "battery_manufacture": selected["battery_manufacture"],
        "battery_disposal": selected["battery_retirement"],
        "maintenance": service,
    }
    provenance = {
        "evidence_tier": "observed_parent_stock_with_declared_component_service_and_future_assumptions",
        "public_sources": report["public_sources"],
        "iam_source": report["iam_source"],
        "assumptions": report["assumptions"],
        "boundaries": audit["boundaries"],
        "case": case,
        "observation_selection": selected["observation_selection"],
        "survival": selected["survival"],
        "weighting": selected["weighting"],
        "battery_interval_years": selected["battery_interval_years"],
        "battery_quantity_basis": selected["battery_quantity_basis"],
        "inventory_scope": "Constant ecoinvent 3.12 cutoff technology at anchors; not an IAM-transformed operating inventory",
    }
    result, bindings, expectations = [], [], []

    def bind(name, caller, supplier, role, records, amount=None, physical_role=None):
        years = [
            {k: r[k] for k in ("service_year", "event_years", "weights")}
            for r in records
        ]
        result.append(
            {
                "id": name,
                "event_role": role,
                "allocation_basis": "common_amortisation",
                "asset_unit": supplier["unit"],
                "service_unit": caller["unit"],
                "provenance": provenance,
                "years": years,
            }
        )
        bindings.append({"profile_id": name, "caller": caller, "supplier": supplier})
        if amount is not None:
            expectations.append(
                {
                    "profile_id": name,
                    "caller": caller,
                    "supplier": supplier,
                    "physical_role": physical_role,
                    "amount_per_calling_dataset": amount,
                    "annual": years,
                }
            )

    bind(
        "bev-parent",
        audit["caller"],
        audit["capital_root"],
        "existing_asset_service",
        parent,
        audit["preserved_capital_amount"],
        "parent_construction",
    )
    for i, row in enumerate(audit["lifted_exchanges"]):
        bind(
            f"bev-parent-end-{i}",
            row["caller"],
            row["supplier"],
            "lifecycle_service",
            calendars["parent_disposal"],
            row["amount_per_calling_dataset"],
            "parent_disposal",
        )
    for row in audit["direct_ports"]:
        physical = row["physical_role"]
        bind(
            f"bev-{physical}",
            row["caller"],
            row["supplier"],
            (
                "existing_asset_service"
                if physical == "battery_manufacture"
                else "lifecycle_service"
            ),
            calendars[physical],
            row["amount_per_calling_dataset"],
            physical,
        )
    first = min(min(y["event_years"]) for p in result for y in p["years"])
    last = max(max(y["event_years"]) for p in result for y in p["years"])
    zero = [
        {"service_year": y, "event_years": [y], "weights": [1.0]}
        for y in range(first, last + 1)
    ]
    for i, binding in enumerate(audit["zero_shift_bindings"]):
        bind(
            f"bev-internal-{i}",
            binding["caller"],
            binding["supplier"],
            "new_asset_component",
            zero,
        )
    return {
        "schema_version": 1,
        "inventory_context": {
            "source_database": "ecoinvent-3.12-cutoff",
            "source_version": "3.12",
            "system_model": "cutoff",
            "model": "remind",
            "pathway": report["iam_source"]["scenario"],
        },
        "profiles": result,
        "bindings": bindings,
    }, expectations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--cohorts", type=Path, required=True)
    parser.add_argument("--case", default="primary")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--skip-legacy", action="store_true")
    args = parser.parse_args()
    inventory, source_hash = load_source_inventory(args.inventory)
    updated, audit = rewrite_bev(inventory)
    report = json.loads(args.cohorts.read_text())
    payload, expectations = profiles(report, audit, args.case)
    anchors = sorted({report["reference_year"], 2025, report["last_service_year"]})
    packages = {}
    for variant, data, resource in [
        ("legacy", inventory, None),
        ("corrected", updated, payload),
    ]:
        if variant == "legacy" and args.skip_legacy:
            continue
        packages[variant] = export(
            data,
            args.output_dir.resolve() / variant,
            report,
            resource,
            anchors,
            f"bev_{variant}",
        )
    evidence = {
        "pilot": "uk-passenger-bev",
        "rights": "Restricted local ecoinvent and IAM-derived data; do not redistribute",
        "background": "constant technology at each anchor",
        "inventory_sha256": source_hash,
        "cohorts_sha256": hashlib.sha256(args.cohorts.read_bytes()).hexdigest(),
        "cohorts_path": str(args.cohorts.resolve()),
        "case": args.case,
        "source_activities": len(inventory),
        "exported_activities": len(updated),
        "anchors": anchors,
        "rewrite": audit,
        "packages": packages,
        "expected_exchanges": expectations,
        "legacy_role_suppliers": [
            {"supplier": r["original_supplier"], "role": r["physical_role"]}
            for r in audit["direct_ports"]
        ],
    }
    (args.output_dir / "export-evidence.json").write_text(
        json.dumps(evidence, indent=2, allow_nan=False) + "\n"
    )
    print(
        json.dumps(
            {
                "packages": packages,
                "anchors": anchors,
                "parent_end_ports": len(audit["lifted_exchanges"]),
            }
        )
    )


if __name__ == "__main__":
    main()
