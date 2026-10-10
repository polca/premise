"""Export the bounded truck timing pilot with maintenance and physical disposal.

The RER EURO6 operating inventory is a declared technology proxy for the UK
32–40 tonne diesel capital cohorts. Keep all original source coefficients.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path

from premise.stock_lifecycle import scope_event_market, split_embedded_lifecycle
from premise.stock_vintage import identity

if __package__:
    from .export_ccgt_pilot import export, load_source_inventory
else:
    from export_ccgt_pilot import export, load_source_inventory

CODES = {
    "caller": "14ef57aa246952105aa2e6c5c5e32f36",
    "market": "6ef32683d04ca65bb1345468dc8dcba5",
    "constructor": "d989e6f9515d9e2ca291436755880c08",
    "used_lorry": "feaa6a90af6c4675516a9b263df9d18e",
    "maintenance": "c703a8565570258b358d3227a0a597ce",
    "factory": "63636c5301f04645fa47754f552b2ff1",
}
MAINTENANCE = ["b56d9bef61ed6d7bef34be0238894971", "6666dbccf94ada1380779b30da5a752d"]
TREATMENTS = ["c5c1f9c214cbaba251a18bee708b2d6c", "63198f32414eb647e140acca9d7b8664"]
MANUFACTURING_WASTE = {
    "7caa8b6e21cad0b9b15fe36de647b21a",
    "07861136669ab1caa5d46d496c0a14b7",
    "0c50d83e7f0ce3e01d262dc7a0b76cfd",
}


def rewrite_trucks(inventory):
    lookup = {d["code"]: d for d in inventory}
    caller, market, constructor, used, maintenance = [
        lookup[CODES[k]]
        for k in ("caller", "market", "constructor", "used_lorry", "maintenance")
    ]
    if (caller["location"], caller["unit"], market["unit"], used["unit"]) != (
        "RER",
        "ton kilometer",
        "unit",
        "unit",
    ):
        raise ValueError("Reviewed truck identity or unit changed")
    negative = {
        e["input"][1]
        for e in constructor["exchanges"]
        if e["type"] == "technosphere" and e["amount"] < 0
    }
    if negative != MANUFACTURING_WASTE | {CODES["used_lorry"]}:
        raise ValueError("Reviewed truck factory/disposal boundary changed")
    for node, expected in ((maintenance, MAINTENANCE), (used, TREATMENTS)):
        if {
            e["input"][1] for e in node["exchanges"] if e["type"] == "technosphere"
        } != set(expected):
            raise ValueError("Reviewed truck event-market providers changed")
    updated, audit = split_embedded_lifecycle(
        inventory,
        caller=caller,
        paths=[[market, constructor, used]],
        context_id="uk-truck-pilot",
        uncertainty_mode="deterministic",
    )
    if not math.isclose(audit["preserved_capital_amount"], 9.65e-8, rel_tol=1e-12):
        raise ValueError("Reviewed truck common-amortisation coefficient changed")
    adapter = audit["lifted_exchanges"][0]["supplier"]
    updated, audit = scope_event_market(
        updated,
        audit,
        caller=adapter,
        supplier=used,
        event_suppliers=[lookup[c] for c in TREATMENTS],
    )
    updated, audit = scope_event_market(
        updated,
        audit,
        caller=audit["caller"],
        supplier=maintenance,
        event_suppliers=[lookup[c] for c in MAINTENANCE],
    )
    audit["boundaries"] = {
        "population": "UK articulated road-using diesel goods vehicles with 32 < maximum gross weight <= 40 tonnes; no observed EURO class",
        "technology_proxy": "RER >32-tonne diesel EURO6 service with 40-tonne lorry capital; timing experiment, not an observed EURO6 fleet average",
        "parent": "Existing parent manufacture follows service-weighted first-use cohorts",
        "disposal": "Original used-lorry quantity is lifted from manufacture to conditional physical retirement; territorial exits do not define scrapping",
        "maintenance": "Original lifetime maintenance package attributed at service, including its materials and maintenance waste; no additional maintenance lifetime spread",
        "event_markets": "Scoped used-lorry and maintenance markets bind their reviewed providers at the event year; negative reference production retained",
        "manufacturing_waste": "All three wastewater ports remain at manufacture; they are not used-lorry disposal",
        "independent_capital": "Road vehicle factory and road infrastructure retain their own background temporal roles",
        "quantity": "Original 9.65e-8 lorry units and maintenance units per tkm retained; no new survival or lifetime multiplier",
        "uncertainty": "Deterministic equivalence; lifted products do not preserve Monte Carlo correlations",
    }
    return updated, audit


def profiles(report, audit, case):
    if report["group"] != "heavy_trucks_32_40t":
        raise ValueError("Require the bounded articulated-truck cohort report")
    selected = report["cases"][case]
    parent, retirement = selected["annual"], selected["parent_retirement"]
    service = [
        {
            "service_year": r["service_year"],
            "event_years": [r["service_year"]],
            "weights": [1.0],
        }
        for r in parent
    ]
    provenance = {
        "evidence_tier": "observed_parent_cohorts_with_declared_service_survival_and_future_proxies",
        "public_sources": report["public_sources"],
        "iam_source": report["iam_source"],
        "assumptions": report["assumptions"],
        "boundaries": audit["boundaries"],
        "case": case,
        "observation_selection": selected["observation_selection"],
        "survival": selected["survival"],
        "weighting": selected["weighting"],
        "allocation_basis": "common_amortisation",
        "exchange_amount_changed": False,
        "inventory_scope": "Constant ecoinvent 3.12 cut-off coefficients at anchors; not an IAM-transformed operating inventory",
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
        "truck-parent",
        audit["caller"],
        audit["capital_root"],
        "existing_asset_service",
        parent,
        audit["preserved_capital_amount"],
        "parent_construction",
    )
    for i, row in enumerate(audit["lifted_exchanges"]):
        bind(
            f"truck-disposal-{i}",
            row["caller"],
            row["supplier"],
            "lifecycle_service",
            retirement,
            row["amount_per_calling_dataset"],
            "parent_disposal",
        )
    maintenance = [
        r
        for r in audit["event_markets"]
        if identity(r["caller"]) == identity(audit["caller"])
    ]
    if len(maintenance) != 1:
        raise ValueError("Require one scoped direct maintenance input")
    row = maintenance[0]
    bind(
        "truck-maintenance",
        row["caller"],
        row["supplier"],
        "lifecycle_service",
        service,
        row["amount_per_calling_dataset"],
        "maintenance",
    )
    first = min(min(y["event_years"]) for p in result for y in p["years"])
    last = max(max(y["event_years"]) for p in result for y in p["years"])
    zero = [
        {"service_year": y, "event_years": [y], "weights": [1.0]}
        for y in range(first, last + 1)
    ]
    for i, binding in enumerate(audit["zero_shift_bindings"]):
        bind(
            f"truck-internal-{i}",
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
    updated, audit = rewrite_trucks(inventory)
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
            f"truck_{variant}",
        )
    evidence = {
        "pilot": "uk-articulated-diesel-truck",
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
    }
    (args.output_dir / "export-evidence.json").write_text(
        json.dumps(evidence, indent=2, allow_nan=False) + "\n"
    )
    print(
        json.dumps(
            {
                "packages": packages,
                "anchors": anchors,
                "event_markets": len(audit["event_markets"]),
            }
        )
    )


if __name__ == "__main__":
    main()
