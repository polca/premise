"""Export the real Quebec water-network pilot with separate pipes and tanks.

All source coefficients, own-product losses and geographic waste ports remain.
Only explicitly reviewed exchanges receive revised timing in scoped copies.
"""

import argparse
import hashlib
import json
from pathlib import Path

from premise.stock_lifecycle import split_embedded_lifecycle
from premise.stock_vintage import identity

try:
    from .export_ccgt_pilot import export, load_source_inventory
except ImportError:
    from export_ccgt_pilot import export, load_source_inventory

CODES = {
    "caller": "f89f17b92a4c207c708fae02a3ccd890",
    "market": "ee37b146a20a800628e5dbe95bf809ed",
    "constructor": "31b5b1b80a306de7cb628978f285fa9b",
}
# Reviewed source comments identify eight concrete markets and reinforcing steel
# as the tank portion. All other construction inputs retain the pipe/network date.
TANK_MATERIALS = {
    "7cd1142e07e11c0d6348f4fabf190b8a",
    "665067e2e7b61744ac10ae503ebd4f97",
    "0b3d9725a29d05769a1c67e8f83db8e3",
    "bf788bfaa897d0f18d748fd7f764b811",
    "8d7ccbd0f23a9416d6a4fcd6ab757aa4",
    "5140c3d68f36e6fbb0558e4a48b29997",
    "a0dfedc9bcf703f67034c96e311a0ed8",
    "cacdddcf47fb2405967a6378469c0893",
    "90b1aebe430db73035f8757d5a7d86cc",
}
TANK_DISPOSAL = {
    "576b86e7d0c0989532c2e82a352080c9",
    "9b432d92b47bd8aaebff26a8d81b808c",
}
PIPE_DISPOSAL = {
    "80ffaed2edf7eff73c8252b2172e3a69",
    "b7eb02393586a8e2e46f312d316826a0",
    "abdb0593b2a91e0f82b5d2ab8683d498",
    "da2df6e7d0a3403c352ff09a8d2edbde",
    "eaeea1dff047353d3699d0c640cbe171",
    "7ebaaf4ca1aa185084f5c92119ef77d8",
    "4e7934c863cfa0c715fe23f213970f07",
    "e56e64807f4675b0a62c380eb236d013",
    "ea3bc391c0ed4c29fea67959b6f1c025",
    "6e3e16e9faed916ce6681b75a99687c7",
    "bc6724a19b80c387c0dfe698c51ef1a8",
    "3b1eabbc0c25e1bc0a73612547678e32",
    "24696752c2fcb6f7721971bbf677cf9b",
    "005d64b99c166c5c142b21ecc88a5a13",
    "99502c4ceda0cb1b6791cab41b0232b8",
    "e456610b61766a91cdfd0e265278a1c4",
    "ab434167448917d621c6cc05d957f3f5",
    "97d036ed54e62ae44169de8ab2daf9d7",
    "959f10b6345ef5bef79976217abf2127",
    "1fb0bc00f420198aae5ed6b1638a0121",
    "d5c136bef12011b61a94c384f3e1ae6d",
    "f1cfa74b8829e0b9a1ded34eca4814f5",
}
RIGHTS = "Restricted local ecoinvent-derived inventories; Statistics Canada observations under its Open Licence. Do not redistribute inventory packages."


def rewrite_water(inventory):
    by_code = {d["code"]: d for d in inventory}
    caller, market, constructor = [by_code[CODES[k]] for k in CODES]
    if (caller["location"], caller["unit"], market["unit"]) != (
        "CA-QC",
        "kilogram",
        "kilometer",
    ):
        raise ValueError("Water pilot identity or unit changed")
    edges = {
        e["input"][1]: e
        for e in constructor["exchanges"]
        if e["type"] == "technosphere"
    }
    # A version change requires renewed boundary review, not a new regex match.
    if {
        c for c, e in edges.items() if e["amount"] < 0
    } != PIPE_DISPOSAL | TANK_DISPOSAL:
        raise ValueError("Reviewed water disposal boundary changed")
    if not TANK_MATERIALS <= edges.keys() or any(
        edges[c]["amount"] <= 0 for c in TANK_MATERIALS
    ):
        raise ValueError("Reviewed tank construction boundary changed")
    chosen = TANK_MATERIALS | PIPE_DISPOSAL | TANK_DISPOSAL
    paths = [[market, constructor, by_code[c]] for c in sorted(chosen)]
    updated, audit = split_embedded_lifecycle(
        inventory,
        caller=caller,
        paths=paths,
        context_id="quebec-water-pilot",
        uncertainty_mode="deterministic",
    )
    by_identity = {identity(d): d["code"] for d in inventory}
    for row in audit["lifted_exchanges"]:
        code = by_identity[identity(row["original_supplier"])]
        row["reviewed_source_code"] = code
        row["water_role"] = (
            "tank_construction"
            if code in TANK_MATERIALS
            else "tank_disposal" if code in TANK_DISPOSAL else "pipe_disposal"
        )
    audit["boundaries"] = {
        "pipe_materials": "Remaining network construction inputs and excavation",
        "tank_materials": "Eight geographic concrete ports and reinforcing steel, per source tank comments",
        "tank_disposal": "Two geographic reinforced-concrete waste ports; embedded reinforcement is not disposed twice",
        "pipe_disposal": "All 22 other reviewed negative construction ports; original source waste categories retained",
        "operation": "Water production and market own-product losses retained at the service caller",
        "components": "Material inputs are new materials at installation, not independently aged stocks",
        "retirement": "Conditional physical end of life under explicit survival assumptions; no observed demolition delay",
    }
    return updated, audit


def profiles(report, audit, case):
    selected = report["cases"][case]["assets"]
    provenance = {
        "evidence_tier": report["evidence_tier"],
        "sources": report["sources"],
        "observation_geography": "CA-QC",
        "manufacturing_proxy": "ecoinvent RoW network",
        "assumptions": report["assumptions"],
        "sensitivity_case": case,
        "inventory_scope": "constant ecoinvent 3.12 cut-off, deterministic coefficients",
        "boundaries": audit["boundaries"],
        "amortisation": "Unchanged original capital, tank and disposal coefficients; caller own-product loss remains in net reference production",
        "models": {
            k: {
                f: v[f]
                for f in (
                    "survival",
                    "annual_stock_growth",
                    "utilisation_age_scale",
                    "initial_reconciliation",
                )
            }
            for k, v in selected.items()
        },
    }
    result, bindings, expectations = [], [], []

    def bind(name, caller, supplier, role, records, amount=None, physical_role=None):
        years = [
            {k: row[k] for k in ("service_year", "event_years", "weights")}
            for row in records
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
        "water-pipe-construction",
        audit["caller"],
        audit["capital_root"],
        "existing_asset_service",
        selected["pipes"]["annual"],
        audit["preserved_capital_amount"],
        "pipe_construction",
    )
    for i, row in enumerate(audit["lifted_exchanges"]):
        asset = "pipes" if row["water_role"] == "pipe_disposal" else "storage"
        records = "annual" if row["water_role"] == "tank_construction" else "retirement"
        role = "existing_asset_service" if records == "annual" else "lifecycle_service"
        bind(
            f"water-lifted-{i}",
            row["caller"],
            row["supplier"],
            role,
            selected[asset][records],
            row["amount_per_calling_dataset"],
            row["water_role"],
        )
    first = min(min(y["event_years"]) for p in result for y in p["years"])
    last = max(max(y["event_years"]) for p in result for y in p["years"])
    zero = [
        {"service_year": y, "event_years": [y], "weights": [1.0]}
        for y in range(first, last + 1)
    ]
    for i, binding in enumerate(audit["zero_shift_bindings"]):
        bind(
            f"water-internal-{i}",
            binding["caller"],
            binding["supplier"],
            "new_asset_component",
            zero,
        )
    payload = {
        "schema_version": 1,
        "inventory_context": {
            "source_database": "ecoinvent-3.12-cutoff",
            "source_version": "3.12",
            "system_model": "cutoff",
            "model": "observed",
            "pathway": "quebec-water-replacement",
        },
        "profiles": result,
        "bindings": bindings,
    }
    return payload, expectations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--cohorts", type=Path, required=True)
    parser.add_argument("--case", default="primary")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--skip-legacy", action="store_true")
    args = parser.parse_args()
    args.output_dir = args.output_dir.resolve()
    inventory, source_hash = load_source_inventory(args.inventory)
    updated, audit = rewrite_water(inventory)
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
            args.output_dir / variant,
            report,
            resource,
            anchors,
            f"water_{variant}",
            model="observed",
            pathway="quebec-water-replacement",
        )
    evidence = {
        "rights": RIGHTS,
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
                "lifted_ports": len(audit["lifted_exchanges"]),
            }
        )
    )


if __name__ == "__main__":
    main()
