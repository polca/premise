"""Export the bounded real PV pilot with parent, replacement and disposal dates.

Preserve all original signed quantities and common amortisation. Small panel
replacements are expected annual maintenance, not observed replacement cohorts.
Mixed factory/end-of-life polymer waste and conflicting packaging comments are
explicit timing assumptions with endpoint sensitivities.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path

from premise.stock_lifecycle import lift_scoped_biosphere, split_embedded_lifecycle
from premise.stock_vintage import identity

if __package__:
    from .export_ccgt_pilot import export, load_source_inventory
else:
    from export_ccgt_pilot import export, load_source_inventory

CODES = {
    "caller": "666fcf50d7973283a39ffa3a2cfcff81",
    "market": "288d75128a6a01a0e536d551222be989",
    "constructor": "9200781106b014524c285d277f0f2009",
    "inverter_market": "4619d99c22211e61c8988a7b3dcb345d",
    "panel_market": "15d44382b3255510840c9c65d7938d53",
    "mounting_market": "39fe46885103686c9d5ad807c9f4fb91",
    "electrical_market": "fa3010b4e66ca3c8551020cef2b23d18",
    "mounting_producer": "fd161edc1cc8431efe8372632f2e4848",
}
PRODUCERS = {
    "inverter_market": [
        "6242b747386e58fa255b4a31c393593d",
        "46bacc3721e739aa29b81e10d6cbd4a1",
    ],
    "panel_market": [
        "7b0e3daf856e66d0e087fdb2bfb2a177",
        "026641b5e4abd0e086197f96be64c2cb",
    ],
    "mounting_market": [CODES["mounting_producer"]],
    "electrical_market": ["a0bd9dedbe92e8610de876ab86a111f9"],
}
OCCUPATION = "fe9c3a98-a6d2-452d-a9a4-a13e64f1b95b"
RIGHTS = "Restricted local ecoinvent inventories and IAM-derived profiles. Do not redistribute."


def rewrite_pv(inventory):
    by_code = {d["code"]: d for d in inventory}
    caller, market, constructor = [
        by_code[CODES[k]] for k in ("caller", "market", "constructor")
    ]
    if (caller["location"], caller["unit"], market["unit"]) != (
        "US-WECC",
        "kilowatt hour",
        "unit",
    ):
        raise ValueError("PV pilot identity or unit changed")
    boundary_path = Path(__file__).with_name("pv_boundary.json")
    reviewed = json.loads(boundary_path.read_text())["producers"]
    paths, roles = [], {}
    for market_name, codes in PRODUCERS.items():
        component_market = by_code[CODES[market_name]]
        for code in codes:
            producer = by_code[code]
            review = reviewed[code]
            if (producer["name"], producer["location"]) != (
                review["name"],
                review["location"],
            ):
                raise ValueError("Reviewed PV producer changed")
            negative = {
                e["input"][1]: e
                for e in producer["exchanges"]
                if e["type"] == "technosphere" and e["amount"] < 0
            }
            if negative.keys() != review["negative_ports"].keys():
                raise ValueError("Reviewed PV waste boundary changed")
            for supplier, port in review["negative_ports"].items():
                if negative[supplier]["name"] != port["name"]:
                    raise ValueError("Reviewed PV waste identity changed")
                if port["role"].startswith("retain_"):
                    continue
                paths.append(
                    [market, constructor, component_market, producer, by_code[supplier]]
                )
                roles[(identity(producer), identity(by_code[supplier]))] = port["role"]
    cuts = [
        [constructor, by_code[CODES[name]]]
        for name in ("inverter_market", "panel_market")
    ]
    for _, component in cuts:
        roles[(identity(constructor), identity(component))] = (
            "inverter_manufacture"
            if component["code"] == CODES["inverter_market"]
            else "panel_manufacture"
        )
    updated, audit = split_embedded_lifecycle(
        inventory,
        caller=caller,
        paths=paths,
        context_id="wecc-pv-pilot",
        uncertainty_mode="deterministic",
        component_cuts=cuts,
    )
    for row in audit["lifted_exchanges"]:
        row["pv_role"] = roles[
            (identity(row["original_caller"]), identity(row["original_supplier"]))
        ]
    mounting = by_code[CODES["mounting_producer"]]
    occupation = [
        e
        for e in mounting["exchanges"]
        if e["type"] == "biosphere" and e["input"][1] == OCCUPATION
    ]
    if len(occupation) != 1 or occupation[0]["unit"] != "square meter-year":
        raise ValueError("Reviewed PV occupation flow changed")
    node = next(
        r
        for r in audit["scoped_nodes"]
        if identity(r["original"]) == identity(mounting)
    )
    updated, audit = lift_scoped_biosphere(
        updated, audit, [{"source": node["scoped"], "flow": occupation[0]["input"]}]
    )
    audit["boundary_manifest_sha256"] = hashlib.sha256(
        boundary_path.read_bytes()
    ).hexdigest()
    audit["boundaries"] = {
        "parent": "Plant installation and new mounting/electrical components follow observed parent vintage",
        "inverter": "Active 15-year generation primary; existing amount already contains one nominal replacement over 30 years",
        "panels": "Source 100 installed + 1 handling loss + 2 lifetime maintenance replacements; no observed maintenance dates",
        "retained_waste": "Factory oil, water and municipal waste follow panel manufacture; inverter packaging follows inverter manufacture",
        "mixed_panel_polymer": "Source includes end-of-life and manufacturing loss; unknown partition receives explicit endpoint timing sensitivity",
        "mounting_packaging": "Positive inputs identify packaging while waste comments say end of life; installation primary with retirement sensitivity",
        "occupation": "Lifetime occupation coefficient attributed at requested service year, without dividing by lifetime again",
        "transformation": "Original land transformation pair remains at installation; absent restoration flow is not invented",
        "independent_capital": "Factory inputs retain their own background temporal roles",
        "uncertainty": "Deterministic coefficients; lifted products do not preserve Monte Carlo correlations",
    }
    return updated, audit


def mix_records(*weighted_records):
    """Mix declared quantity fractions without changing their common coefficient."""
    if any(
        not math.isfinite(w) or w < 0 for w, _ in weighted_records
    ) or not math.isclose(
        math.fsum(w for w, _ in weighted_records), 1, rel_tol=0, abs_tol=1e-12
    ):
        raise ValueError("Timing fractions must be nonnegative and sum to one")
    calendars = [
        dict((r["service_year"], r) for r in records) for _, records in weighted_records
    ]
    if any(c.keys() != calendars[0].keys() for c in calendars):
        raise ValueError("Mixed timing calendars must cover identical service years")
    result = []
    for year in sorted(calendars[0]):
        events = {}
        for (fraction, _), calendar in zip(weighted_records, calendars):
            record = calendar[year]
            for event, weight in zip(record["event_years"], record["weights"]):
                if fraction * weight:
                    events[event] = events.get(event, 0) + fraction * weight
        if not math.isclose(math.fsum(events.values()), 1, rel_tol=0, abs_tol=1e-10):
            raise ValueError("Mixed event calendar does not conserve weights")
        result.append(
            {
                "service_year": year,
                "event_years": sorted(events),
                "weights": [events[y] for y in sorted(events)],
            }
        )
    return result


def profiles(
    report,
    audit,
    case,
    *,
    polymer_eol_share=1.0,
    mounting_packaging_at_retirement=False,
):
    if not math.isfinite(polymer_eol_share) or not 0 <= polymer_eol_share <= 1:
        raise ValueError("Mixed polymer EOL share must be between zero and one")
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
    panel_manufacture = mix_records((101 / 103, parent), (2 / 103, service))
    # Over one nominal life: 100 panel equivalents at terminal retirement,
    # two failed modules during maintenance, and one handling-loss equivalent.
    # Replacements' terminal disposal replaces the failed initial panels' share.
    panel_end = mix_records(
        (100 / 103, retirement), (2 / 103, service), (1 / 103, parent)
    )
    role_records = {
        "inverter_manufacture": selected["inverter_manufacture"],
        "inverter_disposal": selected["inverter_retirement"],
        "panel_manufacture": panel_manufacture,
        "panel_mixed_polymer": mix_records(
            (polymer_eol_share, panel_end), (1 - polymer_eol_share, panel_manufacture)
        ),
        "mounting_disposal": retirement,
        "electrical_disposal": retirement,
        "mounting_ambiguous_packaging": (
            retirement if mounting_packaging_at_retirement else parent
        ),
    }
    provenance = {
        "evidence_tier": report["evidence_tier"],
        "public_sources": report["public_sources"],
        "iam_source": report["iam_source"],
        "assumptions": report["assumptions"],
        "boundaries": audit["boundaries"],
        "case": case,
        "polymer_eol_share": polymer_eol_share,
        "mounting_packaging_at_retirement": mounting_packaging_at_retirement,
        "small_panel_maintenance": "Expected lifetime maintenance quantity attributed in service year; not observed component cohorts",
        "manufacture_shares": {
            "installation_and_handling": 101 / 103,
            "maintenance": 2 / 103,
        },
        "panel_disposal_shares": {
            "terminal": 100 / 103,
            "maintenance": 2 / 103,
            "handling": 1 / 103,
        },
        "inventory_scope": "Constant ecoinvent 3.12 cut-off coefficients at inventory anchors; all original amounts retained",
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
        "pv-parent-construction",
        audit["caller"],
        audit["capital_root"],
        "existing_asset_service",
        parent,
        audit["preserved_capital_amount"],
        "parent_construction",
    )
    for i, row in enumerate(audit["lifted_exchanges"]):
        physical = row["pv_role"]
        role = (
            "existing_asset_service"
            if physical.endswith("manufacture")
            else "lifecycle_service"
        )
        bind(
            f"pv-lifted-{i}",
            row["caller"],
            row["supplier"],
            role,
            role_records[physical],
            row["amount_per_calling_dataset"],
            physical,
        )
    for i, row in enumerate(audit["lifted_biosphere"]):
        bind(
            f"pv-occupation-{i}",
            row["caller"],
            row["supplier"],
            "lifecycle_service",
            service,
            row["amount_per_calling_dataset"],
            "land_occupation",
        )
    first = min(min(y["event_years"]) for p in result for y in p["years"])
    last = max(max(y["event_years"]) for p in result for y in p["years"])
    zero = [
        {"service_year": y, "event_years": [y], "weights": [1.0]}
        for y in range(first, last + 1)
    ]
    for i, row in enumerate(audit["zero_shift_bindings"]):
        bind(
            f"pv-internal-zero-{i}",
            row["caller"],
            row["supplier"],
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
    parser.add_argument("--polymer-eol-share", type=float, default=1.0)
    parser.add_argument("--mounting-packaging-at-retirement", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--skip-legacy", action="store_true")
    args = parser.parse_args()
    inventory, source_hash = load_source_inventory(args.inventory)
    updated, audit = rewrite_pv(inventory)
    report = json.loads(args.cohorts.read_text())
    payload, expectations = profiles(
        report,
        audit,
        args.case,
        polymer_eol_share=args.polymer_eol_share,
        mounting_packaging_at_retirement=args.mounting_packaging_at_retirement,
    )
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
            f"pv_{variant}",
        )
    evidence = {
        "pilot": "wecc-pv",
        "rights": RIGHTS,
        "background": "constant technology at each anchor",
        "inventory_sha256": source_hash,
        "cohorts_sha256": hashlib.sha256(args.cohorts.read_bytes()).hexdigest(),
        "cohorts_path": str(args.cohorts.resolve()),
        "case": args.case,
        "polymer_eol_share": args.polymer_eol_share,
        "mounting_packaging_at_retirement": args.mounting_packaging_at_retirement,
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
                "lifted_ports": len(expectations) - 1,
            }
        )
    )


if __name__ == "__main__":
    main()
