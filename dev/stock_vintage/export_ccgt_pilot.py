"""Export the real CCGT pilot with a constant licensed background inventory.

Use the source extract written by TRAILS' extract_local_inventory.py. Both
legacy and corrected packages are restricted local artifacts. Matrices use the
same ecoinvent technology at each anchor: this isolates timing/conservation and
is not an IAM-transformed electricity-background result.
"""

import argparse
from copy import deepcopy
import gc
import gzip
import hashlib
import json
import os
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

from premise.export import Export, biosphere_flows_dictionary
from premise.inventory_store import CompactInventoryStore
from premise.new_database import NewDatabase
from premise.stock_lifecycle import scope_capital_chain
from premise.stock_vintage import StockVintageExport
from premise.trails import FILEPATH_TEMPORAL_PARAMETERS, TrailsDataPackage
from premise.utils import load_database

CODES = [
    "c310abcc54726059b3bf8496e05ffb5d",
    "6fb21f72782c0b3fc6c6725dfb1dd7fb",
    "409bc961971c08001fda366891c86c18",
]
RIGHTS = "Restricted local ecoinvent inventory and IAM-derived profiles. Original source rights apply; do not redistribute."


def profiles(report, audit, case):
    primary = report["cases"][case]
    years = [
        {k: row[k] for k in ("service_year", "event_years", "weights")}
        for row in primary["annual"]
    ]
    first = min(min(row["event_years"]) for row in years)
    last = max(row["service_year"] for row in years)
    provenance = {
        "evidence_tier": "observed_initial_service_with_labelled_IAM_relative_reconstruction",
        "observation_geography": "US-WECC selected complete non-CHP CCGT blocks",
        "future_proxy_geography": "USA",
        "source_files": report["public_sources"],
        "iam_source": report["iam_source"],
        "sensitivity_case": case,
        "assumptions": report["assumptions"],
        "model": {k: primary[k] for k in ("mode", "weighting", "survival")},
        "inventory_scope": "constant ecoinvent 3.12 cut-off background at every anchor; no IAM technology transformation",
        "amortisation": "Preserved source coefficient: 400 MW design plant over 180000 full-load hours; not recalculated from survival",
        "lifecycle_boundary": "No direct disposal exchange in the reviewed CCGT construction inventory; none invented",
    }
    result, bindings = [], []

    def bind(name, caller, supplier, role, records):
        result.append(
            {
                "id": name,
                "event_role": role,
                "allocation_basis": "common_amortisation",
                "asset_unit": supplier["unit"],
                "service_unit": caller["unit"],
                "provenance": provenance,
                "years": records,
            }
        )
        bindings.append({"profile_id": name, "caller": caller, "supplier": supplier})

    bind(
        "ccgt-construction",
        audit["caller"],
        audit["capital_root"],
        "existing_asset_service",
        years,
    )
    zero = [
        {"service_year": y, "event_years": [y], "weights": [1.0]}
        for y in range(first, last + 1)
    ]
    for i, binding in enumerate(audit["zero_shift_bindings"]):
        bind(
            f"ccgt-internal-{i}",
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
    }


def set_local_rights(path):
    """Correct the legacy builder's blanket licence in a private pilot copy.

    All matrix/resource bytes are copied unchanged. This deliberately does not
    alter legacy public exporter defaults as part of the stock pilot.
    """
    temporary = path.with_suffix(".restricted.zip")
    with (
        ZipFile(path) as source,
        ZipFile(temporary, "w", compression=ZIP_DEFLATED) as target,
    ):
        for info in source.infolist():
            raw = source.read(info.filename)
            if info.filename.endswith("datapackage.json"):
                descriptor = json.loads(raw)
                descriptor.pop("licenses", None)
                descriptor["rights"] = RIGHTS
                descriptor["pilot_background"] = (
                    "constant ecoinvent 3.12 cut-off; cohort scenario does not transform background technology"
                )
                raw = (json.dumps(descriptor, indent=2) + "\n").encode()
            target.writestr(info, raw)
    os.replace(temporary, path)


def export(inventory, directory, report, payload, anchors, name):
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "trails_temp").exists() or (directory / f"{name}.zip").exists():
        raise ValueError(
            "Choose a fresh export directory to avoid stale package resources"
        )
    database = NewDatabase.__new__(NewDatabase)
    database._inventory_api_active = True
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.datapackage = database
    obj.stock_vintage_export = StockVintageExport(payload) if payload else None
    obj.scenario_names = ["remind - " + report["iam_source"]["scenario"]]
    (
        obj.stock_asset_params,
        obj.end_of_life_suppliers,
        obj.biomass_growth_params,
        obj.maintenance_suppliers,
        obj.long_term_biosphere_params,
        obj.dataset_lifetimes,
    ) = obj._load_temporal_specs_from_csv(FILEPATH_TEMPORAL_PARAMETERS)
    previous = Path.cwd()
    try:
        os.chdir(directory)
        for year in anchors:
            scenario = {
                "model": "remind",
                "pathway": report["iam_source"]["scenario"],
                "year": year,
                "_inventory_store": CompactInventoryStore(deepcopy(inventory)),
            }
            database.scenarios = [scenario]
            obj.add_temporal_distributions()
            loaded = load_database(scenario, [])
            Export(
                scenario=loaded,
                filepath=directory
                / "trails_temp/inventories/remind"
                / scenario["pathway"]
                / str(year),
                version="3.12",
            ).export_db_to_matrices()
            database.scenarios = []
            del scenario, loaded
            gc.collect()
        obj._build_datapackage(name)
        path = directory / f"{name}.zip"
        set_local_rights(path)
        return {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    finally:
        os.chdir(previous)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--cohorts", type=Path, required=True)
    parser.add_argument("--case", default="primary")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--skip-legacy", action="store_true")
    args = parser.parse_args()
    args.output_dir = args.output_dir.resolve()
    raw_hash = hashlib.sha256(args.inventory.read_bytes()).hexdigest()
    manifest = json.loads(
        args.inventory.with_suffix(args.inventory.suffix + ".manifest.json").read_text()
    )
    if raw_hash != manifest["sha256"]:
        raise ValueError("Inventory extract differs from its local manifest")
    with gzip.open(args.inventory, "rt") as stream:
        source = json.load(stream)
    if source["source_version"] != "3.12" or source["system_model"] != "cutoff":
        raise ValueError("This pilot requires ecoinvent 3.12 cut-off")
    inventory = source["inventory"]
    known_bio = set(biosphere_flows_dictionary("3.12").values())
    for dataset in inventory:
        for exc in dataset["exchanges"]:
            for key in ("input", "output", "categories"):
                if key in exc:
                    exc[key] = tuple(exc[key])
            if exc["type"] == "biosphere" and exc["input"][1] not in known_bio:
                raise ValueError(
                    f"Exporter cannot represent source biosphere flow {exc['input'][1]}"
                )
    lookup = {d["code"]: d for d in inventory}
    selected = [lookup[code] for code in CODES]
    updated, audit = scope_capital_chain(
        inventory, caller=selected[0], chain=selected[1:], context_id="ccgt-wecc-pilot"
    )
    report = json.loads(args.cohorts.read_text())
    payload = profiles(report, audit, args.case)
    anchors = sorted({report["reference_year"], 2025, report["last_service_year"]})
    packages = {}
    if not args.skip_legacy:
        packages["legacy"] = export(
            inventory, args.output_dir / "legacy", report, None, anchors, "ccgt_legacy"
        )
    packages["corrected"] = export(
        updated,
        args.output_dir / "corrected",
        report,
        payload,
        anchors,
        "ccgt_corrected",
    )
    evidence = {
        "rights": RIGHTS,
        "background": "constant technology at each anchor",
        "inventory_sha256": raw_hash,
        "cohorts_sha256": hashlib.sha256(args.cohorts.read_bytes()).hexdigest(),
        "cohorts_path": str(args.cohorts.resolve()),
        "case": args.case,
        "source_activities": len(inventory),
        "exported_activities": len(updated),
        "anchors": anchors,
        "rewrite": audit,
        "packages": packages,
        "annual": report["cases"][args.case]["annual"],
    }
    (args.output_dir / "export-evidence.json").write_text(
        json.dumps(evidence, indent=2, allow_nan=False) + "\n"
    )
    print(json.dumps({"packages": packages, "anchors": anchors}))


if __name__ == "__main__":
    main()
