"""Export actual enriched IAM profiles with an explicit synthetic inventory.

This is a producer/consumer integration fixture, not a regional LCA result.
The four service/capital coefficients match the documented pilot units; simple
independent biosphere factors make calendar and conservation errors observable.
"""

import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path

from premise.data_collection import IAMDataCollection
from premise.export import Export, biosphere_flows_dictionary
from premise.inventory_store import CompactInventoryStore
from premise.new_database import NewDatabase
from premise.stock_vintage import StockVintageExport
from premise.trails import TrailsDataPackage
from premise.utils import load_database

ASSETS = {
    "passenger-bev": ("EUR", "kilogram", "kilometer", 918.22 / 150000, 2.0),
    "heavy-trucks": ("EUR", "unit", "ton kilometer", 9.65e-8, 1000.0),
    "pv": ("USA", "unit", "kilowatt hour", 4.42343137603572e-8, 1000.0),
    "ccgt": ("USA", "unit", "kilowatt hour", 1.38888888888889e-11, 1e6),
}


def identity(asset, kind, unit, region):
    return {
        "name": f"enrichment fixture {asset} {kind}",
        "reference product": kind,
        "unit": unit,
        "location": region,
    }


def exchange(record, kind, amount):
    data = dict(record, type=kind, amount=amount)
    data["product"] = data.pop("reference product")
    return data


def export(scenario_dir, report, output, *, varying):
    report_data = json.loads(report.read_text())
    source = IAMDataCollection.__new__(IAMDataCollection)
    source.model = "remind"
    source.pathway = report_data["assumptions"]["scenario"]
    source.year = 2030
    source.data = source._IAMDataCollection__get_iam_data(
        key=None, filedir=scenario_dir, variables=[]
    )
    first = report_data["assumptions"]["reference_year"]
    last = report_data["assumptions"]["end_year"]
    anchors = sorted({first, 2030, last})
    payload = {
        "schema_version": 1,
        "inventory_context": {
            "source_database": "synthetic-enrichment-fixture",
            "source_version": "3.12",
            "system_model": "cutoff",
            "model": source.model,
            "pathway": source.pathway,
        },
        "profiles": [],
        "bindings": [],
    }
    base_inventory, checks = [], []
    bio_key, bio_code = next(
        (key, code)
        for key, code in biosphere_flows_dictionary("3.12").items()
        if key[0] == "Carbon dioxide, fossil" and key[1:3] == ("air", "unspecified")
    )
    for asset, (
        region,
        asset_unit,
        service_unit,
        coefficient,
        factor,
    ) in ASSETS.items():
        caller = identity(asset, "service", service_unit, region)
        supplier = identity(asset, "asset", asset_unit, region)
        payload["profiles"].append(
            source.stock_vintage_profile(
                asset_id=asset,
                region=region,
                years=range(first, last + 1),
                report_path=report,
                asset_unit=asset_unit,
                service_unit=service_unit,
            )
        )
        payload["bindings"].append(
            dict(profile_id=asset, caller=caller, supplier=supplier)
        )
        base_inventory.extend(
            [
                dict(
                    caller,
                    exchanges=[
                        exchange(caller, "production", 1),
                        exchange(supplier, "technosphere", coefficient),
                    ],
                ),
                dict(
                    supplier,
                    exchanges=[
                        exchange(supplier, "production", 1),
                        dict(
                            name=bio_key[0],
                            categories=bio_key[1:3],
                            unit=bio_key[3],
                            input=("biosphere3", bio_code),
                            type="biosphere",
                            amount=factor,
                        ),
                    ],
                ),
            ]
        )
        checks.append(
            dict(
                asset=asset,
                caller=caller,
                supplier=supplier,
                coefficient=coefficient,
                biosphere_factor=factor,
            )
        )
    output.mkdir(parents=True, exist_ok=False)
    database = NewDatabase.__new__(NewDatabase)
    database._inventory_api_active = True
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.datapackage = database
    obj.stock_vintage_export = StockVintageExport(payload)
    obj.stock_asset_params = {}
    obj.scenario_names = [source.model + " - " + source.pathway]
    previous = Path.cwd()
    try:
        os.chdir(output)
        for year in anchors:
            inventory = deepcopy(base_inventory)
            multiplier = (
                (1.0 if year == first else (0.8 if year == 2030 else 0.5))
                if varying
                else 1.0
            )
            for activity in inventory:
                for exc in activity["exchanges"]:
                    if exc["type"] == "biosphere":
                        exc["amount"] *= multiplier
            scenario = dict(
                model=source.model,
                pathway=source.pathway,
                year=year,
                _inventory_store=CompactInventoryStore(inventory),
            )
            database.scenarios = [scenario]
            obj.add_temporal_distributions()
            Export(
                scenario=load_database(scenario, []),
                filepath=output
                / "trails_temp/inventories"
                / source.model
                / source.pathway
                / str(year),
                version="3.12",
            ).export_db_to_matrices()
        name = "iam_stock_varying" if varying else "iam_stock_constant"
        obj._build_datapackage(name)
        package = output / f"{name}.zip"
        evidence = {
            "package": str(package),
            "sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
            "kind": "actual_REMIND_profiles_with_synthetic_inventory",
            "varying_background": varying,
            "anchors": anchors,
            "multipliers": [1.0, 0.8, 0.5] if varying else [1.0] * 3,
            "assets": checks,
            "profiles": payload["profiles"],
        }
        (output / "fixture-evidence.json").write_text(
            json.dumps(evidence, indent=2) + "\n"
        )
        return evidence
    finally:
        os.chdir(previous)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    for varying in (False, True):
        result = export(
            args.scenario_dir.resolve(),
            args.report.resolve(),
            args.output_dir.resolve() / ("varying" if varying else "constant"),
            varying=varying,
        )
        print(
            json.dumps(
                {k: result[k] for k in ("package", "sha256", "varying_background")}
            )
        )
