"""Contract, inventory-store and real matrix/package export checks."""

from copy import deepcopy
import csv
import hashlib
import json
from pathlib import Path

import pytest

from premise.export import Export, biosphere_flows_dictionary
from premise.inventory_store import CompactInventoryStore, LegacyInventoryStore
from premise.new_database import NewDatabase
from premise.stock_vintage import StockVintageExport
from premise.trails import TrailsDataPackage
from premise.utils import load_database

CALLER = dict(
    name="synthetic fleet service",
    **{"reference product": "service", "unit": "kilometer", "location": "GB"},
)
SUPPLIER = dict(
    name="synthetic new asset",
    **{"reference product": "asset", "unit": "unit", "location": "GLO"},
)


def payload():
    return {
        "schema_version": 1,
        "inventory_context": {
            "source_database": "synthetic",
            "source_version": "3.12",
            "system_model": "cutoff",
            "model": "test",
            "pathway": "stock",
        },
        "profiles": [
            {
                "id": "synthetic-fleet",
                "event_role": "existing_asset_service",
                "allocation_basis": "common_amortisation",
                "asset_unit": "unit",
                "service_unit": "kilometer",
                "provenance": {"evidence_tier": "synthetic"},
                "years": [
                    {
                        "service_year": 2020,
                        "event_years": [2010, 2015],
                        "weights": [0.75, 0.25],
                    },
                    {
                        "service_year": 2021,
                        "event_years": [2010, 2021],
                        "weights": [0.4, 0.6],
                    },
                    {
                        "service_year": 2022,
                        "event_years": [2010, 2021],
                        "weights": [0.2, 0.8],
                    },
                ],
            }
        ],
        "bindings": [
            {
                "profile_id": "synthetic-fleet",
                "caller": deepcopy(CALLER),
                "supplier": deepcopy(SUPPLIER),
            }
        ],
    }


def inventory():
    bio_key, bio_code = next(
        (key, code)
        for key, code in biosphere_flows_dictionary("3.12").items()
        if key[0] == "Carbon dioxide, fossil" and key[1:3] == ("air", "unspecified")
    )

    def exchange(metadata, kind, amount):
        result = dict(metadata, type=kind, amount=amount)
        result["product"] = result.pop("reference product")
        return result

    return [
        dict(
            CALLER,
            exchanges=[
                exchange(CALLER, "production", 1.0),
                exchange(SUPPLIER, "technosphere", 0.25),
            ],
        ),
        dict(
            SUPPLIER,
            exchanges=[
                exchange(SUPPLIER, "production", 1.0),
                {
                    "name": bio_key[0],
                    "categories": bio_key[1:3],
                    "unit": bio_key[3],
                    "input": ("biosphere3", bio_code),
                    "type": "biosphere",
                    "amount": 2.0,
                },
            ],
        ),
    ]


def make_exporter(directory, store_class=CompactInventoryStore, persisted=False):
    database = NewDatabase.__new__(NewDatabase)
    database._inventory_api_active = True
    database.scenarios = []
    for year in (2020, 2022):
        store = store_class(inventory())
        scenario = {"model": "test", "pathway": "stock", "year": year}
        if persisted:
            scenario["_inventory_checkpoint"] = store.checkpoint(
                directory / f"{year}.inventory-store"
            )
        else:
            scenario["_inventory_store"] = store
        database.scenarios.append(scenario)
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.datapackage = database
    obj.stock_vintage_export = StockVintageExport(payload())
    # A supplier-only legacy rule must lose to the exact stock binding.
    obj.stock_asset_params = {
        (SUPPLIER["name"], SUPPLIER["reference product"]): {
            "temporal_distribution": 6,
            "temporal_offsets": [-99],
            "temporal_weights": [1.0],
        }
    }
    obj.scenario_names = ["test - stock"]
    return obj


def export_fixture(directory, store_class=CompactInventoryStore, persisted=False):
    obj = make_exporter(directory, store_class, persisted)
    obj.add_temporal_distributions()
    for scenario in obj.datapackage.scenarios:
        loaded = load_database(scenario, [])
        target = (
            directory
            / "trails_temp"
            / "inventories"
            / "test"
            / "stock"
            / str(scenario["year"])
        )
        Export(scenario=loaded, filepath=target, version="3.12").export_db_to_matrices()
    obj._build_datapackage("synthetic_stock")
    return obj


@pytest.mark.parametrize("store_class", [CompactInventoryStore, LegacyInventoryStore])
@pytest.mark.parametrize("persisted", [False, True])
def test_profiles_survive_store_matrix_and_package_export(
    store_class, persisted, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    export_fixture(tmp_path, store_class, persisted)
    folder = tmp_path / "trails_temp"
    descriptor = json.loads((folder / "datapackage.json").read_text())
    spec = descriptor["stock_vintage"]
    assert "licenses" not in descriptor
    resource = next(r for r in descriptor["resources"] if r["name"] == spec["resource"])
    raw = (folder / resource["path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == spec["sha256"]
    assert json.loads(raw) == payload()
    for year, offsets in ((2020, [-10, -5]), (2022, [-12, -1])):
        path = folder / "inventories" / "test" / "stock" / str(year) / "A_matrix.csv"
        with path.open() as stream:
            rows = list(csv.DictReader(stream, delimiter=";"))
        bound = [row for row in rows if row["temporal_distribution"] == "6"]
        assert len(bound) == 1
        assert float(bound[0]["value"]) == 0.25
        assert bound[0]["temporal_amount_source"] == "port"
        assert json.loads(bound[0]["temporal_offsets"]) == offsets


def test_unmatched_exchange_fails_inside_store_transaction(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    obj = make_exporter(tmp_path)
    changed = payload()
    changed["bindings"][0]["supplier"]["location"] = "MISSING"
    obj.stock_vintage_export = StockVintageExport(changed)
    with pytest.raises(ValueError, match="Unmatched"):
        obj.add_temporal_distributions()
    actual = load_database(obj.datapackage.scenarios[0], [])
    assert "temporal_distribution" not in actual["database"][0]["exchanges"][1]


@pytest.mark.parametrize(
    "case", ["weight", "missing_year", "future", "units", "duplicate", "context"]
)
def test_invalid_profiles_fail_closed(case):
    data = payload()
    if case == "weight":
        data["profiles"][0]["years"][0]["weights"] = [0.1, 0.2]
    elif case == "missing_year":
        data["profiles"][0]["years"].pop(1)
    elif case == "future":
        data["profiles"][0]["years"][0]["event_years"][1] = 2040
    elif case == "units":
        data["profiles"][0]["asset_unit"] = "kilogram"
    elif case == "duplicate":
        data["bindings"].append(deepcopy(data["bindings"][0]))
    else:
        del data["inventory_context"]
    with pytest.raises(ValueError):
        StockVintageExport(data)


def test_scenario_context_is_checked():
    export = StockVintageExport(payload())
    with pytest.raises(ValueError, match="pathway"):
        export.validate_scenario({"model": "test", "pathway": "another", "year": 2020})


if __name__ == "__main__":
    # Optional local cross-environment fixture generation (premise uses Python
    # 3.12, TRAILS uses 3.11). Only synthetic data are written.
    import os
    import sys

    target = Path(sys.argv[1]).resolve()
    target.mkdir(parents=True, exist_ok=True)
    os.chdir(target)
    export_fixture(target)
