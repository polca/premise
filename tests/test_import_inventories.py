# content of test_activity_maps.py
from pathlib import Path

import pytest

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import BaseInventoryImport, DefaultInventory

FILEPATH_CARMA_INVENTORIES = INVENTORY_DIR / "lci-Carma-CCS.xlsx"
FILEPATH_BIOFUEL_INVENTORIES = INVENTORY_DIR / "lci-biofuels.xlsx"
FILEPATH_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-biogas.xlsx"
FILEPATH_HYDROGEN_INVENTORIES = INVENTORY_DIR / "lci-hydrogen.xlsx"
FILEPATH_SYNFUEL_INVENTORIES = INVENTORY_DIR / "lci-synfuel.xlsx"
FILEPATH_SYNGAS_INVENTORIES = INVENTORY_DIR / "lci-syngas.xlsx"
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-coal-gasification.xlsx"
)


def get_db():
    db = [
        {
            "code": "argsthyfujgyftdgr",
            "name": "fake activity",
            "reference product": "fake product",
            "location": "IAI Area, Africa",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "fake activity",
                    "product": "fake product",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "input": ("dummy_db", "6543541"),
                },
                {
                    "name": "1,4-Butanediol",
                    "categories": ("air", "urban air close to ground"),
                    "amount": 1,
                    "type": "biosphere",
                    "unit": "kilogram",
                    "input": ("dummy_bio", "123"),
                },
            ],
        }
    ]
    version = "3.5"
    return db, version


def get_replacement_source_db():
    return [
        {
            "code": "source",
            "name": "source process",
            "reference product": "source product",
            "location": "RoW",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "source process",
                    "product": "source product",
                    "amount": 1,
                    "type": "production",
                    "unit": "kilogram",
                    "location": "RoW",
                },
                {
                    "name": "market for heat",
                    "product": "heat",
                    "amount": 3,
                    "type": "technosphere",
                    "unit": "megajoule",
                    "location": "RoW",
                },
                {
                    "name": "market for heat",
                    "product": "heat",
                    "amount": 0.5,
                    "type": "technosphere",
                    "unit": "megajoule",
                    "location": "Europe without Switzerland",
                },
                {
                    "name": "market group for electricity",
                    "product": "electricity, high voltage",
                    "amount": 2.5,
                    "type": "technosphere",
                    "unit": "kilowatt hour",
                    "location": "RER",
                },
                {
                    "name": "market group for electricity",
                    "product": "electricity, high voltage",
                    "amount": 9,
                    "type": "technosphere",
                    "unit": "kilowatt hour",
                    "location": "RoW",
                },
            ],
        }
    ]


def get_base_inventory_import(tmp_path, database):
    testpath = tmp_path / "testfile"
    testpath.write_text("")
    return BaseInventoryImport(
        database,
        version_in="3.8",
        version_out="3.8",
        path=testpath,
        system_model="cutoff",
    )


def test_file_exists():
    db, version = get_db()
    with pytest.raises(FileNotFoundError) as wrapped_error:
        BaseInventoryImport(
            db,
            version_in=version,
            version_out="3.8",
            path="testfile",
            system_model="cutoff",
        )
    assert wrapped_error.type == FileNotFoundError


def test_biosphere_dict():
    db, version = get_db()
    testpath = Path("testfile")
    open(testpath, "w")
    dbc = BaseInventoryImport(
        db, version_in=version, version_out="3.8", path=testpath, system_model="cutoff"
    )
    assert (
        dbc.biosphere_dict[
            ("1,4-Butanediol", "air", "urban air close to ground", "kilogram")
        ]
        == "38a622c6-f086-4763-a952-7c6b3b1c42ba"
    )

    testpath.unlink()


def test_biosphere_dict_2():
    db, version = get_db()
    testpath = Path("testfile")
    open(testpath, "w")
    dbc = BaseInventoryImport(
        db, version_in=version, version_out="3.8", path=testpath, system_model="cutoff"
    )

    for act in dbc.database:
        for exc in act["exchanges"]:
            if exc["type"] == "biosphere":
                assert (
                    dbc.biosphere_dict[
                        (
                            exc["name"],
                            exc["categories"][0],
                            exc["categories"][1],
                            exc["unit"],
                        )
                    ]
                    == "38a622c6-f086-4763-a952-7c6b3b1c42ba"
                )

    testpath.unlink()


def test_fill_data_gaps_matches_technosphere_exchange_location(tmp_path):
    dbc = get_base_inventory_import(tmp_path, get_replacement_source_db())
    exchange = {
        "name": "market for heat",
        "product": "heat",
        "location": "RoW",
        "amount": 0,
        "type": "technosphere",
        "unit": "megajoule",
        "replacement name": "source process",
        "replacement product": "source product",
        "replacement location": "RoW",
    }

    dbc.fill_data_gaps(exchange)

    assert exchange["amount"] == 3
    assert "replacement name" not in exchange
    assert "replacement product" not in exchange
    assert "replacement location" not in exchange


def test_fill_data_gaps_sums_technosphere_exchanges_without_location(tmp_path):
    dbc = get_base_inventory_import(tmp_path, get_replacement_source_db())
    exchange = {
        "name": "market for heat",
        "product": "heat",
        "amount": 0,
        "type": "technosphere",
        "unit": "megajoule",
        "replacement name": "source process",
        "replacement product": "source product",
        "replacement location": "RoW",
    }

    dbc.fill_data_gaps(exchange)

    assert exchange["amount"] == 3.5


def test_fill_data_gaps_market_group_fallback_matches_location(tmp_path):
    dbc = get_base_inventory_import(tmp_path, get_replacement_source_db())
    exchange = {
        "name": "market for electricity",
        "product": "electricity, high voltage",
        "location": "RER",
        "amount": 0,
        "type": "technosphere",
        "unit": "kilowatt hour",
        "replacement name": "source process",
        "replacement product": "source product",
        "replacement location": "RoW",
    }

    dbc.fill_data_gaps(exchange)

    assert exchange["amount"] == 2.5


def test_load_carma():
    db, version = get_db()
    carma = DefaultInventory(
        db,
        version_in="3.5",
        version_out="3.8",
        path=FILEPATH_CARMA_INVENTORIES,
        system_model="cutoff",
        keep_uncertainty_data=False,
    )
    assert len(carma.import_db.data) >= 81


def test_load_biofuel():
    db, version = get_db()
    bio = DefaultInventory(
        db,
        version_in="3.7",
        version_out="3.8",
        path=FILEPATH_BIOFUEL_INVENTORIES,
        system_model="cutoff",
        keep_uncertainty_data=False,
    )
    assert len(bio.import_db.data) >= 150
