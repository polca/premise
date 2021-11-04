# content of test_activity_maps.py
from pathlib import Path

import pytest

from premise import DATA_DIR, INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

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


def test_file_exists():
    db, version = get_db()
    with pytest.raises(FileNotFoundError) as wrapped_error:
        DefaultInventory(db, version_in=version, path="somepath", version_out="3.7")
    assert wrapped_error.type == FileNotFoundError


def test_biosphere_dict():
    db, version = get_db()
    testpath = Path("tests/data/somefile.xlsx")
    dbc = DefaultInventory(db, version_in=version, path=testpath, version_out="3.7")
    assert (
        dbc.biosphere_dict[
            ("1,4-Butanediol", "air", "urban air close to ground", "kilogram")
        ]
        == "38a622c6-f086-4763-a952-7c6b3b1c42ba"
    )


def test_biosphere_dict_2():
    db, version = get_db()
    testpath = Path("tests/data/somefile.xlsx")

    dbc = DefaultInventory(db, version_in=version, path=testpath, version_out="3.7")

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


def test_load_inventories():
    db, version = get_db()
    testpath = Path("tests/data/somefile.xlsx")
    carma = DefaultInventory(db, version_in="3.7", path=testpath, version_out="3.7")
    assert len(carma.import_db.data) == 22
