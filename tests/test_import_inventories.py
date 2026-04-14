# content of test_activity_maps.py
from pathlib import Path
from types import SimpleNamespace

import pytest

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import (
    BaseInventoryImport,
    DefaultInventory,
    get_classification_entry,
    get_classifications,
)

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


class DummyInventoryImport(BaseInventoryImport):
    def load_inventory(self):
        return SimpleNamespace(data=[])


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


def test_get_classifications_repairs_mojibake(tmp_path, monkeypatch):
    filepath = tmp_path / "classifications.csv"
    name = (
        "market for sawlog and veneer log, paran√° pine, "
        "measured as solid wood under bark"
    )
    product = "sawlog and veneer log, paran√° pine, measured as solid wood under bark"
    filepath.write_text(
        "\n".join(
            [
                "name,product,ISIC rev.4 ecoinvent,CPC",
                f'"{name}","{product}",0220:Logging,"3110: Wood"',
            ]
        ),
        encoding="utf-8",
    )

    get_classifications.cache_clear()
    monkeypatch.setattr("premise.inventory_imports.FILEPATH_CLASSIFICATIONS", filepath)

    classifications = get_classifications()
    classification = get_classification_entry(
        classifications,
        (
            "market for sawlog and veneer log, parana\u0301 pine, "
            "measured as solid wood under bark"
        ),
        "sawlog and veneer log, paraná pine, measured as solid wood under bark",
    )

    assert classification == {
        "ISIC rev.4 ecoinvent": "0220:Logging",
        "CPC": "3110: Wood",
    }

    get_classifications.cache_clear()


def test_fill_data_gaps_prefers_matching_supplier_location(tmp_path):
    testpath = tmp_path / "dummy.xlsx"
    testpath.write_text("")

    reference_db = [
        {
            "name": "pig iron production",
            "reference product": "pig iron",
            "location": "RER",
            "exchanges": [
                {
                    "name": "market for coke",
                    "product": "coke",
                    "location": "GLO",
                    "amount": 9.72,
                    "type": "technosphere",
                    "unit": "megajoule",
                },
                {
                    "name": "market for coke",
                    "product": "coke",
                    "location": "RoW",
                    "amount": 9.68,
                    "type": "technosphere",
                    "unit": "megajoule",
                },
            ],
        }
    ]

    importer = DummyInventoryImport(
        reference_db,
        version_in="3.8",
        version_out="3.8",
        path=testpath,
        system_model="cutoff",
    )

    exchange = {
        "name": "market for coke",
        "product": "coke",
        "location": "GLO",
        "unit": "megajoule",
        "type": "technosphere",
        "replacement name": "pig iron production",
        "replacement product": "pig iron",
        "replacement location": "RER",
    }

    importer.fill_data_gaps(exchange)

    assert exchange["amount"] == pytest.approx(9.72)
    assert "replacement name" not in exchange
    assert "replacement product" not in exchange
    assert "replacement location" not in exchange


def test_fill_dataset_data_gaps_replaces_migrated_split_markets(tmp_path):
    testpath = tmp_path / "dummy.xlsx"
    testpath.write_text("")

    reference_db = [
        {
            "name": "pig iron production",
            "reference product": "pig iron",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for coke",
                    "product": "coke",
                    "location": "RoW",
                    "amount": 9.724,
                    "type": "technosphere",
                    "unit": "megajoule",
                },
                {
                    "name": "market group for hard coal",
                    "product": "hard coal",
                    "location": "RER",
                    "amount": 0.15,
                    "type": "technosphere",
                    "unit": "kilogram",
                },
            ],
        }
    ]

    importer = DummyInventoryImport(
        reference_db,
        version_in="3.8",
        version_out="3.8",
        path=testpath,
        system_model="cutoff",
    )

    dataset = {
        "name": "pig iron production, blast furnace, with carbon capture and storage",
        "reference product": "pig iron",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "pig iron production, blast furnace, with carbon capture and storage",
                "product": "pig iron",
                "amount": 1,
                "type": "production",
                "unit": "kilogram",
            },
            {
                "name": "market for coke",
                "product": "coke",
                "location": "CN",
                "unit": "megajoule",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
            {
                "name": "market for coke",
                "product": "coke",
                "location": "RoW",
                "unit": "megajoule",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
            {
                "name": "market for hard coal",
                "product": "hard coal",
                "location": "DE",
                "unit": "kilogram",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
            {
                "name": "market for hard coal",
                "product": "hard coal",
                "location": "FR",
                "unit": "kilogram",
                "amount": 0,
                "type": "technosphere",
                "replacement name": "pig iron production",
                "replacement product": "pig iron",
                "replacement location": "RER",
            },
        ],
    }

    importer.fill_dataset_data_gaps(dataset)

    techno = [exc for exc in dataset["exchanges"] if exc["type"] == "technosphere"]

    assert techno == [
        {
            "name": "market for coke",
            "product": "coke",
            "location": "RoW",
            "unit": "megajoule",
            "amount": pytest.approx(9.724),
            "type": "technosphere",
        },
        {
            "name": "market group for hard coal",
            "product": "hard coal",
            "location": "RER",
            "unit": "kilogram",
            "amount": pytest.approx(0.15),
            "type": "technosphere",
        },
    ]
