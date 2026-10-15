"""Published dry tank material and manufacturing-energy boundaries, without ecoinvent."""

from pathlib import Path

import pytest
from bw2io import ExcelImporter
from bw2io.strategies import csv_numerize, normalize_units


@pytest.fixture(scope="module")
def inventories():
    path = (
        Path(__file__).resolve().parents[1]
        / "premise/data/additional_inventories/lci-trucks.xlsx"
    )
    importer = ExcelImporter(path)
    # These tank recipes need numeric/unit preparation, not biosphere
    # migrations or an installed background database (also works with bw2io 0.8).
    importer.apply_strategies([csv_numerize, normalize_units])
    return importer.data


def test_imported_tank_closes_dry_mass_and_preserves_source_energy(inventories):
    mass = 87.71
    energy = 12.87
    materials = {
        "market for aluminium alloy, AlLi": 7.09,
        (
            "carbon fiber production, weaved, at factory, "
            "carculator regional truck 2020"
        ): 19.78,
        "epoxy resin production, liquid": 29.67,
        "market for glass fibre": 5.6,
        "polyethylene production, high density, granulate": 10.26,
        "market for polyurethane, flexible foam": 4.67,
        "market for steel, chromium steel 18/8": 10.64,
    }
    services = {
        "metal working, average for chromium steel product manufacturing": 10.64
    }
    name = "fuel tank, compressed hydrogen gas, 700bar, with HDPE liner"
    (dataset,) = [d for d in inventories if d["name"] == name]
    assert dataset["unit"] == "kilogram"
    assert dataset["location"] == "RER"
    assert dataset["reference product"] == "hydrogen tank"
    assert "excluding stored hydrogen" in dataset["comment"]
    assert "10.1016/j.jclepro.2016.11.159" in dataset["source"]
    assert "3.6" in dataset["comment"]
    (output,) = [e for e in dataset["exchanges"] if e["type"] == "production"]
    assert output["amount"] == 1
    assert output["name"] == name
    exchanges = {
        e["name"]: e for e in dataset["exchanges"] if e["type"] == "technosphere"
    }
    electricity = "market group for electricity, low voltage"
    assert set(exchanges) == materials.keys() | services.keys() | {electricity}
    assert sum(exchanges[key]["amount"] for key in materials) == pytest.approx(1)
    assert sum(materials.values()) == pytest.approx(mass)
    for key, value in {**materials, **services}.items():
        assert exchanges[key]["amount"] == pytest.approx(value / mass, rel=1e-12)
        assert exchanges[key]["unit"] == "kilogram"
        assert exchanges[key]["reference product"]
        assert exchanges[key]["location"]
    assert exchanges[electricity]["unit"] == "kilowatt hour"
    assert exchanges[electricity]["amount"] * mass * 3.6 == pytest.approx(energy)
    assert "12.87 MJ / 3.6 / 87.71 kg" in dataset["comment"]


@pytest.mark.parametrize("version", ["3.9", "3.10", "3.11", "3.12"])
def test_tank_correction_survives_premise_migration(version, monkeypatch):
    from premise.inventory_imports import DefaultInventory

    path = (
        Path(__file__).resolve().parents[1]
        / "premise/data/additional_inventories/lci-trucks.xlsx"
    )
    monkeypatch.setattr(
        DefaultInventory, "display_unlinked_exchanges", lambda self: None
    )
    importer = DefaultInventory([], "3.12", version, path, "cutoff", False)
    importer.prepare_inventory()
    name = "fuel tank, compressed hydrogen gas, 700bar, with HDPE liner"
    (tank,) = [d for d in importer.import_db.data if d["name"] == name]
    exchanges = {e["name"]: e for e in tank["exchanges"] if e["type"] == "technosphere"}
    services = {"metal working, average for chromium steel product manufacturing"}
    materials = [
        e
        for exchange_name, e in exchanges.items()
        if e["unit"] == "kilogram" and exchange_name not in services
    ]
    assert sum(e["amount"] for e in materials) == pytest.approx(1, abs=1e-12)
    electricity = exchanges["market group for electricity, low voltage"]
    assert electricity["unit"] == "kilowatt hour"
    assert electricity["amount"] * 87.71 * 3.6 == pytest.approx(12.87)
