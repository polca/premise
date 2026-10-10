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


@pytest.mark.parametrize(
    "liner,mass,energy,materials,services",
    [
        (
            "HDPE",
            87.71,
            12.87,
            {
                "market for aluminium alloy, AlLi": 7.09,
                "carbon fiber production, weaved, at factory": 19.78,
                "epoxy resin production, liquid": 29.67,
                "market for glass fibre": 5.6,
                "polyethylene production, high density, granulate": 10.26,
                "market for polyurethane, flexible foam": 4.67,
                "market for steel, chromium steel 18/8": 10.64,
            },
            {"metal working, average for chromium steel product manufacturing": 10.64},
        ),
        (
            "aluminium",
            113.169,
            16.422,
            {
                "market for aluminium alloy, AlLi": 35.7 + 7.021,
                "carbon fiber production, weaved, at factory": 19.873,
                "epoxy resin production, liquid": 29.75,
                "market for glass fibre": 5.593,
                "market for polyurethane, flexible foam": 4.641,
                "market for steel, chromium steel 18/8": 10.591,
            },
            {
                "metal working, average for chromium steel product manufacturing": 10.591,
                "market for sheet rolling, aluminium": 35.7,
            },
        ),
    ],
)
def test_imported_tanks_close_dry_mass_and_preserve_source_energy(
    inventories, liner, mass, energy, materials, services
):
    name = f"Fuel tank, compressed hydrogen gas, 700bar, with {liner} liner"
    (dataset,) = [d for d in inventories if d["name"] == name]
    assert dataset["unit"] == "kilogram"
    assert dataset["location"] == "RER"
    assert dataset["reference product"] == "Hydrogen tank"
    assert "excluding stored hydrogen" in dataset["description"]
    assert "10.1016/j.jclepro.2016.11.159" in dataset["comment"]
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
        assert "Evangelisti" in exchanges[key]["comment"]
    assert exchanges[electricity]["unit"] == "kilowatt hour"
    assert exchanges[electricity]["amount"] * mass * 3.6 == pytest.approx(energy)
    assert "MJ/kWh" in exchanges[electricity]["comment"]


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
    importer = DefaultInventory([], "3.7", version, path, "cutoff", False)
    importer.prepare_inventory()
    for liner, mass, energy in [("HDPE", 87.71, 12.87), ("aluminium", 113.169, 16.422)]:
        name = f"fuel tank, compressed hydrogen gas, 700bar, with {liner} liner"
        (tank,) = [d for d in importer.import_db.data if d["name"] == name]
        exchanges = {
            e["name"]: e for e in tank["exchanges"] if e["type"] == "technosphere"
        }
        services = {
            "metal working, average for chromium steel product manufacturing",
            "market for sheet rolling, aluminium",
        }
        materials = [
            e
            for name, e in exchanges.items()
            if e["unit"] == "kilogram" and name not in services
        ]
        assert sum(e["amount"] for e in materials) == pytest.approx(1, abs=1e-12)
        electricity = exchanges["market group for electricity, low voltage"]
        assert electricity["unit"] == "kilowatt hour"
        assert electricity["amount"] * mass * 3.6 == pytest.approx(energy)
        assert all("Evangelisti" in e["comment"] for e in exchanges.values())


def test_tank_content_changes_invalidate_only_inventory_cache(tmp_path, monkeypatch):
    import premise.new_database as module

    workbook = tmp_path / "trucks.xlsx"
    workbook.write_bytes(module.FILEPATH_TRUCKS.read_bytes())
    monkeypatch.setattr(module, "FILEPATH_TRUCKS", workbook)
    model = object.__new__(module.NewDatabase)
    model.keep_imports_uncertainty = False
    model.keep_source_db_uncertainty = False
    old_inventory = model._database_cache_path("source", inventories=True)
    old_background = model._database_cache_path("source")
    workbook.write_bytes(workbook.read_bytes() + b"changed tank recipe")
    assert model._database_cache_path("source", inventories=True) != old_inventory
    assert model._database_cache_path("source") == old_background
