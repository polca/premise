"""Published dry tank material and manufacturing-energy boundaries, without ecoinvent."""

import json
import math
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
        # The source lists aluminium ingot without an alloy grade. The reviewed
        # ordinary-aluminium proxy replaces the unsupported AlLi supplier only.
        "market for aluminium, wrought alloy": 7.09,
        "carbon fiber production, weaved, at factory": 19.78,
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
    aluminium = exchanges["market for aluminium, wrought alloy"]
    assert aluminium["reference product"] == "aluminium, wrought alloy"
    assert aluminium["location"] == "GLO"
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


@pytest.mark.parametrize("version", ["3.9", "3.10", "3.11", "3.12"])
def test_rail_storage_preserves_legacy_recipe_and_lifetime_system(version, monkeypatch):
    from premise.filesystem_constants import INVENTORY_DIR
    from premise.inventory_imports import DefaultInventory

    monkeypatch.setattr(
        DefaultInventory, "display_unlinked_exchanges", lambda self: None
    )
    importer = DefaultInventory(
        [], "3.9", version, INVENTORY_DIR / "lci-rail-freight.xlsx", "cutoff", False
    )
    importer.prepare_inventory()
    name = "fuel tank, compressed hydrogen gas, 700bar, with aluminium liner"
    (tank,) = [d for d in importer.import_db.data if d["name"] == name]
    assert tank["unit"] == "kilogram"
    assert tank["reference product"] == "hydrogen tank"
    provenance = json.loads(tank["inventory provenance"])
    assert provenance["role"] == "retained legacy component"
    assert provenance["restored in"] == "lci-rail-freight.xlsx"
    exchanges = {e["name"]: e for e in tank["exchanges"] if e["type"] == "technosphere"}
    materials = {
        "market for steel, chromium steel 18/8": 10.591,
        "epoxy resin production, liquid": 29.75,
        "carbon fiber production, weaved, at factory": 19.873,
        "market for aluminium alloy, AlLi": 35.7 + 7.021,
        "market for glass fibre": 5.593,
        "market for polyurethane, flexible foam": 4.641,
    }
    assert sum(materials.values()) == pytest.approx(113.169)
    assert sum(exchanges[k]["amount"] for k in materials) == pytest.approx(1)
    for key, mass in materials.items():
        assert exchanges[key]["amount"] == pytest.approx(mass / 113.169, rel=1e-12)
    electricity = exchanges["market group for electricity, low voltage"]
    assert electricity["unit"] == "kilowatt hour"
    assert electricity["amount"] * 113.169 * 3.6 == pytest.approx(16.422)
    (locomotive,) = [
        d
        for d in importer.import_db.data
        if d["name"] == "production of locomotive, fuel cell, hydrogen"
    ]
    # Issue 69 supersedes the old 30,323.85 kg road-tank allocation. One rail
    # lifetime system contains all replacements; the retained legacy recipe
    # above remains independently checked without requiring a current buyer.
    system_name = "hydrogen fuel tank system production, rail, 700 bar"
    (system,) = [d for d in importer.import_db.data if d["name"] == system_name]
    (purchase,) = [e for e in locomotive["exchanges"] if e["name"] == system_name]
    assert purchase["unit"] == system["unit"] == "unit"
    assert purchase["product"] == system["reference product"]
    assert purchase["amount"] == 1
    assert not any(e["name"] == name for e in locomotive["exchanges"])
    cylinder = "hydrogen fuel tank production, rail, type 3 cylinder, 700 bar"
    rack = "hydrogen fuel tank mounting rack production, rail"
    controls = "hydrogen fuel tank valve and control system production, rail"
    # Preserve the old implied installed hydrogen budget, then round up to
    # complete 8.3 kg survey cylinders; replacements belong inside the system.
    hydrogen_budget = 30323.84571428572 / 4 * 5.6 / 93
    installed_cylinders = math.ceil(hydrogen_budget / 8.3)
    inputs = [e for e in system["exchanges"] if e["type"] == "technosphere"]
    assert {e["name"]: e["amount"] for e in inputs} == {
        cylinder: installed_cylinders * (40 / 10),
        rack: 1,
        controls: 40 / 20,
    }
    for link in inputs:
        (supplier,) = [d for d in importer.import_db.data if d["name"] == link["name"]]
        assert link["product"] == supplier["reference product"]
        assert link["unit"] == supplier["unit"] == "unit"
        assert link["location"] == supplier["location"]
    # FCH2RAIL D1.5 Table 20: 8.3 kg H2 and 179 kg including gas per cylinder.
    # The 55-cylinder choice and 95% material retention are engineering inputs.
    (vessel,) = [d for d in importer.import_db.data if d["name"] == cylinder]
    dry_mass = 179 - 8.3
    feeds = [e for e in vessel["exchanges"] if e["type"] == "technosphere"]
    assert sum(e["amount"] for e in feeds if e["unit"] == "kilogram") * 0.95 == (
        pytest.approx(dry_mass)
    )
    power = [e for e in feeds if e["unit"] == "kilowatt hour"]
    assert sum(e["amount"] for e in power) == pytest.approx(dry_mass)
    assert installed_cylinders * 8.3 >= hydrogen_budget
    assert installed_cylinders * 8.3 - hydrogen_budget < 8.3
