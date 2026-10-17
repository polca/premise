"""Check NMC955 material and capacity boundaries without ecoinvent."""

from copy import deepcopy

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR

PASTE = "anode paste production, silicon-graphite, for Li-ion battery, NMC955"
PRODUCT = "anode paste, silicon-graphite, for Li-ion battery, NMC955"
CELL = "battery cell production, Li-ion, NMC955"
CAPACITY_MARKETS = (
    "market for battery capacity, Li-ion, NMC955",
    "market for battery capacity, Li-ion, NMC955, stationary",
)


@pytest.fixture(scope="module")
def inventory():
    return ExcelImporter(INVENTORY_DIR / "lci-batteries-NMC955-LTO.xlsx").data


@pytest.fixture(scope="module")
def capacity_inventory():
    return ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data


def activity(inventory, name):
    (dataset,) = [ds for ds in inventory if ds["name"] == name]
    return dataset


def exchange(dataset, name):
    (exc,) = [exc for exc in dataset["exchanges"] if exc["name"] == name]
    return exc


def test_nmc955_market_preserves_pack_and_cell_mass(inventory):
    (market,) = [
        ds for ds in inventory if ds["name"] == "market for battery, Li-ion, NMC955"
    ]
    (pack,) = [
        ds for ds in inventory if ds["name"] == "battery production, Li-ion, NMC955"
    ]
    (market_output,) = [
        exc for exc in market["exchanges"] if exc["type"] == "production"
    ]
    (pack_output,) = [exc for exc in pack["exchanges"] if exc["type"] == "production"]
    (pack_input,) = [
        exc
        for exc in market["exchanges"]
        if exc["type"] == "technosphere" and exc["name"] == pack["name"]
    ]
    (cell_input,) = [
        exc
        for exc in pack["exchanges"]
        if exc["name"] == "battery cell production, Li-ion, NMC955"
    ]
    assert {
        exc["unit"] for exc in (market_output, pack_output, pack_input, cell_input)
    } == {"kilogram"}
    # One kg sold requires one kg of complete packs. The cell fraction belongs
    # inside pack manufacture and must not be applied again by the market.
    packs_per_kg_sold = pack_input["amount"] / market_output["amount"]
    assert packs_per_kg_sold == pytest.approx(1)
    cells_per_kg_pack = cell_input["amount"] / pack_output["amount"]
    assert cells_per_kg_pack == pytest.approx(0.72591)
    assert packs_per_kg_sold * cells_per_kg_pack == pytest.approx(0.72591)


def test_anode_dry_solids_close_and_implement_graphite_substitution(inventory):
    paste = activity(inventory, PASTE)
    assert paste["reference product"] == PRODUCT
    assert paste["location"] == "CN"
    assert exchange(paste, PASTE)["amount"] == 1
    solids = {
        "market for graphite, battery grade": 0.8554,
        "market for silicon, metallurgical grade": 0.0846,
        "market for carbon black": 0.01,
        "market for carboxymethyl cellulose, powder": 0.03,
        "market for latex": 0.02,  # Original source proxy for nominal dry SBR.
    }
    for name, amount in solids.items():
        exc = exchange(paste, name)
        assert exc["unit"] == "kilogram"
        assert exc["amount"] == pytest.approx(amount)
    mass = sum(exchange(paste, name)["amount"] for name in solids)
    assert mass == pytest.approx(1)
    silicon = exchange(paste, "market for silicon, metallurgical grade")["amount"]
    graphite = exchange(paste, "market for graphite, battery grade")["amount"]
    assert silicon / (silicon + graphite) == pytest.approx(0.09)
    # Solvent is a process input, not retained dry-anode mass.
    kg_inputs = {
        e["name"]
        for e in paste["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "kilogram"
    }
    assert kg_inputs == solids.keys() | {"market for water, deionised"}


def test_cell_anode_has_one_collector_and_correct_dry_ingredients(inventory):
    cell = activity(inventory, CELL)
    paste = activity(inventory, PASTE)
    paste_input = exchange(cell, PASTE)
    assert paste_input["reference product"] == PRODUCT
    assert paste_input["location"] == paste["location"]
    assert paste_input["amount"] == pytest.approx(0.2)
    foil = exchange(cell, "copper collector foil, for Li-ion battery")
    assert foil["amount"] == pytest.approx(0.12)
    assert paste_input["amount"] + foil["amount"] == pytest.approx(0.32)
    assert not any("copper" in e["name"] for e in paste["exchanges"])
    assert not any(
        e["name"] == "market for anode, silicon coated graphite, for Li-ion battery"
        for e in cell["exchanges"]
    )
    assert paste_input["amount"] * exchange(
        paste, "market for graphite, battery grade"
    )["amount"] == pytest.approx(0.17108)
    assert paste_input["amount"] * exchange(
        paste, "market for silicon, metallurgical grade"
    )["amount"] == pytest.approx(0.01692)


def test_dry_paste_keeps_source_processing_energy_and_solvent(inventory):
    paste = activity(inventory, PASTE)
    expected = {
        "market group for electricity, medium voltage": (
            0.46810728671732227,
            "kilowatt hour",
        ),
        "market for heat, district or industrial, natural gas": (
            8.48487394434316,
            "megajoule",
        ),
        "market for water, deionised": (0.96, "kilogram"),
        "market for wastewater, average": (-0.00096, "cubic meter"),
        "market for chemical factory, organics": (4e-10, "unit"),
    }
    for name, (amount, unit) in expected.items():
        exc = exchange(paste, name)
        assert exc["amount"] == pytest.approx(amount)
        assert exc["unit"] == unit
    assert "105619" in paste["source"]
    assert "proxy" in paste["comment"]


@pytest.mark.parametrize("version", ["3.8", "3.9", "3.10", "3.11", "3.12"])
def test_dry_anode_boundary_survives_premise_migration(version, monkeypatch):
    from premise.inventory_imports import DefaultInventory

    monkeypatch.setattr(
        DefaultInventory, "display_unlinked_exchanges", lambda self: None
    )
    importer = DefaultInventory(
        [],
        "3.8",
        version,
        INVENTORY_DIR / "lci-batteries-NMC955-LTO.xlsx",
        "cutoff",
        False,
    )
    importer.prepare_inventory()
    data = importer.import_db.data
    paste = activity(data, PASTE)
    cell = activity(data, CELL)
    assert paste["classifications"]
    assert exchange(cell, PASTE)["product"] == PRODUCT
    assert exchange(cell, PASTE)["location"] == paste["location"]
    assert exchange(cell, PASTE)["amount"] == pytest.approx(0.2)
    assert exchange(cell, "copper collector foil, for Li-ion battery")[
        "amount"
    ] == pytest.approx(0.12)
    solids = [
        e
        for e in paste["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "kilogram"
        and e["name"] != "market for water, deionised"
    ]
    assert sum(e["amount"] for e in solids) == pytest.approx(1)
    assert not any("copper" in e["name"] for e in paste["exchanges"])
    # Some versions split the wastewater market between regional suppliers.
    wastewater = [
        e
        for e in paste["exchanges"]
        if e["type"] == "technosphere"
        and e.get("reference product") == "wastewater, average"
    ]
    assert all(e["unit"] == "cubic meter" for e in wastewater)
    assert sum(e["amount"] for e in wastewater) == pytest.approx(-0.00096)


def assert_capacity_mass_balance(market, cell_fraction, densities):
    manufacture = exchange(market, "market for battery, Li-ion, NMC955")
    disposal = exchange(market, "market for used Li-ion battery")
    assert market["battery cell mass share"] == pytest.approx(cell_fraction)
    assert manufacture["unit"] == disposal["unit"] == "kilogram"
    assert manufacture["uncertainty type"] == disposal["uncertainty type"] == 5
    # Cell kg per pack kg * cell kWh per cell kg * pack kg = one kWh.
    assert manufacture["amount"] * cell_fraction * densities["mean"] == pytest.approx(1)
    assert manufacture["minimum"] * cell_fraction * densities[
        "maximum"
    ] == pytest.approx(1)
    assert manufacture["maximum"] * cell_fraction * densities[
        "minimum"
    ] == pytest.approx(1)
    assert manufacture["minimum"] <= manufacture["loc"] <= manufacture["maximum"]
    assert manufacture["loc"] == manufacture["amount"]
    assert disposal["amount"] == -manufacture["amount"]
    assert disposal["loc"] == -manufacture["loc"]
    assert disposal["minimum"] == -manufacture["maximum"]
    assert disposal["maximum"] == -manufacture["minimum"]
    assert disposal["negative"] is True


@pytest.mark.parametrize("name", CAPACITY_MARKETS)
def test_capacity_conversion_uses_manufactured_cell_fraction(
    inventory, capacity_inventory, name
):
    from premise.battery import load_cell_energy_density

    pack = activity(inventory, "battery production, Li-ion, NMC955")
    fraction = exchange(pack, CELL)["amount"] / exchange(pack, pack["name"])["amount"]
    market = activity(capacity_inventory, name)
    assert_capacity_mass_balance(
        market, fraction, load_cell_energy_density()[name][2020]
    )


@pytest.mark.parametrize("year,density", [(2020, 0.34), (2035, 0.36), (2050, 0.38)])
def test_capacity_mean_and_bounds_follow_density_change(
    inventory, capacity_inventory, year, density
):
    from premise.battery import Battery, load_cell_energy_density

    pack = activity(inventory, "battery production, Li-ion, NMC955")
    fraction = exchange(pack, CELL)["amount"] / exchange(pack, pack["name"])["amount"]
    battery = object.__new__(Battery)
    battery.database = [
        deepcopy(activity(capacity_inventory, name)) for name in CAPACITY_MARKETS
    ]
    battery.year = year
    battery.write_log = lambda *args, **kwargs: None
    battery.adjust_battery_mass()
    targets = load_cell_energy_density()
    fraction_of_period = (year - 2020) / 30
    for market in battery.database:
        manufacture = exchange(market, "market for battery, Li-ion, NMC955")
        disposal = exchange(market, "market for used Li-ion battery")
        assert manufacture["amount"] * fraction * density == pytest.approx(1)
        assert disposal["amount"] == -manufacture["amount"]
        target = targets[market["name"]]
        densities = {
            key: (1 - fraction_of_period) * target[2020][key]
            + fraction_of_period * target[2050][key]
            for key in ("mean", "minimum", "maximum")
        }
        assert_capacity_mass_balance(market, fraction, densities)


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
def test_capacity_conversion_survives_premise_migration(
    inventory, version, monkeypatch
):
    from premise.battery import load_cell_energy_density
    from premise.inventory_imports import DefaultInventory

    monkeypatch.setattr(
        DefaultInventory, "display_unlinked_exchanges", lambda self: None
    )
    importer = DefaultInventory(
        [], "3.10", version, INVENTORY_DIR / "lci-battery-capacity.xlsx", "cutoff", True
    )
    importer.prepare_inventory()
    pack = activity(inventory, "battery production, Li-ion, NMC955")
    fraction = exchange(pack, CELL)["amount"] / exchange(pack, pack["name"])["amount"]
    targets = load_cell_energy_density()
    for name in CAPACITY_MARKETS:
        assert_capacity_mass_balance(
            activity(importer.import_db.data, name), fraction, targets[name][2020]
        )
