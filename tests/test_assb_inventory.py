"""Independent capacity/material checks for the three published ASSB designs."""

from copy import deepcopy
import json
from types import SimpleNamespace

from bw2io import ExcelImporter
import numpy as np
import pytest

from premise.battery import Battery, load_cell_energy_density
from premise.data_collection import IAMDataCollection
from premise.filesystem_constants import DATA_DIR, INVENTORY_DIR

CHEMISTRIES = ("polymer", "oxidic", "sulfidic")
FRACTION = 0.72591


@pytest.fixture(scope="module")
def inventory():
    return ExcelImporter(INVENTORY_DIR / "lci-batteries-ASSB.xlsx").data


@pytest.fixture(scope="module")
def sources():
    return json.loads(
        (DATA_DIR / "battery/solid_state_reference_designs.json").read_text()
    )


def activity(data, name):
    (ds,) = [ds for ds in data if ds["name"] == name]
    return ds


def feed(ds, name):
    return sum(
        e["amount"]
        for e in ds["exchanges"]
        if e["type"] == "technosphere" and e["name"] == name
    )


@pytest.mark.parametrize(
    "chemistry,cam,capacity,voltage",
    [
        ("polymer", "market for lithium iron phosphate", 150, 3.5),
        ("oxidic", "market for NMC811 oxide", 213.6, 4.15),
    ],
)
def test_pouch_dry_mass_and_cathode_capacity_close(
    inventory, chemistry, cam, capacity, voltage
):
    cell = activity(inventory, f"battery cell production, ASSB, {chemistry}")
    retained = [
        e
        for e in cell["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "kilogram"
        and "sheet rolling" not in e["name"]
        and "extrusion" not in e["name"]
    ]
    assert sum(e["amount"] for e in retained) == pytest.approx(1)
    # kg active/kg cell * Ah/kg active * V / 1000 = kWh/kg cell.
    assert feed(cell, cam) * capacity * voltage / 1000 == pytest.approx(
        cell["battery cell energy density"]
    )
    assert not any(
        "graphite" in e["name"] or "copper collector" in e["name"]
        for e in cell["exchanges"]
    )


@pytest.mark.parametrize("chemistry", CHEMISTRIES)
@pytest.mark.parametrize("year", [2020, 2035, 2050])
def test_capacity_pack_cell_and_end_of_life_close(inventory, chemistry, year):
    data = deepcopy(inventory)
    cell = activity(data, f"battery cell production, ASSB, {chemistry}")
    pack = activity(data, f"battery production, ASSB, {chemistry}")
    market = activity(data, f"market for battery capacity, ASSB, {chemistry}")
    battery = object.__new__(Battery)
    battery.database = data
    battery.year = year
    battery.write_log = lambda *args, **kwargs: None
    before = deepcopy(market)
    battery.adjust_battery_mass()
    assert {k: v for k, v in market.items() if k != "log parameters"} == before
    pack_market = activity(data, f"market for battery, ASSB, {chemistry}")
    assert feed(pack_market, pack["name"]) == 1
    pack_kg = feed(market, pack_market["name"])
    assert feed(pack, cell["name"]) == pytest.approx(FRACTION)
    assert pack_kg * FRACTION * cell["battery cell energy density"] == pytest.approx(1)
    assert feed(market, "market for used Li-ion battery") == pytest.approx(-pack_kg)
    assert load_cell_energy_density()[market["name"]][2020]["mean"] == pytest.approx(
        cell["battery cell energy density"]
    )
    assert [
        e["name"]
        for e in pack["exchanges"]
        if e["type"] == "technosphere" and "battery cell" in e["name"]
    ] == [cell["name"]]


def test_sulfide_cathode_yields_purchase_exactly_one_kwh(inventory, sources):
    cell = activity(inventory, "battery cell production, ASSB, sulfidic")
    mass_per_kwh = 1 / cell["battery cell energy density"]
    data = sources["rietdorf_cell"]
    retained_cam = (
        feed(cell, "market for NMC811 oxide")
        * mass_per_kwh
        * data["cathode_yield"]
        * data["assembly_yield"]
        * data["cell_yield"]
    )
    assert retained_cam == pytest.approx(1000 / (200 * 3.75))
    assert retained_cam * 200 * 3.75 / 1000 == pytest.approx(1)
    assert sum(data["retained_kg_per_cell"].values()) == pytest.approx(
        data["total_mass_kg"]
    )
    assert data["retained_kg_per_cell"][
        "NMC811 (row 24)"
    ] * 200 * 3.75 == pytest.approx(data["cell_energy_Wh"])


def test_sulfide_hdpe_uses_market_with_original_material_quantity(inventory, sources):
    cell = activity(inventory, "battery cell production, ASSB, sulfidic")
    (exchange,) = [
        e
        for e in cell["exchanges"]
        if (e.get("reference product") or e.get("product"))
        == "polyethylene, high density, granulate"
    ]
    (source,) = [
        r
        for r in sources["rietdorf_scenario_1_rows"]
        if r["flow"] == "polyethylene, high density, granulate"
    ]
    expected = (
        source["amount"]
        * cell["source capacity normalization"]
        * cell["battery cell energy density"]
    )
    assert exchange["name"] == "market for polyethylene, high density, granulate"
    assert exchange["location"] == "GLO"
    assert exchange["unit"] == "kilogram"
    assert exchange["amount"] == pytest.approx(expected)


def test_sulfide_plating_uses_area_and_original_process_units(inventory, sources):
    cell = activity(inventory, "battery cell production, ASSB, sulfidic")
    normalization = cell["source capacity normalization"]
    area = 0.610659 * normalization  # m2 per normalized kWh, not kg nickel.
    kg_per_kwh = 1 / cell["battery cell energy density"]
    nickel = feed(cell, "market for nickel, class 1") * kg_per_kwh
    assert nickel == pytest.approx(area * 1e-6 * 8900 / 0.95)
    assert 0.005 < nickel < 0.006
    assert nickel / (0.610659 * normalization) == pytest.approx(0.00936842105263158)
    wastewater = [
        e
        for e in cell["exchanges"]
        if "Chordia" in e.get("comment", "")
        and e["type"] == "technosphere"
        and e["unit"] == "cubic meter"
    ]
    assert len(wastewater) == 1
    assert wastewater[0]["amount"] * kg_per_kwh == pytest.approx(-area * 8 / 1000)
    # Gross precursor supply still includes losses; no retained-stock assertion.
    assert feed(cell, "market for lithium carbonate") > 0


def test_electrolyte_identity_and_lithium_supply_are_explicit(inventory):
    peo = activity(inventory, "polyethylene oxide production, proxy, for ASSB")
    assert feed(peo, "market for ethylene oxide") == 1
    polymer = activity(
        inventory, "polymer electrolyte production, PEO-LiTFSI, for ASSB"
    )
    assert feed(polymer, peo["name"]) == pytest.approx(0.8)
    assert feed(
        polymer, "LiTFSi, (CF3SO2)2NLi, production, for LiS battery"
    ) == pytest.approx(0.2)
    llzo = activity(inventory, "oxide electrolyte production, LLZO, for ASSB")
    for name, quantity in [
        ("lithium carbonate", 0.339),
        ("lanthanum oxide", 0.582),
        ("zirconium oxide", 0.318),
    ]:
        assert feed(llzo, f"market for {name}") == pytest.approx(quantity)
    assert not any(
        "Sodium-ion" in e["name"] for ds in inventory for e in ds["exchanges"]
    )


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
def test_premise_import_retains_capacity_identity_and_biosphere_links(
    version, monkeypatch
):
    from premise.inventory_imports import DefaultInventory

    monkeypatch.setattr(
        DefaultInventory, "display_unlinked_exchanges", lambda self: None
    )
    importer = DefaultInventory(
        [], "3.10", version, INVENTORY_DIR / "lci-batteries-ASSB.xlsx", "cutoff", False
    )
    importer.prepare_inventory()
    assert len(importer.import_db.data) == 15
    for ds in importer.import_db.data:
        assert ds["classifications"]
        for e in ds["exchanges"]:
            if e["type"] == "biosphere":
                assert e["input"][1]
            elif e["type"] == "technosphere":
                # Background is deliberately absent in this portable test;
                # external product identity must remain explicit for linking.
                assert e.get("product") or e.get("reference product")
    for chemistry in CHEMISTRIES:
        market = activity(
            importer.import_db.data, f"market for battery capacity, ASSB, {chemistry}"
        )
        assert sum(
            e["amount"] for e in market["exchanges"] if e["type"] == "technosphere"
        ) == pytest.approx(0)


@pytest.mark.parametrize("scenario", ["MIX", "PLIB", "LFP", "NCX"])
@pytest.mark.parametrize("year", [2020, 2022, 2035, 2040, 2050])
def test_installed_forecasts_preserve_distinct_chemistries(inventory, scenario, year):
    data = ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data + deepcopy(
        inventory
    )
    mobile = IAMDataCollection.fetch_external_data_battery_mobile_scenarios(None)
    battery = object.__new__(Battery)
    battery.database = data
    battery.year = year
    battery.iam_data = SimpleNamespace(
        battery_mobile_scenarios=mobile, battery_stationary_scenarios=None
    )
    battery.write_log = lambda *args, **kwargs: None
    battery.adjust_battery_market_shares()
    name = {"MIX": "MIX", "PLIB": "PLiB", "LFP": "LFP", "NCX": "NCx"}[scenario]
    market = activity(data, f"market for battery capacity ({name} scenario)")
    shares = mobile.sel(scenario=scenario).interp(year=min(year, 2040)).fillna(0)
    total = shares.sum().item()
    assert sum(
        e["amount"] for e in market["exchanges"] if e["type"] == "technosphere"
    ) == pytest.approx(1)
    for chemistry in CHEMISTRIES:
        assert feed(
            market, f"market for battery capacity, ASSB, {chemistry}"
        ) == pytest.approx(shares.sel(chemistry=f"ASSB ({chemistry})").item() / total)
    assert feed(
        market, "market for battery capacity, Sodium-ion, SiB"
    ) == pytest.approx(shares.sel(chemistry="SIB").item() / total)


@pytest.mark.parametrize("chemistry", CHEMISTRIES)
def test_no_write_foreground_solve_has_no_hidden_conventional_cells(
    inventory, chemistry
):
    # Solve the full foreground, including electrolytes, rather than checking
    # only individual formulas. External background inputs are boundary flows.
    keys = {
        (ds["name"], ds["reference product"], ds["location"]): i
        for i, ds in enumerate(inventory)
    }
    matrix = np.eye(len(inventory))
    for j, ds in enumerate(inventory):
        for e in ds["exchanges"]:
            key = (e["name"], e.get("reference product"), e.get("location"))
            if e["type"] == "technosphere" and key in keys:
                matrix[keys[key], j] -= e["amount"]
    demand = np.zeros(len(inventory))
    market = activity(inventory, f"market for battery capacity, ASSB, {chemistry}")
    demand[keys[(market["name"], market["reference product"], "GLO")]] = 1
    supply = np.linalg.solve(matrix, demand)
    expected_cells = 1 / market["battery cell energy density"]
    cell = activity(inventory, f"battery cell production, ASSB, {chemistry}")
    assert supply[
        keys[(cell["name"], cell["reference product"], "GLO")]
    ] == pytest.approx(expected_cells)
    for other in set(CHEMISTRIES) - {chemistry}:
        assert (
            supply[
                keys[
                    (
                        f"battery cell production, ASSB, {other}",
                        f"battery cell, ASSB, {other}",
                        "GLO",
                    )
                ]
            ]
            == 0
        )


def test_default_registry_imports_assb_before_scenario_markets(monkeypatch):
    import premise.new_database as module

    builder = object.__new__(module.NewDatabase)
    builder.version = "3.12"
    selected = []

    def capture(filepaths):
        selected.extend(filepaths)
        return []  # Registry check only; no source database is needed.

    monkeypatch.setattr(module, "_extract_default_inventory_importers", capture)
    builder._NewDatabase__import_inventories()
    assert (module.FILEPATH_ASSB_BATTERIES, "3.10") in selected
    assert selected.index((module.FILEPATH_ASSB_BATTERIES, "3.10")) < selected.index(
        (module.FILEPATH_BATTERY_CAPACITY, "3.10")
    )


def test_assb_change_invalidates_imports_without_reextracting_source(
    tmp_path, monkeypatch
):
    import premise.new_database as module

    builder = object.__new__(module.NewDatabase)
    builder.keep_imports_uncertainty = builder.keep_source_db_uncertainty = False
    path = tmp_path / "assb.xlsx"
    path.write_bytes(b"first ASSB inventory")
    monkeypatch.setattr(module, "FILEPATH_ASSB_BATTERIES", path)
    original = builder._database_cache_path("test", inventories=True)
    source = builder._database_cache_path("test")
    path.write_bytes(b"updated ASSB inventory")
    assert builder._database_cache_path("test", inventories=True) != original
    assert builder._database_cache_path("test") == source
