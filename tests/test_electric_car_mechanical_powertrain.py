"""Component and per-MJ checks independent of whole-ICE proxy substitution."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import runpy
import shutil

from bw2io import ExcelImporter
import numpy as np
import pytest

from premise.filesystem_constants import INVENTORY_DIR
from premise import vehicle_energy_use as energy

OLD = "market for internal combustion engine, passenger car"
PRODUCT = "mechanical powertrain, electric passenger car"
MARKET = f"market for {PRODUCT}"
AUXILIARY = f"market for {energy.COMMON_AUXILIARY_BATTERY}"


@pytest.fixture(scope="module")
def cars():
    return ExcelImporter(INVENTORY_DIR / "lci-pass_cars.xlsx").data


@pytest.fixture(scope="module")
def mechanical():
    return ExcelImporter(
        INVENTORY_DIR / "lci-electric-car-mechanical-powertrain.xlsx"
    ).data


@pytest.fixture(scope="module")
def final_energy():
    return ExcelImporter(INVENTORY_DIR / "lci-final-energy.xlsx").data


def activity(data, name):
    (ds,) = [ds for ds in data if ds["name"] == name]
    return ds


def feed(ds, name):
    return sum(
        e["amount"]
        for e in ds["exchanges"]
        if e["type"] == "technosphere" and e["name"] == name
    )


def electric_cars(data):
    return [
        ds
        for ds in data
        if ds["name"].startswith(
            ("Passenger car, battery electric,", "Passenger car, fuel cell electric,")
        )
    ]


def test_electric_cars_do_not_buy_combustion_engines(cars):
    vehicles = electric_cars(cars)
    assert len(vehicles) == 17
    for car in vehicles:
        assert not any(e["name"] == OLD for e in car["exchanges"])
        assert feed(car, MARKET) > 0
        assert feed(car, AUXILIARY) > 0
        # Both common and energy-specific components sum to the original model
        # powertrain mass; its two source parameters are independent inputs.
        power = json.loads(car["vehicle inventory provenance"])[
            "baseline vehicle parameters"
        ]["power"]
        expected = 34.34782608695652 + 0.3934782608695652 * power
        assert feed(car, MARKET) + feed(car, AUXILIARY) == pytest.approx(
            expected, rel=2e-6
        )
        assert feed(car, "market for electric motor, electric passenger car") > 0
        assert feed(car, "market for inverter, for electric passenger car") > 0
        if "battery electric" in car["name"]:
            assert feed(car, "market for charger, electric passenger car") > 0
        if "fuel cell electric" in car["name"]:
            assert any(
                "fuel cell" in e["name"]
                and e["type"] == "technosphere"
                and e["amount"] > 0
                for e in car["exchanges"]
            )


def test_mechanical_recipe_does_not_duplicate_electric_modules(mechanical):
    production = activity(
        mechanical, "mechanical powertrain production, electric passenger car"
    )
    forbidden = (
        "platinum",
        "palladium",
        "lead",
        "electric motor",
        "inverter",
        "converter",
        "charger",
        "fuel cell",
        "battery",
        "cable",
    )
    assert not any(
        any(term in e["name"].lower() for term in forbidden)
        for e in production["exchanges"]
        if e["type"] == "technosphere"
    )
    assert feed(
        production, "market for steel, low-alloyed, hot rolled"
    ) == pytest.approx(12.5 / 25)
    assert feed(production, "market for aluminium, cast alloy") == pytest.approx(
        (11.25 + 0.75) / 25
    )
    assert feed(production, "market for reinforcing steel") == pytest.approx(
        (0.9 + 0.45) / 25
    )
    assert feed(production, "market for ethylene glycol") == pytest.approx(2.1 / 25)
    gross_materials = [
        e
        for e in production["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "kilogram"
        and e["amount"] > 0
        and e["name"] != "market group for tap water"
    ]
    scrap = [
        e
        for e in production["exchanges"]
        if e["type"] == "technosphere"
        and e["amount"] < 0
        and "used mechanical" not in e["name"]
    ]
    assert sum(e["amount"] for e in gross_materials) == pytest.approx(1.2212)
    assert sum(e["amount"] for e in gross_materials + scrap) == pytest.approx(1)


def test_mechanical_end_of_life_uses_its_own_materials(mechanical):
    treatment = activity(
        mechanical, "treatment of used mechanical powertrain, electric passenger car"
    )
    assert treatment["production amount"] == -1
    waste = [
        e
        for e in treatment["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "kilogram"
    ]
    assert sum(e["amount"] for e in waste) == pytest.approx(-1)
    assert feed(treatment, "market for iron scrap, unsorted") == pytest.approx(-0.436)
    assert feed(
        treatment, "market for aluminium scrap, post-consumer"
    ) == pytest.approx(-0.384)
    assert not any(
        "copper" in e["name"] or "electronics scrap" in e["name"] for e in waste
    )


def test_vehicle_scrap_uses_native_markets_in_both_workbooks(mechanical):
    inventories = [
        mechanical,
        ExcelImporter(INVENTORY_DIR / "lci-two_wheelers.xlsx").data,
    ]
    locations = {
        "iron scrap, unsorted": "GLO",
        "aluminium scrap, new": "RoW",
        "aluminium scrap, post-consumer": "GLO",
    }
    checked = 0
    for datasets in inventories:
        for dataset in datasets:
            for exchange in dataset["exchanges"]:
                product = exchange.get("product") or exchange.get("reference product")
                if exchange["type"] != "technosphere" or product not in locations:
                    continue
                assert exchange["name"] == f"market for {product}"
                assert exchange["location"] == locations[product]
                assert exchange["unit"] == "kilogram"
                assert exchange["amount"] < 0
                checked += 1
    assert checked == 20


def test_common_battery_does_not_mean_zero_full_vehicle_lead(cars, mechanical):
    source = activity(
        mechanical, "auxiliary battery production, passenger car, historical proxy"
    )
    assert feed(source, "market for lead") == pytest.approx(13 / 18)
    for car in electric_cars(cars):
        original_mass = feed(car, MARKET) + feed(car, AUXILIARY)
        # Preserve the old source's 13 kg lead / 275 kg reference in the full
        # vehicle, without asserting a measured EV battery mass or service life.
        assert feed(car, AUXILIARY) * feed(source, "market for lead") == pytest.approx(
            original_mass * 13 / 275
        )
    assert "unvalidated" in source["comment"]


@pytest.mark.parametrize("field", ["product", "reference product", "name only"])
def test_common_auxiliary_boundary_does_not_exclude_traction_batteries(field):
    e = {"name": AUXILIARY, "type": "technosphere", "unit": "kilogram"}
    if field != "name only":
        e[field] = energy.COMMON_AUXILIARY_BATTERY
    assert energy.equipment(e) is False
    for name in [
        "market for battery capacity (MIX scenario)",
        "market for inverter, for electric passenger car",
        MARKET,
    ]:
        assert energy.equipment({"name": name}) is True


@pytest.mark.parametrize(
    "name",
    [
        "electricity, used in passenger car",
        "hydrogen, used in a fuel cell in a passenger car",
    ],
)
def test_final_energy_reconstructs_mechanics_and_excludes_common_battery(
    cars, final_energy, name
):
    wrapper = activity(final_energy, name)
    record = json.loads(wrapper[energy.SOURCE_KEY])
    source = activity(cars, record["source"][0])
    vehicle = next(
        e
        for e in source["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "unit"
        and e["name"].startswith("Passenger car,")
    )
    production = activity(cars, vehicle["name"])
    carrier = "electricity" if name.startswith("electricity") else "hydrogen"
    amount = sum(
        e["amount"]
        for e in source["exchanges"]
        if e["type"] == "technosphere"
        and (
            (
                carrier == "electricity"
                and e["unit"] == "kilowatt hour"
                and "electricity"
                in (e.get("reference product") or e.get("product") or "")
            )
            or (
                carrier == "hydrogen"
                and e["unit"] == "kilogram"
                and "hydrogen" in (e.get("reference product") or e.get("product") or "")
            )
        )
    )
    q = amount * (3.6 if carrier == "electricity" else 120)
    assert record["purchased MJ per source service"] == pytest.approx(q)
    assert feed(wrapper, MARKET) == pytest.approx(
        feed(production, MARKET) * vehicle["amount"] / q
    )
    assert not any(e["name"] in {OLD, AUXILIARY} for e in wrapper["exchanges"])
    assert any(
        row["supplier"][0] == AUXILIARY
        for row in record["excluded"]
        if isinstance(row["supplier"], list)
    )
    rebuilt = deepcopy(wrapper)
    energy.normalize(rebuilt, source, cars)
    assert feed(rebuilt, MARKET) == pytest.approx(feed(wrapper, MARKET))
    assert not any(e["name"] in {OLD, AUXILIARY} for e in rebuilt["exchanges"])
    # Existing electric motor/controls/traction-battery and fuel-cell quantities
    # must agree after independent normalization of the actual source service.
    for e in wrapper["exchanges"]:
        if e["type"] == "technosphere":
            assert feed(rebuilt, e["name"]) == pytest.approx(
                feed(wrapper, e["name"]), rel=2e-6
            )


def test_foreground_solve_closes_treatment_and_keeps_auxiliary_separate(mechanical):
    keys = {
        (d["name"], d["reference product"], d["location"]): i
        for i, d in enumerate(mechanical)
    }
    matrix = np.diag([d["production amount"] for d in mechanical]).astype(float)
    for column, ds in enumerate(mechanical):
        for e in ds["exchanges"]:
            key = (e["name"], e.get("reference product"), e.get("location"))
            if e["type"] == "technosphere" and key in keys:
                matrix[keys[key], column] -= e["amount"]
    demand = np.zeros(len(mechanical))
    demand[keys[(MARKET, PRODUCT, "GLO")]] = 1
    supply = np.linalg.solve(matrix, demand)
    for ds, value in zip(mechanical, supply):
        assert value == pytest.approx(0 if "auxiliary battery" in ds["name"] else 1)


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
def test_premise_import_retains_internal_products_and_waste_signs(version, monkeypatch):
    from premise.inventory_imports import DefaultInventory

    monkeypatch.setattr(
        DefaultInventory, "display_unlinked_exchanges", lambda self: None
    )
    importer = DefaultInventory(
        [],
        "3.10",
        version,
        INVENTORY_DIR / "lci-electric-car-mechanical-powertrain.xlsx",
        "cutoff",
        False,
    )
    importer.prepare_inventory()
    assert len(importer.import_db.data) == 6
    assert all(ds["classifications"] for ds in importer.import_db.data)
    for ds in importer.import_db.data:
        production = [e for e in ds["exchanges"] if e["type"] == "production"]
        assert len(production) == 1
        assert production[0]["amount"] == (-1 if "used mechanical" in ds["name"] else 1)


def test_registry_order_and_cache_invalidation(tmp_path, monkeypatch):
    import premise.new_database as module

    builder = object.__new__(module.NewDatabase)
    builder.version = "3.12"
    selected = []
    monkeypatch.setattr(
        module,
        "_extract_default_inventory_importers",
        lambda paths: selected.extend(paths) or [],
    )
    builder._NewDatabase__import_inventories()
    assert selected.index(
        (module.FILEPATH_ELECTRIC_CAR_MECHANICAL, "3.10")
    ) < selected.index((module.FILEPATH_PASS_CARS, "3.12"))
    builder.keep_imports_uncertainty = builder.keep_source_db_uncertainty = False
    path = tmp_path / "mechanical.xlsx"
    path.write_bytes(b"old recipe")
    monkeypatch.setattr(module, "FILEPATH_ELECTRIC_CAR_MECHANICAL", path)
    old = builder._database_cache_path("test", inventories=True)
    source = builder._database_cache_path("test")
    path.write_bytes(b"new recipe")
    assert builder._database_cache_path("test", inventories=True) != old
    assert builder._database_cache_path("test") == source


def test_workbook_updater_is_idempotent_without_rewriting_inputs(tmp_path):
    script = (
        Path(__file__).parents[1]
        / "scripts/update_electric_car_mechanical_inventory.py"
    )
    updater = runpy.run_path(str(script))
    for filename, function in [
        ("lci-pass_cars.xlsx", "patch_cars"),
        ("lci-final-energy.xlsx", "patch_energy"),
    ]:
        path = tmp_path / filename
        shutil.copyfile(INVENTORY_DIR / filename, path)
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        assert updater[function](path) == 0
        assert hashlib.sha256(path.read_bytes()).hexdigest() == before
