"""The supplied PHEV inventories must purchase the full installed battery."""

import csv
import json
from pathlib import Path

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR

PARAMETERS_DIR = Path(__file__).parent / "data" / "vehicle_inventory_parameters"


@pytest.fixture(scope="module", params=[("pass_cars", 16), ("trucks", 7)])
def catalogue(request):
    family, expected_phevs = request.param
    datasets = ExcelImporter(str(INVENTORY_DIR / f"lci-{family}.xlsx")).data
    with (PARAMETERS_DIR / f"lci-{family}.parameters.csv").open() as stream:
        parameters = list(csv.DictReader(stream))
    phevs = [p for p in parameters if p["powertrain"].startswith("PHEV-")]
    assert len(phevs) == expected_phevs
    return {d["name"]: d for d in datasets}, phevs


def test_phev_capacity_agrees_with_physical_pack(catalogue):
    _, parameters = catalogue
    for row in parameters:
        p = {
            name: float(row[name])
            for name in (
                "electric energy stored",
                "battery cell mass",
                "battery cell energy density",
                "energy battery mass",
                "battery BoP mass",
                "battery cell mass share",
            )
        }
        assert p["electric energy stored"] > 0
        assert p["electric energy stored"] == pytest.approx(
            p["battery cell mass"] * p["battery cell energy density"], rel=1e-6
        )
        assert p["battery cell mass"] == pytest.approx(
            p["energy battery mass"] * p["battery cell mass share"], rel=1e-6
        )
        assert p["energy battery mass"] == pytest.approx(
            p["battery cell mass"] + p["battery BoP mass"], rel=1e-6
        )


def test_car_phev_calibration_survives_excel_and_keeps_relative_density(catalogue):
    datasets, rows = catalogue
    cars = [r for r in rows if r["name"].startswith("transport, passenger car,")]
    if not cars:
        pytest.skip("Passenger-car calibration only")
    expected = {
        "Mini": (8.8, 120),
        "Small": (8.8, 120),
        "Lower medium": (13, 128.375),
        "Medium": (12, 118.5),
        "Large": (12, 118.5),
        "Medium SUV": (18.1, 178.7375),
        "Large SUV": (24, 226.8),
        "Van": (13.6, 134.3),
    }
    for row in cars:
        capacity, mass = expected[row["size"]]
        assert float(row["electric energy stored"]) == pytest.approx(capacity, rel=2e-6)
        assert float(row["energy battery mass"]) == pytest.approx(mass, rel=2e-6)
        service = datasets[row["name"]]
        specification = json.loads(service["vehicle battery sizing"])
        assert specification["density projection basis"] == "relative chemistry trend"
        provenance = json.loads(service["vehicle inventory provenance"])
        pack = provenance["PHEV battery calibration"]
        assert pack["capacity kWh"] == capacity
        assert pack["pack mass kg"] == pytest.approx(mass)
        assert pack["capacity source"] in service["comment"]
        assert pack["mass source"] in service["comment"]
        assert "MIX capacity market" in service["comment"]


def test_phev_lifetime_capacity_purchased_once(catalogue):
    datasets, parameters = catalogue
    for row in parameters:
        service = datasets[row["name"]]
        vehicles = [
            e
            for e in service["exchanges"]
            if e["type"] == "technosphere"
            and e["name"].startswith(("Passenger car,", "lorry production,"))
        ]
        assert len(vehicles) == 1
        production = datasets[vehicles[0]["name"]]
        battery = [
            e
            for e in production["exchanges"]
            if e["name"] == "market for battery capacity (MIX scenario)"
        ]
        assert len(battery) == 1
        assert battery[0]["unit"] == "kilowatt hour"
        expected = float(row["electric energy stored"]) * (
            1 + float(row["battery lifetime replacements"])
        )
        assert battery[0]["amount"] == pytest.approx(expected, rel=1e-6)
        assert not any(
            e["name"].startswith("market for battery, Li-")
            or e["name"] == "market for used Li-ion battery"
            for e in production["exchanges"]
        )


def test_phev_replacements_follow_the_family_lifetime_policy(catalogue):
    _, parameters = catalogue
    for row in parameters:
        lifetime = float(row["lifetime kilometers"])
        if row["name"].startswith("transport, passenger car,"):
            battery_km = float(row["battery lifetime kilometers"])
            assert battery_km == 200000
            expected = max(
                lifetime / battery_km - 1,
                lifetime / float(row["kilometers per year"]) / 18 - 1,
                0,
            )
        else:
            # Regional delivery: 60 km electric per 400-km trip, with a
            # nominal pack storing 60/DoD electric-km per equivalent full cycle.
            electric_km = lifetime * float(row["electric utility factor"])
            one_pack_km = (
                60 / float(row["battery DoD"]) * float(row["battery cycle life"])
            )
            expected = min(max(electric_km / one_pack_km - 1, 0), 3)
        assert float(row["battery lifetime replacements"]) == pytest.approx(
            expected, abs=2e-6
        )


@pytest.fixture(scope="module")
def truck_hevs():
    datasets = ExcelImporter(str(INVENTORY_DIR / "lci-trucks.xlsx")).data
    with (PARAMETERS_DIR / "lci-trucks.parameters.csv").open() as stream:
        parameters = [
            row for row in csv.DictReader(stream) if row["powertrain"] == "HEV-d"
        ]
    assert len(parameters) == 9  # seven diesel HEVs and two gasoline proxies
    return {d["name"]: d for d in datasets}, parameters


def test_hev_storage_and_lifetime_capacity_reach_the_inventory(truck_hevs):
    datasets, parameters = truck_hevs
    for row in parameters:
        capacity = float(row["electric energy stored"])
        mass = float(row["energy battery mass"])
        assert capacity > 0 and mass > 0
        assert capacity == pytest.approx(
            mass
            * float(row["battery cell mass share"])
            * float(row["battery cell energy density"]),
            rel=1e-6,
        )
        lifetime_energy = float(row["lifetime kilometers"]) * float(
            row["HEV discharged stored kWh per km"]
        )
        expected_packs = min(
            max(lifetime_energy / capacity / float(row["battery cycle life"]), 1), 4
        )
        assert float(row["battery lifetime replacements"]) + 1 == pytest.approx(
            expected_packs, rel=2e-6
        )
        service = datasets[row["name"]]
        production = datasets[
            next(
                e["name"]
                for e in service["exchanges"]
                if e["name"].startswith("lorry production,")
            )
        ]
        purchases = [
            e
            for e in production["exchanges"]
            if e["name"] == "market for battery capacity (MIX scenario)"
        ]
        assert len(purchases) == 1
        assert purchases[0]["unit"] == "kilowatt hour"
        assert purchases[0]["amount"] == pytest.approx(
            capacity * expected_packs, rel=2e-6
        )
        assert not any(
            e["name"].startswith("market for battery, Li-")
            or e["name"] == "market for used Li-ion battery"
            for e in production["exchanges"]
        )
        assert 0 < float(row["HEV regeneration reuse fraction"]) <= 1


def test_hev_comments_disclose_sources_and_the_approved_mix_proxy(truck_hevs):
    datasets, parameters = truck_hevs
    for row in parameters:
        comment = datasets[row["name"]]["comment"]
        assert "A123 Systems (2014)" in comment
        assert "carculator_truck" in comment
        assert "MIX capacity-market proxy" in comment
        assert "No mass-equivalence correction" in comment
        assert "not a high-power pack quotation" in comment
