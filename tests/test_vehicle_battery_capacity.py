"""The supplied PHEV inventories must purchase the full installed battery."""

import csv

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR


@pytest.fixture(scope="module", params=[("pass_cars", 16), ("trucks", 7)])
def catalogue(request):
    family, expected_phevs = request.param
    datasets = ExcelImporter(str(INVENTORY_DIR / f"lci-{family}.xlsx")).data
    with (INVENTORY_DIR / f"lci-{family}.parameters.csv").open() as stream:
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
