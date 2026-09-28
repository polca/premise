"""Fuel-floor invariants and the Reunion NEU 2050 regression."""

from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from premise.car_energy import apply_floor, car_class, fuel_balance, minimum_energy
from premise.transport import Transport
from premise.validation import CarValidation


def car(energy=0.8, fuel="natural gas", unit="kilogram", production=1):
    lhv = {
        "natural gas": 47.5 if unit == "kilogram" else 36,
        "diesel": 43,
        "petrol": 42.6,
    }[fuel]
    factor = {"natural gas": 0.06, "diesel": 0.0732, "petrol": 0.0737}[fuel]
    powertrain = {
        "natural gas": "compressed gas",
        "diesel": "diesel",
        "petrol": "gasoline",
    }[fuel]
    return {
        "name": f"transport, passenger car, {powertrain}, Small, EURO-6",
        "reference product": "transport, passenger car, EURO-6",
        "location": "NEU",
        "unit": "kilometer",
        "exchanges": [
            {"name": "car service", "type": "production", "amount": production},
            {
                "name": f"market for {fuel}",
                "unit": unit,
                "type": "technosphere",
                "amount": energy / lhv * production,
            },
            {
                "name": "vehicle manufacture",
                "type": "technosphere",
                "unit": "unit",
                "amount": 0.001,
            },
            {
                "name": "Carbon dioxide, fossil",
                "categories": ("air",),
                "type": "biosphere",
                "amount": energy * factor * production,
            },
            {
                "name": "Particulates, tyre wear",
                "categories": ("air",),
                "type": "biosphere",
                "amount": 0.001,
            },
            {
                "name": "Water",
                "categories": ("natural resource",),
                "type": "biosphere",
                "amount": 0.002,
            },
        ],
    }


def validate(ds):
    validator = object.__new__(CarValidation)
    issues = []
    validator.log_issue = lambda ds, reason, message, issue_type="minor": issues.append(
        (reason, message, issue_type)
    )
    validator.check_car_fuel_energy(ds)
    return issues


@pytest.mark.parametrize(
    "fuel,unit",
    [
        ("diesel", "kilogram"),
        ("petrol", "kilogram"),
        ("natural gas", "kilogram"),
        ("natural gas", "cubic meter"),
    ],
)
def test_floor_and_validation(fuel, unit):
    ds = car(fuel=fuel, unit=unit)
    before = deepcopy(ds)
    event = apply_floor(ds)
    assert fuel_balance(ds)[0] == pytest.approx(0.852)
    assert event["floor correction factor"] == pytest.approx(0.852 / 0.8)
    assert validate(ds) == []
    for index in (0, 2, 4, 5):
        assert ds["exchanges"][index] == before["exchanges"][index]
    once = deepcopy(ds)
    assert apply_floor(ds) is None
    assert ds == once


@pytest.mark.parametrize("energy", [0.852, 1.0, 2.0])
def test_above_floor_unchanged(energy):
    ds = car(energy)
    before = deepcopy(ds)
    assert apply_floor(ds) is None
    assert ds == before


def test_multiple_fuels_and_nonunit_production():
    ds = car(0.4, production=2)
    extra = car(0.4, fuel="diesel", production=2)
    ds["exchanges"].extend([extra["exchanges"][1], extra["exchanges"][3]])
    before_ratio = ds["exchanges"][1]["amount"] / ds["exchanges"][-2]["amount"]
    assert fuel_balance(ds)[0] == pytest.approx(0.8)
    apply_floor(ds)
    assert fuel_balance(ds)[0] == pytest.approx(0.852)
    assert ds["exchanges"][1]["amount"] / ds["exchanges"][-2][
        "amount"
    ] == pytest.approx(before_ratio)
    assert validate(ds) == []


@pytest.mark.parametrize(
    "name",
    [
        "transport, passenger car, plugin gasoline hybrid, Small",
        "transport, passenger car, battery electric, Small",
        "transport, passenger car, fuel cell electric, Small",
        "transport, freight, lorry, diesel, Small",
    ],
)
def test_excluded_vehicles(name):
    ds = car()
    ds["name"] = name
    assert car_class(name) is None
    assert apply_floor(ds) is None


@pytest.mark.parametrize("amount", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_fuel_rejected(amount):
    ds = car()
    ds["exchanges"][1]["amount"] = amount
    with pytest.raises(ValueError):
        apply_floor(ds)


def test_unknown_units_and_missing_production_rejected():
    ds = car()
    ds["exchanges"][1]["unit"] = "litre"
    with pytest.raises(ValueError, match="Unsupported"):
        apply_floor(ds)
    ds = car()
    ds["exchanges"].pop(0)
    with pytest.raises(ValueError, match="production"):
        apply_floor(ds)


def test_configurable_and_disabled_floor():
    ds = car()
    config = {
        "enabled": True,
        "minimum_mj_per_vehicle_km": 0.852,
        "overrides": {"compressed gas": {"small": 0.9}},
    }
    assert minimum_energy(ds, config) == 0.9
    apply_floor(ds, config)
    assert fuel_balance(ds)[0] == pytest.approx(0.9)
    config["enabled"] = False
    ds = car()
    before = deepcopy(ds)
    assert apply_floor(ds, config) is None
    assert ds == before


def test_uncertainty_and_carbon_split_preserved():
    ds = car()
    fuel = ds["exchanges"][1]
    fuel.update({"uncertainty type": 2, "loc": np.log(fuel["amount"]), "scale": 0.2})
    bio = deepcopy(ds["exchanges"][3])
    bio.update(name="Carbon dioxide, non-fossil", amount=0.01)
    ds["exchanges"].append(bio)
    ratio = ds["exchanges"][3]["amount"] / bio["amount"]
    apply_floor(ds)
    assert fuel["loc"] == pytest.approx(np.log(fuel["amount"]))
    assert fuel["scale"] == 0.2
    assert ds["exchanges"][3]["amount"] / bio["amount"] == pytest.approx(ratio)


def test_co2_checked_even_above_floor_and_validator_does_not_mutate():
    ds = car(1.0)
    ds["exchanges"][3]["amount"] *= 2
    before = deepcopy(ds)
    assert validate(ds)[0][0] == "CO2 emissions incorrect"
    assert ds == before


def test_recorded_neu_2050_regression():
    ds = car(1.9635545928637712 / 100 * 42.6)
    ds["exchanges"][3]["amount"] = 0.04700672745633079
    assert [i[0] for i in validate(ds)] == ["fuel consumption incorrect"]
    event = apply_floor(ds)
    assert event["floor correction factor"] == pytest.approx(2 / 1.9635545928637712)
    assert validate(ds) == []


@pytest.mark.parametrize(
    "fuel,alias", [("natural gas", "natural gas"), ("petrol", "gasoline")]
)
def test_transformation_applies_floor_after_iam_and_logs_it(fuel, alias):
    ds = car(1.0, fuel=fuel)
    ds["exchanges"][2]["name"] = "Passenger car, gasoline, Small, EURO-6d"
    manufacture = deepcopy(ds["exchanges"][2])
    tr = object.__new__(Transport)
    tr.vehicle_type = "car"
    tr.year, tr.model, tr.scenario = 2050, "remind", "SSP3-rollBack"
    tr.rev_map = {ds["name"]: "gas car"}
    tr.vehicle_fuel_map = {"gas car": [{"name": alias}]}
    tr.iam_data = SimpleNamespace(
        passenger_car_efficiencies=xr.DataArray(
            [1.0], coords={"variables": ["gas car"]}, dims=["variables"]
        )
    )
    tr.find_iam_efficiency_change = lambda **kwargs: 1.25
    tr.write_log = lambda ds: None
    tr.adjust_transport_efficiency(ds)
    assert fuel_balance(ds)[0] == pytest.approx(0.852)
    assert ds["log parameters"]["car energy floor"][
        "source energy MJ/km"
    ] == pytest.approx(1.0)
    assert validate(ds) == []
    assert ds["exchanges"][2] == manufacture
    tr.iam_data.passenger_car_efficiencies = None
    before = deepcopy(ds)
    tr.adjust_transport_efficiency(ds)
    assert ds == before
