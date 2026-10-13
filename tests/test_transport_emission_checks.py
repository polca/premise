"""Transport plausibility diagnostics preserve inventories and pollutant identity."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise import transport, validation as v


def air(name, amount, **extra):
    return dict(
        name=name,
        type="biosphere",
        unit="kilogram",
        amount=amount,
        categories=("air", "urban air close to ground"),
        **extra
    )


def dataset(flows, vehicle="car", euro=None):
    prefix = (
        "transport, passenger car" if vehicle == "car" else "transport, freight, lorry"
    )
    name = (
        prefix + ", gasoline, Mini, " + (euro or "EURO-6")
        if vehicle == "car"
        else prefix + ", diesel, 40t gross weight, " + (euro or "EURO-VI")
    )
    return dict(
        name=name,
        location="EUR",
        unit="kilometer",
        exchanges=[
            dict(name=name, type="production", unit="kilometer", amount=1),
            dict(
                name=(
                    "market for petrol, low-sulfur"
                    if vehicle == "car"
                    else "market for diesel, low-sulfur"
                ),
                type="technosphere",
                unit="kilogram",
                amount=0.05,
            ),
            *flows,
        ],
    )


def validator(ds, vehicle="car"):
    obj = object.__new__(v.TransportValidation)
    obj.database, obj.regions = [ds], ["EUR"]
    obj.major_issues_log, obj.minor_issues_log, obj.validation_issues = [], [], []
    obj.exhaust = (
        v.load_car_exhaust_pollutants()
        if vehicle == "car"
        else v.load_truck_exhaust_pollutants()
    )
    return obj


def run(obj, vehicle="car"):
    obj.check_pollutant_emissions(
        "transport, passenger car" if vehicle == "car" else "transport, freight, lorry"
    )


@pytest.mark.parametrize(
    "pollutant,name,matched",
    [
        ("Methane", "Methane, fossil", True),
        ("Methane", "Methane, non-fossil", True),
        ("Methane", "NMVOC, non-methane volatile organic compounds", False),
        ("Methane", "Dichloromethane", False),
        ("Hydrocarbon", "Hydrocarbons", True),
        ("Hydrocarbon", "PAH, polycyclic aromatic hydrocarbons", False),
        ("Hydrocarbon", "Hydrocarbons, chlorinated", False),
        ("Particulate", "Particulate Matter, < 2.5 um", True),
        ("Particulate", "Particulates, < 2.5 um", True),
        ("Particulate", "Particulate Matter, > 10 um", False),
        ("Particulate", "Particulates, > 2.5 um, and < 10um", False),
        ("Benzene", "Chlorobenzene", False),
        ("Ethene", "Ethylene", True),
        ("Chromium", "Chromium VI", False),
        ("Lead", "Lead II", True),
        ("Carbon monoxide", "Carbon monoxide, non-fossil", True),
    ],
)
def test_exact_pollutant_aliases(pollutant, name, matched):
    assert v.matches_transport_air_pollutant(air(name, 1), pollutant) is matched
    assert not v.matches_transport_air_pollutant(
        dict(air(name, 1), categories=("water",)), pollutant
    )
    assert not v.matches_transport_air_pollutant(
        dict(air(name, 1), type="technosphere"), pollutant
    )


@pytest.mark.parametrize(
    "label,expected",
    [
        ("EURO-III", "3"),
        ("EURO-IV", "4"),
        ("EURO-V", "5"),
        ("EURO-VI", "6"),
        ("EURO-6ab", "6"),
        ("EURO-6", "6.2"),
        ("EURO-60", None),
        ("EURO-VII", None),
    ],
)
def test_complete_class_labels(label, expected):
    assert (
        v.transport_emission_class("transport, vehicle, diesel, size, " + label)
        == expected
    )


@pytest.mark.parametrize(
    "vehicle,euro,expected_code", [("car", "EURO-6ab", "6"), ("truck", "EURO-VI", "6")]
)
def test_validator_uses_the_actual_class_factor(vehicle, euro, expected_code):
    ds = dataset([air("Nitrogen oxides", 1e-4)], vehicle, euro)
    obj = validator(ds, vehicle)
    calls = []
    obj.validate_emissions = lambda ds, actual, expected, pollutant: calls.append(
        (pollutant, expected)
    )
    run(obj, vehicle)
    pt = "gasoline" if vehicle == "car" else "diesel"
    factors = obj.exhaust[pt][expected_code]
    factor = (factors if vehicle == "car" else factors["40t"])["Nitrogen oxides"]
    assert dict(calls)["Nitrogen oxides"] == pytest.approx(
        0.05 * (42.6 if vehicle == "car" else 43) * factor / 1000
    )


def test_combined_emissions_are_preserved_and_order_independent():
    ds = dataset(
        [
            air(
                "Carbon monoxide, fossil",
                0.000759,
                **{"uncertainty type": 3, "loc": 0.000759, "scale": 1e-5}
            ),
            air("Methane, fossil", 5.38683e-6),
            air("NMVOC, non-methane volatile organic compounds", 69.0586e-6),
            air("Benzene", 4.98645e-6),
        ]
    )
    before = deepcopy(ds)
    obj = validator(ds)
    run(obj)
    assert ds == before
    assert not obj.major_issues_log
    assert any("hot-only" in row["message"] for row in obj.minor_issues_log)
    assert any(
        row["reason"] == "transport emission scope unavailable"
        for row in obj.minor_issues_log
    )
    assert obj.calculate_actual_emission(ds, "Methane") == 5.38683e-6
    first = sorted((x["reason"], x["message"]) for x in obj.minor_issues_log)
    obj = validator(ds)
    obj.exhaust = deepcopy(obj.exhaust)
    obj.exhaust["gasoline"]["6.2"] = dict(
        reversed(list(obj.exhaust["gasoline"]["6.2"].items()))
    )
    run(obj)
    assert ds == before
    assert first == sorted((x["reason"], x["message"]) for x in obj.minor_issues_log)


@pytest.mark.parametrize("ratio", [0.49999995, 0.5, 0.50000005, 1, 2, 2.0000001])
def test_diagnostic_boundary_roundoff_is_stable(ratio):
    ds = dataset([])
    obj = validator(ds)
    obj.validate_emissions(ds, ratio * 1e-6, 1e-6, "Ammonia")
    assert not obj.validation_issues


@pytest.mark.parametrize("ratio", [0.49, 2.01, 1000])
def test_real_discrepancy_is_visible_without_mutation(ratio):
    ds = dataset([air("Carbon monoxide, fossil", ratio * 1e-6)])
    before = deepcopy(ds)
    obj = validator(ds)
    obj.validate_emissions(ds, ratio * 1e-6, 1e-6, "Carbon monoxide")
    assert ds == before
    assert [issue.severity for issue in obj.validation_issues] == ["warning"]
    assert "not a like-for-like" in obj.validation_issues[0].message


@pytest.mark.parametrize("quantity", [-1, float("inf"), float("nan")])
def test_invalid_emissions_remain_errors(quantity):
    ds = dataset([air("Methane, fossil", quantity)])
    obj = validator(ds)
    run(obj)
    assert any(
        issue.rule_id == "LEGACY.INVALID_TRANSPORT_EMISSION_AMOUNT"
        and issue.severity == "error"
        for issue in obj.validation_issues
    )


def test_wrong_emission_unit_remains_an_error():
    ds = dataset([dict(air("Nitrogen oxides", 1), unit="gram")])
    obj = validator(ds)
    run(obj)
    assert any(
        issue.rule_id == "LEGACY.INVALID_TRANSPORT_EMISSION_UNIT"
        and issue.severity == "error"
        for issue in obj.validation_issues
    )


@pytest.mark.parametrize("prefix", ["market for", "market group for"])
def test_truck_fuel_markets_are_recognized(prefix):
    ds = dataset([air("Nitrogen oxides", 1e-4)], "truck")
    ds["exchanges"][1]["name"] = prefix + " diesel, low-sulfur"
    ds["exchanges"].append(dict(ds["exchanges"][1], type="biosphere", amount=100))
    obj = validator(ds, "truck")
    assert obj.calculate_fuel_consumption(ds) == pytest.approx(0.05 * 43)


def test_missing_truck_fuel_is_not_zero_fuel():
    ds = dataset([air("Nitrogen oxides", 1e-4)], "truck")
    del ds["exchanges"][1]
    before = deepcopy(ds)
    obj = validator(ds, "truck")
    run(obj, "truck")
    assert ds == before
    assert any(
        issue.rule_id == "LEGACY.TRANSPORT_EMISSION_FUEL_REFERENCE_UNAVAILABLE"
        for issue in obj.validation_issues
    )


@pytest.mark.parametrize("amount", [-1, float("inf"), float("nan")])
def test_invalid_fuel_remains_an_error(amount):
    ds = dataset([air("Nitrogen oxides", 1e-4)], "truck")
    ds["exchanges"][1]["amount"] = amount
    obj = validator(ds, "truck")
    run(obj, "truck")
    assert any(
        issue.rule_id == "LEGACY.INVALID_TRANSPORT_FUEL_INPUT"
        and issue.severity == "error"
        for issue in obj.validation_issues
    )


@pytest.mark.parametrize("vehicle", ["car", "truck"])
def test_update_does_not_normalize_pollutants(monkeypatch, vehicle):
    ds = dataset(
        [
            air("Methane, fossil", 1e-5),
            air("NMVOC, non-methane volatile organic compounds", 1e-3),
        ],
        vehicle,
    )
    before = deepcopy(ds)

    class StubTransport:
        def __init__(self, **kwargs):
            self.database = kwargs["database"]
            self.cache, self.index, self.vehicle_map = {}, {}, {}

        def regionalize_transport_datasets(self):
            pass

        def normalize_pollutant_emissions(self, *args):
            pytest.fail("Transport update still calls the automatic pollutant clamp")

    class StubValidator:
        def __init__(self, **kwargs):
            pass

        def run_checks(self):
            return None

    iam = SimpleNamespace(
        **{
            name: None
            for name in (
                "passenger_car_fleet",
                "road_freight_fleet",
                "bus_fleet",
                "rail_freight_fleet",
                "two_wheelers_fleet",
                "sea_freight_fleet",
            )
        },
        regions=["EUR"]
    )
    scenario = {
        "iam data": iam,
        "database": [ds],
        "year": 2020,
        "model": "remind",
        "pathway": "SSP2-NPi",
    }
    monkeypatch.setattr(transport, "Transport", StubTransport)
    monkeypatch.setattr(transport, "CarValidation", StubValidator)
    monkeypatch.setattr(transport, "TruckValidation", StubValidator)
    monkeypatch.setattr(transport, "get_scenario_inventory", lambda s: s["database"])
    monkeypatch.setattr(
        transport, "replace_scenario_inventory", lambda s, d: s.update(database=d)
    )
    monkeypatch.setattr(transport, "record_validation_phase", lambda *args: None)
    transport._update_vehicles(scenario, vehicle, "3.12", "cutoff")
    assert ds == before
