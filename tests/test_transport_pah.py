"""Read-only transport checks keep aggregate hydrocarbons separate from PAH."""

from copy import deepcopy

import pytest

from premise.validation import TransportValidation, load_car_exhaust_pollutants


def air(name, amount, compartment="urban air close to ground"):
    return {
        "name": name,
        "type": "biosphere",
        "categories": ("air", compartment),
        "unit": "kilogram",
        "amount": amount,
    }


def dataset(exchanges, vehicle="car"):
    name = (
        "transport, passenger car, gasoline, Mini, EURO-6"
        if vehicle == "car"
        else "transport, freight, lorry, diesel, 40t gross weight, EURO-VI"
    )
    return {
        "name": name,
        "location": "EUR",
        "unit": "kilometer",
        "exchanges": [
            {"name": name, "type": "production", "amount": 1},
            {
                "name": (
                    "market for petrol, low-sulfur"
                    if vehicle == "car"
                    else "market for diesel, low-sulfur"
                ),
                "type": "technosphere",
                "unit": "kilogram",
                "amount": 0.05,
            },
            *exchanges,
        ],
    }


def check(ds, factors, vehicle="car"):
    validator = object.__new__(TransportValidation)
    validator.database, validator.regions = [ds], ["EUR"]
    validator.major_issues_log, validator.minor_issues_log = [], []
    validator.validation_issues = []
    if vehicle == "car":
        validator.exhaust = {"gasoline": {"6.2": factors}}
        prefix = "transport, passenger car"
    else:
        validator.exhaust = {"diesel": {"6": {"40t": factors}}}
        prefix = "transport, freight, lorry"
    validator.check_pollutant_emissions(prefix)


@pytest.mark.parametrize("vehicle", ["car", "truck"])
def test_hydrocarbon_factor_cannot_change_pah_or_other_species(vehicle):
    ds = dataset(
        [
            air("PAH, polycyclic aromatic hydrocarbons", 1e-9),
            air(
                "PAH, polycyclic aromatic hydrocarbons",
                2e-9,
                "non-urban air or from high stacks",
            ),
            air("Hydrocarbons, chlorinated", 3e-9),
            air("NMVOC, non-methane volatile organic compounds", 4e-6),
        ],
        vehicle,
    )
    before = deepcopy(ds)
    check(ds, {"Hydrocarbon": 0.002261953}, vehicle)
    assert ds == before
    validator = object.__new__(TransportValidation)
    assert validator.calculate_actual_emission(ds, "Hydrocarbon") == 0
    assert validator.calculate_actual_emission(ds, "PAH") == pytest.approx(3e-9)


@pytest.mark.parametrize("aggregate", ["Hydrocarbon", "Hydrocarbons", "HYDROCARBONS"])
def test_explicit_hydrocarbon_total_remains_separate_from_pah(aggregate):
    aggregate_exchanges = [air(aggregate, 1e-3), air(aggregate, 2e-3, "unspecified")]
    untouched = [
        air("PAH, polycyclic aromatic hydrocarbons", 1e-9),
        dict(air(aggregate, 0.5), categories=("water",)),
        dict(air(aggregate, 0.7), type="technosphere"),
        dict(air(aggregate, 0.8), categories=None),
    ]
    ds = dataset(aggregate_exchanges + untouched)
    before = deepcopy(untouched)
    check(ds, {"Hydrocarbon": 0.002261953})
    # The explicit aggregate is checked without replacing its mass.
    expected = 0.003
    assert sum(e["amount"] for e in aggregate_exchanges) == pytest.approx(expected)
    assert aggregate_exchanges[1]["amount"] / aggregate_exchanges[0][
        "amount"
    ] == pytest.approx(2)
    assert untouched == before
    validator = object.__new__(TransportValidation)
    assert validator.calculate_actual_emission(ds, "Hydrocarbon") == pytest.approx(
        expected
    )


def test_packaged_gasoline_factors_preserve_pah_and_co():
    pah = air("PAH, polycyclic aromatic hydrocarbons", 1.485159e-9)
    co = air("Carbon monoxide, fossil", 1e-2)
    ds = dataset([pah, co])
    factors = load_car_exhaust_pollutants()["gasoline"]["6.2"]
    check(ds, factors)
    assert pah["amount"] == 1.485159e-9
    assert co["amount"] == 1e-2
    before = deepcopy(ds)
    check(ds, factors)
    assert ds == before
