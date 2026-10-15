"""Future BEV efficiency screens must preserve valid source trajectories."""

from copy import deepcopy
import pytest
from premise.validation import CarValidation


def vehicle(energy):
    return {
        "name": "transport, passenger car, battery electric, Mini",
        "reference product": "transport, passenger car",
        "location": "OAS",
        "unit": "kilometer",
        "exchanges": [
            {"type": "production", "amount": 1, "unit": "kilometer"},
            {
                "name": "market for electricity, low voltage",
                "product": "electricity, low voltage",
                "type": "technosphere",
                "unit": "kilowatt hour",
                "amount": energy,
            },
        ],
    }


def validate(ds):
    validator = object.__new__(CarValidation)
    validator.model, validator.scenario, validator.year = "remind", "test", 2045
    validator.regions, validator.database = ["OAS"], [ds]
    validator.major_issues_log, validator.minor_issues_log = [], []
    validator.validation_issues = []
    validator.check_vehicle_efficiency("transport, passenger car", elec_minimum=0.1)
    return validator


@pytest.mark.parametrize("energy", [0.09791549911678693, 0.4])
def test_beyond_heuristic_is_warning_without_inventory_changes(energy):
    ds = vehicle(energy)
    before = deepcopy(ds)
    validator = validate(ds)
    assert ds == before
    assert [(i.rule_id, i.severity) for i in validator.validation_issues] == [
        ("LEGACY.CAR_ELECTRICITY_PLAUSIBILITY", "warning")
    ]
    validator._finalize_logs()


@pytest.mark.parametrize("energy", [0, -0.1, float("nan"), float("inf")])
def test_invalid_purchased_energy_remains_an_error(energy):
    assert any(
        i.severity == "error" for i in validate(vehicle(energy)).validation_issues
    )


def test_wrong_units_and_missing_electricity_remain_errors():
    ds = vehicle(0.18)
    ds["exchanges"][1]["unit"] = "megajoule"
    assert any(i.severity == "error" for i in validate(ds).validation_issues)
    ds["exchanges"].pop()
    assert any(i.severity == "error" for i in validate(ds).validation_issues)


def test_reference_production_sets_per_kilometer_energy():
    ds = vehicle(0.36)
    ds["exchanges"][0]["amount"] = 2
    assert not validate(ds).validation_issues


def test_explicit_vehicle_kilometer_alias_is_supported():
    ds = vehicle(0.18)
    ds["unit"] = ds["exchanges"][0]["unit"] = "vehicle-kilometer"
    assert not validate(ds).validation_issues
