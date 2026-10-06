"""Validation accepts legitimate zeros and roundoff without hiding mismatches."""

import copy

import pytest

from premise.validation import (
    ElectricityValidation,
    TransportValidation,
    load_car_exhaust_pollutants,
)


def validator_for(validator_class, database):
    validator = object.__new__(validator_class)
    validator.database = database
    validator.major_issues_log = []
    validator.minor_issues_log = []
    validator.validation_issues = []
    return validator


@pytest.mark.parametrize(
    ("actual", "expected", "rule", "severity"),
    [
        pytest.param(0.0, 0.0, None, None, id="expected-zero"),
        pytest.param(-0.0, 0.0, None, None, id="signed-zero"),
        pytest.param(1e-6, 1e-6, None, None, id="matching-positive"),
        pytest.param(1.1e-6, 1e-6, None, None, id="within-existing-tolerance"),
        pytest.param(
            0.0,
            1e-6,
            "LEGACY.NO_EMISSION_FACTOR_FOR_LEAD",
            "warning",
            id="missing-positive-emission",
        ),
        pytest.param(
            0.0,
            1e-15,
            "LEGACY.NO_EMISSION_FACTOR_FOR_LEAD",
            "warning",
            id="missing-small-positive-emission",
        ),
        pytest.param(
            1e-15,
            0.0,
            "LEGACY.INCORRECT_EMISSION_FACTOR",
            "error",
            id="unexpected-small-emission",
        ),
        pytest.param(
            3e-6,
            1e-6,
            "LEGACY.INCORRECT_EMISSION_FACTOR",
            "error",
            id="incorrect-positive-emission",
        ),
    ],
)
def test_transport_emission_zero_reference(actual, expected, rule, severity):
    dataset = {
        "name": "transport, passenger car, gasoline, Medium, EURO-6",
        "reference product": "transport, passenger car, EURO-6",
        "location": "EUR",
        "exchanges": [],
    }
    before = copy.deepcopy(dataset)
    validator = validator_for(TransportValidation, [dataset])

    validator.validate_emissions(dataset, actual, expected, "Lead")

    assert dataset == before
    assert [
        (issue.rule_id, issue.severity) for issue in validator.validation_issues
    ] == ([] if rule is None else [(rule, severity)])


@pytest.mark.parametrize(
    "lead_exchange", [False, True], ids=["absent", "explicit-zero"]
)
def test_euro6_reference_accepts_zero_lead(lead_exchange):
    validator = validator_for(TransportValidation, [])
    dataset = {"exchanges": []}
    if lead_exchange:
        dataset["exchanges"].append(
            {"type": "biosphere", "name": "Lead", "categories": ("air",), "amount": 0.0}
        )
    reference = load_car_exhaust_pollutants()["gasoline"]["6.2"]["Lead"]
    assert reference == 0.0

    actual = validator.calculate_actual_emission(dataset, "Lead")
    validator.validate_emissions(dataset, actual, reference, "Lead")

    assert validator.validation_issues == []


@pytest.mark.parametrize(
    ("efficiency", "warn"),
    [
        pytest.param(0.25, False, id="exact-minimum"),
        pytest.param(0.24999999627470976, False, id="observed-float32-minimum"),
        pytest.param(0.65, False, id="exact-maximum"),
        pytest.param(0.65000004, False, id="float32-maximum"),
        pytest.param(0.4, False, id="interior"),
        pytest.param(0.249999, True, id="small-real-shortfall"),
        pytest.param(0.650001, True, id="small-real-excess"),
        pytest.param(0.15900000265359884, True, id="observed-low-efficiency"),
        pytest.param(0.75, True, id="high-efficiency"),
    ],
)
def test_electricity_efficiency_boundary_roundoff(efficiency, warn):
    fuel = 3.6 / (efficiency * 36)
    dataset = {
        "name": "electricity production, natural gas, conventional power plant",
        "reference product": "electricity, high voltage",
        "unit": "kilowatt hour",
        "location": "CN-JL",
        "exchanges": [
            {
                "name": "market for natural gas, high pressure",
                "unit": "cubic meter",
                "type": "technosphere",
                "amount": fuel,
            },
            {
                "name": "Carbon dioxide, fossil",
                "type": "biosphere",
                "categories": ("air",),
                "amount": fuel * 36 * 0.054,
            },
        ],
    }
    before = copy.deepcopy(dataset)
    validator = validator_for(ElectricityValidation, [dataset])

    validator.check_efficiency()

    assert dataset == before
    assert [issue.rule_id for issue in validator.validation_issues] == (
        ["LEGACY.ELECTRICITY_EFFICIENCY_POSSIBLY_INCORRECT"] if warn else []
    )

    # Accepting an efficiency boundary must not silence a separate CO2 mismatch.
    dataset["exchanges"][1]["amount"] *= 0.5
    validator = validator_for(ElectricityValidation, [dataset])
    validator.check_efficiency()
    assert any(
        issue.rule_id == "LEGACY.CO2_EMISSIONS_POSSIBLY_INCORRECT"
        for issue in validator.validation_issues
    )
