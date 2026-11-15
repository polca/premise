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
            "LEGACY.TRANSPORT_EMISSION_UNAVAILABLE",
            "warning",
            id="missing-positive-emission",
        ),
        pytest.param(
            0.0,
            1e-15,
            "LEGACY.TRANSPORT_EMISSION_UNAVAILABLE",
            "warning",
            id="missing-small-positive-emission",
        ),
        pytest.param(
            1e-15,
            0.0,
            "LEGACY.TRANSPORT_HOT_EMISSION_DIAGNOSTIC",
            "warning",
            id="unexpected-small-emission",
        ),
        pytest.param(
            3e-6,
            1e-6,
            "LEGACY.TRANSPORT_HOT_EMISSION_DIAGNOSTIC",
            "warning",
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


def ccs_electricity_dataset(air_co2, captured_co2, capture_unit="kilogram"):
    return {
        "name": "electricity production, natural gas, combined cycle power plant",
        "reference product": "electricity, high voltage",
        "unit": "kilowatt hour",
        "location": "RER",
        "exchanges": [
            {
                "name": "methane, from electrochemical methanation",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 0.14035087823867798,
            },
            {
                "name": "Carbon dioxide, non-fossil",
                "unit": "kilogram",
                "type": "biosphere",
                "categories": ("air",),
                "amount": air_co2,
            },
            {
                "name": "carbon dioxide, captured from synthetic gas power plant",
                "unit": capture_unit,
                "type": "technosphere",
                "amount": captured_co2,
            },
        ],
    }


@pytest.mark.parametrize("capture_fraction", [0.0, 0.9, 0.99, 1.0])
@pytest.mark.parametrize(
    "gross_factor,warn", [(1.033, False), (0.7, True), (1.3, True)]
)
def test_electricity_carbon_tolerance_uses_gross_balance(
    capture_fraction, gross_factor, warn
):
    expected = 0.14035087823867798 * 47.5 * 0.054
    gross = expected * gross_factor
    dataset = ccs_electricity_dataset(
        gross * (1 - capture_fraction), gross * capture_fraction
    )
    before = copy.deepcopy(dataset)
    validator = validator_for(ElectricityValidation, [dataset])

    validator.check_efficiency()

    assert dataset == before
    assert [issue.rule_id for issue in validator.validation_issues] == (
        ["LEGACY.CO2_EMISSIONS_POSSIBLY_INCORRECT"] if warn else []
    )
    if warn:
        message = validator.validation_issues[0].message
        assert "Gross CO2 (air + captured)" in message
        assert "Expected from fuel" in message


def test_reviewed_synthetic_gas_ccs_carbon_balance():
    dataset = ccs_electricity_dataset(0.03719298169016838, 0.3347368538379669)
    # A storage service and a captured-CO2 production exchange do not count as
    # captured-CO2 inputs at the power plant's combustion boundary.
    dataset["exchanges"].extend(
        [
            {
                "name": "carbon dioxide storage",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 0.3347368538379669,
            },
            {
                "name": "carbon dioxide, captured",
                "unit": "kilogram",
                "type": "production",
                "amount": 0.3347368538379669,
            },
        ]
    )
    before = copy.deepcopy(dataset)
    validator = validator_for(ElectricityValidation, [dataset])

    validator.check_efficiency()

    assert dataset == before
    assert validator.validation_issues == []


@pytest.mark.parametrize(
    "air,captured,unit",
    [
        pytest.param(0.37, -0.01, "kilogram", id="negative-capture"),
        pytest.param(0.36, float("nan"), "kilogram", id="nan-capture"),
        pytest.param(0.36, float("inf"), "kilogram", id="infinite-capture"),
        pytest.param(0.36, 0.001, "ton", id="unsupported-capture-unit"),
        pytest.param(0.36, 0.0, None, id="missing-capture-unit"),
        pytest.param(-0.01, 0.37, "kilogram", id="negative-air-balanced-gross"),
        pytest.param(0.0, 0.7, "kilogram", id="capture-exceeds-fuel-carbon"),
    ],
)
def test_invalid_ccs_amounts_remain_warnings(air, captured, unit):
    dataset = ccs_electricity_dataset(air, captured, unit)
    before = copy.deepcopy(dataset)
    validator = validator_for(ElectricityValidation, [dataset])

    validator.check_efficiency()

    assert dataset == before
    assert [issue.rule_id for issue in validator.validation_issues] == [
        "LEGACY.CO2_EMISSIONS_POSSIBLY_INCORRECT"
    ]
    if air >= 0 and captured != 0.7:
        assert "Invalid capture input" in validator.validation_issues[0].message


@pytest.mark.parametrize(
    "air,captured,warn",
    [
        pytest.param(
            -0.8009099811315536, 0.8454049825668335, False, id="reviewed-oil-wrapper"
        ),
        pytest.param(-0.845, 0.845, False, id="exact-upstream-credit"),
        pytest.param(0.1, 0.845, True, id="missing-upstream-credit"),
        pytest.param(-1.5, 0.845, True, id="excess-upstream-credit"),
        pytest.param(0.01, -0.01, True, id="negative-capture-balanced-adjustment"),
    ],
)
def test_ccs_upstream_generation_wrappers_preserve_credit_boundary(air, captured, warn):
    dataset = ccs_electricity_dataset(air, captured)
    dataset["exchanges"][0] = {
        "name": "electricity production, oil",
        "unit": "kilowatt hour",
        "type": "technosphere",
        "amount": 1.1,
    }
    before = copy.deepcopy(dataset)
    validator = validator_for(ElectricityValidation, [dataset])

    validator.check_efficiency()

    assert dataset == before
    assert [issue.rule_id for issue in validator.validation_issues] == (
        ["LEGACY.CO2_EMISSIONS_POSSIBLY_INCORRECT"] if warn else []
    )
    if warn:
        assert "capture-credit comparison" in validator.validation_issues[0].message
