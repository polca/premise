"""Native capitalized heat names must receive the existing physical checks."""

import pytest

from premise.validation.validation import HeatValidation


@pytest.mark.parametrize(
    "name,amount,unit,expected_issue",
    [
        ("Heat, at industrial boiler", 0.25, "megajoule", True),
        ("heat production, industrial boiler", 0.25, "megajoule", True),
        ("Heat, at industrial boiler", 1.05, "megajoule", False),
        ("Heat, at HEAT PUMP", 0.05, "kilowatt hour", False),
        ("Heat, at cogen, allocation exergy", 0.398, "megajoule", False),
    ],
)
def test_native_heat_names_enter_physical_validation_with_existing_boundaries(
    name, amount, unit, expected_issue
):
    validator = object.__new__(HeatValidation)
    validator.regions = ["EUR"]
    validator.database = [
        {
            "name": name,
            "unit": "megajoule",
            "location": "EUR",
            "exchanges": [
                {
                    "name": (
                        "electricity" if unit == "kilowatt hour" else "fuel combustion"
                    ),
                    "product": "fuel",
                    "type": "technosphere",
                    "amount": amount,
                    "unit": unit,
                }
            ],
        }
    ]
    issues = []
    validator.log_issue = lambda dataset, kind, message, **kwargs: issues.append(kind)
    validator.check_heat_conversion_efficiency()
    assert ("heat conversion efficiency" in issues) is expected_issue
