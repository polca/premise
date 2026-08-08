import copy

import pytest

from premise.inventory_imports import (
    _resolve_versioned_replacement,
    check_for_datasets_compliance_with_consequential_database,
    get_consequential_blacklist,
)


def test_versioned_blacklist_replacement_location():
    replacement = {
        "name": "market for aluminium oxide, non-metallurgical",
        "reference product": "aluminium oxide, non-metallurgical",
        "location": {
            "<3.12": "IAI Area, EU27 & EFTA",
            ">=3.12": "IAI Area, Western and Central Europe",
        },
    }

    assert _resolve_versioned_replacement(replacement, "3.11")["location"] == (
        "IAI Area, EU27 & EFTA"
    )
    assert _resolve_versioned_replacement(replacement, "3.12")["location"] == (
        "IAI Area, Western and Central Europe"
    )


@pytest.mark.parametrize(
    ("version", "expected_location"),
    [
        ("3.11", "IAI Area, EU27 & EFTA"),
        ("3.12", "IAI Area, Western and Central Europe"),
    ],
)
def test_blacklist_applies_versioned_replacement_location(version, expected_location):
    datasets = [
        {
            "name": "test dataset",
            "reference product": "test product",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "treatment of aluminium scrap, new, at refiner",
                    "reference product": "aluminium oxide, non-metallurgical",
                    "unit": "kilogram",
                    "type": "technosphere",
                }
            ],
        }
    ]

    result = check_for_datasets_compliance_with_consequential_database(
        copy.deepcopy(datasets), get_consequential_blacklist(), version
    )

    assert result[0]["exchanges"][0]["location"] == expected_location
