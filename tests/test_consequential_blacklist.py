import copy

import pytest

from premise.inventory_imports import (
    _resolve_versioned_replacement,
    check_for_datasets_compliance_with_consequential_database,
    get_consequential_blacklist,
)


@pytest.mark.parametrize(
    ("activity", "product", "other_product", "target_location"),
    [
        (
            "soda production, solvay process",
            "calcium chloride",
            "soda ash, light",
            "RER",
        ),
        (
            "tissue paper production, recycled",
            "tissue paper",
            "waste paper, sorted",
            "GLO",
        ),
        (
            "tissue paper production",
            "tissue paper",
            "waste paper, sorted",
            "GLO",
        ),
    ],
)
def test_coproduct_market_mapping_preserves_other_product(
    activity, product, other_product, target_location
):
    original = {
        "name": activity,
        "reference product": product,
        "product": product,
        "unit": "kilogram",
        "location": "RER",
        "type": "technosphere",
        "amount": 2.5,
        "uncertainty type": 2,
        "loc": 0.9162907318741551,
        "scale": 0.1,
    }
    other_exchange = {
        **original,
        "reference product": other_product,
        "product": other_product,
    }
    datasets = [
        {
            "name": "consumer",
            "reference product": "product",
            "unit": "kilogram",
            "exchanges": [copy.deepcopy(original), copy.deepcopy(other_exchange)],
        }
    ]
    result = check_for_datasets_compliance_with_consequential_database(
        datasets, get_consequential_blacklist(), "3.11"
    )
    assert result[0]["exchanges"] == [
        {**original, "name": f"market for {product}", "location": target_location},
        other_exchange,
    ]


@pytest.mark.parametrize(
    ("energy", "unit"), [("electricity", "kilowatt hour"), ("heat", "megajoule")]
)
def test_incineration_energy_mapping_preserves_waste_treatment(energy, unit):
    product = f"{energy}, for reuse in municipal waste incineration only"
    electricity = {
        "name": "treatment of municipal solid waste, municipal incineration FAE",
        "reference product": product,
        "product": product,
        "unit": unit,
        "location": "CH",
        "type": "technosphere",
        "amount": 2.5,
    }
    waste = {
        **electricity,
        "reference product": "municipal solid waste",
        "product": "municipal solid waste",
        "unit": "kilogram",
        "amount": -1.0,
    }
    datasets = [
        {
            "name": "consumer",
            "reference product": "product",
            "unit": "kilogram",
            "exchanges": [copy.deepcopy(electricity), copy.deepcopy(waste)],
        }
    ]
    result = check_for_datasets_compliance_with_consequential_database(
        datasets, get_consequential_blacklist(), "3.11"
    )
    assert result[0]["exchanges"] == [
        {**electricity, "name": f"market for {product}"},
        waste,
    ]


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


@pytest.mark.parametrize(
    ("technology", "source_location", "regional_location"),
    [("average production", "RER", "RER"), ("mercury cell", "GLO", "RoW")],
)
@pytest.mark.parametrize(
    ("version", "location"),
    [("3.8", "GLO"), ("3.9.1", "GLO"), ("3.10", None), ("3.11", None)],
)
def test_chlor_alkali_sodium_hydroxide_mapping(
    version, location, technology, source_location, regional_location
):
    location = location or regional_location
    product = "sodium hydroxide, without water, in 50% solution state"
    original = {
        "name": f"chlor-alkali electrolysis, {technology}",
        "reference product": product,
        "product": product,
        "unit": "kilogram",
        "location": source_location,
        "type": "technosphere",
        "amount": 2.5,
        "uncertainty type": 2,
        "loc": 0.9162907318741551,
        "scale": 0.1,
    }
    chlorine = {
        **original,
        "reference product": "chlorine, gaseous",
        "product": "chlorine, gaseous",
    }
    datasets = [
        {
            "name": "consumer",
            "reference product": "product",
            "unit": "kilogram",
            "exchanges": [copy.deepcopy(original), copy.deepcopy(chlorine)],
        }
    ]
    result = check_for_datasets_compliance_with_consequential_database(
        datasets, get_consequential_blacklist(), version
    )
    assert result[0]["exchanges"] == [
        {**original, "name": f"market for {product}", "location": location},
        chlorine,
    ]
