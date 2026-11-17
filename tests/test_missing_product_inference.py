"""Only a unique provider identity can supply an absent exchange product."""

from copy import deepcopy

import pytest

from premise.validation.validation import DatasetNormalizer


def normalize_integrity(providers, exchange):
    consumer = {"name": "consumer", "exchanges": [exchange], "unused": None}
    normalizer = object.__new__(DatasetNormalizer)
    normalizer.database = [*deepcopy(providers), consumer]
    normalizer.normalize_dataset_integrity()
    return normalizer.database[-1], consumer


@pytest.mark.parametrize(
    "products, expected",
    [
        ([], {}),
        (["heat"], {"product": "heat"}),
        ([None], {"product": None}),
        (["heat", "heat"], {}),
        (["heat", "steam"], {}),
    ],
)
def test_product_inference_requires_one_provider_even_when_products_agree(
    products, expected
):
    providers = [
        {"name": "boiler", "location": "RER", "reference product": product}
        for product in products
    ]
    exchange = {"type": "technosphere", "name": "boiler", "location": "RER"}

    result, original = normalize_integrity(providers, exchange)

    assert result["exchanges"][0] == {
        "type": "technosphere",
        "name": "boiler",
        "location": "RER",
        **expected,
    }
    assert result is not original
    assert result["exchanges"] is original["exchanges"]
    assert "unused" not in result


@pytest.mark.parametrize(
    "identity", [("boiler", "RER"), (None, None), (["boiler"], ["RER"])]
)
def test_matching_uses_both_name_and_location_including_unhashable_values(identity):
    name, location = identity
    providers = [
        {"name": name, "location": location, "reference product": "heat"},
        {"name": "other boiler", "location": location, "reference product": "other"},
        {"name": name, "location": "elsewhere", "reference product": "other"},
    ]
    result, _ = normalize_integrity(
        providers,
        {"type": "technosphere", "name": name, "location": location, "product": None},
    )

    assert result["exchanges"][0]["product"] == "heat"


@pytest.mark.parametrize(
    "exchange",
    [
        {"type": "biosphere", "name": "boiler", "location": "RER"},
        {"type": "production", "name": "boiler", "location": "RER"},
        {
            "type": "technosphere",
            "name": "boiler",
            "location": "RER",
            "product": "existing",
        },
        {"type": "technosphere", "name": ["boiler"], "location": "RER"},
    ],
)
def test_other_exchange_types_existing_products_and_unmatched_values_are_unchanged(
    exchange,
):
    expected = deepcopy(exchange)
    providers = [{"name": "boiler", "location": "RER", "reference product": "heat"}]

    result, _ = normalize_integrity(providers, exchange)

    assert result["exchanges"][0] == expected
