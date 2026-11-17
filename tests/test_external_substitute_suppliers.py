"""Supplier fallbacks return the list contract used by external relinking."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from premise.external import ExternalScenario


def supplier(location, production):
    return {
        "name": "new supplier",
        "reference product": "product",
        "location": location,
        "unit": "kilogram",
        "production volume": production,
        "exchanges": [],
    }


@pytest.mark.parametrize(
    "volumes,expected", [([1, 3], "B"), ([3, 1], "A"), ([3, 3], "A")]
)
def test_substitute_returns_highest_share_and_keeps_first_tie(volumes, expected):
    obj = object.__new__(ExternalScenario)
    obj.database = [
        supplier(location, volume) for location, volume in zip("AB", volumes)
    ]
    before = deepcopy(obj.database)

    selected = obj.find_best_substitute_suppliers("new supplier", "product", ["A", "B"])

    assert len(selected) == 1
    assert selected[0]["location"] == expected
    assert selected[0]["share"] == max(volumes) / sum(volumes)
    assert obj.database == before


def test_substitute_respects_name_product_and_region_filters():
    obj = object.__new__(ExternalScenario)
    obj.database = [supplier("A", 1), supplier("B", 100)]
    obj.database += [{**supplier("A", 1000), "name": "other"}]

    assert (
        obj.find_best_substitute_suppliers("new supplier", "product", ["A"])[0][
            "location"
        ]
        == "A"
    )
    assert obj.find_best_substitute_suppliers("absent", "product", ["A"]) == []
    assert obj.find_best_substitute_suppliers("new supplier", "other", ["A"]) == []
    assert (
        obj.find_best_substitute_suppliers("new supplier", "product", ["absent"]) == []
    )


@pytest.mark.parametrize("targeted", [False, True])
def test_external_relinking_uses_best_substitute_in_global_fallback(targeted):
    obj = object.__new__(ExternalScenario)
    old = {
        "name": "old supplier",
        "reference product": "old product",
        "unit": "kilogram",
        "location": "GLO",
        "exchanges": [{"type": "production", "amount": 1.0}],
    }
    consumer = {
        "name": "consumer",
        "reference product": "service",
        "unit": "kilogram",
        "location": "GLO",
        "exchanges": [
            {
                "name": "old supplier",
                "product": "old product",
                "unit": "kilogram",
                "location": "GLO",
                "type": "technosphere",
                "amount": 2.0,
                "uncertainty type": 0,
            }
        ],
    }
    obj.database = [old, consumer, supplier("A", 1), supplier("B", 3)]
    obj.geo = SimpleNamespace(ecoinvent_to_iam_location=lambda location: "outside")

    obj.relink_to_new_datasets(
        replaces=[{"name": "old supplier", "product": "old product"}],
        replaces_in=[{"name": "consumer"}] if targeted else None,
        new_name="new supplier",
        new_ref="product",
        ratio=1.0,
        regions=["A", "B"],
    )

    assert len(consumer["exchanges"]) == 1
    assert consumer["exchanges"][0]["location"] == "B"
    assert consumer["exchanges"][0]["amount"] == 2.0
    if not targeted:
        assert old["exchanges"][-1]["name"] == "new supplier"
        assert old["exchanges"][-1]["location"] == "B"
