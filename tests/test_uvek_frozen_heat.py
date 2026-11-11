"""Frozen source selection retains the shared heat algorithm and source vectors."""

import copy
import json
import math

import pytest

from premise.core.filesystem_constants import DATA_DIR
from premise.energy.heat import Heat


def frozen_heat(source, *, declared=True):
    heat = object.__new__(Heat)
    heat.database = [source]
    heat.frozen_suppliers = {}
    heat.created_dataset_codes = set()
    heat.regions = ["EUR", "USA", "World"]
    heat.iam_to_ecoinvent_loc = {"EUR": ["CH"], "USA": ["US"]}
    heat.diagnostics = {}
    heat.add_to_index = lambda dataset: None
    heat.market_mapping = (
        {
            "frozen_legacy_sources": {
                "secondary_supply": {
                    "name": "reviewed native mix",
                    "reference product": "native heat",
                    "limitations": [
                        "Historical composition is a geographic screening proxy."
                    ],
                }
            }
        }
        if declared
        else {}
    )
    return heat


def supplier(name, product):
    return {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "database": "test",
        "code": "source",
        "unit": "megajoule",
        "exchanges": [
            {"type": "production", "amount": 1, "name": name, "product": product},
            {
                "type": "technosphere",
                "amount": 0.6,
                "name": "fixed producer",
                "product": "heat",
                "unit": "megajoule",
                "location": "CH",
                "input": ("test", "producer"),
                "uncertainty type": 2,
                "loc": math.log(0.6),
                "scale": 0.1,
            },
        ],
    }


def test_declared_source_is_copied_without_relinking_or_mutating_its_inputs():
    source = supplier("reviewed native mix", "native heat")
    before = copy.deepcopy(source)
    heat = frozen_heat(source)
    with pytest.warns(UserWarning, match="geographic screening proxy"):
        result = heat.create_frozen_legacy_suppliers("secondary_supply")
    assert len(result) == 2 and len(heat.database) == 3
    assert source == before
    assert {a["location"] for a in result} == {"EUR", "USA"}
    assert all(a["heat frozen proxy"] for a in result)
    assert all(a["exchanges"][1] == before["exchanges"][1] for a in result)
    assert all(a["reference product"] == "heat, district or industrial" for a in result)
    assert heat.create_frozen_legacy_suppliers("secondary_supply") is result
    assert len(heat.database) == 3


def test_default_ecoinvent_frozen_source_selection_is_unchanged():
    source = supplier(
        "market for heat, district or industrial, other than natural gas",
        "heat, district or industrial, other than natural gas",
    )
    heat = frozen_heat(source, declared=False)
    result = heat.create_frozen_legacy_suppliers("secondary_supply")
    assert len(result) == 2
    assert result[0]["exchanges"][1] == source["exchanges"][1]


def test_missing_declared_source_does_not_fall_back_to_another_heat_boundary():
    source = supplier(
        "market for heat, district or industrial, other than natural gas",
        "heat, district or industrial, other than natural gas",
    )
    heat = frozen_heat(source)
    with pytest.warns(UserWarning):
        with pytest.raises(ValueError, match="reviewed native mix"):
            heat.create_frozen_legacy_suppliers("secondary_supply")


def test_native_resource_uses_only_normalized_generation_before_delivery():
    resource = json.loads(
        (DATA_DIR / "heat/uvek2026_frozen_heat_inventory.json").read_text()
    )
    root = resource["activities"][-1]
    producers = {a["code"]: a for a in resource["activities"][:-1]}
    edges = [e for e in root["exchanges"] if e["type"] == "technosphere"]
    assert len(producers) == len(edges) == 8
    assert sum(e["amount"] for e in edges) == pytest.approx(1)
    total = root["premise heat"]["native_heat_input_total_MJ"]
    assert total == pytest.approx(1.10978)
    source_weights = {
        r["selector"]["name"]: r["amount"]
        for r in resource["supplier_checks"][0]["exchanges"]
        if r["selector"]["type"] == "technosphere"
    }
    for e in edges:
        producer = producers[e["input"][1]]
        original = producer["premise heat"]["source_identity"]["name"]
        assert e["amount"] == pytest.approx(source_weights[original] / total)
        assert e["loc"] == pytest.approx(math.log(e["amount"]))
        assert producer["name"].startswith("heat supply, frozen legacy mix,")
        assert producer["heat frozen proxy"] is True
    assert "Transport, district heat, average" in source_weights
    assert not any(e["name"] == "Transport, district heat, average" for e in edges)
    (waste,) = [
        a
        for a in producers.values()
        if a["premise heat"]["source_identity"]["name"]
        == "Heat from waste, at municipal waste incineration plant"
    ]
    assert all(e["type"] == "production" for e in waste["exchanges"])
