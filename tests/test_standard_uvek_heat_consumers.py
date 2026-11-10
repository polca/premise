"""Native useful-heat consumers enter the shared heat relinker by exact identity."""

import copy
from pathlib import Path

import pytest
import yaml

from premise.energy.heat import INDUSTRIAL_MARKET, Heat


@pytest.fixture
def heat(monkeypatch):
    updater = object.__new__(Heat)
    updater.market_mapping = yaml.safe_load(
        (
            Path(__file__).parents[1]
            / "premise/iam_variables_mapping/heat_markets.yaml"
        ).read_text()
    )["uvek"]
    updater.created_dataset_codes = set()
    updater.regions = ["EUR", "NEU", "World"]
    updater.ecoinvent_to_iam_loc = {"CH": "NEU", "RER": "EUR"}
    monkeypatch.setattr(updater, "is_in_index", lambda candidate, location: True)
    return updater


def consumer(*, unit="kilogram", **metadata):
    return {
        "name": "material consumer",
        "reference product": "material",
        "location": "CH",
        "unit": unit,
        "code": "consumer",
        "exchanges": [
            {
                "name": "Heat, natural gas, at industrial furnace 1MW",
                "product": "Heat, natural gas, at industrial furnace 1MW",
                "location": "CH",
                "unit": "megajoule",
                "type": "technosphere",
                "input": ("source", "heat"),
                "amount": -2.5,
                "formula": "-heat_demand",
                "uncertainty type": 2,
                "loc": 0.9162907318741551,
                "scale": 0.2,
                "negative": True,
            }
        ],
        **metadata,
    }


def test_native_heat_purchase_uses_standard_market_without_changing_basis(heat):
    material = consumer()
    original = copy.deepcopy(material["exchanges"][0])
    heat.database = [material]
    aliases = heat._legacy_heat_inputs("industrial_end_use", [])
    heat.relink_heat_markets(aliases, INDUSTRIAL_MARKET)
    actual = material["exchanges"][0]
    assert actual["name"] == INDUSTRIAL_MARKET["name"]
    assert actual["product"] == INDUSTRIAL_MARKET["reference product"]
    assert actual["location"] == "NEU"
    assert "input" not in actual
    for key in ("amount", "formula", "uncertainty type", "loc", "scale", "negative"):
        assert actual[key] == original[key]


@pytest.mark.parametrize(
    "metadata",
    [
        {"unit": "megajoule"},
        {"unit": "kilowatt hour"},
        {"clic provenance": {"slug": "published-root"}},
        {"brightpath_conversion": {"recipe_id": "reviewed-helper"}},
    ],
)
def test_native_generation_and_converted_foregrounds_remain_complete(heat, metadata):
    material = consumer(**metadata)
    before = copy.deepcopy(material)
    heat.database = [material]
    heat.relink_heat_markets(
        heat._legacy_heat_inputs("industrial_end_use", []), INDUSTRIAL_MARKET
    )
    assert material == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("product", "unreviewed heat"),
        ("product", "Heat, light fuel oil, at industrial furnace 1MW"),
        ("unit", "kilogram"),
    ],
)
def test_similar_name_with_different_reference_basis_is_not_relinked(
    heat, field, value
):
    material = consumer()
    material["exchanges"][0][field] = value
    before = copy.deepcopy(material)
    heat.database = [material]
    heat.relink_heat_markets(
        heat._legacy_heat_inputs("industrial_end_use", []), INDUSTRIAL_MARKET
    )
    assert material == before


def test_declared_buildings_inputs_are_distinct_from_industrial_and_retained_services(
    heat,
):
    buildings = heat._legacy_heat_inputs("buildings_end_use", [])
    industrial = heat._legacy_heat_inputs("industrial_end_use", [])
    names = {row["name"] for row in [*buildings, *industrial]}
    assert "Heat, natural gas, at industrial furnace 1MW" in {
        row["name"] for row in industrial
    }
    assert "Heat, natural gas, at boiler condensing modulating 50kW" in {
        row["name"] for row in buildings
    }
    assert not names.intersection(
        {
            "Heat from waste, at municipal waste incineration plant",
            "Transport, district heat, average",
            "District heat, at consumer, natural gas in industrial furnace 1MW",
        }
    )
    heat.market_mapping = {}
    defaults = [{"name": "original ecoinvent market"}]
    assert heat._legacy_heat_inputs("buildings_end_use", defaults) is defaults


@pytest.mark.parametrize("formula", [None, "heat_demand"])
def test_relinking_retains_independent_uncertain_or_formula_bound_purchases(
    heat, formula
):
    material = consumer()
    first = material["exchanges"][0]
    first.pop("formula", None)
    if formula:
        first["formula"] = formula
        first["uncertainty type"] = 0
    second = copy.deepcopy(first)
    second["amount"] = -3.0
    second["loc"] = 1.0986122886681098
    second["scale"] = 0.4
    material["exchanges"].append(second)
    originals = copy.deepcopy(material["exchanges"])
    heat.database = [material]
    heat.relink_heat_markets(
        heat._legacy_heat_inputs("industrial_end_use", []), INDUSTRIAL_MARKET
    )
    assert len(material["exchanges"]) == 2
    for original, rewritten in zip(originals, material["exchanges"]):
        for key in (
            "amount",
            "loc",
            "scale",
            "uncertainty type",
            "negative",
            "formula",
        ):
            assert rewritten.get(key) == original.get(key)
    assert material["exchanges"][0]["name"] == INDUSTRIAL_MARKET["name"]
