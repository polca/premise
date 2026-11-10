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


def test_district_delivery_reuses_complete_template_and_shared_regional_copying(
    heat, monkeypatch
):
    from types import SimpleNamespace

    from premise.energy.heat import SECONDARY_MARKET

    definition = heat.market_mapping["delivery"]
    source = {
        **definition["source"]["fltr"],
        "code": "delivery-source",
        "exchanges": [
            {
                "type": "production",
                "name": "source",
                "product": "source",
                "unit": "megajoule",
                "amount": 1.0,
            },
            {
                "type": "technosphere",
                "name": definition["heat_input"]["name"],
                "product": definition["heat_input"]["reference product"],
                "location": "CH",
                "unit": "megajoule",
                "amount": 1.08,
                "uncertainty type": 2,
                "scale": 0.15,
                "input": ("native", "plant"),
            },
            {
                "type": "technosphere",
                "name": "Transport, district heat, average",
                "product": "Transport, district heat, average",
                "location": "CH",
                "unit": "megajoule",
                "amount": 1.0,
                "input": ("native", "network"),
            },
        ],
    }
    original = copy.deepcopy(source)
    heat.mapping = SimpleNamespace(
        generate_map=lambda definitions: {"delivery": [source]}
    )
    indexed = set()
    monkeypatch.setattr(
        heat,
        "is_in_index",
        lambda candidate, location: (candidate["name"], location) in indexed,
    )
    monkeypatch.setattr(
        heat,
        "add_to_index",
        lambda dataset: indexed.add((dataset["name"], dataset["location"])),
    )
    monkeypatch.setattr(heat, "write_log", lambda dataset: None)

    def proxies(datasets, *, regions, relink, unlist):
        assert datasets == [source] and relink is False and unlist is False
        return {
            region: {
                **copy.deepcopy(source),
                "code": f"delivery-{region}",
                "location": region,
            }
            for region in regions
        }

    monkeypatch.setattr(heat, "fetch_proxies", proxies)
    material = consumer()
    material["exchanges"][0].update(
        name=source["name"], product=source["reference product"]
    )
    heat.database = [source, material]
    heat.diagnostics = {}
    with pytest.warns(UserWarning, match="native Swiss district-delivery"):
        heat.create_heat_delivery_activities()
    assert source == original
    copies = [row for row in heat.database if row.get("heat delivery provenance")]
    assert len(copies) == len(heat.regions)
    for row in copies:
        supplied = row["exchanges"][1]
        assert supplied["name"] == SECONDARY_MARKET["name"]
        assert supplied["location"] == row["location"]
        assert supplied["amount"] == 1.08
        assert supplied["uncertainty type"] == 2 and supplied["scale"] == 0.15
        assert "input" not in supplied
        assert row["exchanges"][2] == original["exchanges"][2]
        assert row["exchanges"][0]["amount"] == 1.0
    rewritten = material["exchanges"][0]
    assert rewritten["name"] == definition["target"]["name"]
    assert rewritten["location"] == "NEU"
    assert rewritten["amount"] == -2.5
    # A repeat reuses already indexed regional deliveries.
    with pytest.warns(UserWarning):
        heat.create_heat_delivery_activities()
    assert len(
        [row for row in heat.database if row.get("heat delivery provenance")]
    ) == len(copies)


def test_delivery_does_not_choose_an_ambiguous_template(heat):
    from types import SimpleNamespace

    heat.mapping = SimpleNamespace(
        generate_map=lambda definitions: {"delivery": [{}, {}]}
    )
    with pytest.raises(ValueError, match="one exact reviewed source"):
        heat.create_heat_delivery_activities()


def test_shared_proxy_copier_returns_datasets_when_upstream_relinking_is_disabled(
    heat, monkeypatch
):
    source = consumer(unit="megajoule")
    source["exchanges"].append(
        {
            "type": "production",
            "name": source["name"],
            "product": source["reference product"],
            "amount": 1.0,
            "unit": "megajoule",
        }
    )
    original = copy.deepcopy(source)
    heat.database = [source]
    monkeypatch.setattr(
        heat, "region_to_proxy_dataset_mapping", lambda **kwargs: {"NEU": source}
    )
    monkeypatch.setattr(heat, "is_in_index", lambda dataset, region: False)
    copies = heat.fetch_proxies([source], regions=["NEU"], relink=False, unlist=False)
    assert set(copies) == {"NEU"}
    assert copies["NEU"]["location"] == "NEU"
    assert copies["NEU"]["code"] != source["code"]
    assert copies["NEU"]["exchanges"][0] == source["exchanges"][0]
    assert source == original
