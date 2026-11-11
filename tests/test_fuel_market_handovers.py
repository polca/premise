"""Market forwarding preserves signed demand and separate fossil components."""

from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pytest

from premise.core.transformation import BaseTransformation
from premise.fuels import markets
from premise.fuels.base import Fuels


def node(name, location, unit="megajoule", **fields):
    return {
        "name": name,
        "reference product": name,
        "location": location,
        "unit": unit,
        "exchanges": [
            {
                "name": name,
                "product": name,
                "location": location,
                "unit": unit,
                "amount": 1,
                "type": "production",
            }
        ],
        **fields,
    }


def buy(supplier, amount, **fields):
    return {
        "name": supplier["name"],
        "product": supplier["reference product"],
        "location": supplier["location"],
        "unit": supplier["unit"],
        "amount": amount,
        "type": "technosphere",
        **fields,
    }


def fixture(monkeypatch, location="CH"):
    fossil = node("illustrative gas supply", "NEU", regionalized=True)
    fossil["exchanges"].append(
        {"type": "biosphere", "name": "illustrative fossil emission", "amount": 1}
    )
    renewable = node("illustrative renewable gas", "NEU")
    regional = node("illustrative fuel market", "NEU", "cubic meter")
    regional["exchanges"].extend([buy(fossil, 18), buy(renewable, 18)])
    world = deepcopy(regional)
    world["location"] = "World"
    world["exchanges"][0]["location"] = "World"
    original = node(fossil["name"], location, emptied=True)
    original["exchanges"].append(buy(fossil, 1))
    consumer = node("illustrative consumer", location, "kilogram")
    consumer["exchanges"].append(
        buy(
            original,
            -72,
            **{
                "uncertainty type": 2,
                "loc": np.log(72),
                "scale": 0.2,
                "negative": True,
            },
        )
    )
    fuels = object.__new__(Fuels)
    BaseTransformation.__init__(
        fuels,
        database=[fossil, renewable, regional, world, original, consumer],
        iam_data=SimpleNamespace(regions=["NEU", "World"]),
        model="remind",
        pathway="SSP2-NPi",
        year=2050,
        version="2026",
        family="uvek",
        biosphere_version="3.10",
        system_model="cutoff",
    )
    fuels.write_log = lambda *args, **kwargs: None
    fuels.remove_from_index(original)
    entry = {
        "source": {
            "name": original["name"],
            "reference product": original["reference product"],
            "unit": "megajoule",
        },
        "target": {
            "name": regional["name"],
            "reference product": regional["reference product"],
            "unit": "cubic meter",
        },
        "conversion_factor": 1 / 36,
        "limitations": ["Illustrative basis only."],
    }
    monkeypatch.setattr(
        markets, "get_mapping", lambda *args, **kwargs: {"illustrative role": [entry]}
    )
    target = {
        "name": regional["name"],
        "reference_product": regional["reference product"],
        "unit": "cubic meter",
    }
    return fuels, original, consumer, fossil, entry, target


@pytest.mark.parametrize(
    "location,replace_original", [("CH", False), ("GLO", False), ("CH", True)]
)
def test_forwarder_converts_units_without_changing_signed_uncertainty_or_fossil_inventory(
    monkeypatch, location, replace_original
):
    fuels, original, consumer, fossil, entry, target = fixture(monkeypatch, location)
    if replace_original:
        original.pop("emptied")
        entry["replace_original_supply"] = True
    before_consumer, before_fossil = deepcopy(consumer), deepcopy(fossil)
    fuels._handover_fuel_market_aliases(target)
    fuels.relink_datasets()
    assert consumer == before_consumer
    assert fossil == before_fossil
    exchange = original["exchanges"][1]
    assert exchange["unit"] == "cubic meter"
    assert exchange["amount"] == pytest.approx(1 / 36)
    assert exchange["location"] == ("NEU" if location == "CH" else "World")
    assert fuels.is_in_index(original, location)
    # Independently solve the five-node toy graph: 72 MJ credit with a 50%
    # fossil blend must retain 36 MJ fossil credit, without a mixture self-loop.
    nodes = fuels.database
    keys = {
        (a["name"], a["reference product"], a["location"], a["unit"]): i
        for i, a in enumerate(nodes)
    }
    matrix = np.eye(len(nodes))
    for j, dataset in enumerate(nodes):
        for edge in dataset["exchanges"]:
            if edge["type"] == "technosphere":
                key = edge["name"], edge["product"], edge["location"], edge["unit"]
                matrix[keys[key], j] -= edge["amount"]
    demand = np.zeros(len(nodes))
    demand[nodes.index(consumer)] = 1
    supply = np.linalg.solve(matrix, demand)
    assert supply[nodes.index(fossil)] == pytest.approx(-36)


def test_unregionalized_complete_inventory_cannot_be_silently_emptied(monkeypatch):
    fuels, original, _, _, _, target = fixture(monkeypatch)
    original.pop("emptied")
    before = deepcopy(original)
    with pytest.raises(ValueError, match="unit-reference source forwarder"):
        fuels._handover_fuel_market_aliases(target)
    assert original == before


@pytest.mark.parametrize("factor", [0, -1, float("nan")])
def test_invalid_conversion_is_refused_before_inventory_mutation(monkeypatch, factor):
    fuels, original, _, _, entry, target = fixture(monkeypatch)
    entry["conversion_factor"] = factor
    before = deepcopy(original)
    with pytest.raises(ValueError, match="finite positive"):
        fuels._handover_fuel_market_aliases(target)
    assert original == before


def test_country_trade_network_stays_fossil_after_geographic_market_handover(
    monkeypatch,
):
    fuels, original, consumer, fossil, entry, target = fixture(monkeypatch)
    original.pop("emptied")
    other = node(original["name"], "DE")
    original["exchanges"] = [
        original["exchanges"][0],
        {"type": "biosphere", "name": "illustrative fossil emission", "amount": 1},
        buy(other, 0.5, **{"uncertainty type": 2, "loc": np.log(0.5), "scale": 0.2}),
    ]
    other["exchanges"].extend(
        [
            {"type": "biosphere", "name": "illustrative fossil emission", "amount": 2},
            buy(original, 0.25),
        ]
    )
    unused = node(original["name"], "FR")
    fuels.database.extend([other, unused])
    fuels.add_to_index([other, unused])
    for location in ["DE", "FR"]:
        fuels.ecoinvent_to_iam_loc[location] = fuels.geo.ecoinvent_to_iam_location(
            location
        )
    entry["preserve_supply_network"] = True
    fuels._preserve_fuel_supply_networks()
    baselines = [a for a in fuels.database if a.get("premise preserved fuel supply")]
    assert len(baselines) == 2
    baseline_names = {a["name"] for a in baselines}
    assert all(
        e["name"] in baseline_names
        for a in baselines
        for e in a["exchanges"]
        if e["type"] == "technosphere"
    )
    imported = next(
        e for a in baselines for e in a["exchanges"] if e.get("amount") == 0.5
    )
    assert imported["loc"] == pytest.approx(np.log(0.5))
    assert imported["scale"] == 0.2
    # The normal regional copy inherits the protected historical trade links.
    fossil["exchanges"] = [fossil["exchanges"][0], *deepcopy(original["exchanges"][1:])]
    fuels.empty_original_datasets(
        [original, other, unused], {"NEU": 1}, {"CH": "NEU", "DE": "NEU", "FR": "NEU"}
    )
    before = deepcopy(consumer)
    fuels._handover_fuel_market_aliases(target)
    assert consumer == before
    nodes = fuels.database
    keys = {
        (a["name"], a["reference product"], a["location"], a["unit"]): i
        for i, a in enumerate(nodes)
    }
    matrix = np.eye(len(nodes))
    emissions = np.zeros(len(nodes))
    for j, a in enumerate(nodes):
        for e in a["exchanges"]:
            if e["type"] == "technosphere":
                matrix[
                    keys[(e["name"], e["product"], e["location"], e["unit"])], j
                ] -= e["amount"]
            elif e["type"] == "biosphere":
                emissions[j] += e["amount"]
    demand = np.zeros(len(nodes))
    demand[nodes.index(consumer)] = 1
    result = emissions @ np.linalg.solve(matrix, demand)
    # Historical A = 1 + .5 B; B = 2 + .25 A. A's intensity is 2/.875.
    # A 72 MJ credit through a 50% fossil blend must credit 36 MJ of that A.
    assert result == pytest.approx(-36 * 2 / 0.875)
    size = len(nodes)
    fuels._preserve_fuel_supply_networks()
    assert len(fuels.database) == size


def test_missing_iam_array_does_not_preserve_or_regionalize_native_gas(monkeypatch):
    fuels, _, _, _, entry, _ = fixture(monkeypatch)
    entry.update(preserve_supply_network=True, requires_iam_array="natural_gas_blend")
    before = deepcopy(fuels.database)
    fuels._preserve_fuel_supply_networks()
    assert fuels.database == before
    fuels.fuel_map = {"natural gas": []}
    fuels.iam_data = SimpleNamespace(natural_gas_blend=None, production_volumes=None)

    def unexpected(**kwargs):
        raise AssertionError("Missing IAM gas data must not regionalize native supply")

    fuels.process_and_add_activities = unexpected
    fuels.generate_biogas_activities()
    assert fuels.database == before


def test_delivery_template_retains_kg_water_and_signed_waste_auxiliaries(monkeypatch):
    fuels, _, _, _, _, _ = fixture(monkeypatch)
    stock = node("illustrative delivered liquid", "CH", "kilogram")
    fuel = node("illustrative refinery product", "CH", "kilogram")
    water = node("illustrative water", "CH", "kilogram")
    waste = node("illustrative treatment", "CH", "kilogram")
    stock["exchanges"].extend(
        [
            buy(fuel, 1),
            buy(water, 0.2),
            buy(
                waste,
                -0.03,
                **{
                    "uncertainty type": 2,
                    "loc": np.log(0.03),
                    "scale": 0.1,
                    "negative": True,
                },
            ),
            {"type": "biosphere", "name": "illustrative emission", "amount": 0.01},
        ]
    )
    fuels.database.append(stock)
    template = {
        "source": {
            "name": stock["name"],
            "reference product": stock["reference product"],
            "unit": "kilogram",
        },
        "fuel_inputs": [fuel["name"]],
        "fallback_location": "CH",
    }
    actual = fuels._native_fuel_market_ancillaries("illustrative", template)
    assert [e["name"] for e in actual["CH"]] == [
        water["name"],
        waste["name"],
        "illustrative emission",
    ]
    assert actual["CH"][1]["amount"] == -0.03
    assert actual["CH"][1]["scale"] == 0.1
    assert actual["GLO"] == actual["CH"]
    # Another update restores the retained baseline, not the new fuel mix.
    fuels.__dict__.pop("_fuel_market_ancillaries")
    stock["emptied"] = True
    stock["exchanges"] = stock["exchanges"][:1]
    assert fuels._native_fuel_market_ancillaries("illustrative", template) == actual
