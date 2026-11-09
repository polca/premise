"""Native district losses, network inventory, source ownership and signed fields."""

import copy
import json

import pytest

from premise.energy.uvek_heat_distribution import (
    REVIEW_PATH,
    SECONDARY_NAME,
    SECONDARY_PRODUCT,
    build_native_district_delivery,
)


def record(identity, code):
    a = {**identity, "database": "native", "code": code}
    a["exchanges"] = [edge(a, 1, "production")]
    return a


def edge(a, amount, kind="technosphere"):
    return {
        "name": a["name"],
        "product": a["reference product"],
        "location": a["location"],
        "unit": a["unit"],
        "type": kind,
        "amount": amount,
        "input": (a["database"], a["code"]),
    }


@pytest.fixture
def graph():
    review = json.loads(REVIEW_PATH.read_text())
    heat = record(review["heat_input"], "heat")
    network = record(review["network_input"], "network")
    source = record(review["source"], "delivery")
    # Synthetic coefficients deliberately differ from the licensed prototype.
    source["exchanges"] += [edge(heat, 1.2), edge(network, 0.7)]
    source["exchanges"][1].update(
        {"uncertainty type": 2, "loc": 0.182, "scale": 0.1, "formula": "losses"}
    )
    source["exchanges"].append(
        {
            "name": "Retained signed direct flow",
            "amount": -0.1,
            "type": "biosphere",
            "unit": "kilogram",
            "input": ("bio310", "flow"),
        }
    )
    target = record(
        {
            "name": SECONDARY_NAME,
            "reference product": SECONDARY_PRODUCT,
            "location": "EUR",
            "unit": "megajoule",
        },
        "IAM-secondary",
    )
    target["database"] = "scenario"
    target["exchanges"][0]["input"] = (target["database"], target["code"])
    target["exchanges"].append(edge(heat, 1))
    target["premise heat"] = {
        "kind": "uvek_iam_market",
        "layer": "secondary_supply",
        "scenario": {"model": "remind", "pathway": "test", "year": 2050},
    }
    return [heat, network, source], target


def build(graph, **kwargs):
    data, target = graph
    return build_native_district_delivery(
        data, [target], model="remind", pathway="test", year=2050, **kwargs
    )[0]


def test_preserve_actual_loss_coefficient_and_full_network_inventory(graph):
    before = copy.deepcopy(graph)
    result = build(graph)
    data, target = graph
    assert graph == before
    heat = result["exchanges"][1]
    assert heat["amount"] == 1.2
    assert heat["input"] == (target["database"], target["code"])
    for field in ("amount", "uncertainty type", "loc", "scale", "formula"):
        assert heat[field] == data[-1]["exchanges"][1][field]
    assert result["exchanges"][2:] == data[-1]["exchanges"][2:]
    audit = result["premise heat"]["substitution"]
    assert audit["source_exchange"] == data[-1]["exchanges"][1]
    assert audit["target_amount"] == 1.2
    assert result["premise heat"]["limitations"]
    assert result["exchanges"][0]["input"] == (result["database"], result["code"])
    assert build(graph) == result


def test_other_scenario_market_refused(graph):
    graph[1]["premise heat"]["scenario"]["year"] = 2060
    with pytest.raises(ValueError, match="scenario's owned"):
        build(graph)


def test_foreground_cannot_impersonate_native_network(graph):
    graph[0][-1]["clic provenance"] = {"slug": "foreground"}
    with pytest.raises(ValueError, match="impersonate"):
        build(graph)


@pytest.mark.parametrize("amount", [-1, 0, float("nan"), float("inf"), True])
def test_invalid_source_heat_coefficient_refused(graph, amount):
    graph[0][-1]["exchanges"][1]["amount"] = amount
    with pytest.raises(ValueError, match="inputs must be positive"):
        build(graph)


def test_unreviewed_network_boundary_refused(graph):
    graph[0][-1]["exchanges"].pop(2)
    with pytest.raises(ValueError, match="boundary disagrees"):
        build(graph)


def test_duplicate_prototype_refused(graph):
    graph[0].append(copy.deepcopy(graph[0][-1]))
    with pytest.raises(ValueError, match="ambiguous"):
        build(graph)


@pytest.mark.parametrize("part", [0, 1])
def test_nonunit_source_or_secondary_reference_refused(graph, part):
    owner = graph[0][-1] if part == 0 else graph[1]
    owner["exchanges"][0]["amount"] = 1.1
    with pytest.raises(ValueError, match="reference"):
        build(graph)


@pytest.mark.parametrize("amount", [0, -1, 0.9, float("nan")])
def test_invalid_secondary_mix_refused(graph, amount):
    graph[1]["exchanges"][-1]["amount"] = amount
    with pytest.raises(ValueError, match="positive unit supplier mix"):
        build(graph)


@pytest.mark.parametrize(
    "context",
    [
        {"version": "3.9"},
        {"system_model": "consequential"},
        {"biosphere_version": "2026"},
    ],
)
def test_wrong_background_context_refused(graph, context):
    with pytest.raises(ValueError, match="UVEK2026cutoff"):
        build(graph, **context)


def test_duplicate_region_refused_without_appending_any_plan(graph):
    data, target = graph
    before = copy.deepcopy(graph)
    with pytest.raises(ValueError, match="Duplicate regional"):
        build_native_district_delivery(
            data, [target, target], model="remind", pathway="test", year=2050
        )
    assert graph == before
