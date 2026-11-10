"""Delivered-energy mixing, native delivery, source ownership and refusal checks."""

import copy
import json
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

import premise.energy.uvek_heat_markets as markets
from premise.energy.uvek_heat_distribution import (
    DELIVERY_NAME,
    REVIEW_PATH,
    SECONDARY_NAME,
)
from test_uvek_heat_distribution import edge, record

SECONDARY = "heat, secondary, from natural gas CHP"
BUILDING = "heat, buildings, from natural gas boiler"
INDUSTRIAL = "heat, industrial, from coal furnace"
PURCHASED_B = "heat, buildings, from district heating"
PURCHASED_I = "heat, industrial, from district heating"


def array(values):
    roles = list(values)
    data = np.array([[values[role] for role in roles]] * 2, dtype=float)
    return xr.DataArray(
        data,
        dims=("region", "variables"),
        coords={"region": ["EUR", "ASI"], "variables": roles},
        attrs={"unit": {role: "EJ/yr" for role in roles}},
    ).expand_dims(year=[2050])


@pytest.fixture
def context(monkeypatch):
    review = json.loads(REVIEW_PATH.read_text())
    heat = record(review["heat_input"], "heat")
    network = record(review["network_input"], "network")
    delivery = record(review["source"], "delivery")
    delivery["exchanges"] += [edge(heat, 1.2), edge(network, 0.7)]
    delivery["exchanges"][1].update(
        {"formula": "losses", "uncertainty type": 2, "loc": 0.182, "scale": 0.1}
    )
    delivery["exchanges"].append(
        {
            "name": "Retained direct burden",
            "type": "biosphere",
            "amount": -0.3,
            "unit": "kilogram",
            "input": ("bio", "flow"),
        }
    )
    database = [heat, network, delivery]

    def suppliers(data, roles, **kwargs):
        return {
            role: {
                "supplier": heat,
                "delivered_heat_factor": {BUILDING: 0.5, INDUSTRIAL: 0.8}.get(role, 1),
                "energy_basis": "heat_output" if role == SECONDARY else "final_energy",
                "limitations": ["synthetic complete supplier"],
                "review": "synthetic review",
            }
            for role in roles
        }

    monkeypatch.setattr(markets, "plan_native_heat_suppliers", suppliers)
    monkeypatch.setattr(
        markets,
        "plan_heat_supplements",
        lambda *args, **kwargs: {"suppliers": {}, "activities": []},
    )
    iam = SimpleNamespace(
        secondary_heat_supply=array({SECONDARY: 1}),
        buildings_heat_end_use=array({BUILDING: 2, PURCHASED_B: 1}),
        industrial_heat_end_use=array({INDUSTRIAL: 3, PURCHASED_I: 1}),
    )
    return database, iam


def plan(context, **kwargs):
    data, iam = context
    return markets.plan_native_heat_markets(
        data, iam, model="remind", pathway="test", year=2050, **kwargs
    )


def test_three_layers_use_delivered_energy_shares_once_and_leave_sources_unchanged(
    context,
):
    data, iam = context
    before = copy.deepcopy(data)
    arrays = copy.deepcopy(iam)
    result = plan(context)
    assert data == before
    for attribute in (
        "secondary_heat_supply",
        "buildings_heat_end_use",
        "industrial_heat_end_use",
    ):
        xr.testing.assert_identical(getattr(iam, attribute), getattr(arrays, attribute))
    assert result["diagnostics"]["market_counts"] == {
        "secondary_supply": 3,
        "buildings_end_use": 3,
        "industrial_end_use": 3,
    }
    assert len(result["activities"]) == 12 and len(result["deliveries"]) == 3
    for a in result["markets"]["buildings_end_use"]:
        if a["location"] == "World":
            continue
        assert a["premise heat"]["IAM_shares"] == {BUILDING: 0.5, PURCHASED_B: 0.5}
        assert a["premise heat"]["delivered_volumes_EJ_per_year"] == {
            BUILDING: 1,
            PURCHASED_B: 1,
        }
        assert [e["amount"] for e in a["exchanges"] if e["type"] == "technosphere"] == [
            0.5,
            0.5,
        ]
        assert a["exchanges"][2]["name"] == DELIVERY_NAME
    for a in result["markets"]["industrial_end_use"]:
        assert a["premise heat"]["IAM_shares"][INDUSTRIAL] == pytest.approx(2.4 / 3.4)
    for a in result["deliveries"]:
        assert a["exchanges"][1]["name"] == SECONDARY_NAME
        assert a["exchanges"][1]["amount"] == 1.2
        assert a["exchanges"][1]["formula"] == "losses"
        assert a["exchanges"][2:] == before[-1]["exchanges"][2:]


def test_planning_and_json_cache_replay_are_deterministic_without_duplicate_markets(
    context,
):
    data, iam = context
    first = plan(context)
    augmented = json.loads(json.dumps([*data, *first["activities"]]))
    before = copy.deepcopy(augmented)
    second = markets.plan_native_heat_markets(
        augmented, iam, model="remind", pathway="test", year=2050
    )
    assert second["activities"] == [] and augmented == before
    assert [a["code"] for a in second["markets"]["buildings_end_use"]] == [
        a["code"] for a in first["markets"]["buildings_end_use"]
    ]


@pytest.mark.parametrize("bad", [-1, np.nan, np.inf])
def test_invalid_IAM_quantities_refused_before_source_mutation(context, bad):
    data, iam = context
    iam.buildings_heat_end_use.loc[dict(region="EUR", variables=BUILDING)] = bad
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="Nonfinite/negative"):
        plan(context)
    assert data == before


def test_purchased_heat_cannot_bypass_missing_regional_delivery(context):
    data, iam = context
    iam.secondary_heat_supply = None
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="no regional secondary/delivery"):
        plan(context)
    assert data == before


def test_onsite_only_heat_does_not_invent_secondary_supply(context):
    _, iam = context
    iam.secondary_heat_supply = None
    iam.buildings_heat_end_use = array({BUILDING: 1})
    iam.industrial_heat_end_use = None
    result = plan(context)
    assert result["markets"]["secondary_supply"] == [] and result["deliveries"] == []
    assert len(result["activities"]) == 3


def test_secondary_only_IAM_does_not_invent_end_use_heat(context):
    _, iam = context
    iam.buildings_heat_end_use = iam.industrial_heat_end_use = None
    result = plan(context)
    assert (
        result["markets"]["buildings_end_use"]
        == result["markets"]["industrial_end_use"]
        == []
    )
    assert len(result["activities"]) == 6


def test_wrong_layer_and_unreviewed_residual_are_refused(context, monkeypatch):
    _, iam = context
    iam.buildings_heat_end_use = array({SECONDARY: 1})
    with pytest.raises(ValueError, match="matching reviewed layer"):
        plan(context)
    metadata = markets.load_heat_mapping(markets.HEAT_TECHS, "remind")
    metadata["heat, buildings, residual"] = {
        "layer": "buildings_end_use",
        "supplier_type": "frozen_legacy",
    }
    monkeypatch.setattr(markets, "load_heat_mapping", lambda *args: metadata)
    iam.buildings_heat_end_use = array({"heat, buildings, residual": 1})
    with pytest.raises(ValueError, match="frozen/residual"):
        plan(context)


def test_incompatible_IAM_units_and_background_are_refused(context):
    _, iam = context
    iam.buildings_heat_end_use.attrs["unit"] = {BUILDING: "TWh", PURCHASED_B: "EJ/yr"}
    with pytest.raises(ValueError, match="canonical EJ/yr"):
        plan(context)
    with pytest.raises(ValueError, match="UVEK2026"):
        plan(context, system_model="consequential")


def test_existing_unrelated_market_cannot_be_overwritten(context):
    data, _ = context
    result = plan(context)
    collision = copy.deepcopy(result["markets"]["buildings_end_use"][0])
    collision["exchanges"][1]["amount"] = 0.2
    data.append(collision)
    before = copy.deepcopy(data)
    with pytest.raises(ValueError, match="differs from this scenario"):
        plan(context)
    assert data == before


def test_world_fallback_uses_delivered_regional_volumes_without_counting_World_twice(
    context,
):
    _, iam = context
    iam.buildings_heat_end_use.loc[dict(region="ASI", variables=BUILDING)] = 6
    regional = iam.buildings_heat_end_use
    world = regional.sel(region="EUR").expand_dims(region=["World"]) * 1e6
    iam.buildings_heat_end_use = xr.concat([regional, world], dim="region")
    result = plan(context)
    market = next(
        a for a in result["markets"]["buildings_end_use"] if a["location"] == "World"
    )
    assert market["premise heat"]["regional_delivered_volume_shares"] == pytest.approx(
        {"EUR": 1 / 3, "ASI": 2 / 3}
    )
    assert market["premise heat"]["raw_IAM_volumes_EJ_per_year"] == {
        BUILDING: 8,
        PURCHASED_B: 2,
    }
    assert market["premise heat"]["IAM_shares"] == pytest.approx(
        {BUILDING: 2 / 3, PURCHASED_B: 1 / 3}
    )
    assert {e["location"] for e in market["exchanges"][1:]} == {"EUR", "ASI"}


def test_positive_World_only_heat_cannot_be_reported_as_an_unchanged_success(context):
    _, iam = context
    iam.buildings_heat_end_use = (
        array({BUILDING: 1}).sel(region="EUR").expand_dims(region=["World"])
    )
    with pytest.raises(ValueError, match="no regional volume basis"):
        plan(context)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1])
def test_world_row_is_validated_before_regional_aggregation(context, value):
    _, iam = context
    regional = iam.buildings_heat_end_use
    world = regional.sel(region="EUR").expand_dims(region=["World"]).copy(deep=True)
    world.loc[dict(region="World", variables=BUILDING)] = value
    iam.buildings_heat_end_use = xr.concat([regional, world], dim="region")
    with pytest.raises(ValueError, match="Nonfinite/negative IAM heat volume"):
        plan(context)
