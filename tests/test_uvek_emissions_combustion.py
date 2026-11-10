"""Caller-specific GAINS variants preserve shared inventories and physical links."""

import copy
import json
import math
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

import premise.emissions.uvek_combustion as combustion
from premise.emissions.uvek_combustion import (
    CALLER,
    VARIANT,
    plan_native_combustion_variants,
)
from premise.emissions.uvek_factors import plan_native_gains_reductions
from uvek_inventory_fixtures import edge, record


def ident(record):
    return {k: record[k] for k in ("name", "reference product", "location", "unit")}


@pytest.fixture
def case():
    def activity(name, unit="megajoule"):
        return record(
            {"name": name, "reference product": name, "location": "CH", "unit": unit},
            name,
        )

    fuel = activity("Upstream fuel", "kilogram")
    source = activity("Shared combustion")
    source["exchanges"].append(edge(fuel, 0.03))
    source["exchanges"] += [
        {
            "name": "Nitrogen oxides",
            "amount": 10.0,
            "unit": "kilogram",
            "type": "biosphere",
            "input": ("bio310", "nox"),
            "categories": ("air", "urban air close to ground"),
            "uncertainty type": 2,
            "loc": math.log(10),
            "scale": 0.3,
            "formula": "original_NOx",
        },
        {
            "name": "Sulfur dioxide",
            "amount": -4.0,
            "unit": "kilogram",
            "type": "biosphere",
            "input": ("bio310", "so2"),
            "categories": ("air",),
            "uncertainty type": 3,
            "loc": -4.0,
            "scale": 2.0,
            "minimum": -10.0,
            "maximum": -1.0,
        },
        {
            "name": "Methane, fossil",
            "amount": 2.0,
            "unit": "kilogram",
            "type": "biosphere",
            "input": ("bio310", "water"),
            "categories": ("water",),
        },
        {
            "name": "Particulate Matter, < 2.5 um",
            "amount": 3.0,
            "unit": "kilogram",
            "type": "biosphere",
            "input": ("bio310", "pm"),
            "categories": ("air",),
        },
    ]
    power, heat = activity("Power", "kilowatt hour"), activity("Heat")
    for caller, amount in [(power, 2.0), (heat, -1.0)]:
        caller["exchanges"].append(edge(source, amount))
        caller["exchanges"][-1].update(
            formula="signed_fuel",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.1},
        )
    unreviewed = activity("Unreviewed use")
    unreviewed["exchanges"].append(edge(source, 0.2))
    data = [fuel, source, power, heat, unreviewed]
    gains = xr.DataArray(
        np.array(
            [
                [
                    [[100.0, 100.0, 100.0], [50.0, 50.0, 50.0]],
                    [[100.0, 100.0, 100.0], [20.0, 20.0, 20.0]],
                ]
            ]
        ),
        dims=("region", "sector", "year", "pollutant"),
        coords={
            "region": ["EUR"],
            "sector": ["power", "heat"],
            "year": [2020, 2050],
            "pollutant": ["NOx", "SO2", "CH4"],
        },
    )
    factors = plan_native_gains_reductions(gains, 2050)
    bindings = [
        {
            "caller": ident(caller),
            "supplier": ident(source),
            "sector": sector,
            "region": "EUR",
        }
        for caller, sector in [(power, "power"), (heat, "heat")]
    ]
    return (
        data,
        bindings,
        factors,
        {"model": "remind", "pathway": "test", "year": 2050, "gains_scenario": "CLE"},
    )


def plan(case):
    return plan_native_combustion_variants(*case, biosphere_name="bio310")


def materialize(case, result):
    replacements = {
        tuple(r["source_key"]): r["activity"] for r in result["replacements"]
    }
    return [replacements.get((a["database"], a["code"]), a) for a in case[0]] + result[
        "activities"
    ]


def test_separate_sector_factors_preserve_negative_inputs_and_original_source(case):
    before = copy.deepcopy(case[0])
    result = plan(case)
    assert case[0] == before
    assert len(result["activities"]) == len(result["replacements"]) == 2
    for variant in result["activities"]:
        factor = 0.5 if variant[VARIANT]["sector"] == "power" else 0.2
        assert variant["exchanges"][2]["amount"] == 10 * factor
        assert variant["exchanges"][2]["loc"] == pytest.approx(
            math.log(10) + math.log(factor)
        )
        assert variant["exchanges"][2]["scale"] == 0.3
        assert variant["exchanges"][3]["amount"] == -4 * factor
        assert variant["exchanges"][3]["scale"] == 2 * factor
        assert variant["exchanges"][3]["minimum"] == -10 * factor
        assert variant["exchanges"][3]["maximum"] == -factor
        assert variant["exchanges"][1]["amount"] == 0.03
        assert variant["exchanges"][4]["amount"] == 2
        assert variant["exchanges"][5]["amount"] == 3
        assert variant["exchanges"][0]["amount"] == 1
    for row in result["replacements"]:
        caller = row["activity"]
        old = next(a for a in before if a["code"] == caller["code"])
        old_edge, new_edge = old["exchanges"][1], caller["exchanges"][1]
        for field in ["amount", "unit", "formula", "loc", "scale", "uncertainty type"]:
            assert new_edge[field] == old_edge[field]
        assert caller[CALLER][0]["source_exchange"] == old_edge
    assert materialize(case, result)[4] == before[4]


def test_json_replay_is_idempotent_and_rejects_a_changed_owned_variant(case):
    result = plan(case)
    data = json.loads(json.dumps(materialize(case, result)))
    replay = plan((data, *case[1:]))
    assert replay["activities"] == replay["replacements"] == []
    assert replay["diagnostics"]["already_applied"] == 2
    data[-1]["exchanges"][2]["amount"] *= 2
    with pytest.raises(ValueError, match="Existing combustion variant"):
        plan((data, *case[1:]))


@pytest.mark.parametrize(
    "failure",
    [
        "duplicate",
        "wrong_link",
        "missing_input",
        "source_foreground",
        "wrong_reference",
        "wrong_biosphere",
    ],
)
def test_invalid_binding_refused_without_partial_mutation(case, failure):
    data, bindings, *_ = case
    if failure == "duplicate":
        bindings.append(copy.deepcopy(bindings[0]))
    elif failure == "wrong_link":
        data[2]["exchanges"][1]["input"] = ("native", "wrong")
    elif failure == "missing_input":
        data[2]["exchanges"].pop()
    elif failure == "source_foreground":
        data[1]["clic provenance"] = {"slug": "wrong"}
    elif failure == "wrong_reference":
        data[1]["exchanges"][0]["amount"] = 2
    elif failure == "wrong_biosphere":
        data[1]["exchanges"][2]["input"] = ("wrong", "nox")
    before = copy.deepcopy(data)
    with pytest.raises(ValueError):
        plan(case)
    assert data == before


def test_mismatched_scenario_and_missing_GAINS_axis_are_refused(case):
    case[2]["diagnostics"]["selected_year"] = 2040
    with pytest.raises(ValueError, match="requested scenario"):
        plan(case)
    case[2]["diagnostics"]["selected_year"] = 2050
    case[1][0]["sector"] = "unknown"
    with pytest.raises(ValueError, match="sector/region/pollutant"):
        plan(case)


def test_duplicate_product_metadata_is_retained_on_owned_inventory(case):
    case[0][1]["product"] = case[0][1]["reference product"]
    result = plan(case)
    assert all(a["product"] == case[0][1]["product"] for a in result["activities"])


def test_several_fuels_on_one_caller_keep_all_supplier_changes_and_replay(case):
    data, bindings, *_ = case
    other = copy.deepcopy(data[1])
    other.update(
        name="Second combustion",
        **{"reference product": "Second combustion"},
        code="second",
    )
    other["exchanges"][0].update(
        name="Second combustion",
        product="Second combustion",
        input=("native", "second"),
    )
    data.append(other)
    data[2]["exchanges"].append(edge(other, -3))
    data[2]["exchanges"][-1].update(
        formula="second_signed_fuel", **{"uncertainty type": 3, "loc": -3, "scale": 0.2}
    )
    bindings.append(
        {
            "caller": ident(data[2]),
            "supplier": ident(other),
            "sector": "heat",
            "region": "EUR",
        }
    )
    before = copy.deepcopy(data)
    result = plan(case)
    power = next(
        row["activity"]
        for row in result["replacements"]
        if row["activity"]["name"] == "Power"
    )
    assert len(power[CALLER]) == 2
    assert power["exchanges"][1]["input"] != power["exchanges"][2]["input"]
    assert power["exchanges"][2]["amount"] == -3
    assert power["exchanges"][2]["formula"] == "second_signed_fuel"
    assert data == before
    replay = plan((json.loads(json.dumps(materialize(case, result))), *case[1:]))
    assert replay["activities"] == replay["replacements"] == []
    assert replay["diagnostics"]["already_applied"] == 3


def test_shared_variant_is_computed_once_and_keeps_both_caller_links(case, monkeypatch):
    data, bindings, *_ = case
    bindings.append({**bindings[0], "caller": ident(data[4])})
    factory = Mock(wraps=combustion._variant)
    monkeypatch.setattr(combustion, "_variant", factory)
    result = plan(case)
    assert factory.call_count == 2
    assert len(result["activities"]) == 2
    assert len(result["replacements"]) == 3


def test_reviewed_caller_wrapper_checks_aliases_and_maps_emitter_location(
    case, monkeypatch, tmp_path
):
    data, bindings, factors, scenario = case
    review = {
        "schema_version": 1,
        "bindings": bindings,
        "limitations": ["reviewed scope"],
    }
    path = tmp_path / "review.json"
    path.write_text(json.dumps(review))
    monkeypatch.setattr(combustion, "REVIEW_PATH", path)
    definitions = {
        row["sector"]: {
            "exact": True,
            "use_legacy_templates": False,
            "fltr": {"name": [row["caller"]["name"]]},
        }
        for row in bindings
    }
    monkeypatch.setattr(combustion, "get_mapping", lambda *args: definitions)
    locations = []

    def mapper(location):
        locations.append(location)
        return "EUR"

    result = combustion.plan_reviewed_gas_combustion(
        data, factors, scenario, mapper, biosphere_name="bio310"
    )
    assert locations == ["CH", "CH"]
    assert result["diagnostics"]["limitations"] == ["reviewed scope"]
    review["emitting_location_aliases"] = {
        "CH": {"mapping_location": "US", "basis": "synthetic reviewed proxy"}
    }
    path.write_text(json.dumps(review))
    locations.clear()
    result = combustion.plan_reviewed_gas_combustion(
        data, factors, scenario, mapper, biosphere_name="bio310"
    )
    assert locations == ["US", "US"]
    assert all(a["location"] == "CH" for a in result["activities"])
    assert all(
        a[VARIANT]["geography_proxy"]["source_location"] == "CH"
        for a in result["activities"]
    )
    review["retained_cases"] = [{**bindings[0], "reason": "pending independent review"}]
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError, match="explicit review disposition"):
        combustion.plan_reviewed_gas_combustion(
            data, factors, scenario, mapper, biosphere_name="bio310"
        )
    review["supersedes_retained_cases"] = [bindings[0]]
    path.write_text(json.dumps(review))
    resolved = combustion.plan_reviewed_gas_combustion(
        data, factors, scenario, mapper, biosphere_name="bio310"
    )
    assert len(resolved["diagnostics"]["resolved_retained_cases"]) == 1
    assert resolved["diagnostics"]["retained_reviewed_cases"] == []
    definitions["power"]["mask"] = ["Power"]
    with pytest.raises(ValueError, match="aliases disagree"):
        combustion.plan_reviewed_gas_combustion(
            data, factors, scenario, mapper, biosphere_name="bio310"
        )
