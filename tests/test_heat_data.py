from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import yaml

from premise.heat_data import (
    evaluate_heat_layer,
    heat_expression_variables,
    load_heat_mapping,
)

HEAT_MAPPING = (
    Path(__file__).parents[1] / "premise" / "iam_variables_mapping" / "heat.yaml"
)


def make_data(values, variables):
    array = xr.DataArray(
        np.asarray(values, dtype=float).reshape(1, len(variables), -1),
        dims=("region", "variables", "year"),
        coords={
            "region": ["WEU"],
            "variables": variables,
            "year": [2020, 2030],
        },
    )
    array.attrs["unit"] = {variable: "EJ/yr" for variable in variables}
    return array


def test_heat_mapping_schema_is_explicit_for_all_iam_entries():
    mapping = yaml.safe_load(HEAT_MAPPING.read_text(encoding="utf-8"))
    for technology, metadata in mapping.items():
        if not metadata.get("iam_aliases"):
            continue
        assert metadata["layer"] in {
            "buildings_end_use",
            "industrial_end_use",
            "secondary_supply",
        }, technology
        assert metadata["energy_basis"] in {"final_energy", "heat_output"}
        assert metadata["conversion"] in {
            "combustion",
            "electric_boiler",
            "heat_pump",
            "none",
        }


def test_plain_aliases_are_normalized_to_linear_terms():
    mapping = load_heat_mapping(HEAT_MAPPING, "message")
    assert mapping["heat, buildings, from biomethane boiler"]["terms"] == [
        {
            "variable": "Final Energy|Residential and Commercial|Gases|Biomass",
            "coefficient": 1.0,
        }
    ]
    assert "Secondary Energy|Heat|Nuclear" in heat_expression_variables(mapping)


def test_parent_minus_leaf_residual_and_structural_blanks():
    data = make_data([[10, np.nan], [3, np.nan]], ["parent", "leaf"])
    mapping = {
        "residual": {
            "layer": "secondary_supply",
            "energy_basis": "heat_output",
            "terms": [
                {"variable": "parent", "coefficient": 1},
                {"variable": "leaf", "coefficient": -1},
            ],
            "residual": True,
        }
    }
    result, diagnostics = evaluate_heat_layer(data, mapping, "secondary_supply")
    assert result.sel(variables="residual").values.tolist() == [[7.0, 0.0]]
    assert diagnostics["residual technologies"] == ["residual"]


def test_tiny_negative_residual_is_clipped_and_reported():
    data = make_data([[1, 1], [1.000000001, 1]], ["parent", "leaf"])
    mapping = {
        "residual": {
            "layer": "secondary_supply",
            "energy_basis": "heat_output",
            "terms": [
                {"variable": "parent", "coefficient": 1},
                {"variable": "leaf", "coefficient": -1},
            ],
            "residual": True,
        }
    }
    result, diagnostics = evaluate_heat_layer(data, mapping, "secondary_supply")
    assert float(result.min()) == 0
    assert diagnostics["tiny negative clips"] == ["residual"]


def test_material_negative_expression_raises():
    data = make_data([[1, 1], [1.1, 1]], ["parent", "leaf"])
    mapping = {
        "residual": {
            "layer": "secondary_supply",
            "energy_basis": "heat_output",
            "terms": [
                {"variable": "parent", "coefficient": 1},
                {"variable": "leaf", "coefficient": -1},
            ],
            "residual": True,
        }
    }
    with pytest.raises(ValueError, match="beyond the accepted closure tolerance"):
        evaluate_heat_layer(data, mapping, "secondary_supply")


def test_whole_missing_layer_is_unavailable_but_partial_layer_raises():
    mapping = {
        "one": {
            "layer": "industrial_end_use",
            "energy_basis": "final_energy",
            "terms": [{"variable": "one", "coefficient": 1}],
        },
        "two": {
            "layer": "industrial_end_use",
            "energy_basis": "final_energy",
            "terms": [{"variable": "two", "coefficient": 1}],
        },
    }
    unavailable, diagnostics = evaluate_heat_layer(
        make_data([[1, 1]], ["unrelated"]), mapping, "industrial_end_use"
    )
    assert unavailable is None
    assert diagnostics["available"] is False

    with pytest.raises(ValueError, match="partially available"):
        evaluate_heat_layer(make_data([[1, 1]], ["one"]), mapping, "industrial_end_use")


def test_model_specific_overlap_and_residual_decisions_are_encoded():
    message = load_heat_mapping(HEAT_MAPPING, "message")
    assert message["heat, buildings, from natural gas boiler"]["terms"][0][
        "variable"
    ].endswith("Gases|Gas")
    assert message["heat, buildings, from biomethane boiler"]["terms"][0][
        "variable"
    ].endswith("Gases|Biomass")
    assert (
        sum(metadata["layer"] == "secondary_supply" for metadata in message.values())
        == 7
    )

    remind = load_heat_mapping(HEAT_MAPPING, "remind")
    assert "heat, industrial, from electric boiler" in remind
    assert "heat, secondary, from geothermal" not in remind
    assert [
        technology
        for technology, metadata in remind.items()
        if any(
            term["variable"] == "SE|Heat|Electricity|Heat Pump"
            for term in metadata["terms"]
        )
    ] == ["heat, secondary, from heat pump"]

    image = load_heat_mapping(HEAT_MAPPING, "image")
    residual_terms = image["heat, secondary, residual"]["terms"]
    assert residual_terms[0]["coefficient"] == 1
    assert all(term["coefficient"] == -1 for term in residual_terms[1:])

    tiam = load_heat_mapping(HEAT_MAPPING, "tiam-ucl")
    assert all(metadata["layer"] == "secondary_supply" for metadata in tiam.values())
    assert "heat, secondary, from coke with CCS" in tiam
