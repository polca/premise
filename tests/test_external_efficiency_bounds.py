from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr
import yaml

from premise.efficiency_bounds import (
    bound_external_efficiency,
    combined_efficiency_check,
    validate_policy,
)
from premise.external import adjust_efficiency
from premise.external_data_validation import (
    check_config_file,
    flag_activities_to_adjust,
)


def plant(name="heat and power co-generation, wood chips, 6667 kW"):
    return {
        "name": name,
        "reference product": "electricity, high voltage",
        "unit": "kilowatt hour",
        "location": "CH",
        "current efficiency": 0.25,
        "exchanges": [
            {"name": "wood", "type": "technosphere", "unit": "kilogram", "amount": 2},
            {"name": "CO2", "type": "biosphere", "unit": "kilogram", "amount": 3},
            {"name": name, "type": "production", "unit": "kilowatt hour", "amount": 1},
        ],
    }


@pytest.mark.parametrize(
    "target,expected", [(0.000117, 0.05), (0.31, 0.3), (1, 0.3), (0.24, 0.24)]
)
def test_external_scaling_uses_bounded_target_once_for_both_exchange_families(
    target, expected
):
    ds = plant()
    ds.update(
        {
            "absolute efficiency": {"arbitrary variable": True},
            "technosphere filters": {"arbitrary variable": [None, {"CH": target}]},
            "biosphere filters": {"arbitrary variable": [None, {"CH": target}]},
            "efficiency bounds": {
                "arbitrary variable": {"bounds": {"min": 0.05, "max": 0.30}}
            },
        }
    )
    adjust_efficiency(ds, {}, {})
    assert ds["exchanges"][0]["amount"] == pytest.approx(2 * 0.25 / expected)
    assert ds["exchanges"][1]["amount"] == pytest.approx(3 * 0.25 / expected)
    assert ds["exchanges"][2]["amount"] == 1
    assert len(ds["efficiency bounds audit"]) == 1
    record = ds["efficiency bounds audit"][0]
    assert record["requested_efficiency"] == target
    assert record["applied_efficiency"] == expected
    assert record["combined_efficiency_check"]["status"] == "not_available"
    assert ds["log parameters"]["new efficiency"] == expected


def test_no_technology_inference_even_for_known_combustion_names():
    ds = plant()
    applied, audit = bound_external_efficiency(ds, 1e-4)
    assert applied == 1e-4
    assert audit["status"] == "not_configured"
    applied, audit = bound_external_efficiency(
        plant("arbitrary producer"), 1e-4, {"bounds": {"min": 0.05, "max": 0.3}}
    )
    assert applied == 0.05


def test_unconfigured_fuel_cell_renewables_heat_pumps_and_gas_proxy_are_unchanged():
    for name in (
        "electricity production, photovoltaic",
        "electricity production, wind turbine",
        "electricity from fuel cell",
        "heat pump",
        "heat and power co-generation, natural gas, 1MW electrical, lean burn",
    ):
        applied, record = bound_external_efficiency(plant(name), 0.01)
        assert applied == 0.01
        assert record["status"] == "not_configured"
    ds = plant()
    ds.update(unit="megajoule", **{"reference product": "heat"})
    assert bound_external_efficiency(ds, 3.5)[0] == 3.5


def test_custom_bounds_and_unconfigured_policy_do_not_modify_raw_target():
    ds = plant()
    policy = {"bounds": {"min": 0.08, "max": 0.35}}
    original = deepcopy(policy)
    assert bound_external_efficiency(ds, 0.0001, policy)[0] == 0.08
    assert policy == original
    assert bound_external_efficiency(ds, 0.0001, {})[0] == 0.0001


@pytest.mark.parametrize(
    "bounds",
    [
        {"min": 0, "max": 0.3},
        {"min": 0.5, "max": 0.3},
        {"min": 0.05, "max": float("inf")},
        {"min": True, "max": 0.3},
    ],
)
def test_invalid_electrical_bounds_rejected(bounds):
    with pytest.raises(ValueError):
        validate_policy({"bounds": bounds})


@pytest.mark.parametrize("target", [-0.1, float("nan"), float("inf")])
def test_invalid_target_rejected(target):
    with pytest.raises(ValueError):
        bound_external_efficiency(plant(), target)


def test_zero_and_unclassified_targets_are_not_replaced_by_a_floor():
    assert bound_external_efficiency(plant(), 0)[0] == 0
    applied, record = bound_external_efficiency(plant("custom process"), 0.0001)
    assert applied == 0.0001
    assert record["status"] == "not_configured"


def test_combined_diagnostic_converts_units_without_clipping_to_100_percent():
    outputs = {
        "electricity": 1,
        "electricity unit": "PJ/yr.",
        "heat": 3e6,
        "heat unit": "GJ/yr",
    }
    record = combined_efficiency_check(0.31, 0.30, outputs)
    assert record["requested_combined_efficiency"] == pytest.approx(1.24)
    assert record["applied_combined_efficiency"] == pytest.approx(1.20)
    assert record["applied_above_100_percent"]
    outputs["heat unit"] = "kilogram/yr"
    assert combined_efficiency_check(0.31, 0.30, outputs)["status"] == "not_available"
    del outputs["heat"]
    assert combined_efficiency_check(0.31, 0.30, outputs)["status"] == "not_available"


@pytest.mark.parametrize("provide_heat", [False, True])
def test_external_flagging_requires_no_heat_output(provide_heat):
    data = xr.DataArray(
        [[[1.0, 2.0], [3.0, 6.0]]],
        dims=("region", "variables", "year"),
        coords={"region": ["CH"], "variables": ["power", "heat"], "year": [2020, 2050]},
        attrs={"unit": {"power": "PJ/yr.", "heat": "PJ/yr."}},
    )
    efficiency = xr.DataArray(
        [[[0.001, 0.001]]],
        dims=("region", "variables", "year"),
        coords={
            "region": ["CH"],
            "variables": ["arbitrary efficiency"],
            "year": [2020, 2050],
        },
    )
    setting = {
        "variable": "arbitrary efficiency",
        "absolute": True,
        "bounds": {"min": 0.05, "max": 0.30},
    }
    if provide_heat:
        setting["heat pathway"] = "heat"
    values = {
        "production volume variable": "power",
        "efficiency": [setting],
        "replaces": [],
        "replaces in": [],
        "replacement ratio": 1,
        "regionalize": False,
    }
    ds = plant()
    flag_activities_to_adjust(
        ds,
        {"regions": ["CH"], "production volume": data, "efficiency": efficiency},
        2035,
        values,
    )
    adjust_efficiency(ds, {}, {})
    record = ds["efficiency bounds audit"][0]
    assert record["applied_efficiency"] == 0.05
    assert record["combined_efficiency_check"]["status"] == (
        "checked" if provide_heat else "not_available"
    )
    if provide_heat:
        assert record["combined_efficiency_check"][
            "requested_combined_efficiency"
        ] == pytest.approx(0.004)


def test_config_schema_accepts_optional_bounds_without_heat():
    config = {
        "production pathways": {
            "power": {
                "production volume": {"variable": "electricity"},
                "ecoinvent alias": {
                    "name": "plant",
                    "reference product": "electricity",
                },
                "efficiency": [
                    {
                        "variable": "eff",
                        "absolute": True,
                        "bounds": {"min": 0.05, "max": 0.3},
                    }
                ],
            }
        }
    }
    package = SimpleNamespace(
        get_resource=lambda _: SimpleNamespace(raw_read=lambda: yaml.safe_dump(config))
    )
    check_config_file(package)
    config["production pathways"]["power"]["efficiency"][0]["bounds"] = {
        "min": 0.8,
        "max": 0.3,
    }
    with pytest.raises(ValueError, match="Efficiency bounds"):
        check_config_file(package)


def test_missing_absolute_target_is_not_treated_as_one():
    from premise.external_data_validation import find_iam_efficiency_change

    data = xr.DataArray(
        [[[float("nan"), float("nan")]]],
        dims=("region", "variables", "year"),
        coords={"region": ["CH"], "variables": ["missing"], "year": [2020, 2050]},
    )
    for variable in ("missing", "absent"):
        assert (
            find_iam_efficiency_change(variable, "CH", data, 2035, absolute=True) == 0
        )
        assert find_iam_efficiency_change(variable, "CH", data, 2035) == 1
    data.values[:] = 1
    assert find_iam_efficiency_change("missing", "CH", data, 2035, absolute=True) == 1


def test_relative_normalization_never_fills_missing_absolute_targets():
    import numpy as np
    import pandas as pd
    from premise.data_collection import IAMDataCollection

    config = {
        "production pathways": {
            "power": {
                "production volume": {"variable": "production"},
                "efficiency": [
                    {
                        "variable": "absolute",
                        "absolute": True,
                        "bounds": {"min": 0.05, "max": 0.3},
                    },
                    {"variable": "relative", "reference year": 2020},
                ],
            }
        }
    }
    rows = [
        ["MODEL", "arbitrary scenario", "CH", "production", "PJ/yr", 1.0, 2.0],
        ["MODEL", "arbitrary scenario", "CH", "absolute", "fraction", np.nan, np.nan],
        ["MODEL", "arbitrary scenario", "CH", "relative", "fraction", 0.3, 0.6],
    ]
    frame = pd.DataFrame(
        rows,
        columns=["model", "scenario", "region", "variables", "unit", "2020", "2050"],
    )
    resources = {
        "config": SimpleNamespace(raw_read=lambda: yaml.safe_dump(config).encode()),
        "scenario_data": SimpleNamespace(
            raw_read=lambda: frame.to_csv(index=False).encode()
        ),
    }
    dp = SimpleNamespace(get_resource=lambda name: resources[name])
    data = IAMDataCollection.__new__(IAMDataCollection).get_external_data(
        [{"scenario": "arbitrary scenario", "data": dp}]
    )[0]["efficiency"]
    assert np.isnan(data.sel(variables="absolute")).all()
    np.testing.assert_allclose(
        data.sel(variables="relative").values.ravel(), [1.0, 2.0]
    )


def test_small_absolute_adjustment_is_not_skipped_at_a_configured_boundary():
    ds = plant()
    ds["current efficiency"] = 0.3001
    ds.update(
        {
            "absolute efficiency": {"eff": True},
            "technosphere filters": {"eff": [None, {"CH": 0.3001}]},
            "biosphere filters": {"eff": [None, {"CH": 0.3001}]},
            "efficiency bounds": {"eff": {"bounds": {"min": 0.05, "max": 0.30}}},
        }
    )
    adjust_efficiency(ds, {}, {})
    assert ds["exchanges"][0]["amount"] == pytest.approx(2 * 0.3001 / 0.30)
    assert ds["exchanges"][1]["amount"] == pytest.approx(3 * 0.3001 / 0.30)


def test_excludes_are_kept_for_every_efficiency_variable():
    production = xr.DataArray(
        [[[1.0, 1.0]]],
        dims=("region", "variables", "year"),
        coords={"region": ["CH"], "variables": ["power"], "year": [2020, 2050]},
    )
    efficiency = xr.DataArray(
        [[[2.0, 2.0], [2.0, 2.0]]],
        dims=("region", "variables", "year"),
        coords={
            "region": ["CH"],
            "variables": ["electricity efficiency", "heat efficiency"],
            "year": [2020, 2050],
        },
    )
    values = {
        "production volume variable": "power",
        "efficiency": [
            {
                "variable": "electricity efficiency",
                "includes": {"technosphere": ["electricity"]},
                "excludes": {"technosphere": ["hydro"]},
            },
            {
                "variable": "heat efficiency",
                "includes": {"technosphere": ["heat"]},
                "excludes": {"technosphere": ["wood"]},
            },
        ],
        "replaces": [],
        "replaces in": [],
        "replacement ratio": 1,
        "regionalize": False,
    }
    ds = plant()
    ds["exchanges"] = [
        {"name": n, "type": "technosphere", "unit": "kilowatt hour", "amount": 1.0}
        for n in ("electricity, grid", "electricity, hydro", "heat, gas", "heat, wood")
    ]
    flag_activities_to_adjust(
        ds, {"production volume": production, "efficiency": efficiency}, 2035, values
    )
    adjust_efficiency(ds, {}, {})
    amounts = {exc["name"]: exc["amount"] for exc in ds["exchanges"]}
    assert amounts == {
        "electricity, grid": 0.5,
        "electricity, hydro": 1.0,
        "heat, gas": 0.5,
        "heat, wood": 1.0,
    }
