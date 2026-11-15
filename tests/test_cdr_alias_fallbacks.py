"""Ordered IMAGE DAC alternatives remain distinct from additive IAM components."""

from copy import deepcopy

import numpy as np
import pytest
import xarray as xr

from premise.emissions.carbon_dioxide_removal import CarbonDioxideRemoval
from premise.iam.data_collection import (
    IAM_CDR_VARS,
    IAMDataCollection,
    cdr_alias_candidates,
)

TECH = "direct air capture (solvent, high-temp, gas heat) with storage"
NEW_PRODUCTION = "Carbon Removal|Geological Storage|Direct Air Capture|Liquid Solvent"
OLD_PRODUCTION = "Carbon Capture|Geological Storage|Direct Air Capture"
NEW_ENERGY = "Final Energy|Carbon Management|Direct Air Capture|Liquid Solvent|Gases"
OLD_ENERGY = "Final Energy|Carbon Management|Direct Air Capture|Gases"
VALUES = {
    NEW_PRODUCTION: [10.0, 20.0],
    OLD_PRODUCTION: [100.0, 400.0],
    NEW_ENERGY: [4.0, 6.0],
    OLD_ENERGY: [40.0, 80.0],
    "other": [1.0, 1.0],
}


def iam():
    obj = object.__new__(IAMDataCollection)
    obj.model, obj.year, obj.min_year = "image", 2050, 2020
    obj.filepath_iam_files = "fixture-image.csv"
    return obj


def data_for(names):
    return xr.DataArray(
        np.array([[VALUES[n] for n in names]]),
        dims=("region", "variables", "year"),
        coords={"region": ["EUR"], "variables": names, "year": [2020, 2050]},
        attrs={"unit": {n: "EJ/yr" if "Energy" in n else "Mt CO2/yr" for n in names}},
    )


def mappings(obj):
    production = obj._IAMDataCollection__get_iam_variable_labels(
        IAM_CDR_VARS, "iam_aliases"
    )
    energy = obj._IAMDataCollection__get_cdr_energy_variable_labels(IAM_CDR_VARS)
    return {TECH: production[TECH]}, {TECH: energy[TECH]}, {}


@pytest.mark.parametrize(
    "production_names,chosen_production",
    [
        ([OLD_PRODUCTION], OLD_PRODUCTION),
        ([NEW_PRODUCTION], NEW_PRODUCTION),
        ([OLD_PRODUCTION, NEW_PRODUCTION], NEW_PRODUCTION),
        ([], None),
    ],
)
@pytest.mark.parametrize(
    "energy_names,chosen_energy",
    [
        ([OLD_ENERGY], OLD_ENERGY),
        ([NEW_ENERGY], NEW_ENERGY),
        ([OLD_ENERGY, NEW_ENERGY], NEW_ENERGY),
        ([], None),
    ],
)
def test_image_dac_fallbacks_are_consistent_across_cdr_consumers(
    production_names, chosen_production, energy_names, chosen_energy, capsys
):
    obj = iam()
    data = data_for(production_names + energy_names or ["other"])
    original = data.copy(deep=True)
    raw = mappings(obj)
    before = deepcopy(raw)
    production, energy, _ = obj._IAMDataCollection__resolve_cdr_alias_fallbacks(
        data, *raw
    )
    output = capsys.readouterr().out
    assert raw == before
    xr.testing.assert_identical(data, original)
    assert production == ({TECH: chosen_production} if chosen_production else {})
    assert energy == {TECH: {"gases": chosen_energy} if chosen_energy else {}}
    missing_groups = int(chosen_production is None) + int(chosen_energy is None)
    assert output.count("none of:") == missing_groups
    if missing_groups:
        assert output.count("The following variables are missing") == 1
        if chosen_production is None:
            assert NEW_PRODUCTION in output and OLD_PRODUCTION in output
        if chosen_energy is None:
            assert NEW_ENERGY in output and OLD_ENERGY in output
    else:
        assert output == ""
    flat = obj._IAMDataCollection__flatten_cdr_energy_variable_labels(energy)
    mix = obj._IAMDataCollection__fetch_market_data(data, production, normalize=False)
    volumes = obj._IAMDataCollection__get_iam_production_volumes(production, data)
    energy_use = obj._IAMDataCollection__fetch_market_data(data, flat, normalize=False)
    grouped = obj._IAMDataCollection__group_cdr_energy_variable_labels(energy)
    efficiency = obj.get_iam_efficiencies_by_carrier(data, production, grouped)
    # Absence stays unavailable; it does not become a fabricated zero series.
    if chosen_production is None:
        assert mix is volumes is None
    else:
        for result in (mix, volumes):
            np.testing.assert_array_equal(
                result.sel(variables=TECH, region="EUR"), VALUES[chosen_production]
            )
            assert result.attrs["unit"][TECH] == "Mt CO2/yr"
    if chosen_energy is None:
        assert energy_use is None
    else:
        np.testing.assert_array_equal(
            energy_use.sel(variables=f"{TECH} - gases", region="EUR"),
            VALUES[chosen_energy],
        )
        assert energy_use.attrs["unit"][f"{TECH} - gases"] == "EJ/yr"
    if chosen_production is None or chosen_energy is None:
        assert efficiency is None
        return
    p0, p1 = VALUES[chosen_production]
    e0, e1 = VALUES[chosen_energy]
    expected = np.clip((e0 / p0) / (e1 / p1), 1, 2)
    assert efficiency.sel(
        variables=TECH, region="EUR", carrier="heat", year=2050
    ).item() == pytest.approx(expected)


@pytest.mark.parametrize("value", [0.0, np.nan])
def test_present_preferred_series_is_selected_even_with_zero_or_missing_values(value):
    obj = iam()
    data = data_for([OLD_PRODUCTION, OLD_ENERGY, NEW_PRODUCTION, NEW_ENERGY])
    data.loc[dict(variables=[NEW_PRODUCTION, NEW_ENERGY])] = value
    production, energy, _ = obj._IAMDataCollection__resolve_cdr_alias_fallbacks(
        data, *mappings(obj)
    )
    assert production[TECH] == NEW_PRODUCTION
    assert energy[TECH]["gases"] == NEW_ENERGY


@pytest.mark.parametrize("partial", [False, True])
def test_plain_lists_remain_additive_and_missing_components_still_warn(partial, capsys):
    obj = iam()
    names = [OLD_PRODUCTION] if partial else [OLD_PRODUCTION, NEW_PRODUCTION]
    data = data_for(names)
    labels = {"additive technology": [OLD_PRODUCTION, NEW_PRODUCTION]}
    resolved, _, _ = obj._IAMDataCollection__resolve_cdr_alias_fallbacks(
        data, labels, {}, {}
    )
    assert resolved == labels
    output = obj._IAMDataCollection__get_iam_production_volumes(resolved, data)
    np.testing.assert_array_equal(
        output.sel(variables="additive technology", region="EUR"),
        np.sum([VALUES[n] for n in names], axis=0),
    )
    assert ("The following variables are missing" in capsys.readouterr().out) == partial


def test_cdr_raw_mapping_consumers_accept_explicit_fallbacks():
    obj = iam()
    production, energy, _ = mappings(obj)
    cdr = object.__new__(CarbonDioxideRemoval)
    cdr.model = "image"
    config = {
        TECH: {
            "iam_aliases": {"image": production[TECH]},
            "energy_use_aliases": {"gases": {"image": energy[TECH]["gases"]}},
        }
    }
    assert cdr._get_carrier_energy_variables(config) == {
        TECH: {"gases": [NEW_ENERGY, OLD_ENERGY]}
    }
    volumes = data_for([OLD_PRODUCTION]).assign_coords(variables=[TECH])
    assert cdr._get_shared_production_alias_groups(config, volumes) == []


@pytest.mark.parametrize(
    "spec",
    [
        {"fallback": []},
        {"fallback": "single name"},
        {"fallback": [1]},
        {"fallback": ["valid"], "unknown": True},
        {"unknown": ["name"]},
    ],
)
def test_bad_fallback_definitions_are_rejected(spec):
    with pytest.raises(ValueError):
        cdr_alias_candidates(spec)
