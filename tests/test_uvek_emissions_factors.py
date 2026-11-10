"""Missing observations and non-reductions must not become accepted factors."""

import numpy as np
import pytest
import xarray as xr

from premise.emissions.uvek_factors import plan_native_gains_reductions


def observations(values, *, regions=None):
    values = np.asarray(values, dtype=float)
    return xr.DataArray(
        values[:, None, :, None],
        dims=("region", "sector", "year", "pollutant"),
        coords={
            "region": regions or [f"R{i}" for i in range(len(values))],
            "sector": ["reviewed sector"],
            "year": [2020, 2050],
            "pollutant": ["NOx"],
        },
    )


def test_actual_reductions_and_emissions_weighted_world_do_not_mutate_input():
    data = observations(
        [[100, 50], [300, 60], [9999, 9999]], regions=["R0", "R1", "World"]
    )
    before = data.copy(deep=True)
    plan = plan_native_gains_reductions(data, 2050)
    assert plan["factors"].sel(region="World").item() == pytest.approx(110 / 400)
    assert plan["factors"].sel(region="R0").item() == 0.5
    assert plan["factors"].sel(region="R1").item() == 0.2
    xr.testing.assert_identical(data, before)


@pytest.mark.parametrize(
    "values,status",
    [
        ([0, 10], "undefined_zero_baseline"),
        ([0, 0], "undefined_zero_baseline"),
        ([10, 0], "zero_projection_not_applied"),
        ([10, 20], "increase_not_applied"),
        ([10, 10], "unchanged"),
        ([np.nan, 5], "missing_data"),
        ([10, np.nan], "missing_data"),
    ],
)
def test_non_reductions_have_explicit_dispositions_and_no_invented_factor(
    values, status
):
    plan = plan_native_gains_reductions(observations([values]), 2050)
    assert plan["status"].sel(region="R0").item() == status
    assert np.isnan(plan["factors"].sel(region="R0").item())


def test_world_missing_region_is_not_silently_zero_filled():
    plan = plan_native_gains_reductions(observations([[100, 50], [np.nan, 60]]), 2050)
    assert plan["status"].sel(region="World").item() == "missing_data"
    assert np.isnan(plan["factors"].sel(region="World").item())


def test_interpolation_uses_absolute_observations_before_taking_ratios():
    plan = plan_native_gains_reductions(observations([[100, 40]]), 2035)
    assert plan["factors"].sel(region="R0").item() == pytest.approx(0.7)


@pytest.mark.parametrize("year", [2019, 2051, True, float("nan")])
def test_invalid_or_outside_year_is_refused(year):
    with pytest.raises(ValueError, match="exact 2020 base"):
        plan_native_gains_reductions(observations([[100, 50]]), year)


@pytest.mark.parametrize("value", [-1, float("inf")])
def test_invalid_observations_are_refused(value):
    with pytest.raises(ValueError, match="negative or infinite"):
        plan_native_gains_reductions(observations([[100, value]]), 2050)


def test_duplicate_axis_and_missing_exact_base_are_refused():
    data = observations([[100, 50], [300, 60]], regions=["R", "R"])
    with pytest.raises(ValueError, match="Duplicate"):
        plan_native_gains_reductions(data, 2050)
    data = observations([[100, 50]]).assign_coords(year=[2019, 2050])
    with pytest.raises(ValueError, match="exact 2020 base"):
        plan_native_gains_reductions(data, 2050)
