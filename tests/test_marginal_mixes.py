import numpy as np
import pytest
import xarray as xr

import premise.marginal_mixes as marginal_mixes


def make_shares(values=(0.75, 0.25)):
    return xr.DataArray(
        np.asarray(values),
        dims=("variables",),
        coords={"variables": ["technology A", "technology B"]},
    )


def make_production_data(constant=False):
    years = np.arange(2020, 2051)
    if constant:
        technology_a = np.full_like(years, 100, dtype=float)
        technology_b = np.full_like(years, 100, dtype=float)
    else:
        technology_a = 100 + 10 * np.clip(years - 2031, 0, 4)
        technology_b = 100 + 30 * np.clip(years - 2035, 0, 2)

    return xr.DataArray(
        np.stack([technology_a, technology_b])[None, :, :],
        dims=("region", "variables", "year"),
        coords={
            "region": ["World"],
            "variables": ["technology A", "technology B"],
            "year": years,
        },
    )


def patch_technology_metadata(monkeypatch, lifetimes=(40.0, 40.0)):
    monkeypatch.setattr(
        marginal_mixes, "get_leadtime", lambda technologies: np.array([2.0, 6.0])
    )
    monkeypatch.setattr(
        marginal_mixes,
        "get_lifetime",
        lambda technologies: np.array(lifetimes, dtype=float),
    )
    monkeypatch.setattr(marginal_mixes, "get_list_contrained_suppliers", lambda: [])


def test_market_average_and_individual_short_lead_time_intervals():
    common = {
        "year": 2050,
        "range_time": 2,
        "duration": 0,
        "foresight": False,
        "lead_times": np.array([2.0, 6.0]),
        "shares": make_shares(),
    }

    average = marginal_mixes._get_time_parameters(individual_lead_times=False, **common)
    individual = marginal_mixes._get_time_parameters(
        individual_lead_times=True, **common
    )

    assert average["average lead time"] == 3
    assert average["start"] == 2051
    assert average["end"] == 2055
    np.testing.assert_array_equal(individual["start"], [2050, 2054])
    np.testing.assert_array_equal(individual["end"], [2054, 2058])
    assert individual["start_avg"] == 2051
    assert individual["end_avg"] == 2055


def test_perfect_foresight_is_not_shifted_by_lead_time_mode():
    common = {
        "year": 2050,
        "range_time": 2,
        "duration": 0,
        "foresight": True,
        "lead_times": np.array([2.0, 6.0]),
        "shares": make_shares(),
    }

    average = marginal_mixes._get_time_parameters(individual_lead_times=False, **common)
    individual = marginal_mixes._get_time_parameters(
        individual_lead_times=True, **common
    )

    assert average["start"] == individual["start"] == 2048
    assert average["end"] == individual["end"] == 2052


def test_individual_long_lead_time_intervals():
    parameters = marginal_mixes._get_time_parameters(
        year=2050,
        range_time=0,
        duration=10,
        foresight=False,
        individual_lead_times=True,
        lead_times=np.array([2.0, 6.0]),
        shares=make_shares(),
    )

    np.testing.assert_array_equal(parameters["start"], [2052, 2056])
    np.testing.assert_array_equal(parameters["end"], [2062, 2066])
    assert parameters["start_avg"] == 2053
    assert parameters["end_avg"] == 2063


def test_legacy_fallback_uses_lead_time_for_actual_and_average_intervals():
    common = {
        "year": 2050,
        "range_time": 0,
        "duration": 0,
        "foresight": False,
        "lead_times": np.array([2.0, 6.0]),
        "shares": make_shares(),
    }

    average = marginal_mixes._get_time_parameters(individual_lead_times=False, **common)
    individual = marginal_mixes._get_time_parameters(
        individual_lead_times=True, **common
    )

    assert average["start"] == average["start_avg"] == 2050
    assert average["end"] == average["end_avg"] == 2053
    np.testing.assert_array_equal(individual["start"], [2050, 2050])
    np.testing.assert_array_equal(individual["end"], [2052, 2056])
    assert individual["start_avg"] == 2050
    assert individual["end_avg"] == 2053


def test_requested_years_are_clamped_individually_with_warning():
    with pytest.warns(RuntimeWarning, match="outside the available IAM years"):
        result = marginal_mixes._nearest_available_year(
            np.array([2010, 2025, 2040]),
            np.arange(2020, 2031),
            "technology start years",
        )

    np.testing.assert_array_equal(result, [2020, 2025, 2030])


@pytest.mark.parametrize(
    "args, message",
    [
        ({"range time": 2, "duration": 10}, "cannot both be non-zero"),
        ({"range time": 0, "duration": 2}, "must be 0 or at least 3"),
        ({"range time": -1}, "cannot be negative"),
        (
            {"measurement": 4, "lead time": True},
            "requires a common market interval",
        ),
    ],
)
def test_invalid_consequential_arguments_raise(args, message):
    with pytest.raises(ValueError, match=message):
        marginal_mixes._validate_consequential_args(args)


@pytest.mark.parametrize("measurement", [0, 1, 2, 3, 5])
def test_individual_lead_times_change_marginal_mix(monkeypatch, measurement, capsys):
    patch_technology_metadata(monkeypatch)
    data = make_production_data()
    common = {
        "range time": 1,
        "duration": 0,
        "foresight": False,
        "capital replacement rate": False,
        "measurement": measurement,
    }

    average = marginal_mixes.consequential_method(
        data, 2030, {**common, "lead time": False}, "test market"
    )
    individual = marginal_mixes.consequential_method(
        data, 2030, {**common, "lead time": True}, "test market"
    )

    assert not np.allclose(average.values, individual.values)
    assert individual.sum().item() == pytest.approx(1.0)
    capsys.readouterr()


def test_area_capital_replacement_uses_squared_individual_intervals(
    monkeypatch, capsys
):
    patch_technology_metadata(monkeypatch, lifetimes=(1.0, 1.0))

    result = marginal_mixes.consequential_method(
        make_production_data(constant=True),
        2030,
        {
            "range time": 0,
            "duration": 0,
            "foresight": False,
            "lead time": True,
            "capital replacement rate": True,
            "measurement": 2,
        },
        "test market",
    )

    np.testing.assert_allclose(result.values.ravel(), [0.1, 0.9])
    capsys.readouterr()
