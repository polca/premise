"""Shared time windows for IAM electricity markets and their inventories."""

import numpy as np


def electricity_market_periods(system_model):
    return (0,) if system_model == "consequential" else (0, 20, 40, 60)


def electricity_market_name(voltage, period):
    name = f"market group for electricity, {voltage} voltage"
    return f"{name}, {period}-year period" if period else name


def is_residential_solar(technology):
    return "solar pv residential" in technology.lower()


def electricity_window_average(mix, year, period):
    """Average the annual IAM values over an inclusive market window."""
    years = np.arange(year, year + period + 1)
    return (
        mix.interp(year=years, kwargs={"fill_value": "extrapolate"})
        .mean(dim="year")
        .clip(min=0)
    )


def compute_time_weighted_mix(mix, region, year, period):
    values = electricity_window_average(mix.sel(region=region), year, period).values
    total = values.sum()
    if total == 0:
        raise ValueError(
            "All mix components are zero or negative after filtering; "
            "cannot compute a normalized mix."
        )
    return dict(zip(mix.variables.values, values / total))
