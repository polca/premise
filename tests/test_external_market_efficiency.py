from types import SimpleNamespace

import pytest
import xarray as xr

from premise.external import ExternalScenario

EFFICIENCY = xr.DataArray(
    [[[2.0, 2.0]]],
    dims=("region", "variables", "year"),
    coords={"region": ["CH"], "variables": ["eff"], "year": [2020, 2050]},
)


def adjusted_amounts(setting):
    market = {
        "exchanges": [
            {"name": n, "type": "technosphere", "unit": "kilowatt hour", "amount": 1.0}
            for n in ("electricity, grid", "electricity, hydro", "heat")
        ]
        + [{"name": "Water", "type": "biosphere", "unit": "cubic meter", "amount": 1.0}]
    }
    ExternalScenario.adjust_efficiency_of_new_markets(
        SimpleNamespace(year=2035),
        market,
        {"efficiency": [{"variable": "eff", **setting}]},
        "CH",
        EFFICIENCY,
    )
    return [exc["amount"] for exc in market["exchanges"]]


@pytest.mark.parametrize(
    "setting,expected",
    [
        ({}, [0.5, 0.5, 0.5, 0.5]),
        ({"includes": {"technosphere": ["electricity"]}}, [0.5, 0.5, 1, 1]),
        ({"includes": {"technosphere": [{"name": "electricity"}]}}, [0.5, 0.5, 1, 1]),
        (
            {
                "includes": {"technosphere": ["electricity"]},
                "excludes": {"technosphere": ["hydro"]},
            },
            [0.5, 1, 1, 1],
        ),
        ({"excludes": {"biosphere": ["Water"]}}, [0.5, 0.5, 0.5, 1]),
    ],
)
def test_market_efficiency_filters_match_production_pathways(setting, expected):
    assert adjusted_amounts(setting) == expected
