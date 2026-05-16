import numpy as np
import pytest
import xarray as xr

from premise.external import _interpolate_year_with_bounds
from premise.transformation import get_shares_from_production_volume


def test_interpolate_year_with_bounds_clamps_outside_range():
    data = xr.DataArray(
        [10.0, 20.0, 30.0],
        coords={"year": [2020, 2030, 2040]},
        dims=["year"],
    )

    assert _interpolate_year_with_bounds(data, 2010).values.item(0) == 10.0
    assert _interpolate_year_with_bounds(data, 2025).values.item(0) == 15.0
    assert _interpolate_year_with_bounds(data, 2050).values.item(0) == 30.0


def test_get_shares_from_production_volume_handles_nan():
    suppliers = [
        {
            "name": "supplier a",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "production volume": np.nan,
        },
        {
            "name": "supplier b",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "production volume": 3.0,
        },
    ]

    shares = get_shares_from_production_volume(suppliers)

    assert all(np.isfinite(supplier["share"]) for supplier in shares)
    assert shares[0]["share"] == pytest.approx(1e-9 / (3.0 + 1e-9))
    assert shares[1]["share"] == pytest.approx(3.0 / (3.0 + 1e-9))
