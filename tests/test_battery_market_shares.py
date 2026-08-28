from types import SimpleNamespace

import pytest
import xarray as xr

from premise.battery import Battery


def test_battery_market_supplier_shares_are_normalized():
    market = {
        "name": "market for test battery capacity",
        "reference product": "electricity storage capacity",
        "location": "GLO",
        "unit": "kilowatt hour",
        "exchanges": [
            {
                "name": "supplier A",
                "product": "electricity storage capacity",
                "location": "GLO",
                "unit": "kilowatt hour",
                "type": "technosphere",
                "amount": 0.5,
            },
            {
                "name": "supplier B",
                "product": "electricity storage capacity",
                "location": "GLO",
                "unit": "kilowatt hour",
                "type": "technosphere",
                "amount": 0.5,
            },
        ],
    }
    battery = object.__new__(Battery)
    battery.database = [market]
    battery.year = 2025
    battery.iam_data = SimpleNamespace(
        battery_mobile_scenarios=xr.DataArray(
            [[[0.6], [0.401]]],
            dims=("scenario", "chemistry", "year"),
            coords={
                "scenario": ["MIX"],
                "chemistry": ["A", "B"],
                "year": [2025],
            },
        )
    )
    battery.write_log = lambda dataset, status: None

    battery._adjust_shares(
        {market["name"]: "MIX"},
        {"supplier A": "A", "supplier B": "B"},
        "mobile",
    )

    amounts = [exc["amount"] for exc in market["exchanges"]]
    assert sum(amounts) == pytest.approx(1.0)
    assert amounts == pytest.approx([0.6 / 1.001, 0.401 / 1.001])
