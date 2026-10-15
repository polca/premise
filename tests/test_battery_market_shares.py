from types import SimpleNamespace

import pytest
import xarray as xr

from premise.battery import Battery, load_cell_energy_density


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


@pytest.mark.parametrize("year,density", [(2020, 0.2), (2035, 0.21), (2050, 0.22)])
def test_installed_nmc532_capacity_mass_follows_density(year, density):
    from bw2io import ExcelImporter
    from premise.filesystem_constants import INVENTORY_DIR

    data = ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data
    market = next(
        d for d in data if d["name"] == "market for battery capacity, Li-ion, NMC532"
    )
    old = [e["amount"] for e in market["exchanges"]]
    battery = object.__new__(Battery)
    battery.database = [market]
    battery.year = year
    battery.write_log = lambda *args, **kwargs: None
    battery.adjust_battery_mass()
    for before, exchange in zip(old, market["exchanges"]):
        if exchange["type"] == "technosphere" and exchange["unit"] == "kilogram":
            # Both pack manufacture and end of life follow the same mass.
            assert exchange["amount"] == pytest.approx(before * 0.2 / density)
        else:
            assert exchange["amount"] == before


@pytest.mark.parametrize("year", [2020, 2035, 2050])
def test_active_mobile_mix_chemistries_have_density_aliases(year):
    from bw2io import ExcelImporter
    from premise.data_collection import IAMDataCollection
    from premise.filesystem_constants import INVENTORY_DIR

    data = ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data
    battery = object.__new__(Battery)
    battery.database = data
    battery.year = year
    battery.iam_data = SimpleNamespace(
        battery_mobile_scenarios=IAMDataCollection.fetch_external_data_battery_mobile_scenarios(
            None
        ),
        battery_stationary_scenarios=None,
    )
    battery.write_log = lambda *args, **kwargs: None
    battery.adjust_battery_market_shares()
    mix = next(
        d for d in data if d["name"] == "market for battery capacity (MIX scenario)"
    )
    targets = load_cell_energy_density()
    for exchange in mix["exchanges"]:
        if (
            exchange["type"] == "technosphere"
            and exchange["unit"] == "kilowatt hour"
            and exchange["amount"] > 0
        ):
            assert exchange["name"] in targets
    # Keep old inventories readable while using the actual installed name.
    assert (
        targets["market for battery capacity, Li-ion, NMC523"]
        == targets["market for battery capacity, Li-ion, NMC532"]
    )
