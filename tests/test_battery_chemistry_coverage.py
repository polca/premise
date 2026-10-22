"""Prevent plausible market totals from hiding unsupported battery chemistries."""

from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest
import xarray as xr
from bw2io import ExcelImporter

from premise.battery import Battery
from premise.data_collection import (
    BATTERY_MOBILE_SCENARIO_DATA,
    IAMDataCollection,
)
from premise.filesystem_constants import INVENTORY_DIR


@pytest.fixture(scope="module")
def mobile_scenarios():
    return IAMDataCollection.fetch_external_data_battery_mobile_scenarios(None)


@pytest.fixture(scope="module")
def capacity_inventory():
    return ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data


def make_battery(database, year, mobile_scenarios):
    battery = object.__new__(Battery)
    battery.database = deepcopy(database)
    battery.year = year
    battery.iam_data = SimpleNamespace(
        battery_mobile_scenarios=mobile_scenarios,
        battery_stationary_scenarios=None,
    )
    battery.write_log = lambda *args, **kwargs: None
    return battery


def without_assb_exchanges(database):
    """Represent the legacy inventory or a custom inventory missing ASSB links."""
    data = deepcopy(database)
    for ds in data:
        ds["exchanges"] = [
            exc
            for exc in ds["exchanges"]
            if not exc["name"].startswith(
                (
                    "market for battery capacity, ASSB,",
                    "market for battery capacity, all-solid-state battery,",
                )
            )
        ]
    return data


def test_source_chemistries_are_preserved(mobile_scenarios):
    source = pd.read_csv(BATTERY_MOBILE_SCENARIO_DATA)
    for (scenario, chemistry, year), frame in source.groupby(
        ["scenario", "chemistry", "year"]
    ):
        if chemistry in {"NMC900", "NMC900-Si"}:
            continue  # Existing, separate high-nickel proxy is unchanged.
        assert mobile_scenarios.sel(
            scenario=scenario, chemistry=chemistry, year=year
        ).item() == pytest.approx(frame.value.sum())


def test_mix_separates_sodium_and_each_solid_state_share(mobile_scenarios):
    shares = mobile_scenarios.sel(scenario="MIX", year=2040)
    for chemistry in ("SIB", "ASSB (polymer)", "ASSB (oxidic)", "ASSB (sulfidic)"):
        assert shares.sel(chemistry=chemistry).item() == pytest.approx(0.078)
    assert shares.sum().item() == pytest.approx(1)


@pytest.mark.parametrize(
    "year,expected_missing",
    [
        (2021.5, {"ASSB (polymer)": 0.0015}),
        (2022, {"ASSB (polymer)": 0.003}),
        (
            2040,
            dict.fromkeys(
                ["ASSB (polymer)", "ASSB (oxidic)", "ASSB (sulfidic)"], 0.078
            ),
        ),
        (
            2050,
            dict.fromkeys(
                ["ASSB (polymer)", "ASSB (oxidic)", "ASSB (sulfidic)"], 0.078
            ),
        ),
    ],
)
def test_installed_mix_rejects_unsupported_shares_before_mutation(
    capacity_inventory, mobile_scenarios, year, expected_missing
):
    battery = make_battery(
        without_assb_exchanges(capacity_inventory), year, mobile_scenarios
    )
    # A valid stationary update must not be applied before a failed mobile one.
    battery.iam_data.battery_stationary_scenarios = (
        IAMDataCollection.fetch_external_data_battery_stationary_scenarios(None)
    )
    original = deepcopy(battery.database)
    with pytest.raises(
        ValueError, match="positive chemistry shares lack mapped"
    ) as err:
        battery.adjust_battery_market_shares()
    for chemistry, share in expected_missing.items():
        assert f"{chemistry}: {share:.6g}" in str(err.value)
    assert "MIX" in str(err.value)
    assert str(year) in str(err.value)
    assert battery.database == original


@pytest.mark.parametrize("year", [2020, 2021, 2022, 2035, 2050])
def test_supported_markets_still_build_when_mix_is_absent(
    capacity_inventory, mobile_scenarios, year
):
    names = {
        "market for battery capacity (LFP scenario)",
        "market for battery capacity (NCx scenario)",
    }
    battery = make_battery(
        [ds for ds in capacity_inventory if ds["name"] in names],
        year,
        mobile_scenarios,
    )
    battery.adjust_battery_market_shares()
    for market in battery.database:
        assert sum(
            exc["amount"]
            for exc in market["exchanges"]
            if exc["type"] == "technosphere" and exc["unit"] == market["unit"]
        ) == pytest.approx(1)


@pytest.mark.parametrize("missing_kind", ["unmapped", "absent", "wrong unit"])
def test_mapping_alone_cannot_cover_a_missing_capacity_exchange(missing_kind):
    market = {
        "name": "test capacity market",
        "unit": "kilowatt hour",
        "exchanges": [
            {
                "name": "supplier A",
                "type": "technosphere",
                "unit": "kilowatt hour",
                "amount": 1,
            },
        ],
    }
    mapping = {"supplier A": "A"}
    if missing_kind != "unmapped":
        mapping["supplier B"] = "B"
    if missing_kind == "wrong unit":
        market["exchanges"].append(
            {
                "name": "supplier B",
                "type": "technosphere",
                "unit": "kilogram",
                "amount": 1,
            }
        )
    scenarios = xr.DataArray(
        [[[0.8], [0.2]]],
        dims=("scenario", "chemistry", "year"),
        coords={"scenario": ["test"], "chemistry": ["A", "B"], "year": [2040]},
    )
    battery = make_battery([market], 2040, scenarios)
    with pytest.raises(ValueError, match="B: 0.2"):
        battery._adjust_shares({market["name"]: "test"}, mapping, "mobile")
    assert battery.database == [market]


def test_plib_also_rejects_positive_solid_state_share(
    capacity_inventory, mobile_scenarios
):
    market = next(
        ds
        for ds in capacity_inventory
        if ds["name"] == "market for battery capacity (PLiB scenario)"
    )
    legacy = without_assb_exchanges([market])
    battery = make_battery(legacy, 2050, mobile_scenarios)
    with pytest.raises(ValueError, match=r"ASSB \(polymer\): 0.12"):
        battery.adjust_battery_market_shares()
    assert battery.database == legacy


def test_failed_coverage_does_not_change_pack_masses(
    capacity_inventory, mobile_scenarios, monkeypatch
):
    import premise.battery as module

    battery = make_battery(
        without_assb_exchanges(capacity_inventory), 2050, mobile_scenarios
    )
    original = deepcopy(battery.database)
    monkeypatch.setattr(module, "Battery", lambda **kwargs: battery)
    monkeypatch.setattr(
        module, "get_scenario_inventory", lambda scenario: battery.database
    )
    scenario = {
        "iam data": battery.iam_data,
        "model": "image",
        "pathway": "test",
        "year": 2050,
    }
    with pytest.raises(ValueError, match="positive chemistry shares lack mapped"):
        module._update_battery(scenario, "3.12", "cutoff")
    assert battery.database == original
    assert "mapping" not in scenario
