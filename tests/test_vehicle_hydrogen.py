"""Hydrogen efficiency changes affect tank compression, not unrelated power."""

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr

from premise.inventory_store import compact_exchange_payload
from premise.transport import Transport
from premise.vehicle_hydrogen import (
    BALANCE_KEY,
    COMPRESSION_KEY,
    finish_hydrogen_compression,
    prepare_hydrogen_compression,
    record_hydrogen_compression_balances,
)


def vehicle(kind="truck", *, compact=False, residual=0):
    name = (
        "transport, passenger car, fuel cell electric, Medium"
        if kind == "car"
        else "transport, freight, lorry, fuel cell electric, 40 metric ton"
    )
    specification = {
        "electricity kWh per kg H2": 2.5,
        "hydrogen product": "hydrogen, gaseous, low pressure",
        "electricity product": "electricity, low voltage",
    }
    ds = {
        "name": name,
        "location": "EUR",
        "unit": "kilometer" if kind == "car" else "ton kilometer",
        COMPRESSION_KEY: json.dumps(specification),
        "exchanges": [
            dict(name=name, type="production", unit="kilometer", amount=1),
            dict(
                name="market for hydrogen, gaseous, low pressure",
                product=specification["hydrogen product"],
                location="EUR",
                unit="kilogram",
                type="technosphere",
                amount=0.01,
            ),
            dict(
                name="market group for electricity, low voltage",
                product=specification["electricity product"],
                location="EUR",
                unit="kilowatt hour",
                type="technosphere",
                amount=0.025 + residual,
                **{"uncertainty type": 3, "loc": 0.025 + residual, "scale": 0.001},
            ),
            dict(
                name="market for electricity, medium voltage",
                product="electricity, medium voltage",
                location="EUR",
                unit="kilowatt hour",
                type="technosphere",
                amount=0.007,
            ),
        ],
    }
    if compact:
        ds["exchanges"] = [compact_exchange_payload(e) for e in ds["exchanges"]]
    return ds


def transformer(ds, kind, factor, *, electricity_alias=False, data=True):
    transport = object.__new__(Transport)
    transport.vehicle_type = kind
    values = xr.DataArray([1.0], dims="variables", coords={"variables": ["FCEV"]})
    transport.iam_data = SimpleNamespace(
        passenger_car_efficiencies=values if data else None,
        road_freight_efficiencies=values if data else None,
    )
    transport.rev_map = {ds["name"]: "FCEV"}
    transport.vehicle_fuel_map = {"FCEV": [{"name": "hydrogen"}]}
    if electricity_alias:
        transport.vehicle_fuel_map["FCEV"].append({"name": "electricity, low voltage"})
    transport.find_iam_efficiency_change = lambda **kwargs: 1 / factor
    transport.write_log = lambda *args: None
    return transport


@pytest.mark.parametrize("kind", ["car", "truck"])
@pytest.mark.parametrize("factor", [1, 0.8, 1.2])
@pytest.mark.parametrize("compact", [False, True])
def test_compression_follows_final_hydrogen_with_unrelated_power_unchanged(
    kind, factor, compact
):
    ds = vehicle(kind, compact=compact)
    transport = transformer(ds, kind, factor)
    auxiliary = deepcopy(dict(ds["exchanges"][3]))
    transport.adjust_transport_efficiency(ds)
    assert ds["exchanges"][1]["amount"] == pytest.approx(0.01 * factor)
    assert ds["exchanges"][2]["amount"] == pytest.approx(0.025 * factor)
    assert ds["exchanges"][2]["loc"] == pytest.approx(0.025 * factor)
    assert ds["exchanges"][2]["scale"] == pytest.approx(0.001 * factor)
    assert dict(ds["exchanges"][3]) == auxiliary
    once = deepcopy(ds)
    finish_hydrogen_compression(ds, prepare_hydrogen_compression(ds))
    assert ds == once


def test_merged_auxiliary_electricity_and_alias_are_not_scaled_twice():
    ds = vehicle("car", residual=0.005)
    transformer(ds, "car", 0.8, electricity_alias=True).adjust_transport_efficiency(ds)
    assert ds["exchanges"][2]["amount"] == pytest.approx(0.005 + 0.025 * 0.8)
    assert ds[BALANCE_KEY]["other electricity kWh"] == pytest.approx(0.005)


def test_no_iam_efficiency_keeps_compression_and_records_boundary():
    ds = vehicle()
    transformer(ds, "truck", 1, data=False).adjust_transport_efficiency(ds)
    assert ds["exchanges"][2]["amount"] == 0.025
    assert ds[BALANCE_KEY]["compression electricity kWh"] == 0.025


def test_unmarked_inventory_has_no_assumed_electricity_purpose():
    ds = vehicle()
    ds.pop(COMPRESSION_KEY)
    assert prepare_hydrogen_compression(ds) is None
    transformer(ds, "truck", 0.8).adjust_transport_efficiency(ds)
    assert ds["exchanges"][2]["amount"] == 0.025
    assert BALANCE_KEY not in ds


@pytest.mark.parametrize("bad_amount", [-1, float("nan"), float("inf"), 0.01])
def test_invalid_or_insufficient_compression_fails_before_scaling(bad_amount):
    ds = vehicle()
    ds["exchanges"][2]["amount"] = bad_amount
    with pytest.raises(ValueError):
        transformer(ds, "truck", 0.8).adjust_transport_efficiency(ds)
    assert ds["exchanges"][1]["amount"] == 0.01


def test_provider_split_retains_supply_shares():
    ds = vehicle()
    first = ds["exchanges"][2]
    second = deepcopy(first)
    first["amount"], second["amount"] = 0.01, 0.015
    second["location"] = "USA"
    ds["exchanges"].append(second)
    transformer(ds, "truck", 0.8).adjust_transport_efficiency(ds)
    assert first["amount"] == pytest.approx(0.008)
    assert second["amount"] == pytest.approx(0.012)


def test_retained_templates_and_forwarders_have_distinct_accounting():
    template = vehicle()
    forwarder = vehicle()
    forwarder["emptied"] = True
    forwarder[BALANCE_KEY] = {"stale": 1}
    record_hydrogen_compression_balances([template, forwarder], "truck")
    assert template[BALANCE_KEY]["compression electricity kWh"] == 0.025
    assert BALANCE_KEY not in forwarder


def test_validator_detects_unscaled_electricity_from_current_fuel_input():
    from premise.validation import TransportValidation

    ds = vehicle()
    transformer(ds, "truck", 0.8).adjust_transport_efficiency(ds)
    validator = object.__new__(TransportValidation)
    validator.database = [ds]
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append((args, kwargs))
    validator.check_hydrogen_compression("transport,")
    assert not issues
    ds["exchanges"][2]["amount"] = 0.025
    validator.check_hydrogen_compression("transport,")
    assert len(issues) == 1
    assert issues[0][1]["issue_type"] == "major"


def test_installed_workbooks_retain_explicit_role_through_excel_import():
    from bw2io import ExcelImporter
    from premise.filesystem_constants import INVENTORY_DIR
    from premise.vehicle_hydrogen import compression_specification

    # Independent equal-ratio, intercooled ideal-gas compression calculation.
    expected = (
        3
        * (1.4 / 0.4)
        * (8.314462618 / 0.00201588)
        * 300
        * ((700 / 30) ** (0.4 / 1.4 / 3) - 1)
        / (0.56 * 0.92 * 3_600_000)
    )
    counts = []
    for filename in ("lci-pass_cars.xlsx", "lci-trucks.xlsx"):
        data = ExcelImporter(str(INVENTORY_DIR / filename)).data
        selected = [ds for ds in data if COMPRESSION_KEY in ds]
        counts.append(len(selected))
        for ds in selected:
            spec = compression_specification(ds)
            assert spec["inlet pressure bar"] == 30
            assert spec["tank pressure bar"] == 700
            assert spec["electricity kWh per kg H2"] == pytest.approx(expected)
            prepared = prepare_hydrogen_compression(ds)
            assert prepared[2] == pytest.approx(0, abs=1e-12)
    assert counts == [8, 7]
