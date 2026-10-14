"""Road-vehicle carbon must follow actual suppliers after regionalization."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr

from premise.fuels.carbon import (
    VEHICLE_CO2_KEY,
    reclassify_fuel_co2,
    replace_vehicle_fuel_co2,
    update_vehicle_fuel_co2,
    vehicle_fuel_carbon,
)
from premise.fuels.properties import PROPERTIES_KEY, activity_key
from premise.inventory_imports import get_biosphere_code
from premise.inventory_store import compact_exchange_payload
from premise.transformation import BaseTransformation
from premise.transport import Transport
from premise.utils import FUEL_CO2_TRANSFER_KEY, rescale_exchange, rescale_exchanges
from premise.validation import CarValidation

FLOWS = get_biosphere_code("3.12")


def emission(name, amount, **kwargs):
    return {
        "name": name,
        "type": "biosphere",
        "unit": "kilogram",
        "amount": amount,
        "categories": ("air", "unspecified"),
        **kwargs,
    }


def supplier(location="EUR", share=0.2, total=3, name="market for petrol, low-sulfur"):
    product = name.removeprefix("market for ")
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": "kilogram",
        "exchanges": [
            {
                "name": name,
                "product": product,
                "location": location,
                "unit": "kilogram",
                "type": "production",
                "amount": 1,
            }
        ],
        PROPERTIES_KEY: {
            "reference unit": "kilogram",
            "fossil CO2 kg per unit": total * (1 - share),
            "non-fossil CO2 kg per unit": total * share,
            "unresolved CO2 kg per unit": 0,
        },
    }


def vehicle(fuel, kind="car", location="RER", production=1):
    name = (
        "transport, passenger car, gasoline, Mini, EURO-6"
        if kind == "car"
        else "transport, freight, lorry, diesel, 40 metric ton"
    )
    unit = "kilometer" if kind == "car" else "ton kilometer"
    product = (
        "transport, passenger car, EURO-6"
        if kind == "car"
        else "transport, freight, lorry"
    )
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": unit,
        "exchanges": [
            {
                "name": name,
                "product": product,
                "location": location,
                "type": "production",
                "amount": production,
                "unit": unit,
            },
            {
                "name": fuel["name"],
                "product": fuel["reference product"],
                "location": fuel["location"],
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 0.1 * production,
            },
            emission("Carbon dioxide, fossil", 0.3 * production),
            emission("Nitrogen oxides", 0.001 * production),
        ],
    }


def amounts(dataset):
    return tuple(
        sum(
            e["amount"]
            for e in dataset["exchanges"]
            if e["type"] == "biosphere" and e["name"] == name and e["amount"] >= 0
        )
        for name in ("Carbon dioxide, fossil", "Carbon dioxide, non-fossil")
    )


@pytest.mark.parametrize("share", [0, 0.2, 1])
@pytest.mark.parametrize("compact", [False, True])
def test_actual_supplier_and_carbon_total_replace_inherited_split(share, compact):
    eur, usa = supplier(), supplier("USA", share, 3.2)
    ds = vehicle(eur)
    if compact:
        ds["exchanges"] = [compact_exchange_payload(e) for e in ds["exchanges"]]
    database = [eur, usa, ds]
    update_vehicle_fuel_co2(database, "3.12")
    assert amounts(ds) == pytest.approx((0.24, 0.06))
    ds["location"] = "USA"
    ds["exchanges"][1]["location"] = "USA"
    update_vehicle_fuel_co2(database, "3.12")
    assert amounts(ds) == pytest.approx((0.32 * (1 - share), 0.32 * share))
    once = deepcopy(ds)
    update_vehicle_fuel_co2(database, "3.12")
    assert ds == once
    assert all(e["amount"] >= 0 for e in ds["exchanges"] if e["type"] == "biosphere")


@pytest.mark.parametrize("factor", [0.4, 2])
def test_efficiency_scaling_scales_transfer_amounts_and_preserves_reclassification(
    factor,
):
    ds = {"exchanges": [emission("Carbon dioxide, fossil", 5)]}
    reclassify_fuel_co2(ds, 5, 0.2, FLOWS, "diesel")
    rescale_exchanges(ds, factor)
    assert ds["exchanges"][0][FUEL_CO2_TRANSFER_KEY]["diesel"] == pytest.approx(factor)
    reclassify_fuel_co2(ds, 5 * factor, 0.75, FLOWS, "diesel")
    assert amounts(ds) == pytest.approx((5 * factor * 0.25, 5 * factor * 0.75))
    reclassify_fuel_co2(ds, 5 * factor, 0, FLOWS, "diesel")
    assert amounts(ds) == pytest.approx((5 * factor, 0))


def test_phev_mixed_fuels_reference_production_and_non_co2_are_preserved():
    fossil, bio = supplier(share=0), supplier(
        name="market for diesel, low-sulfur", share=1, total=3.3
    )
    ds = vehicle(fossil, production=2)
    ds["name"] = "transport, passenger car, plugin gasoline hybrid, Medium, EURO-6"
    ds["exchanges"].extend(
        [
            {
                "name": bio["name"],
                "product": bio["reference product"],
                "location": "EUR",
                "type": "technosphere",
                "unit": "kilogram",
                "amount": 0.05,
            },
            {
                "name": "market for electricity, low voltage",
                "type": "technosphere",
                "amount": 0.5,
                "unit": "kilowatt hour",
            },
            emission("Carbon dioxide, fossil", -0.02),
        ]
    )
    untouched = deepcopy(
        [
            e
            for e in ds["exchanges"]
            if e["name"] != "Carbon dioxide, fossil" or e["amount"] < 0
        ]
    )
    update_vehicle_fuel_co2([fossil, bio, ds], "3.12")
    assert amounts(ds) == pytest.approx((0.6, 0.165))
    assert ds[VEHICLE_CO2_KEY]["production amount"] == 2
    for exchange in untouched:
        assert exchange in ds["exchanges"]


def test_fossil_lpg_fallback_records_actual_geography():
    fuel = supplier("RER", name="market for liquefied petroleum gas")
    fuel.pop(PROPERTIES_KEY)
    ds = vehicle(fuel)
    update_vehicle_fuel_co2([fuel, ds], "3.12")
    assert amounts(ds) == pytest.approx((0.1 * 45.5 * 0.0631, 0))
    source = ds[VEHICLE_CO2_KEY]["suppliers"][0]
    assert source["location"] == "RER" and "fossil LPG fallback" in source["source"]


def test_unknown_origin_is_diagnostic_and_leaves_emissions_untouched():
    fuel = supplier()
    fuel[PROPERTIES_KEY]["unresolved CO2 kg per unit"] = 0.1
    ds = vehicle(fuel)
    before = deepcopy(ds["exchanges"])
    update_vehicle_fuel_co2([fuel, ds], "3.12")
    assert ds["exchanges"] == before
    assert ds[VEHICLE_CO2_KEY]["status"] == "unresolved"


def test_forwarders_have_no_direct_co2_or_stale_vehicle_record():
    ds = vehicle(supplier())
    ds["exchanges"] = ds["exchanges"][:1] + [
        dict(ds["exchanges"][0], type="technosphere", location="EUR")
    ]
    ds[VEHICLE_CO2_KEY] = {"status": "resolved", "fossil CO2 kg": 0.3}
    before = deepcopy(ds["exchanges"])
    update_vehicle_fuel_co2([ds], "3.12")
    assert ds["exchanges"] == before
    assert VEHICLE_CO2_KEY not in ds


def test_bio_only_source_and_uncertainty_are_handled_atomically():
    fuel = supplier(share=0.5)
    ds = vehicle(fuel)
    ds["exchanges"][2] = emission("Carbon dioxide, non-fossil", 0.3)
    balance = vehicle_fuel_carbon(ds, {activity_key(fuel): fuel})
    before = deepcopy(ds)
    with pytest.raises(ValueError, match="Carbon dioxide, fossil"):
        replace_vehicle_fuel_co2(ds, balance, {})
    assert ds == before
    replace_vehicle_fuel_co2(ds, balance, FLOWS)
    assert amounts(ds) == pytest.approx((0.15, 0.15))
    fossil = next(e for e in ds["exchanges"] if e["name"] == "Carbon dioxide, fossil")
    fossil.update(
        {
            "uncertainty type": 5,
            "loc": fossil["amount"],
            "minimum": fossil["amount"] * 0.8,
            "maximum": fossil["amount"] * 1.2,
        }
    )
    rescale_exchange(fossil, 0.5, remove_uncertainty=False)
    assert fossil["minimum"] == pytest.approx(fossil["amount"] * 0.8)
    assert fossil[FUEL_CO2_TRANSFER_KEY]["vehicle operating fuel"] == pytest.approx(
        0.075
    )


def test_validator_detects_wrong_split_even_with_correct_total_and_saved_metadata():
    fuel = supplier()
    ds = vehicle(fuel)
    database = [fuel, ds]
    update_vehicle_fuel_co2(database, "3.12")
    validator = object.__new__(CarValidation)
    validator.database = database
    issues = []
    validator.log_issue = lambda *args, **kwargs: issues.append(args)
    validator.check_vehicle_fuel_carbon("transport, passenger car,")
    assert issues == []
    fossil = next(e for e in ds["exchanges"] if e["name"] == "Carbon dioxide, fossil")
    bio = next(e for e in ds["exchanges"] if e["name"] == "Carbon dioxide, non-fossil")
    fossil["amount"], bio["amount"] = bio["amount"], fossil["amount"]
    validator.check_vehicle_fuel_carbon("transport, passenger car,")
    assert len(issues) == 2
    assert all(i[1] == "vehicle fuel carbon split incorrect" for i in issues)


@pytest.mark.parametrize("kind", ["car", "truck"])
@pytest.mark.parametrize("year,factor", [(2020, 1), (2050, 0.5)])
def test_transport_entrypoint_uses_final_suppliers_and_efficiency(
    monkeypatch, kind, year, factor
):
    import premise.transport as module

    regions = ["EUR", "USA", "World"]
    name = (
        "market for petrol, low-sulfur"
        if kind == "car"
        else "market for diesel, low-sulfur"
    )
    suppliers = [
        supplier(r, share=s, name=name) for r, s in zip(regions, (0.2, 0.7, 0.4))
    ]
    original = vehicle(suppliers[0], kind=kind)
    data = suppliers + [original]
    update_vehicle_fuel_co2(data, "3.12")
    variable = "vehicle"
    efficiencies = xr.DataArray([1], dims="variables", coords={"variables": [variable]})
    iam = SimpleNamespace(
        regions=regions,
        passenger_car_efficiencies=efficiencies,
        road_freight_efficiencies=efficiencies,
        passenger_car_fleet=None,
        road_freight_fleet=None,
        bus_fleet=None,
        rail_freight_fleet=None,
        two_wheelers_fleet=None,
        sea_freight_fleet=None,
    )
    transport = object.__new__(Transport)
    BaseTransformation.__init__(
        transport,
        database=data,
        iam_data=iam,
        model="remind",
        pathway="SSP2-NPi",
        year=year,
        version="3.12",
        system_model="cutoff",
    )
    transport.vehicle_type = kind
    transport.vehicle_map = {variable: [original]}
    transport.rev_map = {original["name"]: variable}
    transport.vehicle_fuel_map = {variable: [{"name": name}]}
    transport.find_iam_efficiency_change = lambda **kwargs: 1 / factor
    transport.write_log = lambda *args, **kwargs: None
    monkeypatch.setattr(module, "Transport", lambda **kwargs: transport)
    monkeypatch.setattr(module, "get_scenario_inventory", lambda scenario: data)
    monkeypatch.setattr(module, "replace_scenario_inventory", lambda *args: None)
    monkeypatch.setattr(module, "record_validation_phase", lambda *args: None)
    for cls in ("CarValidation", "TruckValidation"):
        monkeypatch.setattr(
            module, cls, lambda **kwargs: SimpleNamespace(run_checks=lambda: None)
        )
    scenario = {"iam data": iam, "model": "remind", "pathway": "SSP2-NPi", "year": year}
    module._update_vehicles(scenario, kind, "3.12", "cutoff")
    for region, share in zip(regions, (0.2, 0.7, 0.4)):
        ds = next(
            d
            for d in transport.database
            if d["name"] == original["name"] and d["location"] == region
        )
        assert amounts(ds) == pytest.approx(
            (0.3 * factor * (1 - share), 0.3 * factor * share)
        )
        assert ds[VEHICLE_CO2_KEY]["suppliers"][0]["location"] == region
    before = deepcopy(transport.database)
    module._update_vehicles(scenario, kind, "3.12", "cutoff")
    assert transport.database == before
