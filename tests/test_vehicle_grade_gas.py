"""CNG routing must preserve physical inputs through regional compression."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import xarray as xr

from premise.fuels import biogas
from premise.fuels.base import Fuels
from premise.transformation import BaseTransformation

PRODUCT = "natural gas, medium pressure, vehicle grade"
PRODUCTION = "natural gas production, medium pressure, vehicle grade"
MARKET = "market for natural gas, medium pressure, vehicle grade"
GAS = "market for natural gas, high pressure"
ELECTRICITY = "market group for electricity, medium voltage"
REGIONS = ["EUR", "USA", "IND", "CHA", "NEU", "World"]


def activity(name, product, location, unit, inputs=()):
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": unit,
        "code": f"{name}-{location}",
        "exchanges": [
            {
                "name": name,
                "product": product,
                "location": location,
                "unit": unit,
                "amount": 1.0,
                "type": "production",
            },
            *deepcopy(inputs),
        ],
    }


def purchase(name, product, location, unit, amount):
    return {
        "name": name,
        "product": product,
        "location": location,
        "unit": unit,
        "amount": amount,
        "type": "technosphere",
    }


@pytest.fixture
def fuels(request):
    database = []
    for region in REGIONS:
        database.extend(
            [
                activity(GAS, "natural gas, high pressure", region, "cubic meter"),
                activity(
                    ELECTRICITY, "electricity, medium voltage", region, "kilowatt hour"
                ),
            ]
        )
    station = "market for natural gas service station"
    database.append(activity(station, "natural gas service station", "GLO", "unit"))
    inputs = [
        purchase(GAS, "natural gas, high pressure", "World", "cubic meter", 1.36),
        purchase(
            ELECTRICITY, "electricity, medium voltage", "World", "kilowatt hour", 0.361
        ),
        purchase(station, "natural gas service station", "GLO", "unit", 8.48e-8),
        {
            "name": "Methane, fossil",
            "categories": ("air",),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.00017652,
        },
    ]
    # A second proxy ensures geographic originals become valid forwarding
    # activities as well as covering the RoW/global fallback.
    database.extend(
        activity(PRODUCTION, PRODUCT, location, "kilogram", inputs)
        for location in ("CH", "RoW")
    )
    database.append(
        activity(
            MARKET,
            PRODUCT,
            "GLO",
            "kilogram",
            [purchase(PRODUCTION, PRODUCT, "RoW", "kilogram", 1.0)],
        )
    )
    result = object.__new__(Fuels)
    BaseTransformation.__init__(
        result,
        database=database,
        iam_data=SimpleNamespace(regions=REGIONS),
        model="remind",
        pathway="SSP2-NPi",
        year=request.param,
        version="3.12",
        system_model="cutoff",
    )
    result.write_log = lambda *args, **kwargs: None
    return result


@pytest.mark.parametrize("fuels", [2020, 2050], indirect=True)
def test_cng_compression_and_market_use_regional_suppliers(fuels):
    fuels.regionalize_vehicle_grade_gas()
    by_key = {
        (ds["name"], ds["reference product"], ds["location"]): ds
        for ds in fuels.database
    }
    for region in REGIONS:
        market = by_key[MARKET, PRODUCT, region]
        provider = next(e for e in market["exchanges"] if e["type"] == "technosphere")
        assert provider["location"] == region
        assert provider["amount"] == 1.0
        compression = by_key[
            provider["name"], provider["product"], provider["location"]
        ]
        inputs = {e["name"]: e for e in compression["exchanges"]}
        assert inputs[GAS]["location"] == region
        assert inputs[ELECTRICITY]["location"] == region
        assert inputs[GAS]["unit"] == "cubic meter"
        assert inputs[GAS]["amount"] == pytest.approx(1.36)
        assert inputs[ELECTRICITY]["amount"] == pytest.approx(0.361)
        assert inputs["market for natural gas service station"][
            "amount"
        ] == pytest.approx(8.48e-8)
        assert inputs["Methane, fossil"]["amount"] == pytest.approx(0.00017652)


@pytest.mark.parametrize("fuels", [2020, 2050], indirect=True)
def test_original_cng_forwarders_supply_one_kg_without_duplicate_compression(fuels):
    fuels.regionalize_vehicle_grade_gas()
    for name, location, target in (
        (PRODUCTION, "CH", "NEU"),
        (PRODUCTION, "RoW", "World"),
        (MARKET, "GLO", "World"),
    ):
        original = next(
            ds
            for ds in fuels.database
            if ds["name"] == name and ds["location"] == location
        )
        assert original["emptied"]
        inputs = [e for e in original["exchanges"] if e["type"] != "production"]
        assert inputs == [
            {**purchase(name, PRODUCT, target, "kilogram", 1.0), "uncertainty type": 0}
        ]


@pytest.mark.parametrize("fuels", [2020, 2050], indirect=True)
def test_cng_regionalization_is_idempotent(fuels):
    fuels.regionalize_vehicle_grade_gas()
    before = deepcopy(fuels.database)
    index_before = deepcopy(fuels.index)
    fuels.regionalize_vehicle_grade_gas()
    assert fuels.database == before
    assert fuels.index == index_before


@pytest.mark.parametrize("fuels", [2020, 2050], indirect=True)
def test_gas_update_regionalizes_compression_before_the_market(fuels, monkeypatch):
    """Exercise the public update, not just the new regionalization helper."""
    fuel_map = {"natural gas": []}
    fuels.fuel_map = fuel_map
    fuels.mapping = SimpleNamespace(generate_fuel_map=lambda **kwargs: fuel_map)
    fuels.iam_data.production_volumes = None
    fuels.iam_data.natural_gas_blend = xr.DataArray(
        [1.0], dims="variables", coords={"variables": ["natural gas"]}
    )
    monkeypatch.setattr(
        biogas,
        "fetch_mapping",
        lambda _: {
            "natural gas": [
                {"name": GAS, "reference product": "natural gas, high pressure"}
            ]
        },
    )
    # The fixture already supplies regional gas/electricity markets; exclude
    # their unrelated construction and the separate carbon-accounting step.
    process = fuels.process_and_add_activities

    def process_cng(mapping, **kwargs):
        if "vehicle-grade natural gas" in mapping:
            process(mapping, **kwargs)

    monkeypatch.setattr(fuels, "process_and_add_activities", process_cng)
    monkeypatch.setattr(fuels, "process_and_add_fuel_markets", lambda **kwargs: None)
    monkeypatch.setattr(fuels, "update_carbon_dioxide_emissions", lambda: None)
    fuels.generate_biogas_activities()

    for region in REGIONS:
        market = next(
            ds
            for ds in fuels.database
            if ds["name"] == MARKET and ds["location"] == region
        )
        providers = [e for e in market["exchanges"] if e["type"] == "technosphere"]
        assert len(providers) == 1
        assert providers[0]["name"] == PRODUCTION
        assert providers[0]["location"] == region
        assert any(
            ds["name"] == PRODUCTION and ds["location"] == region
            for ds in fuels.database
        )


@pytest.mark.parametrize("fuels", [2020], indirect=True)
def test_absent_vehicle_grade_inventories_are_optional(fuels):
    fuels.database[:] = [
        ds for ds in fuels.database if ds["reference product"] != PRODUCT
    ]
    before = deepcopy(fuels.database)
    fuels.regionalize_vehicle_grade_gas()
    assert fuels.database == before


@pytest.mark.parametrize("fuels", [2050], indirect=True)
@pytest.mark.parametrize("gas_amount", [48.0, 0.0, -1.0, float("nan")])
def test_declared_native_station_uses_same_regionalization_and_retains_losses(
    fuels, gas_amount
):
    from premise.inventory.activity_maps import FUELS_TECHS, get_mapping

    entry = get_mapping(FUELS_TECHS, "uvek_vehicle_gas_suppliers")["natural gas"][0]
    source = entry["upstream replacement"]["source"]
    gas = purchase(source["name"], source["product"], "CH", source["unit"], gas_amount)
    gas.update({"uncertainty type": 3, "loc": 48.0, "scale": 3.6})
    inputs = [
        gas,
        purchase(
            ELECTRICITY, "electricity, medium voltage", "World", "kilowatt hour", 0.2
        ),
        {
            "name": "Methane, fossil",
            "categories": ("air",),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.001,
        },
    ]
    native = activity(
        entry["name"], entry["reference product"], "CH", "kilogram", inputs
    )
    fossil = activity(source["name"], source["product"], "CH", source["unit"])
    dedicated = activity(
        "Dedicated biomethane station", "biomethane", "CH", "kilogram", inputs
    )
    fuels.database.extend([native, fossil, dedicated])
    fuels.add_to_index([native, fossil, dedicated])
    fuels.family = "uvek"
    before_dedicated = deepcopy(dedicated)
    before_fossil = deepcopy(fossil)

    if not gas_amount > 0:
        with pytest.raises(
            ValueError, match="reviewed vehicle-gas upstream replacement"
        ):
            fuels.regionalize_vehicle_grade_gas()
        assert not native.get("emptied")
        assert dedicated == before_dedicated
        assert fossil == before_fossil
        return

    fuels.regionalize_vehicle_grade_gas()

    for region in REGIONS:
        copy = next(
            d
            for d in fuels.database
            if d["name"] == entry["name"] and d["location"] == region
        )
        exchanges = {e["name"]: e for e in copy["exchanges"]}
        assert exchanges[GAS]["amount"] == pytest.approx(48 / 36)
        assert exchanges[GAS]["loc"] == pytest.approx(48 / 36)
        assert exchanges[GAS]["scale"] == pytest.approx(3.6 / 36)
        assert exchanges[GAS]["uncertainty type"] == 3
        assert exchanges[GAS]["location"] == region
        assert exchanges[GAS]["unit"] == "cubic meter"
        assert exchanges[ELECTRICITY]["amount"] == 0.2
        assert exchanges[ELECTRICITY]["location"] == region
        assert exchanges["Methane, fossil"]["amount"] == 0.001
        assert (
            copy["premise vehicle gas delivery"]["limitations"] == entry["limitations"]
        )
    assert dedicated == before_dedicated
    assert fossil == before_fossil
    assert list(e for e in native["exchanges"] if e["type"] == "technosphere") == [
        {
            **purchase(
                entry["name"], entry["reference product"], "NEU", "kilogram", 1.0
            ),
            "uncertainty type": 0,
        }
    ]
    before = deepcopy(fuels.database)
    fuels.regionalize_vehicle_grade_gas()
    assert fuels.database == before
