from types import SimpleNamespace

import xarray as xr

from premise.fuels.base import Fuels


def test_gcam_coal_methane_inventory_is_regionalized():
    coal_methane = {
        "name": (
            "methane, synthetic, gaseous, 5 bar, from coal-based hydrogen, "
            "at fuelling station"
        ),
        "reference product": "methane, high pressure",
        "location": "RER",
    }
    fuels = object.__new__(Fuels)
    fuels.database = [coal_methane]
    fuels.fuel_map = {"methane, from coal": [coal_methane]}
    fuels.iam_data = SimpleNamespace(
        production_volumes=None,
        natural_gas_blend=None,
    )
    fuels.mapping = SimpleNamespace(generate_fuel_map=lambda: {})

    captured = {}

    def capture_regionalization(mapping, production_volumes):
        captured.update(mapping)

    fuels.process_and_add_activities = capture_regionalization

    fuels.generate_biogas_activities()

    assert captured == {"methane, from coal": [coal_methane]}


def test_diesel_markets_receive_the_marginal_blend():
    diesel_supplier = {
        "name": "diesel production",
        "reference product": "diesel",
        "location": "WEU",
        "unit": "kilogram",
        "exchanges": [],
    }
    diesel_blend = xr.DataArray(
        [[[1.0]]],
        dims=("variables", "region", "year"),
        coords={"variables": ["diesel"], "region": ["WEU"], "year": [2050]},
    )
    production_volumes = xr.DataArray(
        [[[10.0]]],
        dims=("variables", "region", "year"),
        coords={"variables": ["diesel"], "region": ["WEU"], "year": [2050]},
    )
    fuel_map = {"diesel": [diesel_supplier]}
    fuels = object.__new__(Fuels)
    fuels.fuel_map = fuel_map
    fuels.model = "image"
    fuels.system_model = "consequential"
    fuels.iam_data = SimpleNamespace(
        diesel_blend=diesel_blend,
        production_volumes=production_volumes,
    )
    fuels.mapping = SimpleNamespace(
        generate_sets_from_filters=lambda filters: {},
        generate_fuel_map=lambda model: fuel_map,
    )
    fuels.process_and_add_activities = lambda **kwargs: None
    fuels.generate_biofuel_activities = lambda: None
    fuels._filter_biodiesel_feedstocks = lambda: None
    fuels._filter_bioethanol_feedstocks = lambda: None
    market_calls = []
    carbon_calls = []
    fuels.process_and_add_markets = lambda **kwargs: market_calls.append(kwargs)
    fuels.update_fuel_carbon_dioxide_emissions = lambda **kwargs: carbon_calls.append(
        kwargs
    )

    fuels.generate_synthetic_fuel_activities()

    assert len(market_calls) == 4
    assert all(call["technology_shares"] is diesel_blend for call in market_calls)
    assert len(carbon_calls) == 1
    assert carbon_calls[0]["technology_shares"] is diesel_blend


def test_fuel_carbon_update_can_use_marginal_technology_shares():
    fuel_input = {
        "name": "market for diesel",
        "product": "diesel",
        "location": "R1",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 2.0,
    }
    fossil = {
        "name": "Carbon dioxide, fossil",
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 10.0,
    }
    consumer = {
        "name": "diesel consumer",
        "location": "R1",
        "exchanges": [fuel_input, fossil],
    }
    production_volumes = xr.DataArray(
        [[[0.5]], [[0.5]]],
        dims=("variables", "region", "year"),
        coords={
            "variables": ["biodiesel", "diesel"],
            "region": ["R1"],
            "year": [2050],
        },
    )
    marginal_mix = xr.DataArray(
        [[[0.0]], [[1.0]]],
        dims=("variables", "region", "year"),
        coords={
            "variables": ["biodiesel", "diesel"],
            "region": ["R1"],
            "year": [2050],
        },
    )
    fuels = object.__new__(Fuels)
    fuels.database = [consumer]
    fuels.fuel_map = {"biodiesel": [], "diesel": []}
    fuels.iam_data = SimpleNamespace(production_volumes=production_volumes)
    fuels.regions = ["R1"]
    fuels.year = 2050
    fuels.ecoinvent_to_iam_loc = {}
    fuels.is_in_index = lambda exchange, location: True

    fuels.update_fuel_carbon_dioxide_emissions(
        variables=["biodiesel", "diesel"],
        market_names=["market for diesel"],
        co2_intensity=3.15,
        fossil_variables=["diesel"],
        technology_shares=marginal_mix,
    )

    assert fossil["amount"] == 10.0
    assert len(consumer["exchanges"]) == 2
