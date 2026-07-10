import importlib.util

import pandas as pd
import pytest
import xarray as xr

if importlib.util.find_spec("bw2data") is None:
    HydrogenMixin = None
    pytestmark = pytest.mark.skip(reason="bw2data is not installed")
else:
    from premise.fuels.base import Fuels
    from premise.fuels.hydrogen import HydrogenMixin


class GeoStub:
    def __init__(self, mapping=None):
        self.mapping = mapping or {}

    def ecoinvent_to_iam_location(self, location):
        return self.mapping.get(location, location)


def make_iam_data(variables, regions, values, years=None):
    years = years or [2030]
    return type(
        "IamData",
        (),
        {
            "production_volumes": xr.DataArray(
                values,
                dims=["variables", "region", "year"],
                coords={
                    "variables": variables,
                    "region": regions,
                    "year": years,
                },
            )
        },
    )()


def test_sector_hydrogen_market_gets_weighted_transport_exchanges():
    hydrogen = HydrogenMixin()
    hydrogen.year = 2030
    hydrogen.iam_to_ecoinvent_loc = {"EUR": ["RER"]}
    hydrogen.database = [
        {
            "name": (
                "transport, hydrogen, gaseous, lorry, "
                "unspecified"
            ),
            "reference product": (
                "transport, hydrogen, gaseous, lorry, "
                "unspecified"
            ),
            "location": "GLO",
            "unit": "ton kilometer",
        },
        {
            "name": "hydrogen supply, distributed by pipeline",
            "reference product": "hydrogen, gaseous, from pipeline",
            "location": "EUR",
            "unit": "kilogram",
        },
    ]
    hydrogen.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "year": 2030,
                "region": "EUR",
                "sector": "Transport",
                "subsector": "Transport",
                "hydrogen_demand_t_per_year": 100,
                "compressed_gaseous_truck": 0.8,
                "compressed_gaseous_pipeline": 0.2,
            },
            {
                "year": 2030,
                "region": "EUR",
                "sector": "Transport",
                "subsector": "Transport",
                "hydrogen_demand_t_per_year": 300,
                "compressed_gaseous_truck": 0.4,
                "compressed_gaseous_pipeline": 0.6,
            },
        ]
    )
    market = {
        "name": "market for hydrogen, gaseous, low pressure, for transport",
        "location": "EUR",
        "exchanges": [],
    }

    hydrogen._add_transport_to_sector_specific_hydrogen_market(market)

    exchanges = {
        exchange["name"]: exchange for exchange in market["exchanges"]
    }
    truck = exchanges[
        "transport, hydrogen, gaseous, lorry, unspecified"
    ]
    pipeline = exchanges["hydrogen supply, distributed by pipeline"]

    assert truck["amount"] == 0.025
    assert truck["location"] == "GLO"
    assert truck["product"] == (
        "transport, hydrogen, gaseous, lorry, "
        "unspecified"
    )
    assert pipeline["amount"] == 0.5
    assert pipeline["location"] == "EUR"


def test_general_hydrogen_market_name_has_no_sector_transport_shares():
    hydrogen = HydrogenMixin()
    hydrogen.year = 2030
    hydrogen.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "year": 2030,
                "region": "EUR",
                "sector": "Transport",
                "subsector": "Transport",
                "hydrogen_demand_t_per_year": 1,
                "compressed_gaseous_truck": 1,
            }
        ]
    )

    shares = hydrogen._hydrogen_transport_shares_for_market(
        {
            "name": "market for hydrogen, gaseous, low pressure",
            "location": "EUR",
        }
    )

    assert shares == {}


def test_sector_hydrogen_market_is_not_generated_without_demand():
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.system_model = "cut-off"
    hydrogen.regions = ["EUR", "USA", "World"]
    hydrogen.iam_data = make_iam_data(
        variables=[
            "hydrogen electrolysis",
            "hydrogen smr",
            "Industry - Steel - H2",
        ],
        regions=["EUR", "USA"],
        values=[
            [[1], [1]],
            [[1], [1]],
            [[1], [0]],
        ],
    )
    called_markets = []
    called_production_volumes = []

    def fake_process_and_add_markets(**kwargs):
        called_markets.append(kwargs["name"])
        called_production_volumes.append(kwargs["production_volumes"])

    hydrogen.process_and_add_markets = fake_process_and_add_markets

    hydrogen._generate_sector_specific_hydrogen_markets({})

    assert called_markets == [
        "market for hydrogen, gaseous, low pressure, for steel"
    ]
    assert (
        called_production_volumes[0]
        .sel(variables="hydrogen electrolysis", region="EUR")
        .values.item()
        == 1
    )
    assert (
        called_production_volumes[0]
        .sel(variables="hydrogen electrolysis", region="USA")
        .values.item()
        == 0
    )
    assert hydrogen.generated_hydrogen_sector_markets == ["Steel"]
    assert hydrogen.generated_hydrogen_sector_market_regions == {
        "Steel": ["EUR"]
    }
    assert "Cement" in hydrogen.skipped_hydrogen_sector_markets


def test_hydrogen_consumer_is_relinked_to_sector_market():
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.regions = ["EUR", "World"]
    hydrogen.geo = GeoStub({"RER": "EUR"})
    hydrogen.iam_data = make_iam_data(
        variables=["Industry - Chemicals - H2"],
        regions=["EUR"],
        values=[[[1]]],
    )
    hydrogen.database = [
        {
            "name": "ammonia production, with market-average hydrogen",
            "reference product": "ammonia, anhydrous, liquid",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for hydrogen, gaseous, low pressure",
                    "product": "hydrogen, gaseous, low pressure",
                    "location": "RER",
                    "unit": "kilogram",
                    "type": "technosphere",
                    "amount": 0.2,
                }
            ],
        }
    ]

    relinked = hydrogen.relink_hydrogen_consumers_to_sector_markets()

    assert relinked == 1
    assert hydrogen.database[0]["exchanges"][0]["name"] == (
        "market for hydrogen, gaseous, low pressure, for chemicals"
    )
    assert hydrogen.matched_hydrogen_consumers == [
        {
            "name": "ammonia production, with market-average hydrogen",
            "reference product": "ammonia, anhydrous, liquid",
            "location": "RER",
            "hydrogen exchange location": "RER",
            "hydrogen exchange amount": 0.2,
            "sector": "Chemicals",
            "old generic hydrogen market": (
                "market for hydrogen, gaseous, low pressure"
            ),
            "new sector specific hydrogen market": (
                "market for hydrogen, gaseous, low pressure, for chemicals"
            ),
        }
    ]
    assert hydrogen.unmatched_hydrogen_consumers == []
    assert hydrogen.skipped_hydrogen_consumers == []


def test_consumer_stays_on_general_market_when_sector_market_unavailable():
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.regions = ["EUR", "World"]
    hydrogen.geo = GeoStub({"RER": "EUR"})
    hydrogen.iam_data = make_iam_data(
        variables=["hydrogen electrolysis"],
        regions=["EUR"],
        values=[[[1]]],
    )
    hydrogen.database = [
        {
            "name": "cement production, with market-average hydrogen",
            "reference product": "cement",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for hydrogen, gaseous, low pressure",
                    "product": "hydrogen, gaseous, low pressure",
                    "location": "RER",
                    "unit": "kilogram",
                    "type": "technosphere",
                    "amount": 0.2,
                }
            ],
        }
    ]

    relinked = hydrogen.relink_hydrogen_consumers_to_sector_markets()

    assert relinked == 0
    assert hydrogen.database[0]["exchanges"][0]["name"] == (
        "market for hydrogen, gaseous, low pressure"
    )
    assert hydrogen.matched_hydrogen_consumers == []
    assert hydrogen.unmatched_hydrogen_consumers == []
    assert hydrogen.skipped_hydrogen_consumers == [
        {
            "name": "cement production, with market-average hydrogen",
            "reference product": "cement",
            "location": "RER",
            "hydrogen exchange location": "RER",
            "hydrogen exchange amount": 0.2,
            "candidate sectors": ["Cement"],
        }
    ]


def test_unmatched_hydrogen_consumer_is_kept_on_general_market():
    hydrogen = HydrogenMixin()
    hydrogen.database = [
        {
            "name": "generic production, with market-average hydrogen",
            "reference product": "generic product",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for hydrogen, gaseous, low pressure",
                    "product": "hydrogen, gaseous, low pressure",
                    "location": "RER",
                    "unit": "kilogram",
                    "type": "technosphere",
                    "amount": 0.2,
                }
            ],
        }
    ]

    relinked = hydrogen.relink_hydrogen_consumers_to_sector_markets()

    assert relinked == 0
    assert hydrogen.database[0]["exchanges"][0]["name"] == (
        "market for hydrogen, gaseous, low pressure"
    )
    assert hydrogen.unmatched_hydrogen_consumers == [
        {
            "name": "generic production, with market-average hydrogen",
            "reference product": "generic product",
            "location": "RER",
            "hydrogen exchange location": "RER",
            "hydrogen exchange amount": 0.2,
            "candidate sectors": [],
        }
    ]
    assert hydrogen.matched_hydrogen_consumers == []
    assert hydrogen.skipped_hydrogen_consumers == []


def test_synthetic_fuel_hydrogen_consumer_is_kept_on_general_market():
    hydrogen = HydrogenMixin()
    hydrogen.database = [
        {
            "name": (
                "diesel production, synthetic, from Fischer Tropsch process, "
                "market-average hydrogen"
            ),
            "reference product": "diesel, synthetic",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "market for hydrogen, gaseous, low pressure",
                    "product": "hydrogen, gaseous, low pressure",
                    "location": "RER",
                    "unit": "kilogram",
                    "type": "technosphere",
                    "amount": 0.2,
                }
            ],
        }
    ]

    relinked = hydrogen.relink_hydrogen_consumers_to_sector_markets()

    assert relinked == 0
    assert hydrogen.database[0]["exchanges"][0]["name"] == (
        "market for hydrogen, gaseous, low pressure"
    )
    assert hydrogen.matched_hydrogen_consumers == []
    assert hydrogen.unmatched_hydrogen_consumers == []
    assert hydrogen.skipped_hydrogen_consumers == [
        {
            "name": (
                "diesel production, synthetic, from Fischer Tropsch process, "
                "market-average hydrogen"
            ),
            "reference product": "diesel, synthetic",
            "location": "RER",
            "hydrogen exchange location": "RER",
            "hydrogen exchange amount": 0.2,
            "candidate sectors": [],
        }
    ]


def test_hydrogen_demand_nodes_are_written_to_fuel_log(monkeypatch):
    fuels = Fuels.__new__(Fuels)
    fuels.model = "test-model"
    fuels.scenario = "test-scenario"
    fuels.year = 2030
    fuels.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "region": "EUR",
                "sector": "Steel",
                "subsector": "Steel",
                "demand_node_type": "steel_plants",
                "demand_nodes": 1.2,
                "demand_nodes_rounded_up": 2,
                "hydrogen_demand_t_per_year": 100,
                "hydrogen_demand_t_per_node_per_year": 50,
                "hydrogen_demand_t_per_node_per_day": 0.2,
                "compressed_gaseous_truck": 0.7,
                "compressed_gaseous_pipeline": 0.3,
                "liquid_hydrogen_truck": 0,
            }
        ]
    )
    logs = []
    monkeypatch.setattr(
        "premise.fuels.base.logger.info", lambda message: logs.append(message)
    )

    fuels.write_hydrogen_demand_node_logs()

    assert len(logs) == 1
    assert "created (hydrogen demand node)" in logs[0]
    assert "hydrogen demand nodes|EUR" in logs[0]
    assert "demand node|Steel|Steel|steel_plants|1.2|2|100" in logs[0]


def test_relinked_hydrogen_consumers_are_written_to_fuel_log(monkeypatch):
    fuels = Fuels.__new__(Fuels)
    fuels.model = "test-model"
    fuels.scenario = "test-scenario"
    fuels.year = 2030
    fuels.matched_hydrogen_consumers = [
        {
            "name": "ammonia production, with market-average hydrogen",
            "location": "RER",
            "hydrogen exchange location": "RER",
            "hydrogen exchange amount": 0.2,
            "sector": "Chemicals",
            "old generic hydrogen market": (
                "market for hydrogen, gaseous, low pressure"
            ),
            "new sector specific hydrogen market": (
                "market for hydrogen, gaseous, low pressure, for chemicals"
            ),
        }
    ]
    logs = []
    monkeypatch.setattr(
        "premise.fuels.base.logger.info", lambda message: logs.append(message)
    )

    fuels.write_hydrogen_sector_market_relink_logs()

    assert len(logs) == 1
    assert "updated (hydrogen sector market relink)" in logs[0]
    assert "sector market relink|Chemicals" in logs[0]
    assert "RER|0.2|market for hydrogen, gaseous, low pressure|" in logs[0]
    assert (
        "market for hydrogen, gaseous, low pressure, for chemicals" in logs[0]
    )
