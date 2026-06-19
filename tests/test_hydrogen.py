import importlib.util

import pandas as pd
import pytest

if importlib.util.find_spec("bw2data") is None:
    HydrogenMixin = None
    pytestmark = pytest.mark.skip(reason="bw2data is not installed")
else:
    from premise.fuels.hydrogen import HydrogenMixin


def test_sector_hydrogen_market_gets_weighted_transport_exchanges():
    hydrogen = HydrogenMixin()
    hydrogen.year = 2030
    hydrogen.iam_to_ecoinvent_loc = {"EUR": ["RER"]}
    hydrogen.database = [
        {
            "name": (
                "transport, hydrogen, gaseous, lorry, "
                "market average propulsion system"
            ),
            "reference product": (
                "transport, hydrogen, gaseous, lorry , "
                "market average propulsion system"
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
        "transport, hydrogen, gaseous, lorry, market average propulsion system"
    ]
    pipeline = exchanges["hydrogen supply, distributed by pipeline"]

    assert truck["amount"] == 0.5
    assert truck["location"] == "GLO"
    assert truck["product"] == (
        "transport, hydrogen, gaseous, lorry , "
        "market average propulsion system"
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


def test_hydrogen_consumer_is_relinked_to_sector_market():
    hydrogen = HydrogenMixin()
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
            "generic hydrogen market": "market for hydrogen, gaseous, low pressure",
            "sector specific hydrogen market": (
                "market for hydrogen, gaseous, low pressure, for chemicals"
            ),
        }
    ]
    assert hydrogen.unmatched_hydrogen_consumers == []
    assert hydrogen.skipped_hydrogen_consumers == []


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
