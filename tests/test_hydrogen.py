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


def test_image_hydrogen_final_energy_uses_model_specific_hierarchy():
    hydrogen = HydrogenMixin()
    hydrogen.model = "image"
    hydrogen.scenario = "test-scenario"
    hydrogen.iam_data = make_iam_data(
        variables=[
            "Industry - Steel - All steel - H2",
            "Industry - Steel - BF/BOF - H2",
            "Industry - Cement - H2",
            "Industry - Non-Metallic Minerals - H2",
            "Industry - Chemicals - Fertilizer - H2",
            "Industry - Chemicals - High-Value Chemicals - H2",
            "Industry - Paper - H2",
            "Transport - Road - H2",
            "Buildings - Residential - H2",
        ],
        regions=["EUR"],
        values=[
            [[10]],
            [[4]],
            [[2]],
            [[5]],
            [[3]],
            [[7]],
            [[2]],
            [[1]],
            [[1]],
        ],
    )

    result = hydrogen._get_hydrogen_final_energy_by_subsector()
    demand = {
        row.subsector: row.hydrogen_final_energy_ej_per_year
        for row in result.itertuples()
    }

    assert demand == {
        "Cement": 2,
        "Chemicals": 3,
        "Heating": 1,
        "Other": 12,
        "Steel": 10,
        "Transport": 1,
    }
    steel_sources = result.loc[
        result["subsector"] == "Steel", "source_variables"
    ].item()
    assert steel_sources == "Industry - Steel - All steel - H2"
    other_sources = result.loc[
        result["subsector"] == "Other", "source_variables"
    ].item()
    assert "Industry - Steel - BF/BOF - H2" not in other_sources
    assert "minus:Industry - Cement - H2" in other_sources


def test_image_hydrogen_final_energy_uses_steel_detail_as_fallback():
    hydrogen = HydrogenMixin()
    hydrogen.model = "image"
    hydrogen.scenario = "test-scenario"
    hydrogen.iam_data = make_iam_data(
        variables=[
            "Industry - Steel - BF/BOF - H2",
            "Industry - Steel - DRI EAF - H2",
        ],
        regions=["EUR"],
        values=[[[4]], [[6]]],
    )

    result = hydrogen._get_hydrogen_final_energy_by_subsector()

    assert result["subsector"].tolist() == ["Steel"]
    assert result["hydrogen_final_energy_ej_per_year"].item() == 10


def test_image_market_availability_uses_preferred_steel_level():
    hydrogen = HydrogenMixin()
    hydrogen.model = "image"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.regions = ["EUR", "USA", "World"]
    hydrogen.iam_data = make_iam_data(
        variables=[
            "Industry - Steel - All steel - H2",
            "Industry - Steel - BF/BOF - H2",
        ],
        regions=["EUR", "USA"],
        values=[
            [[1], [0]],
            [[0], [5]],
        ],
    )

    available = hydrogen._available_hydrogen_sector_market_regions()

    assert available["Steel"] == {"EUR"}
    assert available["Other"] == set()


def test_custom_model_combines_heating_and_assigns_cdr_to_other():
    hydrogen = HydrogenMixin()
    hydrogen.model = "custom-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.iam_data = make_iam_data(
        variables=[
            "Buildings - Heating - H2",
            "Buildings - Water heating - H2",
            "CDR - DAC - H2",
            "Industry - Other - H2",
        ],
        regions=["EUR"],
        values=[[[1]], [[2]], [[3]], [[4]]],
    )

    result = hydrogen._get_hydrogen_final_energy_by_subsector()
    demand = {
        row.subsector: row.hydrogen_final_energy_ej_per_year
        for row in result.itertuples()
    }

    assert demand == {"Heating": 3, "Other": 7}


def test_hydrogen_logistics_uses_target_year_and_excludes_world():
    hydrogen = HydrogenMixin()
    hydrogen.model = "remind"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.iam_data = make_iam_data(
        variables=["Buildings - Heating - H2"],
        regions=["EUR", "World"],
        years=[2030, 2050],
        values=[[[1, 2], [1, 2]]],
    )
    hydrogen._add_transport_demand_nodes = lambda demand: demand

    hydrogen.set_hydrogen_logistics()

    assert hydrogen.hydrogen_demand_nodes["year"].tolist() == [2030]
    assert hydrogen.hydrogen_demand_nodes["region"].tolist() == ["EUR"]
    assert (
        hydrogen.hydrogen_demand_nodes[
            "hydrogen_final_energy_ej_per_year"
        ].item()
        == 1
    )
    assert hydrogen.hydrogen_demand_nodes["validation_status"].item() == "ok"


def test_sector_market_regions_exclude_all_world_spelling_variants():
    hydrogen = HydrogenMixin()
    hydrogen.model = "remind"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.regions = ["EUR", "World", "WORLD", "world"]
    hydrogen.iam_data = make_iam_data(
        variables=["Industry - Cement - H2"],
        regions=["EUR", "World", "WORLD", "world"],
        values=[[[1], [1], [1], [1]]],
    )

    available = hydrogen._available_hydrogen_sector_market_regions()

    assert available["Cement"] == {"EUR"}


def test_message_hydrogen_final_energy_sums_selected_chemical_details():
    hydrogen = HydrogenMixin()
    hydrogen.model = "message"
    hydrogen.scenario = "test-scenario"
    hydrogen.iam_data = make_iam_data(
        variables=[
            "Industry - Chemicals - Resins - H2",
            "Industry - Chemicals - High-Value Chemicals - H2",
            "Industry - Chemicals - Methanol - H2",
        ],
        regions=["EUR"],
        values=[[[1]], [[2]], [[3]]],
    )

    result = hydrogen._get_hydrogen_final_energy_by_subsector()

    assert result["subsector"].tolist() == ["Chemicals"]
    assert result["hydrogen_final_energy_ej_per_year"].item() == 6


def test_tiam_hydrogen_final_energy_excludes_steel():
    hydrogen = HydrogenMixin()
    hydrogen.model = "tiam-ucl"
    hydrogen.scenario = "SSP2-RCP19"
    hydrogen.iam_data = make_iam_data(
        variables=[
            "Industry - Steel - H-DRI/EAF - H2",
            "Transport - Road - H2",
        ],
        regions=["WEU"],
        values=[[[10]], [[2]]],
    )

    result = hydrogen._get_hydrogen_final_energy_by_subsector()

    assert result["subsector"].tolist() == ["Transport"]
    assert result["hydrogen_final_energy_ej_per_year"].item() == 2
    assert "Industry - Steel" not in result["source_variables"].item()


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
        {
            "name": "transport, hydrogen, liquid, lorry, unspecified",
            "reference product": (
                "transport, hydrogen, liquid, lorry, unspecified"
            ),
            "location": "GLO",
            "unit": "ton kilometer",
        },
        {
            "name": "gaseous hydrogen production",
            "reference product": "gaseous hydrogen production",
            "location": "EUR",
            "unit": "kilogram",
        },
        {
            "name": "liquid hydrogen production",
            "reference product": "liquid hydrogen production",
            "location": "EUR",
            "unit": "kilogram",
        },
        {
            "name": "liquid hydrogen regasification",
            "reference product": "liquid hydrogen regasification",
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
                "compressed_gaseous_truck": 0.6,
                "liquid_hydrogen_truck": 0.2,
                "compressed_gaseous_pipeline": 0.2,
            },
            {
                "year": 2030,
                "region": "EUR",
                "sector": "Transport",
                "subsector": "Transport",
                "hydrogen_demand_t_per_year": 300,
                "compressed_gaseous_truck": 0.3,
                "liquid_hydrogen_truck": 0.1,
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
    gaseous_conversion = exchanges["gaseous hydrogen production"]
    liquid_conversion = exchanges["liquid hydrogen production"]
    liquid_regasification = exchanges["liquid hydrogen regasification"]

    assert truck["amount"] == 0.01875
    assert truck["location"] == "GLO"
    assert truck["product"] == (
        "transport, hydrogen, gaseous, lorry, "
        "unspecified"
    )
    assert pipeline["amount"] == 0.5
    assert pipeline["location"] == "EUR"
    assert gaseous_conversion["amount"] == 0.375
    assert liquid_conversion["amount"] == 0.125
    assert liquid_regasification["amount"] == 0.125


def test_liquid_hydrogen_transport_modes_add_regasification():
    amounts = HydrogenMixin._hydrogen_conversion_amounts_for_sector_market(
        {
            "liquid_hydrogen_truck": 0.2,
            "liquid_hydrogen_ship": 0.3,
        }
    )

    assert amounts == {
        "liquid": 0.5,
        "liquid_regasification": 0.5,
    }


def test_ammonia_conversion_and_reconversion_use_distinct_mass_factors():
    amounts = HydrogenMixin._hydrogen_conversion_amounts_for_sector_market(
        {"liquid_ammonia_ship": 0.4}
    )

    assert amounts["liquid_ammonia"] == pytest.approx(0.4 / 0.175)
    assert amounts["ammonia_cracking"] == pytest.approx(0.4 * 7.67)


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
            "name": "process consuming market-average hydrogen",
            "reference product": "intermediate product",
            "location": "RER",
            "unit": "kilogram",
            "classifications": [
                (
                    "ISIC rev.4 ecoinvent",
                    "2011:Manufacture of basic chemicals",
                )
            ],
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
            "name": "process consuming market-average hydrogen",
            "reference product": "intermediate product",
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


@pytest.mark.parametrize(
    ("name", "reference_product"),
    [
        (
            "transport, hydrogen, gaseous, lorry, unspecified",
            "transport, hydrogen, gaseous, lorry, unspecified",
        ),
        (
            "transport, hydrogen, liquid, lorry, unspecified",
            "transport, hydrogen, liquid, lorry, unspecified",
        ),
        (
            "hydrogen supply, distributed by pipeline",
            "hydrogen, gaseous, from pipeline",
        ),
        (
            "transport, freight, sea, tanker for liquefied ammonia, "
            "ammonia and mgo",
            "transport, freight, sea, tanker for liquefied ammonia, "
            "ammonia and mgo",
        ),
        (
            "transport, freight, sea, tanker for liquefied hydrogen, "
            "heavy fuel oil",
            "transport, freight, sea, tanker for liquefied hydrogen, "
            "heavy fuel oil",
        ),
        ("gaseous hydrogen production", "gaseous hydrogen production"),
        ("liquid hydrogen production", "liquid hydrogen production"),
        (
            "liquid hydrogen regasification",
            "liquid hydrogen regasification",
        ),
        ("liquid ammonia production", "liquid ammonia production"),
        ("ammonia cracking", "ammonia cracking"),
    ],
)
def test_hydrogen_logistics_activities_are_not_end_user_relinked(
    name, reference_product
):
    hydrogen = HydrogenMixin()
    hydrogen.database = [
        {
            "name": name,
            "reference product": reference_product,
            "location": "RER",
            "unit": "kilogram",
            "classifications": [
                (
                    "ISIC rev.4 ecoinvent",
                    "2410:Manufacture of basic iron and steel",
                )
            ],
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
    assert hydrogen.skipped_hydrogen_consumers[0]["candidate sectors"] == []


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
            "name": "process consuming market-average hydrogen",
            "reference product": "construction product",
            "location": "RER",
            "unit": "kilogram",
            "classifications": [
                (
                    "ISIC rev.4 ecoinvent",
                    (
                        "2395:Manufacture of articles of concrete, cement "
                        "and plaster"
                    ),
                )
            ],
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
            "name": "process consuming market-average hydrogen",
            "reference product": "construction product",
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


def test_other_hydrogen_consumer_is_relinked_by_isic_prefix_exclusion():
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.regions = ["EUR", "World"]
    hydrogen.geo = GeoStub({"RER": "EUR"})
    hydrogen.iam_data = make_iam_data(
        variables=["Industry - Other - H2"],
        regions=["EUR"],
        values=[[[1]]],
    )
    hydrogen.database = [
        {
            "name": "process consuming market-average hydrogen",
            "reference product": "metal product",
            "location": "RER",
            "unit": "kilogram",
            "classifications": [
                (
                    "ISIC rev.4 ecoinvent",
                    (
                        "2420:Manufacture of basic precious and other "
                        "non-ferrous metals"
                    ),
                )
            ],
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
        "market for hydrogen, gaseous, low pressure, for other end uses"
    )
    assert hydrogen.matched_hydrogen_consumers[0]["sector"] == "Other"
    assert hydrogen.unmatched_hydrogen_consumers == []
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


def test_electricity_hydrogen_consumer_is_kept_on_general_market():
    hydrogen = HydrogenMixin()
    hydrogen.database = [
        {
            "name": "electricity production, at hydrogen-fired power plant",
            "reference product": "electricity, high voltage",
            "location": "RER",
            "unit": "kilowatt hour",
            "classifications": [
                (
                    "ISIC rev.4 ecoinvent",
                    (
                        "3510:Electric power generation, transmission "
                        "and distribution"
                    ),
                )
            ],
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
            "name": "electricity production, at hydrogen-fired power plant",
            "reference product": "electricity, high voltage",
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
