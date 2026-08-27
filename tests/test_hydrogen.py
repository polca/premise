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
    hydrogen.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "year": 2030,
                "region": "EUR",
                "subsector": "Steel",
                "demand_node_type": "steel_plants",
                "demand_nodes": 2,
                "hydrogen_demand_t_per_node_per_year": 1000,
                "distribution_status": "ok",
            }
        ]
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
    assert hydrogen.hydrogen_demand_nodes["distribution_status"].item() == "ok"
    assert (
        hydrogen.hydrogen_demand_nodes["distribution_rule"].item()
        == "heating_always_pipeline"
    )


def test_transport_fueling_stations_include_refueling_frequency(monkeypatch):
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"

    passenger_variable = "Passenger FCEV hydrogen service"
    freight_variable = "Freight FCEV hydrogen service"
    hydrogen.iam_data = type(
        "IamData",
        (),
        {
            "data": xr.DataArray(
                [
                    [[2.4525]],  # 150,000 passenger cars
                    [[14.34]],  # 40,000 freight vehicles
                ],
                dims=["variables", "region", "year"],
                coords={
                    "variables": [passenger_variable, freight_variable],
                    "region": ["EUR"],
                    "year": [2030],
                },
            )
        },
    )()

    mappings = {
        "transport_passenger_cars.yaml": {
            "passenger car, fuel cell electric": {
                "iam_aliases": {"test-model": passenger_variable},
                "ecoinvent_fuel_aliases": {"fltr": "hydrogen"},
            }
        },
        "transport_road_freight.yaml": {
            "truck, fuel cell electric": {
                "iam_aliases": {"test-model": freight_variable},
                "ecoinvent_fuel_aliases": {"fltr": "hydrogen"},
            }
        },
    }
    monkeypatch.setattr(
        "premise.fuels.hydrogen.fetch_mapping",
        lambda path: mappings[path.name],
    )

    result = hydrogen._get_transport_fueling_stations()

    # Passenger: 150,000 / 7 / 1,500; freight: 40,000 / 3.5 / 400.
    assert result["activity_proxy_value"].item() == pytest.approx(190_000)
    assert result["demand_nodes"].item() == pytest.approx(300 / 7)


@pytest.mark.parametrize(
    ("demand", "expected_rule"),
    [
        (0, "default_small_demand"),
        (999.5, "default_small_demand"),
        (1000, "default_medium_demand"),
        (4999.5, "default_medium_demand"),
        (5000, "default_large_demand"),
        (49999.5, "default_large_demand"),
        (50000, "default_very_large_demand"),
    ],
)
def test_hydrogen_distribution_demand_intervals(demand, expected_rule):
    hydrogen = HydrogenMixin()
    row = pd.Series(
        {"hydrogen_demand_t_per_node_per_year": demand}
    )

    rule = hydrogen._select_hydrogen_distribution_rule(row)

    assert rule["name"] == expected_rule


def test_very_large_demand_reports_on_site_production_separately():
    hydrogen = HydrogenMixin()
    demand = pd.DataFrame(
        [{"hydrogen_demand_t_per_node_per_year": 50_000}]
    )

    result = hydrogen._add_hydrogen_distribution_shares(demand).iloc[0]

    assert result["compressed_gaseous_pipeline"] == pytest.approx(0.8)
    assert result["on_site_production_share"] == pytest.approx(0.2)
    assert sum(
        result[mode]
        for mode in (
            "compressed_gaseous_truck",
            "compressed_gaseous_pipeline",
            "liquid_hydrogen_truck",
            "liquid_ammonia_ship",
            "liquid_hydrogen_ship",
        )
    ) == pytest.approx(0.8)
    assert result["distribution_rule"] == "default_very_large_demand"
    assert result["distribution_status"] == "ok"
    assert result["distribution_share_total"] == pytest.approx(1)
    assert result["distribution_reason"] == ""


def test_missing_demand_nodes_are_reported_as_incomplete_logistics():
    hydrogen = HydrogenMixin()
    demand = pd.DataFrame(
        [
            {
                "region": "EUR",
                "sector": "Transport",
                "subsector": "Transport",
                "hydrogen_demand_t_per_node_per_year": float("nan"),
            }
        ]
    )

    result = hydrogen._add_hydrogen_distribution_shares(demand).iloc[0]

    assert result["distribution_status"] == "missing_demand_nodes"
    assert pd.isna(result["distribution_rule"])
    assert result["distribution_share_total"] == 0
    assert "No finite positive" in result["distribution_reason"]


def test_finite_demand_without_distribution_rule_fails(monkeypatch):
    hydrogen = HydrogenMixin()
    monkeypatch.setattr(
        "premise.fuels.hydrogen.hydrogen_distribution_rules",
        {
            "rules": [
                {
                    "name": "incomplete_domain",
                    "priority": 1,
                    "match": {},
                    "basis": "hydrogen_demand_t_per_node_per_year",
                    "condition": {"max_demand": 1000},
                    "shares": {"compressed_gaseous_truck": 1},
                }
            ]
        },
    )
    demand = pd.DataFrame(
        [{"hydrogen_demand_t_per_node_per_year": 1000}]
    )

    with pytest.raises(ValueError, match="No hydrogen distribution rule"):
        hydrogen._add_hydrogen_distribution_shares(demand)


def test_distribution_rule_with_unaccounted_share_fails(monkeypatch):
    hydrogen = HydrogenMixin()
    monkeypatch.setattr(
        "premise.fuels.hydrogen.hydrogen_distribution_rules",
        {
            "rules": [
                {
                    "name": "invalid_total",
                    "priority": 1,
                    "match": {},
                    "basis": "hydrogen_demand_t_per_node_per_year",
                    "condition": {},
                    "shares": {"compressed_gaseous_truck": 0.9},
                }
            ]
        },
    )
    demand = pd.DataFrame(
        [{"hydrogen_demand_t_per_node_per_year": 1000}]
    )

    with pytest.raises(ValueError, match="must sum to 1"):
        hydrogen._add_hydrogen_distribution_shares(demand)


def test_transport_market_is_ineligible_without_demand_nodes():
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.system_model = "cut-off"
    hydrogen.regions = ["EUR", "World"]
    hydrogen.iam_data = make_iam_data(
        variables=["Transport - Road - H2"],
        regions=["EUR"],
        values=[[[1]]],
    )
    hydrogen._add_transport_demand_nodes = lambda demand: demand

    hydrogen.set_hydrogen_logistics()
    eligible = hydrogen._eligible_hydrogen_sector_market_regions()
    called_markets = []
    hydrogen.process_and_add_markets = lambda **kwargs: called_markets.append(
        kwargs["name"]
    )
    hydrogen._generate_sector_specific_hydrogen_markets({})

    row = hydrogen.hydrogen_demand_nodes.iloc[0]
    assert row["distribution_status"] == "missing_demand_nodes"
    assert eligible["Transport"] == set()
    assert called_markets == []
    assert "Transport" in hydrogen.skipped_hydrogen_sector_markets


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
    hydrogen.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "year": 2030,
                "region": "EUR",
                "subsector": "Cement",
                "demand_node_type": "cement_plants",
                "demand_nodes": 2,
                "hydrogen_demand_t_per_node_per_year": 1000,
                "distribution_status": "ok",
            }
        ]
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


@pytest.mark.parametrize(
    ("model", "subsector", "final_energy_variable"),
    [
        ("remind", "Steel", "Industry - Steel - All steel - H2"),
        ("remind-eu", "Steel", "Industry - Steel - All steel - H2"),
        ("image", "Steel", "Industry - Steel - All steel - H2"),
        ("message", "Steel", "Industry - Steel - All steel - H2"),
        ("gcam", "Steel", "Industry - Steel - BF/BOF - H2"),
        ("remind", "Cement", "Industry - Cement - H2"),
        ("remind-eu", "Cement", "Industry - Cement - H2"),
        ("image", "Cement", "Industry - Cement - H2"),
        ("message", "Cement", "Industry - Cement - H2"),
        ("gcam", "Cement", "Industry - Cement - H2"),
    ],
)
def test_plant_based_market_is_skipped_without_production_proxy(
    model, subsector, final_energy_variable
):
    hydrogen = HydrogenMixin()
    hydrogen.model = model
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.system_model = "cut-off"
    hydrogen.regions = ["EUR", "World"]
    hydrogen.iam_data = make_iam_data(
        variables=[
            "hydrogen electrolysis",
            final_energy_variable,
        ],
        regions=["EUR"],
        values=[[[1]], [[1]]],
    )
    hydrogen.iam_data.data = hydrogen.iam_data.production_volumes
    hydrogen.set_hydrogen_logistics()

    sector_rows = hydrogen.hydrogen_demand_nodes.loc[
        hydrogen.hydrogen_demand_nodes["subsector"] == subsector
    ]
    assert not sector_rows.empty
    assert sector_rows["demand_node_type"].isna().all()
    assert sector_rows["demand_nodes"].isna().all()

    called_markets = []

    def fake_process_and_add_markets(**kwargs):
        called_markets.append(kwargs["name"])
        return {"EUR"}

    hydrogen.process_and_add_markets = fake_process_and_add_markets

    hydrogen._generate_sector_specific_hydrogen_markets({})

    market_name = {
        "Steel": "market for hydrogen, gaseous, low pressure, for steel",
        "Cement": "market for hydrogen, gaseous, low pressure, for cement",
    }[subsector]
    assert market_name not in called_markets
    assert subsector not in hydrogen.generated_hydrogen_sector_markets
    assert subsector in hydrogen.skipped_hydrogen_sector_markets


def test_final_energy_based_chemical_market_needs_no_production_proxy():
    hydrogen = HydrogenMixin()
    hydrogen.model = "message"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.system_model = "cut-off"
    hydrogen.regions = ["EUR", "World"]
    hydrogen.iam_data = make_iam_data(
        variables=[
            "hydrogen electrolysis",
            "Industry - Chemicals - High-Value Chemicals - H2",
        ],
        regions=["EUR"],
        values=[[[1]], [[1]]],
    )
    hydrogen.iam_data.data = hydrogen.iam_data.production_volumes
    hydrogen.set_hydrogen_logistics()

    called_markets = []

    def fake_process_and_add_markets(**kwargs):
        called_markets.append(kwargs["name"])
        return {"EUR"}

    hydrogen.process_and_add_markets = fake_process_and_add_markets

    hydrogen._generate_sector_specific_hydrogen_markets({})

    assert (
        "market for hydrogen, gaseous, low pressure, for chemicals"
        in called_markets
    )
    assert "Chemicals" in hydrogen.generated_hydrogen_sector_markets


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


def test_on_site_production_share_is_not_a_transport_activity():
    hydrogen = HydrogenMixin()
    hydrogen.year = 2030
    hydrogen.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "year": 2030,
                "region": "EUR",
                "sector": "Transport",
                "subsector": "Transport",
                "hydrogen_demand_t_per_year": 100,
                "compressed_gaseous_pipeline": 0.8,
                "on_site_production_share": 0.2,
            }
        ]
    )

    shares = hydrogen._hydrogen_transport_shares_for_market(
        {
            "name": (
                "market for hydrogen, gaseous, low pressure, for transport"
            ),
            "location": "EUR",
        }
    )

    assert shares == {"compressed_gaseous_pipeline": pytest.approx(0.8)}


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
    hydrogen.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "year": 2030,
                "region": "EUR",
                "subsector": "Steel",
                "demand_node_type": "steel_plants",
                "demand_nodes": 2,
                "hydrogen_demand_t_per_node_per_year": 1000,
                "distribution_status": "ok",
            }
        ]
    )
    called_markets = []
    called_production_volumes = []

    def fake_process_and_add_markets(**kwargs):
        called_markets.append(kwargs["name"])
        called_production_volumes.append(kwargs["production_volumes"])
        return {"EUR"}

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
    assert hydrogen.eligible_hydrogen_sector_market_regions == {
        "Steel": ["EUR"]
    }
    assert hydrogen.uncreated_eligible_hydrogen_sector_market_regions == {}
    assert "Cement" in hydrogen.skipped_hydrogen_sector_markets


def test_eligible_market_is_not_reported_or_used_when_creation_skips_it():
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.system_model = "cut-off"
    hydrogen.regions = ["EUR", "World"]
    hydrogen.geo = GeoStub({"RER": "EUR"})
    hydrogen.iam_data = make_iam_data(
        variables=["hydrogen electrolysis", "Industry - Chemicals - H2"],
        regions=["EUR"],
        values=[[[1]], [[1]]],
    )
    hydrogen.hydrogen_demand_nodes = pd.DataFrame(
        [
            {
                "year": 2030,
                "region": "EUR",
                "sector": "Industrial processes",
                "subsector": "Chemicals",
                "distribution_status": "ok",
            }
        ]
    )
    hydrogen.process_and_add_markets = lambda **_kwargs: set()

    hydrogen._generate_sector_specific_hydrogen_markets({})

    assert hydrogen.eligible_hydrogen_sector_market_regions == {
        "Chemicals": ["EUR"]
    }
    assert hydrogen.generated_hydrogen_sector_market_regions == {}
    assert hydrogen.uncreated_eligible_hydrogen_sector_market_regions == {
        "Chemicals": ["EUR"]
    }
    assert "Chemicals" in hydrogen.skipped_hydrogen_sector_markets
    assert not hydrogen._hydrogen_sector_market_is_available(
        "Chemicals", "RER"
    )
    hydrogen.database = [
        {
            "name": "chemical process consuming market-average hydrogen",
            "reference product": "chemical product",
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

    assert relinked == 0
    assert hydrogen.database[0]["exchanges"][0]["name"] == (
        "market for hydrogen, gaseous, low pressure"
    )


def test_hydrogen_consumer_is_relinked_to_sector_market():
    hydrogen = HydrogenMixin()
    hydrogen.model = "test-model"
    hydrogen.scenario = "test-scenario"
    hydrogen.year = 2030
    hydrogen.regions = ["EUR", "World"]
    hydrogen.geo = GeoStub({"RER": "EUR"})
    hydrogen.generated_hydrogen_sector_market_regions = {
        "Chemicals": ["EUR"]
    }
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
    hydrogen.generated_hydrogen_sector_market_regions = {"Other": ["EUR"]}
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
                "liquid_ammonia_ship": 0.25,
                "liquid_hydrogen_ship": 0.05,
                "on_site_production_share": 0.2,
                "distribution_rule": "test_rule",
                "distribution_status": "ok",
                "distribution_share_total": 1.0,
                "distribution_reason": "",
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
    assert logs[0].endswith("|0.25|0.05|0.2|test_rule|ok|1.0|")


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
