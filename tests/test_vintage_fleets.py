import csv
from copy import deepcopy
from types import SimpleNamespace
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import premise.trails as trails
from premise.iam.vintage_fleets import VintageFleets
from premise.temporal_specs import TemporalRegistry, profile_to_temporal_parameters
from premise.trails import TEMPORAL_REGISTRY_DIR, TrailsDataPackage


def vintage_package(
    tmp_path, service_weights=(0.25, 0.75), asset="pv", *, zero_year=None
):
    """Write a small v2 table with deliberately different stock and service shares."""
    rows = [
        {
            "model": "REMIND",
            "scenario": "vintage-test",
            "region": "FRA",
            "asset": asset,
            "year": 2030,
            "cohort_year": cohort,
            "metric": metric,
            "value": weight,
            "unit": "1",
        }
        for metric, weights in [
            ("service_share", service_weights),
            ("stock_share", (0.90, 0.10)),
        ]
        for cohort, weight in zip((2020, 2028), weights, strict=True)
    ]
    if zero_year is not None:
        rows.append(
            {
                **rows[0],
                "year": zero_year,
                "cohort_year": zero_year,
                "metric": "stock",
                "value": 0.0,
                "unit": "GW",
            }
        )
    data = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows), data)
    archive_path = tmp_path / "fleet.zip"
    with ZipFile(archive_path, "w") as archive:
        archive.writestr("vintages.parquet", data.getvalue().to_pybytes())
    return archive_path


def test_vintage_fleets_reject_invalid_weights(tmp_path):
    archive = vintage_package(tmp_path, service_weights=(0.25, 0.55))
    with pytest.raises(
        ValueError, match="Invalid vintage service shares for pv/FRA/2030"
    ):
        VintageFleets(
            archive,
            TEMPORAL_REGISTRY_DIR / "iam_vintage_assets.yaml",
            "remind",
            "vintage-test",
            [2030],
        )


def test_vintage_fleets_distinguishes_zero_stock_from_missing_year(tmp_path):
    archive = vintage_package(tmp_path, zero_year=2020)
    fleets = VintageFleets(
        archive,
        TEMPORAL_REGISTRY_DIR / "iam_vintage_assets.yaml",
        "remind",
        "vintage-test",
        [2020, 2025, 2030],
    )
    assert fleets.zero_stock == {("pv", "FRA", 2020)}
    assert set(fleets.shares) == {("pv", "FRA", 2030)}


@pytest.mark.parametrize(
    ("asset", "caller", "supplier", "product", "unit"),
    [
        (
            "truck-7-5t-bev",
            "transport, freight, lorry, battery electric, 7.5 metric ton",
            "lorry production, battery electric, 7.5 metric ton",
            "truck",
            "unit",
        ),
        (
            "passenger-car-mini-hybrid",
            "transport, passenger car, plugin gasoline hybrid, Mini, EURO-6",
            "Passenger car, plugin gasoline hybrid, Mini, EURO-6d",
            "Passenger car, EURO-6d",
            "unit",
        ),
        (
            "coal-pulverised-no-ccs",
            "electricity production, at lignite-fired power plant, pulverised",
            "market for lignite power plant",
            "lignite power plant",
            "unit",
        ),
        (
            "liquids-gas",
            "diesel production, synthetic, Fischer Tropsch process, from natural gas, energy allocation",
            "gas-to-liquid plant construction",
            "gas-to-liquid plant",
            "unit",
        ),
        (
            "battery-total",
            "electricity supply, from stationary battery (CONT scenario)",
            "market for battery capacity, stationary (CONT scenario)",
            "electricity storage capacity",
            "kilowatt hour",
        ),
        (
            "biochar",
            "biochar, from spruce wood pyrolysis",
            "mini CHP plant construction, common components for heat+electricity",
            "mini CHP plant, common components for heat+electricity",
            "unit",
        ),
        (
            "natural-gas-processing",
            "drying, natural gas",
            "market for natural gas processing plant",
            "natural gas processing plant",
            "unit",
        ),
        (
            "natural-gas-processing",
            "sweetening, natural gas",
            "market for natural gas processing plant",
            "natural gas processing plant",
            "unit",
        ),
        (
            "coal-preparation",
            "hard coal preparation",
            "market for hard coal preparation plant",
            "hard coal preparation plant",
            "unit",
        ),
        (
            "coal-preparation",
            "pulverised lignite production",
            "lignite dust factory construction",
            "lignite dust factory",
            "unit",
        ),
    ],
)
def test_new_fleets_match_their_construction_input(
    tmp_path, asset, caller, supplier, product, unit
):
    fleets = VintageFleets(
        vintage_package(tmp_path, asset=asset),
        TEMPORAL_REGISTRY_DIR / "iam_vintage_assets.yaml",
        "remind",
        "vintage-test",
        [2030],
    )
    operating = {"name": caller, "location": "FRA"}
    exchange = {"name": supplier, "product": product, "unit": unit}
    timing = fleets.construction(None, exchange, operating, 2030, None)
    assert timing["iam_vintage_asset"] == asset
    assert timing["temporal_offsets"] == [-10, -2]
    assert timing["temporal_weights"] == [0.25, 0.75]
    unrelated = {**operating, "name": caller + ", with carbon capture and storage"}
    assert fleets.construction(None, exchange, unrelated, 2030, None) is None


def test_gas_processing_shares_do_not_apply_to_lng_production(tmp_path):
    fleets = VintageFleets(
        vintage_package(tmp_path, asset="natural-gas-processing"),
        TEMPORAL_REGISTRY_DIR / "iam_vintage_assets.yaml",
        "remind",
        "vintage-test",
        [2030],
    )
    exchange = {
        "name": "market for natural gas processing plant",
        "product": "natural gas processing plant",
        "unit": "unit",
    }
    operating = {"name": "natural gas production, liquefied", "location": "FRA"}
    assert fleets.construction(None, exchange, operating, 2030, None) is None


def test_vehicle_fleet_prefers_size_specific_shares_then_uses_pooled_shares(tmp_path):
    fleets = VintageFleets(
        vintage_package(tmp_path, asset="passenger-ldv-4w-bev"),
        TEMPORAL_REGISTRY_DIR / "iam_vintage_assets.yaml",
        "remind",
        "vintage-test",
        [2030],
    )
    operating = {
        "name": "transport, passenger car, battery electric, Small",
        "location": "FRA",
    }
    exchange = {
        "name": "passenger car, battery electric, Small",
        "reference product": "passenger car",
        "unit": "unit",
    }
    assert (
        fleets.construction(None, exchange, operating, 2030, None)["iam_vintage_asset"]
        == "passenger-ldv-4w-bev"
    )
    fleets.shares["passenger-car-small-bev", "FRA", 2030] = ([-4], [1])
    timing = fleets.construction(None, exchange, operating, 2030, None)
    assert timing["iam_vintage_asset"] == "passenger-car-small-bev"
    assert timing["temporal_offsets"] == [-4]


@pytest.mark.parametrize("retirement_first", [False, True])
def test_trails_prefers_vintage_service_shares_for_matched_fleets(
    tmp_path, monkeypatch, retirement_first
):
    capital = {
        "type": "technosphere",
        "name": "market for photovoltaic installation, 3 kWp",
        "product": "photovoltaic installation, 3 kWp",
        "location": "GLO",
        "unit": "unit",
        "amount": 0.002,
    }
    treatment = {
        "type": "technosphere",
        "name": "dismantling test installation",
        "product": "dismantling service",
        "location": "GLO",
        "unit": "unit",
        "amount": 0.003,
    }
    factory = {
        "type": "technosphere",
        "name": "factory",
        "product": "factory",
        "location": "GLO",
        "unit": "unit",
        "amount": 0.004,
    }
    activity = {
        "name": "electricity production, photovoltaic, 3 kWp",
        "reference product": "electricity, low voltage",
        "location": "FRA",
        "unit": "kilowatt hour",
        "exchanges": (
            [treatment, capital] if retirement_first else [capital, treatment]
        )
        + [factory],
    }
    caller_key = activity["name"], activity["reference product"]
    retirement_key = treatment["name"], treatment["product"]
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.source_version = "3.12"
    obj.temporal_registry = TemporalRegistry.from_directory(TEMPORAL_REGISTRY_DIR)
    obj.vintage_package_path = vintage_package(tmp_path)
    obj.stock_asset_params = {
        ("factory", "factory"): {
            "temporal_distribution": 6,
            "temporal_offsets": [-1.0],
            "temporal_weights": [1.0],
        }
    }
    obj.end_of_life_suppliers = {retirement_key}
    obj.retirement_asset_links = {
        retirement_key: ((capital["name"], capital["product"]),)
    }
    obj.dataset_lifetimes = {caller_key: 30}
    obj.lifecycle_timing = {
        caller_key: {
            "timing_reference": "existing_fleet",
            "lifetime": 30,
            "age_offsets": [-1],
            "age_weights": [1],
            "assumption": "Explicit stationary-stock proxy",
        }
    }
    missing_region = deepcopy(activity)
    missing_region["location"] = "USA"
    obj.datapackage = SimpleNamespace(
        scenarios=[
            {
                "model": "remind",
                "pathway": "vintage-test",
                "year": 2030,
                "database": [activity, missing_region],
            },
            {
                "model": "remind",
                "pathway": "vintage-test",
                "year": 2040,
                "database": [deepcopy(activity)],
            },
        ],
        database="db",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    assert capital["temporal_distribution"] == 6
    assert capital["temporal_offsets"] == [-10.0, -2.0]
    assert capital["temporal_weights"] == [0.25, 0.75]
    assert treatment["temporal_offsets"] == [20.0, 28.0]
    assert treatment["temporal_weights"] == [0.25, 0.75]
    assert capital["amount"] == 0.002 and treatment["amount"] == 0.003
    assert factory["temporal_offsets"] == [-1.0]
    static = profile_to_temporal_parameters(
        obj.temporal_registry.profiles[
            "eia-us-solar-photovoltaic-construction-vintage-2025"
        ]
    )
    missing_year = obj.datapackage.scenarios[1]["database"][0]
    for unavailable in (missing_region, missing_year):
        construction = next(
            exchange
            for exchange in unavailable["exchanges"]
            if exchange["name"] == capital["name"]
        )
        assert construction["temporal_weights"] == static["temporal_weights"]

    with (tmp_path / "trails_temp" / "temporal_selector_matches.csv").open() as stream:
        overrides = [row for row in csv.DictReader(stream) if row["iam_vintage_asset"]]
    assert len(overrides) == 1
    assert (overrides[0]["iam_vintage_asset"], overrides[0]["iam_vintage_region"]) == (
        "pv",
        "FRA",
    )


@pytest.mark.parametrize(
    "asset, caller, supplier, product",
    [
        (
            "heat-pump",
            "heat production, water-water heat pump, industrial, 100kW",
            "heat pump production, industrial, water-water, 100 kW",
            "heat pump, water-water, 100kW",
        ),
        (
            "direct-air-capture",
            "carbon dioxide, captured and stored, with a sorbent-based direct air capture system, 100ktCO2",
            "direct air capture system, sorbent-based, 100ktCO2",
            "direct air capture system",
        ),
    ],
)
def test_trails_matches_construction_without_static_profile(
    tmp_path, monkeypatch, asset, caller, supplier, product
):
    capital = {
        "type": "technosphere",
        "name": supplier,
        "product": product,
        "location": "GLO",
        "unit": "unit",
        "amount": 0.002,
    }
    wrong_product = dict(capital, product="other equipment")
    wrong_unit = dict(capital, unit="kilogram")
    activity = {
        "name": caller,
        "reference product": "operating service",
        "location": "FRA",
        "exchanges": [capital, wrong_product, wrong_unit],
    }
    wrong_caller = deepcopy(activity)
    wrong_caller["name"] = "unrelated activity"
    missing_region = deepcopy(activity)
    missing_region["location"] = "USA"
    missing_year = deepcopy(activity)
    obj = TrailsDataPackage.__new__(TrailsDataPackage)
    obj.source_version = "3.12"
    obj.temporal_registry = TemporalRegistry.from_directory(TEMPORAL_REGISTRY_DIR)
    obj.vintage_package_path = vintage_package(tmp_path, asset=asset)
    obj.datapackage = SimpleNamespace(
        scenarios=[
            {
                "model": "remind",
                "pathway": "vintage-test",
                "year": year,
                "database": database,
            }
            for year, database in [
                (2030, [activity, wrong_caller, missing_region]),
                (2040, [missing_year]),
            ]
        ],
        database="db",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(trails, "load_database", lambda scenario, _: scenario)
    monkeypatch.setattr(trails, "dump_database", lambda scenario: scenario)

    obj.add_temporal_distributions()

    assert capital["temporal_offsets"] == [-10.0, -2.0]
    assert capital["temporal_weights"] == [0.25, 0.75]
    assert capital["amount"] == 0.002
    for exchange in (wrong_product, wrong_unit):
        assert "temporal_weights" not in exchange
    for unmatched in (wrong_caller, missing_region, missing_year):
        assert "temporal_weights" not in unmatched["exchanges"][0]
    with (tmp_path / "trails_temp" / "temporal_selector_matches.csv").open() as stream:
        [row] = list(csv.DictReader(stream))
    assert row["assignment_kind"] == "iam-vintage"
    assert row["iam_vintage_asset"] == asset
    assert row["iam_vintage_region"] == "FRA"


@pytest.mark.parametrize(
    "caller, supplier, product, unit, expected",
    [
        (
            "electricity production, at hard coal-fired IGCC power plant",
            "hard coal IGCC power plant 450MW",
            "hard coal IGCC power plant 450MW",
            "unit",
            "coal-igcc",
        ),
        (
            "electricity production, at hard coal-fired IGCC power plant, pre, pipeline 200km, storage 1000m",
            "hard coal IGCC power plant 450MW",
            "hard coal IGCC power plant 450MW",
            "unit",
            "coal-igcc-ccs",
        ),
        (
            "electricity production, at hard coal-fired power plant, post, pipeline 200km, storage 1000m",
            "market for hard coal power plant",
            "hard coal pulverised power plant 500MW",
            "unit",
            None,
        ),
        (
            "hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity",
            "electrolyzer production, 1MWe, PEM, Balance of Plant",
            "electrolyzer, 1MWe, PEM, Balance of Plant",
            "unit",
            "hydrogen-electrolysis",
        ),
        (
            "hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity",
            "electrolyzer production, 1MWe, PEM, Stack",
            "electrolyzer, 1MWe, PEM, Stack",
            "unit",
            None,
        ),
        (
            "electricity supply, from stationary battery (CONT scenario)",
            "market for battery capacity, stationary (CONT scenario)",
            "electricity storage capacity",
            "kilowatt hour",
            "battery-total",
        ),
    ],
)
def test_direct_construction_rules_distinguish_shared_inputs(
    caller, supplier, product, unit, expected
):
    fleets = VintageFleets(
        None, TEMPORAL_REGISTRY_DIR / "iam_vintage_assets.yaml", "remind", "test", []
    )
    matches = fleets._exchange_assets(
        {"name": supplier, "reference product": product, "unit": unit},
        {"name": caller},
    )
    assert matches == ((expected,) if expected is not None else ())
