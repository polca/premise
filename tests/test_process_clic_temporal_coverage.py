"""Build reference scenarios separately so other scenarios cannot supply their inventories."""

import os

import bw2data as bd
import bw2io
import pytest
from dotenv import load_dotenv

from premise import NewDatabase
from premise.inventory.inventory_store import get_scenario_inventory

load_dotenv()

pytestmark = pytest.mark.slow
SOURCE = "ecoinvent-3.11-cutoff"


@pytest.fixture(scope="module")
def build_args(tmp_path_factory):
    key = os.environ.get("IAM_FILES_KEY")
    if not key:
        pytest.skip("An IAM decryption key is required")
    bd.projects.set_current(SOURCE)
    biosphere = "ecoinvent-3.11-biosphere"
    if SOURCE not in bd.databases:
        username = os.environ.get("EI_USERNAME")
        password = os.environ.get("EI_PASSWORD")
        if not (username and password):
            pytest.skip("An ecoinvent 3.11 import or credentials are required")
        bw2io.import_ecoinvent_release(
            "3.11",
            "cutoff",
            username=username,
            password=password,
            biosphere_name=biosphere,
        )
    (biosphere,) = bd.databases[SOURCE]["depends"]
    clic_options = {"cache_dir": tmp_path_factory.mktemp("clic-temporal")}
    if archive := os.environ.get("PREMISE_CLIC_ARCHIVE"):
        clic_options.update(archive_path=archive, offline=True)
    return dict(
        source_db=SOURCE,
        source_version="3.11",
        system_model="cutoff",
        biosphere_name=biosphere,
        key=key.encode(),
        generate_reports=False,
        inventory_backend="compact",
        clic_options=clic_options,
    )


def test_standalone_full_build_loads_the_historical_heat_supplier(build_args):
    db = NewDatabase(
        scenarios=[{"model": "remind", "pathway": "SSP2-NPi", "year": 2040}],
        **build_args,
    )

    db.update()

    scenario = db.scenarios[0]
    role = "heat, buildings, from hydrogen boiler"
    volumes = scenario["iam data"].buildings_heat_end_use.sel(
        variables=role, region="CAZ"
    )
    assert volumes.sel(year=2040).item() == 0
    assert volumes.sel(year=2025).item() > 0
    factors = scenario["heat diagnostics"]["buildings_end_use"]["conversion factors"]
    assert any(row["region"] == "CAZ" and row["technology"] == role for row in factors)
    db.write_db_to_brightway("test-clic-historical-heat")
    market = bd.get_node(
        database="test-clic-historical-heat",
        name="market for heat, for buildings",
        location="CAZ",
    )
    assert len(list(market.technosphere())) > 0
    assert all(
        "combustion of hydrogen using boiler" not in exchange.input["name"]
        for exchange in market.technosphere()
    )


@pytest.mark.parametrize(
    "model,pathway,year,region,technology,minimum_share",
    [
        ("image", "SSP2-VLHO", 2040, "CAN", "Biomass IGCC CCS", 0.14),
        ("remind", "SSP2-PkBudg1000", 2020, "USA", "Storage, Hydrogen", 0.008),
    ],
)
def test_standalone_electricity_build_supplies_future_only_technologies(
    build_args, model, pathway, year, region, technology, minimum_share
):
    db = NewDatabase(
        scenarios=[{"model": model, "pathway": pathway, "year": year}],
        **build_args,
    )

    db.update(["electricity"], persist=False)

    scenario = db.scenarios[0]
    mix = scenario["iam data"].electricity_mix
    assert mix.sel(region=region, variables=technology, year=year).item() == 0
    providers = {
        (provider["name"], provider["reference product"], provider["location"])
        for provider in scenario["mapping"]["electricity"][technology]
    }
    name = "market group for electricity, high voltage, 60-year period"
    (market,) = (
        ds
        for ds in get_scenario_inventory(scenario)
        if (ds["name"], ds["location"]) == (name, region)
    )
    observed = sum(
        exc["amount"]
        for exc in market["exchanges"]
        if exc["type"] == "technosphere"
        and (exc["name"], exc["product"], exc["location"]) in providers
    )
    assert observed >= minimum_share
