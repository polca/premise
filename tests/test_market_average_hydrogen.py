"""Market-average recipes must reach scenario H2 without changing quantities."""

from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pytest

from premise.fuels.base import Fuels
from premise.inventory_store import CompactInventoryStore
from premise.provenance import ProvenanceCollector
from premise.transformation import BaseTransformation

TARGET = "hydrogen, gaseous, low pressure"
NATIVE_PRODUCTS = (
    "hydrogen, liquid",
    "hydrogen, gaseous",
    "hydrogen, gaseous, medium pressure, merchant",
)


def activity(name, product, location="EUR", **metadata):
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": "kilogram",
        "database": "test",
        "code": name + location,
        "exchanges": [
            {
                "name": name,
                "product": product,
                "location": location,
                "unit": "kilogram",
                "type": "production",
                "amount": 1.0,
            }
        ],
        **metadata,
    }


def make_fuels(
    product,
    *,
    version="3.12",
    model="cutoff",
    backend="legacy",
    consumer_location="EUR",
):
    native = activity("market for " + product, product, "RER")
    prospective = activity("market for " + TARGET, TARGET, regionalized=True)
    consumer = activity(
        "ammonia production, with market-average hydrogen", "ammonia", consumer_location
    )
    consumer["exchanges"].append(
        {
            "name": native["name"],
            "product": product,
            "reference product": product,
            "location": "RER",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 0.176,
            "uncertainty type": 2,
            "loc": np.log(0.176),
            "scale": 0.12,
            "comment": "Source hydrogen coefficient",
            "input": ("test", native["code"]),
        }
    )
    database = [native, prospective, consumer]
    if backend == "compact":
        database = CompactInventoryStore(database)._checkout_materialized()
    fuels = object.__new__(Fuels)
    BaseTransformation.__init__(
        fuels,
        database=database,
        iam_data=SimpleNamespace(regions=["EUR", "World"]),
        model="remind",
        pathway="SSP2-PkBudg1000",
        year=2050,
        version=version,
        system_model=model,
    )
    return fuels, fuels.database[-1]


def generate_hydrogen_markets(fuels):
    # The fixture already supplies a scenario market. Isolate its construction
    # while exercising normalization inside the original hydrogen lifecycle.
    fuels.fuel_map = {}
    fuels.iam_data.production_volumes = None
    fuels.process_and_add_activities = lambda **kwargs: None
    fuels.process_and_add_fuel_markets = lambda **kwargs: None
    fuels._regionalize_hydrogen_activities()


@pytest.mark.parametrize("product", NATIVE_PRODUCTS)
@pytest.mark.parametrize("model", ["cutoff", "consequential", "EN15804"])
@pytest.mark.parametrize("backend", ["legacy", "compact"])
def test_native_hydrogen_reaches_regional_scenario_market(product, model, backend):
    fuels, consumer = make_fuels(product, model=model, backend=backend)
    before = dict(consumer["exchanges"][1])

    # The native market is valid, so geographic relinking alone leaves it static.
    fuels.relink_datasets()
    assert consumer["exchanges"][1]["product"] == product

    generate_hydrogen_markets(fuels)
    translated = dict(consumer["exchanges"][1])
    assert "input" not in translated
    assert translated["reference product"] == TARGET
    for key in ("amount", "uncertainty type", "loc", "scale", "comment"):
        assert translated[key] == before[key]

    fuels.relink_datasets()
    exchange = consumer["exchanges"][1]
    assert (exchange["name"], exchange["product"], exchange["location"]) == (
        "market for " + TARGET,
        TARGET,
        "EUR",
    )
    assert exchange["amount"] == pytest.approx(0.176)
    assert exchange["uncertainty type"] == 2
    assert exchange["loc"] == pytest.approx(before["loc"])
    assert exchange["scale"] == before["scale"]
    assert exchange["comment"] == before["comment"]


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.10", "3.11", "3.12"])
def test_translation_does_not_depend_on_ecoinvent_version(version):
    fuels, consumer = make_fuels("hydrogen, liquid", version=version)
    generate_hydrogen_markets(fuels)
    fuels.relink_datasets()
    assert consumer["exchanges"][1]["location"] == "EUR"
    assert consumer["exchanges"][1]["product"] == TARGET


@pytest.mark.parametrize("product", NATIVE_PRODUCTS)
def test_other_consumers_and_technology_specific_suppliers_are_unchanged(product):
    fuels, consumer = make_fuels(product)
    other = deepcopy(consumer)
    other["name"] = "hydrogen liquefaction for delivery"
    fuels.database.append(other)
    technology = deepcopy(consumer["exchanges"][1])
    technology["name"] = "hydrogen production, from electrolysis"
    consumer["exchanges"].append(technology)
    other_before, technology_before = deepcopy(other), deepcopy(technology)

    generate_hydrogen_markets(fuels)

    assert other == other_before
    assert technology == technology_before


def test_no_scenario_hydrogen_leaves_native_purchase_unchanged():
    fuels, consumer = make_fuels("hydrogen, liquid")
    fuels.remove_from_index(fuels.database[1])
    before = deepcopy(consumer)
    generate_hydrogen_markets(fuels)
    assert consumer == before


@pytest.mark.parametrize("product", NATIVE_PRODUCTS)
@pytest.mark.parametrize("backend", ["legacy", "compact"])
def test_later_regionalization_inherits_scenario_hydrogen(product, backend):
    fuels, source = make_fuels(product, backend=backend, consumer_location="RER")
    generate_hydrogen_markets(fuels)

    # Exercise the real cloning/relinking path after hydrogen normalization.
    BaseTransformation.process_and_add_activities(
        fuels, mapping={"ammonia": [source]}, regions=["EUR"]
    )

    (regional,) = [
        ds
        for ds in fuels.database
        if ds["name"] == source["name"] and ds.get("regionalized", False)
    ]
    (exchange,) = [e for e in regional["exchanges"] if e["type"] == "technosphere"]
    assert exchange["product"] == TARGET
    assert exchange["location"] == "EUR"
    assert exchange["amount"] == pytest.approx(0.176)
    assert exchange["scale"] == 0.12


def test_translation_records_boundary_choice_once():
    fuels, consumer = make_fuels("hydrogen, liquid")
    collector = ProvenanceCollector("hydrogen-relinking")
    with collector.session(("remind", "SSP2-PkBudg1000", 2050), "fuels"):
        generate_hydrogen_markets(fuels)
        first = deepcopy(consumer)
        generate_hydrogen_markets(fuels)
        assert consumer == first
    events = collector.events_for(("remind", "SSP2-PkBudg1000", 2050))
    assert len(events) == 1
    assert events[0].reason_code == "fuels.market_average_hydrogen"
    assert "30-bar pipeline-delivery" in events[0].explanation
