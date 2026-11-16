"""Native complete rail services replace only their own fuel-class imports."""

import pytest

from premise.core.filesystem_constants import VARIABLES_DIR
from premise.inventory.activity_maps import InventorySet, get_mapping
from premise.inventory.clic_requirements import required_roots
from premise.transport.transport import Transport, get_vehicles_mapping


def test_native_rail_filters_select_complete_services_without_operating_parts():
    electric = "Transport, freight, rail, electricity only"
    diesel = "Transport, freight, rail, diesel, with particle filter"
    records = [
        {
            "name": electric,
            "reference product": electric,
            "unit": "ton kilometer",
            "location": location,
        }
        for location in ("RER", "AT", "DE", "FR", "IT")
    ] + [
        {
            "name": diesel,
            "reference product": diesel,
            "unit": "ton kilometer",
            "location": "CH",
        }
    ]
    other_names = [
        "Transport, freight, rail",
        "Transport, freight, rail, electricity with shunting, Betrieb",
        "Transport, freight, rail, electricity with shunting, Infrastruktur",
        "Transport, freight, rail, diesel, without particle filter",
    ]
    unrelated = [
        {**records[0], "name": name, "reference product": name} for name in other_names
    ] + [{**records[0], "unit": "person kilometer"}]
    selected = InventorySet(
        [*records, *unrelated], "2026", "remind", family="uvek"
    ).generate_transport_map("train")
    assert set(selected) == {"train, electric", "train, diesel-electric"}
    assert selected["train, electric"] == records[:5]
    assert selected["train, diesel-electric"] == records[5:]


def test_required_hydrogen_train_geographies_remain_on_the_clic_path():
    scenario = [{"model": "remind"}]
    roots = required_roots(["trains"], scenario, family="uvek")
    assert len(roots) == 6
    assert all(
        slug.startswith("transport-freight-train-fuel-cell-hydrogen") for slug in roots
    )
    assert len(required_roots(["trains"], scenario, family="ecoinvent")) == 18
    definitions = get_mapping(
        VARIABLES_DIR / "transport_rail_freight.yaml", "uvek_aliases", model="remind"
    )
    assert definitions["train, fuel cell"].get("use_legacy_templates", True)
    for role in ("train, electric", "train, diesel-electric"):
        assert definitions[role]["use_legacy_templates"] is False
        assert definitions[role]["native_reference_amount"] == 1
        assert definitions[role]["fltr"]["unit"] == "ton kilometer"


def test_rail_energy_filters_preserve_voltage_and_diesel_delivery_interface():
    names = [
        "Electricity, medium voltage, production ENTSO-E, at grid",
        "Electricity, medium voltage, railway, at grid",
        "Electricity, medium voltage, DB, at grid",
        "Electricity, medium voltage, ÖBB, at grid",
        "market group for electricity, medium voltage",
        "market for electricity, medium voltage",
    ]
    grids = [{"name": n, "unit": "kilowatt hour"} for n in names]
    diesel = {"name": "Diesel, at regional storage", "unit": "kilogram"}
    hydrogen = {
        "name": "Hydrogen production, steam methane reforming of natural gas, 25 bar",
        "unit": "kilogram",
    }
    unrelated = [
        {"name": "market group for electricity, low voltage", "unit": "kilowatt hour"},
        {**diesel, "unit": "unit"},
        {"name": "Fuel supply for diesel vehicles", "unit": "kilogram"},
        {"name": "Hydrogen chloride, at plant", "unit": "kilogram"},
        {**hydrogen, "unit": "unit"},
    ]
    selected = InventorySet(
        [*grids, diesel, hydrogen, *unrelated], "2026", "remind", family="uvek"
    ).generate_vehicle_fuel_map("train")
    assert selected["train, electric"] == grids
    assert selected["train, diesel-electric"] == [diesel]
    assert selected["train, fuel cell"] == [hydrogen]


def test_native_generic_freight_users_follow_the_shared_relinker():
    owner = Transport.__new__(Transport)
    owner.model = "remind"
    owner.vehicle_type = "train"
    owner.mapping = get_vehicles_mapping()
    original = {
        "type": "technosphere",
        "name": "Transport, freight, rail",
        "product": "Transport, freight, rail",
        "location": "RER",
        "unit": "ton kilometer",
        "amount": -0.25,
        "uncertainty type": 3,
        "loc": -0.25,
        "scale": 0.02,
        "input": ("source", "old"),
    }
    material = {"unit": "kilogram", "exchanges": [dict(original)]}
    transport = {"unit": "ton kilometer", "exchanges": [dict(original)]}
    passenger = {
        "unit": "kilogram",
        "exchanges": [{**original, "unit": "person kilometer"}],
    }
    owner.database = [material, transport, passenger]
    owner._find_available_transport_market = lambda **kwargs: (
        kwargs["market_name"],
        "EUR",
    )
    owner.relink_transport_datasets()
    linked = material["exchanges"][0]
    assert linked["name"] == "market for transport, freight, train"
    assert linked["product"] == "transport, freight, train"
    assert linked["location"] == "EUR" and "input" not in linked
    for key in ("amount", "unit", "uncertainty type", "loc", "scale"):
        assert linked[key] == original[key]
    assert transport["exchanges"] == [original]
    assert passenger["exchanges"][0]["unit"] == "person kilometer"
    assert passenger["exchanges"][0]["name"] == original["name"]


@pytest.mark.parametrize(
    "locations, duplicate", [(("IT", "AT"), False), (("IT", "IT"), True)]
)
def test_relinking_keeps_distinct_origins_without_hiding_exact_duplicates(
    locations, duplicate
):
    from test_validation_framework import rule_issues, validate

    owner = Transport.__new__(Transport)
    owner.model = "remind"
    owner.vehicle_type = "train"
    owner.mapping = get_vehicles_mapping()
    owner._find_available_transport_market = lambda **kwargs: (
        "market for transport, freight, train",
        "EUR",
    )
    owner.database = [
        {
            "name": "material",
            "reference product": "material",
            "location": "EUR",
            "unit": "kilogram",
            "database": "test-db",
            "code": "material",
            "exchanges": [
                {
                    "name": "material",
                    "product": "material",
                    "location": "EUR",
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1,
                },
                *[
                    {
                        "name": "Transport, freight, rail",
                        "product": "Transport, freight, rail",
                        "location": location,
                        "unit": "ton kilometer",
                        "type": "technosphere",
                        "amount": 0.1,
                        "uncertainty type": 3,
                        "loc": 0.1,
                        "scale": 0.02,
                    }
                    for location in locations
                ],
            ],
        },
        {
            "name": "market for transport, freight, train",
            "reference product": "transport, freight, train",
            "location": "EUR",
            "unit": "ton kilometer",
            "database": "test-db",
            "code": "supplier",
            "exchanges": [
                {
                    "name": "market for transport, freight, train",
                    "product": "transport, freight, train",
                    "location": "EUR",
                    "unit": "ton kilometer",
                    "type": "production",
                    "amount": 1,
                }
            ],
        },
    ]
    owner.relink_transport_datasets()
    edges = owner.database[0]["exchanges"][1:]
    assert all(e["amount"] == e["loc"] == 0.1 and e["scale"] == 0.02 for e in edges)
    assert (edges[0]["comment"] == edges[1]["comment"]) is duplicate
    report = validate(owner.database)
    assert bool(rule_issues(report, "GRAPH.DUPLICATE_SUPPLIER")) is duplicate


def test_shared_train_update_refreshes_native_carbon_origin_and_keeps_recorded_total(
    monkeypatch,
):
    from copy import deepcopy
    from types import SimpleNamespace
    from premise.fuels.properties import PROPERTIES_KEY
    from premise.inventory.inventory_store import get_scenario_inventory
    import premise.inventory.inventory_imports as imports
    import premise.transport.transport as module

    fuel_name = "Diesel, at regional storage"
    rail_name = "Transport, freight, rail, diesel, with particle filter"
    fuel = {
        "name": fuel_name,
        "reference product": fuel_name,
        "location": "EUR",
        "unit": "kilogram",
        PROPERTIES_KEY: {
            "reference unit": "kilogram",
            "canonical market name": "market for diesel",
            "fossil CO2 kg per unit": 1.6,
            "non-fossil CO2 kg per unit": 1.6,
            "unresolved CO2 kg per unit": 0,
        },
        "exchanges": [],
    }
    rail = {
        "name": rail_name,
        "reference product": rail_name,
        "location": "EUR",
        "unit": "ton kilometer",
        "exchanges": [
            {
                "name": fuel_name,
                "product": fuel_name,
                "location": "EUR",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 0.1,
            },
            {
                "name": "Carbon dioxide, fossil",
                "categories": ("air", "unspecified"),
                "unit": "kilogram",
                "type": "biosphere",
                "amount": 0.2,
            },
        ],
    }
    for activity in (fuel, rail):
        activity["exchanges"].insert(
            0,
            {
                "name": activity["name"],
                "product": activity["reference product"],
                "location": activity["location"],
                "unit": activity["unit"],
                "type": "production",
                "amount": 1,
            },
        )
    unmapped = deepcopy(rail)
    unmapped_name = "Transport, freight, rail, diesel, without particle filter"
    unmapped.update(name=unmapped_name, **{"reference product": unmapped_name})
    unmapped["exchanges"][0].update(name=unmapped_name, product=unmapped_name)

    class PreparedTransport:
        def __init__(self, **kwargs):
            self.database = kwargs["database"]
            self.vehicle_map = {"train, diesel-electric": [rail]}
            self.cache = {}
            self.index = {}

        def regionalize_transport_datasets(self):
            pass

        def create_vehicle_markets(self):
            pass

        def relink_transport_datasets(self):
            pass

    monkeypatch.setattr(module, "Transport", PreparedTransport)
    monkeypatch.setattr(
        imports,
        "get_biosphere_code",
        lambda version: {
            (name, "air", "unspecified", "kilogram"): code
            for name, code in [
                ("Carbon dioxide, fossil", "fossil"),
                ("Carbon dioxide, non-fossil", "biogenic"),
            ]
        },
    )
    iam = SimpleNamespace(
        **{
            name: None
            for name in [
                "passenger_car_fleet",
                "road_freight_fleet",
                "bus_fleet",
                "two_wheelers_fleet",
                "sea_freight_fleet",
            ]
        },
        rail_freight_fleet=True,
    )
    scenario = {
        "database": [fuel, rail, unmapped],
        "iam data": iam,
        "model": "remind",
        "pathway": "test",
        "year": 2050,
    }
    result = module._update_vehicles(
        scenario, "train", "2026", "cutoff", family="uvek", biosphere_version="3.10"
    )
    updated = next(a for a in get_scenario_inventory(result) if a["name"] == rail_name)
    carbon = {
        e["name"]: e["amount"] for e in updated["exchanges"] if e["type"] == "biosphere"
    }
    assert carbon == {
        "Carbon dioxide, fossil": pytest.approx(0.1),
        "Carbon dioxide, non-fossil": pytest.approx(0.1),
    }
    assert sum(carbon.values()) == pytest.approx(0.2)
    control = next(
        a for a in get_scenario_inventory(result) if a["name"] == unmapped_name
    )
    assert [e["amount"] for e in control["exchanges"] if e["type"] == "biosphere"] == [
        0.2
    ]
