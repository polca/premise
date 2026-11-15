"""Native complete rail services replace only their own fuel-class imports."""

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
    unrelated = [
        {"name": "market group for electricity, low voltage", "unit": "kilowatt hour"},
        {**diesel, "unit": "unit"},
        {"name": "Fuel supply for diesel vehicles", "unit": "kilogram"},
    ]
    selected = InventorySet(
        [*grids, diesel, *unrelated], "2026", "remind", family="uvek"
    ).generate_vehicle_fuel_map("train")
    assert selected["train, electric"] == grids
    assert selected["train, diesel-electric"] == [diesel]


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
