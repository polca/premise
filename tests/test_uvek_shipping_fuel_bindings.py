"""Native shipping uses kg fuel stocks and explicit published child bindings."""

from copy import deepcopy
from pathlib import Path

import pytest

from premise.inventory.activity_maps import InventorySet
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_requirements import bind_activity_roles, registry

BIO = "biodiesel-production-via-fischer-tropsch-from-forest-product-non-residual-energy-allocation"
AMMONIA = "ammonia-production-with-market-average-hydrogen"


def stock(slug, product, name="Publisher renamed this fuel stock", unit="kilogram"):
    return {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "unit": unit,
        "clic provenance": {"slug": slug},
        "exchanges": [
            {
                "type": "production",
                "name": name,
                "product": product,
                "amount": 1,
                "unit": unit,
            }
        ],
    }


def source_files(*slugs):
    return [
        (Path(n["file"]), None)
        for n in registry()["hybrid_sources"]["nodes"]
        if n["clic_slug"] in slugs
    ]


def fuel_map(records):
    return InventorySet(
        records, version="2026", model="remind", family="uvek"
    ).generate_vehicle_fuel_map("ship")


@pytest.mark.parametrize(
    "slug,product,role",
    [
        (BIO, "biodiesel, from forest residues", "ship, biodiesel"),
        (AMMONIA, "ammonia, anhydrous, liquid", "ship, ammonia"),
    ],
)
def test_published_fuel_needs_its_kg_stock_binding_and_survives_a_title_change(
    slug, product, role
):
    activity = stock(slug, product)
    assert role not in fuel_map([activity])
    bind_activity_roles([activity], source_files(slug), family="uvek")
    assert role in fuel_map([activity])
    assert fuel_map([activity])[role] == [activity]
    assert activity["exchanges"][0]["amount"] == 1


@pytest.mark.parametrize(
    "slug,product",
    [(BIO, "biodiesel, from forest residues"), (AMMONIA, "ammonia, anhydrous, liquid")],
)
def test_stock_reference_unit_changes_still_refuse(slug, product):
    activity = stock(slug, product, unit="megajoule")
    with pytest.raises(ClicInventoryError, match="reference unit changed"):
        bind_activity_roles([activity], source_files(slug), family="uvek")


def test_a_published_name_match_cannot_borrow_another_slug_or_vehicle_binding():
    candidate = stock(
        "unreviewed-fuel",
        "biodiesel, from forest residues",
        "biodiesel production, via fischer-tropsch, from forest product (non-residual), energy allocation",
    )
    candidate["clic provenance"]["role_bindings"] = [
        "ship_fuel_activities.yaml:forest FT biodiesel"
    ]
    assert "ship, biodiesel" not in fuel_map([candidate])
    candidate["clic provenance"].update(
        slug=BIO, role_bindings=["transport_sea_freight.yaml:ship, biodiesel"]
    )
    assert "ship, biodiesel" not in fuel_map([candidate])


def test_native_fuel_filters_exclude_heat_network_infrastructure_and_powered_vehicles():
    records = [
        {
            "name": "Heavy fuel oil, at regional storage",
            "reference product": "Heavy fuel oil, at regional storage",
            "location": "RER",
            "unit": "kilogram",
            "exchanges": [],
        },
        {
            "name": "Heat, heavy fuel oil, at industrial furnace 1MW",
            "reference product": "heat",
            "location": "RER",
            "unit": "megajoule",
            "exchanges": [],
        },
        {
            "name": "Distribution network, electricity, low voltage",
            "reference product": "network",
            "location": "CH",
            "unit": "kilometer",
            "exchanges": [],
        },
        {
            "name": "Operation, passenger car, methanol",
            "reference product": "operation",
            "location": "CH",
            "unit": "kilometer",
            "exchanges": [],
        },
    ]
    before = deepcopy(records)
    selected = fuel_map(records)
    assert selected == {"ship, marine oil": [records[0]]}
    assert records == before


def test_native_container_supplier_is_a_transport_service_and_fuel_stocks_are_not_vehicles():
    native = {
        "name": "Transport, transoceanic container ship",
        "reference product": "Transport, transoceanic container ship",
        "location": "OCE",
        "unit": "ton kilometer",
        "exchanges": [{"type": "production", "amount": 1}],
    }
    fuel = stock(BIO, "biodiesel, from forest residues")
    bind_activity_roles([fuel], source_files(BIO), family="uvek")
    mapping = InventorySet(
        [native, fuel], version="2026", model="remind", family="uvek"
    ).generate_transport_map("ship")
    assert mapping == {"ship, marine oil": [native]}
