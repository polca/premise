"""Native passenger-km bus services use the shared fleet and efficiency update."""

from copy import deepcopy
import warnings

import pytest

from premise.core.filesystem_constants import VARIABLES_DIR
from premise.inventory.activity_maps import InventorySet, get_mapping
from premise.inventory.clic_requirements import required_roots
from premise.transport.transport import Transport


def definitions():
    return get_mapping(
        VARIABLES_DIR / "transport_bus.yaml", "uvek_aliases", model="remind"
    )


def source_records():
    return [
        {
            **row["fltr"],
            "location": "CH",
            "comment": "Original source documentation.",
            "exchanges": [
                {"type": "production", "amount": 1, "unit": "person kilometer"},
                {"type": "technosphere", "amount": 0.03, "unit": "kilogram"},
            ],
        }
        for row in definitions().values()
    ]


def test_four_native_services_exclude_other_sizes_vintages_and_reference_units():
    records = source_records()
    other_size = deepcopy(records[0])
    other_size["name"] = other_size["name"].replace("13m", "18m")
    other_year = deepcopy(records[0])
    other_year["name"] = other_year["name"].replace("2020", "2010")
    manufacturing = deepcopy(records[0])
    manufacturing["unit"] = "unit"
    graph = [*records, other_size, other_year, manufacturing]
    original = deepcopy(graph)
    selected = InventorySet(
        graph, "2026", "remind", family="uvek"
    ).generate_transport_map("bus")
    assert set(selected) == {
        "bus, diesel",
        "bus, compressed gas",
        "bus, fuel cell",
        "bus, battery electric",
    }
    for row in selected.values():
        assert len(row) == 1 and row[0] in records
        assert row[0]["unit"] == "person kilometer"
    assert graph == original


def test_native_fleet_does_not_request_legacy_bus_inventories():
    scenario = {"model": "remind"}
    assert required_roots(["buses"], [scenario], family="uvek") == ()
    assert required_roots(["buses"], [scenario], family="ecoinvent")
    assert all(row["use_legacy_templates"] is False for row in definitions().values())


def test_electricity_filter_handles_prior_electricity_update_at_same_voltage():
    names = [
        "Electricity, low voltage, at grid",
        "market group for electricity, low voltage",
        "market for electricity, low voltage",
        "market group for electricity, medium voltage",
        "market group for electricity, low voltage, unrelated equipment",
    ]
    records = [
        {"name": name, "unit": "kilowatt hour", "location": "EUR"} for name in names
    ]
    equipment = {**records[0], "unit": "unit"}
    selected = InventorySet(
        [*records, equipment], "2026", "remind", family="uvek"
    ).generate_vehicle_fuel_map("bus")
    assert selected["bus, battery electric"] == records[:3]


def owner(family="uvek"):
    records = source_records()
    model = Transport.__new__(Transport)
    model.family = family
    model.model = "remind"
    model.vehicle_type = "bus"
    model.mapping = {"bus": {"aliases_file": "transport_bus.yaml"}}
    model.vehicle_map = {
        role: [record] for role, record in zip(definitions(), records, strict=True)
    }
    return model


def test_source_assumptions_travel_with_copies_without_rescaling_inventory():
    model = owner()
    original = deepcopy(model.vehicle_map)
    with pytest.warns(UserWarning, match="source assumptions") as caught:
        model.annotate_source_assumptions()
    assert len(caught) == 4
    for role, records in model.vehicle_map.items():
        updated = deepcopy(records[0])
        comment = updated.pop("comment")
        before = deepcopy(original[role][0])
        before.pop("comment")
        assert updated == before
        assert comment.startswith("Original source documentation.")
        assert all(
            message in comment for message in definitions()[role]["native_limitations"]
        )
        regional = deepcopy(records[0])
        regional["location"] = "EUR"
        assert regional["comment"] == comment
    prepared = deepcopy(model.vehicle_map)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model.annotate_source_assumptions()
    assert caught == [] and model.vehicle_map == prepared


def test_ecoinvent_path_retains_its_existing_comments_and_inventories():
    model = owner("ecoinvent")
    original = deepcopy(model.vehicle_map)
    model.annotate_source_assumptions()
    assert model.vehicle_map == original
