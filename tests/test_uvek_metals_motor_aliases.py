"""Vehicle motor identities use their documented kilogram reference basis."""

from copy import deepcopy

import pytest

from premise.inventory.activity_maps import InventorySet
from premise.materials.metals_rules import (
    load_material_rules,
    load_technology_conversions,
)
from test_pem_iridium_provider import model

NAMES = [
    "Electric motor, for electric scooter",
    "Electric motor, electric vehicle, at plant",
    "Electric motor, for electric passenger car",
]


def motor(name):
    return {
        "name": name,
        "reference product": name,
        "location": "RER",
        "unit": "kilogram",
        "exchanges": [
            {"type": "production", "unit": "kilogram", "amount": 1},
            {
                "name": "existing magnet",
                "product": "permanent magnet",
                "location": "RER",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 0.2,
            },
        ],
    }


def test_exact_motor_product_pairs_exclude_industrial_motors_and_complete_vehicles():
    motors = [motor(name) for name in NAMES]
    mismatched = deepcopy(motors[0])
    mismatched["reference product"] = NAMES[1]
    whole_vehicle = motor("Passenger car, battery electric")
    industrial = motor("Electric motor, three phase, industrial")
    whole_device = deepcopy(motors[0])
    whole_device["unit"] = "unit"
    graph = [*motors, mismatched, whole_vehicle, industrial, whole_device]
    before = deepcopy(graph)
    selected = InventorySet(
        graph, "2026", "remind", family="uvek"
    ).generate_metals_activities_map()
    assert selected["EV"] == motors
    assert graph == before


@pytest.mark.parametrize("name", NAMES)
def test_motor_conversion_never_uses_example_device_mass_or_rated_power(name):
    (conversion,) = [
        c for c in load_technology_conversions() if c.activity_name == name
    ]
    assert conversion.ecoinvent_unit == "kilogram"
    assert conversion.intensity_unit == "kg metal/kg motor"
    assert conversion.factor == 1


@pytest.mark.parametrize("name", NAMES)
def test_existing_whole_vehicle_scope_guard_preserves_native_motor_materials(name):
    dataset = motor(name)
    before = deepcopy(dataset)
    runtime = model([], version="2026")
    runtime.material_rules_by_technology = {
        "EV": [
            r
            for r in load_material_rules().enabled_rules
            if r.technology == "EV"
            and r.target.kind == "mapped_activity"
            and r.preserve_reason
        ]
    }
    runtime.update_metal_use(dataset, "EV")
    assert dataset == before
    assert runtime.material_decisions
    assert all(
        d["reason code"] == "metals.material_rule.component_scope_unverified"
        for d in runtime.material_decisions
    )
    assert runtime.material_update_diagnostics == []
