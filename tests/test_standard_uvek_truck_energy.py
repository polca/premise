"""Truck energy filters respect products, units and approved helper boundaries."""

from copy import deepcopy

from premise.inventory.activity_maps import InventorySet


def test_truck_energy_filters_select_fuel_and_not_equipment_or_compression():
    definitions = [
        ("electricity", "Electricity, low voltage, at grid", "kilowatt hour"),
        ("diesel", "Diesel, fossil, at regional storage", "kilogram"),
        (
            "gas",
            "Natural gas, from medium pressure network (0.1-1 bar), at service station",
            "kilogram",
        ),
        ("petrol", "Petrol, fossil, at regional storage", "kilogram"),
        ("lpg", "Liquefied petroleum gas, at service station", "kilogram"),
        ("hydrogen", "hydrogen, gaseous, low pressure", "kilogram"),
    ]
    fuels = {
        key: {
            "name": "Complete supply of " + key,
            "reference product": product,
            "unit": unit,
        }
        for key, product, unit in definitions
    }
    fuels["hydrogen"]["clic provenance"] = {"role": "generated helper"}
    unrelated = [{**row, "unit": "unit"} for row in fuels.values()] + [
        {
            "name": "diesel truck engine",
            "reference product": "engine",
            "unit": "kilogram",
        },
        {
            "name": "Hydrogen chloride, at plant",
            "reference product": "hydrogen chloride",
            "unit": "kilogram",
        },
        {
            **deepcopy(fuels["hydrogen"]),
            "clic provenance": {
                "role": "published",
                "slug": "unbound-published-hydrogen",
            },
        },
        {**fuels["electricity"], "unit": "kilogram"},
    ]
    selected = InventorySet(
        [*fuels.values(), *unrelated], "2026", "remind", family="uvek"
    ).generate_vehicle_fuel_map("truck")
    assert len(selected) == 38
    expected_keys = {
        "battery electric": ["electricity"],
        "fuel cell electric": ["hydrogen"],
        "diesel": ["diesel"],
        "diesel hybrid": ["diesel"],
        "plugin diesel hybrid": ["electricity", "diesel"],
        "diesel plugin hybrid": ["electricity", "diesel"],
        "gasoline": ["petrol"],
        "gasoline hybrid": ["petrol"],
        "compressed gas": ["gas"],
        "liquefied petroleum gas": ["lpg"],
    }
    for role, rows in selected.items():
        assert rows == [fuels[key] for key in expected_keys[role.split(", ")[1]]]
