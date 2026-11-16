"""Native car aliases select complete services and preserve missing classes."""

import json
from pathlib import Path

from premise.core.filesystem_constants import VARIABLES_DIR
from premise.inventory.activity_maps import InventorySet, get_mapping
from premise.inventory.clic_requirements import required_roots


def test_native_car_filters_select_reviewed_complete_services_only():
    review = json.loads(
        (
            Path(__file__).parents[1]
            / "docs/development/uvek-native-car-alias-review.json"
        ).read_text()
    )
    sources = {
        row["role"]: [s["identity"] for s in row["sources"]]
        for row in review["native_roles"]
    }
    records = [record for rows in sources.values() for record in rows]
    unrelated = [
        {**record, **replacement}
        for record in records
        for replacement in (
            {"unit": "person kilometer"},
            {"reference product": "vehicle production"},
            {"name": record["name"].replace("2021", "2040")},
            {"name": record["name"] + ", Betrieb"},
        )
    ]
    selected = InventorySet(
        [*records, *unrelated], "2026", "remind", family="uvek"
    ).generate_transport_map("car")
    assert selected == sources
    assert len(selected) == 21 and sum(map(len, selected.values())) == 33


def test_absent_mini_and_van_classes_keep_their_clic_imports():
    definitions = get_mapping(
        VARIABLES_DIR / "transport_passenger_cars.yaml", "uvek_aliases", "remind"
    )
    retained = {
        role
        for role, definition in definitions.items()
        if definition.get("use_legacy_templates", True)
    }
    assert len(definitions) == 30 and len(retained) == 9
    assert all(role.endswith(", van") or role.endswith(", mini") for role in retained)
    assert "passenger car, battery electric, mini" not in retained
    assert all(not definitions[role].get("fltr") for role in retained)
    roots = required_roots(["cars"], [{"model": "remind"}], family="uvek")
    assert len(roots) == 9
    assert len(required_roots(["cars"], [{"model": "remind"}], family="ecoinvent")) > 9


def test_native_car_fuel_filters_do_not_cross_units_or_select_equipment():
    petrol = {"name": "Fuel supply for gasoline vehicles", "unit": "kilogram"}
    gas = {"name": "Fuel supply for gas vehicles", "unit": "kilogram"}
    electricity = {
        "name": "Electricity, low voltage, at grid",
        "unit": "kilowatt hour",
    }
    hydrogen = {
        "name": "Hydrogen, gaseous, 700 bar, from SMR of NG, at fuelling station",
        "unit": "kilogram",
    }
    unrelated = [
        {**petrol, "unit": "kilowatt hour"},
        {**electricity, "unit": "kilogram"},
        {**hydrogen, "unit": "unit"},
        {"name": "Hydrogen chloride, at plant", "unit": "kilogram"},
        {"name": "Electric motor, passenger car", "unit": "kilogram"},
    ]
    selected = InventorySet(
        [petrol, gas, electricity, hydrogen, *unrelated],
        "2026",
        "remind",
        family="uvek",
    ).generate_vehicle_fuel_map("car")
    for role, rows in selected.items():
        if ", plugin gasoline hybrid," in role:
            assert rows == [petrol, electricity]
        elif ", battery electric," in role:
            assert rows == [electricity]
        elif ", fuel cell electric," in role:
            assert rows == [hydrogen]
        elif ", compressed gas," in role:
            assert rows == [gas]
        else:
            assert ", gasoline," in role and rows == [petrol]


def test_retained_car_classes_match_complete_fossil_petrol_and_medium_pressure_cng():
    from premise.transport.vehicle_energy import canonical_vehicle_energy_product

    petrol = {"name": "Petrol, fossil, at regional storage", "unit": "kilogram"}
    gas = {
        "name": "Natural gas, from medium pressure network (0.1-1 bar), at service station",
        "unit": "kilogram",
    }
    selected = InventorySet(
        [
            petrol,
            gas,
            {**petrol, "unit": "kilowatt hour"},
            {**gas, "unit": "cubic meter"},
        ],
        "2026",
        "remind",
        family="uvek",
    ).generate_vehicle_fuel_map("car")
    expected = {
        f"passenger car, {powertrain}, {size}"
        for powertrain in ("gasoline", "plugin gasoline hybrid", "compressed gas")
        for size in ("mini", "van")
    }
    assert set(selected) == expected
    for role, records in selected.items():
        assert records == [gas if ", compressed gas," in role else petrol]
    assert canonical_vehicle_energy_product(petrol["name"], "kilogram") == "petrol"
    assert canonical_vehicle_energy_product(gas["name"], "kilogram") == "natural gas"
    assert canonical_vehicle_energy_product(gas["name"], "cubic meter") != "natural gas"
