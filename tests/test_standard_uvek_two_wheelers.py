"""Native two-wheeler filters preserve vehicle class and the vehicle-km basis."""

from copy import deepcopy

from premise.core.filesystem_constants import VARIABLES_DIR
from premise.inventory.activity_maps import InventorySet, get_mapping
from premise.inventory.clic_requirements import required_roots


def definitions():
    return get_mapping(
        VARIABLES_DIR / "transport_two_wheelers.yaml", "uvek_aliases", model="remind"
    )


def test_six_native_services_keep_power_class_year_chemistry_and_reference_unit():
    records = [{**row["fltr"], "location": "CH"} for row in definitions().values()]
    variants = []
    for original in records:
        wrong_year = {**original, "name": original["name"].replace("2020", "2016")}
        wrong_unit = {**original, "unit": "person kilometer"}
        variants.extend([wrong_year, wrong_unit])
        if "NMC" in original["name"]:
            variants.append(
                {**original, "name": original["name"].replace("NMC", "LFP")}
            )
    graph = [*records, *variants]
    before = deepcopy(graph)
    selected = InventorySet(
        graph, "2026", "remind", family="uvek"
    ).generate_transport_map("two-wheeler")
    assert len(selected) == 6
    assert selected["moped, gasoline"] == [records[0]]
    assert selected["moped, battery electric"] == [records[1]]
    assert "4-11kW" in records[0]["name"]
    assert "11-35kW" in selected["motorcycle, 50-250cc, gasoline"][0]["name"]
    assert ">35kW" in selected["motorcycle, >250cc, gasoline"][0]["name"]
    assert all(len(rows) == 1 and rows[0] in records for rows in selected.values())
    assert graph == before


def test_native_two_wheelers_need_no_legacy_manufacturing_imports():
    assert required_roots(["two_wheelers"], [{"model": "remind"}], family="uvek") == ()
    assert required_roots(["two_wheelers"], [{"model": "remind"}], family="ecoinvent")
    for row in definitions().values():
        assert row["native_reference_amount"] == 1
        assert row["fltr"]["unit"] == "kilometer"
        assert row["use_legacy_templates"] is False


def test_fuel_filters_match_vehicle_supply_and_low_voltage_scenario_markets():
    gasoline = {
        "name": "Fuel supply for gasoline vehicles",
        "unit": "kilogram",
        "location": "CH",
    }
    grids = [
        {"name": name, "unit": "kilowatt hour", "location": "EUR"}
        for name in (
            "Electricity, low voltage, at grid",
            "market group for electricity, low voltage",
            "market for electricity, low voltage",
        )
    ]
    unrelated = [
        {**gasoline, "unit": "unit"},
        {**gasoline, "name": "Petrol, at regional storage"},
        {**grids[0], "name": "market group for electricity, medium voltage"},
    ]
    selected = InventorySet(
        [gasoline, *grids, *unrelated], "2026", "remind", family="uvek"
    ).generate_vehicle_fuel_map("two-wheeler")
    assert len(selected) == 6
    for role, records in selected.items():
        assert records == ([gasoline] if "gasoline" in role else grids)
