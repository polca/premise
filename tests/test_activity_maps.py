# content of test_activity_maps.py
import pytest

import premise.activity_maps as activity_maps_module
from premise.activity_maps import InventorySet, act_fltr
from premise.inventory_store import (
    IndexedInventoryList,
    get_wurst_query_diagnostics,
)

dummy_minimal_db = [
    {
        "name": "electricity production, at BIGCC power plant, pre, pipeline 200km, storage 1000m"
    },
    {"name": "electricity production, at BIGCC power plant, no CCS"},
    {"name": "electricity production, at power plant/lignite, IGCC, no CCS"},
    {
        "name": "electricity production, at power plant/hard coal, pre, pipeline 200km, storage 1000m"
    },
    {
        "name": "electricity production, at power plant/hard coal, post, pipeline 200km, storage 1000m"
    },
    {
        "name": "electricity production, at power plant/natural gas, pre, pipeline 200km, storage 1000m"
    },
    {"name": "heat and power co-generation, biogas, gas engine, label-certified"},
    {"name": "electricity production, hard coal"},
    {"name": "heat and power co-generation, hard coal"},
    {"name": "electricity production, natural gas, conventional power plant"},
    {"name": "electricity production, natural gas, combined cycle power plant"},
    {
        "name": "heat and power co-generation, natural gas, conventional power plant, 100MW electrical"
    },
    {"name": "electricity production, deep geothermal"},
    {"name": "electricity production, hydro, reservoir, tropical region"},
    {"name": "electricity production, nuclear, pressure water reactor"},
    {"name": "electricity production, oil"},
    {"name": "electricity production, solar thermal parabolic trough, 50 MW"},
    {
        "name": "electricity production, photovoltaic, 3kWp facade installation, multi-Si, laminated, integrated"
    },
    {
        "name": "electricity production, wind, 2.3MW turbine, precast concrete tower, onshore"
    },
    {"name": "steel production"},
    {"name": "market for aluminium, primary"},
]

for act in dummy_minimal_db:
    act["location"] = "DE"
    act["unit"] = "kilowatt hour"
    act["reference product"] = "electricity"


def test_presence_of_dict():
    maps = InventorySet(dummy_minimal_db)
    assert isinstance(maps.generate_powerplant_map(), dict)


def test_mapping_file_is_parsed_once_across_variable_lookups(tmp_path, monkeypatch):
    mapping_file = tmp_path / "mapping.yaml"
    mapping_file.write_text(
        "technology:\n  first: one\n  second: two\n",
        encoding="utf-8",
    )
    parse_calls = []
    original_load = activity_maps_module.yaml.load

    def counted_load(stream, *, Loader):
        parse_calls.append((stream.name, Loader))
        return original_load(stream, Loader=Loader)

    activity_maps_module.get_mapping.cache_clear()
    activity_maps_module._load_mapping_file.cache_clear()
    monkeypatch.setattr(activity_maps_module.yaml, "load", counted_load)

    first = activity_maps_module.get_mapping(mapping_file, "first")
    second = activity_maps_module.get_mapping(mapping_file, "second")

    assert first == {"technology": "one"}
    assert second == {"technology": "two"}
    assert parse_calls == [
        (str(mapping_file), activity_maps_module._YAML_FULL_LOADER),
    ]


def test_length_dict():
    maps = InventorySet(dummy_minimal_db)

    assert len(maps.generate_powerplant_map()) > 0
    assert len(maps.generate_fuel_map()) > 0
    assert len(maps.generate_cement_map()) > 0


def test_act_fltr_preserves_or_includes_and_excludes_any_mask_match():
    database = [
        {"name": "electricity production, coal", "location": "DE"},
        {"name": "electricity production, gas", "location": "US"},
        {"name": "heat production, coal", "location": "DE"},
    ]

    result = act_fltr(
        database,
        fltr={"name": ["electricity", "heat"], "location": ["DE", "FR"]},
        mask={"name": ["heat", "market"]},
    )

    assert result == [database[0]]


def test_generate_sets_supports_filters_without_a_name_prefilter():
    database = [
        {"name": "first", "reference product": "target fuel"},
        {"name": "second", "reference product": "other product"},
    ]
    mapping = InventorySet(database).generate_sets_from_filters(
        {"target": {"fltr": {"reference product": "target"}}}
    )

    assert mapping == {"target": [database[0]]}


def test_generate_sets_keeps_indexed_prefilter_for_inner_queries():
    database = IndexedInventoryList(
        [
            {"name": "electricity production, coal", "location": "DE"},
            {"name": "electricity production, gas", "location": "US"},
            {"name": "heat production, coal", "location": "DE"},
        ]
    )
    filters = {
        "coal": {"fltr": {"name": "electricity", "location": "DE"}},
        "gas": {"fltr": {"name": "electricity", "location": "US"}},
    }
    get_wurst_query_diagnostics(reset=True)

    mapping = InventorySet(database).generate_sets_from_filters(filters)
    diagnostics = get_wurst_query_diagnostics(reset=True)

    assert mapping == {"coal": [database[0]], "gas": [database[1]]}
    assert diagnostics == {"indexed": 3, "fallback": 0}


def test_image_cdr_map_includes_cement_biogenic_ccs_variant():
    activity_name = (
        "carbon dioxide, captured and stored, at cement production plant, "
        "from non-fossil carbon dioxide, using monoethanolamine"
    )
    maps = InventorySet(
        [
            {
                "name": activity_name,
                "reference product": "carbon dioxide, captured",
                "location": "RER",
                "unit": "kilogram",
            }
        ],
        model="image",
    )

    cdr_map = maps.generate_cdr_map(model="image")

    assert "cement production, non-fossil CO2, with CCS" in cdr_map
    assert cdr_map["cement production, non-fossil CO2, with CCS"][0]["name"] == (
        activity_name
    )


def test_image_sorbent_dac_maps_to_heat_pump_inventory():
    heat_pump_activity = (
        "carbon dioxide, captured and stored, with a sorbent-based direct air "
        "capture system, 100ktCO2, with heat pump heat, and grid electricity"
    )
    base_activity = (
        "carbon dioxide, captured and stored, with a sorbent-based direct air "
        "capture system, 100ktCO2"
    )
    maps = InventorySet(
        [
            {
                "name": heat_pump_activity,
                "reference product": "carbon dioxide, captured",
                "location": "RER",
                "unit": "kilogram",
            },
            {
                "name": base_activity,
                "reference product": "carbon dioxide, captured",
                "location": "RER",
                "unit": "kilogram",
            },
        ],
        model="image",
    )

    cdr_map = maps.generate_cdr_map(model="image")

    assert "direct air capture (sorbent, low-temp, heat pump) with storage" in cdr_map
    assert (
        cdr_map["direct air capture (sorbent, low-temp, heat pump) with storage"][0][
            "name"
        ]
        == heat_pump_activity
    )
    assert "direct air capture (sorbent, low-temp) with storage" not in cdr_map


@pytest.mark.parametrize("model", ["remind", "remind-eu", "tiam-ucl"])
def test_heat_pump_dac_maps_to_sorbent_inventory_for_selected_models(model):
    solvent_activity = (
        "carbon dioxide, captured and stored, with a solvent-based direct air "
        "capture system, 1MtCO2, with heat pump heat, and grid electricity"
    )
    sorbent_activity = (
        "carbon dioxide, captured and stored, with a sorbent-based direct air "
        "capture system, 100ktCO2, with heat pump heat, and grid electricity"
    )
    maps = InventorySet(
        [
            {
                "name": solvent_activity,
                "reference product": "carbon dioxide, captured",
                "location": "RER",
                "unit": "kilogram",
            },
            {
                "name": sorbent_activity,
                "reference product": "carbon dioxide, captured",
                "location": "RER",
                "unit": "kilogram",
            },
        ],
        model=model,
    )

    cdr_map = maps.generate_cdr_map(model=model)

    technology = "direct air capture (sorbent, low-temp, heat pump) with storage"
    assert cdr_map[technology][0]["name"] == sorbent_activity
    assert (
        "direct air capture (solvent, high-temp, heat pump) with storage" not in cdr_map
    )
