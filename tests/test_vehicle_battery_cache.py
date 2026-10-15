"""Actual vehicle inventories must retain sizing inputs through source caching."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter

import premise.new_database as new_database_module
import premise.utils as utils_module
from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_store import CompactInventoryStore, InventoryStore
from premise.new_database import NewDatabase
from premise.vehicle_battery import SPEC_KEY, STATE_KEY, resize_regional_batteries
from premise.vehicle_energy_use import SOURCE_KEY, select_source, source_name


@pytest.fixture(scope="module")
def road_catalogue():
    data = []
    for stem in ("pass_cars", "trucks", "buses", "two_wheelers", "final-energy"):
        data.extend(ExcelImporter(INVENTORY_DIR / f"lci-{stem}.xlsx").data)
    for dataset in data:
        for exchange in dataset["exchanges"]:
            if "reference product" in exchange:
                exchange["product"] = exchange.pop("reference product")
    return data


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize(
    "wrapper_name,capacity,lifetime,services,replacements",
    [
        ("electricity, used in passenger car", 67.19999694824219, 200000, 1, 0),
        ("electricity, used in heavy-duty vehicle", 550.864990234375, 710000, 10.3, 0),
        ("electricity, used in single deck coach or urban bus", 399, 700000, 10, 0),
        ("electricity, used in single deck trolleybus", 13, 1000000, 10, 0),
        ("electricity, used in battery electric motorcycle", 8.1, 38500, 1, 1),
    ],
)
def test_final_energy_battery_allocation_survives_real_cache_and_runtime_update(
    road_catalogue,
    compact,
    wrapper_name,
    capacity,
    lifetime,
    services,
    replacements,
    tmp_path,
    monkeypatch,
):
    import premise.final_energy as final_module

    # Independent inputs are the documented physical vehicle specifications.
    # Exercise both source-cache trimming and the final-energy update hook.
    path = tmp_path / "road.pickle"
    utils_module.create_cache(deepcopy(road_catalogue), path)
    data = utils_module.load_cached_database(path)
    if compact:
        data = CompactInventoryStore(data).materialize()
    wrapper = next(d for d in data if d["name"] == wrapper_name)
    assert SOURCE_KEY in wrapper
    source = select_source(data, source_name(wrapper_name), wrapper["location"])
    grid = next(
        e
        for e in source["exchanges"]
        if e["type"] == "technosphere"
        and e.get("product", "").startswith("electricity,")
        and e["unit"] == "kilowatt hour"
    )
    baseline_grid = grid["amount"]
    # A later efficiency update must change the per-MJ denominator. Packs in
    # this test remain fixed; sizing is exercised separately above.
    grid["amount"] *= 0.8
    wrapper["exchanges"] = [
        e for e in wrapper["exchanges"] if e["type"] == "production"
    ]
    # Other wrappers have no role in this integration case.
    for d in data:
        if d is not wrapper:
            d.pop(SOURCE_KEY, None)
    transform = SimpleNamespace(
        database=data,
        index={},
        cache={},
        final_energy_map={},
        regionalize_heating_datasets=lambda: None,
        relink_datasets=lambda: None,
    )
    monkeypatch.setattr(final_module, "FinalEnergy", lambda **kwargs: transform)
    monkeypatch.setattr(final_module, "get_scenario_inventory", lambda s: s["database"])
    monkeypatch.setattr(
        final_module, "replace_scenario_inventory", lambda s, d: s.update(database=d)
    )
    scenario = dict(
        database=data,
        model="remind",
        pathway="test",
        year=2050,
        **{"iam data": SimpleNamespace(final_energy_use=True)},
    )
    final_module._update_final_energy(scenario, "3.12", "cutoff")
    actual = sum(
        e["amount"]
        for e in wrapper["exchanges"]
        if e["type"] == "technosphere"
        and e.get("product") == "electricity storage capacity"
    )
    expected = (
        capacity
        * (1 + replacements)
        / (lifetime * services * baseline_grid * 0.8 * 3.6)
    )
    assert actual == pytest.approx(expected, rel=2e-6)
    assert sum(
        e["amount"]
        for e in wrapper["exchanges"]
        if e["type"] == "technosphere"
        and e.get("product", "").startswith("electricity,")
        and e["unit"] == "kilowatt hour"
    ) == pytest.approx(1 / 3.6)


@pytest.fixture(scope="module", params=["pass_cars", "trucks", "buses"])
def catalogue(request):
    data = ExcelImporter(INVENTORY_DIR / f"lci-{request.param}.xlsx").data
    for dataset in data:
        for exchange in dataset["exchanges"]:
            if "reference product" in exchange:
                exchange["product"] = exchange.pop("reference product")
    return request.param, data


@pytest.mark.parametrize("route", ["fresh", "reused", "compact checkpoint"])
@pytest.mark.parametrize("year,factor", [(2020, 1), (2050, 0.8)])
def test_source_cache_preserves_range_sizing_and_MJ_allocation(
    catalogue, route, year, factor, tmp_path, monkeypatch
):
    family, originals = catalogue
    monkeypatch.setattr(utils_module, "DIR_CACHED_DB", tmp_path)
    monkeypatch.setattr(new_database_module, "DIR_CACHED_DB", tmp_path)
    obj = object.__new__(NewDatabase)
    obj.source_type = "brightway"
    obj.keep_source_db_uncertainty = False
    obj.keep_imports_uncertainty = False
    obj._database = []

    def import_inventories(collect_data=True):
        obj._database.extend(deepcopy(originals))
        return obj._database if collect_data else []

    obj._NewDatabase__import_inventories = import_inventories
    assert obj._NewDatabase__find_cached_inventories("test") is None
    data = obj._database
    if route != "fresh":

        def unexpected_import(**kwargs):
            pytest.fail("A reused source cache unexpectedly imported inventories")

        obj._NewDatabase__import_inventories = unexpected_import
        data = obj._NewDatabase__find_cached_inventories("test")
    if route == "compact checkpoint":
        utils_module.create_cache([], obj._database_cache_path("test"))
        path = obj._write_compact_source_checkpoint("test", CompactInventoryStore(data))
        assert obj._find_compact_source_checkpoint("test")[0] == path
        data = InventoryStore.open(path).materialize()

    # Check the entire imported catalogue, including production datasets.
    by_name = {d["name"]: d for d in data}
    for original in originals:
        if SPEC_KEY in original:
            assert by_name[original["name"]].get(SPEC_KEY) == original[SPEC_KEY]

    name = {
        "pass_cars": "transport, passenger car, battery electric, Medium",
        "trucks": "transport, freight, lorry, battery electric, 3.5 metric ton",
        "buses": "transport, passenger bus, battery electric - overnight charging, 13m single deck urban bus",
    }[family]
    service = by_name[name]
    from premise.vehicle_battery import specification

    spec = specification(service)
    service["location"] = "NEU"
    service["regionalized"] = True
    energy = [
        e
        for e in service["exchanges"]
        if e["type"] == "technosphere"
        and e.get("product") == "electricity, low voltage"
    ]
    assert len(energy) == 1
    energy[0]["amount"] *= factor
    transform = SimpleNamespace(
        year=year,
        vehicle_type=spec["family"],
        database=data,
        vehicle_map={"BEV": [service]},
        add_to_index=lambda _: None,
    )
    resize_regional_batteries(transform)
    state = service[STATE_KEY]
    assert state["capacity kWh"] == pytest.approx(
        spec["capacity kWh"] * factor, rel=2e-6
    )
    if family != "buses":
        assert state["electric range km"] == pytest.approx(spec["electric range km"])
        assert state["cell mass kg"] * state["cell density kWh/kg"] == pytest.approx(
            state["capacity kWh"], rel=2e-6
        )

    link = next(
        e
        for e in service["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "unit"
        and e["name"]
        .lower()
        .startswith(("passenger car,", "lorry production,", "passenger bus,"))
    )
    provider = next(
        d
        for d in data
        if d["name"] == link["name"] and d["location"] == link["location"]
    )
    purchase = next(
        e
        for e in provider["exchanges"]
        if e["type"] == "technosphere"
        and e.get("product") == "electricity storage capacity"
    )
    actual_per_MJ = purchase["amount"] * link["amount"] / (energy[0]["amount"] * 3.6)
    # Independent lifetime calculation: payload changes cancel when both the
    # freight-service and energy denominators are treated consistently.
    service_per_km = spec.get("passengers", spec.get("cargo mass kg", 1000) / 1000)
    expected_per_MJ = (
        spec["capacity kWh"]
        * factor
        * (1 + spec["replacements"])
        / (
            spec["lifetime km"]
            * spec["grid kWh per service"]
            * service_per_km
            * factor
            * 3.6
        )
    )
    assert actual_per_MJ == pytest.approx(expected_per_MJ, rel=2e-6)
    before = deepcopy(data)
    resize_regional_batteries(transform)
    assert data == before


def test_pre_sizing_source_caches_and_compact_checkpoints_are_not_reused(
    tmp_path, monkeypatch
):
    # Version 6 caches omitted the sizing specification from runtime payloads.
    # Its metadata sidecars are not restored before transport transformations.
    obj = object.__new__(NewDatabase)
    obj.source_type = "brightway"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    monkeypatch.setattr(new_database_module, "DIR_CACHED_DB", tmp_path)
    monkeypatch.setattr(utils_module, "DIR_CACHED_DB", tmp_path)
    source = [
        {
            "name": "activity",
            "reference product": "product",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [],
        }
    ]
    with monkeypatch.context() as old:
        old.setattr(new_database_module, "CACHE_SCHEMA_VERSION", 6)
        utils_module.create_cache(deepcopy(source), obj._database_cache_path("test"))
        utils_module.create_cache(
            [], obj._database_cache_path("test", inventories=True)
        )
        obj._write_compact_source_checkpoint("test", CompactInventoryStore(source))
        assert obj._find_compact_source_checkpoint("test") is not None
    assert obj._compact_source_cache_references("test") is None
    assert obj._find_compact_source_checkpoint("test") is None
