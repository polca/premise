"""PV recipe and integration regressions independent of a Brightway project."""

import csv
import importlib.util
import json
from copy import deepcopy
from pathlib import Path

import pytest
import xarray as xr

from premise.photovoltaic import (
    country_adapters,
    identity,
    installation_metadata,
    module_area,
    module_exchange,
    use_pv_2026,
)
from premise.electricity import Electricity

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "build_pv", ROOT / "scripts/build_pv_2026.py"
)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


@pytest.fixture(scope="module")
def inventories():
    directory = ROOT / "premise/data/additional_inventories"
    return (
        builder.read_workbook(directory / "lci-PV-2026.xlsx"),
        builder.read_workbook(directory / "lci-PV-2026-electricity.xlsx"),
    )


def test_rollout_is_explicit():
    assert use_pv_2026("3.12", "cutoff")
    assert not use_pv_2026("3.12", "consequential")
    assert not use_pv_2026("3.11", "cutoff")


def test_country_coverage_normalisation_and_water_balance(inventories):
    core, adapters = inventories
    parameters = json.loads(
        (ROOT / "premise/data/solar/pv_2026_parameters.json").read_text()
    )
    plants = {identity(d): d for d in core if installation_metadata(d)}
    countries = set(parameters["locations"])
    for segment, product in [
        ("residential", "electricity, low voltage"),
        ("commercial", "electricity, medium voltage"),
        ("production mix", "electricity, photovoltaic, at plant"),
    ]:
        group = [
            d
            for d in adapters
            if d["name"] == f"electricity production, photovoltaic, {segment}"
        ]
        assert {d["location"] for d in group} == countries
        assert "REF" not in countries and len(countries) == 250
        for dataset in group:
            assert dataset["reference product"] == product
            plant_inputs = [e for e in dataset["exchanges"] if identity(e) in plants]
            assert plant_inputs
            # Recover the documented electricity shares, independently of capacity.
            shares = [
                float(e["comment"].split(" / ")[0].removeprefix("Electricity share "))
                for e in plant_inputs
            ]
            assert sum(shares) == pytest.approx(1, abs=1e-10)
            if segment != "production mix":
                assert {plants[identity(e)]["pv segment"] for e in plant_inputs} == {
                    segment
                }
            water = next(
                e["amount"]
                for e in dataset["exchanges"]
                if e["name"] == "market for tap water"
            )
            wastewater = next(
                e["amount"]
                for e in dataset["exchanges"]
                if "treatment of wastewater" in e["name"]
            )
            evaporation = next(
                e["amount"] for e in dataset["exchanges"] if e["name"] == "Water"
            )
            assert water == pytest.approx(1000 * (-wastewater + evaporation))
            assert water == pytest.approx(
                20
                * sum(
                    e["amount"] * module_area(plants[identity(e)]) for e in plant_inputs
                )
            )
    iceland = next(d for d in adapters if d["location"] == "IS")
    assert iceland["pv yield origin"].startswith("fallback:")


def test_generation_weights_and_reproducible_recipes(inventories):
    core, adapters = inventories
    with (ROOT / "premise/data/solar/pv_generation_2023.csv").open() as stream:
        generation = {
            r["location"]: float(r["generation_gwh"]) for r in csv.DictReader(stream)
        }
    assert {c: generation[c] for c in ("CN", "FR", "IN", "CL")} == {
        "CN": 583854,
        "FR": 22126,
        "IN": 108134,
        "CL": 18310,
    }
    parameters = json.loads(
        (ROOT / "premise/data/solar/pv_2026_parameters.json").read_text()
    )
    rebuilt = country_adapters(deepcopy(core), parameters, generation)
    actual = {identity(d): d for d in adapters}
    for dataset in rebuilt:
        saved = actual[identity(dataset)]
        assert len(saved["exchanges"]) == len(dataset["exchanges"])
        assert [e["amount"] for e in saved["exchanges"]] == pytest.approx(
            [e["amount"] for e in dataset["exchanges"]], rel=1e-14
        )
    for country in parameters["report_yields"]:
        segments = [
            d
            for d in rebuilt
            if d["location"] == country
            and d["name"].endswith(("residential", "commercial"))
        ]
        volume = sum(
            next(
                e["production volume"]
                for e in d["exchanges"]
                if e["type"] == "production"
            )
            for d in segments
        )
        assert volume == pytest.approx(generation[country] * 1e6)


def test_efficiency_uses_total_module_area_and_mw_capacity(inventories):
    core, _ = inventories
    dataset = deepcopy(
        next(
            d for d in core if "10 mw, ground-mounted, single-crystalline" in d["name"]
        )
    )
    assert dataset["pv capacity kwp"] == 10000
    original = deepcopy(dataset)
    original_area = module_area(dataset)
    efficiency = Electricity.__new__(Electricity)
    efficiency.year = 2050
    efficiency.write_log = lambda **kwargs: None
    projection = xr.DataArray(
        [[[0.25, 0.27, 0.30]]],
        dims=["technology", "year", "efficiency_type"],
        coords={
            "technology": ["single-Si"],
            "year": [2050],
            "efficiency_type": ["min", "mean", "max"],
        },
    )
    efficiency._update_pv_2026_efficiency(dataset, projection)
    assert module_area(dataset) == pytest.approx(10000 / 0.27)
    factor = module_area(dataset) / original_area
    for before, after in zip(original["exchanges"], dataset["exchanges"]):
        name = before["name"]
        dependent = module_exchange(before) or (
            before["type"] == "technosphere"
            and (
                (before["unit"] == "square meter" and "photovoltaic mounting" in name)
                or (
                    before["unit"] == "kilogram"
                    and "treatment" in name
                    and "photovoltaic module" in name
                )
            )
        )
        assert after["amount"] == pytest.approx(
            before["amount"] * (factor if dependent else 1)
        )
        if module_exchange(before):
            assert after["minimum"] <= after["amount"] <= after["maximum"]
    # Applying the same year twice must not rescale the plant a second time.
    once = deepcopy(dataset)
    efficiency._update_pv_2026_efficiency(dataset, projection)
    assert [e["amount"] for e in dataset["exchanges"]] == pytest.approx(
        [e["amount"] for e in once["exchanges"]]
    )


def test_efficiency_never_reduces_the_baseline(inventories):
    core, _ = inventories
    dataset = deepcopy(
        next(
            d for d in core if "10 mw, ground-mounted, single-crystalline" in d["name"]
        )
    )
    before = deepcopy(dataset)
    efficiency = Electricity.__new__(Electricity)
    efficiency.year = 2020
    efficiency.write_log = lambda **kwargs: None
    projection = xr.DataArray(
        [[[0.1, 0.12, 0.15]]],
        dims=["technology", "year", "efficiency_type"],
        coords={
            "technology": ["single-Si"],
            "year": [2020],
            "efficiency_type": ["min", "mean", "max"],
        },
    )
    efficiency._update_pv_2026_efficiency(dataset, projection)
    assert dataset == before


def test_operational_metadata_survives_inventory_caches(inventories):
    from premise.utils import (
        _trim_cache_dataset_in_place,
        _trim_scenario_dataset_in_place,
    )

    plant = next(d for d in inventories[0] if "pv capacity kwp" in d)
    for trim in (_trim_cache_dataset_in_place, _trim_scenario_dataset_in_place):
        result = trim(deepcopy(plant))
        assert result["pv capacity kwp"] == plant["pv capacity kwp"]
        assert result["pv technology"] == plant["pv technology"]


def test_cleaning_tracks_changed_area(inventories):
    core, adapters = deepcopy(inventories)
    plants = [d for d in core if "pv capacity kwp" in d]
    adapter = next(d for d in adapters if d["location"] == "CH")
    old = next(
        e["amount"] for e in adapter["exchanges"] if e["name"] == "market for tap water"
    )
    original_areas = {identity(d): module_area(d) for d in plants}
    for plant in plants:
        for exchange in plant["exchanges"]:
            if module_exchange(exchange):
                exchange["amount"] *= 0.8
    efficiency = Electricity.__new__(Electricity)
    efficiency.database = plants + [adapter]
    efficiency._update_pv_cleaning_water(original_areas)
    new = next(
        e["amount"] for e in adapter["exchanges"] if e["name"] == "market for tap water"
    )
    assert new == pytest.approx(0.8 * old)


@pytest.mark.parametrize(
    "model", ["image", "remind", "remind-eu", "message", "tiam-ucl", "gcam", "witch"]
)
def test_country_locations_map_to_supported_iam_regions(inventories, model):
    from premise.geomap import Geomap

    geography = Geomap(model)
    for location in {d["location"] for d in inventories[0] + inventories[1]}:
        assert geography.ecoinvent_to_iam_location(location) in geography.iam_regions


def test_iam_aliases_select_country_adapters_only(inventories):
    from premise.activity_maps import InventorySet

    core, adapters = inventories
    mapping = InventorySet(
        core + adapters, version="3.12", model="image"
    ).generate_powerplant_map()
    for technology, segment in [
        ("Solar PV Residential", "residential"),
        ("Solar PV Centralized", "commercial"),
    ]:
        selected = mapping[technology]
        assert len(selected) == 250
        assert all(
            d["name"] == f"electricity production, photovoltaic, {segment}"
            for d in selected
        )
        assert all("pv reference location" not in d for d in selected)


def test_reference_cleaning_is_preserved_without_efficiency_change(inventories):
    core = deepcopy(inventories[0])
    original = deepcopy(core)
    efficiency = Electricity.__new__(Electricity)
    efficiency.database = core
    areas = {identity(d): module_area(d) for d in core if "pv capacity kwp" in d}
    efficiency._update_pv_cleaning_water(areas)
    assert core == original


@pytest.mark.parametrize(
    "version,system_model,new_core",
    [
        ("3.12", "cutoff", True),
        ("3.12", "consequential", False),
        ("3.11", "cutoff", False),
    ],
)
def test_default_import_selects_exactly_one_pv_core(
    monkeypatch, version, system_model, new_core
):
    import premise.new_database as module

    selected = []
    monkeypatch.setattr(
        module,
        "_extract_default_inventory_importers",
        lambda paths: selected.extend(paths) or [],
    )
    database = module.NewDatabase.__new__(module.NewDatabase)
    database.version = version
    database.system_model = system_model
    database._NewDatabase__import_inventories()
    paths = {path.name: source_version for path, source_version in selected}
    assert ("lci-PV.xlsx" in paths) != new_core
    assert ("lci-PV-2026.xlsx" in paths) == new_core
    assert ("lci-PV-2026-electricity.xlsx" in paths) == new_core
    assert ("lci-PV-CIGS.xlsx" in paths) == new_core
    assert "lci-PV-GaAs.xlsx" in paths and "lci-PV-perovskite.xlsx" in paths
    if new_core:
        assert paths["lci-PV-2026.xlsx"] == "3.12"
        assert paths["lci-PV-CIGS.xlsx"] == "3.7"


def test_pv_workbook_edits_invalidate_inventory_cache_only(monkeypatch, tmp_path):
    import premise.new_database as module

    for field in (
        "FILEPATH_PHOTOVOLTAICS_2026",
        "FILEPATH_PHOTOVOLTAICS_2026_ELECTRICITY",
        "FILEPATH_PHOTOVOLTAICS_CIGS",
    ):
        path = tmp_path / f"{field}.xlsx"
        path.write_bytes(b"first revision")
        monkeypatch.setattr(module, field, path)
    database = module.NewDatabase.__new__(module.NewDatabase)
    database.source_type = "brightway"
    database.version = "3.12"
    database.system_model = "cutoff"
    database.keep_source_db_uncertainty = False
    database.keep_imports_uncertainty = True
    original = database._database_cache_path("source", inventories=True)
    source = database._database_cache_path("source")
    module.FILEPATH_PHOTOVOLTAICS_2026_ELECTRICITY.write_bytes(
        b"changed country recipes"
    )
    assert database._database_cache_path("source", inventories=True) != original
    assert database._database_cache_path("source") == source


def test_all_new_silicon_cells_preserve_material_inventory_and_uncertainty(inventories):
    from premise.activity_maps import InventorySet
    from premise.metals import Metals
    from premise.metals_rules import load_material_rules

    core, _ = inventories
    cells = InventorySet(core, "3.12").generate_metals_activities_map()["c-Si"]
    assert len(cells) == 20
    before = [deepcopy(d) for d in cells]
    metals = object.__new__(Metals)
    metals.material_policies = load_material_rules().policies
    metals.material_rules_by_technology = {
        "c-Si": [
            r for r in load_material_rules().enabled_rules if r.technology == "c-Si"
        ]
    }
    metals.activities_metals_map = {"c-Si": cells}
    # Preserved source inventories need neither a conversion nor a metal provider.
    metals.technology_conversions_by_name = {}
    metals.db_index = {}
    metals.material_decisions = []
    metals._validation_targets = {}
    plan = metals._compile_material_update_plan()
    assert len(plan) == 140
    for item in plan:
        metals._apply_material_rule(
            dataset=item["dataset"],
            technology=item["technology"],
            rule=item["rule"],
            conversion_factor=item["conversion_factor"],
        )
    assert cells == before
    assert len(metals.material_decisions) == 140
    assert {d["reason code"] for d in metals.material_decisions} == {
        "metals.material_rule.preserved_source"
    }
