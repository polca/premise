"""Bundled profile selection preserves scientific quantities and import scope."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.inventory.profiles import (
    apply_inventory_profiles,
    profiles,
    select_companion_profiles,
)
from test_inventory_contracts import inventory

HYDROGEN = "lci-hydrogen-electrolysis.xlsx"
PIPELINES = "lci-hydrogen-distribution.xlsx"
VRFB = "lci-batteries-vanadium-redox-flow.xlsx"


def source(filename):
    return SimpleNamespace(data=deepcopy(inventory(filename)))


def one(importer, name):
    (record,) = [r for r in importer.data if r["name"] == name]
    return record


@pytest.mark.parametrize(
    "version,model,expected",
    [
        ("3.8", "cutoff", set()),
        ("3.11", "cutoff", set()),
        (
            "3.12",
            "cutoff",
            {
                "pipeline-coatings",
                "ptfe-gasket",
                "fkm-gasket",
                "transformer-core",
                "nmc532-cell",
                "nmc955-references",
            },
        ),
        (
            "3.12",
            "EN15804",
            {
                "pipeline-coatings",
                "ptfe-gasket",
                "fkm-gasket",
                "transformer-core",
                "nmc532-cell",
                "nmc955-references",
            },
        ),
        ("3.12", "consequential", {"ptfe-gasket", "fkm-gasket", "transformer-core"}),
        ("3.12.1", "cutoff", {"ptfe-gasket", "fkm-gasket", "transformer-core"}),
        ("3.13", "cutoff", {"ptfe-gasket", "fkm-gasket"}),
    ],
)
def test_profile_version_and_system_model_boundaries(version, model, expected):
    assert {p["id"] for p in profiles(version, model)} == expected


@pytest.mark.parametrize("model", ["cutoff", "EN15804"])
def test_pipeline_selection_changes_only_two_supplier_identities(model):
    from premise.database.new_database import FILEPATH_HYDROGEN_DISTRI_INVENTORIES

    importer = source(FILEPATH_HYDROGEN_DISTRI_INVENTORIES.name)
    background = deepcopy(inventory("lci-pipeline-coating-native-312.xlsx"))
    original, saved = deepcopy(importer.data), deepcopy(background)
    apply_inventory_profiles(
        importer, FILEPATH_HYDROGEN_DISTRI_INVENTORIES, "3.12", model, background
    )
    expected = [
        r
        for r in original
        if r["name"] != "aluminium-zinc coating alloy production, for galvanizing"
    ]
    for kind, quantity in [
        ("distribution", 75.25328256144047),
        ("transmission", 876.7114153394484),
    ]:
        parent = next(
            r for r in expected if r["name"] == f"hydrogen {kind} pipeline construction"
        )
        (purchase,) = [
            e
            for e in parent["exchanges"]
            if e["name"] == "aluminium-zinc coating alloy production, for galvanizing"
        ]
        assert purchase["amount"] == quantity
        purchase.update(
            name=f"aluminium-zinc-silicon coating production, hot-dip, on {kind} pipe",
            location="RER",
            unit="kilogram",
        )
        for field in ("product", "reference product"):
            if field in purchase:
                purchase[field] = f"aluminium-zinc-silicon coating, on {kind} pipe"
        for field in ("input", "output", "database", "code"):
            purchase.pop(field, None)
    assert importer.data == expected and background == saved
    apply_inventory_profiles(
        importer, FILEPATH_HYDROGEN_DISTRI_INVENTORIES, "3.12", model, background
    )
    assert importer.data == expected


def test_ptfe_selection_preserves_stack_dose_and_other_inputs():
    importer = source(HYDROGEN)
    expected = deepcopy(importer.data)
    old = "polytetrafluoroethylene gasket production, polymerization and forming"
    expected = [r for r in expected if r["name"] != old]
    stack = next(
        r for r in expected if r["name"] == "electrolyzer production, 1MWe, AEC, Stack"
    )
    (edge,) = [e for e in stack["exchanges"] if e["name"] == old]
    assert edge["amount"] == 144.2
    edge["name"] = (
        "polytetrafluoroethylene gasket production, forming, resin supply proxy"
    )
    for field in ("product", "reference product"):
        if field in edge:
            edge[field] = "PTFE gasket, unfilled, engineering reference"
    for field in ("input", "output", "database", "code"):
        edge.pop(field, None)
    apply_inventory_profiles(importer, HYDROGEN, "3.12", "cutoff", [])
    assert importer.data == expected
    apply_inventory_profiles(importer, HYDROGEN, "3.12", "cutoff", [])
    assert importer.data == expected


def test_fkm_selection_removes_only_legacy_producers_and_rejects_external_users():
    importer = source(VRFB)
    removed = {
        "gasket production, FKM, for VRFB system",
        "vinylidene fluoride production, chlorination and fluorination",
        "hexafluoropropylene production, tetrafluoroethylene pyrolysis",
        "fluoroelastomer production, fkm terpolymer, 75/16/9 molar composition",
        "fluoroelastomer gasket compound production, fkm, fe5540 type",
    }
    expected = [deepcopy(r) for r in importer.data if r["name"] not in removed]
    apply_inventory_profiles(importer, VRFB, "3.12", "cutoff", [])
    assert importer.data == expected
    apply_inventory_profiles(importer, VRFB, "3.12", "cutoff", [])
    assert importer.data == expected
    external = SimpleNamespace(
        data=[
            {
                "name": "another buyer",
                "exchanges": [
                    {
                        "type": "technosphere",
                        "name": "vinylidene fluoride production, chlorination and fluorination",
                    }
                ],
            }
        ]
    )
    for candidate in (source(VRFB), importer):
        original, saved = candidate.data, deepcopy(candidate.data)
        with pytest.raises(ValueError, match="another consumer"):
            apply_inventory_profiles(
                candidate, VRFB, "3.12", "cutoff", [], other_importers=[external]
            )
        assert candidate.data is original and candidate.data == saved


@pytest.mark.parametrize(
    "change", ["quantity", "duplicate", "unknown science", "alias", "boolean"]
)
def test_invalid_legacy_source_fails_without_partial_changes(change):
    importer = source(VRFB)
    record = one(importer, "gasket production, FKM, for VRFB system")
    if change == "quantity":
        record["exchanges"][1]["amount"] *= 1.01
    if change == "boolean":
        record["exchanges"][0]["amount"] = True
    if change == "duplicate":
        importer.data.append(deepcopy(record))
    if change == "unknown science":
        record["exchanges"][1]["unreviewed field"] = 1
    if change == "alias":
        record["product"] = "different product"
    original, saved = importer.data, deepcopy(importer.data)
    with pytest.raises(ValueError):
        apply_inventory_profiles(importer, VRFB, "3.12", "cutoff", [])
    assert importer.data is original and importer.data == saved


def test_private_helper_link_is_checked_when_descriptive_fields_are_stale():
    importer = source(VRFB)
    record = one(
        importer, "vinylidene fluoride production, chlorination and fluorination"
    )
    record.update(database="private-source", code="private-helper")
    external = SimpleNamespace(
        data=[
            {
                "name": "another buyer",
                "exchanges": [
                    {
                        "type": "technosphere",
                        "name": "stale description",
                        "input": ("private-source", "private-helper"),
                    }
                ],
            }
        ]
    )
    original, saved = importer.data, deepcopy(importer.data)
    with pytest.raises(ValueError, match="another consumer"):
        apply_inventory_profiles(
            importer, VRFB, "3.12", "cutoff", [], other_importers=[external]
        )
    assert importer.data is original and importer.data == saved


@pytest.mark.parametrize("model", ["cutoff", "consequential", "EN15804"])
def test_transformer_core_selects_finished_steel_and_preserves_other_inputs(model):
    importer = source(HYDROGEN)
    record = one(importer, "transformer production, electrolysis, 1.1 mva")
    original = deepcopy(record)
    removed = {
        "market for steel, unalloyed",
        "market for silicon, metallurgical grade",
        "market for hot rolling, steel",
        "sheet rolling, steel",
        "market group for electricity, medium voltage",
        "heat production, natural gas, at industrial furnace >100kW",
    }
    provider = {
        "name": "market for grain oriented electrical steel, 0.23 mm coil",
        "reference product": "grain oriented electrical steel, 0.23 mm coil",
        "location": "RER",
        "unit": "kilogram",
    }
    apply_inventory_profiles(
        importer, HYDROGEN, "3.12", model, [provider], phase="migrated"
    )
    selected = one(importer, original["name"])
    assert selected["exchanges"][1]["amount"] == 947.32
    assert selected["exchanges"][1]["name"] == provider["name"]
    assert selected["exchanges"][0] == original["exchanges"][0]
    assert selected["exchanges"][2:] == [
        e for e in original["exchanges"][1:] if e["name"] not in removed
    ]
    once = deepcopy(importer.data)
    apply_inventory_profiles(
        importer, HYDROGEN, "3.12", model, [provider], phase="migrated"
    )
    assert importer.data == once


def test_profile_data_changes_invalidate_imports_without_invalidating_source(
    tmp_path, monkeypatch
):
    from premise.database.new_database import NewDatabase
    from premise.inventory import profiles as module

    builder = object.__new__(NewDatabase)
    builder.version, builder.system_model = "3.12", "cutoff"
    builder.keep_imports_uncertainty = builder.keep_source_db_uncertainty = False
    data = tmp_path / "inventory_profiles.json"
    data.write_bytes(module.PROFILE_PATH.read_bytes())
    monkeypatch.setattr(module, "PROFILE_PATH", data)
    source_path = builder._database_cache_path("profile-cache")
    imported_path = builder._database_cache_path("profile-cache", inventories=True)
    text = data.read_text()
    assert "947.32" in text
    data.write_text(text.replace("947.32", "947.33"))
    assert (
        builder._database_cache_path("profile-cache", inventories=True) != imported_path
    )
    assert builder._database_cache_path("profile-cache") == source_path


def companion_sources():
    from premise.core.filesystem_constants import INVENTORY_DIR

    names = [
        "lci-batteries-NMC955-LTO.xlsx",
        "lci-battery-capacity.xlsx",
        "lci-batteries-NMC955-modern-312.xlsx",
        "lci-batteries-NMC955-fixed-reference-38.xlsx",
        "lci-batteries-NMC955-capacity-reference-310.xlsx",
    ]
    return [(INVENTORY_DIR / n, "3.12") for n in names], [source(n) for n in names]


def test_companion_projection_preserves_every_unrelated_record():
    paths, importers = companion_sources()
    original = [deepcopy(i.data) for i in importers]
    removed = {
        "battery cell production, Li-ion, NMC955",
        "cathode, NMC955, for Li-ion battery",
        "NMC955 oxide",
        "NMC955 hydroxide",
        "anode paste production, silicon-graphite, for lithium-ion battery, nmc955",
        "battery pack component production and end-of-life treatment, lithium-ion, nmc955, without cells and controls",
    }
    capacity = {
        "market for battery capacity, Li-ion, NMC955",
        "market for battery capacity, Li-ion, NMC955, stationary",
    }
    select_companion_profiles(paths, importers, "3.12", "cutoff")
    assert importers[0].data == [r for r in original[0] if r["name"] not in removed]
    assert importers[1].data == [r for r in original[1] if r["name"] not in capacity]
    assert [i.data for i in importers[2:]] == original[2:]


@pytest.mark.parametrize("problem", ["duplicate", "reference output", "missing role"])
def test_companion_role_validation_is_atomic(problem):
    paths, importers = companion_sources()
    if problem == "duplicate":
        importers[2].data.append(deepcopy(importers[2].data[0]))
    if problem == "reference output":
        (production,) = [
            e for e in importers[2].data[0]["exchanges"] if e["type"] == "production"
        ]
        production["amount"] = 2
    if problem == "missing role":
        paths, importers = paths[:-1], importers[:-1]
    original = [i.data for i in importers]
    saved = deepcopy(original)
    with pytest.raises(ValueError):
        select_companion_profiles(paths, importers, "3.12", "cutoff")
    assert all(i.data is old for i, old in zip(importers, original))
    assert [i.data for i in importers] == saved


def test_unselected_companions_do_not_access_source_data():
    select_companion_profiles([("unused", "3.8")], [], "3.11", "cutoff")
    select_companion_profiles([("unused", "3.8")], [], "3.12", None)


@pytest.mark.parametrize("opt_in", [False, True])
def test_default_preparation_keeps_profile_phases_around_migration(monkeypatch, opt_in):
    from pathlib import Path
    from premise.inventory import inventory_imports as module
    from premise.inventory import profiles as profile_module

    importer = object.__new__(module.DefaultInventory)
    importer.import_db = SimpleNamespace(data=[])
    importer.path, importer.version_in, importer.version_out = (
        Path(HYDROGEN),
        "3.9",
        "3.12",
    )
    importer.system_model, importer.database = "consequential", []
    importer.source_inventories = [] if opt_in else None
    importer.keep_uncertainty_data, importer.list_unlinked = False, []
    importer.consequential_blacklist = {}
    events = []
    monkeypatch.setattr(
        profile_module,
        "apply_inventory_profiles",
        lambda *a, **kw: events.append(kw.get("phase", "raw")),
    )
    monkeypatch.setattr(
        module, "migrate_import_db", lambda *a: events.append("migration")
    )
    monkeypatch.setattr(
        module,
        "check_for_datasets_compliance_with_consequential_database",
        lambda data, *a: events.append("consequential") or data,
    )
    monkeypatch.setattr(module, "remove_categories", lambda data: data)
    monkeypatch.setattr(module, "remove_uncertainty", lambda data: data)
    monkeypatch.setattr(module, "check_for_duplicate_datasets", lambda data: data)
    for name in (
        "adapt_reviewed_38_chemical_proxies",
        "adapt_hydrogen_market_exchanges_for_legacy_versions",
        "lower_case_technosphere_exchanges",
        "add_product_field_to_exchanges",
        "check_units",
        "correct_keys",
        "add_classifications",
        "check_for_already_existing_datasets",
    ):
        monkeypatch.setattr(importer, name, lambda: None)
    monkeypatch.setattr(importer, "add_biosphere_links", lambda: events.append("links"))
    importer.prepare_inventory()
    assert events == (["raw", "migration", "migrated"] if opt_in else ["migration"]) + [
        "consequential",
        "links",
    ]
