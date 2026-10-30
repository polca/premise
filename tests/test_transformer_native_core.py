"""ALIGN-024: opt-in, post-migration native transformer core for 3.12 only."""

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory
from premise.transformer_core import (
    native_transformer_core_enabled,
    use_native_transformer_core,
)

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-hydrogen-electrolysis.xlsx"
NAME = "transformer production, electrolysis, 1.1 mva"
NATIVE = (
    "market for grain oriented electrical steel, 0.23 mm coil",
    "grain oriented electrical steel, 0.23 mm coil",
    "RER",
    "kilogram",
)
CORE_NAMES = {
    "market for steel, unalloyed",
    "market for silicon, metallurgical grade",
    "market for hot rolling, steel",
    "sheet rolling, steel",
    "market group for electricity, medium voltage",
    "heat production, natural gas, at industrial furnace >100kW",
}


def snapshot(value):
    return json.loads(json.dumps(value, sort_keys=True))


def transformer(source):
    (match,) = [d for d in source.data if d["name"] == NAME]
    return match


def identity(record):
    return tuple(record[k] for k in ("name", "reference product", "location", "unit"))


def supplier(key=NATIVE):
    return dict(
        zip(("name", "reference product", "location", "unit"), key), exchanges=[]
    )


def state(source):
    # Preserve strategy callable identities: copying functools.partial would
    # create distinct objects even when the importer has not changed.
    return {
        k: tuple(v) if k == "strategies" else snapshot(v)
        for k, v in vars(source).items()
    }


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (
            ROOT / "scripts/data/transformer-native-core-adoption-20261027.json"
        ).read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


@pytest.fixture
def source(adoption):
    return SimpleNamespace(
        data=[deepcopy(adoption["migrated_snapshot"])], source_version="3.9"
    )


def assert_atomic_failure(source, background):
    original, saved, native = source.data, state(source), deepcopy(background)
    with pytest.raises(ValueError):
        use_native_transformer_core(source, "3.12", background)
    assert source.data is original
    assert state(source) == saved
    assert background == native


def test_portable_reference_pins_unchanged_source_profile_and_clic_core(adoption, raw):
    assert adoption["approval"] == "ALIGN-024; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK

    assert transformer(raw) == adoption["legacy_snapshot"]
    assert adoption["migrated_snapshot"] == adoption["legacy_snapshot"]
    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["target_version"] == "3.12"
    assert tuple(adoption["native_identity"]) == NATIVE
    assert adoption["core_kg"] == 947.32
    assert (adoption["CLIC_dataset_id"], adoption["CLIC_version_id"]) == (3703, 5618)

    (core,) = [
        e for e in adoption["CLIC_snapshot"]["exchanges"] if e["name"] == NATIVE[0]
    ]
    assert identity(core) == NATIVE and core["amount"] == 947.32


def test_raw_selection_changes_only_six_core_flows_and_qualified_comment(raw, adoption):
    source = deepcopy(raw)
    old_data, original = source.data, deepcopy(transformer(source))
    flags = {k: v for k, v in state(source).items() if k != "data"}
    background = [supplier(), supplier((*NATIVE[:2], "RoW", NATIVE[3]))]
    saved = deepcopy(background)
    assert use_native_transformer_core(source, "3.12", background) == 1
    selected = transformer(source)
    assert len(old_data) == len(source.data) == 60
    assert [d for d in source.data if d["name"] != NAME] == [
        d for d in old_data if d["name"] != NAME
    ]
    assert all(a is b for a, b in zip(old_data, source.data) if a["name"] != NAME)
    assert transformer(SimpleNamespace(data=old_data)) == original
    assert {k: v for k, v in state(source).items() if k != "data"} == flags
    assert background == saved
    retained = [e for e in original["exchanges"] if e["name"] not in CORE_NAMES]
    assert len(original["exchanges"]) == 19 and len(retained) == 13
    assert len(selected["exchanges"]) == 14
    assert [e for e in selected["exchanges"] if e["name"] != NATIVE[0]] == retained
    assert {k: v for k, v in selected.items() if k not in {"comment", "exchanges"}} == {
        k: v for k, v in original.items() if k not in {"comment", "exchanges"}
    }
    expected = deepcopy(adoption["native_snapshot"])
    expected["comment"] = selected["comment"]
    assert selected == expected
    assert selected["comment"].endswith(original["comment"])
    note = selected["comment"][: -len(original["comment"])]
    assert note.startswith("APPROVED NATIVE TRANSFORMER-CORE SUPPLY FOR TARGET 3.12")
    for term in ("3.2%", "3%", "grade", "proxies", "unverified", "3.9"):
        assert term in note
    (core,) = [e for e in selected["exchanges"] if e["name"] == NATIVE[0]]
    assert identity(core) == NATIVE and core["amount"] == 947.32
    assert "Qualified" in core["comment"]
    assert sum(e["amount"] for e in original["exchanges"][1:3]) == pytest.approx(
        947.32, rel=1e-14
    )
    after = state(source)
    assert use_native_transformer_core(source, "3.12", background) == 0
    assert state(source) == after


@pytest.mark.parametrize(
    "version", (None, "3.9", "3.10", "3.10.1", "3.11", "3.13", "3.120", "4.0")
)
def test_unreviewed_versions_are_strict_noops_even_for_invalid_content(source, version):
    source.data.append({"name": NAME, "exchanges": "unreviewed"})
    before = state(source)
    assert native_transformer_core_enabled(version) is False
    assert use_native_transformer_core(source, version, []) == 0
    assert state(source) == before


@pytest.mark.parametrize("version", ("3.12", "3.12.0", "3.12.1", "3.12rc1"))
def test_only_reviewed_release_family_selects_native(source, version):
    assert native_transformer_core_enabled(version) is True
    assert use_native_transformer_core(source, version, [supplier()]) == 1


@pytest.mark.parametrize(
    "change",
    (
        "core_amount",
        "other_amount",
        "formula",
        "uncertainty",
        "unknown_field",
        "duplicate_input",
        "extra_input",
        "missing_input",
        "product_alias",
        "production_amount",
        "production_exchange",
        "activity_product_alias",
    ),
)
def test_complete_scientific_recipe_guards_fail_before_mutation(source, change):
    data = transformer(source)
    core = data["exchanges"][1]
    if change == "core_amount":
        core["amount"] += 1
    elif change == "other_amount":
        data["exchanges"][-1]["amount"] += 1
    elif change == "formula":
        core["formula"] = "amount * 2"
    elif change == "uncertainty":
        core.update({"uncertainty type": 2, "scale": 0.1})
    elif change == "unknown_field":
        core["unreviewed scientific field"] = 1
    elif change == "duplicate_input":
        data["exchanges"].append(deepcopy(core))
    elif change == "extra_input":
        data["exchanges"].append(dict(core, name="unreviewed input"))
    elif change == "missing_input":
        data["exchanges"].pop()
    elif change == "product_alias":
        core["product"] = "different product"
    elif change == "production_amount":
        data["production amount"] = 2
    elif change == "production_exchange":
        data["exchanges"][0]["amount"] = 2
    elif change == "activity_product_alias":
        data["product"] = "different product"
    assert_atomic_failure(source, [supplier()])


@pytest.mark.parametrize(
    "failure",
    (
        "missing_transformer",
        "duplicate_transformer",
        "missing_native",
        "wrong_region",
        "wrong_product",
        "wrong_unit",
        "native_alias",
        "duplicate_native",
    ),
)
def test_requires_one_transformer_and_one_exact_native_supplier(source, failure):
    background = [supplier()]
    if failure == "missing_transformer":
        source.data.clear()
    elif failure == "duplicate_transformer":
        source.data.append(deepcopy(source.data[0]))
    elif failure == "missing_native":
        background.clear()
    elif failure == "wrong_region":
        background[0]["location"] = "GLO"
    elif failure == "wrong_product":
        background[0]["reference product"] = "other grade"
    elif failure == "wrong_unit":
        background[0]["unit"] = "unit"
    elif failure == "native_alias":
        background[0]["product"] = "other grade"
    elif failure == "duplicate_native":
        background.append(deepcopy(background[0]))
    assert_atomic_failure(source, background)


@pytest.mark.parametrize(
    "corruption", (None, "amount", "formula", "uncertainty", "missing_native")
)
def test_already_native_is_validated_before_idempotent_return(
    source, adoption, corruption
):
    source.data = [deepcopy(adoption["native_snapshot"])]
    background = [supplier()]
    core = transformer(source)["exchanges"][1]
    if corruption == "amount":
        core["amount"] += 1
    elif corruption == "formula":
        core["formula"] = "947.32"
    elif corruption == "uncertainty":
        core["uncertainty type"] = 2
    elif corruption == "missing_native":
        background.clear()
    if corruption is not None:
        assert_atomic_failure(source, background)
    else:
        before, saved = state(source), deepcopy(background)
        assert use_native_transformer_core(source, "3.12", background) == 0
        assert state(source) == before and background == saved


def background_for(adoption):
    keys = {
        identity(e)
        for tag in ("legacy_snapshot", "native_snapshot")
        for e in adoption[tag]["exchanges"]
        if e["type"] == "technosphere"
    }
    return [supplier(key) for key in sorted(keys)]


@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_calls_hook_after_migration_before_consequential_and_links(
    adoption, system_model, monkeypatch
):
    import premise.inventory_imports as module

    source = SimpleNamespace(data=[deepcopy(adoption["legacy_snapshot"])])
    background = background_for(adoption)
    saved, events = deepcopy(background), []
    migrate = module.migrate_import_db
    consequence = module.check_for_datasets_compliance_with_consequential_database
    links = DefaultInventory.add_biosphere_links

    def observe_migration(importer, version_in, version_out):
        assert (version_in, version_out) == ("3.9", "3.12")
        result = migrate(importer, version_in, version_out)
        events.append("migration")
        return result

    def callback(importer, version, database):
        assert events == ["migration"]
        assert importer is source and database is background
        assert transformer(importer) == adoption["migrated_snapshot"]
        assert use_native_transformer_core(importer, version, database) == 1
        events.append("callback")

    def observe_consequence(*args, **kwargs):
        assert events == ["migration", "callback"]
        events.append("consequential")
        return consequence(*args, **kwargs)

    def observe_links(importer):
        assert "callback" in events
        events.append("links")
        return links(importer)

    monkeypatch.setattr(module, "migrate_import_db", observe_migration)
    monkeypatch.setattr(
        module,
        "check_for_datasets_compliance_with_consequential_database",
        observe_consequence,
    )
    monkeypatch.setattr(DefaultInventory, "add_biosphere_links", observe_links)
    importer = DefaultInventory(
        background,
        "3.9",
        "3.12",
        INVENTORY_DIR / WORKBOOK,
        system_model,
        True,
        preloaded_importer=source,
        post_migration=callback,
    )
    importer.prepare_inventory()
    assert events == [
        "migration",
        "callback",
        *(["consequential"] if system_model == "consequential" else []),
        "links",
    ]
    assert importer.version_in == "3.9" and importer.version_out == "3.12"
    assert importer.list_unlinked == [] and background == saved
    prepared = transformer(importer.import_db)
    assert len(prepared["exchanges"]) == 14
    assert [(identity(e), e["amount"]) for e in prepared["exchanges"]] == [
        (identity(e), e["amount"]) for e in adoption["native_snapshot"]["exchanges"]
    ]


def test_default_inventory_without_callback_preserves_legacy_recipe(adoption):
    source = SimpleNamespace(data=[deepcopy(adoption["legacy_snapshot"])])
    importer = DefaultInventory(
        background_for(adoption),
        "3.9",
        "3.12",
        INVENTORY_DIR / WORKBOOK,
        "cutoff",
        True,
        preloaded_importer=source,
    )
    importer.prepare_inventory()
    assert len(transformer(importer.import_db)["exchanges"]) == 19
    assert not any(
        e["name"] == NATIVE[0] for e in transformer(importer.import_db)["exchanges"]
    )


@pytest.mark.parametrize("version", ("3.9", "3.12"))
def test_actual_default_caller_opts_in_only_hydrogen_with_original_39_source(
    version, adoption, monkeypatch
):
    import premise.new_database as module

    observed = []

    def extract(filepaths):
        # Identical transformer records in unrelated workbooks must not opt in.
        return [
            SimpleNamespace(data=[deepcopy(adoption["legacy_snapshot"])])
            for _ in filepaths
        ]

    class ObserveInventory:
        def __init__(self, **kwargs):
            observed.append(kwargs)
            self.list_unlinked = []

        def merge_inventory(self):
            return []

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract)
    monkeypatch.setattr(module, "DefaultInventory", ObserveInventory)
    monkeypatch.setattr(module, "use_native_ptfe_gasket", lambda *args: None)
    monkeypatch.setattr(module, "_select_nmc532_native_312", lambda *args: None)
    monkeypatch.setattr(module, "use_native_fkm_gasket", lambda *args, **kwargs: None)
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, "cutoff"
    obj.keep_imports_uncertainty, obj._database = True, []
    assert obj._NewDatabase__import_inventories() == []
    (hydrogen,) = [d for d in observed if Path(d["path"]).name == WORKBOOK]
    assert hydrogen["version_in"] == "3.9"
    assert hydrogen["post_migration"] is use_native_transformer_core
    assert all(d["post_migration"] is None for d in observed if d is not hydrogen)


@pytest.mark.parametrize("version", ("3.9", "3.10", "3.11", "3.12", "3.12.1", "3.13"))
def test_cache_revision_invalidates_only_reviewed_family_imports(version, monkeypatch):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    base = obj._database_cache_path("ALIGN024")
    imports = obj._database_cache_path("ALIGN024", inventories=True)
    monkeypatch.setattr(
        module,
        "TRANSFORMER_CORE_CACHE_REVISION",
        module.TRANSFORMER_CORE_CACHE_REVISION + 1,
    )
    assert obj._database_cache_path("ALIGN024") == base
    changed = obj._database_cache_path("ALIGN024", inventories=True)
    assert (changed != imports) is version.startswith("3.12")


def test_native_selection_invalidates_legacy_cache_without_revision_token(monkeypatch):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = "3.12"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    # Reproduce the pre-hook key, whose inventory digest had no core token.
    with monkeypatch.context() as legacy:
        legacy.setattr(module, "native_transformer_core_enabled", lambda version: False)
        old_source = obj._database_cache_path("ALIGN024")
        old_imports = obj._database_cache_path("ALIGN024", inventories=True)
    assert module.native_transformer_core_enabled(obj.version)
    assert obj._database_cache_path("ALIGN024") == old_source
    assert obj._database_cache_path("ALIGN024", inventories=True) != old_imports
