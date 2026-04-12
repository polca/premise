import json
import sys
import types
import pickle
from pathlib import Path

import pytest

try:
    import xarray  # noqa: F401
except ModuleNotFoundError:
    xarray_stub = types.ModuleType("xarray")
    xarray_stub.DataArray = object
    xarray_stub.Dataset = object
    xarray_stub.__version__ = "0.0.0"
    sys.modules["xarray"] = xarray_stub

try:
    import schema  # noqa: F401
except ModuleNotFoundError:
    schema_stub = types.ModuleType("schema")

    class DummySchema:
        def __init__(self, *args, **kwargs):
            pass

        def validate(self, value):
            return value

    schema_stub.And = lambda *args, **kwargs: ("And", args, kwargs)
    schema_stub.Optional = lambda *args, **kwargs: ("Optional", args, kwargs)
    schema_stub.Schema = DummySchema
    schema_stub.Use = lambda *args, **kwargs: ("Use", args, kwargs)
    sys.modules["schema"] = schema_stub

import premise.new_database as new_database_module
import premise.pathways as pathways_module
from premise.new_database import NewDatabase, check_presence_biosphere_database
from premise.pathways import PathwaysDataPackage
from premise.utils import get_cache_manifest_path


class DummyIAMDataCollection:
    def __init__(self, *args, **kwargs):
        self.regions = []

    def get_external_data(self, external_scenarios):
        return {}


def _write_cache_manifest(cache_ref, *shard_files):
    manifest_path = get_cache_manifest_path(cache_ref)

    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(
            {
                "cache_format": 1,
                "storage": "pickle-shards",
                "files": [Path(shard_file).name for shard_file in shard_files],
            },
            file,
        )

    return manifest_path


def test_ecospold_constructor_does_not_check_biosphere_database(monkeypatch):
    def fail_if_called(_):
        raise AssertionError("constructor should not validate biosphere presence")

    def fake_find_cached_db(self, db_name):
        self.database_metadata_cache_filepath = Path("db-metadata.pickle")
        return []

    def fake_find_cached_inventories(self, db_name):
        self.inventories_metadata_cache_filepath = Path("inventories-metadata.pickle")
        return []

    monkeypatch.setattr(
        new_database_module,
        "check_presence_biosphere_database",
        fail_if_called,
    )
    monkeypatch.setattr(
        new_database_module,
        "check_scenarios",
        lambda scenario, key: scenario,
    )
    monkeypatch.setattr(new_database_module, "delete_all_pickles", lambda: None)
    monkeypatch.setattr(
        new_database_module,
        "IAMDataCollection",
        DummyIAMDataCollection,
    )
    monkeypatch.setattr(
        NewDatabase,
        "_NewDatabase__find_cached_db",
        fake_find_cached_db,
    )
    monkeypatch.setattr(
        NewDatabase,
        "_NewDatabase__find_cached_inventories",
        fake_find_cached_inventories,
    )

    obj = NewDatabase(
        scenarios=[
            {
                "model": "image",
                "pathway": "SSP2-Base",
                "year": 2030,
                "filepath": Path("."),
            }
        ],
        source_type="ecospold",
        source_file_path=".",
        biosphere_name="missing-biosphere",
        quiet=True,
    )

    assert obj.biosphere_name == "missing-biosphere"


def test_constructor_clears_importer_state_after_inventory_cache_miss(monkeypatch):
    calls = {"clear_inventory_importer_state": 0}

    def fake_find_cached_db(self, db_name):
        self.database_metadata_cache_filepath = Path("db-metadata.pickle")
        return []

    def fake_find_cached_inventories(self, db_name):
        self.inventories_metadata_cache_filepath = Path("inventories-metadata.pickle")
        return None

    def fake_clear_inventory_importer_state():
        calls["clear_inventory_importer_state"] += 1

    monkeypatch.setattr(
        new_database_module,
        "check_scenarios",
        lambda scenario, key: scenario,
    )
    monkeypatch.setattr(new_database_module, "delete_all_pickles", lambda: None)
    monkeypatch.setattr(
        new_database_module,
        "IAMDataCollection",
        DummyIAMDataCollection,
    )
    monkeypatch.setattr(
        NewDatabase,
        "_NewDatabase__find_cached_db",
        fake_find_cached_db,
    )
    monkeypatch.setattr(
        NewDatabase,
        "_NewDatabase__find_cached_inventories",
        fake_find_cached_inventories,
    )
    monkeypatch.setattr(
        NewDatabase,
        "_clear_inventory_importer_state",
        staticmethod(fake_clear_inventory_importer_state),
    )

    NewDatabase(
        scenarios=[
            {
                "model": "image",
                "pathway": "SSP2-Base",
                "year": 2030,
                "filepath": Path("."),
            }
        ],
        source_type="ecospold",
        source_file_path=".",
        biosphere_name="missing-biosphere",
        quiet=True,
    )

    assert calls["clear_inventory_importer_state"] == 1


def test_check_presence_biosphere_database_is_noninteractive(monkeypatch):
    monkeypatch.setattr(new_database_module.bw2data, "databases", {})
    monkeypatch.setattr(
        "builtins.input",
        lambda prompt: (_ for _ in ()).throw(AssertionError("input should not run")),
    )

    with pytest.raises(
        ValueError, match="Brightway export requires a biosphere database"
    ):
        check_presence_biosphere_database("missing-biosphere")


def test_write_db_to_brightway_requires_registered_biosphere(monkeypatch):
    monkeypatch.setattr(new_database_module.bw2data, "databases", {})

    obj = object.__new__(NewDatabase)
    obj.biosphere_name = "missing-biosphere"
    obj.scenarios = [{"model": "image", "pathway": "SSP2-Base", "year": 2030}]

    with pytest.raises(
        ValueError, match="Brightway export requires a biosphere database"
    ):
        obj.write_db_to_brightway(name=["test-db"])


def test_write_superstructure_to_brightway_requires_registered_biosphere(monkeypatch):
    monkeypatch.setattr(new_database_module.bw2data, "databases", {})

    obj = object.__new__(NewDatabase)
    obj.biosphere_name = "missing-biosphere"
    obj.scenarios = [
        {"model": "image", "pathway": "SSP2-Base", "year": 2030},
        {"model": "image", "pathway": "SSP2-Base", "year": 2035},
    ]

    with pytest.raises(
        ValueError, match="Brightway export requires a biosphere database"
    ):
        obj.write_superstructure_db_to_brightway(name="super-db")


def test_pathways_datapackage_does_not_prevalidate_biosphere_database(monkeypatch):
    captured = {}

    class DummyNewDatabase:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(pathways_module, "NewDatabase", DummyNewDatabase)
    monkeypatch.setattr(pathways_module, "get_classifications", lambda: {})

    PathwaysDataPackage(
        scenarios=[{"model": "image", "pathway": "SSP2-Base"}],
        years=[2030],
        source_type="ecospold",
        source_file_path=".",
        biosphere_name="missing-biosphere",
    )

    assert captured["biosphere_name"] == "missing-biosphere"


def test_update_uses_in_memory_base_database_for_first_scenario_when_reloadable():
    obj = object.__new__(NewDatabase)
    original_database = [{"name": "dataset"}]
    obj.database = original_database
    obj.database_cache_filepath = Path("db-cache.pickle")
    obj.inventories_cache_filepath = Path("inventories-cache.pickle")
    obj.additional_inventories = None
    obj._database_is_complete = True

    scenario = {}

    loaded = obj._load_scenario_database_for_update(scenario, scenario_position=0)

    assert loaded["database"] is original_database
    assert obj.database is None


def test_update_reloads_first_scenario_from_cache_after_cache_miss(tmp_path):
    base_cache = tmp_path / "base.pickle"
    inventories_cache = tmp_path / "inventories.pickle"

    with open(base_cache, "wb") as file:
        pickle.dump([{"name": "base-from-cache"}], file)

    with open(inventories_cache, "wb") as file:
        pickle.dump([{"name": "inventory-from-cache"}], file)

    obj = object.__new__(NewDatabase)
    original_database = [{"name": "in-memory"}]
    obj.database = original_database
    obj.database_cache_filepath = base_cache
    obj.inventories_cache_filepath = inventories_cache
    obj.additional_inventories = None
    obj._database_is_complete = True
    obj._reload_original_database_from_cache_for_update = True

    loaded = obj._load_scenario_database_for_update({}, scenario_position=0)

    assert loaded["database"] == [
        {"name": "base-from-cache"},
        {"name": "inventory-from-cache"},
    ]
    assert loaded["database"] is not original_database
    assert obj.database is None
    assert obj._reload_original_database_from_cache_for_update is False


def test_load_original_database_reloads_released_base_database_from_cache(tmp_path):
    base_cache = tmp_path / "base.pickle"
    inventories_cache = tmp_path / "inventories.pickle"

    with open(base_cache, "wb") as file:
        pickle.dump([{"name": "base"}], file)

    with open(inventories_cache, "wb") as file:
        pickle.dump([{"name": "inventory"}], file)

    obj = object.__new__(NewDatabase)
    obj.database = None
    obj.database_cache_filepath = base_cache
    obj.inventories_cache_filepath = inventories_cache
    obj.additional_inventories = None
    obj._database_is_complete = True

    loaded = obj._load_original_database()

    assert loaded == [{"name": "base"}, {"name": "inventory"}]


def test_find_cached_db_supports_manifest_bundle(monkeypatch, tmp_path):
    cache_ref = tmp_path / "cached_239_source-db_wo_uncertainty.pickle"
    metadata_ref = Path(str(cache_ref).replace(".pickle", " (metadata).pickle"))
    shard = tmp_path / "cached-db.part-a.pickle"
    metadata_shard = tmp_path / "cached-db.metadata.part-a.pickle"

    with open(shard, "wb") as file:
        pickle.dump([{"name": "base"}], file)

    with open(metadata_shard, "wb") as file:
        pickle.dump({("base", None, None): {"comment": "metadata"}}, file)

    manifest_path = _write_cache_manifest(cache_ref, shard)
    metadata_manifest_path = _write_cache_manifest(metadata_ref, metadata_shard)

    obj = object.__new__(NewDatabase)
    obj.source_type = "brightway"
    obj.keep_source_db_uncertainty = False

    monkeypatch.setattr(new_database_module, "DIR_CACHED_DB", tmp_path)

    loaded = obj._NewDatabase__find_cached_db("source-db")

    assert loaded == [{"name": "base"}]
    assert obj.database_cache_filepath == manifest_path
    assert obj.database_metadata_cache_filepath == metadata_manifest_path


def test_constructor_marks_database_complete_after_inventory_cache_miss(monkeypatch):
    def fake_find_cached_db(self, db_name):
        self.database_cache_filepath = Path("db-cache.pickle")
        self.database_metadata_cache_filepath = Path("db-metadata.pickle")
        return [{"name": "base"}]

    def fake_find_cached_inventories(self, db_name):
        self.inventories_cache_filepath = Path("inventories-cache.pickle")
        self.inventories_metadata_cache_filepath = Path("inventories-metadata.pickle")
        self.database.extend([{"name": "inventory"}])
        return None

    monkeypatch.setattr(
        new_database_module,
        "check_scenarios",
        lambda scenario, key: scenario,
    )
    monkeypatch.setattr(new_database_module, "delete_all_pickles", lambda: None)
    monkeypatch.setattr(
        new_database_module,
        "IAMDataCollection",
        DummyIAMDataCollection,
    )
    monkeypatch.setattr(
        NewDatabase,
        "_NewDatabase__find_cached_db",
        fake_find_cached_db,
    )
    monkeypatch.setattr(
        NewDatabase,
        "_NewDatabase__find_cached_inventories",
        fake_find_cached_inventories,
    )

    obj = NewDatabase(
        scenarios=[
            {
                "model": "image",
                "pathway": "SSP2-Base",
                "year": 2030,
                "filepath": Path("."),
            }
        ],
        source_db="source-db",
        source_version="3.12",
        biosphere_name="biosphere3",
        quiet=True,
    )

    assert obj._database_is_complete is True
    assert obj._can_reload_original_database() is True
    assert obj.database == [{"name": "base"}, {"name": "inventory"}]


def test_inventory_cache_miss_replaces_full_inventory_tail_with_trimmed_cache(
    monkeypatch,
):
    base_dataset = {"name": "base"}
    imported_inventory = {"name": "inventory", "extra": "full"}
    trimmed_inventory = {"name": "inventory"}

    obj = object.__new__(NewDatabase)
    obj.database = [base_dataset]
    obj.source_type = "brightway"
    obj.keep_imports_uncertainty = False

    def fake_import_inventories(self):
        self.database.extend([imported_inventory])
        return [imported_inventory]

    def fake_create_cache(data, file_name):
        return [trimmed_inventory], Path("inventories-metadata.pickle")

    monkeypatch.setattr(
        NewDatabase,
        "_NewDatabase__import_inventories",
        fake_import_inventories,
    )
    monkeypatch.setattr(new_database_module, "create_cache", fake_create_cache)

    result = obj._NewDatabase__find_cached_inventories("source-db")

    assert result is None
    assert obj.database == [base_dataset, trimmed_inventory]
    assert obj.inventories_cache_filepath.name.endswith("_inventories.pickle")
    assert obj.inventories_metadata_cache_filepath == Path(
        "inventories-metadata.pickle"
    )
    assert obj._reload_original_database_from_cache_for_update is True
