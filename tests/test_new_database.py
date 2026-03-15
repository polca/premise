import sys
import types
from pathlib import Path

import pytest

try:
    import xarray  # noqa: F401
except ModuleNotFoundError:
    xarray_stub = types.ModuleType("xarray")
    xarray_stub.DataArray = object
    xarray_stub.Dataset = object
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


class DummyIAMDataCollection:
    def __init__(self, *args, **kwargs):
        self.regions = []

    def get_external_data(self, external_scenarios):
        return {}


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
