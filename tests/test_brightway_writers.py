from copy import deepcopy
import sqlite3

import numpy as np
import pytest

import premise.brightway2 as brightway2_module
import premise.brightway25 as brightway25_module
from premise._fast_sqlite import fast_sqlite_settings
from premise.inventory_store import CompactInventoryStore, InventoryStore


class _SQLiteAdapter:
    def __init__(self, connection):
        self.connection = connection

    def execute_sql(self, statement):
        return self.connection.execute(statement)


def test_fast_sqlite_settings_apply_and_restore_all_pragmas(tmp_path):
    connection = sqlite3.connect(tmp_path / "fast-settings.sqlite")
    database = _SQLiteAdapter(connection)
    original = {
        name: connection.execute(f"PRAGMA {name};").fetchone()[0]
        for name in (
            "journal_mode",
            "synchronous",
            "temp_store",
            "cache_size",
            "foreign_keys",
        )
    }

    with fast_sqlite_settings(database, cache_mib=16):
        assert connection.execute("PRAGMA journal_mode;").fetchone()[0] == "memory"
        assert connection.execute("PRAGMA synchronous;").fetchone()[0] == 0
        assert connection.execute("PRAGMA temp_store;").fetchone()[0] == 2
        assert connection.execute("PRAGMA cache_size;").fetchone()[0] == -(16 * 1024)
        assert connection.execute("PRAGMA foreign_keys;").fetchone()[0] == 0

    restored = {
        name: connection.execute(f"PRAGMA {name};").fetchone()[0] for name in original
    }
    connection.close()
    assert restored == original


def test_fast_writer_preserves_primary_exception_when_cleanup_fails(monkeypatch):
    primary = RuntimeError("row insertion failed")

    monkeypatch.setattr(
        brightway25_module,
        "fast_sqlite_settings",
        lambda database: __import__("contextlib").nullcontext(),
    )
    monkeypatch.setattr(
        brightway25_module,
        "_write_processed_database_fast_impl",
        lambda *args, **kwargs: (_ for _ in ()).throw(primary),
    )
    monkeypatch.setattr(
        brightway25_module,
        "_cleanup_failed_fast_database",
        lambda name: (_ for _ in ()).throw(RuntimeError("cleanup failed")),
    )

    with pytest.raises(RuntimeError, match="row insertion failed") as error:
        brightway25_module._write_processed_database_fast([], "failed-db")

    assert error.value is primary


def test_collect_fast_export_geography_discards_unknown_geocollections():
    data = [
        {"type": "process", "location": "CH"},
        {"type": "process", "location": "UNKNOWN"},
        {"type": "product", "location": "GLO"},
        {"type": "process", "location": None},
    ]

    def get_geocollection(location):
        if location == "UNKNOWN":
            raise KeyError("Can't find location: UNKNOWN.")
        return {"CH": "ecoinvent"}.get(location)

    geocollections, locations = brightway25_module._collect_fast_export_geography(
        data=data,
        process_node_types={"process"},
        get_geocollection=get_geocollection,
    )

    assert geocollections == ["ecoinvent"]
    assert locations == {"CH", "UNKNOWN", "GLO"}


def test_brightway2_collect_fast_export_geography_uses_compat_geocollections():
    data = [
        {"type": "process", "location": "CH"},
        {"type": "process", "location": ("ecoinvent", "RER")},
        {"type": "process", "location": "CUSTOM"},
        {"type": "process", "location": None},
    ]

    geocollections, locations = brightway2_module._collect_fast_export_geography(data)

    assert geocollections == ["ecoinvent", "world"]
    assert locations == {"CH", ("ecoinvent", "RER"), "CUSTOM"}


def test_write_brightway25_database_fast_prints_completion_message(monkeypatch, capsys):
    calls = {
        "change_db_name": None,
        "check_internal": 0,
        "compact": None,
        "write": None,
    }

    monkeypatch.setattr(
        brightway25_module,
        "change_db_name",
        lambda data, name: calls.__setitem__("change_db_name", (data, name)),
    )
    monkeypatch.setattr(brightway25_module, "link_internal", lambda data: None)
    monkeypatch.setattr(
        brightway25_module,
        "check_internal_linking",
        lambda data: calls.__setitem__("check_internal", calls["check_internal"] + 1),
    )
    monkeypatch.setattr(
        brightway25_module,
        "_compact_payload_for_fast_write",
        lambda data, name: calls.__setitem__("compact", (data, name)),
    )
    monkeypatch.setattr(
        brightway25_module,
        "_write_processed_database_fast",
        lambda data, name, **kwargs: calls.__setitem__("write", (data, name, kwargs)),
    )

    data = [{"code": "a", "exchanges": []}]

    brightway25_module.write_brightway_database(
        data=data,
        name="fast-db",
        fast=True,
        check_internal=True,
    )

    assert calls["change_db_name"] is None
    assert calls["check_internal"] == 1
    assert calls["compact"] == (data, "fast-db")
    assert calls["write"] == (
        data,
        "fast-db",
        {"exchange_payloads_prepared": True},
    )
    assert "Brightway database written: fast-db" in capsys.readouterr().out


def test_write_brightway25_database_fast_prints_overwrite_message(monkeypatch, capsys):
    monkeypatch.setattr(brightway25_module, "databases", {"fast-db": {}})
    monkeypatch.setattr(brightway25_module, "change_db_name", lambda data, name: None)
    monkeypatch.setattr(brightway25_module, "link_internal", lambda data: None)
    monkeypatch.setattr(brightway25_module, "check_internal_linking", lambda data: None)
    monkeypatch.setattr(
        brightway25_module,
        "_compact_payload_for_fast_write",
        lambda data, name: None,
    )
    monkeypatch.setattr(
        brightway25_module,
        "_write_processed_database_fast",
        lambda data, name, **kwargs: None,
    )

    brightway25_module.write_brightway_database(
        data=[{"code": "a", "exchanges": []}],
        name="fast-db",
        fast=True,
        check_internal=True,
    )

    output = capsys.readouterr().out
    assert "Database fast-db already exists: it will be overwritten." in output
    assert "Brightway database written: fast-db" in output


def test_write_brightway2_database_prints_completion_message(monkeypatch, capsys):
    calls = {"change_db_name": None, "check_internal": 0, "write": 0}

    monkeypatch.setattr(
        brightway2_module,
        "change_db_name",
        lambda data, name: calls.__setitem__("change_db_name", (data, name)),
    )
    monkeypatch.setattr(brightway2_module, "link_internal", lambda data: None)
    monkeypatch.setattr(
        brightway2_module,
        "check_internal_linking",
        lambda data: calls.__setitem__("check_internal", calls["check_internal"] + 1),
    )

    class DummyImporter:
        def __init__(self, name, data):
            self.name = name
            self.data = data

        def write_database(self):
            calls["write"] += 1

    monkeypatch.setattr(brightway2_module, "BW2Importer", DummyImporter)

    data = [{"code": "a", "exchanges": []}]

    brightway2_module.write_brightway_database(
        data=data,
        name="bw2-db",
        fast=False,
        check_internal=True,
    )

    assert calls["change_db_name"] == (data, "bw2-db")
    assert calls["check_internal"] == 1
    assert calls["write"] == 1
    assert "Brightway database written: bw2-db" in capsys.readouterr().out


def test_write_brightway2_database_normalizes_process_like_types_before_import(
    monkeypatch,
):
    captured = {}

    monkeypatch.setattr(brightway2_module, "change_db_name", lambda data, name: None)
    monkeypatch.setattr(brightway2_module, "link_internal", lambda data: None)
    monkeypatch.setattr(brightway2_module, "check_internal_linking", lambda data: None)

    class DummyImporter:
        def __init__(self, name, data):
            captured["name"] = name
            captured["data"] = data

        def write_database(self):
            return None

    monkeypatch.setattr(brightway2_module, "BW2Importer", DummyImporter)

    data = [
        {
            "database": "source-db",
            "code": "a",
            "name": "activity",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "type": "processwithreferenceproduct",
            "exchanges": [],
        }
    ]

    brightway2_module.write_brightway_database(
        data=data,
        name="bw2-db",
        fast=False,
        check_internal=True,
    )

    assert captured["name"] == "bw2-db"
    assert captured["data"][0]["type"] == "process"


def test_write_brightway2_database_sets_geocollections_metadata(monkeypatch):
    calls = {"compact": 0, "write": 0}

    monkeypatch.setattr(brightway2_module, "change_db_name", lambda data, name: None)
    monkeypatch.setattr(brightway2_module, "link_internal", lambda data: None)
    monkeypatch.setattr(brightway2_module, "check_internal_linking", lambda data: None)
    monkeypatch.setattr(
        brightway2_module,
        "_compact_payload_for_fast_write",
        lambda data: calls.__setitem__("compact", calls["compact"] + 1),
    )

    class DummyImporter:
        def __init__(self, name, data):
            self.name = name
            self.data = data

        def write_database(self):
            calls["write"] += 1
            brightway2_module.databases[self.name] = {}

    monkeypatch.setattr(brightway2_module, "BW2Importer", DummyImporter)

    class DummyDatabases(dict):
        def flush(self):
            return None

    monkeypatch.setattr(brightway2_module, "databases", DummyDatabases())

    data = [
        {"code": "a", "location": "CH", "type": "process", "exchanges": []},
        {
            "code": "b",
            "location": ("ecoinvent", "RER"),
            "type": "process",
            "exchanges": [],
        },
        {"code": "c", "location": "CUSTOM", "type": "process", "exchanges": []},
    ]

    brightway2_module.write_brightway_database(
        data=data,
        name="bw2-db",
        fast=True,
        check_internal=True,
    )

    assert calls["compact"] == 1
    assert calls["write"] == 1
    assert brightway2_module.databases["bw2-db"]["geocollections"] == [
        "ecoinvent",
        "world",
    ]


def test_brightway2_fast_compaction_preserves_nonempty_metadata():
    data = [
        {
            "database": "source-db",
            "code": "act-1",
            "name": "activity",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "type": "process",
            "comment": "activity comment",
            "classifications": [("ISIC rev.4 ecoinvent", "1234")],
            "parameters": [{"name": "alpha", "amount": 1.0}],
            "custom metadata": {"region": "alpine"},
            "blank field": "",
            "empty field": None,
            "exchanges": [
                {
                    "name": "supplier",
                    "product": "product",
                    "unit": "kilogram",
                    "location": "CH",
                    "amount": 1.0,
                    "type": "technosphere",
                    "input": ("source-db", "act-2"),
                    "output": ("source-db", "act-1"),
                    "comment": "exchange comment",
                    "properties": {"source": "premise"},
                    "custom metadata": {"tag": "kept"},
                    "blank field": "",
                    "empty field": None,
                }
            ],
        }
    ]

    compacted = deepcopy(data)
    brightway2_module._compact_payload_for_fast_write(compacted)

    dataset = compacted[0]
    exchange = dataset["exchanges"][0]

    assert dataset["comment"] == "activity comment"
    assert dataset["classifications"] == [("ISIC rev.4 ecoinvent", "1234")]
    assert dataset["parameters"] == [{"name": "alpha", "amount": 1.0}]
    assert dataset["custom metadata"] == {"region": "alpine"}
    assert "blank field" not in dataset
    assert "empty field" not in dataset

    assert exchange["name"] == "supplier"
    assert exchange["product"] == "product"
    assert exchange["unit"] == "kilogram"
    assert exchange["location"] == "CH"
    assert exchange["output"] == ("source-db", "act-1")
    assert exchange["comment"] == "exchange comment"
    assert exchange["properties"] == {"source": "premise"}
    assert exchange["custom metadata"] == {"tag": "kept"}
    assert "blank field" not in exchange
    assert "empty field" not in exchange


def test_brightway2_fast_compaction_keeps_required_descriptive_fields():
    data = [
        {
            "database": "source-db",
            "code": "act-1",
            "name": None,
            "reference product": None,
            "location": None,
            "unit": None,
            "type": None,
            "exchanges": [
                {
                    "name": None,
                    "product": None,
                    "unit": None,
                    "location": None,
                    "amount": 1.0,
                    "type": "production",
                    "input": ("source-db", "act-1"),
                    "output": ("source-db", "act-1"),
                }
            ],
        }
    ]

    compacted = deepcopy(data)
    brightway2_module._compact_payload_for_fast_write(compacted)

    dataset = compacted[0]
    exchange = dataset["exchanges"][0]

    assert dataset["name"] == ""
    assert dataset["reference product"] == ""
    assert dataset["location"] == ""
    assert dataset["unit"] == ""
    assert dataset["type"] == "process"

    assert exchange["name"] == ""
    assert exchange["product"] == ""
    assert exchange["unit"] == ""
    assert exchange["location"] == ""
    assert exchange["output"] == ("source-db", "act-1")


def test_brightway2_fast_compaction_normalizes_process_type_without_production():
    data = [
        {
            "database": "source-db",
            "code": "act-1",
            "name": "activity",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "type": None,
            "exchanges": [],
        }
    ]

    compacted = deepcopy(data)
    brightway2_module._compact_payload_for_fast_write(compacted)

    assert compacted[0]["type"] == "process"


def test_brightway2_fast_compaction_coerces_chimaera_type_to_process():
    data = [
        {
            "database": "source-db",
            "code": "act-1",
            "name": "activity",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "type": "processwithreferenceproduct",
            "exchanges": [],
        }
    ]

    compacted = deepcopy(data)
    brightway2_module._compact_payload_for_fast_write(compacted)

    assert compacted[0]["type"] == "process"


def test_brightway25_fast_exchange_payload_preserves_nonempty_metadata():
    exchange = {
        "name": "supplier",
        "product": "product",
        "unit": "kilogram",
        "location": "CH",
        "amount": 1.0,
        "type": "technosphere",
        "input": ("source-db", "act-2"),
        "output": ("source-db", "act-1"),
        "comment": "exchange comment",
        "properties": {"source": "premise"},
        "custom metadata": {"tag": "kept"},
        "blank field": "",
        "empty field": None,
    }

    compact_exchange = brightway25_module._prepare_fast_exchange_payload(exchange)

    assert compact_exchange["name"] == "supplier"
    assert compact_exchange["product"] == "product"
    assert compact_exchange["unit"] == "kilogram"
    assert compact_exchange["location"] == "CH"
    assert compact_exchange["output"] == ("source-db", "act-1")
    assert compact_exchange["comment"] == "exchange comment"
    assert compact_exchange["properties"] == {"source": "premise"}
    assert compact_exchange["custom metadata"] == {"tag": "kept"}
    assert "blank field" not in compact_exchange
    assert "empty field" not in compact_exchange


def test_brightway25_fast_exchange_payload_keeps_required_descriptive_fields():
    exchange = {
        "name": None,
        "product": None,
        "unit": None,
        "location": None,
        "amount": 1.0,
        "type": "technosphere",
        "input": ("source-db", "act-2"),
        "output": ("source-db", "act-1"),
    }

    compact_exchange = brightway25_module._prepare_fast_exchange_payload(exchange)

    assert compact_exchange["name"] == ""
    assert compact_exchange["product"] == ""
    assert compact_exchange["unit"] == ""
    assert compact_exchange["location"] == ""
    assert compact_exchange["output"] == ("source-db", "act-1")


def test_brightway25_fast_exchange_payload_normalizes_no_uncertainty_loc():
    exchange = {
        "name": "supplier",
        "product": "product",
        "unit": "kilogram",
        "location": "CH",
        "amount": 1.23,
        "type": "technosphere",
        "input": ("source-db", "act-2"),
        "output": ("source-db", "act-1"),
        "uncertainty type": 0,
        "loc": 0,
        "scale": 2,
        "minimum": 0,
        "maximum": 10,
    }

    compact_exchange = brightway25_module._prepare_fast_exchange_payload(exchange)

    assert compact_exchange["loc"] == 1.23
    assert "scale" not in compact_exchange
    assert "minimum" not in compact_exchange
    assert "maximum" not in compact_exchange


def test_brightway25_fast_exchange_payload_applies_exchange_schema_cleanup():
    technosphere = {
        "name": "supplier",
        "product": "product",
        "unit": "kilogram",
        "location": "CH",
        "categories": ("unused",),
        "amount": np.float64(1.0),
        "type": "technosphere",
        "input": ("source-db", "act-2"),
        "output": ("source-db", "act-1"),
    }
    biosphere = {
        "name": "Carbon dioxide, fossil",
        "product": "unused",
        "unit": "kilogram",
        "location": "unused",
        "categories": ("air", "urban air close to ground"),
        "amount": np.float64(2.0),
        "type": "biosphere",
        "input": ("biosphere3", "flow-1"),
        "output": ("source-db", "act-1"),
    }

    compact_technosphere = brightway25_module._prepare_fast_exchange_payload(
        technosphere
    )
    compact_biosphere = brightway25_module._prepare_fast_exchange_payload(biosphere)

    assert "categories" not in compact_technosphere
    assert compact_technosphere["amount"] == 1.0
    assert type(compact_technosphere["amount"]) is float
    assert "product" not in compact_biosphere
    assert "location" not in compact_biosphere
    assert compact_biosphere["categories"] == (
        "air",
        "urban air close to ground",
    )
    assert compact_biosphere["amount"] == 2.0
    assert type(compact_biosphere["amount"]) is float


def test_brightway2_fast_exchange_payload_applies_exchange_schema_cleanup():
    technosphere = {
        "name": "supplier",
        "product": "product",
        "unit": "kilogram",
        "location": "CH",
        "categories": ("unused",),
        "amount": np.float64(1.0),
        "type": "technosphere",
        "input": ("source-db", "act-2"),
        "output": ("source-db", "act-1"),
    }
    biosphere = {
        "name": "Carbon dioxide, fossil",
        "product": "unused",
        "unit": "kilogram",
        "location": "unused",
        "categories": ("air", "urban air close to ground"),
        "amount": np.float64(2.0),
        "type": "biosphere",
        "input": ("biosphere3", "flow-1"),
        "output": ("source-db", "act-1"),
    }

    compact_technosphere = brightway2_module._prepare_fast_exchange_payload(
        technosphere
    )
    compact_biosphere = brightway2_module._prepare_fast_exchange_payload(biosphere)

    assert "categories" not in compact_technosphere
    assert type(compact_technosphere["amount"]) is float
    assert "product" not in compact_biosphere
    assert "location" not in compact_biosphere
    assert compact_biosphere["categories"] == (
        "air",
        "urban air close to ground",
    )
    assert type(compact_biosphere["amount"]) is float


def test_brightway2_fast_exchange_payload_normalizes_no_uncertainty_loc():
    exchange = {
        "name": "supplier",
        "product": "product",
        "unit": "kilogram",
        "location": "CH",
        "amount": 2.5,
        "type": "technosphere",
        "input": ("source-db", "act-2"),
        "output": ("source-db", "act-1"),
        "uncertainty type": 1,
        "loc": 0,
        "shape": 1,
    }

    compact_exchange = brightway2_module._prepare_fast_exchange_payload(exchange)

    assert compact_exchange["loc"] == 2.5
    assert "shape" not in compact_exchange


def test_brightway25_fast_compaction_preserves_nonempty_activity_metadata(
    monkeypatch,
):
    monkeypatch.setattr(
        "bw2data.utils.set_correct_process_type",
        lambda dataset: dataset.setdefault("type", "process"),
    )

    data = [
        {
            "database": "source-db",
            "code": "act-1",
            "name": "activity",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "comment": "activity comment",
            "classifications": [("ISIC rev.4 ecoinvent", "1234")],
            "parameters": [{"name": "alpha", "amount": 1.0}],
            "custom metadata": {"region": "alpine"},
            "blank field": "",
            "empty field": None,
            "exchanges": [
                {
                    "name": "supplier",
                    "product": "product",
                    "unit": "kilogram",
                    "location": "CH",
                    "amount": 1.0,
                    "type": "technosphere",
                    "input": ("source-db", "act-2"),
                }
            ],
        }
    ]

    compacted = deepcopy(data)
    brightway25_module._compact_payload_for_fast_write(compacted, "fast-db")

    dataset = compacted[0]

    assert dataset["type"] == "process"
    assert dataset["comment"] == "activity comment"
    assert dataset["classifications"] == [("ISIC rev.4 ecoinvent", "1234")]
    assert dataset["parameters"] == [{"name": "alpha", "amount": 1.0}]
    assert dataset["custom metadata"] == {"region": "alpine"}
    assert "blank field" not in dataset
    assert "empty field" not in dataset


def test_brightway25_fast_compaction_keeps_required_descriptive_fields(
    monkeypatch,
):
    monkeypatch.setattr(
        "bw2data.utils.set_correct_process_type",
        lambda dataset: dataset.__setitem__("type", dataset.get("type") or "process"),
    )

    data = [
        {
            "database": "source-db",
            "code": "act-1",
            "name": None,
            "reference product": None,
            "location": None,
            "unit": None,
            "exchanges": [],
        }
    ]

    compacted = deepcopy(data)
    brightway25_module._compact_payload_for_fast_write(compacted, "fast-db")

    dataset = compacted[0]

    assert dataset["name"] == ""
    assert dataset["reference product"] == ""
    assert dataset["location"] == ""
    assert dataset["unit"] == ""
    assert dataset["type"] == "process"


def test_brightway25_fast_compaction_streams_columnar_exchange_views(tmp_path):
    data = [
        {
            "database": "source-db",
            "code": "act-1",
            "name": "activity",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "comment": "remains lazy during payload preparation",
            "exchanges": [
                {
                    "name": "activity",
                    "product": "product",
                    "unit": "kilogram",
                    "location": "CH",
                    "amount": 1.0,
                    "type": "production",
                    "input": ("source-db", "act-1"),
                    "output": ("source-db", "act-1"),
                }
            ],
        }
    ]
    checkpoint = CompactInventoryStore(data).checkpoint(
        tmp_path / "scenario.inventory-store"
    )
    columnar = InventoryStore.open(checkpoint)._checkout_materialized()
    exchange = columnar[0]["exchanges"][0]
    storage = columnar[0]._storage

    brightway25_module._compact_payload_for_fast_write(columnar, "fast-db")

    assert columnar[0]["exchanges"][0] is exchange
    assert type(exchange).__name__ == "_ColumnarExchangeMapping"
    assert columnar[0]["type"] == "processwithreferenceproduct"
    assert len(storage._activity_cache) == 0
    assert brightway25_module._prepare_fast_exchange_payload(
        exchange
    ) == brightway25_module._prepare_fast_exchange_payload(data[0]["exchanges"][0])


def test_brightway25_fast_exchange_payload_uses_materialization_protocol():
    class LazyExchange(dict):
        def _premise_fast_export_payload(self):
            return {
                "name": "supplier",
                "product": "product",
                "unit": "kilogram",
                "location": "CH",
                "amount": 1.0,
                "type": "technosphere",
                "input": ("fast-db", "supplier"),
            }

        def items(self):
            raise AssertionError("generic mapping iteration should not be used")

    payload = brightway25_module._prepare_fast_exchange_payload(LazyExchange())

    assert payload["input"] == ("fast-db", "supplier")
    assert payload["amount"] == 1.0
