from copy import deepcopy

import premise.brightway2 as brightway2_module
import premise.brightway25 as brightway25_module


def test_collect_fast_export_geography_discards_unknown_geocollections():
    data = [
        {"type": "process", "location": "CH"},
        {"type": "process", "location": "UNKNOWN"},
        {"type": "product", "location": "GLO"},
        {"type": "process", "location": None},
    ]

    geocollections, locations = brightway25_module._collect_fast_export_geography(
        data=data,
        process_node_types={"process"},
        get_geocollection=lambda location: {"CH": "ecoinvent"}.get(location),
    )

    assert geocollections == ["ecoinvent"]
    assert locations == {"CH", "UNKNOWN", "GLO"}


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
        lambda data, name: calls.__setitem__("write", (data, name)),
    )

    data = [{"code": "a", "exchanges": []}]

    brightway25_module.write_brightway_database(
        data=data,
        name="fast-db",
        fast=True,
        check_internal=True,
    )

    assert calls["change_db_name"] == (data, "fast-db")
    assert calls["check_internal"] == 1
    assert calls["compact"] == (data, "fast-db")
    assert calls["write"] == (data, "fast-db")
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
        lambda data, name: None,
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
