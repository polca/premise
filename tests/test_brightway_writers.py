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


def test_write_brightway25_database_fast_prints_overwrite_message(
    monkeypatch, capsys
):
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
