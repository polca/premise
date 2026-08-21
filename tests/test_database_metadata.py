"""Tests for the scenario metadata attached to exported Brightway databases."""

import premise.brightway2 as brightway2_module
import premise.brightway25 as brightway25_module
from premise import __version__
from premise.utils import database_metadata, scenario_metadata

SCENARIO = {
    "model": "remind",
    "pathway": "SSP2-PkBudg500",
    "year": 2050,
}


def test_scenario_metadata_describes_scenario_and_time():
    metadata = scenario_metadata(SCENARIO, version="3.10.1", system_model="cutoff")

    assert metadata["iam_model"] == "remind"
    assert metadata["pathway"] == "SSP2-PkBudg500"
    assert metadata["representative_time"] == "2050-01-01T00:00:00"
    # the year is already carried by the ISO timestamp
    assert "year" not in metadata
    assert metadata["ecoinvent_version"] == "3.10.1"
    assert metadata["system_model"] == "cutoff"
    assert metadata["premise_version"] == ".".join(str(i) for i in __version__)
    assert "external_scenarios" not in metadata


def test_scenario_metadata_includes_external_scenarios():
    scenario = dict(
        SCENARIO,
        **{
            "external scenarios": [
                {"scenario": "Business As Usual", "data": {"some": "package"}}
            ]
        },
    )

    metadata = scenario_metadata(scenario)

    assert metadata["external_scenarios"] == ["Business As Usual"]
    assert "ecoinvent_version" not in metadata


def test_database_metadata_for_single_scenario_is_flat():
    metadata = database_metadata([SCENARIO], version="3.10.1", system_model="cutoff")

    assert metadata == scenario_metadata(
        SCENARIO, version="3.10.1", system_model="cutoff"
    )


def test_database_metadata_for_several_scenarios_lists_them():
    scenarios = [SCENARIO, dict(SCENARIO, year=2030)]

    metadata = database_metadata(scenarios, version="3.10.1", system_model="cutoff")

    assert [s["representative_time"] for s in metadata["scenarios"]] == [
        "2050-01-01T00:00:00",
        "2030-01-01T00:00:00",
    ]
    assert metadata["ecoinvent_version"] == "3.10.1"
    assert metadata["system_model"] == "cutoff"
    # years differ, so no single representative point in time
    assert "representative_time" not in metadata


def test_database_metadata_shares_time_when_scenarios_agree():
    scenarios = [SCENARIO, dict(SCENARIO, pathway="SSP2-NPi")]

    metadata = database_metadata(scenarios)

    assert metadata["representative_time"] == "2050-01-01T00:00:00"
    assert len(metadata["scenarios"]) == 2


class DummyDatabases(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flushed = 0
        self.modified = []

    def flush(self):
        self.flushed += 1

    def set_modified(self, name):
        self.modified.append(name)


def test_write_brightway2_database_stores_metadata(monkeypatch):
    monkeypatch.setattr(brightway2_module, "change_db_name", lambda data, name: None)
    monkeypatch.setattr(brightway2_module, "link_internal", lambda data: None)
    monkeypatch.setattr(brightway2_module, "check_internal_linking", lambda data: None)

    class DummyImporter:
        def __init__(self, name, data):
            self.name = name

        def write_database(self):
            brightway2_module.databases[self.name] = {}

    monkeypatch.setattr(brightway2_module, "BW2Importer", DummyImporter)
    databases = DummyDatabases()
    monkeypatch.setattr(brightway2_module, "databases", databases)

    brightway2_module.write_brightway_database(
        data=[{"code": "a", "location": "CH", "type": "process", "exchanges": []}],
        name="bw2-db",
        metadata={"iam_model": "remind", "representative_time": "2050-01-01T00:00:00"},
    )

    assert databases["bw2-db"]["iam_model"] == "remind"
    assert databases["bw2-db"]["representative_time"] == "2050-01-01T00:00:00"


def test_write_brightway25_database_stores_metadata_on_fast_path(monkeypatch):
    monkeypatch.setattr(brightway25_module, "change_db_name", lambda data, name: None)
    monkeypatch.setattr(brightway25_module, "link_internal", lambda data: None)
    monkeypatch.setattr(brightway25_module, "check_internal_linking", lambda data: None)
    monkeypatch.setattr(
        brightway25_module, "_compact_payload_for_fast_write", lambda data, name: None
    )
    monkeypatch.setattr(
        brightway25_module, "_write_processed_database_fast", lambda data, name: None
    )
    databases = DummyDatabases({"fast-db": {}})
    monkeypatch.setattr(brightway25_module, "databases", databases)

    brightway25_module.write_brightway_database(
        data=[{"code": "a", "exchanges": []}],
        name="fast-db",
        fast=True,
        metadata={"iam_model": "image", "representative_time": "2030-01-01T00:00:00"},
    )

    assert databases["fast-db"]["iam_model"] == "image"
    assert databases["fast-db"]["representative_time"] == "2030-01-01T00:00:00"
    assert databases.modified == ["fast-db"]


def test_write_brightway25_database_stores_metadata_on_slow_path(monkeypatch):
    monkeypatch.setattr(brightway25_module, "change_db_name", lambda data, name: None)
    monkeypatch.setattr(brightway25_module, "link_internal", lambda data: None)
    monkeypatch.setattr(brightway25_module, "check_internal_linking", lambda data: None)

    class DummyImporter:
        def __init__(self, name, data):
            self.name = name

        def write_database(self):
            brightway25_module.databases[self.name] = {}

    monkeypatch.setattr(brightway25_module, "BW25Importer", DummyImporter)
    databases = DummyDatabases()
    monkeypatch.setattr(brightway25_module, "databases", databases)

    brightway25_module.write_brightway_database(
        data=[{"code": "a", "exchanges": []}],
        name="slow-db",
        fast=False,
        metadata={"pathway": "SSP2-RCP19"},
    )

    assert databases["slow-db"]["pathway"] == "SSP2-RCP19"
