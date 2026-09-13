from types import SimpleNamespace

import yaml

from premise.inventory_store import CompactInventoryStore, _compact_scenario_mapping
from premise.pathways import PathwaysDataPackage


def test_cleanup_after_export_releases_loaded_scenarios(monkeypatch):
    obj = object.__new__(PathwaysDataPackage)
    scenario = {
        "database": [{"name": "activity"}],
        "applied functions": ["electricity"],
        "cache": {"foo": "bar"},
        "index": {"baz": "qux"},
    }
    obj.datapackage = SimpleNamespace(scenarios=[scenario, {"model": "remind"}])

    deleted_pickles = {"called": False}
    cleared_runtime_caches = {"called": False}
    collected = {"called": False}

    monkeypatch.setattr(
        "premise.pathways.delete_all_pickles",
        lambda: deleted_pickles.__setitem__("called", True),
    )
    monkeypatch.setattr(
        "premise.pathways.clear_runtime_caches",
        lambda: cleared_runtime_caches.__setitem__("called", True),
    )
    monkeypatch.setattr(
        "premise.pathways.gc.collect",
        lambda: collected.__setitem__("called", True),
    )

    obj._cleanup_after_export()

    assert "database" not in scenario
    assert "applied functions" not in scenario
    assert scenario["cache"] == {}
    assert scenario["index"] == {}
    assert deleted_pickles["called"] is True
    assert cleared_runtime_caches["called"] is True
    assert collected["called"] is True


def test_variables_mapping_uses_resident_lazy_activity_fields(tmp_path, monkeypatch):
    activity = {
        "name": "market for electricity, low voltage",
        "reference product": "electricity, low voltage",
        "location": "CH",
        "unit": "kilowatt hour",
        "lhv": 3.6,
        "comment": "kept lazy",
        "exchanges": [],
    }
    mapping = {"electricity": {"grid": [activity]}}
    store = CompactInventoryStore([activity], take_ownership=True)
    checkpoint = store.checkpoint(tmp_path / "scenario.inventory-store")
    compacted = _compact_scenario_mapping(mapping, store, checkpoint)
    reference = compacted["electricity"]["grid"][0]

    obj = object.__new__(PathwaysDataPackage)
    obj.datapackage = SimpleNamespace(scenarios=[{"mapping": compacted}])
    obj.variables_name_change = {}
    monkeypatch.chdir(tmp_path)

    obj._add_variables_mapping()

    written = yaml.safe_load(
        (tmp_path / "pathways_temp" / "mapping" / "mapping.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert reference._resolver._store is None
    assert written == {
        "SE - electricity - grid": {
            "dataset": [
                {
                    "name": "market for electricity, low voltage",
                    "reference product": "electricity, low voltage",
                    "unit": "kilowatt hour",
                    "lhv": 3.6,
                }
            ]
        }
    }
