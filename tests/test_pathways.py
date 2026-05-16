from types import SimpleNamespace

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
