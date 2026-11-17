"""Cache deletion retains its version, filename-filter and failure semantics."""

from types import SimpleNamespace

import pytest

import premise.core.utils as module


@pytest.mark.parametrize(
    "all_versions,file_filter,removed",
    [
        (False, None, {"old", "old_inventory", "unrelated"}),
        (True, None, {"current", "inventory", "old", "old_inventory", "unrelated"}),
        (False, "inventories", {"old_inventory"}),
        (True, "inventories", {"inventory", "old_inventory"}),
        (True, "absent", set()),
    ],
)
def test_cache_selection_preserves_version_filter_and_subdirectories(
    tmp_path, monkeypatch, all_versions, file_filter, removed
):
    tag = "".join(map(str, module.__version__))
    names = {
        "current": f"cache-{tag}.pickle",
        "inventory": f"inventories-{tag}.pickle",
        "old": "cache-previous.pickle",
        "old_inventory": "inventories-previous.pickle",
        "unrelated": "unrelated.txt",
    }
    for filename in names.values():
        (tmp_path / filename).write_text("cached data")
    child = tmp_path / "nested"
    child.mkdir()
    (child / "inventories-previous.pickle").write_text("nested data")
    monkeypatch.setattr(module, "DIR_CACHED_DB", tmp_path)

    assert module.clear_existing_cache(all_versions, file_filter) is None

    assert {
        key for key, filename in names.items() if not (tmp_path / filename).exists()
    } == removed
    assert (child / "inventories-previous.pickle").read_text() == "nested data"


def test_cache_deletion_stops_and_propagates_the_first_unlink_failure(monkeypatch):
    calls = []

    class Entry:
        def __init__(self, name):
            self.name = name

        def is_file(self):
            return True

        def unlink(self):
            calls.append(self.name)
            if self.name == "blocked":
                raise PermissionError("blocked entry")

    entries = [Entry(name) for name in ("first", "blocked", "last")]
    monkeypatch.setattr(
        module, "DIR_CACHED_DB", SimpleNamespace(glob=lambda pattern: iter(entries))
    )

    with pytest.raises(PermissionError, match="blocked entry"):
        module.clear_existing_cache(all_versions=True)

    assert calls == ["first", "blocked"]
