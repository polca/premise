"""Native cache misses and hits retain full linked inventories and precision."""

from copy import deepcopy
from unittest.mock import Mock

import premise.database.new_database as database_module
from premise import NewDatabase
from premise.core.utils import create_cache, load_cached_database


def linked_inventory():
    return [
        {
            "name": "Native heat",
            "reference product": "Native heat",
            "location": "CH",
            "unit": "megajoule",
            "database": "native",
            "code": "heat",
            "parameters": {"dose": 1 / 3},
            "native provenance": {"revision": "pinned"},
            "exchanges": [
                {
                    "type": "production",
                    "input": ("native", "heat"),
                    "output": ("native", "heat"),
                    "amount": 1.08,
                },
                {
                    "type": "technosphere",
                    "name": "Native heat",
                    "product": "Native heat",
                    "location": "CH",
                    "unit": "megajoule",
                    "input": ("native", "heat"),
                    "output": ("native", "heat"),
                    "amount": -1 / 3,
                    "formula": "-dose",
                    "uncertainty type": 3,
                    "loc": -1 / 3,
                    "scale": 0.2,
                    "minimum": -0.8,
                    "maximum": -0.1,
                    "negative": True,
                },
            ],
        }
    ]


def test_lossless_cache_retains_keys_signed_quantities_formulas_and_uncertainty(
    tmp_path,
):
    data = linked_inventory()
    before = deepcopy(data)
    path = tmp_path / "native.pickle"
    runtime, _ = create_cache(data, path, preserve_inventory=True)
    assert runtime == before and data == before
    restored = load_cached_database(path)
    assert restored == before
    assert restored[0]["exchanges"][1]["amount"] == -1 / 3


def test_native_constructor_cache_miss_and_hit_equal_uncached_payload(
    tmp_path, monkeypatch
):
    owner = object.__new__(NewDatabase)
    owner.source_family = "uvek"
    path = tmp_path / "native.pickle"
    owner._database_cache_path = Mock(return_value=path)
    clean = Mock(return_value=linked_inventory())
    monkeypatch.setattr(owner, "_NewDatabase__clean_database", clean)
    monkeypatch.setattr(database_module, "clear_existing_cache", lambda: None)
    expected = deepcopy(clean.return_value)
    first = owner._NewDatabase__find_cached_db("native")
    assert first == expected
    clean.side_effect = AssertionError("A cache hit must not re-extract the source")
    second = owner._NewDatabase__find_cached_db("native")
    assert second == expected
    assert clean.call_count == 1


def test_legacy_cache_default_still_trims_fields(tmp_path):
    data = linked_inventory()
    runtime, _ = create_cache(data, tmp_path / "legacy.pickle")
    assert "database" not in runtime[0]
    assert "native provenance" not in runtime[0]
