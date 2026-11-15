"""Inventory cache refreshes must preserve bases of previously saved scenarios."""

import json
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor

import pytest

from premise.database import new_database as module
from premise.inventory.inventory_store import (
    CompactInventoryStore,
    InventoryStore,
    InventoryStoreError,
    STORE_SCHEMA_VERSION,
)


@pytest.fixture
def builder(tmp_path, monkeypatch):
    obj = object.__new__(module.NewDatabase)
    obj.source_type, obj.system_model, obj.version = "brightway", "cutoff", "3.12"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    monkeypatch.setattr(module, "DIR_CACHED_DB", tmp_path)
    classes = tmp_path / "classifications.csv"
    classes.write_bytes(module.FILEPATH_CLASSIFICATIONS.read_bytes())
    monkeypatch.setattr(module, "FILEPATH_CLASSIFICATIONS", classes)
    ensure_caches(obj)
    return obj


def ensure_caches(builder):
    for inventory in (False, True):
        path = builder._database_cache_path("source-db", inventories=inventory)
        for cache in (path, builder._metadata_cache_path(path)):
            if not cache.exists():
                cache.write_bytes(b"fixture cache")


def source_data(amount):
    return [
        {
            "name": "activity",
            "reference product": "product",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                {
                    "name": "activity",
                    "product": "product",
                    "location": "GLO",
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1.0,
                },
                {
                    "name": "Carbon dioxide, fossil",
                    "categories": ("air",),
                    "unit": "kilogram",
                    "type": "biosphere",
                    "amount": amount,
                    "input": ("biosphere3", "co2"),
                },
            ],
        }
    ]


def save_scenario(base, target, amount):
    database = InventoryStore.open(base)._checkout_materialized()
    database[0]["exchanges"][1]["amount"] = amount
    scenario = CompactInventoryStore(database, take_ownership=True)
    scenario._scenario_cache_compatibility = True
    checkpoint = scenario.checkpoint(target)
    manifest = json.loads((checkpoint / "manifest.json").read_text())
    assert manifest["columnar_format"] == "scenario-delta-v2"
    assert (checkpoint.parent / manifest["base_checkpoint"]).resolve() == base.resolve()
    return checkpoint


@pytest.mark.parametrize(
    "refresh",
    [
        "source",
        "source_metadata",
        "inventories",
        "inventories_metadata",
        "classifications",
        "legacy_base",
    ],
)
def test_old_scenario_reopens_after_source_or_inventory_refresh(
    builder, tmp_path, refresh
):
    data = source_data(2.0)
    if refresh == "legacy_base":
        source = builder._database_cache_path("source-db")
        original_base = CompactInventoryStore(data).checkpoint(
            source.with_name(
                f"{source.stem}_with_inventories_store_v{STORE_SCHEMA_VERSION}.inventory-store"
            )
        )
    else:
        original_base = builder._write_compact_source_checkpoint(
            "source-db", CompactInventoryStore(data)
        )
    saved = save_scenario(
        original_base, tmp_path / "original-scenario.inventory-store", 3.0
    )
    checksum = (original_base / "checksums.json").read_bytes()
    original_scenario = InventoryStore.open(saved).materialize()

    if refresh == "classifications":
        classes = module.FILEPATH_CLASSIFICATIONS
        classes.write_bytes(classes.read_bytes() + b"\n")
        assert builder._compact_source_cache_references("source-db") is None
        assert builder._find_compact_source_checkpoint("source-db") is None
        ensure_caches(builder)
    elif refresh != "legacy_base":
        refs = builder._compact_source_cache_references("source-db")
        refs[refresh].write_bytes(b"updated fixture cache")

    assert builder._find_compact_source_checkpoint("source-db") is None
    new_base = builder._write_compact_source_checkpoint(
        "source-db", CompactInventoryStore(source_data(5.0))
    )
    assert new_base != original_base
    assert builder._find_compact_source_checkpoint("source-db")[0] == new_base
    assert (original_base / "checksums.json").read_bytes() == checksum
    new_saved = save_scenario(new_base, tmp_path / "new-scenario.inventory-store", 7.0)
    assert InventoryStore.open(saved).materialize() == original_scenario
    assert (
        InventoryStore.open(new_saved).materialize()[0]["exchanges"][1]["amount"] == 7.0
    )
    assert (
        InventoryStore.open(original_base).materialize()[0]["exchanges"][1]["amount"]
        == 2.0
    )
    assert (
        InventoryStore.open(new_base).materialize()[0]["exchanges"][1]["amount"] == 5.0
    )


def test_identical_inputs_reuse_checkpoint_without_replacing_it(builder):
    path = builder._write_compact_source_checkpoint(
        "source-db", CompactInventoryStore(source_data(2.0))
    )
    checksum = (path / "checksums.json").read_bytes()
    stat = (path / "manifest.json").stat()

    def unexpected_write(_):
        pytest.fail("An existing combined base must remain immutable")

    reused = builder._write_compact_source_checkpoint(
        "source-db", SimpleNamespace(checkpoint=unexpected_write)
    )
    assert reused == path
    assert (path / "checksums.json").read_bytes() == checksum
    assert (path / "manifest.json").stat().st_mtime_ns == stat.st_mtime_ns


def test_concurrent_writers_share_one_immutable_checkpoint(builder):
    def write(_):
        return builder._write_compact_source_checkpoint(
            "source-db", CompactInventoryStore(source_data(2.0))
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(write, range(2))
    assert first == second
    assert InventoryStore.open(first).materialize()[0]["exchanges"][1]["amount"] == 2.0


@pytest.mark.parametrize("marker", ["invalid JSON", "{}"])
def test_bad_marker_cannot_cause_existing_base_to_be_replaced(
    builder, tmp_path, marker
):
    path = builder._write_compact_source_checkpoint(
        "source-db", CompactInventoryStore(source_data(2.0))
    )
    saved = save_scenario(path, tmp_path / "scenario.inventory-store", 3.0)
    checksum = (path / "checksums.json").read_bytes()
    (path / "source-cache.json").write_text(marker)
    with pytest.raises(InventoryStoreError, match="Refusing to replace"):
        builder._write_compact_source_checkpoint(
            "source-db", CompactInventoryStore(source_data(5.0))
        )
    assert (path / "checksums.json").read_bytes() == checksum
    assert InventoryStore.open(saved).materialize()[0]["exchanges"][1]["amount"] == 3.0
