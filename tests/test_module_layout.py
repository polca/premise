"""Topic packages retain public APIs, import-order safety and old cache classes."""

import importlib
import os
from pathlib import Path
import pickle
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "legacy,canonical",
    [
        ("premise.new_database", "premise.database.new_database"),
        ("premise.inventory_store", "premise.inventory.inventory_store"),
        ("premise.data_collection", "premise.iam.data_collection"),
        ("premise.car_energy", "premise.transport.car_energy"),
        ("premise.battery", "premise.batteries.battery"),
        ("premise.validation_framework", "premise.validation.validation_framework"),
    ],
)
def test_legacy_module_names_share_canonical_state_and_specs(legacy, canonical):
    actual = importlib.import_module(legacy)
    assert actual is importlib.import_module(canonical)
    assert actual.__spec__.name == canonical
    assert actual.__package__ == actual.__spec__.parent


def test_old_inventory_cache_class_path_is_still_loadable():
    from premise.inventory.inventory_store import CompactInventoryStore

    assert (
        pickle.loads(b"cpremise.inventory_store\nCompactInventoryStore\n.")
        is CompactInventoryStore
    )


@pytest.mark.parametrize(
    "first_import",
    [
        "premise.transport.car_energy",
        "premise.validation.validation_framework",
        "premise.inventory.inventory_store",
    ],
)
def test_topic_children_can_load_before_public_build_api(first_import, tmp_path):
    brightway = tmp_path / "brightway"
    brightway.mkdir()
    env = dict(
        os.environ,
        PYTHONPATH=str(Path(__file__).resolve().parents[1]),
        BRIGHTWAY2_DIR=str(brightway),
    )
    script = f"""
import importlib
importlib.import_module({first_import!r})
from premise import NewDatabase, InventoryStore, audit_database
from premise.transport import Transport
from premise.export import prepare_db_for_fast_export
assert callable(NewDatabase) and callable(Transport)
assert callable(audit_database) and callable(prepare_db_for_fast_export)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
