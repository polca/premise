"""NMC955 source changes invalidate imports and their dependent compact checkpoint."""

from copy import deepcopy

import pytest

import premise.new_database as module
import premise.utils as utils
from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_store import CompactInventoryStore


@pytest.mark.parametrize("version", ("3.8", "3.9", "3.10", "3.12"))
def test_nmc955_content_invalidates_imports_and_compact_checkpoint_only(
    version, tmp_path, monkeypatch
):
    builder = object.__new__(module.NewDatabase)
    builder.version = version
    builder.keep_source_db_uncertainty = builder.keep_imports_uncertainty = False
    monkeypatch.setattr(module, "DIR_CACHED_DB", tmp_path)
    monkeypatch.setattr(utils, "DIR_CACHED_DB", tmp_path)

    assert (
        module.FILEPATH_BATTERIES_NMC955_LTO
        == INVENTORY_DIR / "lci-batteries-NMC955-LTO.xlsx"
    )
    original = module.FILEPATH_BATTERIES_NMC955_LTO.read_bytes()
    native = builder._database_cache_path("ALIGN047")
    imports = builder._database_cache_path("ALIGN047", inventories=True)
    copied = tmp_path / module.FILEPATH_BATTERIES_NMC955_LTO.name
    copied.write_bytes(original)
    monkeypatch.setattr(module, "FILEPATH_BATTERIES_NMC955_LTO", copied)
    assert builder._database_cache_path("ALIGN047", inventories=True) == imports

    source = [
        {
            "name": "native source",
            "reference product": "material",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [],
        }
    ]
    utils.create_cache(deepcopy(source), native)
    utils.create_cache([], imports)
    checkpoint = builder._write_compact_source_checkpoint(
        "ALIGN047", CompactInventoryStore(source)
    )
    assert checkpoint is not None
    assert builder._find_compact_source_checkpoint("ALIGN047")[0] == checkpoint
    original_refs = builder._compact_source_cache_references("ALIGN047")
    marker = checkpoint / "source-cache.json"
    original_marker = marker.read_bytes()

    # Appending bytes only to a temporary copy isolates content fingerprinting.
    copied.write_bytes(original + b"reviewed central-control fastener retirement")
    changed_imports = builder._database_cache_path("ALIGN047", inventories=True)
    assert changed_imports != imports
    assert builder._database_cache_path("ALIGN047") == native
    assert builder._compact_source_checkpoint_path("ALIGN047") == checkpoint
    assert builder._compact_source_cache_references("ALIGN047") is None
    assert builder._find_compact_source_checkpoint("ALIGN047") is None
    assert checkpoint.is_dir()  # Stale data exists, but cannot be reused.

    # Even when the replacement import cache exists, the old marker is stale.
    utils.create_cache([], changed_imports)
    assert builder._compact_source_cache_references("ALIGN047") is not None
    assert marker.read_bytes() == original_marker
    assert builder._find_compact_source_checkpoint("ALIGN047") is None

    copied.write_bytes(original)
    assert builder._database_cache_path("ALIGN047", inventories=True) == imports
    assert builder._database_cache_path("ALIGN047") == native
    assert builder._compact_source_cache_references("ALIGN047") == original_refs
    assert builder._find_compact_source_checkpoint("ALIGN047")[0] == checkpoint
