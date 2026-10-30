"""Approved battery source edits invalidate imports, preserving native caches."""

from copy import deepcopy
from io import BytesIO
from zipfile import ZipFile

from lxml import etree
import pytest

import premise.new_database as module
import premise.utils as utils
from premise.inventory_store import CompactInventoryStore

SOURCES = (
    ("FILEPATH_LFP_REFERENCE", "C42"),
    ("FILEPATH_BATTERIES_NMC_NCA_LFP", "B811"),
    ("FILEPATH_BATTERIES_NMC622_532", "B594"),
    ("FILEPATH_BATTERIES_NMC955_LTO", "B442"),
    ("FILEPATH_ASSB_BATTERIES", "B394"),
    ("FILEPATH_HOME_STORAGE_BATTERIES", "B240"),
)
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def changed_coefficient(payload, address):
    """Change an actual source coefficient in a temporary valid workbook."""
    out = BytesIO()
    with ZipFile(BytesIO(payload)) as source, ZipFile(out, "w") as target:
        for info in source.infolist():
            data = source.read(info.filename)
            if info.filename == "xl/worksheets/sheet1.xml":
                root = etree.fromstring(data)
                cell = root.find(f".//{NS}c[@r='{address}']")
                assert cell is not None and cell.find(NS + "f") is None
                value = cell.find(NS + "v")
                value.text = repr(float(value.text) + 0.0001)
                data = etree.tostring(root)
            target.writestr(info, data)
    return out.getvalue()


@pytest.mark.parametrize("version", ("3.8", "3.12"))
@pytest.mark.parametrize("constant,address", SOURCES)
def test_actual_battery_source_content_invalidates_imports_and_checkpoint_only(
    version, constant, address, tmp_path, monkeypatch
):
    builder = object.__new__(module.NewDatabase)
    builder.version = version
    builder.keep_source_db_uncertainty = builder.keep_imports_uncertainty = False
    monkeypatch.setattr(module, "DIR_CACHED_DB", tmp_path)
    monkeypatch.setattr(utils, "DIR_CACHED_DB", tmp_path)
    inventory = getattr(module, constant)
    original = inventory.read_bytes()
    copied = tmp_path / inventory.name
    copied.write_bytes(original)
    native = builder._database_cache_path("delegated-source-content")
    imports = builder._database_cache_path("delegated-source-content", inventories=True)
    monkeypatch.setattr(module, constant, copied)
    assert (
        builder._database_cache_path("delegated-source-content", inventories=True)
        == imports
    )
    source = [
        {
            "name": "unmodified native source",
            "reference product": "material",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [],
        }
    ]
    utils.create_cache(deepcopy(source), native)
    utils.create_cache([], imports)
    checkpoint = builder._write_compact_source_checkpoint(
        "delegated-source-content", CompactInventoryStore(source)
    )
    assert (
        builder._find_compact_source_checkpoint("delegated-source-content")[0]
        == checkpoint
    )
    copied.write_bytes(changed_coefficient(original, address))
    assert (
        builder._database_cache_path("delegated-source-content", inventories=True)
        != imports
    )
    assert builder._database_cache_path("delegated-source-content") == native
    assert builder._find_compact_source_checkpoint("delegated-source-content") is None
    assert checkpoint.is_dir()
    copied.write_bytes(original)
    assert (
        builder._database_cache_path("delegated-source-content", inventories=True)
        == imports
    )
    assert builder._database_cache_path("delegated-source-content") == native
    assert (
        builder._find_compact_source_checkpoint("delegated-source-content")[0]
        == checkpoint
    )
