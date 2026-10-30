"""ALIGN-090: explicit products survive preparation before later foregrounds."""

import json
from copy import deepcopy
from types import SimpleNamespace
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from delegated_source_history import (
    SUCCESSORS,
    _reviewed_predecessor,
    previous_product_source,
    previous_product_xml,
)

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

WORKBOOK = "lci-batteries-LiS.xlsx"
WRAPPER = "battery control and cooling system production, 16 modules"
COOLING = "cooling system production, for Li-O2 battery"


NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


def activity(data, name):
    (record,) = [r for r in data if r["name"] == name]
    return record


@pytest.fixture(scope="module")
def predecessor():
    entry = json.loads(SUCCESSORS.read_text())[WORKBOOK]
    path = SUCCESSORS.parent / entry["predecessor_fixture"]

    return path


def test_cooling_product_metadata_matches_the_existing_supplier_recipe(predecessor):
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
    for address, value in [("I1129", "product"), ("I1133", "cooling system")]:
        cell = root.find(f".//{NS}c[@r='{address}']")
        assert "".join(cell.find(NS + "is").itertext()) == value
    original = ExcelImporter(predecessor)
    candidate = ExcelImporter(INVENTORY_DIR / WORKBOOK)
    normalized = previous_product_source(candidate, INVENTORY_DIR / WORKBOOK)
    assert activity(normalized.data, WRAPPER) == activity(original.data, WRAPPER)
    exchange = activity(candidate.data, WRAPPER)["exchanges"][3]
    assert exchange["product"] == exchange["reference product"] == "cooling system"
    assert (exchange["amount"], exchange["location"], exchange["unit"]) == (
        10,
        "RER",
        "kilogram",
    )


def prepare(path):
    wrapper = activity(ExcelImporter(path).data, WRAPPER)
    # Supply identities for the already available controls and waste route.
    # The Li-O2 cooling foreground has deliberately not been loaded yet.
    background = [
        {
            "name": e["name"],
            "reference product": e["reference product"],
            "location": e["location"],
            "unit": e["unit"],
            "exchanges": [],
        }
        for e in wrapper["exchanges"]
        if e["type"] == "technosphere" and e["name"] != COOLING
    ]
    assert not any(r["name"] == COOLING for r in background)
    importer = DefaultInventory(
        background,
        "3.9",
        "3.8",
        path,
        "cutoff",
        False,
        preloaded_importer=SimpleNamespace(data=[deepcopy(wrapper)]),
    )
    importer.prepare_inventory()
    record = activity(importer.import_db.data, WRAPPER)
    exchange = next(e for e in record["exchanges"] if e["name"] == COOLING)
    return importer, exchange


def test_actual_default_inventory_retains_product_before_late_supplier(predecessor):
    before, old = prepare(predecessor)
    after, new = prepare(INVENTORY_DIR / WORKBOOK)
    assert old["reference product"] == new["reference product"] == "cooling system"
    assert old["product"] is None
    assert len(before.list_unlinked) == 1 and before.list_unlinked[0][0] == COOLING
    assert new["product"] == "cooling system" and after.list_unlinked == []
    assert (
        (old["amount"], old["location"], old["unit"])
        == (new["amount"], new["location"], new["unit"])
        == (10, "RER", "kilogram")
    )
    # Resolve the identity against the actual later workbook, without importing
    # a native database or expanding its scientific supplier recipes.
    supplier = activity(
        ExcelImporter(INVENTORY_DIR / "lci-batteries-LiO2.xlsx").data, COOLING
    )
    assert (new["name"], new["product"], new["location"], new["unit"]) == (
        supplier["name"],
        supplier["reference product"],
        supplier["location"],
        supplier["unit"],
    )


def test_bounded_product_normalization_rejects_unknown_or_tampered_data(
    predecessor, tmp_path
):
    payload = (INVENTORY_DIR / WORKBOOK).read_bytes()
    assert (
        _reviewed_predecessor(payload + b"benign archive trailer", WORKBOOK)
        == predecessor.read_bytes()
    )
    source = ExcelImporter(INVENTORY_DIR / WORKBOOK)
    activity(source.data, WRAPPER)["exchanges"][3]["product"] = "wrong product"
    unchanged = deepcopy(source.data)
    with pytest.raises(AssertionError):
        previous_product_source(source, INVENTORY_DIR / WORKBOOK)
    assert source.data == unchanged
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
    root.find(f".//{NS}c[@r='I1133']/{NS}is/{NS}t").text = "wrong product"
    unchanged_xml = ET.tostring(root)
    with pytest.raises(AssertionError):
        previous_product_xml(root, INVENTORY_DIR / WORKBOOK)
    assert ET.tostring(root) == unchanged_xml
