"""ALIGN-044 factory allocation, with a guarded two-cell historical view."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from inventory_assertions import assert_same_cells, formula_caches
from test_central_fastener_alignment import (
    previous_fastener_source,
    previous_fastener_xml,
)

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiS.xlsx"
CELL = "battery cell production, Li-S"
FACTORY = "factory construction and operation, for LiS battery"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


def activity(data, name=CELL):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(record, name=FACTORY):
    (match,) = [e for e in record["exchanges"] if e["name"] == name]
    return match


def science(record):
    return sorted(
        [
            [
                e["type"],
                e["name"],
                e["reference product"],
                e.get("location"),
                e["unit"],
                e["amount"],
            ]
            for e in record["exchanges"]
        ]
    )


def reference():
    approved = json.loads(
        (
            ROOT / "tests/data/inventory_review/lis-factory-allocation-adoption-20261027.json"
        ).read_text()
    )
    assert approved["case_id"] == "ALIGN-044"

    assert (approved["CLIC_dataset_id"], approved["CLIC_version_id"]) == (1178, 5547)

    return approved


def validate_current_factory(source):
    approved = reference()
    actual = activity(source.data)
    expected = deepcopy(approved["before_snapshot"])
    expected_purchase = exchange(expected)
    assert expected_purchase["amount"] == 2.4303134047634144e-7
    expected_purchase["amount"] = 7.5e-8
    assert expected == approved["after_snapshot"]
    note = exchange(actual)["comment"]
    assert note.endswith(approved["before_exchange_comment"])

    expected_purchase["comment"] = note
    assert actual == expected  # Complete raw fields, including other utilities.
    assert science(actual) == science(approved["CLIC_snapshot"])

    return approved


def previous_factory_source(source):
    """Guard the approved present recipe before exposing the prior raw view."""
    approved = validate_current_factory(source)
    source.data = [
        deepcopy(approved["before_snapshot"]) if d["name"] == CELL else d
        for d in source.data
    ]
    return source


def factory_xml_scope(root):
    return [
        [
            address,
            ET.tostring(root.find(f".//{NS}c[@r='{address}']"), encoding="unicode"),
        ]
        for address in ("B472", "G472")
    ]


def previous_factory_xml(root, sheet=SHEET):
    """Restore exact before formula/cache and shared-string cell, nothing else."""
    if sheet != SHEET:
        return root
    validate_current_factory(
        previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)
    )

    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        expected_root = previous_fastener_xml(
            ET.fromstring(archive.read(SHEET)), WORKBOOK
        )
    assert_same_cells(root, expected_root, ("B472", "G472"))
    quantity = root.find(f".//{NS}c[@r='B472']")
    quantity.find(NS + "f").text = "1.15e-08/0.047319"
    quantity.find(NS + "v").text = "2.4303134047634144e-07"
    row = root.find(f".//{NS}row[@r='472']")
    comment = row.find(f"{NS}c[@r='G472']")
    position = list(row).index(comment)
    original = ET.Element(NS + "c", r="G472", t="s")
    ET.SubElement(original, NS + "v").text = "417"
    row.remove(comment)
    row.insert(position, original)

    return root


@pytest.fixture(scope="module")
def adoption():
    return reference()


@pytest.fixture(scope="module")
def raw():
    return previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)


def test_approval_pins_independent_clic_and_original_paired39_profiles(adoption):
    assert adoption["workbook"] == WORKBOOK
    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["formula_reference_audit"] == {
        "formula_count": 127,
        "dependent_formula_cells": [],
    }


def test_current_cell_recipe_changes_only_factory_allocation_and_qualified_exchange_note(
    raw, adoption
):
    validate_current_factory(raw)
    cell = activity(raw.data)
    assert len(cell["exchanges"]) == 4
    assert cell["production amount"] == exchange(cell, CELL)["amount"] == 1
    note = exchange(cell)["comment"]
    prefix = note[: -len(adoption["before_exchange_comment"])]
    assert prefix.startswith("APPROVED Li-S FACTORY ALLOCATION BASIS")
    for term in (
        "per nominal kWh, not per cell",
        "qualified static allocation convention",
        "not measured industrial productivity",
        "47.319 g",
        "150 Wh/kg",
        "factory construction and utility recipe",
        "not authorize a new cell design",
        "both remain 3.9",
    ):
        assert term in prefix


def test_nominal_kwh_allocation_closes_at_retained_specific_energy_not_cell_mass(raw):
    cell = activity(raw.data)
    dose = exchange(cell)["amount"]
    assert dose == 7.5e-8
    assert dose == pytest.approx(5e-7 * 0.15, rel=1e-15)
    assert dose / 0.15 == pytest.approx(5e-7, rel=1e-15)
    assert dose != pytest.approx(1.15e-8 / 0.047319, rel=1e-6)
    assert exchange(cell, "market group for electricity, medium voltage")[
        "amount"
    ] * 0.047319 == pytest.approx(0.225, rel=1e-15)
    assert exchange(cell, "battery cell assembly, Li-S")["amount"] == 1


def test_factory_recipe_other84_records_and_all_consumers_remain_exactly_unchanged(raw):
    others = [d for d in raw.data if d["name"] != CELL]
    assert len(raw.data) == 85 and len(others) == 84

    previous = previous_factory_source(deepcopy(raw))
    assert [d for d in previous.data if d["name"] != CELL] == others


def test_factory_allocation_formula_note_and_unrelated_cached_values(
    adoption,
):
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        strings = ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(NS + "si")
        parts = [
            [name, archive.read(name)]
            for name in sorted(archive.namelist())
            if name != SHEET
        ]
    assert "".join(strings[417].itertext()) == adoption["before_exchange_comment"]
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        expected_root = previous_fastener_xml(
            ET.fromstring(archive.read(SHEET)), WORKBOOK
        )
    assert_same_cells(root, expected_root, ("B472", "G472"))
    quantity = root.find(f".//{NS}c[@r='B472']")
    assert quantity.attrib == {"r": "B472", "s": "13"}
    assert quantity.find(NS + "f").text == "5e-7*0.15"
    assert quantity.find(NS + "v").text == "7.5e-08"
    assert root.find(f".//{NS}c[@r='G472']").get("t") == "inlineStr"
    formulas = [c for c in root.iter(NS + "c") if c.find(NS + "f") is not None]
    before_caches = formula_caches(root)
    other = [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in formulas
        if c.get("r") != "B472"
    ]
    assert {address for address in before_caches if address != "B472"} == {
        address for address, _ in other
    }
    assert all(c.find(NS + "v").text is not None for c in formulas)
    assert len(parts) == 12
    previous_factory_xml(root)
    assert {
        address: value
        for address, value in formula_caches(root).items()
        if address != "B472"
    } == {
        address: value for address, value in before_caches.items() if address != "B472"
    }


def test_historical_factory_reversal_rejects_unapproved_science_note_formula_or_cache(
    raw,
):
    for name, field in (
        (FACTORY, "amount"),
        (FACTORY, "comment"),
        ("market group for electricity, medium voltage", "amount"),
    ):
        broken = deepcopy(raw)
        exc = exchange(activity(broken.data), name)
        exc[field] = "Changed note" if field == "comment" else exc[field] + 1e-6
        untouched = deepcopy(broken.data)
        with pytest.raises(AssertionError):
            previous_factory_source(broken)
        assert broken.data == untouched
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
    for field, value in (("f", "5e-7*0.16"), ("v", "8e-08")):
        damaged = deepcopy(root)
        damaged.find(f".//{NS}c[@r='B472']/{NS}{field}").text = value
        with pytest.raises(AssertionError):
            previous_factory_xml(damaged)


@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real39_preparation_preserves_factory_allocation_basis_and_supplier_identity(
    raw, adoption, system_model
):
    source = deepcopy(raw)
    source.data = [deepcopy(activity(raw.data))]
    fields = ("name", "reference product", "location", "unit")
    identities = {
        tuple(e[k] for k in fields)
        for e in source.data[0]["exchanges"]
        if e["type"] == "technosphere"
    }
    background = [dict(zip(fields, key), exchanges=[]) for key in sorted(identities)]
    before = deepcopy(background)
    importer = DefaultInventory(
        background,
        "3.9",
        "3.9",
        INVENTORY_DIR / WORKBOOK,
        system_model,
        True,
        preloaded_importer=source,
    )
    importer.prepare_inventory()
    assert importer.version_in == importer.version_out == "3.9"
    assert importer.list_unlinked == [] and background == before
    assert science(activity(importer.import_db.data)) == science(
        adoption["CLIC_snapshot"]
    )
