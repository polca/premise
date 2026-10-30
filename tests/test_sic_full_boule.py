"""ALIGN-023: account for the full SiC boule without rescaling electricity."""

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-hydrogen-electrolysis.xlsx"
NAME = "silicon carbide wafer production, physical vapour transport"
PRODUCT = "silicon carbide wafer"
FEED = "market for silicon carbide"
ELECTRICITY = "market group for electricity, medium voltage"
WASTE = "market for inert waste, for final disposal"
FEED_KG = 6.079027355623101
WASTE_KG = -5.079027355623101
ELECTRICITY_KWH = 19932.858629976276
SHEET = "xl/worksheets/sheet1.xml"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
ALLOWED = {"B1229", "B1234", "B1236", "H1235"}


def activity(data):
    (match,) = [d for d in data if d["name"] == NAME]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def scientific_vector(data):
    result = {}
    for exc in data["exchanges"]:
        assert exc["name"] not in result
        result[exc["name"]] = {k: v for k, v in exc.items() if k != "comment"}
    return result


def xml_cells(path):
    cells = {}
    with ZipFile(path) as archive:
        strings = ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(NS + "si")
        for sheet in archive.namelist():
            if not (sheet.startswith("xl/worksheets/sheet") and sheet.endswith(".xml")):
                continue
            for cell in ET.fromstring(archive.read(sheet)).iter(NS + "c"):
                value = cell.find(NS + "v")
                shared = strings[int(value.text)] if cell.get("t") == "s" else None
                cells[sheet, cell.get("r")] = cell, [
                    ET.tostring(cell, encoding="unicode"),
                    (
                        ET.tostring(shared, encoding="unicode")
                        if shared is not None
                        else None
                    ),
                ]
    return cells


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (ROOT / "scripts/data/sic-full-boule-adoption-20261027.json").read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


def test_portable_approval_pins_independent_original_and_current_clic(adoption):
    assert adoption["approval"] == "ALIGN-023; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK

    assert adoption["CLIC_dataset_id"] == 3695
    assert adoption["CLIC_version_id"] == 5610

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "version": "3.9",
        "system_model": "cutoff",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["CLIC_source_background"] == {
        "family": "ecoinvent",
        "version": "3.12",
        "system_model": "cutoff",
    }
    assert adoption["CLIC_source_biosphere"] == {
        "family": "ecoinvent",
        "version": "3.12",
    }
    assert set(adoption["changed_cells"]) == ALLOWED
    assert len(adoption["changed_cells"]) == 4
    assert adoption["amount_cells"] == {"B1234": FEED_KG, "B1236": WASTE_KG}
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}


def test_only_two_amounts_and_two_comments_change_in_the_complete_raw_record(
    raw, adoption
):
    before = adoption["before_snapshot"]
    expected = deepcopy(before)
    exchange(expected, FEED)["amount"] = FEED_KG
    exchange(expected, WASTE)["amount"] = WASTE_KG
    assert adoption["after_snapshot"] == expected
    actual = deepcopy(activity(raw.data))
    actual["comment"] = before["comment"]
    exchange(actual, ELECTRICITY)["comment"] = exchange(before, ELECTRICITY)["comment"]
    assert actual == expected
    assert (
        actual["name"],
        actual["reference product"],
        actual["location"],
        actual["unit"],
        actual["production amount"],
    ) == (NAME, PRODUCT, "GLO", "kilogram", 1)
    assert len(actual["exchanges"]) == 4
    assert Counter(e["type"] for e in actual["exchanges"]) == {
        "production": 1,
        "technosphere": 3,
    }


def test_authoritative_note_and_electricity_comment_qualify_the_new_boundary(
    raw, adoption
):
    actual = activity(raw.data)
    old = adoption["before_snapshot"]["comment"]
    assert actual["comment"].endswith(old)
    note = actual["comment"][: -len(old)]
    assert note.startswith("APPROVED FULL-BOULE SiC MATERIAL ACCOUNTING")
    for term in (
        "100 mm",
        "66 mm",
        "0.35 mm",
        "0.1645",
        str(FEED_KG),
        str(-WASTE_KG),
        str(ELECTRICITY_KWH),
        "supersedes",
    ):
        assert term in note
    assert "qualified mineral-waste proxy once" in note
    assert "without a recovery credit" in note
    assert "classification and destination remain unverified" in note
    assert "Purification" in note and "gases" in note and "unresolved" in note
    electricity = exchange(actual, ELECTRICITY)["comment"]
    assert electricity != exchange(adoption["before_snapshot"], ELECTRICITY)["comment"]
    assert "4 kWh/cm2" not in electricity
    assert str(ELECTRICITY_KWH) in electricity
    assert "physical usable wafer area" in electricity
    assert "does not rescale" in electricity


def test_full_crystal_retention_material_closure_and_separate_electricity_basis(
    raw, adoption
):
    full, usable, count, thickness = (
        adoption["full_boule_length_mm"],
        adoption["usable_boule_length_mm"],
        adoption["wafer_count"],
        adoption["wafer_thickness_mm"],
    )
    assert (full, usable, count, thickness) == (100, 66, 47, 0.35)
    retention = count * thickness / full
    assert adoption["retention"] == 0.1645
    assert retention == pytest.approx(adoption["retention"], rel=1e-14)
    wafer = activity(raw.data)
    feed = exchange(wafer, FEED)["amount"]
    discard = -exchange(wafer, WASTE)["amount"]
    assert feed == full / (count * thickness)
    assert discard == feed - 1
    assert feed * retention == pytest.approx(1, rel=1e-14)
    assert discard / feed == pytest.approx(1 - retention, rel=1e-14)
    assert feed == pytest.approx(1 + discard, rel=1e-14)
    old_feed = exchange(adoption["before_snapshot"], FEED)["amount"]
    assert old_feed == pytest.approx(usable / (count * thickness), rel=1e-14)
    assert feed / old_feed == pytest.approx(full / usable, rel=1e-14)
    # Electricity already uses physical wafer area, independently of boule
    # retention. Do not apply the 100/66 material correction a second time.
    physical_area_kwh = 4.04 * (1314 * 3.42 / 100) / 81.07
    kwh_per_kg = physical_area_kwh * 10000 / (3210 * 0.00035)
    assert (
        exchange(wafer, ELECTRICITY)["amount"]
        == (adoption["retained_electricity_kWh_per_kg"])
        == ELECTRICITY_KWH
    )
    assert ELECTRICITY_KWH == pytest.approx(kwh_per_kg, rel=1e-14)


def test_all_four_scientific_flows_match_qualified_clic_reference(raw, adoption):
    actual, clic = activity(raw.data), adoption["CLIC_snapshot"]
    assert len(clic["exchanges"]) == 4
    assert scientific_vector(actual) == scientific_vector(clic)
    for field in (
        "name",
        "reference product",
        "location",
        "unit",
        "production amount",
        "type",
        "source",
    ):
        assert actual[field] == clic[field]


def test_all_59_other_activities_and_downstream_coefficients_remain_exact(raw):
    others = [d for d in raw.data if d["name"] != NAME]
    assert len(raw.data) == 60
    assert len(others) == 59


def test_xml_exempts_only_four_cells_and_keeps_every_formula_cache():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        if not (sheet == SHEET and address in ALLOWED):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert cell.find(NS + "v") is not None
            assert cell.find(NS + "v").text is not None
            formulas.append([sheet, address, payload])
    assert len(cells) == 14756
    assert len(outside) == 14752

    assert len(formulas) == 484

    for address, amount in (("B1234", FEED_KG), ("B1236", WASTE_KG)):
        cell = cells[SHEET, address][0]
        assert cell.attrib == {"r": address}
        assert [child.tag for child in cell] == [NS + "v"]
        assert cell.find(NS + "v").text == str(amount)


def test_worksheet_structure_and_every_other_archive_part_remain_exact():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
        parts = [
            [name, archive.read(name)]
            for name in sorted(archive.namelist())
            if name != SHEET
        ]
    for parent in root.iter():
        for cell in list(parent):
            if cell.tag == NS + "c" and cell.get("r") in ALLOWED:
                parent.remove(cell)

    assert len(parts) == 14


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_focused_preparation_keeps_original_39_source_and_supplier_identities(
    raw, version, system_model
):
    source = deepcopy(raw)
    source.data = [activity(source.data)]
    background = [
        {
            "name": e["name"],
            "reference product": e["reference product"],
            "location": e["location"],
            "unit": e["unit"],
            "exchanges": [],
        }
        for e in source.data[0]["exchanges"]
        if e["type"] == "technosphere"
    ]
    before = deepcopy(background)
    importer = DefaultInventory(
        background,
        "3.9",
        version,
        INVENTORY_DIR / WORKBOOK,
        system_model,
        True,
        preloaded_importer=source,
    )
    importer.prepare_inventory()
    assert importer.version_in == "3.9"
    assert importer.version_out == version
    assert background == before
    assert importer.list_unlinked == []
    prepared = activity(importer.import_db.data)
    assert len(prepared["exchanges"]) == 4

    # Product aliases and classifications are preparation details; compare the
    # complete physical vector, including signs, units and supplier geography.
    def vector(dataset):
        return {
            (e["type"], e["name"], e["reference product"], e["location"], e["unit"]): e[
                "amount"
            ]
            for e in dataset["exchanges"]
        }

    assert vector(prepared) == vector(activity(raw.data))
    assert all(e["product"] == e["reference product"] for e in prepared["exchanges"])
