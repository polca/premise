"""ALIGN-025: 270 micrometre wafer allocation for the retained 20 g silicon."""

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from inventory_assertions import inventory_cell_address

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-rail-freight.xlsx"
NAME = "power semiconductor module production, rail converter"
PRODUCT = "power semiconductor module, rail converter"
PARENT = "traction converter production, rail, medium voltage"
WAFER = "market for wafer, fabricated, for integrated circuit"
ELECTRICITY = "market group for electricity, medium voltage"
AREA = 0.03179144810046098
PARENT_PURCHASE = 0.037369640787949014
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
ALLOWED = {"B9", "B26", "B41", "B1012", "B1022", "B1032"}


OLD_METADATA = (
    "using300um silicon thickness and2330kg/m3 density:"
    "0.02/(2330*0.0003)m2/kgmodule."
)
NEW_METADATA = (
    "using270um fabricated-wafer allocation thickness and2330kg/m3 density:"
    "0.02/(2330*0.00027)=0.03179144810046098m2/kgmodule. This is an allocation "
    "reference;300um remains a diagnostic alternative, not measured rail die geometry."
)
METADATA_PINS = dict.fromkeys(("B9", "B26", "B41", "B1032"))
METADATA_FIELDS = (
    ("production of locomotive, diesel-electric", "source"),
    ("production of locomotive, electric catenary", "source"),
    ("production of locomotive, fuel cell, hydrogen", "description"),
    (PARENT, "comment"),
)


def activity(data, name=NAME):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def vector(data):
    return [
        (
            e["type"],
            e["name"],
            e["reference product"],
            e["location"],
            e["unit"],
            e["amount"],
        )
        for e in data["exchanges"]
    ]


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
        (ROOT / "scripts/data/rail-wafer-allocation-adoption-20261027.json").read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


def test_portable_approval_pins_original_module_clic_and_unchanged_39_profiles(
    adoption,
):
    assert adoption["approval"] == "ALIGN-025; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK

    assert (adoption["CLIC_dataset_id"], adoption["CLIC_version_id"]) == (3706, 5625)

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "version": "3.9",
        "system_model": "cutoff",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert set(adoption["changed_cells"]) == ALLOWED
    assert len(adoption["changed_cells"]) == 6
    assert adoption["amount_cell"] == "B1022"
    assert adoption["source_comment_cell"] == "B1012"
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}


def test_complete_raw_record_changes_only_wafer_area_and_root_note(raw, adoption):
    expected = deepcopy(adoption["before_snapshot"])
    exchange(expected, WAFER)["amount"] = AREA
    assert adoption["after_snapshot"] == expected
    actual = deepcopy(activity(raw.data))
    actual["comment"] = expected["comment"]
    assert actual == expected
    assert (
        actual["name"],
        actual["reference product"],
        actual["location"],
        actual["unit"],
        actual["production amount"],
    ) == (NAME, PRODUCT, "GLO", "kilogram", 1)
    assert len(actual["exchanges"]) == 8
    assert Counter(e["type"] for e in actual["exchanges"]) == {
        "production": 1,
        "technosphere": 7,
    }
    clic = adoption["CLIC_snapshot"]
    assert len(clic["exchanges"]) == 8
    assert vector(actual) == vector(clic)
    assert {
        k: v for k, v in actual.items() if k not in {"comment", "exchanges", "database"}
    } == {
        k: v for k, v in clic.items() if k not in {"comment", "exchanges", "database"}
    }
    # The complete vector forbids extra silicon, raw wafer, yield multipliers
    # or complete device purchases alongside the one fabricated-wafer service.
    assert [e for e in actual["exchanges"] if "wafer" in e["name"]] == [
        exchange(actual, WAFER)
    ]


def test_20_gram_geometry_allocation_and_retained_module_material_closure(
    raw, adoption
):
    assert adoption["wafer_thickness_um"] == 270
    assert adoption["alternative_thickness_um"] == 300
    assert adoption["retained_silicon_kg_per_kg_module"] == 0.02
    assert adoption["silicon_density_kg_per_m3"] == 2330
    assert adoption["wafer_area_m2_per_kg_module"] == AREA
    module = activity(raw.data)
    area = exchange(module, WAFER)["amount"]
    assert area == pytest.approx(0.02 / (2330 * 0.00027), rel=1e-14)
    assert area * 2330 * 0.00027 == pytest.approx(0.02, rel=1e-14)
    old = exchange(adoption["before_snapshot"], WAFER)["amount"]
    assert old == pytest.approx(0.02 / (2330 * 0.0003), rel=1e-14)
    assert area / old == pytest.approx(300 / 270, rel=1e-14)
    retained = sum(
        e["amount"]
        for e in module["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "kilogram"
    )
    # This is a nominal retained-mass allocation, not a fabrication-yield claim.
    assert retained + area * 2330 * 0.00027 == pytest.approx(1, rel=1e-14)
    assert (
        exchange(module, ELECTRICITY)["amount"]
        == adoption["retained_module_electricity_kWh_per_kg"]
        == 1
    )


def test_authoritative_module_note_retains_history_and_qualifies_both_thicknesses(
    raw, adoption
):
    comment = activity(raw.data)["comment"]
    old = adoption["before_snapshot"]["comment"]
    assert comment.endswith(old)
    note = comment[: -len(old)]
    assert note.startswith("APPROVED RAIL-MODULE WAFER ALLOCATION")
    for term in (
        "270 micrometre",
        "300 micrometre",
        "diagnostic alternative",
        "0.02 kg",
        str(AREA),
        "3.9",
        "supersedes",
    ):
        assert term in note
    assert "Native wafer fabrication and yields are purchased once" in note
    assert "no extra raw wafer or complete packaged-device input" in note
    assert "qualified engineering proxy" in note
    assert "Neither value establishes measured" in note


def test_four_exact_root_metadata_substitutions_preserve_all_other_cell_payloads(
    adoption,
):
    assert adoption["metadata_replacement"] == {
        "cells": ["B9", "B26", "B41", "B1032"],
        "fields": ["comment", "source", "description"],
        "before": OLD_METADATA,
        "after": NEW_METADATA,
    }
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    for name, field in METADATA_FIELDS:
        address = inventory_cell_address(INVENTORY_DIR / WORKBOOK, name, field)
        cell, payload = cells[SHEET, address]
        assert cell.get("t") == "inlineStr" and payload[1] is None
        assert OLD_METADATA not in payload[0]
        assert payload[0].count(NEW_METADATA) == 1


def test_parent_purchase_and_other_33_activities_change_only_four_metadata_fields(
    raw, adoption
):
    others = deepcopy([d for d in raw.data if d["name"] != NAME])
    assert len(raw.data) == 34 and len(others) == 33
    for name, field in METADATA_FIELDS:
        record = activity(others, name)
        assert OLD_METADATA not in record[field]
        assert record[field].count(NEW_METADATA) == 1
        record[field] = record[field].replace(NEW_METADATA, OLD_METADATA)

    buyers = [
        (d["name"], e)
        for d in raw.data
        for e in d["exchanges"]
        if e["type"] == "technosphere" and e["name"] == NAME
    ]
    ((parent, purchase),) = buyers
    assert parent == PARENT
    assert purchase == {
        "name": NAME,
        "amount": PARENT_PURCHASE,
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "reference product": PRODUCT,
    }
    assert purchase["amount"] == adoption["retained_parent_module_kg_per_kg_converter"]


def test_xml_exempts_only_six_approved_cells_and_preserves_formulas_and_caches():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        if not (sheet == SHEET and address in ALLOWED):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert cell.find(NS + "v") is not None
            assert cell.find(NS + "v").text is not None
            formulas.append([sheet, address, payload])
    assert cells

    assert formulas  # Formula/cache pairs are checked above.
    address = inventory_cell_address(
        INVENTORY_DIR / WORKBOOK, NAME, "amount", supplier=WAFER
    )
    amount = cells[SHEET, address][0]
    assert amount.attrib == {"r": address}
    assert [child.tag for child in amount] == [NS + "v"]
    assert amount.find(NS + "v").text == str(AREA)


def test_worksheet_structure_and_all_other_archive_parts_remain_exact():
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

    assert len(parts) == 10


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_preserves_native_wafer_allocation_and_original_source(
    raw, version, system_model
):
    source = deepcopy(raw)
    source.data = [activity(source.data)]
    background = [
        {k: e[k] for k in ("name", "reference product", "location", "unit")}
        | {"exchanges": []}
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
    assert importer.version_in == "3.9" and importer.version_out == version
    assert importer.list_unlinked == [] and background == before
    prepared = activity(importer.import_db.data)
    assert len(prepared["exchanges"]) == 8
    assert vector(prepared) == vector(activity(raw.data))
    assert all(e["product"] == e["reference product"] for e in prepared["exchanges"])


def test_rail_workbook_bytes_invalidate_only_import_cache(tmp_path, monkeypatch):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = "3.12"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    base = obj._database_cache_path("ALIGN025")
    imports = obj._database_cache_path("ALIGN025", inventories=True)
    original = module.FILEPATH_RAIL_FREIGHT.read_bytes()
    copied = tmp_path / WORKBOOK
    copied.write_bytes(original)
    monkeypatch.setattr(module, "FILEPATH_RAIL_FREIGHT", copied)
    assert obj._database_cache_path("ALIGN025", inventories=True) == imports
    copied.write_bytes(original + b"changed rail wafer allocation")
    assert obj._database_cache_path("ALIGN025", inventories=True) != imports
    assert obj._database_cache_path("ALIGN025") == base
