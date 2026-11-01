"""ALIGN-030: itemize four fitted packages within the existing 70 g board."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from test_central_fastener_alignment import (
    previous_fastener_source,
    previous_fastener_xml,
)
from test_interface_context_boundaries import (
    previous_foil_source,
    previous_interface_xml,
    restore_previous_interfaces,
)
from test_lis_factory_allocation import previous_factory_source

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiS.xlsx"
BOARD = "printed circuit board production, battery module control, mounted"
CENTRAL = "printed circuit board production, battery central control, mounted"
RESIDUAL = "battery auxiliary interface and power conditioning unit production"
CAPACITORS = "capacitor assembly production, battery monitoring board, 134 capacitors"
RESISTORS = "resistor assembly production, battery monitoring board, 128 resistors"
WAFER = "market for wafer, fabricated, for integrated circuit"
TPS3436 = "semiconductor package production, tps3436bdadaddfrq1"
TPS71533 = "semiconductor package production, tps71533qdckrq1"
TPD6 = "semiconductor package production, tpd6e004rser"
ISO7741 = "semiconductor package production, iso7741fqdwrq1"
# CLIC ID/version/hash, full package kg, silicon kg, flow count.
PINS = {
    TPS3436: (3668, 5583, 2.3422924e-05, 2.48103e-07, 15),
    TPS71533: (3669, 5584, 6.965561e-06, 1.29113e-07, 15),
    TPD6: (3670, 5585, 2.839255e-06, 1.72501e-07, 14),
    ISO7741: (3671, 5586, 0.000683625638, 3.19765e-06, 15),
}


NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
OLD_BOARD_ROWS = set(range(1206, 1235))
HELPER_RANGES = ((1725, 1749), (1752, 1776), (1779, 1802), (1805, 1829))
ADDED_ROWS = {*range(1832, 1865)} | {
    row for start, end in HELPER_RANGES for row in range(start, end + 1)
}


def snapshot(value):
    return json.loads(json.dumps(value, sort_keys=True))


def activity(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def plan():
    return json.loads(
        (
            ROOT / "tests/data/inventory_review/fitted-control-packages-adoption-20261027.json"
        ).read_text()
    )


def restore_previous_board(source):
    """Guard the approved new record before exposing its previous test view."""
    source = restore_previous_interfaces(source)
    approved = plan()
    actual = activity(source.data, BOARD)
    expected = approved["after_board_snapshot"]
    assert len(source.data) == 85
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert actual["comment"].endswith(approved["before_board_snapshot"]["comment"])
    assert [d["name"] for d in source.data[-5:]] == [*PINS, BOARD]
    for name in PINS:
        activity(source.data, name)  # Each new helper must be unique.
    previous = [d for d in source.data if d["name"] not in {*PINS, BOARD}]
    assert previous[56]["name"] == CENTRAL  # Independent before.xlsx board index 57.
    previous.insert(57, deepcopy(approved["before_board_snapshot"]))
    source.data = previous
    return source


def historical_board_cell(cells, sheet, address):
    """Restore the two approved board fields and their historical addresses."""
    current = {"B1212": "B1838", "B1233": "B1859"}.get(address, address)
    cell = deepcopy(cells[sheet, current][0])
    if address == "B1212":
        original = plan()["before_board_snapshot"]["comment"]
        text = cell.find(f"{NS}is/{NS}t")
        assert text.text.endswith(original)
        text.text = original
        cell.set("r", "B1212")
    elif address == "B1233":
        approved = plan()
        value = cell.find(NS + "v")
        assert value.text == str(approved["new_residual_kg_per_kg_board"])
        value.text = str(approved["old_residual_kg_per_kg_board"])
        cell.set("r", "B1233")
    return cell


def science(dataset):
    # Lists deliberately retain both independent TPS3436 silica-sand exchanges.
    return [
        {k: v for k, v in e.items() if k not in {"product", "comment"}}
        for e in dataset["exchanges"]
    ]


def xml_cells(path):
    cells = {}
    with ZipFile(path) as archive:
        strings = ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(NS + "si")
        for sheet in archive.namelist():
            if not (sheet.startswith("xl/worksheets/sheet") and sheet.endswith(".xml")):
                continue
            for cell in previous_interface_xml(
                previous_fastener_xml(
                    ET.fromstring(archive.read(sheet)), WORKBOOK, sheet
                ),
                sheet,
            ).iter(NS + "c"):
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
    return plan()


@pytest.fixture(scope="module")
def raw():
    return restore_previous_interfaces(
        previous_foil_source(
            previous_factory_source(
                previous_fastener_source(
                    ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK
                )
            )
        )
    )


def test_approval_pins_old_board_four_clic_packages_and_original39_profiles(adoption):
    assert adoption["approval"] == "ALIGN-030; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK

    assert (adoption["CLIC_board_dataset_id"], adoption["CLIC_board_version_id"]) == (
        3676,
        5591,
    )

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert len(adoption["new_helpers"]) == 4
    for helper in adoption["new_helpers"]:
        name = helper["snapshot"]["name"]
        dataset, version, mass, silicon, count = PINS[name]
        assert (helper["CLIC_dataset_id"], helper["CLIC_version_id"]) == (
            dataset,
            version,
        )

        assert helper["full_package_mass_kg"] == mass
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}
    assert adoption["unquantified_fitted_parts"] == {
        "CC2662R1FRGZT": 1,
        "ESD2CAN24DBZR": 2,
    }


def test_expanded_board_changes_only_residual_note_and_four_new_purchases(
    raw, adoption
):
    expected = deepcopy(adoption["before_board_snapshot"])
    exchange(expected, RESIDUAL)["amount"] = 0.07424213682857134
    for helper in adoption["new_helpers"]:
        a = helper["snapshot"]
        expected["exchanges"].append(
            {
                "name": a["name"],
                "reference product": a["reference product"],
                "product": a["reference product"],
                "location": "GLO",
                "unit": "unit",
                "type": "technosphere",
                "amount": 1 / 0.07,
            }
        )
    assert adoption["after_board_snapshot"] == expected
    actual = activity(raw.data, BOARD)
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert len(actual["exchanges"]) == 23
    assert actual["production amount"] == exchange(actual, BOARD)["amount"] == 1
    for name in PINS:
        purchase = exchange(actual, name)
        assert purchase["amount"] * 0.07 == pytest.approx(1, rel=1e-14)
        assert science({"exchanges": [purchase]}) == science(
            {"exchanges": [exchange(adoption["CLIC_board_snapshot"], name)]}
        )
    old = adoption["before_board_snapshot"]["comment"]
    assert actual["comment"].endswith(old)
    prefix = actual["comment"][: -len(old)]
    assert prefix.startswith("APPROVED FITTED CONTROL-PACKAGE ITEMIZATION")
    for term in (
        "70 g",
        "134 capacitors",
        "128 resistors",
        "including chemically unresolved additives",
        "rather than adding board mass",
        "do not fabricate",
        "renormalize",
        "270 micrometre",
        "CC2662R1FRGZT",
        "two ESD2CAN24DBZR",
        "unquantified",
        "3.9",
        "active",
        "superseded",
    ):
        assert term in prefix


@pytest.mark.parametrize("name", PINS)
def test_four_full_package_recipes_match_clic_preserve_unknown_mass_and_native_wafer(
    raw, adoption, name
):
    (helper,) = [h for h in adoption["new_helpers"] if h["snapshot"]["name"] == name]
    actual, snapshot = activity(raw.data, name), helper["snapshot"]
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in snapshot.items() if k != "comment"
    }
    assert science(actual) == science(helper["CLIC_snapshot"])
    mass, silicon, count = PINS[name][2:]
    assert len(actual["exchanges"]) == count
    assert actual["production amount"] == exchange(actual, name)["amount"] == 1
    assert actual["unit"] == "unit"
    assert "production amount" not in helper["CLIC_snapshot"]
    assert all(e["product"] == e["reference product"] for e in actual["exchanges"])
    wafer = exchange(actual, WAFER)
    assert wafer["amount"] * 2330 * 0.00027 == pytest.approx(silicon, rel=1e-14)
    assert wafer["unit"] == "square meter" and wafer["location"] == "GLO"
    known = (
        sum(
            e["amount"]
            for e in actual["exchanges"]
            if e["type"] == "technosphere" and e["unit"] == "kilogram"
        )
        + silicon
    )
    unknown = helper["unresolved_nominal_material_kg"]
    assert known == pytest.approx(helper["known_nominal_material_kg"], rel=1e-14)
    assert 0 < unknown and known < mass
    assert known + unknown == pytest.approx(mass, rel=1e-14)
    assert exchange(actual, "market group for electricity, medium voltage")[
        "amount"
    ] == pytest.approx(mass, rel=1e-14)
    assert actual["comment"].endswith(snapshot["comment"])
    assert actual["comment"].startswith("APPROVED FITTED CONTROL-PACKAGE ITEMIZATION")
    if name == TPS3436:
        assert [
            e["amount"]
            for e in actual["exchanges"]
            if e["name"] == "market for silica sand"
        ] == [2.3969e-8, 5.6543219999999996e-6]


def test_full_package_mass_including_unknowns_is_deducted_once_from_current_residual(
    raw, adoption
):
    assert adoption["board_mass_kg"] == 0.07
    total = sum(p[2] for p in PINS.values())
    assert adoption["added_full_package_mass_kg_per_board"] == 0.000716853378
    assert total == pytest.approx(0.000716853378, rel=1e-14)
    known = sum(h["known_nominal_material_kg"] for h in adoption["new_helpers"])
    unknown = sum(h["unresolved_nominal_material_kg"] for h in adoption["new_helpers"])
    assert known < total and unknown > 0
    assert known + unknown == pytest.approx(total, rel=1e-14)
    old, new = (
        0.08448289937142849,
        exchange(activity(raw.data, BOARD), RESIDUAL)["amount"],
    )
    assert old == adoption["old_residual_kg_per_kg_board"]
    assert new == adoption["new_residual_kg_per_kg_board"] == 0.07424213682857134
    assert (old - new) * 0.07 == pytest.approx(total, rel=1e-14)
    assert new * 0.07 == pytest.approx(0.005196949578, rel=1e-14)
    assert old * 0.07 == pytest.approx(total + new * 0.07, rel=1e-14)
    board = activity(raw.data, BOARD)
    assert (
        exchange(board, CAPACITORS)["amount"]
        == exchange(board, RESISTORS)["amount"]
        == 1 / 0.07
    )
    assert (
        adoption["retained_capacitor_count"],
        adoption["retained_resistor_count"],
    ) == (134, 128)


def test_other_80_original_records_parent_doses_and_previous_packages_are_unchanged(
    raw,
):
    others = [a for a in raw.data if a["name"] not in {*PINS, BOARD}]
    assert len(raw.data) == 85 and len(others) == 80

    assert [a["name"] for a in raw.data[-5:]] == [*PINS, BOARD]
    # Replacing the module board must not duplicate its already purchased ICs.
    board = activity(raw.data, BOARD)
    for name in (
        "battery monitoring integrated circuit production, copper wire bonding",
        "semiconductor package production, txu0204qpwrq1",
        "semiconductor package production, lm5168fqddarq1",
    ):
        assert exchange(board, name)["amount"] == 1 / 0.07


def test_xml_changes_only_removed_board_block_new_helpers_and_expanded_board():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        row = int("".join(x for x in address if x.isdigit()))
        assert not (sheet == SHEET and row in OLD_BOARD_ROWS)
        if not (sheet == SHEET and row in ADDED_ROWS):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert (
                cell.find(NS + "v") is not None and cell.find(NS + "v").text is not None
            )
            formulas.append([sheet, address, payload])
    assert len(outside) == 6621
    assert formulas  # Every formula above must retain its cached value.


def test_board_rows_are_exact_address_only_clones_except_one_note_and_residual(
    raw, adoption
):
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_interface_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        )
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    rows = {r.get("r"): r for r in root.iter(NS + "row")}

    comment_row = deepcopy(rows["1838"])
    text = comment_row.find(f"{NS}c[@r='B1838']/{NS}is/{NS}t")
    assert text.text == activity(raw.data, BOARD)["comment"]
    assert text.text.endswith(adoption["before_board_snapshot"]["comment"])
    text.text = adoption["before_board_snapshot"]["comment"]

    residual_row = deepcopy(rows["1859"])
    value = residual_row.find(f"{NS}c[@r='B1859']/{NS}v")
    assert value.text == "0.07424213682857134"
    value.text = "0.08448289937142849"

    assert (
        adoption["old_board_start"],
        adoption["old_board_end"],
        adoption["new_board_start"],
        adoption["new_board_end"],
        adoption["board_row_delta"],
    ) == (1206, 1234, 1832, 1864, 626)
    assert adoption["new_package_purchase_rows"] == [1861, 1862, 1863, 1864]
    assert {(h["start_row"], h["end_row"]) for h in adoption["new_helpers"]} == set(
        HELPER_RANGES
    )
    assert root.find(NS + "dimension").get("ref") == "A1:Q1864"
    root.find(NS + "dimension").set("ref", "A1:Q1722")
    for parent in root.iter():
        for child in list(parent):
            if child.tag == NS + "row" and int(child.get("r")) in ADDED_ROWS:
                parent.remove(child)

    assert len(parts) == 12


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_closes_four_helpers_and_board_without_mutating_background(
    raw, version, system_model
):
    selected = {*PINS, BOARD}
    source = deepcopy(raw)
    source.data = [a for a in source.data if a["name"] in selected]
    identities = {
        tuple(e[k] for k in ("name", "reference product", "location", "unit"))
        for a in source.data
        for e in a["exchanges"]
        if e["type"] == "technosphere" and e["name"] not in selected
    }
    if system_model == "consequential":
        identities = {
            (
                ("market for copper scrap, sorted, pressed", *key[1:])
                if key[0] == "copper scrap, sorted, pressed, Recycled Content cut-off"
                else key
            )
            for key in identities
        }
    background = [
        dict(zip(("name", "reference product", "location", "unit"), key), exchanges=[])
        for key in sorted(identities)
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
    assert len(importer.import_db.data) == 5
    fields = ("type", "name", "reference product", "location", "unit", "amount")
    for name in selected:
        actual = activity(importer.import_db.data, name)
        expected = deepcopy(activity(raw.data, name))
        if system_model == "consequential":
            for e in expected["exchanges"]:
                if (
                    e["name"]
                    == "copper scrap, sorted, pressed, Recycled Content cut-off"
                ):
                    e["name"] = "market for copper scrap, sorted, pressed"
        assert [tuple(e[k] for k in fields) for e in actual["exchanges"]] == [
            tuple(e[k] for k in fields) for e in expected["exchanges"]
        ]
        assert all(e["product"] == e["reference product"] for e in actual["exchanges"])
