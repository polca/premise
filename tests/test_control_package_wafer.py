"""ALIGN-028: four package wafer allocations, retaining every other quantity."""

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
from test_fitted_control_packages import (
    ADDED_ROWS as FITTED_ROWS,
)
from test_fitted_control_packages import (
    OLD_BOARD_ROWS,
    historical_board_cell,
    restore_previous_board,
)
from test_interface_context_boundaries import (
    previous_foil_source,
    previous_interface_xml,
)
from test_lis_factory_allocation import previous_factory_source

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiS.xlsx"
WAFER = "market for wafer, fabricated, for integrated circuit"
MONITOR = "battery monitoring integrated circuit production, copper wire bonding"
CC2662 = "semiconductor package production, cc2662r1ftwrgzrq1"
LM5168 = "semiconductor package production, lm5168fqddarq1"
TXU0204 = "semiconductor package production, txu0204qpwrq1"
PINS = {
    MONITOR: (3677, 5592, 2.0286076e-05),
    CC2662: (3665, 5580, 6.9672410000000005e-06),
    LM5168: (3666, 5581, 3.867195e-06),
    TXU0204: (3667, 5582, 3.1738899999999993e-07),
}
AMOUNTS = {
    "B1176": 3.224618661580035e-5,
    "B1535": 1.1074934032745192e-5,
    "B1563": 6.147186456843109e-6,
    "B1589": 5.045127960578604e-7,
}
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
COMMENT_PINS = dict.fromkeys(
    ("B1085", "B1126", "B1157", "B1185", "B1212", "B1518", "B1545", "B1573")
)
ALLOWED = set(AMOUNTS) | set(COMMENT_PINS)
REPLACEMENTS = [
    {
        "cells": ["B1085", "B1126", "B1157", "B1185", "B1212"],
        "before": "Fabricatedwafer proxy300um2330kg/m3",
        "after": "Fabricatedwafer proxy270um allocation reference at2330kg/m3 (300um diagnostic alternative; fitted die geometry unverified)",
    },
    {
        "cells": ["B1185", "B1212", "B1518", "B1545", "B1573"],
        "before": "wafer area from declared Si mass at explicit300micrometrethickness,2330kg/m3",
        "after": "wafer area from declared Si mass at the270micrometre allocation reference,2330kg/m3 (300micrometre diagnostic alternative; fitted die geometry unverified)",
    },
]


# ALIGN-029 before.xlsx (013a068b247f561ccb6295c5c5b75bcd28dfe82b65ee8102a
# 9db69fd074c42e0) independently reproduces the previous outside pin. The
# connector tests protect these exact additional cells, rows and record.
CONNECTOR = "connector production, battery control board, plated contacts"
CONNECTOR_PARENTS = {
    "printed circuit board production, battery central control, mounted",
    "printed circuit board production, battery module control, mounted",
}
CONNECTOR_CELLS = {"B1185", "B1212", "B1667", "B1668"} | {
    f"B{r}" for r in range(1673, 1681)
}


def activity(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def wafer(dataset):
    (match,) = [e for e in dataset["exchanges"] if e["name"] == WAFER]
    return match


def science(dataset):
    # Raw product aliases are checked separately; CLIC omits those aliases.
    return [
        {k: v for k, v in e.items() if k not in {"comment", "product"}}
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
    return json.loads(
        (ROOT / "tests/data/inventory_review/control-package-wafer-adoption-20261027.json").read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return restore_previous_board(
        previous_foil_source(
            previous_factory_source(
                previous_fastener_source(
                    ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK
                )
            )
        )
    )


@pytest.fixture(scope="module")
def connector_note(raw):
    plan = json.loads(
        (ROOT / "tests/data/inventory_review/connector-reference-adoption-20261027.json").read_text()
    )
    comment = activity(raw.data, CONNECTOR)["comment"]
    old = plan["before_snapshot"]["comment"]
    assert comment.endswith(old)
    note = comment[: -len(old)]
    assert note
    return note


def test_portable_approval_pins_four_packages_original_profiles_and_exact_twelve_cells(
    adoption,
):
    assert adoption["approval"] == "ALIGN-028; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK
    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "version": "3.9",
        "system_model": "cutoff",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert len(adoption["cases"]) == 4
    assert {c["before_snapshot"]["name"] for c in adoption["cases"]} == PINS.keys()
    assert adoption["amount_cells"] == AMOUNTS
    assert set(adoption["comment_cells"]) == set(COMMENT_PINS)
    assert adoption["metadata_replacements"] == REPLACEMENTS
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}


@pytest.mark.parametrize("name", PINS)
def test_complete_raw_package_changes_only_one_wafer_amount_and_matches_clic(
    raw, adoption, name
):
    (case,) = [c for c in adoption["cases"] if c["before_snapshot"]["name"] == name]
    dataset_id, version_id, silicon = PINS[name]

    assert (case["CLIC_dataset_id"], case["CLIC_version_id"]) == (
        dataset_id,
        version_id,
    )

    assert case["retained_silicon_kg_per_package"] == silicon
    expected = deepcopy(case["before_snapshot"])
    wafer(expected)["amount"] = AMOUNTS[case["amount_cell"]]
    assert case["after_snapshot"] == expected
    actual = activity(raw.data, name)
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert science(actual) == science(case["CLIC_snapshot"])
    for field in ("name", "reference product", "location", "unit", "production amount"):
        assert actual[field] == case["CLIC_snapshot"][field]
    assert actual["unit"] == "unit" and actual["production amount"] == 1
    assert [e for e in actual["exchanges"] if "wafer" in e["name"]] == [wafer(actual)]
    assert (
        wafer(actual)["reference product"],
        wafer(actual)["location"],
        wafer(actual)["unit"],
    ) == ("wafer, fabricated, for integrated circuit", "GLO", "square meter")


def test_270um_geometry_retains_each_silicon_mass_without_a_second_yield_factor(
    raw, adoption
):
    assert (
        adoption["original_thickness_um"],
        adoption["adopted_thickness_um"],
        adoption["silicon_density_kg_per_m3"],
    ) == (300, 270, 2330)
    for case in adoption["cases"]:
        name = case["before_snapshot"]["name"]
        silicon = PINS[name][2]
        old = wafer(case["before_snapshot"])["amount"]
        new = wafer(activity(raw.data, name))["amount"]
        assert old * 2330 * 0.0003 == pytest.approx(silicon, rel=1e-14)
        assert new == pytest.approx(silicon / (2330 * 0.00027), rel=1e-14)
        assert new * 2330 * 0.00027 == pytest.approx(silicon, rel=1e-14)
        assert new / old == pytest.approx(300 / 270, rel=1e-14)


def test_exact_metadata_substitutions_and_four_qualified_history_preserving_notes(
    adoption,
    connector_note,
):
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    cases = {c["comment_cell"]: c for c in adoption["cases"]}
    assert cases.keys() == {"B1157", "B1518", "B1545", "B1573"}
    for address, pin in COMMENT_PINS.items():
        cell = historical_board_cell(cells, SHEET, address)
        text = cell.find(f"{NS}is/{NS}t")
        if address in {"B1185", "B1212"}:
            assert text.text.startswith(connector_note)
            text.text = text.text[len(connector_note) :]
        original_text = text.text
        for replacement in REPLACEMENTS:
            if address in replacement["cells"]:
                assert original_text.count(replacement["after"]) == 1
                assert replacement["before"] not in original_text
        if address in cases:
            history = cases[address]["before_snapshot"]["comment"]
            for replacement in REPLACEMENTS:
                history = history.replace(replacement["before"], replacement["after"])
            assert original_text.endswith(history)
            prefix = original_text[: -len(history)]
            assert prefix.startswith("APPROVED CONTROL-PACKAGE WAFER ALLOCATION")
            for term in (
                "270 micrometre",
                "300 micrometre",
                "diagnostic alternative",
                "not measured",
                "Native fabrication and yields",
                "purchased once",
                "without another raw-silicon",
                "BQ79616 material proxy",
                "BQ79718 role",
                "without a functional replacement claim",
                "3.9",
                "supersedes",
            ):
                assert term in prefix
            text.text = cases[address]["before_snapshot"]["comment"]
        else:
            for replacement in REPLACEMENTS:
                text.text = text.text.replace(
                    replacement["after"], replacement["before"]
                )


def test_all_76_other_retained_records_keep_parent_doses_board_budgets_and_data(
    raw, connector_note
):
    others = deepcopy([d for d in raw.data if d["name"] not in {*PINS, CONNECTOR}])
    assert len(raw.data) == 81 and len(others) == 76
    for record in others:
        if isinstance(record.get("comment"), str):
            if record["name"] in CONNECTOR_PARENTS:
                assert record["comment"].startswith(connector_note)
                record["comment"] = record["comment"][len(connector_note) :]
            for replacement in REPLACEMENTS:
                record["comment"] = record["comment"].replace(
                    replacement["after"], replacement["before"]
                )


def test_xml_allows_only_twelve_cells_with_cached_formula_values_preserved():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        if not (
            sheet == SHEET
            and (
                address in ALLOWED | CONNECTOR_CELLS
                or row in {1681, 1682} | OLD_BOARD_ROWS | FITTED_ROWS
            )
        ):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert (
                cell.find(NS + "v") is not None and cell.find(NS + "v").text is not None
            )
            formulas.append([sheet, address, payload])
    assert len(outside) == 6593

    assert formulas  # Every formula above must retain its cached value.
    for address, value in AMOUNTS.items():
        cell = cells[SHEET, address][0]
        assert cell.attrib == {"r": address}
        assert [child.tag for child in cell] == [NS + "v"]
        assert cell.find(NS + "v").text == str(value)


def test_every_other_worksheet_element_row_address_and_zip_part_remains_exact():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_interface_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        )
        parts = [
            [name, archive.read(name)]
            for name in sorted(archive.namelist())
            if name != SHEET
        ]
    assert root.find(NS + "dimension").get("ref") == "A1:Q1864"
    root.find(NS + "dimension").set("ref", "A1:Q1722")
    for parent in root.iter():
        for cell in list(parent):
            if cell.tag == NS + "c" and cell.get("r") in ALLOWED | CONNECTOR_CELLS:
                parent.remove(cell)
            elif (
                cell.tag == NS + "row"
                and int(cell.get("r")) in {1681, 1682} | OLD_BOARD_ROWS | FITTED_ROWS
            ):
                parent.remove(cell)

    assert len(parts) == 12


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_four_package_preparation_preserves_native_wafer_and_original39_source(
    raw, version, system_model
):
    source = deepcopy(raw)
    source.data = [activity(source.data, name) for name in PINS]
    identities = {
        tuple(e[k] for k in ("name", "reference product", "location", "unit"))
        for d in source.data
        for e in d["exchanges"]
        if e["type"] == "technosphere"
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
    assert len(importer.import_db.data) == 4
    fields = ("type", "name", "reference product", "location", "unit", "amount")
    for name in PINS:
        actual = activity(importer.import_db.data, name)
        assert [tuple(e[k] for k in fields) for e in actual["exchanges"]] == [
            tuple(e[k] for k in fields) for e in activity(raw.data, name)["exchanges"]
        ]
        assert all(e["product"] == e["reference product"] for e in actual["exchanges"])
