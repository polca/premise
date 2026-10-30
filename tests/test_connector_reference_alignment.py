"""ALIGN-029: five-header Molex material proxy, with unassigned reject fate."""

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
NAME = "connector production, battery control board, plated contacts"
CENTRAL = "printed circuit board production, battery central control, mounted"
MODULE = "printed circuit board production, battery module control, mounted"
WASTE = "market for hazardous waste, for incineration"
ELECTRICITY = "market group for electricity, medium voltage"
MATERIALS = {
    "market for copper, cathode": 0.13010147010651943,
    "market for zinc": 0.023540676135075888,
    "market for tin": 0.0036574969023284313,
    "market for phosphorus, white, liquid": 0.00015693784090050594,
    "market for gold": 0.000249015884276668,
    "market for nickel, class 1": 0.0012759017674509759,
    "market for nylon 6": 0.6255550562175716,
    "market for glass fibre": 0.2680950240932449,
}
AMOUNTS = dict(zip((f"B{r}" for r in range(1673, 1681)), MATERIALS.values()))
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
COMMENT_PINS = dict.fromkeys(("B1185", "B1212", "B1668"))
ALLOWED = set(AMOUNTS) | set(COMMENT_PINS) | {"B1667"}


def activity(data, name=NAME):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def science(data):
    return [
        {k: v for k, v in e.items() if k not in {"product", "comment"}}
        for e in data["exchanges"]
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
        (ROOT / "scripts/data/connector-reference-adoption-20261027.json").read_text()
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
def note(raw, adoption):
    old = adoption["before_snapshot"]["comment"]
    comment = activity(raw.data)["comment"]
    assert comment.endswith(old)
    return comment[: -len(old)]


def test_portable_approval_pins_original_clic_h09_and_original39_source(adoption):
    assert adoption["approval"] == "ALIGN-029; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK

    assert (adoption["CLIC_dataset_id"], adoption["CLIC_version_id"]) == (3674, 5589)

    (
        json.dumps(adoption["independent_calculations"], sort_keys=True, indent=2)
        + "\n"
    ).encode()

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "version": "3.9",
        "system_model": "cutoff",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["amount_cells"] == AMOUNTS
    assert set(adoption["comment_cells"]) == set(COMMENT_PINS)
    assert adoption["source_cell"] == "B1667"
    assert (
        adoption["removed_disposal_row"],
        adoption["relocated_electricity_from"],
        adoption["relocated_electricity_to"],
    ) == (1681, 1682, 1681)
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}


def test_full_raw_record_changes_only_eight_amounts_source_note_and_disposal(
    raw, adoption
):
    before = adoption["before_snapshot"]
    expected = deepcopy(before)
    for name, amount in MATERIALS.items():
        exchange(expected, name)["amount"] = amount
    expected["exchanges"] = [e for e in expected["exchanges"] if e["name"] != WASTE]
    expected["source"] = adoption["CLIC_snapshot"]["source"]
    assert adoption["after_snapshot"] == expected
    actual = activity(raw.data)
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert len(before["exchanges"]) == 11 and len(actual["exchanges"]) == 10
    assert science(actual) == science(adoption["CLIC_snapshot"])
    assert (actual["location"], actual["unit"], actual["production amount"]) == (
        "GLO",
        "kilogram",
        1,
    )
    assert all(e["product"] == e["reference product"] for e in actual["exchanges"])
    assert exchange(actual, ELECTRICITY) == exchange(before, ELECTRICITY)
    assert before["source"] in actual["comment"]


def test_five_header_cohort_and_handling_ledger_keep_loss_quantified_but_unassigned(
    raw, adoption
):
    counts, contacts, grams = (2, 2, 1), (4, 11, 6), (1.023, 2.564, 1.420)
    assert sum(counts) == adoption["cohort_headers"] == 5
    assert (
        sum(n * p for n, p in zip(counts, contacts))
        == adoption["cohort_contacts"]
        == 36
    )
    assert sum(n * g for n, g in zip(counts, grams)) * 1e-3 == pytest.approx(
        adoption["cohort_mass_kg"], rel=1e-14
    )
    assert adoption["cohort_mass_kg"] == 0.008594
    assert adoption["handling_reference"] == 0.95
    actual = activity(raw.data)
    feed = sum(exchange(actual, name)["amount"] for name in MATERIALS)
    loss = adoption["nominal_reject_kg_per_kg_finished"]
    assert loss == 0.05263157894736836
    assert feed == pytest.approx(1 / 0.95, rel=1e-14)
    assert feed * 0.95 == pytest.approx(1, rel=1e-14)
    assert feed - 1 == pytest.approx(loss, rel=1e-14)
    assert (
        exchange(actual, ELECTRICITY)["amount"]
        == adoption["retained_assembly_kWh_per_kg"]
        == 1
    )
    assert all(e["amount"] > 0 for e in actual["exchanges"])
    assert (
        adoption["independent_calculations"]["manufacturing_reject_fate_assigned"]
        is False
    )


def test_h09_plating_volume_then_alloy_mass_reproduces_eight_material_feeds(
    raw, adoption
):
    # Independent reconstruction of the retained thin-layer approximation.
    n, width, length, cohort = 36, 0.00064, 0.010, 0.008594
    finished = n * width**2 * length
    volumes = {
        "Au": n * 4 * width * 0.003 * 0.381e-6,
        "Ni": n * 4 * width * length * 1.27e-6,
        "Sn": n * 4 * width * 0.0033 * 1.905e-6,
    }
    densities = {"Au": 19300, "Ni": 8900, "Sn": 7310}
    coatings = {metal: volume * densities[metal] for metal, volume in volumes.items()}
    base_volume = finished - sum(volumes.values())
    base_mass = base_volume * 8800
    housing = cohort - base_mass - sum(coatings.values())
    retained = {
        "market for copper, cathode": base_mass * (0.5 * 0.70 + 0.5 * 0.958),
        "market for zinc": base_mass * 0.5 * 0.30,
        "market for tin": base_mass * 0.5 * 0.04 + coatings["Sn"],
        "market for phosphorus, white, liquid": base_mass * 0.5 * 0.002,
        "market for gold": coatings["Au"],
        "market for nickel, class 1": coatings["Ni"],
        "market for nylon 6": housing * 0.70,
        "market for glass fibre": housing * 0.30,
    }
    assert sum(retained.values()) == pytest.approx(cohort, rel=1e-14)
    for name, mass in retained.items():
        assert exchange(activity(raw.data), name)["amount"] == pytest.approx(
            mass / cohort / 0.95, rel=1e-14
        )
    receipt = adoption["independent_calculations"]
    assert base_volume == pytest.approx(
        receipt["corrected_base_metal_volume_m3"], rel=1e-14
    )
    assert base_mass == pytest.approx(receipt["corrected_base_metal_kg"], rel=1e-14)
    # Subtracting plating mass instead of its volume gives the superseded H09
    # approximation even though all geometric and alloy assumptions are fixed.
    old_base_mass = finished * 8800 - sum(coatings.values())
    assert old_base_mass == pytest.approx(receipt["original_base_metal_kg"], rel=1e-14)
    assert base_mass > old_base_mass
    assert (base_mass - old_base_mass) / cohort == pytest.approx(
        receipt["additional_alloy_kg_per_kg_connector"], rel=1e-12
    )


def test_three_authoritative_prepends_and_source_replacement_preserve_all_old_payloads(
    raw, adoption, note
):
    assert note.startswith("APPROVED MOLEX CONNECTOR MATERIAL/REJECT-FATE REFERENCE")
    for term in (
        "five-Molex-header",
        "36-contact",
        "8.594 g",
        "does not establish",
        "Samtec header is absent",
        "engineering assumptions",
        "not measured deposition efficiency",
        "0.05263157894736836",
        "treatment burdens are unassigned",
        "without claiming zero physical loss",
        "no extra component retirement",
        "3.9",
        "supersedes",
    ):
        assert term in note
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    for address, pin in COMMENT_PINS.items():
        cell = historical_board_cell(cells, SHEET, address)
        text = cell.find(f"{NS}is/{NS}t")
        assert text.text.startswith(note)
        text.text = text.text[len(note) :]

    cell = deepcopy(cells[SHEET, "B1667"][0])
    text = cell.find(f"{NS}is/{NS}t")
    assert text.text == adoption["CLIC_snapshot"]["source"]
    text.text = adoption["before_snapshot"]["source"]


def test_other_80_activities_and_downstream_allowances_are_unchanged(
    raw, adoption, note
):
    others = deepcopy([d for d in raw.data if d["name"] != NAME])
    assert len(raw.data) == 81 and len(others) == 80
    for parent in (CENTRAL, MODULE):
        record = activity(others, parent)
        assert record["comment"].startswith(note)
        record["comment"] = record["comment"][len(note) :]

    assert adoption["downstream_allowances_kg"] == {
        "module_board": 0.02,
        "central_board": 0.1,
    }
    assert exchange(activity(raw.data, MODULE), NAME)["amount"] * 0.07 == pytest.approx(
        0.02, rel=1e-14
    )
    assert exchange(activity(raw.data, CENTRAL), NAME)[
        "amount"
    ] * 0.49 == pytest.approx(0.1, rel=1e-14)


def test_xml_exempts_only_eight_amounts_four_metadata_cells_and_two_exchange_rows():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        if not (
            sheet == SHEET
            and (
                address in ALLOWED or row in {1681, 1682} | OLD_BOARD_ROWS | FITTED_ROWS
            )
        ):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert (
                cell.find(NS + "v") is not None and cell.find(NS + "v").text is not None
            )
            formulas.append([sheet, address, payload])
    assert len(outside) == 6603

    assert formulas  # Every formula above must retain its cached value.
    for address, value in AMOUNTS.items():
        cell = cells[SHEET, address][0]
        assert cell.attrib == {"r": address}
        assert [child.tag for child in cell] == [NS + "v"]
        assert cell.find(NS + "v").text == str(value)


def test_final_electricity_row_is_exact_original_clone_and_all_other_structure_parts_persist():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_interface_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        )
        parts = [
            [name, archive.read(name)]
            for name in sorted(archive.namelist())
            if name != SHEET
        ]
    rows = {r.get("r"): r for r in root.iter(NS + "row")}
    assert "1682" not in rows

    assert root.find(NS + "dimension").get("ref") == "A1:Q1864"
    root.find(NS + "dimension").set("ref", "A1:Q1722")
    for parent in root.iter():
        for child in list(parent):
            if child.tag == NS + "c" and child.get("r") in ALLOWED:
                parent.remove(child)
            elif (
                child.tag == NS + "row"
                and int(child.get("r")) in {1681, 1682} | OLD_BOARD_ROWS | FITTED_ROWS
            ):
                parent.remove(child)

    assert len(parts) == 12


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_retains_all_nine_inputs_without_reintroducing_disposal(
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
    actual = activity(importer.import_db.data)
    assert len(actual["exchanges"]) == 10
    fields = ("type", "name", "reference product", "location", "unit", "amount")
    assert [tuple(e[k] for k in fields) for e in actual["exchanges"]] == [
        tuple(e[k] for k in fields) for e in activity(raw.data)["exchanges"]
    ]
    assert not any(e["name"] == WASTE for e in actual["exchanges"])
