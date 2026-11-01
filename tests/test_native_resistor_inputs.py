"""ALIGN-026: three scoped native whole-resistor purchases, no helper removal."""

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
BUNDLE = "resistor assembly production, battery monitoring board, 128 resistors"
CENTRAL = "printed circuit board production, battery central control, mounted"
INTERFACE = "battery auxiliary interface and power conditioning unit production"
NAMES = (BUNDLE, CENTRAL, INTERFACE)
FILM, JUMPER = "thick-film resistor production", "zero-ohm jumper production"
NATIVE = (
    "market for resistor, surface-mounted",
    "resistor, surface-mounted",
    "GLO",
    "kilogram",
)
PINS = {
    BUNDLE: (3672, 5587, 0.00053595, 2),
    CENTRAL: (3675, 5590, 0.006122448979591837, 15),
    INTERFACE: (3673, 5588, 0.005, 13),
}
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
IDENTITY_CELLS = {
    f"{column}{row}" for column in "AGI" for row in (1200, 1508, 1612)
} - {"I1200"}
COMMENTS = {"B1185", "B1503", "B1599"}
ALLOWED = IDENTITY_CELLS | COMMENTS | {"B1508"}


CAP_BUNDLE = "capacitor assembly production, battery monitoring board, 134 capacitors"
CAP_BOARD = "printed circuit board production, battery module control, mounted"
CAP_HELPERS = {"capacitor production, c0g, 0603", "capacitor production, c0g, 0805"}
CAP_CELLS = {
    "B1185",
    "B1212",
    "B1243",
    "B1261",
    "B1279",
    "B1299",
    "B1319",
    "B1339",
    "B1359",
    "B1380",
    "B1400",
    "B1233",
    "B1405",
    "B1406",
    "B1407",
    "B1410",
}
CAP_ADDED_ROWS = {1411, 1412, *range(1685, 1703), *range(1705, 1723)}


WAFER_NAMES = {
    "battery monitoring integrated circuit production, copper wire bonding",
    "semiconductor package production, cc2662r1ftwrgzrq1",
    "semiconductor package production, lm5168fqddarq1",
    "semiconductor package production, txu0204qpwrq1",
}
WAFER_CELLS = {
    "B1085",
    "B1126",
    "B1157",
    "B1185",
    "B1212",
    "B1518",
    "B1545",
    "B1573",
    "B1176",
    "B1535",
    "B1563",
    "B1589",
}


# Independent ALIGN-029 before.xlsx (013a068b247f561ccb6295c5c5b75bcd28dfe82
# b65ee8102a9db69fd074c42e0) reproduces the previous outside pin. Connector
# tests separately protect the exact cells/rows and the one revised record.
CONNECTOR = "connector production, battery control board, plated contacts"
CONNECTOR_CELLS = {"B1185", "B1212", "B1667", "B1668"} | {
    f"B{r}" for r in range(1673, 1681)
}


def activity(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def vector(dataset):
    return [
        (
            e["type"],
            e["name"],
            e["reference product"],
            e["location"],
            e["unit"],
            e["amount"],
        )
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
        (
            ROOT / "tests/data/inventory_review/native-resistor-inputs-adoption-20261027.json"
        ).read_text()
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


@pytest.fixture(scope="module")
def capacitor_adoption():
    return json.loads(
        (ROOT / "tests/data/inventory_review/capacitor-family-adoption-20261027.json").read_text()
    )


@pytest.fixture(scope="module")
def control_wafer_adoption():
    return json.loads(
        (ROOT / "tests/data/inventory_review/control-package-wafer-adoption-20261027.json").read_text()
    )


def restore_wafer_text(text, plan):
    for replacement in plan["metadata_replacements"]:
        text = text.replace(replacement["after"], replacement["before"])
    return text


def test_portable_approval_pins_three_cases_original_profiles_and_exact_scope(adoption):
    assert adoption["approval"] == "ALIGN-026; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "version": "3.9",
        "system_model": "cutoff",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert tuple(adoption["native_identity"]) == NATIVE
    assert len(adoption["cases"]) == 3
    assert {c["before_snapshot"]["name"] for c in adoption["cases"]} == set(NAMES)
    assert set(adoption["identity_cells"]) == IDENTITY_CELLS
    assert len(adoption["identity_cells"]) == 8
    assert set(adoption["comment_cells"]) == COMMENTS
    assert adoption["amount_cell"] == "B1508" and adoption["removed_row"] == 1509
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}
    assert adoption["existing_unused_helpers_preserved"] is True


@pytest.mark.parametrize("name", NAMES)
def test_only_reviewed_purchase_changes_with_full_other_fields_and_aliases_preserved(
    raw, adoption, name, capacitor_adoption, control_wafer_adoption, connector_note
):
    (case,) = [c for c in adoption["cases"] if c["before_snapshot"]["name"] == name]
    dataset_id, version_id, amount, count = PINS[name]
    before = case["before_snapshot"]

    assert (case["CLIC_dataset_id"], case["CLIC_version_id"]) == (
        dataset_id,
        version_id,
    )

    expected = deepcopy(before)
    resistor = exchange(expected, FILM)
    resistor.update(
        name=NATIVE[0],
        product=NATIVE[1],
        **{"reference product": NATIVE[1], "amount": amount},
    )
    if name == BUNDLE:
        expected["exchanges"] = [
            e for e in expected["exchanges"] if e["name"] != JUMPER
        ]
    assert case["after_snapshot"] == expected
    actual = activity(raw.data, name)
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert len(actual["exchanges"]) == count
    assert not any(e["name"] in {FILM, JUMPER} for e in actual["exchanges"])
    purchase = exchange(actual, NATIVE[0])
    assert (
        purchase["name"],
        purchase["reference product"],
        purchase["location"],
        purchase["unit"],
    ) == NATIVE
    assert purchase["product"] == NATIVE[1] and purchase["amount"] == amount
    clic = exchange(case["CLIC_snapshot"], NATIVE[0])
    assert vector({"exchanges": [purchase]}) == vector({"exchanges": [clic]})
    if name == BUNDLE:
        assert vector(actual) == vector(case["CLIC_snapshot"])
        assert actual["unit"] == "unit" and actual["production amount"] == 1
    else:
        assert actual["unit"] == "kilogram" and actual["production amount"] == 1
    comment = actual["comment"]
    if name == CENTRAL:
        assert comment.startswith(connector_note)
        comment = comment[len(connector_note) :]
    comment = restore_wafer_text(comment, control_wafer_adoption)
    if name == CENTRAL:
        replacement = capacitor_adoption["metadata_replacement"]
        comment = comment.replace(replacement["after"], replacement["before"])
    assert comment.endswith(before["comment"])
    note = comment[: -len(before["comment"])]
    assert note.startswith("APPROVED NATIVE WHOLE-RESISTOR MANUFACTURING PROXY")
    for term in (
        "generic whole-component",
        "111 nonzero",
        "17 zero-ohm",
        "counts apply only",
        "not the central-board",
        "does not certify",
        "Ru/Pd",
        "not adopted",
        "Native supplier uncertainty remains",
        "no additional EOL",
        "3.9",
        "supersedes",
    ):
        assert term in note


def test_fitted_bundle_mass_ledger_and_distinct_central_interface_allowances(
    raw, adoption
):
    nonzero = (62, 9, 0, 38, 2)
    jumpers = (1, 12, 4, 0, 0)
    family_mg = (0.65, 2, 5.5, 10, 25.5)
    assert sum(nonzero) == adoption["nonzero_resistor_count"] == 111
    assert sum(jumpers) == adoption["jumper_count"] == 17
    assert sum(nonzero) + sum(jumpers) == 128
    film_mass = sum(n * mg for n, mg in zip(nonzero, family_mg)) * 1e-6
    jumper_mass = sum(n * mg for n, mg in zip(jumpers, family_mg)) * 1e-6
    assert film_mass == pytest.approx(adoption["nonzero_resistor_mass_kg"], rel=1e-14)
    assert jumper_mass == pytest.approx(adoption["jumper_mass_kg"], rel=1e-14)
    bundle = exchange(activity(raw.data, BUNDLE), NATIVE[0])["amount"]
    assert bundle == adoption["bundle_mass_kg"] == 0.00053595
    assert bundle == pytest.approx(film_mass + jumper_mass, rel=1e-14)
    central = exchange(activity(raw.data, CENTRAL), NATIVE[0])["amount"]
    assert (
        central == adoption["central_resistor_kg_per_kg_board"] == 0.006122448979591837
    )
    assert central * 0.49 == pytest.approx(0.003, rel=1e-14)
    assert (
        exchange(activity(raw.data, INTERFACE), NATIVE[0])["amount"]
        == adoption["interface_resistor_kg_per_kg_interface"]
        == 0.005
    )


def test_other_69_retained_activities_helpers_and_old_native_board_stay_exact(
    raw,
    capacitor_adoption,
    control_wafer_adoption,
):
    excluded = {*NAMES, CAP_BUNDLE, CAP_BOARD, CONNECTOR} | CAP_HELPERS | WAFER_NAMES
    others = deepcopy([d for d in raw.data if d["name"] not in excluded])
    assert len(raw.data) == 81 and len(others) == 69
    replacement = capacitor_adoption["metadata_replacement"]
    for record in others:
        if isinstance(record.get("comment"), str):
            record["comment"] = restore_wafer_text(
                record["comment"], control_wafer_adoption
            )
            record["comment"] = record["comment"].replace(
                replacement["after"], replacement["before"]
            )

    for helper in (
        FILM,
        JUMPER,
        "ruthenium dioxide production, for resistor films",
        "lead oxide production, for resistor glass",
    ):
        activity(others, helper)
    board = activity(
        others, "board components and panel input to logic board, for LiS battery"
    )
    assert exchange(board, NATIVE[0])["amount"] == 0.04428571428571428


def test_xml_allows_only_twelve_changed_cells_and_deleted_final_jumper_row():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        assert not (sheet == SHEET and row == 1509)
        if not (
            sheet == SHEET
            and (
                address in ALLOWED | CAP_CELLS | WAFER_CELLS | CONNECTOR_CELLS
                or row in CAP_ADDED_ROWS | {1681, 1682} | OLD_BOARD_ROWS | FITTED_ROWS
            )
        ):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert cell.find(NS + "v") is not None
            assert cell.find(NS + "v").text is not None
            formulas.append([sheet, address, payload])
    assert len(outside) == 6391

    assert formulas  # Every formula above must retain its cached value.
    for row in (1200, 1508, 1612):
        for column, text in (("A", NATIVE[0]), ("G", NATIVE[1]), ("I", NATIVE[1])):
            cell = cells[SHEET, f"{column}{row}"][0]
            assert "".join(cell.find(NS + "is").itertext()) == text
    amount = cells[SHEET, "B1508"][0]
    assert amount.attrib == {"r": "B1508"}
    assert [c.tag for c in amount] == [NS + "v"]
    assert amount.find(NS + "v").text == "0.00053595"


def test_worksheet_structure_and_other_zip_parts_are_preserved():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_interface_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        )
        parts = [
            [name, archive.read(name)]
            for name in sorted(archive.namelist())
            if name != SHEET
        ]
    assert not any(r.get("r") == "1509" for r in root.iter(NS + "row"))
    assert root.find(NS + "dimension").get("ref") == "A1:Q1864"
    root.find(NS + "dimension").set("ref", "A1:Q1682")
    for parent in root.iter():
        for child in list(parent):
            if (
                child.tag == NS + "c"
                and child.get("r")
                in ALLOWED | CAP_CELLS | WAFER_CELLS | CONNECTOR_CELLS
            ):
                parent.remove(child)
            elif (
                child.tag == NS + "row"
                and int(child.get("r"))
                in CAP_ADDED_ROWS | {1681, 1682} | OLD_BOARD_ROWS | FITTED_ROWS
            ):
                parent.remove(child)

    assert len(parts) == 12


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_of_three_consumers_closes_on_native_and_retains_quantities(
    raw, version, system_model
):
    source = deepcopy(raw)
    source.data = [activity(source.data, name) for name in NAMES]
    identities = {
        tuple(e[k] for k in ("name", "reference product", "location", "unit"))
        for d in source.data
        for e in d["exchanges"]
        if e["type"] == "technosphere" and e["name"] not in NAMES
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
    assert len(importer.import_db.data) == 3
    for name in NAMES:
        prepared = activity(importer.import_db.data, name)
        expected = deepcopy(activity(raw.data, name))
        if system_model == "consequential":
            for e in expected["exchanges"]:
                if (
                    e["name"]
                    == "copper scrap, sorted, pressed, Recycled Content cut-off"
                ):
                    e["name"] = "market for copper scrap, sorted, pressed"
        assert vector(prepared) == vector(expected)
        assert all(
            e["product"] == e["reference product"] for e in prepared["exchanges"]
        )
        assert exchange(prepared, NATIVE[0])["amount"] == PINS[name][2]


@pytest.mark.parametrize("version", ("3.9", "3.12"))
def test_lis_workbook_bytes_invalidate_import_cache_without_changing_native_cache(
    version, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    native = obj._database_cache_path("ALIGN026")
    imports = obj._database_cache_path("ALIGN026", inventories=True)
    assert module.FILEPATH_LIS_BATTERY == INVENTORY_DIR / WORKBOOK
    original = module.FILEPATH_LIS_BATTERY.read_bytes()
    copied = tmp_path / WORKBOOK
    copied.write_bytes(original)
    monkeypatch.setattr(module, "FILEPATH_LIS_BATTERY", copied)
    assert obj._database_cache_path("ALIGN026", inventories=True) == imports
    copied.write_bytes(original + b"changed native resistor purchases")
    assert obj._database_cache_path("ALIGN026", inventories=True) != imports
    assert obj._database_cache_path("ALIGN026") == native
