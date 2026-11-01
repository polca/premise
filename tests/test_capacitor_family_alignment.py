"""ALIGN-027: retain 134 fitted capacitors and the 70 g board coverage basis."""

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
BUNDLE = "capacitor assembly production, battery monitoring board, 134 capacitors"
BOARD = "printed circuit board production, battery module control, mounted"
RESIDUAL = "battery auxiliary interface and power conditioning unit production"
COG0603, COG0805 = "capacitor production, c0g, 0603", "capacitor production, c0g, 0805"
NEW_NAMES = (COG0603, COG0805)
NEW_RESIDUAL = 0.08448289937142849
OLD_COUNTS = (
    "TI70gmonitorboard has134fittedceramicparts:55Class2 0402,27Class2 0603,"
    "38Class2 0805,6Class2 1206,1flex0805,7C0G0402.2KEMETC0G parts use1.06mg "
    "each,other5use1.2mgfamilyreference."
)
NEW_COUNTS = (
    "TI70gmonitorboard has134fittedceramicparts:51Class2 0402,25Class2 0603,"
    "5Class2 0805,6Class2 1206,1flex0805,11C0G0402,2C0G0603,33C0G0805."
    "2KEMETC0G0402 parts retain the inherited1.06mg allowance each,other9use1.2mg"
    "familyreference; exact fitted-part assays remain unverified."
)
AMOUNTS = {
    "B1405": 7.242e-5,
    "B1406": 0.00015,
    "B1407": 9.49e-5,
    "B1410": 1.292e-5,
    "B1233": NEW_RESIDUAL,
}
FAMILY_AMOUNTS = {
    "capacitor production, class 2, 0402": 7.242e-5,
    "capacitor production, class 2, 0603": 0.00015,
    "capacitor production, class 2, 0805": 9.49e-5,
    "capacitor production, class 2, 1206": 0.00028518,
    "capacitor production, flexible, class 2, 0805": 1.897e-5,
    "capacitor production, c0g, 0402": 1.292e-5,
    COG0603: 9e-6,
    COG0805: 0.00033759,
}


CLIC_PINS = {
    BUNDLE: (3664, 5579),
    BOARD: (3676, 5591),
    COG0603: (3662, 5577),
    COG0805: (3663, 5578),
}
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
COMMENT_PINS = dict.fromkeys(
    (
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
    )
)
ALLOWED = set(COMMENT_PINS) | set(AMOUNTS)
ADDED_ROWS = {1411, 1412, *range(1685, 1703), *range(1705, 1723)}


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


CONNECTOR = "connector production, battery control board, plated contacts"
CONNECTOR_PARENTS = {
    "printed circuit board production, battery central control, mounted",
    BOARD,
}
CONNECTOR_CELLS = {"B1185", "B1212", "B1667", "B1668"} | {
    f"B{r}" for r in range(1673, 1681)
}


def snapshot(value):
    return json.loads(json.dumps(value, sort_keys=True))


def activity(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def scientific_vector(data):
    result = {}
    for exc in data["exchanges"]:
        key = tuple(
            exc[k] for k in ("type", "name", "reference product", "location", "unit")
        )
        assert key not in result
        result[key] = {k: v for k, v in exc.items() if k not in {"product", "comment"}}
    return result


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
            ROOT / "tests/data/inventory_review/capacitor-family-adoption-20261027.json"
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
        (
            ROOT
            / "tests/data/inventory_review/connector-reference-adoption-20261027.json"
        ).read_text()
    )
    comment = activity(raw.data, CONNECTOR)["comment"]
    old = plan["before_snapshot"]["comment"]
    assert comment.endswith(old)
    note = comment[: -len(old)]
    assert note
    return note


@pytest.fixture(scope="module")
def control_wafer_adoption():
    return json.loads(
        (
            ROOT
            / "tests/data/inventory_review/control-package-wafer-adoption-20261027.json"
        ).read_text()
    )


def restore_wafer_text(text, plan):
    for replacement in plan["metadata_replacements"]:
        text = text.replace(replacement["after"], replacement["before"])
    return text


def test_approval_pins_original_records_four_clic_references_and_source39(adoption):
    assert adoption["approval"] == "ALIGN-027; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK
    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "version": "3.9",
        "system_model": "cutoff",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert len(adoption["cases"]) == len(adoption["new_helpers"]) == 2
    for item in [*adoption["cases"], *adoption["new_helpers"]]:
        name = item["CLIC_snapshot"]["name"]
        dataset, version = CLIC_PINS[name]
        assert (item["CLIC_dataset_id"], item["CLIC_version_id"]) == (dataset, version)

    assert adoption["amount_cells"] == AMOUNTS
    assert set(adoption["metadata_replacement"]["cells"]) == set(COMMENT_PINS)
    assert (adoption["original_max_row"], adoption["new_max_row"]) == (1682, 1722)
    assert (adoption["original_dimension"], adoption["new_dimension"]) == (
        "A1:Q1682",
        "A1:Q1722",
    )


@pytest.mark.parametrize("name", (BUNDLE, BOARD))
def test_two_original_records_preserve_every_other_exchange_and_alias(
    raw, adoption, name
):
    (case,) = [c for c in adoption["cases"] if c["before_snapshot"]["name"] == name]
    expected = deepcopy(case["before_snapshot"])
    if name == BUNDLE:
        for supplier, amount in FAMILY_AMOUNTS.items():
            if supplier in NEW_NAMES:
                expected["exchanges"].append(
                    {
                        "name": supplier,
                        "type": "technosphere",
                        "amount": amount,
                        "location": "GLO",
                        "unit": "kilogram",
                        "reference product": supplier.replace(
                            " production, c0g", ", C0G"
                        ),
                        "product": supplier.replace(" production, c0g", ", C0G"),
                    }
                )
            else:
                exchange(expected, supplier)["amount"] = amount
    else:
        exchange(expected, RESIDUAL)["amount"] = NEW_RESIDUAL
    assert case["after_snapshot"] == expected
    actual = activity(raw.data, name)
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    if name == BUNDLE:
        assert len(actual["exchanges"]) == 9
        assert scientific_vector(actual) == scientific_vector(case["CLIC_snapshot"])
    else:
        assert len(actual["exchanges"]) == 19
        assert exchange(actual, BUNDLE) == exchange(case["before_snapshot"], BUNDLE)
        assert (
            exchange(actual, BUNDLE)["amount"]
            == exchange(case["CLIC_snapshot"], BUNDLE)["amount"]
        )


def test_134_part_count_and_mass_delta_preserve_70g_parent_coverage(raw, adoption):
    counts = {
        "class2_0402": 51,
        "class2_0603": 25,
        "class2_0805": 5,
        "class2_1206": 6,
        "flex_0805": 1,
        "c0g_0402": 11,
        "c0g_0603": 2,
        "c0g_0805": 33,
    }
    assert adoption["family_counts"] == counts
    assert sum(counts.values()) == adoption["part_count"] == 134
    assert adoption["misclassified_parts"] == {"0402": 4, "0603": 2, "0805": 33}
    assert sum(adoption["misclassified_parts"].values()) == 39
    bundle = activity(raw.data, BUNDLE)
    amounts = {
        e["name"]: e["amount"]
        for e in bundle["exchanges"]
        if e["type"] == "technosphere"
    }
    assert amounts == FAMILY_AMOUNTS
    expected_mg = (
        51 * 1.42,
        25 * 6,
        5 * 18.98,
        6 * 47.53,
        18.97,
        2 * 1.06 + 9 * 1.2,
        2 * 4.5,
        33 * 10.23,
    )
    for observed, mg in zip(amounts.values(), expected_mg):
        assert observed == pytest.approx(mg * 1e-6, rel=1e-14)
    old_mass, new_mass, delta = 0.00127361, sum(amounts.values()), 0.00029263
    assert adoption["before_bundle_mass_kg"] == old_mass
    assert new_mass == pytest.approx(adoption["after_bundle_mass_kg"], rel=1e-14)
    assert new_mass == pytest.approx(0.00098098, rel=1e-14)
    assert old_mass - new_mass == pytest.approx(delta, rel=1e-14)
    assert adoption["additional_unresolved_kg_per_board"] == delta
    board = activity(raw.data, BOARD)
    assert adoption["board_mass_kg"] == 0.07
    assert exchange(board, BUNDLE)["amount"] * 0.07 == pytest.approx(1, rel=1e-14)
    old_residual = adoption["old_residual_kg_per_kg_board"]
    assert old_residual == 0.08030247079999991
    new_residual = exchange(board, RESIDUAL)["amount"]
    assert new_residual == adoption["new_residual_kg_per_kg_board"] == NEW_RESIDUAL
    assert (new_residual - old_residual) * 0.07 == pytest.approx(delta, rel=1e-14)
    assert new_mass + new_residual * 0.07 == pytest.approx(
        old_mass + old_residual * 0.07, rel=1e-14
    )


@pytest.mark.parametrize("name", NEW_NAMES)
def test_new_helper_full_science_and_unrenormalized_95_percent_known_materials(
    raw, adoption, name
):
    (helper,) = [h for h in adoption["new_helpers"] if h["snapshot"]["name"] == name]
    actual, snapshot = activity(raw.data, name), helper["snapshot"]
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in snapshot.items() if k != "comment"
    }
    assert len(actual["exchanges"]) == 8
    assert scientific_vector(actual) == scientific_vector(helper["CLIC_snapshot"])
    assert all(e["product"] == e["reference product"] for e in actual["exchanges"])
    assert actual["production amount"] == exchange(actual, name)["amount"] == 1
    assert (actual["unit"], actual["location"]) == ("kilogram", "GLO")
    feed = sum(
        e["amount"]
        for e in actual["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "kilogram" and e["amount"] > 0
    )
    reject = -exchange(actual, "market for hazardous waste, for incineration")["amount"]
    coverage = {COG0603: 0.8777777777777778, COG0805: 0.8826979472140764}[name]
    assert feed * 0.95 == pytest.approx(coverage, rel=1e-14)
    assert reject == pytest.approx(feed * 0.05, rel=1e-14)
    assert feed - reject == pytest.approx(coverage, rel=1e-14)
    assert coverage < 1  # Unknown retained ingredients are not silently filled.
    exchange(actual, "calcium zirconate production, for capacitors")
    assert (
        exchange(actual, "market group for electricity, medium voltage")["amount"] == 1
    )
    assert (
        exchange(actual, "market for heat, district or industrial, natural gas")[
            "amount"
        ]
        == 5
    )
    assert actual["comment"].endswith(snapshot["comment"])
    for term in (
        "unknown additives",
        "remain unverified",
        "cross-manufacturer",
        "95% known-feed retention",
        "mixed-waste",
    ):
        assert term in actual["comment"]


def test_exact_eleven_count_note_substitutions_and_two_authoritative_prepends(
    adoption, control_wafer_adoption, connector_note
):
    replacement = adoption["metadata_replacement"]
    assert replacement["before"] == OLD_COUNTS
    assert replacement["after"] == NEW_COUNTS
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    cases = {c["comment_cell"]: c for c in adoption["cases"]}
    for address, pin in COMMENT_PINS.items():
        cell = historical_board_cell(cells, SHEET, address)
        text = cell.find(f"{NS}is/{NS}t")
        if address in {"B1185", "B1212"}:
            assert text.text.startswith(connector_note)
            text.text = text.text[len(connector_note) :]
        text.text = restore_wafer_text(text.text, control_wafer_adoption)
        assert text.text.count(replacement["after"]) == 1
        assert replacement["before"] not in text.text
        if address in cases:
            original = cases[address]["before_snapshot"]["comment"]
            history = original.replace(replacement["before"], replacement["after"])
            assert text.text.endswith(history)
            prefix = text.text[: -len(history)]
            assert prefix.startswith("APPROVED 39-PART CAPACITOR FAMILY CORRECTION")
            for term in (
                "134 capacitors",
                "70 g",
                "0.00029263",
                "not additional",
                "engineering proxies",
                "not renormalized",
                "3.9",
                "supersedes",
            ):
                assert term in prefix
            text.text = original
        else:
            text.text = text.text.replace(replacement["after"], replacement["before"])


def test_all_72_other_retained_records_have_only_exact_metadata_substitutions(
    raw, adoption, control_wafer_adoption, connector_note
):
    others = deepcopy(
        [
            d
            for d in raw.data
            if d["name"] not in {BUNDLE, BOARD, *NEW_NAMES, CONNECTOR} | WAFER_NAMES
        ]
    )
    assert len(raw.data) == 81 and len(others) == 72
    replacement = adoption["metadata_replacement"]
    for record in others:
        if isinstance(record.get("comment"), str):
            if record["name"] in CONNECTOR_PARENTS:
                assert record["comment"].startswith(connector_note)
                record["comment"] = record["comment"][len(connector_note) :]
            record["comment"] = restore_wafer_text(
                record["comment"], control_wafer_adoption
            )
            record["comment"] = record["comment"].replace(
                replacement["after"], replacement["before"]
            )


def test_xml_preserves_all_original_cells_outside_sixteen_exemptions_and_127_caches():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas, added = [], [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        if sheet == SHEET and row in ADDED_ROWS:
            added.append(address)
        elif not (
            sheet == SHEET
            and (
                address in ALLOWED | WAFER_CELLS | CONNECTOR_CELLS
                or row in {1681, 1682} | OLD_BOARD_ROWS | FITTED_ROWS
            )
        ):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert (
                cell.find(NS + "v") is not None and cell.find(NS + "v").text is not None
            )
            formulas.append([sheet, address, payload])
    assert len(added) == 178 and len(outside) == 6402

    assert formulas and all(payload for _, _, payload in formulas)
    for address, value in AMOUNTS.items():
        cell = historical_board_cell(cells, SHEET, address)
        assert cell.attrib == {"r": address}
        assert [c.tag for c in cell] == [NS + "v"]
        assert cell.find(NS + "v").text == str(value)


def test_new_rows_and_dimension_preserve_every_other_worksheet_element_and_zip_part(
    adoption,
):
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_interface_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        )
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    assert root.find(NS + "dimension").get("ref") == "A1:Q1864"
    root.find(NS + "dimension").set("ref", "A1:Q1682")
    rows = {int(r.get("r")): r for r in root.iter(NS + "row")}
    assert 1413 in rows  # Next existing activity directly follows both inputs.
    assert max(rows) == 1864
    assert {(h["start_row"], h["end_row"]) for h in adoption["new_helpers"]} == {
        (1685, 1702),
        (1705, 1722),
    }
    for parent in root.iter():
        for child in list(parent):
            if (
                child.tag == NS + "row"
                and int(child.get("r")) in ADDED_ROWS | OLD_BOARD_ROWS | FITTED_ROWS
            ):
                parent.remove(child)
            elif (
                child.tag == NS + "c"
                and child.get("r") in ALLOWED | WAFER_CELLS | CONNECTOR_CELLS
            ):
                parent.remove(child)
            elif child.tag == NS + "row" and child.get("r") in {"1681", "1682"}:
                parent.remove(child)

    assert len(parts) == 12


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_closes_bundle_and_two_helpers_with_original39_profile(
    raw, version, system_model
):
    selected = (BUNDLE, *NEW_NAMES)
    source = deepcopy(raw)
    source.data = [activity(source.data, name) for name in selected]
    identities = {
        tuple(e[k] for k in ("name", "reference product", "location", "unit"))
        for d in source.data
        for e in d["exchanges"]
        if e["type"] == "technosphere" and e["name"] not in selected
    }
    background = [
        dict(zip(("name", "reference product", "location", "unit"), key), exchanges=[])
        for key in sorted(identities)
    ]
    saved = deepcopy(background)
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
    assert importer.list_unlinked == [] and background == saved
    assert len(importer.import_db.data) == 3
    for name in selected:
        actual = activity(importer.import_db.data, name)
        # Preparation legitimately adds link keys; all physical fields persist.
        fields = ("type", "name", "reference product", "location", "unit", "amount")
        assert [tuple(e[k] for k in fields) for e in actual["exchanges"]] == [
            tuple(e[k] for k in fields) for e in activity(raw.data, name)["exchanges"]
        ]
        assert all(e["product"] == e["reference product"] for e in actual["exchanges"])
