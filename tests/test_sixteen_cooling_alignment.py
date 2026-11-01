"""ALIGN-050: nominal RER cooling and guarded exact pre-adoption views."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from delegated_source_history import previous_product_source, previous_product_xml
from inventory_assertions import (
    assert_review_cells,
    formula_caches,
    records_match_review,
    review_cells_match,
)

from premise.filesystem_constants import INVENTORY_DIR

ROOT = Path(__file__).resolve().parents[1]
LIS = "lci-batteries-LiS.xlsx"
CAPACITY = "lci-battery-capacity.xlsx"
LIO2 = "lci-batteries-LiO2.xlsx"
WORKBOOKS = (LIS, CAPACITY)
WRAPPER = "battery control and cooling system production, 16 modules"
COOLING = "cooling system production, for Li-O2 battery"
OLD_COOLING = "battery cooling system production, liquid cooling"
CENTRAL = "battery central control and protection unit production"
BOARD = "printed circuit board production, battery module control, mounted"
USED = "market for used Li-ion battery"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"

PINS = {
    "lci-batteries-LiS.xlsx": {"parts_count": 12},
    "lci-battery-capacity.xlsx": {"parts_count": 15},
}
ORIGINAL_CELLS = {
    "lci-batteries-LiS.xlsx": {
        "A1133": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="A1133" t="inlineStr"><ns0:is><ns0:t xml:space="preserve">battery cooling system production, liquid cooling</ns0:t></ns0:is></ns0:c>',
        "B1133": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="B1133"><ns0:v>1</ns0:v></ns0:c>',
        "B1134": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="B1134"><ns0:v>-15.6124</ns0:v></ns0:c>',
        "C1133": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="C1133" t="inlineStr"><ns0:is><ns0:t>GLO</ns0:t></ns0:is></ns0:c>',
        "D1133": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="D1133" t="inlineStr"><ns0:is><ns0:t>unit</ns0:t></ns0:is></ns0:c>',
        "G1133": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="G1133" t="inlineStr"><ns0:is><ns0:t xml:space="preserve">battery cooling system, liquid cooling</ns0:t></ns0:is></ns0:c>',
    },
    "lci-battery-capacity.xlsx": {},
}


def reference():
    payload = (
        ROOT / "tests/data/inventory_review/sixteen-cooling-adoption-20261027.json"
    ).read_bytes()

    return json.loads(payload)


def plan(filename):
    return next(w for w in reference()["workbooks"] if w["workbook"] == filename)


def activity(data, name):
    (r,) = [r for r in data if r["name"] == name]
    return r


def exchange(record, name):
    (e,) = [e for e in record["exchanges"] if e["name"] == name]
    return e


def science(record):
    return sorted(
        tuple(
            e[k]
            for k in ("type", "name", "reference product", "location", "unit", "amount")
        )
        for e in record["exchanges"]
    )


def validate_current_cooling(source, filename):
    if filename == LIS:
        source = previous_product_source(source, INVENTORY_DIR / filename)
    w = plan(filename)
    assert len(source.data) == w["activity_count"]

    history = list(source.data)
    for a in w["affected_activities"]:
        before = a["before"]
        expected = deepcopy(before)
        cooling = exchange(expected, OLD_COOLING)
        assert (cooling["amount"], cooling["location"], cooling["unit"]) == (
            1,
            "GLO",
            "unit",
        )
        cooling.update(
            name=COOLING,
            amount=10,
            location="RER",
            unit="kilogram",
            **{"reference product": "cooling system"},
        )
        assert exchange(expected, USED)["amount"] == -15.6124
        exchange(expected, USED)["amount"] = -15.6128
        (note,) = w["notes"]
        assert (
            note["target"] == "activity_comment"
            and note["before"] == expected["comment"]
        )
        assert note["prefix"].startswith(
            "APPROVED SIXTEEN-MODULE RER COOLING REFERENCE"
        )
        expected["comment"] = note["prefix"] + note["before"]
        assert source.data[a["index"]] == expected == a["after"]
        history[a["index"]] = deepcopy(before)

    return w, history


def xml_scope(root, w):
    addresses = {c["cell"] for c in w["changes"]} | {n["cell"] for n in w["notes"]}
    return [
        [a, ET.tostring(root.find(f".//{NS}c[@r='{a}']"), encoding="unicode")]
        for a in sorted(addresses)
    ]


def validate_live_current(filename):
    w, _ = validate_current_cooling(ExcelImporter(INVENTORY_DIR / filename), filename)
    with ZipFile(INVENTORY_DIR / filename) as archive:
        root = ET.fromstring(archive.read(SHEET))
    assert_review_cells(root, w, "after", INVENTORY_DIR / filename)
    return w


def previous_cooling_source(source, filename):
    if filename not in WORKBOOKS:
        return source
    validate_live_current(filename)
    if records_match_review(source.data, plan(filename)):
        return source
    _, history = validate_current_cooling(source, filename)
    source.data = history
    return source


def previous_cooling_xml(root, filename, sheet=SHEET):
    if filename not in WORKBOOKS or sheet != SHEET:
        return root
    if filename == LIS:
        root = previous_product_xml(root, INVENTORY_DIR / filename, sheet=sheet)
    w = validate_live_current(filename)
    if review_cells_match(root, plan(filename), "before", INVENTORY_DIR / filename):
        return root
    assert_review_cells(root, w, "after", INVENTORY_DIR / filename)
    for address, payload in ORIGINAL_CELLS[filename].items():
        parent = root.find(
            f".//{NS}row[@r='{''.join(c for c in address if c.isdigit())}']"
        )
        cell = parent.find(f"{NS}c[@r='{address}']")
        index = list(parent).index(cell)
        parent.remove(cell)
        parent.insert(index, ET.fromstring(payload))
    for note in w["notes"]:
        root.find(f".//{NS}c[@r='{note['cell']}']/{NS}is/{NS}t").text = note["before"]
    assert_review_cells(root, w, "before", INVENTORY_DIR / filename)
    return root


@pytest.fixture(scope="module")
def current():
    return {name: ExcelImporter(INVENTORY_DIR / name) for name in WORKBOOKS}


def test_original_paired_profiles_approval_and_exact_authorized_scope():
    p = reference()
    assert (
        p["case_id"] == "ALIGN-050"
        and p["change_cell_count"] == 6
        and p["note_cell_count"] == 3
    )
    assert p["full_graph_CLIC_equivalence_claimed"] is False
    assert {c["cell"] for c in plan(LIS)["changes"]} == {
        "A1133",
        "B1133",
        "C1133",
        "D1133",
        "G1133",
        "B1134",
    }
    assert [n["cell"] for n in plan(LIS)["notes"]] == ["B1126"]
    assert [n["cell"] for n in plan(CAPACITY)["notes"]] == ["L132", "L158"]
    for filename, version in ((LIS, "3.9"), (CAPACITY, "3.10")):
        w = plan(filename)
        assert (
            w["source_background"]["version"]
            == w["source_biosphere"]["version"]
            == version
        )
        assert w["formula_reference_audit"]["dependent_formula_cells"] == []
    assert (
        p["approved_proposal"]["source_profiles_preserved"][LIO2]["background"][
            "version"
        ]
        == "3.9"
    )


def test_nominal_cooling_and_sixteen_boards_close_retirement_without_filling_rounding_gap(
    current,
):
    r = activity(current[LIS].data, WRAPPER)
    assert exchange(r, CENTRAL)["amount"] == 1
    assert exchange(r, BOARD)["amount"] == 16 * 0.07 == 1.12
    e = exchange(r, COOLING)
    assert (e["amount"], e["location"], e["unit"], e["reference product"]) == (
        10,
        "RER",
        "kilogram",
        "cooling system",
    )
    assert exchange(r, USED)["amount"] == -15.6128
    assert -exchange(r, USED)["amount"] == 4.4928 + 1.12 + 10
    assert len(r["exchanges"]) == 5
    old = activity(current[LIS].data, OLD_COOLING)
    assert old["location"] == "GLO" and old["unit"] == "unit"
    supplier = activity(ExcelImporter(INVENTORY_DIR / LIO2).data, COOLING)
    materials = [
        e
        for e in supplier["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "kilogram"
    ]
    assert len(materials) == 6
    listed = 10 * sum(e["amount"] for e in materials)
    assert listed == pytest.approx(9.9996, rel=1e-15)
    assert 10 - listed == pytest.approx(0.0004, abs=2e-15)


def test_direct_CLIC_science_and_corrected_stock_children_are_reused(current):
    old = json.loads(
        (
            ROOT / "tests/data/inventory_review/sixteen-monitor-adoption-20261027.json"
        ).read_text()
    )
    clic = old["CLIC_reference"]["snapshot"]

    assert science(activity(current[LIS].data, WRAPPER)) == science(clic)
    cooling = json.loads(
        (
            ROOT / "tests/data/inventory_review/lio2-cooling-adoption-20261027.json"
        ).read_text()
    )
    supplier = cooling["CLIC_supplier_snapshot"]

    data = ExcelImporter(INVENTORY_DIR / LIO2).data
    assert science(activity(data, COOLING)) == science(supplier)
    stock = json.loads(
        (
            ROOT
            / "tests/data/inventory_review/cooling-aluminium-stock-adoption-20261027.json"
        ).read_text()
    )

    for candidate in stock["candidate_revisions"]:
        child = activity(data, candidate["candidate_snapshot"]["name"])
        assert science(child) == science(candidate["candidate_snapshot"])
        assert exchange(child, "market for aluminium, wrought alloy")["amount"] == 1
        assert not any(
            e["name"] == "market for aluminium, in mixed metal scrap"
            for e in child["exchanges"]
        )
        assert science(child) != science(candidate["base_snapshot"])


def test_capacity_annotations_stay_outside_imported_exchanges_and_qualify_proxy(
    current,
):
    w = plan(CAPACITY)
    assert w["affected_activities"] == w["changes"] == []

    for note, amount in zip(w["notes"], (0.0215820670357649, 0.02121105988781555)):
        e = exchange(current[CAPACITY].data[note["activity_index"]], WRAPPER)
        assert e["amount"] == amount and e["unit"] == "unit"
        assert not any(k.lower() == "comment" for k in e)
    note = plan(LIS)["notes"][0]
    assert (
        activity(current[LIS].data, WRAPPER)["comment"]
        == note["prefix"] + note["before"]
    )
    for term in (
        "0.0004 kg",
        "without filler, yield correction or recovery credit",
        "not measured equipment performance",
        "full pad/pipe injection-service",
        "stock grade and procurement",
    ):
        assert term in note["prefix"]


@pytest.mark.parametrize("filename", WORKBOOKS)
def test_current_full_raw_and_historical_inversion_preserve119_other_records(
    current, filename
):
    w, history = validate_current_cooling(current[filename], filename)
    assert len(w["affected_activities"]) == (1 if filename == LIS else 0)

    restored = previous_cooling_source(deepcopy(current[filename]), filename)
    assert restored.data == history
    assert previous_cooling_source(restored, filename).data == history


@pytest.mark.parametrize("filename", WORKBOOKS)
def test_exact_nine_cell_XML_scope286_formulas_caches_and_all_other_ZIP_parts(filename):
    w = plan(filename)
    with ZipFile(INVENTORY_DIR / filename) as archive:
        root = ET.fromstring(archive.read(SHEET))
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    assert root.find(NS + "dimension").get("ref") == w["dimension"]
    assert len(parts) == PINS[filename]["parts_count"]
    assert formula_caches(root)

    assert_review_cells(root, w, "after", INVENTORY_DIR / filename)
    restored = previous_cooling_xml(root, filename)

    payload = ET.tostring(restored)
    assert ET.tostring(previous_cooling_xml(restored, filename)) == payload


def test_guard_rejects_unapproved_quantity_identity_note_uncertainty_and_XML(current):
    for field, value in (
        ("amount", 9.9996),
        ("location", "GLO"),
        ("uncertainty type", 2),
    ):
        bad = deepcopy(current[LIS])
        exchange(activity(bad.data, WRAPPER), COOLING)[field] = value
        unchanged = deepcopy(bad.data)
        with pytest.raises(AssertionError):
            previous_cooling_source(bad, LIS)
        assert bad.data == unchanged
    bad = deepcopy(current[LIS])
    activity(bad.data, WRAPPER)["comment"] += "unexpected"
    with pytest.raises(AssertionError):
        previous_cooling_source(bad, LIS)
    for filename, address in ((LIS, "B1133"), (CAPACITY, "L132")):
        with ZipFile(INVENTORY_DIR / filename) as archive:
            root = ET.fromstring(archive.read(SHEET))
        cell = root.find(f".//{NS}c[@r='{address}']")
        if cell.find(NS + "v") is not None:
            cell.find(NS + "v").text = "999"
        else:
            cell.find(f"{NS}is/{NS}t").text = "wrong scientific note"
        unchanged = ET.tostring(root)
        with pytest.raises(AssertionError):
            previous_cooling_xml(root, filename)
        assert ET.tostring(root) == unchanged
