"""ALIGN-048: sixteen 70 g monitors and paired local retirement."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from inventory_assertions import (
    assert_review_cells,
    formula_caches,
    records_match_review,
    review_cells_match,
)
from test_sixteen_cooling_alignment import previous_cooling_source, previous_cooling_xml

from premise.filesystem_constants import INVENTORY_DIR

ROOT = Path(__file__).resolve().parents[1]
LIS = "lci-batteries-LiS.xlsx"
CAPACITY = "lci-battery-capacity.xlsx"
WORKBOOKS = (LIS, CAPACITY)
WRAPPER = "battery control and cooling system production, 16 modules"
BOARD = "printed circuit board production, battery module control, mounted"
CENTRAL = "battery central control and protection unit production"
COOLING = "battery cooling system production, liquid cooling"
USED = "market for used Li-ion battery"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


PINS = {
    "lci-batteries-LiS.xlsx": {"parts_count": 12},
    "lci-battery-capacity.xlsx": {"parts_count": 15},
}


def reference():
    payload = (
        ROOT / "scripts/data/sixteen-monitor-adoption-20261027.json"
    ).read_bytes()

    return json.loads(payload)


def plan(filename):
    (w,) = [w for w in reference()["workbooks"] if w["workbook"] == filename]
    return w


def activity(data, name):
    (r,) = [r for r in data if r["name"] == name]
    return r


def exchange(record, name):
    (e,) = [e for e in record["exchanges"] if e["name"] == name]
    return e


def validate_current_monitors(source, filename):
    w = plan(filename)
    PINS[filename]
    assert len(source.data) == w["activity_count"]

    expected = {a["index"]: deepcopy(a["before"]) for a in w["affected_activities"]}
    for c in w["changes"]:
        r = expected[c["activity_index"]]
        e = r["exchanges"][c["exchange_index"]]
        assert e == c["before_exchange"] and e["amount"] == c["before"]
        assert e.get("uncertainty type", 0) == 0
        if e["name"] == BOARD:
            assert e["amount"] == 1.1 and c["after"] == 16 * 0.07
        else:
            assert e["name"] == USED and e["amount"] == -15.5924
            assert c["after"] == -15.6124
            assert -c["after"] == pytest.approx(4.4928 + 16 * 0.07 + 9.9996, rel=1e-15)
        e["amount"] = c["after"]
    for note in w["notes"]:
        assert note["prefix"].startswith("APPROVED SIXTEEN-MODULE MONITORING REFERENCE")
        if note["target"] == "worksheet_note":
            assert filename == CAPACITY and w["affected_activities"] == []
            e = exchange(source.data[note["activity_index"]], WRAPPER)
            assert not any(k.lower() == "comment" for k in e)
        else:
            assert note["target"] == "activity_comment"
            r = expected[note["activity_index"]]
            assert r["comment"] == note["before"]
            r["comment"] = note["prefix"] + note["before"]
    history = list(source.data)
    for a in w["affected_activities"]:
        assert expected[a["index"]] == a["after"] == source.data[a["index"]]
        history[a["index"]] = deepcopy(a["before"])

    return w, history


def xml_scope(root, w):
    addresses = {c["cell"] for c in w["changes"]} | {n["cell"] for n in w["notes"]}
    return [
        [a, ET.tostring(root.find(f".//{NS}c[@r='{a}']"), encoding="unicode")]
        for a in sorted(addresses)
    ]


def validate_live_current(filename):
    # Check actual source science AND actual worksheet-only notes before any
    # caller can use a historical view. A pre-normalized fixture is no bypass.
    w, _ = validate_current_monitors(
        previous_cooling_source(ExcelImporter(INVENTORY_DIR / filename), filename),
        filename,
    )
    with ZipFile(INVENTORY_DIR / filename) as archive:
        root = previous_cooling_xml(ET.fromstring(archive.read(SHEET)), filename)
    assert_review_cells(root, w, "after", INVENTORY_DIR / filename)
    return w


def previous_monitor_source(source, filename):
    if filename not in WORKBOOKS:
        return source
    validate_live_current(filename)
    if plan(filename)["affected_activities"] and records_match_review(
        source.data, plan(filename)
    ):
        return source  # Authenticated prior view; do not reverse it a second time.
    source = previous_cooling_source(source, filename)
    _, history = validate_current_monitors(source, filename)
    source.data = history
    return source


def previous_monitor_xml(root, filename, sheet=SHEET):
    if filename not in WORKBOOKS or sheet != SHEET:
        return root
    w = validate_live_current(filename)
    if review_cells_match(root, plan(filename), "before", INVENTORY_DIR / filename):
        return root  # Live source checked above; this old view is already exact.
    root = previous_cooling_xml(root, filename, sheet)
    assert_review_cells(root, w, "after", INVENTORY_DIR / filename)
    for c in w["changes"]:
        root.find(f".//{NS}c[@r='{c['cell']}']/{NS}v").text = str(c["before"])
    for note in w["notes"]:
        root.find(f".//{NS}c[@r='{note['cell']}']/{NS}is/{NS}t").text = note["before"]
    assert_review_cells(root, w, "before", INVENTORY_DIR / filename)
    return root


@pytest.fixture(scope="module")
def current():
    return {
        name: previous_cooling_source(ExcelImporter(INVENTORY_DIR / name), name)
        for name in WORKBOOKS
    }


def test_approval_original_profiles_and_clic_provenance_keep_cooling_alignment_separate():
    p = reference()
    assert (
        p["case_id"] == "ALIGN-048"
        and p["amount_cell_count"] == 2
        and p["note_cell_count"] == 3
    )

    assert p["CLIC_reference"]["background"]["version"] == "3.12"
    assert (
        p["cooling_model_alignment_separate"] is True
        and p["full_graph_CLIC_equivalence_claimed"] is False
    )
    for filename, version in ((LIS, "3.9"), (CAPACITY, "3.10")):
        w = plan(filename)
        assert (
            w["source_background"]["version"]
            == w["source_biosphere"]["version"]
            == version
        )
        assert w["formula_reference_audit"]["dependent_formula_cells"] == []


def test_sixteen70g_boards_pair_with_actual_local_cooling_and_one_central_retirement(
    current,
):
    p = reference()
    source = current[LIS].data
    r = activity(source, WRAPPER)
    assert p["monitor_boards"] == 16 and p["board_mass_kg"] == 0.07
    assert exchange(r, BOARD)["amount"] == 16 * 0.07 == 1.12
    assert exchange(p["CLIC_reference"]["snapshot"], BOARD)["amount"] == 1.12
    assert exchange(r, CENTRAL)["amount"] == exchange(r, COOLING)["amount"] == 1
    central_mass = sum(
        e["amount"]
        for e in activity(source, CENTRAL)["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "kilogram"
    )
    cooling = [
        e
        for e in activity(source, COOLING)["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "kilogram"
        and "metal working" not in e["name"]
        and "injection moulding" not in e["name"]
    ]
    assert central_mass == pytest.approx(4.4928, rel=1e-15)
    assert len(cooling) == 8 and sum(e["amount"] for e in cooling) == pytest.approx(
        9.9996, rel=1e-15
    )
    assert exchange(r, USED)["amount"] == -15.6124
    assert -exchange(r, USED)["amount"] == pytest.approx(
        central_mass + 1.12 + sum(e["amount"] for e in cooling), rel=1e-15
    )
    assert exchange(r, USED)["amount"] != -15.6128
    old = plan(LIS)["affected_activities"][0]["before"]
    assert exchange(old, BOARD)["amount"] == 1.1
    assert exchange(r, BOARD)["amount"] - exchange(old, BOARD)[
        "amount"
    ] == pytest.approx(0.02, abs=1e-15)
    assert exchange(old, USED)["amount"] - exchange(r, USED)["amount"] == pytest.approx(
        0.02, abs=1e-15
    )


def test_note_distinguishes_reported1_1kg_basis_from_adopted_engineering_reference(
    current,
):
    note = plan(LIS)["notes"][0]
    actual = activity(current[LIS].data, WRAPPER)["comment"]
    assert actual == note["prefix"] + note["before"]
    for term in (
        "source 1.1 kg allowance remains a distinct reported mass reference, not a proven error",
        "unverified engineering assumptions",
        "does not certify a complete fitted BOM",
        "different 15.6128 kg CLIC",
        "capacity purchases, module mass, lifetime",
    ):
        assert term in note["prefix"]


def test_capacity_notes_remain_outside_parsed_exchanges_with_fixed_purchases(current):
    w = plan(CAPACITY)
    assert w["affected_activities"] == [] and w["changes"] == []

    for note, amount in zip(w["notes"], (0.0215820670357649, 0.02121105988781555)):
        assert note["target"] == "worksheet_note"
        e = exchange(current[CAPACITY].data[note["activity_index"]], WRAPPER)
        assert e["amount"] == amount and e["unit"] == "unit"
        assert not any(k.lower() == "comment" for k in e)


@pytest.mark.parametrize("filename", WORKBOOKS)
def test_current_raw_and_exact_bounded_historical_view_preserve_all_other_records(
    current, filename
):
    w, history = validate_current_monitors(current[filename], filename)

    restored = previous_monitor_source(deepcopy(current[filename]), filename)
    assert restored.data == history
    assert len(w["affected_activities"]) == (1 if filename == LIS else 0)


@pytest.mark.parametrize("filename", WORKBOOKS)
def test_exact_XML_scope_all_formula_caches_and_capacity_second_sheet_are_preserved(
    filename,
):
    w = plan(filename)
    pin = PINS[filename]
    with ZipFile(INVENTORY_DIR / filename) as archive:
        root = previous_cooling_xml(ET.fromstring(archive.read(SHEET)), filename)
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    assert root.find(NS + "dimension").get("ref") == w["dimension"]
    assert len(parts) == pin["parts_count"]
    assert formula_caches(root)
    for n in w["notes"]:
        assert (
            root.find(f".//{NS}c[@r='{n['cell']}']/{NS}is/{NS}t").text
            == n["prefix"] + n["before"]
        )
    if filename == CAPACITY:
        for a, value in (("B132", 0.0215820670357649), ("B158", 0.02121105988781555)):
            assert float(root.find(f".//{NS}c[@r='{a}']/{NS}v").text) == value
    previous_monitor_xml(root, filename)


def test_already_historical_views_are_authenticated_without_repeating_reversals(
    current,
):
    source = previous_monitor_source(deepcopy(current[LIS]), LIS)
    history = deepcopy(source.data)
    assert previous_monitor_source(source, LIS).data == history
    with ZipFile(INVENTORY_DIR / LIS) as archive:
        root = previous_cooling_xml(ET.fromstring(archive.read(SHEET)), LIS)
    restored = previous_monitor_xml(root, LIS)
    payload = ET.tostring(restored)
    assert ET.tostring(previous_monitor_xml(restored, LIS)) == payload


def test_guard_rejects_unapproved_amount_note_or_new_capacity_comment_field(current):
    for filename, problem in ((LIS, "amount"), (LIS, "note"), (CAPACITY, "comment")):
        damaged = deepcopy(current[filename])
        if problem == "amount":
            exchange(activity(damaged.data, WRAPPER), BOARD)["amount"] += 0.001
        elif problem == "note":
            activity(damaged.data, WRAPPER)["comment"] += "Bad note"
        else:
            exchange(damaged.data[9], WRAPPER)["comment"] = "Unexpected imported field"
        untouched = deepcopy(damaged.data)
        with pytest.raises(AssertionError):
            previous_monitor_source(damaged, filename)
        assert damaged.data == untouched
    with ZipFile(INVENTORY_DIR / CAPACITY) as archive:
        root = previous_cooling_xml(ET.fromstring(archive.read(SHEET)), CAPACITY)
    root.find(f".//{NS}c[@r='L132']/{NS}is/{NS}t").text = "Bad worksheet note"
    with pytest.raises(AssertionError):
        previous_monitor_xml(root, CAPACITY)
