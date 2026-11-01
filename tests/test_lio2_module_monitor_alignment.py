"""ALIGN-051: preserve module-monitor mass while selecting the reviewed board."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest
from delegated_source_history import (
    reviewed_excel_importer as ExcelImporter,
)
from delegated_source_history import (
    reviewed_zipfile as ZipFile,
)
from inventory_assertions import (
    assert_review_cells,
    records_match_review,
    review_cells_match,
)

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiO2.xlsx"
LIS = "lci-batteries-LiS.xlsx"
MODULE = "module packaging, for Li-O2 battery"
BOARD = "printed circuit board production, battery module control, mounted"
PRODUCT = "printed circuit board, battery module control, mounted"
OLD_BOARD = (
    "market for printed wiring board, through-hole mounted, unspecified, Pb free"
)
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"

ORIGINAL_CELLS = {
    "A220": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="A220" t="s"><ns0:v>168</ns0:v></ns0:c>',
    "G220": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="G220" t="s"><ns0:v>167</ns0:v></ns0:c>',
}


def reference():
    payload = (
        ROOT / "tests/data/inventory_review/lio2-module-monitor-adoption-20261027.json"
    ).read_bytes()

    return json.loads(payload)


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


def validate_current_monitor(source):
    p = reference()
    (a,) = p["affected_activities"]
    assert a["index"] == 11 and len(source.data) == 34

    expected = deepcopy(a["before"])
    e = exchange(expected, OLD_BOARD)
    assert (e["amount"], e["unit"], e["location"]) == (
        0.03434136340338288,
        "kilogram",
        "GLO",
    )
    e.update(name=BOARD, **{"reference product": PRODUCT})
    (note,) = p["notes"]
    assert note["cell"] == "B206" and expected["comment"] == note["before"]
    assert note["prefix"].startswith("APPROVED Li-O2 MODULE-MONITOR SUPPLIER ALIGNMENT")
    expected["comment"] = note["prefix"] + note["before"]
    assert source.data[11] == expected == a["after"]
    history = list(source.data)
    history[11] = deepcopy(a["before"])

    return history


def xml_scope(root):
    return [
        [a, ET.tostring(root.find(f".//{NS}c[@r='{a}']"), encoding="unicode")]
        for a in ("A220", "B206", "G220")
    ]


def validate_live_current():
    validate_current_monitor(ExcelImporter(INVENTORY_DIR / WORKBOOK))
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
    assert_review_cells(root, reference(), "after", INVENTORY_DIR / WORKBOOK)


def previous_module_monitor_source(source, filename=WORKBOOK):
    if filename != WORKBOOK:
        return source
    validate_live_current()
    if records_match_review(source.data, reference()):
        return source
    source.data = validate_current_monitor(source)
    return source


def previous_module_monitor_xml(root, filename=WORKBOOK, sheet=SHEET):
    if filename != WORKBOOK or sheet != SHEET:
        return root
    validate_live_current()
    if review_cells_match(root, reference(), "before", INVENTORY_DIR / WORKBOOK):
        return root
    assert_review_cells(root, reference(), "after", INVENTORY_DIR / WORKBOOK)
    for address, payload in ORIGINAL_CELLS.items():
        row = root.find(f".//{NS}row[@r='220']")
        cell = row.find(f"{NS}c[@r='{address}']")
        index = list(row).index(cell)
        row.remove(cell)
        row.insert(index, ET.fromstring(payload))
    root.find(f".//{NS}c[@r='B206']/{NS}is/{NS}t").text = reference()["notes"][0][
        "before"
    ]
    assert_review_cells(root, reference(), "before", INVENTORY_DIR / WORKBOOK)
    return root


@pytest.fixture(scope="module")
def current():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


def test_approval_profiles_and_independent_published_provenance():
    p = reference()
    assert p["case_id"] == "ALIGN-051"
    assert (
        p["source_background"]["version"] == p["source_biosphere"]["version"] == "3.9"
    )

    assert {c["cell"] for c in p["changes"]} == {"A220", "G220"}
    assert p["all_amounts_and_other_supplier_recipes_preserved"] is True
    assert p["full_module_recipe_equivalence_claimed"] is False
    for key, version in (
        (
            "CLIC_parent_reference",
            "3.9",
        ),
        (
            "CLIC_supplier_reference",
            "3.12",
        ),
    ):
        r = p[key]

        assert r["background"]["version"] == r["biosphere"]["version"] == version


def test_module_monitor_is_one_mass_based_supplier_at_retained067_per1951(current):
    p = reference()
    r = activity(current.data, MODULE)
    e = exchange(r, BOARD)
    assert e["amount"] == 0.03434136340338288 == pytest.approx(0.67 / 19.51, rel=1e-15)
    assert (e["reference product"], e["location"], e["unit"]) == (
        PRODUCT,
        "GLO",
        "kilogram",
    )
    assert e["type"] == "technosphere"
    assert not any(x["name"] == OLD_BOARD for x in r["exchanges"])
    assert e["amount"] / 0.07 == pytest.approx(0.67 / 19.51 / 0.07, rel=1e-15)
    target = exchange(p["CLIC_parent_reference"]["snapshot"], BOARD)
    assert tuple(
        e[k] for k in ("name", "reference product", "location", "unit", "amount")
    ) == tuple(
        target[k] for k in ("name", "reference product", "location", "unit", "amount")
    )
    old = p["affected_activities"][0]["before"]
    assert len(r["exchanges"]) == len(old["exchanges"])
    assert [x["amount"] for x in r["exchanges"]] == [
        x["amount"] for x in old["exchanges"]
    ]
    assert r["production amount"] == old["production amount"] == 1


def test_existing_reviewed_supplier_recipe_matches_CLIC_without_new_board_or_profile():
    supplier = activity(ExcelImporter(INVENTORY_DIR / LIS).data, BOARD)
    assert (supplier["reference product"], supplier["location"], supplier["unit"]) == (
        PRODUCT,
        "GLO",
        "kilogram",
    )
    assert science(supplier) == science(
        reference()["CLIC_supplier_reference"]["snapshot"]
    )
    assert supplier["production amount"] == 1


def test_proxy_qualification_retains_history_and_does_not_claim_complete_module_equivalence(
    current,
):
    note = reference()["notes"][0]
    assert activity(current.data, MODULE)["comment"] == note["prefix"] + note["before"]
    for term in (
        "mass-based engineering manufacturing proxy",
        "not a verified Li-O2 circuit",
        "integer board-count claim",
        "consumer coefficients and retirement quantities",
        "Other module-packaging recipe differences remain outside",
    ):
        assert term in note["prefix"]


def test_all33_other_records_all_quantities_and_three_consumer_doses_unchanged(current):
    history = validate_current_monitor(current)

    for now, old in zip(current.data, history):
        assert [e["amount"] for e in now["exchanges"]] == [
            e["amount"] for e in old["exchanges"]
        ]
    for name, amount in (
        ("battery production, Li-O2", 0.05843941890070392),
        (
            "battery pack production, lithium-oxygen, without auxiliaries",
            0.07937993327365939,
        ),
        (
            "battery pack component production and end-of-life treatment, lithium-oxygen, without cells and controls",
            0.307244094488189,
        ),
    ):
        assert exchange(activity(current.data, name), MODULE)["amount"] == amount
    restored = previous_module_monitor_source(deepcopy(current))
    assert restored.data == history
    assert previous_module_monitor_source(restored).data == history


def test_exact_three_XML_cells_reverse_to_independent_before_all176_caches_and_other_parts_preserved():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
    assert root.find(NS + "dimension").get("ref") == "A1:T637"

    formulas = [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in root.iter(NS + "c")
        if c.find(NS + "f") is not None
    ]
    assert len(formulas) == 176
    assert_review_cells(root, reference(), "after", INVENTORY_DIR / WORKBOOK)
    restored = previous_module_monitor_xml(root)

    assert all(
        restored.find(f".//{NS}c[@r='{a}']").get("t") == "s" for a in ORIGINAL_CELLS
    )
    payload = ET.tostring(restored)
    assert ET.tostring(previous_module_monitor_xml(restored)) == payload


def test_guard_rejects_unapproved_mass_uncertainty_supplier_note_or_XML_without_mutation(
    current,
):
    for field, value in (
        ("amount", 0.035),
        ("location", "RER"),
        ("uncertainty type", 2),
    ):
        bad = deepcopy(current)
        exchange(activity(bad.data, MODULE), BOARD)[field] = value
        before = deepcopy(bad.data)
        with pytest.raises(AssertionError):
            previous_module_monitor_source(bad)
        assert bad.data == before
    bad = deepcopy(current)
    activity(bad.data, MODULE)["comment"] += "changed"
    with pytest.raises(AssertionError):
        previous_module_monitor_source(bad)
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
    root.find(f".//{NS}c[@r='A220']/{NS}is/{NS}t").text = "tampered supplier"
    before = ET.tostring(root)
    with pytest.raises(AssertionError):
        previous_module_monitor_xml(root)
    assert ET.tostring(root) == before


@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_original39_real_preparation_links_existing_module_monitor_once(
    current, system_model
):
    source = deepcopy(current)
    module = deepcopy(activity(current.data, MODULE))
    board = deepcopy(activity(ExcelImporter(INVENTORY_DIR / LIS).data, BOARD))
    source.data = [module, board]
    fields = ("name", "reference product", "location", "unit")
    identities = {
        tuple(e[k] for k in fields)
        for r in source.data
        for e in r["exchanges"]
        if e["type"] == "technosphere" and e["name"] not in (MODULE, BOARD)
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
    assert len(importer.import_db.data) == 2
    assert science(activity(importer.import_db.data, MODULE)) == science(module)
    assert science(activity(importer.import_db.data, BOARD)) == science(board)
