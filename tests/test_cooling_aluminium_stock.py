"""ALIGN-049: finished aluminium stock for the two existing cooling children."""

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
from test_lio2_module_monitor_alignment import (
    previous_module_monitor_source,
    previous_module_monitor_xml,
)

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiO2.xlsx"
STOCK = "market for aluminium, wrought alloy"
OLD_STOCK = "market for aluminium, in mixed metal scrap"
RADIATOR = "radiator production, for Li-O2 battery"
MANIFOLDS = "manifolds production, for Li-O2 battery"


NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"

ORIGINAL_CELLS = {
    "A466": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="A466" t="s"><ns0:v>217</ns0:v></ns0:c>',
    "A483": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="A483" t="s"><ns0:v>217</ns0:v></ns0:c>',
    "B457": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="B457" t="s"><ns0:v>257</ns0:v></ns0:c>',
    "B474": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="B474" t="s"><ns0:v>257</ns0:v></ns0:c>',
    "E466": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="E466" t="s"><ns0:v>63</ns0:v></ns0:c>',
    "E483": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="E483" t="s"><ns0:v>63</ns0:v></ns0:c>',
    "G466": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="G466" t="s"><ns0:v>218</ns0:v></ns0:c>',
    "G483": '<ns0:c xmlns:ns0="http://schemas.openxmlformats.org/spreadsheetml/2006/main" r="G483" t="s"><ns0:v>218</ns0:v></ns0:c>',
}


def activity(data, name):
    (r,) = [r for r in data if r["name"] == name]
    return r


def exchange(record, name):
    (e,) = [e for e in record["exchanges"] if e["name"] == name]
    return e


def vector(e):
    return tuple(
        e[k]
        for k in ("type", "name", "reference product", "location", "unit", "amount")
    )


def reference():
    payload = (
        ROOT / "scripts/data/cooling-aluminium-stock-adoption-20261027.json"
    ).read_bytes()

    p = json.loads(payload)

    return p


def validate_current_stock(source):
    p = reference()
    assert len(source.data) == p["activity_count"] == 34

    expected = {a["index"]: deepcopy(a["before"]) for a in p["affected_activities"]}
    for a in p["affected_activities"]:
        r = expected[a["index"]]
        stock = exchange(r, OLD_STOCK)
        assert stock["amount"] == 1 and stock["unit"] == "kilogram"
        stock.update(
            name=STOCK,
            location="GLO",
            **{"reference product": "aluminium, wrought alloy"},
        )
    for note in p["notes"]:
        r = expected[note["activity_index"]]
        assert r["comment"] == note["before"]
        assert note["prefix"].startswith("APPROVED COOLING-COMPONENT ALUMINIUM STOCK")
        r["comment"] = note["prefix"] + note["before"]
    history = list(source.data)
    for a in p["affected_activities"]:
        actual = source.data[a["index"]]
        assert expected[a["index"]] == a["after"] == actual
        (candidate,) = [
            c
            for c in p["candidate_revisions"]
            if c["dataset_id"] == a["CLIC_dataset_id"]
        ]
        assert sorted(vector(e) for e in actual["exchanges"]) == sorted(
            vector(e) for e in candidate["candidate_snapshot"]["exchanges"]
        )
        history[a["index"]] = deepcopy(a["before"])

    return p, history


def xml_scope(root):
    return [
        [a, ET.tostring(root.find(f".//{NS}c[@r='{a}']"), encoding="unicode")]
        for a in sorted(ORIGINAL_CELLS)
    ]


def validate_live_current():
    validate_current_stock(
        previous_module_monitor_source(ExcelImporter(INVENTORY_DIR / WORKBOOK))
    )
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_module_monitor_xml(ET.fromstring(archive.read(SHEET)))
    assert_review_cells(root, reference(), "after", INVENTORY_DIR / WORKBOOK)


def previous_stock_source(source, filename):
    if filename != WORKBOOK:
        return source
    validate_live_current()
    if records_match_review(source.data, reference()):
        return source  # Authenticated old view; live current science was checked.
    source = previous_module_monitor_source(source)
    _, history = validate_current_stock(source)
    source.data = history
    return source


def previous_stock_xml(root, filename=WORKBOOK, sheet=SHEET):
    if filename != WORKBOOK or sheet != SHEET:
        return root
    validate_live_current()
    root = previous_module_monitor_xml(root)
    if review_cells_match(root, reference(), "before", INVENTORY_DIR / WORKBOOK):
        return root  # Do not apply a second reversal to an exact prior view.
    assert_review_cells(root, reference(), "after", INVENTORY_DIR / WORKBOOK)
    for address, payload in ORIGINAL_CELLS.items():
        row = root.find(
            f".//{NS}row[@r='{''.join(c for c in address if c.isdigit())}']"
        )
        cell = row.find(f"{NS}c[@r='{address}']")
        position = list(row).index(cell)
        row.remove(cell)
        row.insert(position, ET.fromstring(payload))
    assert_review_cells(root, reference(), "before", INVENTORY_DIR / WORKBOOK)
    return root


@pytest.fixture(scope="module")
def current():
    return previous_module_monitor_source(ExcelImporter(INVENTORY_DIR / WORKBOOK))


def test_published_bases_remain_immutable_and_candidates_are_planned_original39_revisions():
    p = reference()
    assert p["case_id"] == "ALIGN-049" and p["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert p["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert p["staging_performed"] is False and p["published_versions_modified"] is False
    assert p["LiS_cooling_switch_deferred"] is True
    for c in p["candidate_revisions"]:
        assert (
            c["dataset_id"] == c["base_version_id"]
            and c["base_semantic_version"] == "1.0.0"
        )
        assert c["published_base_immutable"] is True and c["new_version_id"] is None
        assert c["revision_status"] == "approved_planned_not_staged"
        assert (
            c["target_background"]["version"]
            == c["target_biosphere"]["version"]
            == "3.9"
        )
        expected = deepcopy(c["base_snapshot"])
        stock = exchange(expected, OLD_STOCK)
        stock.update(
            name=STOCK,
            location="GLO",
            **{"reference product": "aluminium, wrought alloy"},
        )
        assert c["candidate_snapshot"]["comment"].endswith(expected["comment"])
        expected["comment"] = c["candidate_snapshot"]["comment"]
        assert expected == c["candidate_snapshot"]
        assert c["native_stock_binding"]["identity"] == [
            STOCK,
            "aluminium, wrought alloy",
            "GLO",
            "kilogram",
        ]


@pytest.mark.parametrize("name", (RADIATOR, MANIFOLDS))
def test_current_component_changes_only_stock_identity_and_qualified_note_matches_candidate_science(
    current, name
):
    p, _ = validate_current_stock(current)
    r = activity(current.data, name)
    stock = exchange(r, STOCK)
    assert vector(stock) == (
        "technosphere",
        STOCK,
        "aluminium, wrought alloy",
        "GLO",
        "kilogram",
        1,
    )
    assert (
        r["unit"] == "kilogram"
        and r["production amount"] == exchange(r, name)["amount"] == 1
    )
    assert len(r["exchanges"]) == 6
    a = next(a for a in p["affected_activities"] if a["before"]["name"] == name)
    prefix = r["comment"][: -len(a["before"]["comment"])]
    for term in (
        "adopted metal-stock proxy",
        "alloy grade and recycled fraction remain unverified",
        "does not claim 100 percent secondary aluminium",
        "Preserve the 1 kg stock quantity",
        "Do not add a second remelting process",
        "cancellation of native processing stock/losses/uncertainty",
        "both remain 3.9",
    ):
        assert term in prefix


def test_stock_is_purchased_once_without_counting_processing_service_as_retained_metal(
    current,
):
    for name, service in (
        (RADIATOR, "sheet rolling, aluminium"),
        (MANIFOLDS, "metal working, average for aluminium product manufacturing"),
    ):
        r = activity(current.data, name)
        assert exchange(r, STOCK)["amount"] == exchange(r, service)["amount"] == 1
        assert (
            exchange(r, "aluminium casting facility construction")["amount"] == 1.5e-10
        )
        assert exchange(r, "market for transport, freight train")["amount"] == 0.2
        assert (
            exchange(r, "market for transport, freight, lorry >32 metric ton, EURO6")[
                "amount"
            ]
            == 0.1
        )
        assert all(e["amount"] > 0 for e in r["exchanges"])
    cooling = activity(current.data, "cooling system production, for Li-O2 battery")
    assert (
        exchange(cooling, RADIATOR)["amount"] == 0.87
        and exchange(cooling, MANIFOLDS)["amount"] == 0.038
    )
    stock_per_kg_cooling = (0.87 + 0.038) * 1
    assert stock_per_kg_cooling == 0.908
    assert stock_per_kg_cooling * 29.07 == pytest.approx(26.39556, rel=1e-15)


def test_all_other32_records_parent_quantities_and_consumers_are_exactly_unchanged(
    current,
):
    p, _ = validate_current_stock(current)
    affected = {a["index"] for a in p["affected_activities"]}
    others = [r for i, r in enumerate(current.data) if i not in affected]
    assert len(others) == 32
    parent = activity(current.data, "battery production, Li-O2")
    assert (
        exchange(parent, "cooling system production, for Li-O2 battery")["amount"]
        == 0.08707503369776846
    )
    assert (
        exchange(parent, "market group for electricity, medium voltage")["amount"]
        == 0.00034113790291714506
    )
    assert exchange(parent, "treatment of LiO2 battery, for recycling")["amount"] == 0


def test_eight_shared_to_inline_cells_reverse_exactly_with_all_amounts_formulas_and_parts_preserved():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_module_monitor_xml(ET.fromstring(archive.read(SHEET)))
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]

    assert root.find(NS + "dimension").get("ref") == "A1:T637"
    assert all(
        root.find(f".//{NS}c[@r='{a}']").get("t") == "inlineStr" for a in ORIGINAL_CELLS
    )
    for a in ("B466", "B483"):
        cell = root.find(f".//{NS}c[@r='{a}']")
        assert cell.attrib == {"r": a, "s": "3"} and cell.find(NS + "v").text == "1"
    formulas = [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in root.iter(NS + "c")
        if c.find(NS + "f") is not None
    ]
    assert len(formulas) == 176
    assert len(parts) == 12
    restored = previous_stock_xml(root)
    assert all(
        restored.find(f".//{NS}c[@r='{a}']").get("t") == "s" for a in ORIGINAL_CELLS
    )


def test_authenticated_historical_source_and_XML_views_do_not_repeat_reversal(current):
    source = previous_stock_source(deepcopy(current), WORKBOOK)
    history = deepcopy(source.data)

    assert previous_stock_source(source, WORKBOOK).data == history
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_module_monitor_xml(ET.fromstring(archive.read(SHEET)))
    root = previous_stock_xml(root)
    old = ET.tostring(root)
    assert ET.tostring(previous_stock_xml(root)) == old


def test_guard_rejects_changed_stock_quantity_uncertainty_note_or_XML_before_reversal(
    current,
):
    for problem in ("quantity", "uncertainty", "note"):
        source = deepcopy(current)
        r = activity(source.data, RADIATOR)
        if problem == "quantity":
            exchange(r, STOCK)["amount"] += 0.001
        elif problem == "uncertainty":
            exchange(r, STOCK)["uncertainty type"] = 2
        else:
            r["comment"] += "Changed note"
        unchanged = deepcopy(source.data)
        with pytest.raises(AssertionError):
            previous_stock_source(source, WORKBOOK)
        assert source.data == unchanged
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_module_monitor_xml(ET.fromstring(archive.read(SHEET)))
    root.find(f".//{NS}c[@r='A466']/{NS}is/{NS}t").text = "tampered supplier"
    with pytest.raises(AssertionError):
        previous_stock_xml(root)


@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real39_preparation_closes_detailed_cooling_and_two_corrected_stock_children(
    current, system_model
):
    source = deepcopy(current)
    selected = {
        "battery production, Li-O2",
        "cooling system production, for Li-O2 battery",
        RADIATOR,
        MANIFOLDS,
    }
    source.data = [r for r in source.data if r["name"] in selected]
    fields = ("name", "reference product", "location", "unit")
    identities = {
        tuple(e[k] for k in fields)
        for r in source.data
        for e in r["exchanges"]
        if e["type"] == "technosphere" and e["name"] not in selected
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
    assert (
        importer.version_in == importer.version_out == "3.9"
        and importer.list_unlinked == []
        and background == before
    )
    assert len(importer.import_db.data) == 4
    for name in selected:
        assert [
            vector(e) for e in activity(importer.import_db.data, name)["exchanges"]
        ] == [vector(e) for e in activity(current.data, name)["exchanges"]]
