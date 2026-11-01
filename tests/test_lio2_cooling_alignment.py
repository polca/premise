"""ALIGN-046: standalone cooling identity, with guarded ALIGN-045 compatibility."""

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
from inventory_assertions import assert_same_cells
from test_central_fastener_alignment import (
    previous_fastener_source,
    previous_fastener_xml,
)

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiO2.xlsx"
NAME = "battery production, Li-O2"
OLD = "battery cooling system production, lithium-oxygen"
COOLING = "cooling system production, for Li-O2 battery"
ELECTRICITY = "market group for electricity, medium voltage"


NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


def activity(data, name=NAME):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(record, name):
    (match,) = [e for e in record["exchanges"] if e["name"] == name]
    return match


def vector(e):
    return tuple(
        e[k]
        for k in ("type", "name", "reference product", "location", "unit", "amount")
    )


def reference():
    approved = json.loads(
        (
            ROOT / "tests/data/inventory_review/lio2-cooling-adoption-20261027.json"
        ).read_text()
    )
    assert approved["case_id"] == "ALIGN-046"

    return approved


def validate_current_cooling(source):
    approved = reference()
    actual = activity(source.data)
    expected = deepcopy(approved["before_snapshot"])
    purchase = exchange(expected, OLD)
    purchase.update(
        name=COOLING, location="RER", **{"reference product": "cooling system"}
    )
    assert expected == approved["after_snapshot"]
    assert actual["comment"].endswith(approved["before_snapshot"]["comment"])

    expected["comment"] = actual["comment"]
    assert expected == actual  # All other aliases, utilities, uncertainty, output.
    observed = vector(exchange(actual, COOLING))
    curated = vector(exchange(approved["CLIC_snapshot"], COOLING))
    assert observed[:-1] == curated[:-1]
    # The unchanged workbook cache ends ...846, CLIC serializes ...845.
    # Raw fields above and the XML pin below stay exact; only this independent
    # cross-source floating-point comparison permits the rounding difference.
    assert observed[-1] == pytest.approx(curated[-1], rel=1e-15, abs=0)
    supplier = activity(source.data, COOLING)

    assert supplier == approved["supplier_snapshot"]
    assert sorted(vector(e) for e in supplier["exchanges"]) == sorted(
        vector(e) for e in approved["CLIC_supplier_snapshot"]["exchanges"]
    )
    return approved


def previous_cooling_source(source):
    """Validate present scope before exposing the original assembly-test view."""
    approved = validate_current_cooling(source)
    source.data = [
        deepcopy(approved["before_snapshot"]) if d["name"] == NAME else d
        for d in source.data
    ]
    return source


def cooling_xml_scope(root):
    return [
        [a, ET.tostring(root.find(f".//{NS}c[@r='{a}']"), encoding="unicode")]
        for a in ("A283", "E283", "G283", "B269")
    ]


def previous_cooling_xml(root):
    approved = validate_current_cooling(
        previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)
    )

    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        expected_root = previous_fastener_xml(
            ET.fromstring(archive.read(SHEET)), WORKBOOK
        )
    assert_same_cells(root, expected_root, ("A283", "E283", "G283", "B269"))
    old = exchange(approved["before_snapshot"], OLD)
    for address, field in (
        ("A283", "name"),
        ("E283", "location"),
        ("G283", "reference product"),
    ):
        root.find(f".//{NS}c[@r='{address}']/{NS}is/{NS}t").text = old[field]
    root.find(f".//{NS}c[@r='B269']/{NS}is/{NS}t").text = approved["before_snapshot"][
        "comment"
    ]

    return root


@pytest.fixture(scope="module")
def adoption():
    return reference()


@pytest.fixture(scope="module")
def raw():
    return previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)


def test_independent_parent_supplier_snapshots_and_original39_profiles(adoption):
    assert (adoption["CLIC_dataset_id"], adoption["CLIC_version_id"]) == (1142, 5675)
    assert (
        adoption["CLIC_supplier_dataset_id"],
        adoption["CLIC_supplier_version_id"],
    ) == (1150, 1150)
    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["control_alignment_remains_separate"] is True


def test_only_standalone_supplier_identity_and_note_change_assembly_mass_and_controls_stay(
    raw, adoption
):
    validate_current_cooling(raw)
    parent = activity(raw.data)
    purchase = exchange(parent, COOLING)
    assert (purchase["amount"], purchase["location"], purchase["unit"]) == (
        0.08707503369776846,
        "RER",
        "kilogram",
    )
    assert purchase["amount"] * 333.85 == pytest.approx(29.07, rel=1e-15)
    assert len(parent["exchanges"]) == 9
    assert exchange(parent, ELECTRICITY)["amount"] == 0.00034113790291714506
    assert exchange(parent, "treatment of LiO2 battery, for recycling")["amount"] == 0
    prefix = parent["comment"][: -len(adoption["before_snapshot"]["comment"])]
    for term in (
        "APPROVED Li-O2 STANDALONE COOLING SUPPLIER",
        "inherited engineering proxy",
        "not verified battery-specific thermal design",
        "0.99996 kg retains source rounding",
        "Retain the GLO helper",
        "Control alignment remains separate",
        "does not establish complete recipe equivalence",
        "both remain 3.9",
    ):
        assert term in prefix


def test_full_nine_flow_rer_recipe_retains_material_rounding_and_freight_without_repair(
    raw, adoption
):
    supplier = activity(raw.data, COOLING)
    assert len(supplier["exchanges"]) == 9
    assert supplier["production amount"] == exchange(supplier, COOLING)["amount"] == 1
    assert (supplier["location"], supplier["unit"], supplier["reference product"]) == (
        "RER",
        "kilogram",
        "cooling system",
    )
    materials = [
        e
        for e in supplier["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "kilogram"
    ]
    assert [e["amount"] for e in materials] == [
        0.87,
        0.038,
        0.023,
        0.00096,
        0.02,
        0.048,
    ]
    assert sum(e["amount"] for e in materials) == pytest.approx(0.99996, rel=1e-15)
    assert sorted(vector(e) for e in supplier["exchanges"]) == sorted(
        vector(e) for e in adoption["CLIC_supplier_snapshot"]["exchanges"]
    )
    assert exchange(supplier, "market for transport, freight train")["amount"] == 0.22
    assert (
        exchange(
            supplier, "market for transport, freight, lorry >32 metric ton, EURO6"
        )["amount"]
        == 0.1
    )


def test_all_other33_records_including_glo_helper_and_other_consumers_remain_exact(raw):
    others = [d for d in raw.data if d["name"] != NAME]
    assert len(raw.data) == 34 and len(others) == 33

    assert len(activity(raw.data, OLD)["exchanges"]) == 4
    restored = previous_cooling_source(deepcopy(raw))
    assert [d for d in restored.data if d["name"] != NAME] == others


def test_exact_four_cell_reversal_preserves_all176_formula_caches_and_other_parts(
    raw, adoption
):
    validate_current_cooling(raw)
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    quantity = root.find(f".//{NS}c[@r='B283']")
    assert (
        quantity.find(NS + "f").text
        == quantity.find(NS + "v").text
        == "0.08707503369776846"
    )
    assembly = root.find(f".//{NS}c[@r='B285']")
    assert assembly.find(NS + "f").text == "0.41/3.6/333.85"
    assert assembly.find(NS + "v").text == "0.00034113790291714506"
    formulas = [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in root.iter(NS + "c")
        if c.find(NS + "f") is not None
    ]
    assert len(formulas) == 176
    assert len(parts) == 12
    assert root.find(NS + "dimension").get("ref") == "A1:T637"
    previous_cooling_xml(root)


def test_historical_view_rejects_unapproved_parent_supplier_note_or_XML_changes(raw):
    for name in (NAME, COOLING):
        broken = deepcopy(raw)
        activity(broken.data, name)["exchanges"][1]["amount"] += 1e-6
        unchanged = deepcopy(broken.data)
        with pytest.raises(AssertionError):
            previous_cooling_source(broken)
        assert broken.data == unchanged
    broken = deepcopy(raw)
    activity(broken.data)["comment"] += "Changed note"
    with pytest.raises(AssertionError):
        previous_cooling_source(broken)
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
    root.find(f".//{NS}c[@r='A283']/{NS}is/{NS}t").text = "tampered supplier"
    with pytest.raises(AssertionError):
        previous_cooling_xml(root)


@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real39_preparation_closes_parent_and_detailed_cooling_supplier(
    raw, system_model
):
    source = deepcopy(raw)
    selected = {NAME, COOLING}
    source.data = [d for d in source.data if d["name"] in selected]
    fields = ("name", "reference product", "location", "unit")
    identities = {
        tuple(e[k] for k in fields)
        for d in source.data
        for e in d["exchanges"]
        if e["type"] == "technosphere" and e["name"] not in selected
    }
    background = [dict(zip(fields, key), exchanges=[]) for key in sorted(identities)]
    original = deepcopy(background)
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
    assert importer.list_unlinked == [] and background == original
    assert len(importer.import_db.data) == 2
    for name in selected:
        assert [
            vector(e) for e in activity(importer.import_db.data, name)["exchanges"]
        ] == [vector(e) for e in activity(raw.data, name)["exchanges"]]
