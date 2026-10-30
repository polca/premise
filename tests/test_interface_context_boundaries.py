"""ALIGN-031: standalone board manufacture and embedded material coverage."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from inventory_assertions import assert_same_cells, formula_caches
from test_central_fastener_alignment import (
    previous_fastener_source,
    previous_fastener_xml,
)
from test_lis_factory_allocation import previous_factory_source, previous_factory_xml

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiS.xlsx"
INTERFACE = "battery auxiliary interface and power conditioning unit production"
CENTRAL = "printed circuit board production, battery central control, mounted"
MODULE = "printed circuit board production, battery module control, mounted"
BARE_BOARD = (
    "market for printed wiring board, for through-hole mounting, Pb free surface"
)
ELECTRICITY = "market group for electricity, medium voltage"
MATERIALS = (
    "market for copper, cathode",
    "market for ferrite",
    "market for aluminium, wrought alloy",
    "market for nylon 6",
)
FRACTIONS = (
    0.34615384615384615,
    0.28846153846153844,
    0.19230769230769232,
    0.17307692307692307,
)
RESIDUALS = {CENTRAL: 0.04263626663673462, MODULE: 0.07424213682857134}
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
ROWS = {1202, 1859, *range(1204, 1208), *range(1604, 1616), *range(1865, 1869)}
COMMENTS = {"B1185", "B1599", "B1838"}

CLIC_PINS = {INTERFACE: (3673, 5588), CENTRAL: (3675, 5590), MODULE: (3676, 5591)}


# ALIGN-042 before.xlsx (9a40ab76d3fb52dfd48436232e7e3cce2a6d6302e9bbc1304
# bfbced8bd9452cf) independently reproduces every existing LiS pin after the
# previous approvals. Reverse only B302, H299 and row 303, after checking the
# current foil recipe against the independent expanded CLIC 1169/5553 science.
FOIL = "lithium foil production, for LiS battery"
ROLLING = "lithium foil rolling"


def plan():
    return json.loads(
        (
            ROOT / "scripts/data/interface-context-boundaries-adoption-20261027.json"
        ).read_text()
    )


def cases(adoption):
    return [adoption["standalone"], *adoption["embedded_boards"]]


def activity(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def science(dataset):
    # A sorted list preserves duplicate exchanges and includes the production.
    return sorted(
        [
            [
                e["type"],
                e["name"],
                e["reference product"],
                e.get("location"),
                e["unit"],
                e["amount"],
            ]
            for e in dataset["exchanges"]
        ]
    )


def foil_reference():
    approved = json.loads(
        (ROOT / "scripts/data/lithium-foil-return-adoption-20261027.json").read_text()
    )
    assert approved["case_id"] == "ALIGN-042"

    assert approved["comment_field"] == "Comment"
    assert (approved["CLIC_dataset_id"], approved["CLIC_version_id"]) == (1169, 5553)
    assert approved["source_background"]["version"] == "3.9"
    assert approved["source_biosphere"]["version"] == "3.9"
    return approved


def validate_current_foil(source):
    approved = foil_reference()
    actual = activity(source.data, FOIL)
    expected = deepcopy(approved["before_snapshot"])
    expected["exchanges"].remove(approved["removed_exchange"])
    (lithium,) = [
        e
        for e in expected["exchanges"]
        if e["name"] == "electrolysis of lithium chloride"
    ]
    assert lithium["amount"] == 1.0204081632653061
    lithium["amount"] = 1
    assert expected == approved["after_snapshot"]
    (rolling,) = [e for e in actual["exchanges"] if e["name"] == ROLLING]
    note = rolling["Comment"]
    assert note.endswith(approved["before_rolling_comment"])

    (old_rolling,) = [e for e in expected["exchanges"] if e["name"] == ROLLING]
    old_rolling["Comment"] = note
    assert actual == expected  # All fields, aliases, uncertainty and history.

    service = activity(source.data, ROLLING)

    assert rolling["amount"] == 4.95
    expanded = deepcopy(actual)
    expanded["exchanges"].remove(rolling)
    for exc in service["exchanges"]:
        if exc["type"] == "technosphere":
            contribution = deepcopy(exc)
            contribution["amount"] *= rolling["amount"] / service["production amount"]
            expanded["exchanges"].append(contribution)
    assert science(expanded) == science(approved["CLIC_snapshot"])

    return approved


def previous_foil_source(source):
    """Expose the old foil only after guarding the approved current science."""
    approved = validate_current_foil(source)
    source.data = [
        deepcopy(approved["before_snapshot"]) if a["name"] == FOIL else a
        for a in source.data
    ]
    return source


def foil_xml_scope(root):
    row = root.find(f".//{NS}row[@r='303']")
    return [
        [a, ET.tostring(root.find(f".//{NS}c[@r='{a}']"), encoding="unicode")]
        for a in ("B302", "H299")
    ] + [["303", ET.tostring(row, encoding="unicode") if row is not None else None]]


def previous_foil_xml(root, sheet=SHEET):
    """Restore exact independent before payloads; leave all other XML intact."""
    if sheet != SHEET:
        return root
    root = previous_factory_xml(root, sheet)
    source = previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)
    approved = validate_current_foil(source)
    data = root.find(NS + "sheetData")
    assert root.find(f".//{NS}row[@r='303']") is None
    quantity = root.find(f".//{NS}c[@r='B302']/{NS}v")
    assert quantity.text == "1"
    quantity.text = "1.0204081632653061"
    comment = root.find(f".//{NS}c[@r='H299']/{NS}is/{NS}t")
    (rolling,) = [
        e for e in activity(source.data, FOIL)["exchanges"] if e["name"] == ROLLING
    ]
    assert comment.text == rolling["Comment"]
    comment.text = approved["before_rolling_comment"]
    # This legacy block's columns differ from the newer component blocks.
    row = ET.Element(NS + "row", r="303")
    old = approved["removed_exchange"]
    for column, field in (
        ("A", "name"),
        ("B", "amount"),
        ("C", "unit"),
        ("E", "location"),
        ("F", "type"),
        ("G", "reference product"),
        ("H", "Comment"),
    ):
        cell = ET.SubElement(row, NS + "c", r=column + "303")
        if field == "amount":
            ET.SubElement(cell, NS + "v").text = str(old[field])
        else:
            cell.set("t", "inlineStr")
            ET.SubElement(ET.SubElement(cell, NS + "is"), NS + "t").text = old[field]
    data.append(row)
    data[:] = sorted(data, key=lambda r: int(r.get("r")))

    return root


def validate_current(source, adoption):
    assert len(source.data) == 85
    prefixes = []
    for case in cases(adoption):
        actual = activity(source.data, case["name"])

        assert {k: v for k, v in actual.items() if k != "comment"} == {
            k: v for k, v in case["after_snapshot"].items() if k != "comment"
        }

        old = case["before_snapshot"]["comment"]
        assert actual["comment"].endswith(old)
        prefix = actual["comment"][: -len(old)]
        assert prefix.startswith(
            "APPROVED STANDALONE INTERFACE / EMBEDDED RESIDUAL BOUNDARIES"
        )
        prefixes.append(prefix)
    assert len(set(prefixes)) == 1


def restore_previous_interfaces(source):
    """Validate all three current recipes before exposing the ALIGN-030 view."""
    adoption = plan()
    validate_current(source, adoption)
    previous = {c["name"]: c["before_snapshot"] for c in cases(adoption)}
    source.data = [deepcopy(previous.get(a["name"], a)) for a in source.data]
    return source


def xml_scope(root):
    return [
        [r.get("r"), ET.tostring(r, encoding="unicode")]
        for r in root.find(NS + "sheetData")
        if int(r.get("r")) in ROWS
    ] + [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in root.iter(NS + "c")
        if c.get("r") in COMMENTS
    ]


def exchange_row(number, exchange, preserved=(), all_strings_preserved=False):
    """Serialize explicit exchange fields; historical whitespace is significant."""
    row = ET.Element(NS + "row", r=str(number))
    columns = (
        "name",
        "amount",
        "location",
        "unit",
        "categories",
        "type",
        "reference product",
        "comment",
        "product",
    )
    for index, field in enumerate(columns):
        if field not in exchange:
            continue
        value = exchange[field]
        cell = ET.SubElement(row, NS + "c", r=f"{chr(65 + index)}{number}")
        if isinstance(value, (int, float)):
            ET.SubElement(cell, NS + "v").text = str(value)
        else:
            cell.set("t", "inlineStr")
            text = ET.SubElement(ET.SubElement(cell, NS + "is"), NS + "t")
            if field in preserved or all_strings_preserved:
                text.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
            text.text = value
    return row


def previous_interface_xml(root, sheet=SHEET):
    """Bounded XML reversal; present scope is guarded before old pins see it."""
    if sheet != SHEET:
        return root
    root = previous_foil_xml(root, sheet)
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        expected_root = previous_foil_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK), sheet
        )
    addresses = {
        cell.get("r")
        for candidate in (root, expected_root)
        for cell in candidate.iter(NS + "c")
        if int("".join(c for c in cell.get("r") if c.isdigit())) in ROWS
        or cell.get("r") in COMMENTS
    }
    assert_same_cells(root, expected_root, addresses)

    assert root.find(NS + "dimension").get("ref") == "A1:Q1868"

    adoption = plan()
    data = root.find(NS + "sheetData")
    rows = {int(r.get("r")): r for r in data}
    clones = []
    for current in range(1605, 1615):
        row = deepcopy(rows[current])
        row.set("r", str(current + 1))
        for cell in row:
            cell.set("r", cell.get("r").rstrip("0123456789") + str(current + 1))
        clones.append(row)
    for row in list(data):
        if int(row.get("r")) in ROWS:
            data.remove(row)
    data.extend(clones)
    for case in cases(adoption):
        old = case["before_snapshot"]
        comment = root.find(f".//{NS}c[@r='{case['comment_cell']}']/{NS}is/{NS}t")
        assert comment.text.endswith(old["comment"])
        comment.text = old["comment"]
        if case["name"] == INTERFACE:
            for number, exchange in zip((1604, 1605), old["exchanges"][1:3]):
                data.append(exchange_row(number, exchange))
        else:
            (exchange,) = [e for e in old["exchanges"] if e["name"] == INTERFACE]
            data.append(
                exchange_row(
                    case["replaced_row"],
                    exchange,
                    preserved=("name", "reference product"),
                )
            )
    data[:] = sorted(data, key=lambda r: int(r.get("r")))
    root.find(NS + "dimension").set("ref", "A1:Q1864")

    return root


@pytest.fixture(scope="module")
def adoption():
    return plan()


@pytest.fixture(scope="module")
def raw():
    return previous_foil_source(
        previous_factory_source(
            previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)
        )
    )


def test_current_foil_guard_preserves_net_feed_expanded_clic_science_and_qualifications():
    source = previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)
    approved = validate_current_foil(source)
    actual = activity(source.data, FOIL)
    assert len(actual["exchanges"]) == 6
    (rolling,) = [e for e in actual["exchanges"] if e["name"] == ROLLING]
    prefix = rolling["Comment"][: -len(approved["before_rolling_comment"])]
    assert prefix.startswith("APPROVED LITHIUM-FOIL INTERNAL RETURN")
    for term in (
        "1 kg net external lithium",
        "Manufacturing offcuts return internally",
        "density-adjusted rolling-service coefficient",
        "not measured throughput or pass count",
        "not measured zero lithium loss",
        "remelting, atmosphere and lubrication burdens remain unquantified",
        "No recovery credit, external scrap disposal, duplicate cell factory",
        "both remain 3.9",
    ):
        assert term in prefix
    restored = previous_foil_source(deepcopy(source))
    assert activity(restored.data, FOIL) == approved["before_snapshot"]
    assert [a for a in restored.data if a["name"] != FOIL] == [
        a for a in source.data if a["name"] != FOIL
    ]


def test_foil_historical_reversal_rejects_changed_current_recipe_note_and_XML():
    source = previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)
    for name, field in (
        ("electrolysis of lithium chloride", "amount"),
        ("market for cooling energy", "amount"),
        (ROLLING, "Comment"),
    ):
        broken = deepcopy(source)
        (exc,) = [
            e for e in activity(broken.data, FOIL)["exchanges"] if e["name"] == name
        ]
        exc[field] = "Altered note" if field == "Comment" else exc[field] + 1e-6
        untouched = deepcopy(broken.data)
        with pytest.raises(AssertionError):
            previous_foil_source(broken)
        assert broken.data == untouched
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
    root.find(f".//{NS}c[@r='B302']/{NS}v").text = "999"
    with pytest.raises(AssertionError):
        previous_foil_xml(root)


def test_independent_before_and_clic_pins_and_preserved_source_profiles(adoption):
    assert adoption["case_id"] == "ALIGN-031"

    assert adoption["workbook"] == WORKBOOK
    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "version": "3.9",
        "system_model": "cutoff",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}


@pytest.mark.parametrize("name", (INTERFACE, CENTRAL, MODULE))
def test_full_current_recipe_matches_independent_clic_science_and_exact_approved_raw_fields(
    raw, adoption, name
):
    validate_current(raw, adoption)
    case = next(c for c in cases(adoption) if c["name"] == name)
    actual = activity(raw.data, name)
    expected = deepcopy(case["before_snapshot"])
    if name == INTERFACE:
        expected["exchanges"][1:3] = [case["replacement_exchange"]]
    else:
        index = next(
            i for i, e in enumerate(expected["exchanges"]) if e["name"] == INTERFACE
        )
        expected["exchanges"][index] = case["replacement_exchanges"][0]
        expected["exchanges"].extend(case["replacement_exchanges"][1:])
    assert expected == case["after_snapshot"]
    assert len(actual["exchanges"]) == {INTERFACE: 12, CENTRAL: 19, MODULE: 27}[name]
    assert actual["production amount"] == actual["exchanges"][0]["amount"] == 1
    # Unrelated historical aliases remain preserved until generic preparation.
    replacements = (
        [case["replacement_exchange"]]
        if name == INTERFACE
        else case["replacement_exchanges"]
    )
    assert all(e["product"] == e["reference product"] for e in replacements)


def test_standalone_finished_board_redefines_partition_without_changing_component_or_assembly_doses(
    raw, adoption
):
    actual = activity(raw.data, INTERFACE)
    (bare,) = [e for e in actual["exchanges"] if e["name"] == BARE_BOARD]
    assert (bare["unit"], bare["location"]) == ("square meter", "GLO")
    assert bare["amount"] == pytest.approx(0.4 / 3.08, rel=1e-14)
    assert adoption["standalone_finished_board_fraction"] == 0.4
    assert adoption["native_board_mass_kg_per_m2"] == 3.08
    assert not {"market for glass fibre", "market for epoxy resin, liquid"} & {
        e["name"] for e in actual["exchanges"]
    }
    assert (
        actual["exchanges"][2:]
        == adoption["standalone"]["before_snapshot"]["exchanges"][3:]
    )
    assert sum(
        (0.4, 0.18, 0.15, 0.10, 0.09, 0.03, 0.025, 0.005, 0.01, 0.01)
    ) == pytest.approx(1)
    assert actual["exchanges"][-1]["name"] == ELECTRICITY
    assert actual["exchanges"][-1]["amount"] == 2


@pytest.mark.parametrize("name", (CENTRAL, MODULE))
def test_embedded_residual_closes_current_mass_and_energy_without_another_board_or_fitted_parts(
    raw, adoption, name
):
    case = next(c for c in adoption["embedded_boards"] if c["name"] == name)
    actual = activity(raw.data, name)
    residual = RESIDUALS[name]
    assert case["residual_mass_kg_per_kg_board"] == residual
    assert list(adoption["residual_material_fractions"].values()) == list(FRACTIONS)
    assert sum(FRACTIONS) == pytest.approx(1)
    replacements = case["replacement_exchanges"]
    assert [e["name"] for e in replacements] == [*MATERIALS, ELECTRICITY]
    assert [e["amount"] for e in replacements[:4]] == pytest.approx(
        [residual * f for f in FRACTIONS], rel=1e-14
    )
    assert sum(e["amount"] for e in replacements[:4]) == pytest.approx(
        residual, rel=1e-14
    )
    assert replacements[-1]["amount"] == residual * 2
    assert adoption["residual_preparation_kWh_per_kg"] == 2
    assert not {INTERFACE, BARE_BOARD} & {e["name"] for e in actual["exchanges"]}
    physical_mass = 0.49 if name == CENTRAL else 0.07
    assert sum(e["amount"] for e in replacements[:4]) * physical_mass == pytest.approx(
        residual * physical_mass, rel=1e-14
    )
    # Every other input, including assembly electricity, remains in order.
    before = [e for e in case["before_snapshot"]["exchanges"] if e["name"] != INTERFACE]
    current = deepcopy(actual["exchanges"])
    for replacement in replacements:
        current.remove(replacement)
    assert current == before


def test_notes_qualify_context_and_preserve_history_and_all_other_82_records(
    raw, adoption
):
    validate_current(raw, adoption)
    for case in cases(adoption):
        actual = activity(raw.data, case["name"])
        prefix = actual["comment"][: -len(case["before_snapshot"]["comment"])]
        for term in (
            "qualified engineering proxies",
            "external component stock",
            "not known board tracks",
            "unverified material-coverage sensitivity",
            "not measured residual composition",
            "zero missing precious",
            "fabrication burdens",
            "490 g",
            "70 g",
            "retirement",
            "3.9",
        ):
            assert term in prefix
    others = [d for d in raw.data if d["name"] not in CLIC_PINS]
    assert len(others) == 82


def test_exact_xml_scope_clones_formulas_caches_and_all_other_zip_parts(adoption):
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_foil_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        )
        parts = sorted((n, archive.read(n)) for n in archive.namelist() if n != SHEET)
    assert root.find(NS + "dimension").get("ref") == "A1:Q1868"
    data = root.find(NS + "sheetData")
    rows = {int(r.get("r")): r for r in data}
    assert len(rows) == len(set(rows)) and 1615 not in rows
    # Independently renumbered before rows.
    replacements = [(1604, adoption["standalone"]["replacement_exchange"])]
    for case in adoption["embedded_boards"]:
        replacements.extend(
            zip(
                [case["replaced_row"], *case["added_rows"]],
                case["replacement_exchanges"],
            )
        )
    for number, exchange in replacements:
        expected = exchange_row(number, exchange, all_strings_preserved=True)
        assert ET.tostring(rows[number]) == ET.tostring(expected)
    assert formula_caches(root)
    assert all(
        c.find(NS + "v").text is not None
        for c in root.iter(NS + "c")
        if c.find(NS + "f") is not None
    )
    assert len(parts) == 12
    root.find(NS + "dimension").set("ref", "A1:Q1864")
    for row in list(data):
        if int(row.get("r")) in ROWS:
            data.remove(row)
        else:
            for cell in list(row):
                if cell.get("r") in COMMENTS:
                    row.remove(cell)


def test_historical_normalization_rejects_changed_science_before_reversal(
    raw, adoption
):
    broken = deepcopy(raw)
    activity(broken.data, INTERFACE)["exchanges"][1]["amount"] += 1e-6
    with pytest.raises(AssertionError):
        restore_previous_interfaces(broken)
    assert (
        activity(broken.data, INTERFACE)["exchanges"][1]["amount"]
        != adoption["standalone"]["after_snapshot"]["exchanges"][1]["amount"]
    )
    restored = restore_previous_interfaces(deepcopy(raw))
    for case in cases(adoption):
        assert activity(restored.data, case["name"]) == case["before_snapshot"]
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
    root.find(f".//{NS}c[@r='B1604']/{NS}v").text = "99"
    with pytest.raises(AssertionError):
        previous_interface_xml(root)


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_preserves_three_current_recipes_with_identity_suppliers(
    raw, version, system_model
):
    source = deepcopy(raw)
    source.data = [a for a in source.data if a["name"] in CLIC_PINS]
    identities = {
        tuple(e[k] for k in ("name", "reference product", "location", "unit"))
        for a in source.data
        for e in a["exchanges"]
        if e["type"] == "technosphere" and e["name"] not in CLIC_PINS
    }
    background = [
        dict(zip(("name", "reference product", "location", "unit"), key), exchanges=[])
        for key in sorted(identities)
    ]
    original = deepcopy(background)
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
    assert importer.list_unlinked == [] and background == original
    assert len(importer.import_db.data) == 3
    for actual in importer.import_db.data:

        assert all(e["product"] == e["reference product"] for e in actual["exchanges"])
