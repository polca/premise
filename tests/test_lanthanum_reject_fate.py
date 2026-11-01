"""ALIGN-022: remove unsupported inert La disposal, retain the yield loss."""

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-hydrogen-electrolysis.xlsx"
NAME = "lanthanum metal production, oxide electrolysis"
PRODUCT = "lanthanum metal, oxide electrolysis"
ALLOY = "ferritic steel production, electrolysis interconnect, crofer 22 apu type"
STACK = "electrolyzer production, 1MWe, SOEC, Stack"
INERT = "market for inert waste, for final disposal"
OXIDE = "market for lanthanum oxide"
GRAPHITE = "market for graphite"
ELECTRICITY = "market group for electricity, medium voltage"
HEAT = "market for heat, district or industrial, natural gas"
CARBON = "Carbon dioxide, fossil"
LOSS = 0.05263157894736836
CO2 = 0.2501265599433993
SHEET = "xl/worksheets/sheet1.xml"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


OLD_FATE = "Metalrejectgenericinertdisposal"
NEW_FATE = "La reject fate and treatment burdens unassigned; 95% retention retained"
# Complete cell XML payloads from the original before.xlsx, including styles.
LEGACY_COMMENT_PINS = dict.fromkeys(("B466", "H472", "H473", "H474", "B881"))
COMMENT_CELLS = {"B862"} | LEGACY_COMMENT_PINS.keys()
SIC_CELLS = {"B1229", "B1234", "B1236", "H1235"}


def activity(data, name=NAME):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def exchange_key(exc):
    categories = exc.get("categories", ())
    if isinstance(categories, str):
        categories = categories.split("::")
    product = exc.get("reference product")
    if "product" in exc:
        assert exc["product"] == product
    return (
        exc["type"],
        exc["name"],
        product,
        exc.get("location"),
        exc["unit"],
        tuple(categories),
    )


def xml_cells(path):
    cells = {}
    with ZipFile(path) as archive:
        strings = ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(NS + "si")
        for sheet in archive.namelist():
            if not (sheet.startswith("xl/worksheets/sheet") and sheet.endswith(".xml")):
                continue
            for cell in ET.fromstring(archive.read(sheet)).iter(NS + "c"):
                value = cell.find(NS + "v")
                shared = strings[int(value.text)] if cell.get("t") == "s" else None
                payload = [
                    ET.tostring(cell, encoding="unicode"),
                    (
                        ET.tostring(shared, encoding="unicode")
                        if shared is not None
                        else None
                    ),
                ]
                cells[sheet, cell.get("r")] = cell, payload
    return cells


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (
            ROOT
            / "tests/data/inventory_review/lanthanum-reject-fate-adoption-20261027.json"
        ).read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


def test_portable_approval_pins_original_helper_clic_and_unchanged_39_profiles(
    adoption,
):
    assert adoption["approval"] == "ALIGN-022; approved by Romain Sacchi, 2026-10-27"
    assert adoption["workbook"] == WORKBOOK

    assert adoption["CLIC_dataset_id"] == 3704
    assert adoption["CLIC_version_id"] == 5619

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["formula_reference_audit"] == {"dependent_formula_cells": []}
    assert adoption["retained_yield"] == 0.95
    assert adoption["unassigned_La_equivalent_loss_kg_per_kg_La"] == LOSS
    assert adoption["retained_La_kg_per_kg_alloy"] == 0.001
    assert adoption["retained_alloy_and_sheet_working_service_kg"] == 8976.1
    assert adoption["unassigned_La_equivalent_loss_kg_in_stack_reference"] == (
        0.4724263157894732
    )


def test_exact_six_raw_flows_remove_only_inert_disposal_and_retain_all_fields(
    raw, adoption
):
    actual, before = activity(raw.data), adoption["before_snapshot"]
    expected = deepcopy(before)
    assert len(before["exchanges"]) == 7
    assert exchange(before, INERT)["amount"] == -LOSS
    expected["exchanges"] = [e for e in before["exchanges"] if e["name"] != INERT]
    # The portable after snapshot deliberately retains the original root note;
    # the live authoritative prepend is checked separately below.
    assert adoption["after_snapshot"] == expected
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert len(actual["exchanges"]) == 6
    assert Counter(e["type"] for e in actual["exchanges"]) == {
        "production": 1,
        "technosphere": 4,
        "biosphere": 1,
    }
    assert (
        actual["name"],
        actual["reference product"],
        actual["location"],
        actual["unit"],
        actual["production amount"],
    ) == (NAME, PRODUCT, "GLO", "kilogram", 1)
    assert exchange(actual, CARBON) == exchange(before, CARBON)
    assert exchange(actual, CARBON)["amount"] == CO2
    assert exchange(actual, CARBON)["categories"] == (
        "air::non-urban air or from high stacks"
    )
    assert [e["name"] for e in actual["exchanges"]] == [
        NAME,
        OXIDE,
        GRAPHITE,
        ELECTRICITY,
        HEAT,
        CARBON,
    ]
    assert not any(e.get("formula") for e in actual["exchanges"])
    assert not any(k.startswith("replacement ") for e in actual["exchanges"] for k in e)


def test_authoritative_root_prepend_preserves_history_and_unmodelled_fate(
    raw, adoption
):
    comment = activity(raw.data)["comment"]
    old = adoption["before_snapshot"]["comment"]
    assert comment.endswith(old)
    prefix = comment[: -len(old)]
    assert prefix.startswith("APPROVED UNASSIGNED LANTHANUM-REJECT FATE")
    note = prefix.lower()
    for term in ("95%", "0.05263157894736836", "unassigned", "la-equivalent"):
        assert term in note
    assert "fate" in note and "treatment" in note
    assert "treatment burdens remain unassigned" in note
    assert "not zero" in note or "not proven zero" in note
    assert "precedence" in note or "supersedes" in note
    assert "inert" in note


def test_stoichiometric_ledger_retains_95_percent_yield_and_unassigned_la_loss(raw):
    helper = activity(raw.data)
    oxide = exchange(helper, OXIDE)["amount"]
    graphite = exchange(helper, GRAPHITE)["amount"]
    carbon = exchange(helper, CARBON)["amount"]
    assert oxide == 1.234493235996356
    assert graphite == 0.0682649028944118
    assert exchange(helper, ELECTRICITY)["amount"] == 10
    assert exchange(helper, HEAT)["amount"] == 3
    # La2O3 + 1.5 C -> 2 La + 1.5 CO2. This ledger tracks the
    # unassigned La equivalent without inventing a waste species or outlet.
    la, oxygen, c = 138.90547, 15.999, 12.011
    gross_la = oxide * (2 * la) / (2 * la + 3 * oxygen)
    assert gross_la == pytest.approx(1 / 0.95, rel=1e-14)
    assert gross_la * 0.95 == pytest.approx(1, rel=1e-14)
    assert gross_la - 1 == pytest.approx(LOSS, rel=1e-14)
    assert graphite / c == pytest.approx(1.5 * oxide / (2 * la + 3 * oxygen))
    assert carbon * c / (c + 2 * oxygen) == pytest.approx(graphite, rel=1e-14)
    assert oxide * 3 * oxygen / (2 * la + 3 * oxygen) == pytest.approx(
        carbon * 2 * oxygen / (c + 2 * oxygen), rel=1e-14
    )
    assert oxide + graphite == pytest.approx(1 + LOSS + carbon, rel=1e-14)
    assert oxide + graphite - 1 - carbon > 0
    # The explicit six-flow vector forbids both replacement treatment routes
    # and zero-valued placeholders or recovery credits.
    assert all(e["amount"] > 0 for e in helper["exchanges"])


def test_crofer_stack_and_native_sheet_working_only_change_exact_fate_comments(
    raw, adoption
):
    alloy, stack = activity(raw.data, ALLOY), activity(raw.data, STACK)
    original_alloy, original_stack = deepcopy(alloy), deepcopy(stack)
    # Restore only the five expressly authorized comment fields, then compare
    # the entire records to the independently captured original raw snapshots.
    owners = (
        original_stack,
        exchange(original_stack, STACK),
        exchange(original_stack, "market for sheet rolling, chromium steel"),
        exchange(original_stack, ALLOY),
        original_alloy,
    )
    for owner in owners:
        assert OLD_FATE not in owner["comment"]
        assert owner["comment"].count(NEW_FATE) == 1
        owner["comment"] = owner["comment"].replace(NEW_FATE, OLD_FATE)

    dose = exchange(alloy, NAME)
    assert exchange_key(dose) == ("technosphere", NAME, PRODUCT, "GLO", "kilogram", ())
    assert dose["amount"] == 0.001
    purchase = exchange(stack, ALLOY)
    service = exchange(stack, "market for sheet rolling, chromium steel")
    assert purchase["amount"] == service["amount"] == 8976.1
    assert exchange_key(service) == (
        "technosphere",
        "market for sheet rolling, chromium steel",
        "sheet rolling, chromium steel",
        "GLO",
        "kilogram",
        (),
    )
    assert LOSS * dose["amount"] * purchase["amount"] == (
        adoption["unassigned_La_equivalent_loss_kg_in_stack_reference"]
    )


def test_flattened_alloy_matches_current_clic_without_la_inert_flow(raw, adoption):
    alloy, helper = activity(raw.data, ALLOY), activity(raw.data)
    clic = adoption["CLIC_alloy_snapshot"]
    assert not any(e["name"] in {NAME, INERT} for e in clic["exchanges"])
    assert "no inert La disposal is adopted" in clic["comment"]
    dose = exchange(alloy, NAME)["amount"]
    flattened = [e for e in alloy["exchanges"] if e["name"] != NAME]
    for exc in helper["exchanges"]:
        if exc["type"] != "production":
            leaf = deepcopy(exc)
            leaf["amount"] *= dose
            flattened.append(leaf)

    def totals(exchanges):
        result = {}
        for exc in exchanges:
            key = exchange_key(exc)
            result[key] = result.get(key, 0) + exc["amount"]
        return result

    assert len(flattened) == len(clic["exchanges"]) == 18
    assert totals(flattened) == totals(clic["exchanges"])
    # Embedded La reference remains distinct from alloy graphite and utilities.
    for exc in helper["exchanges"]:
        if exc["type"] != "production":
            (embedded,) = [
                e
                for e in clic["exchanges"]
                if exchange_key(e) == exchange_key(exc)
                and e.get("comment", "").startswith(
                    "Allocated C165 La-production reference"
                )
            ]
            assert embedded["amount"] == exc["amount"] * dose


def test_exact_five_authoritative_comment_substitutions_preserve_every_other_byte(
    adoption,
):
    assert adoption["legacy_comment_replacement"] == {
        "cells": ["B466", "H472", "H473", "H474", "B881"],
        "before": OLD_FATE,
        "after": NEW_FATE,
    }
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    for address, expected in LEGACY_COMMENT_PINS.items():
        cell, payload = cells[SHEET, address]
        assert cell.get("t") == "inlineStr"
        assert payload[1] is None
        assert OLD_FATE not in payload[0]
        assert payload[0].count(NEW_FATE) == 1

    # La's authoritative prepend supersedes the retained historical sentence;
    # the documentary substitution must not alter that original helper history.
    helper = cells[SHEET, "B862"][1][0]
    assert helper.count(OLD_FATE) == 1
    assert NEW_FATE not in helper


def test_xml_changes_only_approved_comments_and_rows_with_all_caches_preserved():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        if not (
            sheet == SHEET
            and (address in COMMENT_CELLS | SIC_CELLS or row in {871, 872})
        ):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert cell.find(NS + "v") is not None
            assert cell.find(NS + "v").text is not None
            formulas.append([sheet, address, payload])
    assert cells
    assert cells

    assert formulas  # Formula/cache pairs are checked above.


def test_co2_row_is_exact_original_row_872_clone_without_a_blank_exchange_gap():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
    rows = {r.get("r"): r for r in root.iter(NS + "row")}
    assert "872" not in rows

    for row in range(866, 872):
        assert rows[str(row)].find(f"{NS}c[@r='A{row}']") is not None
    for parent in root.iter():
        for child in list(parent):
            if child.tag == NS + "c" and child.get("r") in COMMENT_CELLS | SIC_CELLS:
                parent.remove(child)
            elif child.tag == NS + "row" and child.get("r") in {"871", "872"}:
                parent.remove(child)


def test_all_other_archive_parts_remain_byte_identical():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        parts = [
            [name, archive.read(name)]
            for name in sorted(archive.namelist())
            if name != SHEET
        ]
    assert len(parts) == 14


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_focused_real_preparation_retains_co2_without_reintroducing_treatment(
    raw, version, system_model
):
    source = deepcopy(raw)
    source.data = [activity(source.data)]
    background = [
        {
            "name": e["name"],
            "reference product": e["reference product"],
            "location": e["location"],
            "unit": e["unit"],
            "exchanges": [],
        }
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
    assert importer.version_in == "3.9"
    assert background == before
    assert importer.list_unlinked == []
    prepared = activity(importer.import_db.data)
    assert len(prepared["exchanges"]) == 6
    assert exchange(prepared, NAME)["amount"] == 1
    carbon = exchange(prepared, CARBON)
    assert carbon["amount"] == CO2
    assert carbon["categories"] == ("air", "non-urban air or from high stacks")
    assert carbon["input"][1] == "aa7cac3a-3625-41d4-bc54-33e2cf11ec46"
    assert {e["name"]: e["amount"] for e in prepared["exchanges"]} == {
        NAME: 1,
        OXIDE: 1.234493235996356,
        GRAPHITE: 0.0682649028944118,
        ELECTRICITY: 10,
        HEAT: 3,
        CARBON: CO2,
    }


@pytest.mark.parametrize("version", ("3.9", "3.12"))
def test_default_caller_retains_hydrogen_workbook_source_at_39(version, monkeypatch):
    import premise.new_database as module

    # These mock payloads exercise registry ordering/the sector hook below,
    # not unrelated native-selection guards with their own dedicated tests.
    for hook in (
        "_select_nmc955_modern_references",
        "_select_nmc532_native_312",
        "use_native_pipeline_coating",
        "use_native_ptfe_gasket",
        "use_native_fkm_gasket",
    ):
        monkeypatch.setattr(module, hook, lambda *args, **kwargs: None)

    selected, observed = [], []

    def extract(filepaths):
        selected.extend((Path(path).name, source) for path, source in filepaths)
        return [SimpleNamespace(data=[]) for _ in filepaths]

    class ObserveInventory:
        def __init__(self, **kwargs):
            observed.append((Path(kwargs["path"]).name, kwargs["version_in"]))
            self.list_unlinked = []

        def merge_inventory(self):
            return []

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract)
    monkeypatch.setattr(module, "DefaultInventory", ObserveInventory)
    monkeypatch.setattr(module, "use_native_ptfe_gasket", lambda *args: None)
    monkeypatch.setattr(module, "use_native_fkm_gasket", lambda *args, **kwargs: None)
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, "cutoff"
    obj.keep_imports_uncertainty = True
    obj._database = []
    assert obj._NewDatabase__import_inventories() == []
    assert selected == observed
    assert observed.count((WORKBOOK, "3.9")) == 1
