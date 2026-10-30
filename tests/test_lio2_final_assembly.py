"""ALIGN-045: final complete-system assembly electricity, once per 333.85 kg."""

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
from test_central_fastener_alignment import (
    previous_fastener_source,
    previous_fastener_xml,
)
from test_lio2_cooling_alignment import previous_cooling_source, previous_cooling_xml

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-LiO2.xlsx"
NAME = "battery production, Li-O2"
ELECTRICITY = "market group for electricity, medium voltage"
TREATMENT = "treatment of LiO2 battery, for recycling"
DOSE = 0.00034113790291714506


NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


def activity(data):
    (match,) = [d for d in data if d["name"] == NAME]
    return match


def exchange(record, name):
    (match,) = [e for e in record["exchanges"] if e["name"] == name]
    return match


def vector(e):
    return tuple(
        e[k]
        for k in ("type", "name", "reference product", "location", "unit", "amount")
    )


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (ROOT / "scripts/data/lio2-final-assembly-adoption-20261027.json").read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return previous_cooling_source(
        previous_fastener_source(ExcelImporter(INVENTORY_DIR / WORKBOOK), WORKBOOK)
    )


def test_independent_before_clic_and_original_paired39_profiles(adoption):
    assert adoption["case_id"] == "ALIGN-045" and adoption["workbook"] == WORKBOOK

    assert (adoption["CLIC_dataset_id"], adoption["CLIC_version_id"]) == (1142, 5675)

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["formula_reference_audit"] == {
        "formula_count": 175,
        "dependent_formula_cells": [],
    }
    assert (
        adoption["remaining_cooling_and_control_differences_outside_this_scope"] is True
    )


def test_actual_recipe_preserves_all_eight_inputs_and_matches_only_reviewed_clic_electricity(
    raw, adoption
):
    actual = activity(raw.data)
    expected = deepcopy(adoption["before_snapshot"])
    expected["exchanges"].append(
        {
            "name": ELECTRICITY,
            "reference product": "electricity, medium voltage",
            "location": "US",
            "unit": "kilowatt hour",
            "amount": DOSE,
            "type": "technosphere",
        }
    )
    assert expected == adoption["after_snapshot"]
    assert len(actual["exchanges"]) == 9
    assert actual["exchanges"][:8] == adoption["before_snapshot"]["exchanges"]
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert vector(exchange(actual, ELECTRICITY)) == vector(
        exchange(adoption["CLIC_snapshot"], ELECTRICITY)
    )
    assert actual["production amount"] == exchange(actual, NAME)["amount"] == 1
    assert exchange(actual, TREATMENT)["amount"] == 0


def test_mj_to_kwh_conversion_is_applied_once_per_complete_system_mass(raw):
    amount = exchange(activity(raw.data), ELECTRICITY)["amount"]
    component_sum = sum((182.6, 19.51, 43.67, 55, 4, 29.07))
    assert component_sum == pytest.approx(333.85, abs=1e-12)
    assert amount == DOSE
    assert amount == pytest.approx(0.41 / 3.6 / 333.85, rel=1e-15)
    assert amount * 333.85 * 3.6 == pytest.approx(0.41, rel=1e-15)
    assert amount != pytest.approx(0.41 / 3.6 / 321, rel=1e-6)
    assert amount != pytest.approx(0.41 / 3.6 / 267, rel=1e-6)
    assert amount != pytest.approx(0.41 / 3.6 / 63.5, rel=1e-6)


def test_note_qualifies_system_boundary_and_unresolved_clic_differences(raw, adoption):
    old = adoption["before_snapshot"]["comment"]
    comment = activity(raw.data)["comment"]
    assert comment.endswith(old)
    prefix = comment[: -len(old)]
    assert prefix.startswith("APPROVED Li-O2 FINAL-ASSEMBLY ELECTRICITY")
    for term in (
        "once per complete 333.85 kg",
        "source US medium-voltage",
        "separate from existing cell processing and casing manufacture",
        "per delivered/lifetime kWh",
        "not a resolution of conflicting 321 kg",
        "zero duplicate-retirement fragment",
        "Cooling and control differences",
        "does not establish whole-recipe equivalence",
        "both remain 3.9",
    ):
        assert term in prefix


def test_other33_upstream_records_and_all_consumers_are_exactly_unchanged(raw):
    others = [d for d in raw.data if d["name"] != NAME]
    assert len(raw.data) == 34 and len(others) == 33

    assert any(
        e["type"] == "technosphere" and e["name"] == NAME
        for d in others
        for e in d["exchanges"]
    )


def test_exact_row285_formula_cache_and_note_preserve_all175_old_caches_and_other_parts(
    raw, adoption
):
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = previous_cooling_xml(
            previous_fastener_xml(ET.fromstring(archive.read(SHEET)), WORKBOOK)
        )
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    assert root.find(NS + "dimension").get("ref") == "A1:T637"
    data = root.find(NS + "sheetData")
    (row,) = [r for r in data if r.get("r") == "285"]
    cells = {c.get("r"): c for c in row}
    expected = {
        "A285": ELECTRICITY,
        "C285": "kilowatt hour",
        "E285": "US",
        "F285": "technosphere",
        "G285": "electricity, medium voltage",
    }
    assert set(cells) == {*expected, "B285"}
    for address, value in expected.items():
        assert cells[address].attrib == {"r": address, "t": "inlineStr"}
        assert "".join(cells[address].itertext()) == value
    assert cells["B285"].attrib == {"r": "B285"}
    assert cells["B285"].find(NS + "f").text == "0.41/3.6/333.85"
    assert cells["B285"].find(NS + "v").text == str(DOSE)
    formulas = [c for c in root.iter(NS + "c") if c.find(NS + "f") is not None]
    assert len(formulas) == 176
    old = [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in formulas
        if c.get("r") != "B285"
    ]
    assert len(old) == 175
    assert all(c.find(NS + "v").text is not None for c in formulas)
    assert len(parts) == 12
    # Guard reviewed present science before restoring the bounded old XML view.
    assert vector(exchange(activity(raw.data), ELECTRICITY)) == vector(
        exchange(adoption["CLIC_snapshot"], ELECTRICITY)
    )
    text = root.find(f".//{NS}c[@r='B269']/{NS}is/{NS}t")
    assert text.text == activity(raw.data)["comment"]
    assert text.text.endswith(adoption["before_snapshot"]["comment"])
    text.text = adoption["before_snapshot"]["comment"]
    data.remove(row)


@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real39_preparation_preserves_new_us_service_and_zero_retirement_fragment(
    raw, system_model
):
    source = deepcopy(raw)
    source.data = [deepcopy(activity(raw.data))]
    fields = ("name", "reference product", "location", "unit")
    identities = {
        tuple(e[k] for k in fields)
        for e in source.data[0]["exchanges"]
        if e["type"] == "technosphere"
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
    actual = activity(importer.import_db.data)
    assert len(actual["exchanges"]) == 9
    assert [vector(e) for e in actual["exchanges"]] == [
        vector(e) for e in activity(raw.data)["exchanges"]
    ]
    assert exchange(actual, TREATMENT)["amount"] == 0
    assert vector(exchange(actual, ELECTRICITY)) == (
        "technosphere",
        ELECTRICITY,
        "electricity, medium voltage",
        "US",
        "kilowatt hour",
        DOSE,
    )
