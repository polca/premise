"""ALIGN-043: explicit water for the dry ZnCl2 batch, with old flows retained."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-batteries-organic-and-hybrid-redox-flow.xlsx"
NAME = "zinc chloride (ZnCl2) production, electroactive RFBs"
WATER = "market for water, deionised"
ACID = "market for hydrochloric acid, without water, in 30% solution state"


NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


def activity(data):
    (match,) = [d for d in data if d["name"] == NAME]
    return match


def exchange(dataset, name):
    (match,) = [e for e in dataset["exchanges"] if e["name"] == name]
    return match


def science(dataset):
    values = []
    for e in dataset["exchanges"]:
        categories = e.get("categories", ())
        if isinstance(categories, str):
            categories = categories.split("::")
        values.append(
            [
                e["type"],
                e["name"],
                e.get("reference product"),
                e.get("location"),
                e["unit"],
                e["amount"],
                list(categories),
            ]
        )
    return sorted(values)


def worksheet():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    return root, parts


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (
            ROOT
            / "tests/data/inventory_review/zinc-chloride-water-adoption-20261027.json"
        ).read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


def test_independent_before_clic_and_original_paired39_profiles(adoption):
    assert adoption["case_id"] == "ALIGN-043"
    assert adoption["workbook"] == WORKBOOK

    assert (adoption["CLIC_dataset_id"], adoption["CLIC_version_id"]) == (1349, 4550)

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["formula_reference_audit"] == {
        "formula_count": 17,
        "dependent_formula_cells": [],
    }


def test_actual_source_retains_all_nine_old_flows_and_matches_complete_clic_science(
    raw, adoption
):
    current = activity(raw.data)
    expected = deepcopy(adoption["before_snapshot"])
    expected["exchanges"].append(
        {
            "name": WATER,
            "reference product": "water, deionised",
            "amount": 1.7013733333333334,
            "database": "cutoff39",
            "location": "Europe without Switzerland",
            "unit": "kilogram",
            "type": "technosphere",
        }
    )
    assert expected == adoption["after_snapshot"]
    assert len(current["exchanges"]) == 10
    assert current["exchanges"][:9] == adoption["before_snapshot"]["exchanges"]
    assert {k: v for k, v in current.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert science(current) == science(adoption["CLIC_snapshot"])
    assert exchange(current, NAME)["amount"] == 1.363


def test_carrier_and_evaporation_use_same_batch_basis_without_second_outlet_or_credit(
    raw,
):
    current = activity(raw.data)
    water, acid, emitted = (
        exchange(current, WATER),
        exchange(current, ACID),
        exchange(current, "Water"),
    )
    assert water["amount"] == pytest.approx(acid["amount"] * (1 / 0.30 - 1), rel=1e-15)
    assert acid["amount"] == 0.72916
    assert water["amount"] == pytest.approx(emitted["amount"] * 1000, rel=1e-15)
    assert (emitted["amount"], emitted["unit"], emitted["categories"]) == (
        0.0017013733333333333,
        "cubic meter",
        "air",
    )
    assert (water["location"], water["unit"], water["type"]) == (
        "Europe without Switzerland",
        "kilogram",
        "technosphere",
    )
    assert exchange(current, "Hydrogen")["amount"] == 0.02
    assert (
        exchange(current, "heat production, natural gas, at industrial furnace >100kW")[
            "amount"
        ]
        == 8.189967723809524
    )
    assert (
        exchange(current, "market group for electricity, medium voltage")["amount"]
        == 0.1363
    )
    assert all(e["amount"] > 0 for e in current["exchanges"])


def test_authoritative_note_supersedes_old_carrier_claim_without_inventing_closure(
    raw, adoption
):
    old = adoption["before_snapshot"]["comment"]
    comment = activity(raw.data)["comment"]
    assert comment.endswith(old)
    prefix = comment[: -len(old)]
    assert prefix.startswith("APPROVED DRY ZINC-CHLORIDE CARRIER WATER")
    for term in (
        "pure-equivalent acid input does not replace",
        "separately purchased carrier water",
        "earlier assertion",
        "superseded",
        "liquid-volume-equivalent evaporation accounting",
        "qualified engineering assumptions",
        "not measured industrial assay",
        "without invented closure or duplicate wastewater",
        "both remain 3.9",
    ):
        assert term in prefix


def test_other_52_activities_and_all_zinc_consumers_remain_exactly_unchanged(
    raw, adoption
):
    others = [d for d in raw.data if d["name"] != NAME]
    assert len(raw.data) == 53 and len(others) == 52

    assert adoption["activity_count"] == 53
    assert any(
        e["type"] == "technosphere" and e["name"] == NAME
        for d in others
        for e in d["exchanges"]
    )


def test_exact_xml_note_plus_added_row_and_extent_preserve_all_other_payloads_and_caches(
    raw, adoption
):
    root, parts = worksheet()
    assert (
        root.find(NS + "dimension").get("ref")
        == adoption["after_dimension"]
        == "A1:N1390"
    )
    data = root.find(NS + "sheetData")
    (row,) = [r for r in data if r.get("r") == "1390"]
    cells = {c.get("r"): c for c in row}
    expected = {
        "A1390": WATER,
        "B1390": "1.7013733333333334",
        "C1390": "Europe without Switzerland",
        "D1390": "kilogram",
        "F1390": "technosphere",
        "G1390": "cutoff39",
        "H1390": "water, deionised",
    }
    assert set(cells) == set(expected)
    for address, value in expected.items():
        cell = cells[address]
        assert cell.attrib == (
            {"r": address} if address == "B1390" else {"r": address, "t": "inlineStr"}
        )
        assert "".join(cell.itertext()) == value
    formulas = [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in root.iter(NS + "c")
        if c.find(NS + "f") is not None
    ]
    assert len(formulas) == 17
    assert all(
        c.find(NS + "v").text is not None
        for c in root.iter(NS + "c")
        if c.find(NS + "f") is not None
    )
    assert len(parts) == 12
    assert len(list(root.iter(NS + "c"))) == 6941 + 7
    # Guard the actual present recipe before exposing an independent old XML
    # view. No extra cells/rows are masked; the complete tree must match before.
    assert science(activity(raw.data)) == science(adoption["CLIC_snapshot"])
    text = root.find(f".//{NS}c[@r='B1377']/{NS}is/{NS}t")
    assert text.text == activity(raw.data)["comment"]
    assert text.text.endswith(adoption["before_snapshot"]["comment"])
    text.text = adoption["before_snapshot"]["comment"]
    data.remove(row)
    root.find(NS + "dimension").set("ref", "A1:N1389")


@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real39_preparation_retains_batch_water_and_original_biosphere_flows(
    raw, adoption, system_model
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
    prepared = activity(importer.import_db.data)
    assert len(prepared["exchanges"]) == 10
    assert science(prepared) == science(adoption["CLIC_snapshot"])
    assert exchange(prepared, NAME)["amount"] == 1.363
    for name in ("Water", "Hydrogen"):
        emitted = exchange(prepared, name)
        assert emitted["input"][0] == "biosphere3" and emitted["categories"] == ("air",)
