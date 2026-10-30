"""ALIGN-092: retain complete-pack freight along the split capacity path."""

import json
from copy import deepcopy

import delegated_source_history as history
import pytest
from bw2io import ExcelImporter
from delegated_source_history import SUCCESSORS, reviewed_intermediate_path
from test_delegated_inventory_source_cache import (
    test_actual_battery_source_content_invalidates_imports_and_checkpoint_only as check_cache,
)

from premise.filesystem_constants import INVENTORY_DIR

WORKBOOK = "lci-batteries-LiO2.xlsx"


CELL = "market for battery cell, lithium-oxygen, including end-of-life treatment"
FIXED = "battery pack component production and end-of-life treatment, lithium-oxygen, without cells and controls"
FULL = "market for battery, Li-oxygen, Li-O2"
AUX = "battery auxiliaries supply and end-of-life treatment, lithium-oxygen"


def activity(data, name):
    (record,) = [r for r in data if r["name"] == name]
    return record


def key(exchange):
    return tuple(
        exchange.get(k)
        for k in ("name", "reference product", "location", "unit", "type")
    )


def freight(record):
    return [
        e
        for e in record["exchanges"]
        if e["type"] == "technosphere" and e["unit"] == "ton kilometer"
    ]


@pytest.fixture(scope="module")
def inventories():
    source = INVENTORY_DIR / WORKBOOK
    json.loads(SUCCESSORS.read_text())[WORKBOOK]

    previous = reviewed_intermediate_path(source)
    return ExcelImporter(previous).data, ExcelImporter(source).data


def test_exact_eight_append_only_exchanges_preserve_all_other_science(inventories):
    before, after = inventories
    assert len(before) == len(after) == 34
    added = 0
    for old, new in zip(before, after):
        if old["name"] not in (CELL, FIXED):
            assert old == new
            continue
        assert not freight(old)
        assert len(freight(new)) == 4
        assert new["exchanges"][: len(old["exchanges"])] == old["exchanges"]
        assert all(e in freight(new) for e in new["exchanges"][len(old["exchanges"]) :])
        restored = deepcopy(new)
        restored["exchanges"] = restored["exchanges"][: len(old["exchanges"])]
        assert restored == old
        added += len(new["exchanges"]) - len(old["exchanges"])
    assert added == 8
    assert activity(before, AUX) == activity(after, AUX)


def test_four_finished_pack_freight_identities_and_baseline_preserved(inventories):
    _, data = inventories
    full = {key(e): e for e in freight(activity(data, FULL))}
    cell = {key(e): e for e in freight(activity(data, CELL))}
    fixed = {key(e): e for e in freight(activity(data, FIXED))}
    assert len(full) == len(cell) == len(fixed) == 4
    assert full.keys() == cell.keys() == fixed.keys()
    for identity, baseline in full.items():
        assert cell[identity]["amount"] == baseline["amount"]
        assert fixed[identity]["amount"] == pytest.approx(
            baseline["amount"] * 151.25 / 63.5, rel=1e-14
        )
        for added in (cell[identity], fixed[identity]):
            assert added["product"] == baseline["reference product"]
            assert "distribution boundary" in added["comment"]
            restored = deepcopy(added)
            del restored["product"]
            del restored["comment"]
            restored["amount"] = baseline["amount"]
            assert restored == baseline
    # These are finished-pack distribution legs; existing supplier-internal
    # freight remains untouched, and auxiliaries do not buy these legs again.
    assert not freight(activity(data, AUX))


@pytest.mark.parametrize("factor", [0.5, 1, 2])
def test_cell_learning_scales_only_cell_freight_with_fixed_noncell_partition(
    inventories, factor
):
    _, data = inventories
    capacity = ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data
    buyers = [
        r
        for r in capacity
        if any(
            e["type"] == "technosphere" and e["name"] == CELL for e in r["exchanges"]
        )
    ]
    assert len(buyers) == 1
    purchases = {
        e["name"]: e for e in buyers[0]["exchanges"] if e["type"] == "technosphere"
    }
    assert purchases[CELL]["amount"] == pytest.approx(182.6 / 63.5, rel=1e-14)
    assert purchases[FIXED]["amount"] == 1
    assert purchases[AUX]["amount"] == 1 / 63.5
    cell = {key(e): e for e in freight(activity(data, CELL))}
    fixed = {key(e): e for e in freight(activity(data, FIXED))}
    full = {key(e): e for e in freight(activity(data, FULL))}
    for identity, baseline in full.items():
        observed = (
            factor * purchases[CELL]["amount"] * cell[identity]["amount"]
            + purchases[FIXED]["amount"] * fixed[identity]["amount"]
        )
        expected = baseline["amount"] * (factor * 182.6 + 151.25) / 63.5
        assert observed == pytest.approx(expected, rel=1e-14)
        if factor == 1:
            assert observed == pytest.approx(
                baseline["amount"] * 333.85 / 63.5, rel=1e-14
            )


def test_intermediate_view_checks_component_science_and_rejects_missing_stage(
    inventories, tmp_path, monkeypatch
):
    from zipfile import BadZipFile

    source = INVENTORY_DIR / WORKBOOK
    with pytest.raises(ValueError):
        reviewed_intermediate_path(source, "nonexistent stage")
    entries = json.loads(SUCCESSORS.read_text())
    view = entries[WORKBOOK]["intermediate_views"][0]
    fixture = tmp_path / "broken-intermediate.xlsx"
    fixture.write_bytes(b"not a workbook")
    view["fixture"] = fixture.name
    descriptor = tmp_path / SUCCESSORS.name
    # Keep the predecessor path resolvable when relocating this descriptor.
    entries[WORKBOOK]["predecessor_fixture"] = str(
        SUCCESSORS.parent / entries[WORKBOOK]["predecessor_fixture"]
    )
    descriptor.write_text(json.dumps(entries))
    monkeypatch.setattr(history, "SUCCESSORS", descriptor)
    with pytest.raises(BadZipFile):
        reviewed_intermediate_path(source)


@pytest.mark.parametrize("version", ["3.8", "3.12"])
@pytest.mark.parametrize("address", ["B622", "B642"])
def test_finished_pack_freight_coefficients_invalidate_import_cache(
    version, address, tmp_path, monkeypatch
):
    # Numeric address of the first appended cell-freight coefficient; guarded
    # by the independent row-map test once the approved candidate is installed.
    check_cache(version, "FILEPATH_LIO2_BATTERY", address, tmp_path, monkeypatch)


def test_append_rows_dimensions_products_and_formula_caches_match_pinned_091(
    inventories,
):
    import re
    from xml.etree import ElementTree as ET
    from zipfile import ZipFile

    source = INVENTORY_DIR / WORKBOOK
    previous = reviewed_intermediate_path(source)
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    sheet = "xl/worksheets/sheet1.xml"
    with ZipFile(previous) as before, ZipFile(source) as after:
        assert before.namelist() == after.namelist()
        assert all(
            before.read(n) == after.read(n) for n in before.namelist() if n != sheet
        )
        old, new = ET.fromstring(before.read(sheet)), ET.fromstring(after.read(sheet))
    assert old.find(ns + "dimension").get("ref") == "A1:T637"
    assert new.find(ns + "dimension").get("ref") == "A1:T645"
    for row in [*range(622, 626), *range(642, 646)]:
        element = new.find(f".//{ns}row[@r='{row}']")
        assert element is not None
        product = element.find(f"{ns}c[@r='I{row}']")
        reference = element.find(f"{ns}c[@r='G{row}']")
        assert "".join(product.find(ns + "is").itertext()) == "".join(
            reference.find(ns + "is").itertext()
        )
        amount = element.find(f"{ns}c[@r='B{row}']")
        assert amount.find(ns + "f") is None and float(amount.find(ns + "v").text) > 0
    old_formulas = [c for c in old.iter(ns + "c") if c.find(ns + "f") is not None]
    new_formulas = [c for c in new.iter(ns + "c") if c.find(ns + "f") is not None]
    assert len(old_formulas) == len(new_formulas) == 176
    for cell in old_formulas:
        column, number = re.fullmatch(r"([A-Z]+)(\d+)", cell.get("r")).groups()
        number = int(number)
        address = column + str(number + (4 if number >= 624 else 0))
        actual = new.find(f".//{ns}c[@r='{address}']")
        for tag in ("f", "v"):
            assert ET.tostring(cell.find(ns + tag)) == ET.tostring(
                actual.find(ns + tag)
            )
