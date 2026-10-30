"""ALIGN-091: fixed auxiliary cooling and assembly along the capacity route."""

import json
from copy import deepcopy

import pytest
from bw2io import ExcelImporter
from delegated_source_history import (
    SUCCESSORS,
    reviewed_excel_importer,
    reviewed_intermediate_path,
    reviewed_zipfile,
)
from test_delegated_inventory_source_cache import (
    test_actual_battery_source_content_invalidates_imports_and_checkpoint_only as check_cache,
)

from premise.filesystem_constants import INVENTORY_DIR

WORKBOOK = "lci-batteries-LiO2.xlsx"
AUX = "battery auxiliaries supply and end-of-life treatment, lithium-oxygen"
COOLER = "cooling system production, for Li-O2 battery"
CENTRAL = "battery central control and protection unit production"
ELECTRICITY = "market group for electricity, medium voltage"


def activity(data, name):
    (record,) = [r for r in data if r["name"] == name]
    return record


def exchange(record, name):
    (item,) = [e for e in record["exchanges"] if e["name"] == name]
    return item


@pytest.fixture(scope="module")
def inventories():
    entry = json.loads(SUCCESSORS.read_text())[WORKBOOK]
    predecessor = SUCCESSORS.parent / entry["predecessor_fixture"]
    # This approval's 33 unchanged-record oracle describes ALIGN-091, before
    # the separately reviewed freight additions. Authenticate live bytes first.
    source = reviewed_intermediate_path(INVENTORY_DIR / WORKBOOK)

    return ExcelImporter(predecessor).data, ExcelImporter(source).data


def test_only_auxiliary_changes_and_old_full_battery_and_33_other_records_exact(
    inventories,
):
    old, new = inventories
    assert len(old) == len(new) == 34
    for before, after in zip(old, new):
        if before["name"] != AUX:
            assert before == after
    assert activity(old, "battery production, Li-O2") == activity(
        new, "battery production, Li-O2"
    )
    before, after = activity(old, AUX), activity(new, AUX)
    restored = deepcopy(after)
    restored["comment"] = before["comment"]
    old_cooler = next(e for e in before["exchanges"] if e["amount"] == 29.07)
    new_cooler = exchange(restored, COOLER)
    for key in ("name", "location", "reference product"):
        new_cooler[key] = old_cooler[key]
    restored["exchanges"].remove(exchange(restored, ELECTRICITY))
    assert restored == before
    assert after["comment"].startswith("APPROVED ALIGN-091") and after[
        "comment"
    ].endswith(before["comment"])


def test_cooling_fixed_mass_assembly_once_and_retirement_control_allowance(inventories):
    _, data = inventories
    aux = activity(data, AUX)
    cooler = exchange(aux, COOLER)
    assert (
        cooler["amount"],
        cooler["location"],
        cooler["reference product"],
        cooler["unit"],
    ) == (29.07, "RER", "cooling system", "kilogram")
    electricity = exchange(aux, ELECTRICITY)
    assert electricity["amount"] == 0.41 / 3.6
    assert (
        electricity["location"],
        electricity["reference product"],
        electricity["unit"],
    ) == ("US", "electricity, medium voltage", "kilowatt hour")
    assert exchange(aux, "treatment of LiO2 battery, for recycling")["amount"] == -88.07
    assert exchange(aux, CENTRAL)["amount"] * 4.4928 == pytest.approx(4)
    assert exchange(aux, "oxygen tank production, for Li-O2 battery")["amount"] == 55
    capacity = ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data
    purchases = [
        e
        for r in capacity
        for e in r["exchanges"]
        if e["type"] == "technosphere" and e["name"] == AUX
    ]
    assert len(purchases) == 1
    assert purchases[0]["amount"] == 1 / 63.5
    assert purchases[0]["amount"] * electricity["amount"] * 63.5 == pytest.approx(
        0.41 / 3.6
    )


def test_historical_initial_import_and_xml_views_are_exact_and_unknowns_fail(
    tmp_path, inventories
):
    before, _ = inventories
    source = INVENTORY_DIR / WORKBOOK
    assert reviewed_excel_importer(source).data == before
    entry = json.loads(SUCCESSORS.read_text())[WORKBOOK]
    predecessor = SUCCESSORS.parent / entry["predecessor_fixture"]

    with reviewed_zipfile(source) as old_view, reviewed_zipfile(predecessor) as pinned:
        assert old_view.namelist() == pinned.namelist()
        assert all(old_view.read(n) == pinned.read(n) for n in pinned.namelist())


@pytest.mark.parametrize("version", ["3.8", "3.12"])
def test_auxiliary_assembly_coefficient_invalidates_only_import_cache(
    version, tmp_path, monkeypatch
):
    check_cache(version, "FILEPATH_LIO2_BATTERY", "B607", tmp_path, monkeypatch)
