"""Curation checks for exact underlying stock values and explicit missingness."""

from zipfile import ZipFile
from xml.etree import ElementTree as ET

import pytest

from dev.stock_vintage.curate_observations import NS, uk_vehicles


def fixture_ods(path, *, total="1.008", missing_low=False):
    table = f"{{{NS['table']}}}"
    office = f"{{{NS['office']}}}"
    root = ET.Element("document")
    sheet = ET.SubElement(root, table + "table", {table + "name": "VEH1111"})
    header = [
        "Geography [note 1]",
        "Date",
        "Units",
        "BodyType",
        "Fuel",
        "2023",
        "2022",
        "2021",
        "1900",
        "Unknown [note 2]",
        "Total",
    ]
    values = [
        "United Kingdom",
        "2022 Q4 (end December)",
        "Thousands",
        "Cars",
        "BATTERY ELECTRIC",
        "[z]",
        "0.7",
        "0.3",
        "[low]",
        "[low]",
        "1.0",
    ]
    raws = [None] * 6 + [
        "0.7",
        "0.278",
        None if missing_low else "0.023",
        "0.007",
        total,
    ]
    for displays, numbers in ((header, [None] * len(header)), (values, raws)):
        row = ET.SubElement(sheet, table + "table-row")
        for display, raw in zip(displays, numbers):
            attrs = (
                {}
                if raw is None
                else {office + "value": raw, office + "value-type": "float"}
            )
            cell = ET.SubElement(row, table + "table-cell", attrs)
            ET.SubElement(cell, "p").text = display
        ET.SubElement(
            row, table + "table-cell", {table + "number-columns-repeated": "16373"}
        )
    with ZipFile(path, "w") as archive:
        archive.writestr("content.xml", ET.tostring(root))


def test_numeric_counts_and_unknowns_survive_display_rounding(tmp_path):
    path = tmp_path / "sample.ods"
    fixture_ods(path)
    (row,) = uk_vehicles(path)
    assert row["total_stock"] == 1008
    assert row["unknown_stock"] == 7
    assert row["stock_balance_residual"] == 0
    assert row["cohorts"] == [
        {"cohort_year": 1900, "stock": 23},
        {"cohort_year": 2021, "stock": 278},
        {"cohort_year": 2022, "stock": 700},
    ]
    assert row["displayed_low_stock_retained"] == 23


def test_unreconciled_stock_is_not_silently_normalised(tmp_path):
    path = tmp_path / "sample.ods"
    fixture_ods(path, total="1.010")
    with pytest.raises(ValueError, match="does not reconcile"):
        uk_vehicles(path)


def test_suppressed_numeric_value_is_not_interpreted_as_zero(tmp_path):
    path = tmp_path / "sample.ods"
    fixture_ods(path, missing_low=True)
    with pytest.raises(ValueError, match="Missing numeric"):
        uk_vehicles(path)
