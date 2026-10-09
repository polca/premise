"""Curation checks for exact underlying stock values and explicit missingness."""

import csv
from zipfile import ZipFile
from xml.etree import ElementTree as ET

import pytest

from dev.stock_vintage.curate_observations import NS, uk_articulated_trucks, uk_vehicles


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


def truck_fixture(path, *, unknown="7", duplicate=False):
    keys = [
        "Geography",
        "TaxClass",
        "WheelPlan",
        "WheelPlanDetailed",
        "BodyTypeDetailed",
        "RoadUsing",
        "MaximumGrossWeightBand",
        "YearFirstUsed",
        "Fuel",
        "2022",
    ]
    base = [
        "United Kingdom",
        "Goods",
        "Articulated",
        "Articulated: 2 and 3 axle",
        "Tractor",
        "1",
        "I: Over 32 and up to 40 tonnes",
        "2020",
        "Diesel",
        "100",
    ]
    rows = [base]
    for year, value in [("Before 1980", "2"), ("Unknown", unknown), ("2023", "[z]")]:
        rows.append(base[:7] + [year, "Diesel", value])
    rows.append(["England"] + base[1:])  # Overlapping geography must not be added.
    rows.append(base[:6] + ["J: Over 40 and up to 44 tonnes"] + base[7:])
    if duplicate:
        rows.append(base)
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(keys)
        writer.writerows(rows)


def test_truck_subgroup_retains_open_and_unknown_bins_without_geographic_overlap(
    tmp_path,
):
    path = tmp_path / "trucks.csv"
    truck_fixture(path)
    (row,) = uk_articulated_trucks(path, years=[2022])
    assert row["total_stock"] == 109
    assert row["unknown_stock"] == 7
    assert row["cohorts"] == [{"cohort_year": 2020, "stock": 100}]
    assert row["unresolved_bins"][0]["stock"] == 2
    assert row["unresolved_bins"][0]["start_year"] is None


@pytest.mark.parametrize(
    "option,match", [("duplicate", "Duplicate"), ("unknown", "Unresolved")]
)
def test_truck_ambiguous_rows_and_suppressed_counts_fail(tmp_path, option, match):
    path = tmp_path / "trucks.csv"
    truck_fixture(
        path, **({"duplicate": True} if option == "duplicate" else {"unknown": "[c]"})
    )
    with pytest.raises(ValueError, match=match):
        uk_articulated_trucks(path, years=[2022])
