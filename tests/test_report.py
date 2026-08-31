from types import SimpleNamespace

import openpyxl
import xarray as xr

from premise.report import (
    convert_log_to_excel_file,
    fetch_columns,
    fetch_data,
    generate_summary_report,
)


def make_heat_layer(variable, values):
    return xr.DataArray(
        [[[value for value in values]]],
        dims=("variables", "region", "year"),
        coords={
            "variables": [variable],
            "region": ["World"],
            "year": [2020, 2030],
        },
    )


def test_fetch_secondary_heat_uses_explicit_layer():
    secondary = make_heat_layer("heat, secondary, from natural gas", [1.0, 2.0])
    iam_data = SimpleNamespace(
        secondary_heat_supply=secondary,
        production_volumes=make_heat_layer("unrelated", [9.0, 9.0]),
    )

    result = fetch_data(
        iam_data,
        "Heat (secondary) - generation",
        ["heat, secondary, from natural gas"],
    )

    assert result.identical(secondary)


def test_summary_report_contains_all_three_heat_sheets(tmp_path):
    iam_data = SimpleNamespace(
        buildings_heat_end_use=make_heat_layer(
            "heat, buildings, from natural gas boiler", [1.0, 1.5]
        ),
        industrial_heat_end_use=make_heat_layer(
            "heat, industrial, from natural gas boiler", [2.0, 2.5]
        ),
        secondary_heat_supply=make_heat_layer(
            "heat, secondary, from natural gas", [3.0, 3.5]
        ),
    )
    destination = tmp_path / "scenario_report.xlsx"

    generate_summary_report(
        [
            {
                "model": "message",
                "pathway": "test",
                "iam data": iam_data,
            }
        ],
        destination,
        with_charts=False,
    )

    workbook = openpyxl.load_workbook(destination, read_only=True, data_only=True)
    assert {
        "Heat (buildings) - generation",
        "Heat (industrial) - generation",
        "Heat (secondary) - generation",
    }.issubset(workbook.sheetnames)


def test_fuel_change_report_preserves_ammonia_and_onsite_shares(tmp_path):
    columns = fetch_columns("premise_fuel")
    contiguous_share_columns = [
        "hydrogen distribution compressed gaseous truck",
        "hydrogen distribution compressed gaseous pipeline",
        "hydrogen distribution liquid truck",
        "hydrogen distribution liquid ammonia ship",
        "hydrogen distribution liquid hydrogen ship",
        "hydrogen on-site production",
    ]
    first_share_column = columns.index(contiguous_share_columns[0])
    assert columns[
        first_share_column : first_share_column + len(contiguous_share_columns)
    ] == contiguous_share_columns

    log_values = [""] * len(columns)
    log_values[
        columns.index("hydrogen distribution liquid ammonia ship")
    ] = "0.3"
    log_values[columns.index("hydrogen on-site production")] = "0.2"

    log_filepath = tmp_path / "premise_fuel.log"
    log_filepath.write_text("|".join(log_values), encoding="utf-8")

    report = convert_log_to_excel_file(log_filepath)

    assert report.loc[
        0, "hydrogen distribution liquid ammonia ship"
    ] == 0.3
    assert report.loc[0, "hydrogen on-site production"] == 0.2
