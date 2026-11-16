from types import SimpleNamespace

import openpyxl
import pytest
import xarray as xr

from premise.report import fetch_data, generate_summary_report


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


@pytest.mark.parametrize(
    "sector,attribute",
    [
        ("Heat (buildings) - generation", "buildings_heat_end_use"),
        ("Heat (industrial) - generation", "industrial_heat_end_use"),
        ("Heat (secondary) - generation", "secondary_heat_supply"),
    ],
)
def test_heat_fallback_requires_an_absent_layer(sector, attribute):
    production = make_heat_layer("heat", [1.0, 2.0])
    iam_data = SimpleNamespace(production_volumes=production)

    assert fetch_data(iam_data, sector, ["heat"]).identical(production)

    setattr(iam_data, attribute, None)
    assert fetch_data(iam_data, sector, ["heat"]) is None


@pytest.mark.parametrize(
    "sector,gate",
    [
        ("Electricity - generation", "electricity_mix"),
        ("Transport (cars)", "passenger_car_fleet"),
    ],
)
def test_generation_availability_checks_presence(sector, gate):
    production = make_heat_layer("production", [1.0, 2.0])
    iam_data = SimpleNamespace(production_volumes=production)

    assert fetch_data(iam_data, sector, ["production"]) is None

    setattr(iam_data, gate, None)
    assert fetch_data(iam_data, sector, ["production"]).identical(production)


@pytest.mark.parametrize("kind", ["mobile", "stationary"])
def test_battery_selection_renames_chemistry_without_changing_source(kind):
    battery = make_heat_layer("LFP", [1.0, 2.0]).rename({"variables": "chemistry"})
    iam_data = SimpleNamespace(**{f"battery_{kind}_scenarios": battery})

    result = fetch_data(iam_data, f"Battery ({kind})", ["LFP"])

    assert result.identical(battery.rename({"chemistry": "variables"}))
    assert "chemistry" in battery.dims


@pytest.mark.parametrize(
    "sector,attribute",
    [
        ("CDR - energy use", "cdr_energy_use"),
        ("CDR - efficiency", "cdr_technology_efficiencies"),
    ],
)
def test_cdr_keeps_all_variables_only_when_no_requested_variable_matches(
    sector, attribute
):
    cdr = xr.concat(
        [make_heat_layer("first", [1.0, 2.0]), make_heat_layer("second", [3.0, 4.0])],
        dim="variables",
    )
    iam_data = SimpleNamespace(**{attribute: cdr})

    assert fetch_data(iam_data, sector, ["unmatched"]).identical(cdr)
    assert fetch_data(iam_data, sector, ["second", "unmatched"]).identical(
        cdr.sel(variables=["second"])
    )


def test_report_selection_does_not_read_unrelated_fields():
    class PartialIAM:
        other_vars = make_heat_layer("population", [1.0, 2.0])

        @property
        def production_volumes(self):
            raise AssertionError(
                "Population reporting must not read production volumes"
            )

    iam_data = PartialIAM()
    assert fetch_data(iam_data, "Population", ["population"]).identical(
        iam_data.other_vars
    )


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
