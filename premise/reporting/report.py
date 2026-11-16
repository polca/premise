"""
This module export a summary of scenario to an Excel file.
"""

from pathlib import Path

import openpyxl
import pandas as pd
import xarray as xr
import yaml
from openpyxl.chart import AreaChart, LineChart, Reference
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

from ..core.filesystem_constants import DATA_DIR, VARIABLES_DIR

IAM_ELEC_VARS = VARIABLES_DIR / "electricity.yaml"
IAM_FUELS_VARS = VARIABLES_DIR / "fuels.yaml"
IAM_BIOMASS_VARS = VARIABLES_DIR / "biomass.yaml"
IAM_CEMENT_VARS = VARIABLES_DIR / "cement.yaml"
IAM_STEEL_VARS = VARIABLES_DIR / "steel.yaml"
IAM_CDR_VARS = VARIABLES_DIR / "carbon_dioxide_removal.yaml"
IAM_HEATING_VARS = VARIABLES_DIR / "heat.yaml"
IAM_TRSPT_TWO_WHEELERS_VARS = VARIABLES_DIR / "transport_two_wheelers.yaml"
IAM_TRSPT_CARS_VARS = VARIABLES_DIR / "transport_passenger_cars.yaml"
IAM_TRSPT_BUSES_VARS = VARIABLES_DIR / "transport_bus.yaml"
IAM_TRSPT_TRUCKS_VARS = VARIABLES_DIR / "transport_road_freight.yaml"
IAM_TRSPT_TRAINS_VARS = VARIABLES_DIR / "transport_rail_freight.yaml"
IAM_TRSPT_SHIPS_VARS = VARIABLES_DIR / "transport_sea_freight.yaml"
IAM_OTHER_VARS = VARIABLES_DIR / "other.yaml"
REPORT_METADATA_FILEPATH = DATA_DIR / "utils" / "report" / "report.yaml"
VEHICLES_MAP = DATA_DIR / "transport" / "vehicles_map.yaml"


# Report sector -> (IAM data field, optional attribute required for availability).
# Availability gates check attribute presence, including attributes set to None.
_REPORT_DATA_FIELDS = {
    "Population": ("other_vars", None),
    "GDP": ("other_vars", None),
    "CO2": ("other_vars", None),
    "GMST": ("other_vars", None),
    "Electricity - generation": ("production_volumes", "electricity_mix"),
    "Electricity (biom) - generation": ("production_volumes", "biomass_mix"),
    "Electricity - efficiency": ("electricity_technology_efficiencies", None),
    "Heat (buildings) - generation": ("buildings_heat_end_use", None),
    "Heat (industrial) - generation": ("industrial_heat_end_use", None),
    "Heat (secondary) - generation": ("secondary_heat_supply", None),
    "Fuel (gasoline) - generation": ("production_volumes", None),
    "Fuel (gasoline) - efficiency": ("petrol_technology_efficiencies", None),
    "Fuel (diesel) - generation": ("production_volumes", None),
    "Fuel (diesel) - efficiency": ("diesel_technology_efficiencies", None),
    "Fuel (gas) - generation": ("production_volumes", None),
    "Fuel (gas) - efficiency": ("gas_technology_efficiencies", None),
    "Fuel (hydrogen) - generation": ("production_volumes", None),
    "Fuel (hydrogen) - efficiency": ("hydrogen_technology_efficiencies", None),
    "Fuel (kerosene) - generation": ("production_volumes", None),
    "Fuel (kerosene) - efficiency": ("kerosene_technology_efficiencies", None),
    "Fuel (LPG) - generation": ("production_volumes", None),
    "Fuel (LPG) - efficiency": ("lpg_technology_efficiencies", None),
    "Cement - generation": ("production_volumes", None),
    "Cement - efficiency": ("cement_technology_efficiencies", None),
    "Steel - generation": ("production_volumes", None),
    "Steel - efficiency": ("steel_technology_efficiencies", None),
    "CDR - generation": ("production_volumes", None),
    "CDR - energy use": ("cdr_energy_use", None),
    "CDR - efficiency": ("cdr_technology_efficiencies", None),
    "Transport (two-wheelers)": ("production_volumes", "two_wheelers_fleet"),
    "Transport (two-wheelers) - eff": ("two_wheelers_efficiencies", None),
    "Transport (cars)": ("production_volumes", "passenger_car_fleet"),
    "Transport (cars) - eff": ("passenger_car_efficiencies", None),
    "Transport (buses)": ("production_volumes", "bus_fleet"),
    "Transport (buses) - eff": ("bus_efficiencies", None),
    "Transport (trucks)": ("production_volumes", "road_freight_fleet"),
    "Transport (trucks) - eff": ("road_freight_efficiencies", None),
    "Transport (trains)": ("production_volumes", "rail_freight_fleet"),
    "Transport (trains) - eff": ("rail_freight_efficiencies", None),
    "Transport (ships)": ("production_volumes", "sea_freight_fleet"),
    "Transport (ships) - eff": ("sea_freight_efficiencies", None),
    "Battery (mobile)": ("battery_mobile_scenarios", None),
    "Battery (stationary)": ("battery_stationary_scenarios", None),
}


def get_variables(
    filepath,
):
    """
    Get the variables from a yaml file.
    :param filepath: path to the yaml file
    """
    with open(filepath, "r", encoding="utf-8") as stream:
        out = yaml.safe_load(stream)

    return list(out.keys())


def fetch_data(
    iam_data: xr.DataArray, sector: str, variable: str
) -> [xr.DataArray, None]:
    """Select report variables from the requested sector's IAM data field.

    Heat uses production volumes only when its dedicated layer is absent.
    Present fields set to None remain unavailable. Battery chemistry is exposed
    as variables; unmatched CDR requests retain all available CDR variables.
    """
    field, availability = _REPORT_DATA_FIELDS[sector]
    if availability is not None:
        if not hasattr(iam_data, availability):
            return None
        data = getattr(iam_data, field)
    else:
        if field in (
            "buildings_heat_end_use",
            "industrial_heat_end_use",
            "secondary_heat_supply",
        ) and not hasattr(iam_data, field):
            field = "production_volumes"
        data = getattr(iam_data, field, None)

    if data is not None:
        iam_data = data
        if sector in ("Battery (mobile)", "Battery (stationary)"):
            iam_data = iam_data.rename({"chemistry": "variables"})

        variables = [v for v in variable if v in iam_data.coords["variables"].values]
        if not variables and sector in ("CDR - energy use", "CDR - efficiency"):
            variables = list(iam_data.coords["variables"].values)

        return iam_data.sel(variables=variables)

    return None


# --- Helpers (new) ------------------------------------------------------------


def _dataarray_to_wide_df(da: xr.DataArray) -> pd.DataFrame:
    """
    Convert a (variables, year[, ...]) DataArray into a wide DataFrame
    with variables as columns and year as the index.
    Returns an empty DataFrame if conversion yields nothing useful.
    """
    if da is None:
        return pd.DataFrame()

    # Defensive: ensure expected coordinates exist
    if "variables" not in da.coords or "year" not in da.coords:
        return pd.DataFrame()

    try:
        da = da.squeeze(drop=True)
        var_dim = (
            "variables"
            if "variables" in da.dims
            else "variable" if "variable" in da.dims else None
        )
        if "year" in da.dims and var_dim:
            extra_dims = [dim for dim in da.dims if dim not in ("year", var_dim)]
            if extra_dims:
                series_dim = "_series"
                da = da.stack({series_dim: [var_dim, *extra_dims]})
                da = da.transpose("year", series_dim)
                columns = [
                    " - ".join(str(part) for part in label if str(part))
                    for label in da.coords[series_dim].values
                ]
            else:
                da = da.transpose("year", var_dim)
                columns = da.coords[var_dim].values

            df = pd.DataFrame(
                da.values,
                index=da.coords["year"].values,
                columns=columns,
            )
        elif "year" in da.dims and not var_dim:
            col_name = (
                da.name
                or da.attrs.get("variable")
                or da.attrs.get("variables")
                or "value"
            )
            df = pd.DataFrame({col_name: da.values}, index=da.coords["year"].values)
        else:
            return pd.DataFrame()
        df = df.rename_axis(index=None)
        # Drop columns that are completely NA
        df = df.dropna(axis=1, how="all")
        # Drop rows that are completely NA
        df = df.dropna(axis=0, how="all")
        if df.empty and getattr(da, "size", 0) > 0:
            print(
                "Warning: DataArray has data but produced empty DataFrame. "
                f"dims={da.dims}, coords={list(da.coords)}, shape={da.shape}"
            )
        # Ensure index is sortable (years)
        if not df.empty:
            try:
                df.index = pd.to_numeric(df.index)
            except Exception:
                pass
            df = df.sort_index()
        return df
    except Exception as exc:
        dims = getattr(da, "dims", None)
        coords = list(getattr(da, "coords", {}).keys())
        shape = getattr(da, "shape", None)
        print(
            "Warning: failed to convert DataArray to DataFrame. "
            f"dims={dims}, coords={coords}, shape={shape}, error={exc}"
        )
        return pd.DataFrame()


def _write_wide_df(
    worksheet, start_row: int, start_col: int, df: pd.DataFrame
) -> tuple[int, int]:
    """
    Write a wide DataFrame (index as categories, columns as series)
    to the worksheet starting at (start_row, start_col).
    Returns (n_rows_written, n_cols_written).
    """
    if df is None or df.empty:
        return (0, 0)

    # Compose a table with the index in the first column
    table = df.copy()
    table.insert(0, "__cats__", table.index)

    # Use openpyxl helper to get row-wise representation
    rows = dataframe_to_rows(table, index=False, header=True)

    n_rows = 0
    n_cols = 0
    for r_idx, row_vals in enumerate(rows):
        # openpyxl sometimes yields trailing None-only rows; filter them
        if not any(v is not None for v in row_vals):
            continue
        for c_idx, value in enumerate(row_vals, 0):
            worksheet.cell(
                row=start_row + n_rows, column=start_col + c_idx, value=value
            )
            n_cols = max(n_cols, c_idx + 1)
        n_rows += 1

    return (n_rows, n_cols)


def _add_chart_if_data(
    worksheet,
    sector: str,
    title: str,
    start_row: int,
    start_col: int,
    n_rows: int,
    n_cols: int,
    y_axis_label: str | None,
    with_charts: bool = True,
):
    """
    Add an Area/Line chart using the block written by _write_wide_df.
    Guarded to avoid invalid ranges and empty data.
    """
    if not with_charts:
        return

    # Need at least header row + 1 data row, and at least 1 series column
    if n_rows < 2 or n_cols < 2:
        return

    # Cells:
    # - Column layout is: [__cats__ | series1 | series2 | ...]
    # - Header at start_row, data from start_row+1 to start_row+n_rows-1
    # - Categories are in (start_col), series start at (start_col+1)
    min_col_vals = start_col + 1
    max_col_vals = start_col + (n_cols - 1)
    min_row_vals = start_row  # include header for titles_from_data=True
    max_row_vals = start_row + (n_rows - 1)

    # Safety: ensure ranges make sense
    if max_col_vals < min_col_vals or max_row_vals <= min_row_vals:
        return

    values = Reference(
        worksheet,
        min_col=min_col_vals,
        min_row=min_row_vals,
        max_col=max_col_vals,
        max_row=max_row_vals,
    )
    cats = Reference(
        worksheet,
        min_col=start_col,
        min_row=start_row + 1,
        max_row=max_row_vals,
    )

    # Pick chart type
    if any(x in sector for x in ("generation", "mix", "Transport")):
        chart = AreaChart(grouping="stacked")
    elif "eff" in sector:
        chart = LineChart()
    elif "CCS" in sector:
        chart = AreaChart()
    else:
        chart = LineChart()

    # Configure and attach
    chart.add_data(values, titles_from_data=True)
    chart.set_categories(cats)
    chart.title = title
    if y_axis_label:
        chart.y_axis.title = y_axis_label

    chart.x_axis.majorTickMark = "out"
    chart.x_axis.tickLblPos = "nextTo"
    chart.y_axis.majorTickMark = "out"
    chart.y_axis.tickLblPos = "nextTo"
    chart.x_axis.delete = False
    chart.y_axis.delete = False

    chart.height = 8
    chart.width = 16
    # Anchor next to the table block
    chart.anchor = f"{get_column_letter(start_col + 2)}{start_row + 1}"

    worksheet.add_chart(chart)


# --- generate_summary_report (updated) ---------------------------------------


def generate_summary_report(
    scenarios: list, filename: Path, *, with_charts: bool = True
) -> None:
    """
    Generate a summary report of the scenarios.

    with_charts: set to False for huge runs to reduce memory usage.
    """

    SECTORS = {
        "Population": {"filepath": IAM_OTHER_VARS, "variables": ["population"]},
        "GDP": {"filepath": IAM_OTHER_VARS, "variables": ["gdp"]},
        "CO2": {"filepath": IAM_OTHER_VARS, "variables": ["CO2"]},
        "GMST": {"filepath": IAM_OTHER_VARS, "variables": ["GMST"]},
        "Electricity - generation": {"filepath": IAM_ELEC_VARS},
        "Electricity (biom) - generation": {"filepath": IAM_BIOMASS_VARS},
        "Electricity - efficiency": {"filepath": IAM_ELEC_VARS},
        "Heat (buildings) - generation": {
            "filepath": IAM_HEATING_VARS,
            "filter": ["heat, buildings"],
        },
        "Heat (industrial) - generation": {
            "filepath": IAM_HEATING_VARS,
            "filter": ["heat, industrial"],
        },
        "Heat (secondary) - generation": {
            "filepath": IAM_HEATING_VARS,
            "filter": ["heat, secondary"],
        },
        "Fuel (gasoline) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["gasoline", "ethanol", "bioethanol", "methanol"],
        },
        "Fuel (gasoline) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["gasoline", "ethanol", "bioethanol", "methanol"],
        },
        "Fuel (diesel) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["diesel", "biodiesel"],
        },
        "Fuel (diesel) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["diesel", "biodiesel"],
        },
        "Fuel (gas) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["natural gas", "biogas", "methane", "biomethane"],
        },
        "Fuel (gas) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["natural gas", "biogas", "methane", "biomethane"],
        },
        "Fuel (hydrogen) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["hydrogen"],
        },
        "Fuel (hydrogen) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["hydrogen"],
        },
        "Fuel (kerosene) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["kerosene"],
        },
        "Fuel (kerosene) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["kerosene"],
        },
        "Fuel (LPG) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["liquefied petroleum gas"],
        },
        "Fuel (LPG) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": ["liquefied petroleum gas"],
        },
        "Cement - generation": {"filepath": IAM_CEMENT_VARS},
        "Cement - efficiency": {"filepath": IAM_CEMENT_VARS},
        "Steel - generation": {"filepath": IAM_STEEL_VARS},
        "Steel - efficiency": {"filepath": IAM_STEEL_VARS},
        "CDR - generation": {"filepath": IAM_CDR_VARS},
        "CDR - energy use": {"filepath": IAM_CDR_VARS},
        "CDR - efficiency": {"filepath": IAM_CDR_VARS},
        "Transport (two-wheelers)": {"filepath": IAM_TRSPT_TWO_WHEELERS_VARS},
        "Transport (two-wheelers) - eff": {"filepath": IAM_TRSPT_TWO_WHEELERS_VARS},
        "Transport (cars)": {"filepath": IAM_TRSPT_CARS_VARS},
        "Transport (cars) - eff": {"filepath": IAM_TRSPT_CARS_VARS},
        "Transport (buses)": {"filepath": IAM_TRSPT_BUSES_VARS},
        "Transport (buses) - eff": {"filepath": IAM_TRSPT_BUSES_VARS},
        "Transport (trucks)": {"filepath": IAM_TRSPT_TRUCKS_VARS},
        "Transport (trucks) - eff": {"filepath": IAM_TRSPT_TRUCKS_VARS},
        "Transport (trains)": {"filepath": IAM_TRSPT_TRAINS_VARS},
        "Transport (trains) - eff": {"filepath": IAM_TRSPT_TRAINS_VARS},
        "Transport (ships)": {"filepath": IAM_TRSPT_SHIPS_VARS},
        "Transport (ships) - eff": {"filepath": IAM_TRSPT_SHIPS_VARS},
        "Battery (mobile)": {
            "variables": [
                "NMC111",
                "NMC532",
                "NMC622",
                "NMC811",
                "NMC900",
                "NMC900-Si",
                "LFP",
                "NCA",
                "LSB",
                "SIB",
                "LAB",
                "ASSB (oxidic)",
                "ASSB (polymer)",
                "ASSB (sulfidic)",
            ]
        },
        "Battery (stationary)": {
            "variables": [
                "NMC111",
                "NMC622",
                "NMC811",
                "LFP",
                "LEAD-ACID",
                "VRFB",
                "NAS",
            ]
        },
    }

    with open(REPORT_METADATA_FILEPATH, encoding="utf-8") as stream:
        metadata = yaml.safe_load(stream)

    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)

    for sector, spec in SECTORS.items():
        # Build variable list
        if "variables" in spec:
            variables = spec["variables"]
        else:
            variables = get_variables(spec["filepath"])

        if "filter" in spec:
            variables = [
                x for x in variables if any(x.startswith(y) for y in spec["filter"])
            ]

        # Skip sectors with no available data across all scenarios
        is_data = False
        for scenario in scenarios:
            iam_da = fetch_data(
                iam_data=scenario["iam data"], sector=sector, variable=variables
            )
            if iam_da is not None:
                is_data = True
                break
        if not is_data:
            continue

        worksheet = workbook.create_sheet(sector)

        col, row = (1, 1)
        expl_text = metadata.get(sector, {}).get("expl_text", "")
        worksheet.cell(column=col, row=row, value=expl_text)

        scenario_list = set()
        last_col_used = 1

        for scenario_idx, scenario in enumerate(scenarios):
            key = (scenario["model"], scenario["pathway"])
            if key in scenario_list:
                continue

            iam_da = fetch_data(
                iam_data=scenario["iam data"], sector=sector, variable=variables
            )
            if iam_da is None:
                continue

            if scenario_idx > 0:
                # Put some horizontal spacing between scenario blocks
                col = last_col_used + metadata.get(sector, {}).get("offset", 2)

            row = 3

            # Header
            header = f"{scenario['model'].upper()} - {scenario['pathway'].upper()}"
            worksheet.cell(column=col, row=row, value=header)
            worksheet.cell(column=col, row=row).font = Font(
                bold=True, size=14, underline="single"
            )
            row += 2

            if sector in ("Battery (mobile)", "Battery (stationary)"):
                # Iterate per sub-scenario within batteries
                for scen in iam_da.coords["scenario"].values:
                    worksheet.cell(column=col, row=row, value=scen)
                    row += 3

                    sub = iam_da.sel(
                        scenario=scen,
                        year=[y for y in iam_da.coords["year"].values if y <= 2100],
                    )
                    df = _dataarray_to_wide_df(sub)
                    if df.empty:
                        continue

                    n_rows, n_cols = _write_wide_df(worksheet, row, col, df)
                    _add_chart_if_data(
                        worksheet=worksheet,
                        sector=sector,
                        title=f"{scen} scenario",
                        start_row=row,
                        start_col=col,
                        n_rows=n_rows,
                        n_cols=n_cols,
                        y_axis_label=metadata.get(sector, {}).get("label"),
                        with_charts=with_charts,
                    )

                    last_col_used = max(last_col_used, col + n_cols - 1)
                    row += n_rows + 2

            else:
                var_dim = (
                    "variables"
                    if "variables" in iam_da.coords
                    else "variable" if "variable" in iam_da.coords else None
                )
                has_region = "region" in iam_da.coords

                regions = (
                    list(iam_da.coords["region"].values) if has_region else ["World"]
                )
                if not regions:
                    print(
                        f"Warning: no regions available for sector {sector} in report."
                    )
                for region in regions:
                    if sector in ("GMST", "CO2") and region != "World":
                        continue

                    worksheet.cell(column=col, row=row, value=region)
                    row += 3

                    if var_dim is None:
                        print(
                            f"Warning: no variables dimension for sector {sector} "
                            f"(dims={iam_da.dims})."
                        )
                        continue

                    valid_vars = list(iam_da.coords[var_dim].values)
                    valid_vars_norm = {str(v).strip().lower(): v for v in valid_vars}
                    pick_vars = []
                    for v in variables:
                        key = str(v).strip().lower()
                        if key in valid_vars_norm:
                            pick_vars.append(valid_vars_norm[key])

                    if not pick_vars and sector in ("Population", "GDP", "GMST", "CO2"):
                        print(
                            "Warning: no matching variables for "
                            f"{sector}. Available variables: {sorted(valid_vars)}"
                        )
                        continue

                    selector = {var_dim: pick_vars}
                    if has_region:
                        selector["region"] = region
                    if "year" in iam_da.coords:
                        selector["year"] = [
                            y for y in iam_da.coords["year"].values if y <= 2100
                        ]

                    sub = iam_da.sel(**selector)

                    df = _dataarray_to_wide_df(sub)
                    if df.empty:
                        continue

                    n_rows, n_cols = _write_wide_df(worksheet, row, col, df)
                    _add_chart_if_data(
                        worksheet=worksheet,
                        sector=sector,
                        title=f"{region} - {sector} ({metadata.get(sector, {}).get('label', '')})",
                        start_row=row,
                        start_col=col,
                        n_rows=n_rows,
                        n_cols=n_cols,
                        y_axis_label=None,  # title already includes label
                        with_charts=with_charts,
                    )

                    last_col_used = max(last_col_used, col + n_cols - 1)
                    row += n_rows + 2

            scenario_list.add(key)

    from .clic import write_inventory_sheet, write_inventory_sidecars

    write_inventory_sheet(workbook, scenarios, workbook_path=filename)
    write_inventory_sidecars(filename, scenarios)
    workbook.save(filename)
