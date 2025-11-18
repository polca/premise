"""
This module export a summary of scenario to an Excel file.
"""

import os
from datetime import datetime
from pathlib import Path

import openpyxl
import pandas as pd
import xarray as xr
import yaml
from openpyxl.chart import AreaChart, LineChart, Reference
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.dimensions import ColumnDimension, DimensionHolder
from pandas.errors import EmptyDataError

from . import __version__
from .filesystem_constants import DATA_DIR, VARIABLES_DIR
from .logger import empty_log_files

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

LOG_REPORTING_FILEPATH = DATA_DIR / "utils" / "logging" / "reporting.yaml"
# directory for log files
DIR_LOGS = Path.cwd() / "export" / "logs"
DIR_LOG_REPORT = Path.cwd() / "export" / "change reports"

# if DIR_LOG_REPORT folder does not exist
# we create it
if not Path(DIR_LOG_REPORT).exists():
    Path(DIR_LOG_REPORT).mkdir(parents=True, exist_ok=True)


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
    data = {
        "Population": iam_data.other_vars if hasattr(iam_data, "other_vars") else None,
        "GDP": iam_data.other_vars if hasattr(iam_data, "other_vars") else None,
        "CO2": iam_data.other_vars if hasattr(iam_data, "other_vars") else None,
        "GMST": iam_data.other_vars if hasattr(iam_data, "other_vars") else None,
        "Electricity - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "electricity_mix")
            else None
        ),
        "Electricity (biom) - generation": (
            iam_data.production_volumes if hasattr(iam_data, "biomass_mix") else None
        ),
        "Electricity - efficiency": (
            iam_data.electricity_technology_efficiencies
            if hasattr(iam_data, "electricity_technology_efficiencies")
            else None
        ),
        "Heat (buildings) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Heat (industrial) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Fuel (gasoline) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Fuel (gasoline) - efficiency": (
            iam_data.petrol_technology_efficiencies
            if hasattr(iam_data, "petrol_technology_efficiencies")
            else None
        ),
        "Fuel (diesel) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Fuel (diesel) - efficiency": (
            iam_data.diesel_technology_efficiencies
            if hasattr(iam_data, "diesel_technology_efficiencies")
            else None
        ),
        "Fuel (gas) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Fuel (gas) - efficiency": (
            iam_data.gas_technology_efficiencies
            if hasattr(iam_data, "gas_technology_efficiencies")
            else None
        ),
        "Fuel (hydrogen) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Fuel (hydrogen) - efficiency": (
            iam_data.hydrogen_technology_efficiencies
            if hasattr(iam_data, "hydrogen_technology_efficiencies")
            else None
        ),
        "Fuel (kerosene) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Fuel (kerosene) - efficiency": (
            iam_data.kerosene_technology_efficiencies
            if hasattr(iam_data, "kerosene_technology_efficiencies")
            else None
        ),
        "Fuel (LPG) - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Fuel (LPG) - efficiency": (
            iam_data.lpg_technology_efficiencies
            if hasattr(iam_data, "lpg_technology_efficiencies")
            else None
        ),
        "Cement - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Cement - efficiency": (
            iam_data.cement_technology_efficiencies
            if hasattr(iam_data, "cement_technology_efficiencies")
            else None
        ),
        "Steel - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Steel - efficiency": (
            iam_data.steel_technology_efficiencies
            if hasattr(iam_data, "steel_technology_efficiencies")
            else None
        ),
        "CDR - generation": (
            iam_data.production_volumes
            if hasattr(iam_data, "production_volumes")
            else None
        ),
        "Direct Air Capture - energy mix": (
            iam_data.daccs_energy_use if hasattr(iam_data, "daccs_energy_use") else None
        ),
        "Direct Air Capture - heat eff.": (
            iam_data.dac_heat_efficiencies
            if hasattr(iam_data, "dac_heat_efficiencies")
            else None
        ),
        "Direct Air Capture - elec eff.": (
            iam_data.dac_electricity_efficiencies
            if hasattr(iam_data, "dac_electricity_efficiencies")
            else None
        ),
        "Transport (two-wheelers)": (
            iam_data.production_volumes
            if hasattr(iam_data, "two_wheelers_fleet")
            else None
        ),
        "Transport (two-wheelers) - eff": (
            iam_data.two_wheelers_efficiencies
            if hasattr(iam_data, "two_wheelers_efficiencies")
            else None
        ),
        "Transport (cars)": (
            iam_data.production_volumes
            if hasattr(iam_data, "passenger_car_fleet")
            else None
        ),
        "Transport (cars) - eff": (
            iam_data.passenger_car_efficiencies
            if hasattr(iam_data, "passenger_car_efficiencies")
            else None
        ),
        "Transport (buses)": (
            iam_data.production_volumes if hasattr(iam_data, "bus_fleet") else None
        ),
        "Transport (buses) - eff": (
            iam_data.bus_efficiencies if hasattr(iam_data, "bus_efficiencies") else None
        ),
        "Transport (trucks)": (
            iam_data.production_volumes
            if hasattr(iam_data, "road_freight_fleet")
            else None
        ),
        "Transport (trucks) - eff": (
            iam_data.road_freight_efficiencies
            if hasattr(iam_data, "road_freight_efficiencies")
            else None
        ),
        "Transport (trains)": (
            iam_data.production_volumes
            if hasattr(iam_data, "rail_freight_fleet")
            else None
        ),
        "Transport (trains) - eff": (
            iam_data.rail_freight_efficiencies
            if hasattr(iam_data, "rail_freight_efficiencies")
            else None
        ),
        "Transport (ships)": (
            iam_data.production_volumes
            if hasattr(iam_data, "sea_freight_fleet")
            else None
        ),
        "Transport (ships) - eff": (
            iam_data.sea_freight_efficiencies
            if hasattr(iam_data, "sea_freight_efficiencies")
            else None
        ),
        "Battery (mobile)": (
            iam_data.battery_mobile_scenarios
            if hasattr(iam_data, "battery_mobile_scenarios")
            else None
        ),
        "Battery (stationary)": (
            iam_data.battery_stationary_scenarios
            if hasattr(iam_data, "battery_stationary_scenarios")
            else None
        ),
    }

    if data[sector] is not None:
        iam_data = data[sector]
        if sector in ("Battery (mobile)", "Battery (stationary)"):
            iam_data = iam_data.rename({"chemistry": "variables"})

        return iam_data.sel(
            variables=[v for v in variable if v in iam_data.coords["variables"].values]
        )

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
        df = da.to_dataframe("val")
        # Want wide format with variables as columns, years as rows
        # After unstack, top level 'val' remains; select it and transpose so rows are years
        df = df.unstack()["val"].T
        df = df.rename_axis(index=None)
        # Drop columns that are completely NA
        df = df.dropna(axis=1, how="all")
        # Drop rows that are completely NA
        df = df.dropna(axis=0, how="all")
        # Ensure index is sortable (years)
        if not df.empty:
            try:
                df.index = pd.to_numeric(df.index)
            except Exception:
                pass
            df = df.sort_index()
        return df
    except Exception:
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
        "Direct Air Capture - energy mix": {
            "filepath": IAM_HEATING_VARS,
            "variables": [
                "energy, for DACCS, from hydrogen turbine",
                "energy, for DACCS, from gas boiler",
                "energy, for DACCS, from other",
                "energy, for DACCS, from electricity",
            ],
        },
        "Direct Air Capture - heat eff.": {
            "filepath": IAM_CDR_VARS,
            "variables": ["dac_solvent"],
        },
        "Direct Air Capture - elec eff.": {
            "filepath": IAM_CDR_VARS,
            "variables": ["dac_solvent"],
        },
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

            # Optional unit conversions (kept from your code; "CCS" sectors not present though)
            if "CCS" in sector:
                try:
                    iam_da = iam_da * 100
                except Exception:
                    pass

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
                # Iterate per region
                for region in getattr(scenario["iam data"], "regions", []):
                    if sector in ("GMST", "CO2") and region != "World":
                        continue

                    worksheet.cell(column=col, row=row, value=region)
                    row += 3

                    # IMPORTANT: use DataArray coords, not DataArray.variables (Dataset-only)
                    valid_vars = set(iam_da.coords["variables"].values)
                    pick_vars = [v for v in variables if v in valid_vars]

                    sub = iam_da.sel(
                        variables=pick_vars,
                        region=region,
                        year=[y for y in iam_da.coords["year"].values if y <= 2100],
                    )

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

    workbook.save(filename)


# --- generate_change_report (updated, hardened) -------------------------------


def generate_change_report(source, version, source_type, system_model):
    """
    Generate a change report of the scenarios from the log files.
    """

    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)

    log_filepaths = [
        "premise_dac",
        "premise_biomass",
        "premise_electricity",
        "premise_fuel",
        "premise_heat",
        "premise_battery",
        "premise_wind_turbine",
        "premise_transport",
        "premise_steel",
        "premise_metal",
        "premise_cement",
        "premise_emissions",
        "premise_external_scenarios",
        "premise_mapping",
        "premise_validation",
    ]

    # fetch reporting metadata
    with open(LOG_REPORTING_FILEPATH, encoding="utf-8") as f:
        metadata = yaml.load(f, Loader=yaml.FullLoader)

    worksheet = workbook.create_sheet("Change report")
    worksheet.cell(row=1, column=1, value="Library name")
    worksheet.cell(row=1, column=2, value="Library version")
    worksheet.cell(row=1, column=3, value="Report date")
    worksheet.cell(row=1, column=4, value="Source database")
    worksheet.cell(row=1, column=5, value="Source database format")
    worksheet.cell(row=1, column=6, value="Database version")
    worksheet.cell(row=1, column=7, value="Database system model")
    worksheet.cell(row=2, column=1, value="premise")
    worksheet.cell(row=2, column=2, value=".".join(map(str, __version__)))
    worksheet.cell(row=2, column=3, value=datetime.now())
    worksheet.cell(row=2, column=4, value=source)
    worksheet.cell(row=2, column=5, value=source_type)
    worksheet.cell(row=2, column=6, value=version)
    worksheet.cell(row=2, column=7, value=system_model)

    dim_holder = DimensionHolder(worksheet=worksheet)
    for col in range(worksheet.min_column, worksheet.max_column + 1):
        dim_holder[get_column_letter(col)] = ColumnDimension(
            worksheet, min=col, max=col, width=20
        )
    worksheet.column_dimensions = dim_holder

    for name in log_filepaths:
        fp = Path(DIR_LOGS / name).with_suffix(".log")
        if not fp.is_file():
            continue
        if os.stat(fp).st_size == 0:
            continue

        try:
            df = convert_log_to_excel_file(fp)
        except Exception as e:
            # skip this file if unreadable
            continue

        # Create per-sector sheet
        tab_meta = metadata.get(name, {})
        tab_name = tab_meta.get(
            "tab", fetch_tab_name(name) if isinstance(name, str) else name
        )
        # Ensure a valid, unique sheet name (Excel max 31 chars)
        tab_name = (tab_name or name)[:31]
        worksheet = workbook.create_sheet(tab_name)

        # Add column descriptions/units
        cols = fetch_columns(fp)
        colmeta = tab_meta.get("columns", {})
        for c_idx, column in enumerate(cols, 1):
            desc = colmeta.get(column, {}).get("description", column)
            unit = colmeta.get(column, {}).get("unit", "")
            worksheet.cell(row=1, column=c_idx, value=desc)
            worksheet.cell(row=2, column=c_idx, value=unit)

        # Append data rows
        for r in dataframe_to_rows(df, index=False):
            # dataframe_to_rows yields header first; we already wrote descriptions/units,
            # so skip the header row it produces.
            if r and r[0] == df.columns[0]:
                continue
            worksheet.append(r)

    # Save workbook
    fp_out = Path(
        DIR_LOG_REPORT / f"change_report {datetime.now().strftime('%Y-%m-%d')}.xlsx"
    )
    workbook.save(fp_out)
    empty_log_files()


# --- fetch_columns / fetch_tab_name (safe) ------------------------------------


def fetch_columns(variable):
    """
    Read reporting.yaml and return the columns for the variable.
    `variable` may be a Path or a string stem.
    """
    stem = variable.stem if hasattr(variable, "stem") else Path(variable).stem
    with open(LOG_REPORTING_FILEPATH, "r", encoding="utf-8") as stream:
        reporting = yaml.safe_load(stream)

    # Defensive: return listed columns or empty list
    cols = reporting.get(stem, {}).get("columns", {})
    return list(cols.keys())


def fetch_tab_name(variable):
    """
    Read reporting.yaml and return the tab name for the variable key (string).
    """
    key = variable if isinstance(variable, str) else str(variable)
    with open(LOG_REPORTING_FILEPATH, "r", encoding="utf-8") as stream:
        reporting = yaml.safe_load(stream)
    return reporting.get(key, {}).get("tab", key)


# --- convert_log_to_excel_file (hardened) -------------------------------------


def convert_log_to_excel_file(filepath):
    """
    Read a '|' delimited log file into a DataFrame with columns from reporting.yaml.
    Handles mismatched column counts by trimming/padding; skips bad lines.
    """
    # Engine=python is more forgiving for irregular lines
    df = pd.read_csv(
        filepath, sep="|", header=None, on_bad_lines="skip", engine="python"
    )
    cols = fetch_columns(filepath)

    # Align column counts
    if df.shape[1] > len(cols):
        df = df.iloc[:, : len(cols)]
    elif df.shape[1] < len(cols):
        # pad with NA columns
        for _ in range(len(cols) - df.shape[1]):
            df[df.shape[1]] = pd.NA

    df.columns = cols
    return df
