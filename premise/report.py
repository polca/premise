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


def generate_summary_report(scenarios: list, filename: Path) -> None:
    """
    Generate a summary report of the scenarios.
    """

    SECTORS = {
        "Population": {
            "filepath": IAM_OTHER_VARS,
            "variables": [
                "population",
            ],
        },
        "GDP": {
            "filepath": IAM_OTHER_VARS,
            "variables": [
                "gdp",
            ],
        },
        "CO2": {
            "filepath": IAM_OTHER_VARS,
            "variables": [
                "CO2",
            ],
        },
        "GMST": {
            "filepath": IAM_OTHER_VARS,
            "variables": [
                "GMST",
            ],
        },
        "Electricity - generation": {
            "filepath": IAM_ELEC_VARS,
        },
        "Electricity (biom) - generation": {
            "filepath": IAM_BIOMASS_VARS,
        },
        "Electricity - efficiency": {
            "filepath": IAM_ELEC_VARS,
        },
        "Heat (buildings) - generation": {
            "filepath": IAM_HEATING_VARS,
            "filter": [
                "heat, buildings",
            ],
        },
        "Heat (industrial) - generation": {
            "filepath": IAM_HEATING_VARS,
            "filter": [
                "heat, industrial",
            ],
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
            "filter": [
                "diesel",
                "biodiesel",
            ],
        },
        "Fuel (diesel) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": [
                "diesel",
                "biodiesel",
            ],
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
            "filter": [
                "hydrogen",
            ],
        },
        "Fuel (hydrogen) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": [
                "hydrogen",
            ],
        },
        "Fuel (kerosene) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": [
                "kerosene",
            ],
        },
        "Fuel (kerosene) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": [
                "kerosene",
            ],
        },
        "Fuel (LPG) - generation": {
            "filepath": IAM_FUELS_VARS,
            "filter": [
                "liquefied petroleum gas",
            ],
        },
        "Fuel (LPG) - efficiency": {
            "filepath": IAM_FUELS_VARS,
            "filter": [
                "liquefied petroleum gas",
            ],
        },
        "Cement - generation": {
            "filepath": IAM_CEMENT_VARS,
        },
        "Cement - efficiency": {
            "filepath": IAM_CEMENT_VARS,
        },
        "Steel - generation": {
            "filepath": IAM_STEEL_VARS,
        },
        "Steel - efficiency": {
            "filepath": IAM_STEEL_VARS,
        },
        "CDR - generation": {
            "filepath": IAM_CDR_VARS,
        },
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
        "Transport (two-wheelers)": {
            "filepath": IAM_TRSPT_TWO_WHEELERS_VARS,
        },
        "Transport (two-wheelers) - eff": {
            "filepath": IAM_TRSPT_TWO_WHEELERS_VARS,
        },
        "Transport (cars)": {
            "filepath": IAM_TRSPT_CARS_VARS,
        },
        "Transport (cars) - eff": {
            "filepath": IAM_TRSPT_CARS_VARS,
        },
        "Transport (buses)": {
            "filepath": IAM_TRSPT_BUSES_VARS,
        },
        "Transport (buses) - eff": {
            "filepath": IAM_TRSPT_BUSES_VARS,
        },
        "Transport (trucks)": {
            "filepath": IAM_TRSPT_TRUCKS_VARS,
        },
        "Transport (trucks) - eff": {
            "filepath": IAM_TRSPT_TRUCKS_VARS,
        },
        "Transport (trains)": {
            "filepath": IAM_TRSPT_TRAINS_VARS,
        },
        "Transport (trains) - eff": {
            "filepath": IAM_TRSPT_TRAINS_VARS,
        },
        "Transport (ships)": {
            "filepath": IAM_TRSPT_SHIPS_VARS,
        },
        "Transport (ships) - eff": {
            "filepath": IAM_TRSPT_SHIPS_VARS,
        },
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
            ],
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
            ],
        },
    }

    with open(REPORT_METADATA_FILEPATH, encoding="utf-8") as stream:
        metadata = yaml.safe_load(stream)

    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)

    for sector, filepath in SECTORS.items():
        if "variables" in filepath:
            variables = filepath["variables"]
        else:
            variables = get_variables(filepath["filepath"])

        if "filter" in filepath:
            variables = [
                x for x in variables if any(x.startswith(y) for y in filepath["filter"])
            ]

        # before creating the worksheet
        # check if we have data to plot

        is_data = False
        for scenario in scenarios:
            iam_data = fetch_data(
                iam_data=scenario["iam data"],
                sector=sector,
                variable=variables,
            )
            if iam_data is not None:
                is_data = True
                break
        if not is_data:
            continue

        worksheet = workbook.create_sheet(sector)

        col, row = (1, 1)

        worksheet.cell(
            column=col,
            row=row,
            value=metadata[sector]["expl_text"],
        )

        scenario_list = []

        last_col_used = 0

        for scenario_idx, scenario in enumerate(scenarios):
            if (scenario["model"], scenario["pathway"]) not in scenario_list:

                iam_data = fetch_data(
                    iam_data=scenario["iam data"],
                    sector=sector,
                    variable=variables,
                )

                if iam_data is None:
                    continue

                if "CCS" in sector and iam_data is not None:
                    iam_data *= 100

                # if sector == "CDR - generation":
                #    iam_data *= -1

                if iam_data is None:
                    continue

                if scenario_idx > 0:
                    col = last_col_used + metadata[sector]["offset"]

                row = 3

                worksheet.cell(
                    column=col,
                    row=row,
                    value=f"{scenario['model'].upper()} - {scenario['pathway'].upper()}",
                )
                worksheet.cell(column=col, row=row).font = Font(
                    bold=True, size=14, underline="single"
                )

                row += 2

                if sector in ("Battery (mobile)", "Battery (stationary)"):
                    for scen in iam_data.coords["scenario"].values:

                        worksheet.cell(column=col, row=row, value=scen)

                        row += 3

                        dataframe = iam_data.sel(
                            scenario=scen,
                            year=[
                                y for y in iam_data.coords["year"].values if y <= 2100
                            ],
                        )

                        if len(dataframe) > 0:
                            dataframe = dataframe.to_dataframe("val")
                            dataframe = dataframe.unstack()["val"]
                            dataframe = dataframe.T
                            dataframe = dataframe.rename_axis(index=None)

                            data = dataframe_to_rows(dataframe)

                            counter = 0
                            for _, data_row in enumerate(data, 1):
                                if data_row != [None]:
                                    for c_idx, value in enumerate(data_row, 1):
                                        worksheet.cell(
                                            row=row + counter,
                                            column=col + c_idx,
                                            value=value,
                                        )
                                        last_col_used = col + c_idx
                                    counter += 1

                            values = Reference(
                                worksheet,
                                min_col=col + 2,
                                min_row=row,
                                max_col=col + c_idx,
                                max_row=row + counter - 1,
                            )
                            cats = Reference(
                                worksheet,
                                min_col=col + 1,
                                min_row=row + 1,
                                max_row=row + counter - 1,
                            )
                            chart = AreaChart(grouping="stacked")

                            chart.add_data(values, titles_from_data=True)
                            chart.set_categories(cats)
                            chart.title = f"{scen} scenario"
                            chart.y_axis.title = metadata[sector]["label"]
                            chart.height = 8
                            chart.width = 16
                            chart.anchor = f"{get_column_letter(col + 2)}{row + 1}"
                            worksheet.add_chart(chart)

                            row += counter + 2

                else:
                    for region in scenario["iam data"].regions:
                        if (
                            sector
                            in [
                                "GMST",
                                "CO2",
                            ]
                            and region != "World"
                        ):
                            continue

                        worksheet.cell(column=col, row=row, value=region)

                        row += 3

                        dataframe = iam_data.sel(
                            variables=[
                                v for v in variables if v in iam_data.variables.values
                            ],
                            region=region,
                            year=[
                                y for y in iam_data.coords["year"].values if y <= 2100
                            ],
                        )

                        if len(dataframe) > 0:
                            dataframe = dataframe.to_dataframe("val")
                            dataframe = dataframe.unstack()["val"]
                            dataframe = dataframe.T
                            dataframe = dataframe.rename_axis(index=None)

                            data = dataframe_to_rows(dataframe)

                            counter = 0
                            for _, data_row in enumerate(data, 1):
                                if data_row != [None]:
                                    for c_idx, value in enumerate(data_row, 1):
                                        worksheet.cell(
                                            row=row + counter,
                                            column=col + c_idx,
                                            value=value,
                                        )
                                        last_col_used = col + c_idx
                                    counter += 1

                            values = Reference(
                                worksheet,
                                min_col=col + 2,
                                min_row=row,
                                max_col=col + c_idx,
                                max_row=row + counter - 1,
                            )
                            cats = Reference(
                                worksheet,
                                min_col=col + 1,
                                min_row=row + 1,
                                max_row=row + counter - 1,
                            )

                            if any(x in sector for x in ("generation", "mix")):
                                chart = AreaChart(grouping="stacked")
                            elif "eff" in sector:
                                chart = LineChart()
                            elif "CCS" in sector:
                                chart = AreaChart()
                            elif "Transport" in sector:
                                chart = AreaChart(grouping="stacked")
                            else:
                                chart = LineChart()

                            chart.add_data(values, titles_from_data=True)
                            chart.set_categories(cats)

                            chart.x_axis.majorTickMark = "out"
                            chart.x_axis.tickLblPos = "nextTo"

                            chart.y_axis.majorTickMark = "out"
                            chart.y_axis.tickLblPos = "nextTo"

                            chart.x_axis.delete = False
                            chart.y_axis.delete = False

                            chart.title = (
                                f"{region} - {sector} ({metadata[sector]['label']})"
                            )
                            # chart.y_axis.title = metadata[sector]["label"] + "\n\n\n\n"
                            chart.height = 8
                            chart.width = 16
                            chart.anchor = f"{get_column_letter(col + 2)}{row + 1}"
                            worksheet.add_chart(chart)

                            row += counter + 2

                scenario_list.append((scenario["model"], scenario["pathway"]))

    workbook.save(filename)


def generate_change_report(source, version, source_type, system_model):
    """
    Generate a change report of the scenarios from the log files.
    """

    # create an Excel workbook
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

    # fetch YAML file containing the reporting metadata
    with open(LOG_REPORTING_FILEPATH, encoding="utf-8") as f:
        metadata = yaml.load(f, Loader=yaml.FullLoader)

    # create a first tab
    # where is displayed
    # the name and version of the library
    # the date of the report
    # and the name of the source database
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

    for filepath in log_filepaths:
        fp = Path(DIR_LOGS / filepath).with_suffix(".log")
        # check if log file exists
        if not fp.is_file():
            continue
        # if file exists, check that it is not empty
        if os.stat(fp).st_size == 0:
            continue

        df = convert_log_to_excel_file(fp)

        # create a worksheet for this sector
        worksheet = workbook.create_sheet(fetch_tab_name(filepath))

        # add a description of each column
        # in each row
        for col, column in enumerate(fetch_columns(fp), 1):
            worksheet.cell(
                row=1,
                column=col,
                value=metadata[filepath]["columns"][column]["description"],
            )
            worksheet.cell(
                row=2,
                column=col,
                value=metadata[filepath]["columns"][column].get("unit"),
            )

        # add the df dataframe to the sheet
        for r in dataframe_to_rows(df, index=False):
            worksheet.append(r)

    # save the workbook in the working directory
    # the file name is change_report with the current date
    fp = Path(
        DIR_LOG_REPORT / f"change_report {datetime.now().strftime('%Y-%m-%d')}.xlsx"
    )
    workbook.save(fp)
    empty_log_files()


def fetch_columns(variable):
    """
    Read reporting.yaml which return
    the columns for the variable.
    """

    with open(LOG_REPORTING_FILEPATH, "r", encoding="utf-8") as stream:
        reporting = yaml.safe_load(stream)

    return list(reporting[variable.stem]["columns"].keys())


def fetch_tab_name(variable):
    """
    Read reporting.yaml which return
    the tab name for the variable.
    """

    with open(LOG_REPORTING_FILEPATH, "r", encoding="utf-8") as stream:
        reporting = yaml.safe_load(stream)

    return reporting[variable]["tab"]


def convert_log_to_excel_file(filepath):
    """
    Read the log file premise.log in the working directory.
    Load into pandas dataframe and group the data by
    scenario and variable.
    """

    df = pd.read_csv(filepath, sep="|", header=None, on_bad_lines="skip")
    df.columns = fetch_columns(filepath)
    return df
