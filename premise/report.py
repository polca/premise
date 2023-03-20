"""
This module export a summary of scenario to an Excel file.
"""

import os
from datetime import datetime
from pathlib import Path

import openpyxl
import pandas as pd
import yaml
from openpyxl.chart import AreaChart, LineChart, Reference
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.dimensions import ColumnDimension, DimensionHolder
from pandas.errors import EmptyDataError

from . import DATA_DIR, VARIABLES_DIR, __version__

IAM_ELEC_VARS = VARIABLES_DIR / "electricity_variables.yaml"
IAM_FUELS_VARS = VARIABLES_DIR / "fuels_variables.yaml"
IAM_BIOMASS_VARS = VARIABLES_DIR / "biomass_variables.yaml"
IAM_CEMENT_VARS = VARIABLES_DIR / "cement_variables.yaml"
IAM_STEEL_VARS = VARIABLES_DIR / "steel_variables.yaml"
IAM_DACCS_VARS = VARIABLES_DIR / "direct_air_capture_variables.yaml"
IAM_OTHER_VARS = VARIABLES_DIR / "other_variables.yaml"
IAM_CARBON_CAPTURE_VARS = VARIABLES_DIR / "carbon_capture_variables.yaml"
REPORT_METADATA_FILEPATH = DATA_DIR / "utils" / "report" / "report.yaml"
VEHICLES_MAP = DATA_DIR / "transport" / "vehicles_map.yaml"

LOG_REPORTING_FILEPATH = DATA_DIR / "utils" / "logging" / "reporting.yaml"

SECTORS = {
    "Population": (IAM_OTHER_VARS, ["Population"]),
    "GDP": (IAM_OTHER_VARS, ["GDP|PPP"]),
    "CO2": (IAM_OTHER_VARS, ["Emi|CO2"]),
    "GMST": (IAM_OTHER_VARS, ["Temperature|Global Mean"]),
    "Electricity - generation": IAM_ELEC_VARS,
    "Electricity (biom) - generation": IAM_BIOMASS_VARS,
    "Electricity - efficiency": IAM_ELEC_VARS,
    "Fuel - generation": IAM_FUELS_VARS,
    "Fuel - efficiency": IAM_FUELS_VARS,
    "Cement - generation": IAM_CEMENT_VARS,
    "Cement - efficiency": IAM_CEMENT_VARS,
    "Cement - CCS": (IAM_CARBON_CAPTURE_VARS, ["cement"]),
    "Steel - generation": IAM_STEEL_VARS,
    "Steel - efficiency": IAM_STEEL_VARS,
    "Steel - CCS": (IAM_CARBON_CAPTURE_VARS, ["steel"]),
    "Direct Air Capture - generation": (IAM_DACCS_VARS, ["dac_solvent", "dac_sorbent"]),
    "Transport (cars)": (
        VEHICLES_MAP,
        ["BEV", "FCEV", "ICEV-d", "ICEV-g", "ICEV-p", "PHEV-d", "PHEV-p"],
    ),
    "Transport (buses)": (
        VEHICLES_MAP,
        ["BEV", "FCEV", "ICEV-d", "ICEV-g", "ICEV-p", "PHEV-d", "PHEV-p"],
    ),
    "Transport (trucks)": (
        VEHICLES_MAP,
        ["BEV", "FCEV", "ICEV-d", "ICEV-g", "ICEV-p", "PHEV-d", "PHEV-p"],
    ),
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


def generate_summary_report(scenarios: list, filename: Path) -> None:
    """
    Generate a summary report of the scenarios.
    """

    with open(REPORT_METADATA_FILEPATH, "r", encoding="utf-8") as stream:
        metadata = yaml.safe_load(stream)

    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)

    for sector, filepath in SECTORS.items():
        if isinstance(filepath, tuple):
            filepath, variables = filepath
        else:
            variables = get_variables(filepath)

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
                if "generation" in sector:
                    iam_data = scenario["iam data"].production_volumes
                elif "efficiency" in sector:
                    iam_data = scenario["iam data"].efficiency
                elif "CCS" in sector:
                    iam_data = scenario["iam data"].carbon_capture_rate * 100
                elif "car" in sector:
                    if scenario["iam data"].trsp_cars is not None:
                        iam_data = scenario["iam data"].trsp_cars.sum(
                            dim=["size", "construction_year"]
                        )
                        iam_data = iam_data.rename({"powertrain": "variables"}).T
                    else:
                        continue
                elif "bus" in sector:
                    if scenario["iam data"].trsp_buses is not None:
                        iam_data = scenario["iam data"].trsp_buses.sum(
                            dim=["size", "construction_year"]
                        )
                        iam_data = iam_data.rename({"powertrain": "variables"}).T
                    else:
                        continue
                elif "truck" in sector:
                    if scenario["iam data"].trsp_trucks is not None:
                        iam_data = scenario["iam data"].trsp_trucks.sum(
                            dim=["size", "construction_year"]
                        )
                        iam_data = iam_data.rename({"powertrain": "variables"}).T
                    else:
                        continue
                else:
                    iam_data = scenario["iam data"].other_vars

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

                for region in scenario["iam data"].regions:
                    if sector == "GMST" and region != "World":
                        continue

                    worksheet.cell(column=col, row=row, value=region)

                    row += 3

                    dataframe = iam_data.sel(
                        variables=[
                            v for v in variables if v in iam_data.variables.values
                        ],
                        region=region,
                        year=[y for y in iam_data.coords["year"].values if y <= 2100],
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

                        if "generation" in sector:
                            chart = AreaChart(grouping="stacked")
                        elif "efficiency" in sector:
                            chart = LineChart()
                        elif "CCS" in sector:
                            chart = AreaChart()
                        elif "Transport" in sector:
                            chart = AreaChart(grouping="stacked")
                        else:
                            chart = LineChart()

                        chart.add_data(values, titles_from_data=True)
                        chart.set_categories(cats)
                        chart.title = f"{region} - {sector}"
                        chart.y_axis.title = metadata[sector]["label"]
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
        "premise_electricity",
        "premise_fuel",
        "premise_transport",
        "premise_steel",
        "premise_cement",
        "premise_emissions",
    ]

    # fetch YAML file containing the reporting metadata
    with open(LOG_REPORTING_FILEPATH, "r") as f:
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
        # check if log ile exists
        if not os.path.isfile(filepath + ".log"):
            continue
        # if file exists, check that it is not empty
        elif os.stat(filepath + ".log").st_size == 0:
            continue

        df = convert_log_to_excel_file(filepath)

        # create a worksheet for this sector
        worksheet = workbook.create_sheet(fetch_tab_name(filepath))

        # add a description of each column
        # in each row
        for col, column in enumerate(fetch_columns(filepath), 1):
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
    fp = os.path.join(
        os.getcwd(), f"change_report {datetime.now().strftime('%Y-%m-%d')}.xlsx"
    )
    workbook.save(fp)


def fetch_columns(variable):
    """
    Read reporting.yaml which return
    the columns for the variable.
    """

    with open(LOG_REPORTING_FILEPATH, "r", encoding="utf-8") as stream:
        reporting = yaml.safe_load(stream)

    return list(reporting[variable]["columns"].keys())


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

    try:
        df = pd.read_csv(filepath + ".log", sep="|", header=None)
        df.columns = fetch_columns(filepath)
        return df

    except EmptyDataError:
        # return an empty dataframe
        return pd.DataFrame(columns=fetch_columns(filepath))
