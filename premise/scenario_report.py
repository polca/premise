"""
This module export a summary of scenario to an Excel file.
"""

import openpyxl
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.chart import AreaChart, LineChart, Reference
from openpyxl.utils import get_column_letter
from pathlib import Path
from . import DATA_DIR
import yaml
import string

IAM_ELEC_VARS = DATA_DIR / "electricity" / "electricity_tech_vars.yml"
IAM_FUELS_VARS = DATA_DIR / "fuels" / "fuel_tech_vars.yml"
IAM_BIOMASS_VARS = DATA_DIR / "electricity" / "biomass_vars.yml"
IAM_CEMENT_VARS = DATA_DIR / "cement" / "cement_tech_vars.yml"
IAM_STEEL_VARS = DATA_DIR / "steel" / "steel_tech_vars.yml"
IAM_OTHER_VARS = DATA_DIR / "utils" / "report" / "other_vars.yaml"
GAINS_TO_IAM_FILEPATH = DATA_DIR / "GAINS_emission_factors" / "GAINStoREMINDtechmap.csv"
GNR_DATA = DATA_DIR / "cement" / "additional_data_GNR.csv"
IAM_CARBON_CAPTURE_VARS = DATA_DIR / "utils" / "carbon_capture_vars.yml"
REPORT_METADATA_FILEPATH = DATA_DIR / "utils" / "report" / "report.yaml"
SECTORS = {
    "Population": (IAM_OTHER_VARS, ["Population"]),
    "GDP": (IAM_OTHER_VARS, ["GDP|PPP"]),
    "CO2": (IAM_OTHER_VARS, ["Emi|CO2"]),
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
}


def get_variables(
    fp,
):
    with open(fp, "r") as stream:
        out = yaml.safe_load(stream)

    return list(out.keys())


def generate_summary_report(scenarios: list, filename: Path) -> None:
    """
    Generate a summary report of the scenarios.
    """

    with open(REPORT_METADATA_FILEPATH, "r") as stream:
        metadata = yaml.safe_load(stream)

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    for sector, fp in SECTORS.items():

        if isinstance(fp, tuple):
            fp, vars = fp
        else:
            vars = get_variables(fp)

        ws = wb.create_sheet(sector)

        col, row = (1, 1)

        ws.cell(
            column=col,
            row=row,
            value=metadata[sector]["expl_text"],
        )

        scenario_list = []

        for s, scenario in enumerate(scenarios):

            if (scenario["model"], scenario["pathway"]) not in scenario_list:

                if "generation" in sector:
                    iam_data = scenario["iam data"].production_volumes
                elif "efficiency" in sector:
                    iam_data = scenario["iam data"].efficiency
                elif "CCS" in sector:
                    iam_data = scenario["iam data"].carbon_capture_rate * 100
                else:
                    iam_data = scenario["iam data"].other_vars

                col += s

                if s == 0:
                    offset = 0
                else:
                    offset = len(
                        iam_data.sel(
                            variables=[
                                v for v in vars
                                if v in iam_data.variables.values
                            ]
                        ).variables
                    )

                    if offset <= 10:
                        offset += 10
                    else:
                        offset += 5

                col += offset

                row = 3

                ws.cell(
                    column=col,
                    row=row,
                    value=f"{scenario['model'].upper()} - {scenario['pathway'].upper()}",
                )
                ws.cell(column=col, row=row).font = Font(
                    bold=True, size=14, underline="single"
                )

                row += 2

                for region in scenario["iam data"].regions:
                    ws.cell(column=col, row=row, value=region)

                    row += 1

                    df = iam_data.sel(
                        variables=[v for v in vars if v in iam_data.variables.values],
                        region=region,
                        year=[y for y in iam_data.coords["year"].values if y <= 2100],
                    )

                    if len(df) > 0:
                        df = df.to_dataframe("val")
                        df = df.unstack()["val"]
                        df = df.T
                        df = df.rename_axis(index=None)

                        data = dataframe_to_rows(df)

                        counter = 0
                        for r_idx, r in enumerate(data, 1):
                            if r != [None]:
                                for c_idx, value in enumerate(r, 1):
                                    ws.cell(
                                        row=row + counter,
                                        column=col + c_idx,
                                        value=value,
                                    )
                                counter += 1

                        values = Reference(
                            ws,
                            min_col=col + 2,
                            min_row=row,
                            max_col=col + c_idx,
                            max_row=row + counter - 1,
                        )
                        cats = Reference(
                            ws,
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
                        else:
                            chart = LineChart()

                        chart.add_data(values, titles_from_data=True)
                        chart.set_categories(cats)
                        chart.title = f"{region} - {sector}"
                        chart.y_axis.title = metadata[sector]["label"]
                        chart.height = 8
                        chart.width = 16
                        chart.anchor = f"{get_column_letter(col + 2)}{row + 1}"
                        ws.add_chart(chart)

                        row += counter + 2

                scenario_list.append((scenario["model"], scenario["pathway"]))

    wb.save(filename)
