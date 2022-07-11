"""
This module export a summary of scenario to an Excel file.
"""

import openpyxl
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.chart import AreaChart, Reference
from pathlib import Path
from . import DATA_DIR
import yaml

IAM_ELEC_VARS = DATA_DIR / "electricity" / "electricity_tech_vars.yml"
IAM_FUELS_VARS = DATA_DIR / "fuels" / "fuel_tech_vars.yml"
IAM_BIOMASS_VARS = DATA_DIR / "electricity" / "biomass_vars.yml"
IAM_CEMENT_VARS = DATA_DIR / "cement" / "cement_tech_vars.yml"
IAM_STEEL_VARS = DATA_DIR / "steel" / "steel_tech_vars.yml"
GAINS_TO_IAM_FILEPATH = DATA_DIR / "GAINS_emission_factors" / "GAINStoREMINDtechmap.csv"
GNR_DATA = DATA_DIR / "cement" / "additional_data_GNR.csv"
IAM_CARBON_CAPTURE_VARS = DATA_DIR / "utils" / "carbon_capture_vars.yml"
SECTORS = {
    "Electricity": IAM_ELEC_VARS,
    "Fuel": IAM_FUELS_VARS,
    "Cement": IAM_CEMENT_VARS,
    "Steel": IAM_CEMENT_VARS
}

def get_variables(fp):
    with open(fp, "r") as stream:
        out = yaml.safe_load(stream)
    return list(out.keys())


def generate_summary_report(scenarios: list, filename: Path) -> None:
    """
    Generate a summary report of the scenarios.
    """
    wb = openpyxl.Workbook()
    wb.remove(wb.active)


    for sector, fp in SECTORS.items():

        vars = get_variables(fp)

        ws = wb.create_sheet(sector)

        for scenario in scenarios:

            row = 1

            ws[f"A{row}"] = f"{scenario['model'].upper()} - {scenario['pathway'].upper()}"
            ws[f"A{row}"].font = Font(bold=True, size=14, underline="single")
            row += 2

            for region in scenario["iam data"].regions:
                ws[f"A{row}"] = region

                row += 1

                df = scenario["iam data"].production_volumes.sel(
                    variables=[v for v in vars if v in scenario["iam data"].production_volumes.variables.values],
                    region=region,
                    year=[y for y in scenario["iam data"].production_volumes.year.values if y <= 2100]
                ).to_dataframe("val").unstack()["val"].reset_index()

                df.index = range(2005, 2105, 5)

                df = df.T

                print(df)


                data = dataframe_to_rows(df)

                for r_idx, r in enumerate(data, 1):
                    for c_idx, value in enumerate(r, 1):
                        ws.cell(row=row + r_idx, column=c_idx, value=value)

                values = Reference(ws, min_col=2, min_row=row + 3, max_col=c_idx, max_row=row + r_idx)
                cats = Reference(ws, min_col=1, min_row=row + 3, max_row=row + r_idx)

                chart = AreaChart(grouping="stacked")
                chart.add_data(values)
                chart.set_categories(cats)
                chart.title = f"{region} - {sector}"
                ws.add_chart(chart, f"S{row}")

                row += r_idx + 2

    wb.save(filename)



