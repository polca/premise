import pandas as pd

from iamc_template_mappings import CEMENT_TECH_MAPPING, INDUSTRY_INPUT_MAPPING
from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)

def run_cement(scenario_name):
    data_dir = get_data_dir(scenario_name)

    cement_output = pd.read_csv(data_dir / "cement production by tech.csv")
    cement_input = pd.read_csv(data_dir / "cement final energy by tech and fuel.csv")

    cement_output["technology"] = cement_output["technology"].replace(CEMENT_TECH_MAPPING)
    cement_output = aggregate_with_world(
        cement_output,
        ["Units", "scenario", "region", "sector", "subsector", "technology", "Year"],
        ["Units", "scenario", "sector", "subsector", "technology", "Year"],
    )

    cement_input["input"] = cement_input["input"].replace(INDUSTRY_INPUT_MAPPING)
    cement_input = aggregate_with_world(
        cement_input,
        ["Units", "scenario", "region", "sector", "subsector", "technology", "input", "Year"],
        ["Units", "scenario", "sector", "subsector", "technology", "input", "Year"],
    )

    allocation = cement_input[cement_input["technology"].isin(["cement", "cement CCS"])].copy()
    allocation = allocation.groupby(["region", "technology", "Year"])["value"].agg("sum").reset_index()
    allocation["percentage"] = allocation["value"] / allocation.groupby(["region", "Year"])["value"].transform("sum")

    other_inputs = cement_input[~cement_input["technology"].isin(["cement", "cement CCS"])].copy()
    other_inputs = other_inputs.merge(
        allocation[["region", "Year", "technology", "percentage"]],
        on=["region", "Year"],
        how="left",
    )
    other_inputs["value"] = other_inputs["value"] * other_inputs["percentage"]
    other_inputs = other_inputs[
        ["Units", "scenario", "region", "sector", "subsector", "technology_y", "input", "Year", "value"]
    ].rename(columns={"technology_y": "technology"})

    cement_input = pd.concat(
        [cement_input[cement_input["technology"].isin(["cement", "cement CCS"])], other_inputs],
        axis=0,
        ignore_index=True,
    )
    cement_input["technology"] = cement_input["technology"].replace(CEMENT_TECH_MAPPING)

    cement_output = format_for_iamc(
        cement_output,
        scenario_name,
        "Mt/yr",
        "Production|Industry|Cement|" + cement_output["technology"],
    )
    cement_input = format_for_iamc(
        cement_input,
        scenario_name,
        "EJ/yr",
        "Final Energy|Industry|Cement|" + cement_input["technology"] + "|" + cement_input["input"],
    )

    combine_and_write(
        [pivot_iamc(cement_input), pivot_iamc(cement_output)],
        scenario_name,
        "iamc_template_gcam_cement.xlsx",
    )