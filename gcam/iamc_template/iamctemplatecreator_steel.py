import numpy as np
import pandas as pd

from iamc_template_mappings import INDUSTRY_INPUT_MAPPING
from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)

def run_steel(scenario_name):
    data_dir = get_data_dir(scenario_name)

    steel_output = pd.read_csv(data_dir / "iron and steel production by tech.csv")
    steel_input = pd.read_csv(data_dir / "iron and steel final energy by tech and fuel.csv")

    steel_output["subsector"] = np.where(
        steel_output["subsector"].str.contains("BLASTFUR"),
        "Primary",
        "Secondary",
    )
    steel_output = aggregate_with_world(
        steel_output,
        ["Units", "scenario", "region", "sector", "subsector", "technology", "Year"],
        ["Units", "scenario", "sector", "subsector", "technology", "Year"],
    )

    steel_input["subsector"] = np.where(
        steel_input["subsector"].str.contains("BLASTFUR"),
        "Primary",
        "Secondary",
    )
    steel_input["input"] = steel_input["input"].replace(INDUSTRY_INPUT_MAPPING)
    steel_input = aggregate_with_world(
        steel_input,
        ["Units", "scenario", "region", "sector", "subsector", "technology", "input", "Year"],
        ["Units", "scenario", "sector", "subsector", "technology", "input", "Year"],
    )

    steel_output = format_for_iamc(
        steel_output,
        scenario_name,
        "Mt/yr",
        "Production|Industry|Steel|" + steel_output["subsector"] + "|" + steel_output["technology"],
    )
    steel_input = format_for_iamc(
        steel_input,
        scenario_name,
        "EJ/yr",
        "Final Energy|Industry|Steel|" + steel_input["subsector"] + "|" + steel_input["input"],
    )

    combine_and_write(
        [pivot_iamc(steel_input), pivot_iamc(steel_output)],
        scenario_name,
        "iamc_template_gcam_steel.xlsx",
    )


