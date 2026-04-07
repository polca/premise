import re

import pandas as pd

from iamc_template_mappings import HEAT_FUEL_MAPPING
from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)


def run_heat(scenario_name):
    data_dir = get_data_dir(scenario_name)

    heat = pd.read_csv(data_dir / "heat by sector and fuel.csv")
    heat["sector"] = heat["sector"].map(
        lambda value: "Industry" if re.search("process heat", value) else "Buildings"
    )
    heat["input"] = heat["input"].replace(HEAT_FUEL_MAPPING)

    heat = aggregate_with_world(
        heat,
        ["Units", "scenario", "region", "sector", "input", "Year"],
        ["Units", "scenario", "sector", "input", "Year"],
    )
    heat = format_for_iamc(
        heat,
        scenario_name,
        "EJ",
        "Final Energy|Heat|" + heat["sector"] + "|" + heat["input"],
    )

    combine_and_write([pivot_iamc(heat)], scenario_name, "iamc_template_gcam_heat.xlsx")
