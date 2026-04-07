import pandas as pd

from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)

def run_biomass(scenario_name):
    data_dir = get_data_dir(scenario_name)

    biofuel_output = pd.read_csv(data_dir / "purpose-grown biomass production.csv")
    residue_output = pd.read_csv(data_dir / "residue biomass production.csv")

    biofuel_output = aggregate_with_world(
        biofuel_output,
        ["Units", "scenario", "region", "sector", "Year"],
        ["Units", "scenario", "sector", "Year"],
    )
    residue_output = aggregate_with_world(
        residue_output,
        ["Units", "scenario", "region", "sector", "Year"],
        ["Units", "scenario", "sector", "Year"],
    )

    biofuel_output = format_for_iamc(
        biofuel_output,
        scenario_name,
        "EJ/yr",
        pd.Series("Production|Energy|Biomass|Energy Crops", index=biofuel_output.index),
    )
    residue_output = format_for_iamc(
        residue_output,
        scenario_name,
        "EJ/yr",
        pd.Series("Production|Energy|Biomass|Residues", index=residue_output.index),
    )

    combine_and_write(
        [pivot_iamc(biofuel_output), pivot_iamc(residue_output)],
        scenario_name,
        "iamc_template_gcam_biomass.xlsx",
    )

