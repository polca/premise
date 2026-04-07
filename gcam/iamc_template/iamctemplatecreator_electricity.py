import pandas as pd

from iamc_template_mappings import ELECTRICITY_TECH_MAPPING
from iamc_template_utils import (
  aggregate_with_world,
  calculate_ratio,
  combine_and_write,
  format_for_iamc,
  get_data_dir,
  pivot_iamc,
)

def run_electricity(scenario_name):
  data_dir = get_data_dir(scenario_name)

  elec_gen = pd.read_csv(data_dir / "elec gen by gen tech.csv")
  elec_input = pd.read_csv(data_dir / "elec energy input by elec gen tech.csv")

  elec_gen["technology"] = elec_gen["technology"].replace(ELECTRICITY_TECH_MAPPING)
  elec_input["technology"] = elec_input["technology"].replace(ELECTRICITY_TECH_MAPPING)

  elec_gen = aggregate_with_world(
    elec_gen.dropna(subset=["technology"]),
    ["Units", "scenario", "region", "technology", "Year"],
    ["Units", "scenario", "technology", "Year"],
  )
  elec_input = aggregate_with_world(
    elec_input.dropna(subset=["technology"]),
    ["Units", "scenario", "region", "technology", "Year"],
    ["Units", "scenario", "technology", "Year"],
  )
  elec_eff = calculate_ratio(
    elec_gen,
    elec_input,
    ["scenario", "region", "technology", "Year"],
  )

  elec_gen = format_for_iamc(
    elec_gen,
    scenario_name,
    "EJ/yr",
    "Secondary Energy|Electricity|" + elec_gen["technology"],
  )
  elec_eff = format_for_iamc(
    elec_eff,
    scenario_name,
    "unitless",
    "Efficiency|Electricity|" + elec_eff["technology"],
  )

  combine_and_write(
    [pivot_iamc(elec_gen), pivot_iamc(elec_eff)],
    scenario_name,
    "iamc_template_gcam_electricity.xlsx",
  )
