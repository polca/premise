import pandas as pd

from iamc_template_mappings import CDR_TECH_MAPPING
from iamc_template_utils import (
  aggregate_with_world,
  combine_and_write,
  format_for_iamc,
  get_data_dir,
  pivot_iamc,
)

def run_cdr(scenario_name):
  data_dir = get_data_dir(scenario_name)

  cdr = pd.read_csv(data_dir / "co2 sequestration.csv")
  cdr_energy = pd.read_csv(data_dir / "cdr final energy.csv")

  cdr["technology"] = cdr["technology"].replace(CDR_TECH_MAPPING)
  cdr_energy["technology"] = cdr_energy["technology"].replace(CDR_TECH_MAPPING)

  cdr = aggregate_with_world(
    cdr,
    ["Units", "scenario", "region", "sector", "subsector", "technology", "Year"],
    ["Units", "scenario", "sector", "subsector", "technology", "Year"],
  )
  cdr_energy = aggregate_with_world(
    cdr_energy,
    ["Units", "scenario", "region", "sector", "subsector", "technology", "Year"],
    ["Units", "scenario", "sector", "subsector", "technology", "Year"],
  )

  cdr = format_for_iamc(
    cdr,
    scenario_name,
    "MtC/yr",
    "Carbon Sequestration|Direct Air Capture|" + cdr["technology"],
  )
  cdr_energy = format_for_iamc(
    cdr_energy,
    scenario_name,
    "EJ/yr",
    "Final Energy|Carbon Management|Direct Air Capture|" + cdr_energy["technology"],
  )

  combine_and_write(
    [pivot_iamc(cdr), pivot_iamc(cdr_energy)],
    scenario_name,
    "iamc_template_gcam_carbon_dioxide_removal.xlsx",
  )
