import pandas as pd

from iamc_template_mappings import (
  FUEL_GAS_TECH_MAPPING,
  FUEL_HYDROGEN_TECH_MAPPING,
  FUEL_LIQUIDS_TECH_MAPPING,
)
from iamc_template_utils import (
  aggregate_with_world,
  calculate_ratio,
  combine_and_write,
  format_for_iamc,
  get_data_dir,
  pivot_iamc,
)


def prepare_output(df: pd.DataFrame, group_columns: list[str], world_group_columns: list[str]) -> pd.DataFrame:
  return aggregate_with_world(df, group_columns, world_group_columns)


def prepare_input(df: pd.DataFrame, group_columns: list[str], world_group_columns: list[str]) -> pd.DataFrame:
  return aggregate_with_world(df, group_columns, world_group_columns)


def filter_energy_inputs(df: pd.DataFrame, technologies: pd.Series) -> pd.DataFrame:
  return df[df["technology"].isin(technologies) & (df["Units"] == "EJ")]

def run_fuel(scenario_name):
  data_dir = get_data_dir(scenario_name)

  liquids_production = pd.read_csv(data_dir / "refined liquids production by tech.csv")
  liquids_input = pd.read_csv(data_dir / "refined liquids inputs by tech.csv")
  gas_production = pd.read_csv(data_dir / "gas production by tech.csv")
  gas_input = pd.read_csv(data_dir / "gas inputs by tech.csv")
  hydrogen_production = pd.read_csv(data_dir / "hydrogen production by tech.csv")
  hydrogen_input = pd.read_csv(data_dir / "hydrogen inputs by tech.csv")

  liquids_production["technology"] = liquids_production["technology"].replace(FUEL_LIQUIDS_TECH_MAPPING)
  liquids_input["technology"] = liquids_input["technology"].replace(FUEL_LIQUIDS_TECH_MAPPING)
  gas_production["technology"] = gas_production["technology"].replace(FUEL_GAS_TECH_MAPPING)
  gas_input["technology"] = gas_input["technology"].replace(FUEL_GAS_TECH_MAPPING)
  hydrogen_production["technology"] = hydrogen_production["technology"].replace(FUEL_HYDROGEN_TECH_MAPPING)
  hydrogen_input["technology"] = hydrogen_input["technology"].replace(FUEL_HYDROGEN_TECH_MAPPING)

  liquids_production = prepare_output(
    liquids_production.dropna(subset=["technology"]),
    ["Units", "scenario", "region", "sector", "technology", "Year"],
    ["Units", "scenario", "sector", "technology", "Year"],
  )
  liquids_input = prepare_input(
    liquids_input,
    ["Units", "scenario", "region", "technology", "Year"],
    ["Units", "scenario", "technology", "Year"],
  )
  liquids_input = filter_energy_inputs(liquids_input, liquids_production["technology"].unique())
  liquids_eff = calculate_ratio(
    liquids_production,
    liquids_input,
    ["scenario", "region", "technology", "Year"],
  )

  gas_production = prepare_output(
    gas_production,
    ["Units", "scenario", "region", "sector", "technology", "Year"],
    ["Units", "scenario", "sector", "technology", "Year"],
  )
  gas_input = prepare_input(
    gas_input,
    ["Units", "scenario", "region", "technology", "Year"],
    ["Units", "scenario", "technology", "Year"],
  )
  gas_input = filter_energy_inputs(gas_input, gas_production["technology"].unique())
  gas_eff = calculate_ratio(
    gas_production,
    gas_input,
    ["scenario", "region", "technology", "Year"],
  )

  hydrogen_production = prepare_output(
    hydrogen_production,
    ["Units", "scenario", "region", "technology", "Year"],
    ["Units", "scenario", "technology", "Year"],
  )
  hydrogen_input = prepare_input(
    hydrogen_input,
    ["Units", "scenario", "region", "technology", "Year"],
    ["Units", "scenario", "technology", "Year"],
  )
  hydrogen_input = filter_energy_inputs(hydrogen_input, hydrogen_production["technology"].unique())
  hydrogen_eff = calculate_ratio(
    hydrogen_production,
    hydrogen_input,
    ["scenario", "region", "technology", "Year"],
  )

  liquids_production = format_for_iamc(
    liquids_production,
    scenario_name,
    "EJ/yr",
    "Secondary Energy|Production|Refined Liquids|" + liquids_production["technology"],
  )
  liquids_eff = format_for_iamc(
    liquids_eff,
    scenario_name,
    "unitless",
    "Efficiency|Refined Liquids|" + liquids_eff["technology"],
  )
  gas_production = format_for_iamc(
    gas_production,
    scenario_name,
    "EJ/yr",
    "Secondary Energy|Production|Natural Gas|" + gas_production["technology"],
  )
  gas_eff = format_for_iamc(
    gas_eff,
    scenario_name,
    "unitless",
    "Efficiency|Natural Gas|" + gas_eff["technology"],
  )
  hydrogen_production = format_for_iamc(
    hydrogen_production,
    scenario_name,
    "EJ/yr",
    "Secondary Energy|Production|Hydrogen|" + hydrogen_production["technology"],
  )
  hydrogen_eff = format_for_iamc(
    hydrogen_eff,
    scenario_name,
    "unitless",
    "Efficiency|Hydrogen|" + hydrogen_eff["technology"],
  )

  combine_and_write(
    [
      pivot_iamc(liquids_production),
      pivot_iamc(liquids_eff),
      pivot_iamc(gas_production),
      pivot_iamc(gas_eff),
      pivot_iamc(hydrogen_production),
      pivot_iamc(hydrogen_eff),
    ],
    scenario_name,
    "iamc_template_gcam_fuels.xlsx",
  )
