import numpy as np
import pandas as pd

from iamc_template_mappings import (
    FINAL_ENERGY_BUILDINGS_SECTOR_MAPPING,
    FINAL_ENERGY_INDUSTRY_SECTOR_MAPPING,
    FINAL_ENERGY_INDUSTRY_SUBSECTOR_MAPPING,
    FINAL_ENERGY_INPUT_MAPPING,
    FINAL_ENERGY_TRANSPORT_SECTOR_MAPPING,
)
from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)


def _prepare_industry_final_energy(industry_fe: pd.DataFrame) -> pd.DataFrame:
    industry_fe["input"] = industry_fe["input"].replace(FINAL_ENERGY_INPUT_MAPPING)
    industry_fe["sector"] = industry_fe["sector"].replace(FINAL_ENERGY_INDUSTRY_SECTOR_MAPPING)
    industry_fe = industry_fe[industry_fe["sector"].notna()].copy()

    industry_fe["subsector_mapped"] = industry_fe["subsector"].replace(
        FINAL_ENERGY_INDUSTRY_SUBSECTOR_MAPPING
    )

    # Keep subsector detail only for iron and steel. For other sectors,
    # use a non-null bucket so groupby/world aggregation does not drop rows.
    steel_mask = industry_fe["sector"] == "Iron and Steel"
    industry_fe["subsector_group"] = "All"
    industry_fe.loc[steel_mask, "subsector_group"] = industry_fe.loc[
        steel_mask, "subsector_mapped"
    ].fillna("Other")

    industry_fe = aggregate_with_world(
        industry_fe,
        ["Units", "scenario", "region", "sector", "subsector_group", "input", "Year"],
        ["Units", "scenario", "sector", "subsector_group", "input", "Year"],
    )

    steel_mask = industry_fe["sector"] == "Iron and Steel"
    industry_fe["Variable"] = np.where(
        steel_mask,
        "Final Energy|Industry|"
        + industry_fe["sector"]
        + "|"
        + industry_fe["subsector_group"]
        + "|"
        + industry_fe["input"],
        "Final Energy|Industry|" + industry_fe["sector"] + "|" + industry_fe["input"],
    )
    return industry_fe


def run_final_energy(scenario_name):
    data_dir = get_data_dir(scenario_name)

    buildings_fe = pd.read_csv(data_dir / "buildings final energy by service and fuel.csv")
    industry_fe = pd.read_csv(data_dir / "industry final energy by tech and fuel.csv")
    transport_fe = pd.read_csv(data_dir / "transport final energy by tech and fuel.csv")

    # filter so year > 1975
    buildings_fe = buildings_fe[buildings_fe["Year"] > 1975]
    industry_fe = industry_fe[industry_fe["Year"] > 1975]
    transport_fe = transport_fe[transport_fe["Year"] > 1975]
    
    buildings_fe["input"] = buildings_fe["input"].replace(FINAL_ENERGY_INPUT_MAPPING)
    buildings_fe["sector"] = buildings_fe["sector"].replace(FINAL_ENERGY_BUILDINGS_SECTOR_MAPPING)
    buildings_fe = aggregate_with_world(
        buildings_fe,
        ["Units", "scenario", "region", "sector", "input", "Year"],
        ["Units", "scenario", "sector", "input", "Year"],
    )
    buildings_fe = format_for_iamc(
        buildings_fe,
        scenario_name,
        "EJ/yr",
        "Final Energy|Buildings|" + buildings_fe["sector"] + "|" + buildings_fe["input"],
    )

    industry_fe = _prepare_industry_final_energy(industry_fe)
    industry_fe = format_for_iamc(
        industry_fe,
        scenario_name,
        "EJ/yr",
        industry_fe["Variable"],
    )

    transport_fe["input"] = transport_fe["input"].replace(FINAL_ENERGY_INPUT_MAPPING)
    transport_fe["sector"] = transport_fe["sector"].replace(FINAL_ENERGY_TRANSPORT_SECTOR_MAPPING)
    transport_fe = aggregate_with_world(
        transport_fe,
        ["Units", "scenario", "region", "sector", "mode", "technology", "input", "Year"],
        ["Units", "scenario", "sector", "mode", "technology", "input", "Year"],
    )
    transport_fe = format_for_iamc(
        transport_fe,
        scenario_name,
        "EJ/yr",
        "Final Energy|Transport|"
        + transport_fe["sector"]
        + "|"
        + transport_fe["mode"]
        + "|"
        + transport_fe["technology"]
        + "|"
        + transport_fe["input"],
    )

    combine_and_write(
        [pivot_iamc(buildings_fe), pivot_iamc(industry_fe), pivot_iamc(transport_fe)],
        scenario_name,
        "iamc_template_gcam_final_energy.xlsx",
    )