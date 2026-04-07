import pandas as pd

from iamc_template_mappings import CROP_MAPPING
from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)


def run_crop(scenario_name):
    data_dir = get_data_dir(scenario_name)

    crop_land = pd.read_csv(data_dir / "land allocation by crop.csv")
    crop_emis = pd.read_csv(data_dir / "LUC emissions by LUT.csv")

    crop_emis["LandLeaf"] = crop_emis["LandLeaf"].str.split("_").str[0]
    crop_emis = crop_emis.groupby(["scenario", "region", "Year", "LandLeaf"])["value"].agg("sum").reset_index()

    crop_land["LandLeaf"] = crop_land["LandLeaf"].replace(CROP_MAPPING)
    crop_emis["LandLeaf"] = crop_emis["LandLeaf"].replace(CROP_MAPPING)
    crop_land = crop_land[crop_land["LandLeaf"].isin(CROP_MAPPING.values())]
    crop_emis = crop_emis[crop_emis["LandLeaf"].isin(CROP_MAPPING.values())]

    crop_land["value"] = crop_land["value"] * 1000 * 100
    crop_emis["value"] = crop_emis["value"] * 1000 * 44 / 12

    crop_land = aggregate_with_world(
        crop_land,
        ["scenario", "region", "LandLeaf", "Year"],
        ["scenario", "LandLeaf", "Year"],
    )
    crop_emis = aggregate_with_world(
        crop_emis,
        ["scenario", "region", "LandLeaf", "Year"],
        ["scenario", "LandLeaf", "Year"],
    )

    crop = pd.merge(
        crop_land,
        crop_emis,
        how="inner",
        on=["scenario", "region", "Year", "LandLeaf"],
        suffixes=("_land", "_emis"),
    )
    crop["value"] = crop["value_emis"] / crop["value_land"]

    crop_land = format_for_iamc(
        crop_land,
        scenario_name,
        "ha",
        "Land Use|Average|Biomass|" + crop_land["LandLeaf"],
    )
    crop = format_for_iamc(
        crop[["scenario", "region", "LandLeaf", "Year", "value"]],
        scenario_name,
        "kg CO2/ha",
        "Emission Factor|CO2|Land Use Change|Average|Biomass|" + crop["LandLeaf"],
    )

    combine_and_write(
        [pivot_iamc(crop_land), pivot_iamc(crop)],
        scenario_name,
        "iamc_template_gcam_crops.xlsx",
    )
