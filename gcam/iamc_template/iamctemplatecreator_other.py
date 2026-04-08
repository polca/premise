import pandas as pd

from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)


def run_other(scenario_name):
    data_dir = get_data_dir(scenario_name)

    population = pd.read_csv(data_dir / "population by region.csv")
    gdp = pd.read_csv(data_dir / "GDP per capita PPP by region.csv")
    gmst = pd.read_csv(data_dir / "global mean temperature.csv")
    co2 = pd.read_csv(data_dir / "CO2 emissions by region.csv")

    population["value"] = population["value"] / 1000
    population = aggregate_with_world(
        population,
        ["Units", "scenario", "region", "Year"],
        ["Units", "scenario", "Year"],
    )

    # GDP per capita is in thousand dollars, need in dollars
    gdp["value"] = gdp["value"] * 1000
    gdp = aggregate_with_world(
        gdp,
        ["Units", "scenario", "region", "Year"],
        ["Units", "scenario", "Year"],
    )


    gmst["region"] = "World"
    gmst = gmst[gmst["Year"].isin(population["Year"].unique())]
    # CO2 emissions are in MtC, need in MtCO2
    co2["value"] = co2["value"] * 44 / 12
    co2 = aggregate_with_world(
        co2,
        ["Units", "scenario", "region", "Year"],
        ["Units", "scenario", "Year"],
    )

    population = format_for_iamc(
        population,
        scenario_name,
        "thousand people",
        pd.Series("Population", index=population.index),
    )
    gdp = format_for_iamc(
        gdp,
        scenario_name,
        "1990 US$ per capita",
        pd.Series("GDP|PPP", index=gdp.index),
    )
    gmst = format_for_iamc(
        gmst,
        scenario_name,
        "degC",
        pd.Series("Temperature|Global Mean", index=gmst.index),
    )
    co2 = format_for_iamc(
        co2,
        scenario_name,
        "MtCO2/yr",
        pd.Series("Emissions|CO2", index=co2.index),
    )

    combine_and_write(
        [pivot_iamc(population), pivot_iamc(gdp), pivot_iamc(gmst), pivot_iamc(co2)],
        scenario_name,
        "iamc_template_gcam_other.xlsx",
    )
