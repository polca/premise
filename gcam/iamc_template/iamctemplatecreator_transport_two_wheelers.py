
import pandas as pd

from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)


def run_two_wheelers(scenario_name):
    data_dir = get_data_dir(scenario_name)

    output_df = pd.read_csv(data_dir / "transport service output by tech.csv")
    input_df = pd.read_csv(data_dir / "transport final energy by tech and fuel.csv")

    output_df = output_df[output_df["subsector"].str.contains("2W")]
    input_df = input_df[input_df["mode"].str.contains("2W")]

    output_df = aggregate_with_world(
        output_df,
        ["Units", "scenario", "region", "sector", "subsector", "technology", "Year"],
        ["Units", "scenario", "sector", "subsector", "technology", "Year"],
    )
    input_df = aggregate_with_world(
        input_df,
        ["Units", "scenario", "region", "sector", "mode", "technology", "Year"],
        ["Units", "scenario", "sector", "mode", "technology", "Year"],
    )

    input_df = format_for_iamc(
        input_df,
        scenario_name,
        "EJ/yr",
        "Final Energy|Transport|Pass|Road|" + input_df["mode"] + "|" + input_df["technology"],
    )
    output_df = format_for_iamc(
        output_df,
        scenario_name,
        "million tkm/yr",
        "Distance|Transport|Pass|Road|" + output_df["subsector"] + "|" + output_df["technology"],
    )

    combine_and_write(
        [pivot_iamc(input_df), pivot_iamc(output_df)],
        scenario_name,
        "iamc_template_gcam_transport_two_wheelers.xlsx",
    )


if __name__ == "__main__":
    run_two_wheelers("ssp24p5tol5")