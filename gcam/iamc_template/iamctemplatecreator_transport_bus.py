
import pandas as pd

from iamc_template_utils import (
    aggregate_with_world,
    combine_and_write,
    format_for_iamc,
    get_data_dir,
    pivot_iamc,
)


def run_bus(scenario_name):
    data_dir = get_data_dir(scenario_name)

    bus_output = pd.read_csv(data_dir / "transport service output by tech.csv")
    bus_input = pd.read_csv(data_dir / "transport final energy by tech and fuel.csv")

    bus_output = bus_output[bus_output["subsector"] == "Bus"]
    bus_input = bus_input[bus_input["mode"] == "Bus"]

    bus_output = aggregate_with_world(
        bus_output,
        ["Units", "scenario", "region", "sector", "subsector", "technology", "Year"],
        ["Units", "scenario", "sector", "subsector", "technology", "Year"],
    )
    bus_input = aggregate_with_world(
        bus_input,
        ["Units", "scenario", "region", "sector", "mode", "technology", "Year"],
        ["Units", "scenario", "sector", "mode", "technology", "Year"],
    )

    bus_input = format_for_iamc(
        bus_input,
        scenario_name,
        "EJ/yr",
        "Final Energy|Transport|Pass|Road|" + bus_input["mode"] + "|" + bus_input["technology"],
    )
    bus_output = format_for_iamc(
        bus_output,
        scenario_name,
        "million pkm/yr",
        "Distance|Transport|Pass|Road|"
        + bus_output["subsector"]
        + "|"
        + bus_output["technology"],
    )

    combine_and_write(
        [pivot_iamc(bus_input), pivot_iamc(bus_output)],
        scenario_name,
        "iamc_template_gcam_transport_bus.xlsx",
    )