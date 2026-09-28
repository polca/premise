"""
module to adjust the battery inputs to reflect progress in
terms of cell energy density.

"""

import yaml

from .filesystem_constants import DATA_DIR
from .logger import create_logger
from .provenance import record_change_event
from .inventory_store import get_scenario_inventory, replace_scenario_inventory
from .transformation import BaseTransformation, IAMDataCollection, List, np, ws
from .validation import BatteryValidation
from .validation_framework import record_validation_phase

logger = create_logger("battery")


def load_cell_energy_density():
    """
    Load cell energy density data.
    """
    with open(DATA_DIR / "battery/energy_density.yaml", "r") as file:
        data = yaml.load(file, Loader=yaml.FullLoader)

    result = {}
    for key, value in data.items():
        names = value["ecoinvent_aliases"]["name"]
        if isinstance(names, list):
            for name in names:
                result[name] = value["target"]
        else:
            result[names] = value["target"]

    return result


def _update_battery(scenario, version, system_model):

    if (
        scenario["iam data"].battery_mobile_scenarios is None
        and scenario["iam data"].battery_stationary_scenarios is None
    ):
        print("No battery scenario data available -- skipping")
        return scenario

    battery = Battery(
        database=get_scenario_inventory(scenario),
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    battery.adjust_battery_mass()

    if (
        battery.iam_data.battery_mobile_scenarios is not None
        or battery.iam_data.battery_stationary_scenarios is not None
    ):
        battery.adjust_battery_market_shares()

    replace_scenario_inventory(scenario, battery.database)
    scenario["index"] = battery.index
    scenario["cache"] = battery.cache
    scenario.setdefault("mapping", {})["battery"] = {
        "transformed activities": list(battery._validation_targets.values())
    }

    validation = BatteryValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=battery.database,
        iam_data=scenario["iam data"],
    )
    record_validation_phase(scenario, validation.run_battery_checks())

    return scenario


class Battery(BaseTransformation):
    """
    Class that modifies the battery market to reflect progress
    in terms of cell energy density.

    """

    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        cache: dict = None,
        index: dict = None,
    ) -> None:
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
            cache,
            index,
        )
        self.system_model = system_model
        self._validation_targets = {}

    def adjust_battery_market_shares(self) -> None:
        """
        Based on scenario data, adjust the shares within the datasets:
        - market for battery capacity (MIX scenario)
        - market for battery capacity (LFP scenario)
        - market for battery capacity (NCx scenario)
        - market for battery capacity (PLiB scenario)
        """

        market_datasets_mobile = {
            "market for battery capacity (MIX scenario)": "MIX",
            "market for battery capacity (LFP scenario)": "LFP",
            "market for battery capacity (NCx scenario)": "NCX",
            "market for battery capacity (PLiB scenario)": "PLIB",
        }

        market_datasets_stationary = {
            "market for battery capacity, stationary (CONT scenario)": "cont",
            "market for battery capacity, stationary (TC scenario)": "tc",
        }

        datasets_mapping_mobile = {
            v: k
            for k, v in {
                "LAB": "market for battery capacity, Li-ion, Li-O2",
                "LFP": "market for battery capacity, Li-ion, LFP",
                "LSB": "market for battery capacity, Li-sulfur, Li-S",
                "NCA": "market for battery capacity, Li-ion, NCA",
                "NMC111": "market for battery capacity, Li-ion, NMC111",
                "NMC532": "market for battery capacity, Li-ion, NMC532",
                "NMC622": "market for battery capacity, Li-ion, NMC622",
                "NMC811": "market for battery capacity, Li-ion, NMC811",
                "NMC900-Si": "market for battery capacity, Li-ion, NMC955",
                "SIB": "market for battery capacity, Sodium-ion, SiB",
            }.items()
        }

        datasets_mapping_stationary = {
            v: k
            for k, v in {
                "LFP": "market for battery capacity, Li-ion, LFP, stationary",
                "NMC111": "market for battery capacity, Li-ion, NMC111, stationary",
                "NMC622": "market for battery capacity, Li-ion, NMC622, stationary",
                "NMC811": "market for battery capacity, Li-ion, NMC811, stationary",
                "VRFB": "market for battery capacity, redox-flow, Vanadium, stationary",
                "LEAD-ACID": "market for battery capacity, lead acid, rechargeable, stationary",
                "NAS": "market for battery capacity, Sodium-Nickel-Chloride, Na-NiCl, stationary",
            }.items()
        }

        self._adjust_shares(
            market_datasets_stationary, datasets_mapping_stationary, "stationary"
        )
        self._adjust_shares(market_datasets_mobile, datasets_mapping_mobile, "mobile")

    def _adjust_shares(self, market_datasets, datasets_mapping, market_type):
        """
        Adjust the shares within the datasets.
        """
        if market_type == "mobile":
            battery_scenarios = self.iam_data.battery_mobile_scenarios
        else:
            battery_scenarios = self.iam_data.battery_stationary_scenarios
        if battery_scenarios is None:
            return

        for ds in ws.get_many(
            self.database,
            ws.either(*[ws.equals("name", name) for name in market_datasets]),
        ):

            if self.year in battery_scenarios.year:
                shares = battery_scenarios.sel(
                    scenario=market_datasets[ds["name"]],
                    year=self.year,
                )
            elif self.year < min(battery_scenarios.year):
                shares = battery_scenarios.sel(
                    scenario=market_datasets[ds["name"]],
                    year=min(battery_scenarios.year),
                )
            elif self.year > max(battery_scenarios.year):
                shares = battery_scenarios.sel(
                    scenario=market_datasets[ds["name"]],
                    year=max(battery_scenarios.year),
                )
            else:
                shares = battery_scenarios.sel(
                    scenario=market_datasets[ds["name"]],
                ).interp(year=self.year)

            # replace NaNs with zeros
            shares = shares.fillna(0)

            for exc in ws.technosphere(ds):
                if exc["name"] in datasets_mapping:
                    exc["amount"] = shares.sel(
                        chemistry=datasets_mapping[exc["name"]]
                    ).values.item()

                    ds.setdefault("log parameters", {})[
                        f"{datasets_mapping[exc['name']]} market share"
                    ] = exc["amount"]

            supplier_exchanges = [
                exc
                for exc in ws.technosphere(ds)
                if exc.get("unit") == ds.get("unit")
                and np.isfinite(exc.get("amount", np.nan))
            ]
            total_share = sum(exc["amount"] for exc in supplier_exchanges)
            if total_share > 0:
                for exc in supplier_exchanges:
                    exc["amount"] /= total_share
                    if exc["name"] in datasets_mapping:
                        ds["log parameters"][
                            f"{datasets_mapping[exc['name']]} market share"
                        ] = exc["amount"]

            self.write_log(ds, status=f"modified ({market_type})")

    def adjust_battery_mass(self) -> None:
        """
        Adjust vehicle components (e.g., battery).
        Adjust the battery mass to reflect progress in battery technology.
        Specifically, we adjust the battery mass to reflect progress in
        terms of cell energy density.
        We leave the density unchanged after 2050.
        """

        energy_density = load_cell_energy_density()

        for ds in ws.get_many(
            self.database,
            ws.contains("name", "market for battery capacity"),
        ):
            if ds["name"] in energy_density:

                mean_2020_energy_density = energy_density[ds["name"]][2020]["mean"]
                minimum_2020_energy_density = energy_density[ds["name"]][2020][
                    "minimum"
                ]
                maximum_2020_energy_density = energy_density[ds["name"]][2020][
                    "maximum"
                ]
                mean_2050_energy_density = energy_density[ds["name"]][2050]["mean"]
                minimum_2050_energy_density = energy_density[ds["name"]][2050][
                    "minimum"
                ]
                maximum_2050_energy_density = energy_density[ds["name"]][2050][
                    "maximum"
                ]

                scaling_factor = mean_2020_energy_density / np.clip(
                    np.interp(
                        self.year,
                        [2020, 2050],
                        [mean_2020_energy_density, mean_2050_energy_density],
                    ),
                    0,
                    None,
                )

                scaling_factor_min = minimum_2020_energy_density / np.clip(
                    np.interp(
                        self.year,
                        [2020, 2050],
                        [minimum_2020_energy_density, minimum_2050_energy_density],
                    ),
                    0,
                    None,
                )

                scaling_factor_max = maximum_2020_energy_density / np.clip(
                    np.interp(
                        self.year,
                        [2020, 2050],
                        [maximum_2020_energy_density, maximum_2050_energy_density],
                    ),
                    0,
                    None,
                )

                ds.setdefault("log parameters", {})["battery input"] = [
                    e["name"]
                    for e in ws.technosphere(
                        ds, ws.contains("name", "market for battery")
                    )
                ][0]

                ds.setdefault("log parameters", {})["old battery mass"] = sum(
                    e["amount"]
                    for e in ws.technosphere(
                        ds, ws.contains("name", "market for battery")
                    )
                )

                for exc in ws.technosphere(ds, ws.equals("unit", "kilogram")):

                    exc["amount"] *= float(scaling_factor)
                    if exc.get("uncertainty type") == 5:
                        exc["loc"] *= float(scaling_factor)
                        exc["minimum"] *= float(scaling_factor_min)
                        exc["maximum"] *= float(scaling_factor_max)

                ds["log parameters"]["new battery mass"] = sum(
                    e["amount"]
                    for e in ws.technosphere(
                        ds, ws.contains("name", "market for battery")
                    )
                )

                self.write_log(ds, status="modified")

    def write_log(self, dataset, status):
        """Record a structured battery provenance event."""

        self._validation_targets[id(dataset)] = dataset
        record_change_event(self, dataset, status, sector="battery")
