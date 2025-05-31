from collections import defaultdict

from .utils import (
    fetch_mapping,
    adjust_electrolysis_electricity_requirement,
)
from .config import HYDROGEN_SOURCES, HYDROGEN_SUPPLY_LOSSES, SUPPLY_CHAIN_SCENARIOS
from ..transformation import ws, uuid, np

hydrogen_parameters = fetch_mapping(HYDROGEN_SOURCES)


class HydrogenMixin:
    def generate_hydrogen_activities(self):

        self._regionalize_hydrogen_activities()
        self._generate_supporting_hydrogen_datasets()

    def _regionalize_hydrogen_activities(self):

        hydrogen_map = {
            k: v
            for k, v in self.fuel_map.items()
            if k.startswith("hydrogen")
        }

        self.process_and_add_activities(
            mapping=hydrogen_map,
            production_volumes=self.iam_data.production_volumes,
            efficiency_adjustment_fn=self._adjust_hydrogen_efficiency,
        )

        # Create markets for hydrogen
        self.process_and_add_markets(
            name="market for hydrogen, gaseous, low pressure",
            reference_product="hydrogen, gaseous, low pressure",
            unit="kilogram",
            mapping={
                k: v
                for k, v in self.fuel_map.items()
                if k.startswith("hydrogen")
            },
            system_model=self.system_model,
            production_volumes=self.iam_data.production_volumes,
            additional_exchanges_fn=self._add_transport_to_hydrogen_datasets
        )

    def _adjust_hydrogen_efficiency(self, dataset, technology):
        """
        Adjust the efficiency of hydrogen production datasets based on the technology.
        """

        params = hydrogen_parameters.get(technology, {})
        feedstock_name = params.get("feedstock name")
        feedstock_unit = params.get("feedstock unit")
        efficiency = params.get("efficiency")
        floor_value = params.get("floor value")

        if params:
            initial_energy_use = sum(
                exc["amount"]
                for exc in dataset["exchanges"]
                if exc["unit"] == feedstock_unit
                and feedstock_name in exc["name"]
                and exc["type"] != "production"
            )

            dataset.setdefault("log parameters", {}).update(
                {
                    "initial energy input for hydrogen production": initial_energy_use
                }
            )

            (
                new_energy_use,
                min_energy_use,
                max_energy_use,
                scaling_factor,
            ) = (
                None,
                None,
                None,
                1,
            )

            if (
                    technology
                    in self.fuel_efficiencies.variables.values.tolist()
            ):

                scaling_factor = 1 / self.find_iam_efficiency_change(
                    data=self.fuel_efficiencies,
                    variable=technology,
                    location=dataset["location"],
                )
                new_energy_use = max(
                    scaling_factor * initial_energy_use, floor_value
                )

            if scaling_factor == 1 and "electrolysis" in technology:
                new_energy_use, min_energy_use, max_energy_use = (
                    adjust_electrolysis_electricity_requirement(
                        self.year, efficiency
                    )
                )

            try:
                scaling_factor = new_energy_use / initial_energy_use
            except (ZeroDivisionError, TypeError):
                scaling_factor = 1

            if scaling_factor != 1:
                for exc in ws.technosphere(
                        dataset,
                        ws.contains("name", feedstock_name),
                        ws.equals("unit", feedstock_unit),
                ):
                    exc["amount"] *= scaling_factor
                    exc["uncertainty type"] = 5
                    exc["loc"] = exc["amount"]
                    if min_energy_use is not None:
                        exc["minimum"] = exc["amount"] * (
                                min_energy_use / new_energy_use
                        )
                    if max_energy_use is not None:
                        exc["maximum"] = exc["amount"] * (
                                max_energy_use / new_energy_use
                        )

                dataset.setdefault("log parameters", {}).update(
                    {"new energy input for hydrogen production": new_energy_use}
                )


    def _generate_supporting_hydrogen_datasets(self):
        keywords = [
            "hydrogen transport, distributed by pipeline",
        ]

        hydrogen_distribution_map = {
            k: [ws.get_one(
                self.database,
                ws.equals("name", k)
            )]
            for k in keywords
        }

        self.process_and_add_activities(
            mapping=hydrogen_distribution_map,
        )

    def _add_transport_to_hydrogen_datasets(self, dataset):

        dataset["exchanges"].append(
            {
                "name": "hydrogen transport, distributed by pipeline",
                "reference product": "hydrogen, gaseous, from pipeline",
                "location": dataset["location"],
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 1,
            }
        )


