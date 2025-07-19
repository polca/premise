"""
Integrates projections regarding steel production.
"""

from typing import List
from collections import defaultdict

from .data_collection import IAMDataCollection
from .logger import create_logger
from .transformation import BaseTransformation, ws
from .utils import rescale_exchanges
from .validation import SteelValidation
from .activity_maps import InventorySet

logger = create_logger("steel")


def _update_steel(scenario, version, system_model):

    if scenario["iam data"].steel_technology_mix is None:
        print("No steel scenario data available -- skipping")
        return scenario

    steel = Steel(
        database=scenario["database"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    steel.create_pig_iron_production_activities()
    steel.create_pig_iron_markets()
    steel.create_steel_production_activities()
    steel.create_steel_markets()
    steel.relink_datasets()
    scenario["database"] = steel.database
    scenario["cache"] = steel.cache
    scenario["index"] = steel.index
    validate = SteelValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=steel.database,
        iam_data=scenario["iam data"],
        system_model=system_model,
    )
    validate.run_steel_checks()

    return scenario


class Steel(BaseTransformation):
    """
    Class that modifies steel markets in ecoinvent based on IAM output data.

    :ivar database: database dictionary from :attr:`.NewDatabase.database`
    :ivar model: can be 'remind' or 'image'. str from :attr:`.NewDatabase.model`
    :ivar iam_data: xarray that contains IAM data, from :attr:`.NewDatabase.rdc`
    :ivar year: year, from :attr:`.NewDatabase.year`

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
        self.version = version
        self.inv = InventorySet(self.database, self.version, self.model)
        self.steel_map = self.inv.generate_steel_map()

    def create_steel_markets(self):
        """
        Create steel markets for different regions

        :return: Does not return anything. Adds new markets to database.
        """

        for ds in ws.get_many(
            self.database,
            ws.contains("name", "market for steel"),
            ws.equals("unit", "kilogram"),
            ws.contains("reference product", "steel"),
            ws.doesnt_contain_any("name", ["chromium", "rolled", "removed", "residue"]),
        ):

            if ds.get("regionalized", False) is True:
                continue

            self.process_and_add_markets(
                name=ds["name"],
                reference_product=ds["reference product"],
                unit=ds["unit"],
                mapping=self.steel_map,
                production_volumes=self.iam_data.production_volumes,
                system_model=self.system_model,
                blacklist={
                    "consequential": [
                        "steel - secondary",
                    ]
                },
            )

    def create_steel_production_activities(self):
        """
        Create steel production activities for different regions.

        """
        # Determine all steel activities in the database.
        # Empty old datasets.

        self.process_and_add_activities(
            efficiency_adjustment_fn=self.adjust_process_efficiency,
            mapping=self.steel_map,
        )

        # make other steel datasets region-specific
        steel_datasets = ws.get_many(
            self.database,
            ws.contains("name", "steel production"),
            ws.contains("reference product", "steel"),
            ws.equals("unit", "kilogram"),
        )
        steel_datasets = {
            "other": [
                ds
                for ds in steel_datasets
                if not any(ds in sublist for sublist in self.steel_map.values())
            ]
        }

        self.process_and_add_activities(
            mapping=steel_datasets,
        )

    def create_pig_iron_production_activities(self):
        """
        Create region-specific pig iron production activities.
        """

        pig_iron = {
            "pig iron": [
                ds
                for ds in self.database
                if ds["name"].startswith("pig iron production")
                and ds["unit"] == "kilogram"
                and ds["reference product"] == "pig iron"
                and ds.get("regionalized", False) is False
            ]
        }

        self.process_and_add_activities(
            mapping=pig_iron,
        )

    def create_pig_iron_markets(self):
        """
        Create region-specific pig iron markets.
        Adds datasets to the database.
        """

        self.process_and_add_markets(
            name="market for pig iron",
            reference_product="pig iron",
            unit="kilogram",
            mapping={
                "pig iron": [
                    ds
                    for ds in self.database
                    if ds["name"] == "pig iron production"
                    and ds["unit"] == "kilogram"
                    and ds["reference product"] == "pig iron"
                ]
            },
        )

    def adjust_process_efficiency(self, dataset, sector):
        """
        Scale down fuel exchanges in the given datasets, according to efficiency improvement as
        forecast by the IAM.

        :param datasets: A dictionary of datasets to modify.
        :type datasets: dict
        :return: The modified datasets.
        :rtype: dict
        """
        list_fuels = [
            "diesel",
            "coal",
            "lignite",
            "coke",
            "fuel",
            "meat",
            "gas",
            "oil",
            "electricity",
            "natural gas",
            "steam",
        ]

        scaling_factor = 1 / self.find_iam_efficiency_change(
            data=self.iam_data.steel_technology_efficiencies,
            variable=sector,
            location=dataset["location"],
        )

        if scaling_factor != 1 and scaling_factor > 0:
            # when sector is steel - secondary, we want to make sure
            # that the scaling down will not bring electricity consumption
            # below the minimum value of 0.444 kWh/kg (1.6 MJ/kg)
            # see Theoretical Minimum Energies To Produce Steel for Selected Conditions
            # US Department of Energy, 2000

            if sector == "steel - secondary":
                electricity = sum(
                    exc["amount"]
                    for exc in ws.technosphere(dataset)
                    if exc["unit"] == "kilowatt hour"
                )

                if electricity > 0:
                    scaling_factor = max(0.444 / electricity, scaling_factor)

            # if pig iron production, we want to make sure
            # that the scaling down will not bring energy consumption
            # below the minimum value of 9.0 MJ/kg
            # see Theoretical Minimum Energies To Produce Steel for Selected Conditions
            # US Department of Energy, 2000

            if dataset["name"].startswith("pig iron production"):
                energy = sum(
                    exc["amount"]
                    for exc in ws.technosphere(dataset)
                    if exc["unit"] == "megajoule"
                )

                # add input of coal
                energy += sum(
                    exc["amount"] * 26.4
                    for exc in ws.technosphere(dataset)
                    if "hard coal" in exc["name"] and exc["unit"] == "kilogram"
                )

                # add input of natural gas
                energy += sum(
                    exc["amount"] * 36
                    for exc in ws.technosphere(dataset)
                    if "natural gas" in exc["name"] and exc["unit"] == "cubic meter"
                )

                scaling_factor = max(9.0 / energy, scaling_factor)

            # Scale down the fuel exchanges using the scaling factor
            rescale_exchanges(
                dataset,
                scaling_factor,
                technosphere_filters=[
                    ws.either(*[ws.contains("name", x) for x in list_fuels])
                ],
                biosphere_filters=[ws.contains("name", "Carbon dioxide, fossil")],
            )

            # Update the comments
            text = (
                f"This dataset has been modified by `premise`, according to the performance "
                f"for steel production indicated by the IAM model {self.model.upper()} for the IAM "
                f"region {dataset['location']} in {self.year}, following the scenario {self.scenario}. "
                f"The energy efficiency of the process has been improved by {int((1 - scaling_factor) * 100)}%."
            )
            dataset["comment"] = text + dataset["comment"]

            if "log parameters" not in dataset:
                dataset["log parameters"] = {}

            dataset["log parameters"].update(
                {
                    "thermal efficiency change": scaling_factor,
                }
            )

        return dataset

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('carbon capture rate', '')}|"
            f"{dataset.get('log parameters', {}).get('thermal efficiency change', '')}|"
            f"{dataset.get('log parameters', {}).get('primary steel share', '')}|"
            f"{dataset.get('log parameters', {}).get('secondary steel share', '')}"
        )
