"""
Integrates projections regarding steel production.
"""

from typing import List

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

    steel.generate_activities()
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

    def generate_activities(self):
        """
        This function generates new activities for primary and secondary steel
        production and add them to the wurst database.

        :return: Returns a modified database with newly added steel activities for the corresponding year
        """

        self.create_pig_iron_production_activities()
        self.create_pig_iron_markets()
        self.create_steel_markets()
        self.create_steel_production_activities()

    def create_steel_markets(self):
        """
        Create steel markets for different regions

        :return: Does not return anything. Adds new markets to database.
        """

        steel_markets_to_create = (
            ("market for steel, low-alloyed", "steel, low-alloyed"),
            ("market for steel, unalloyed", "steel, unalloyed"),
            ("market for steel, chromium steel 18/8", "steel, chromium steel 18/8"),
        )

        list_new_steel_markets = []

        for market, steel_product in steel_markets_to_create:
            steel_markets = self.fetch_proxies(
                name=market,
                ref_prod=steel_product,
                production_variable=["steel - primary", "steel - secondary"],
            )

            # adjust share of primary and secondary steel
            if market == "market for steel, low-alloyed":
                for loc, dataset in steel_markets.items():
                    if loc != "World":
                        # check that the production volumes are positive
                        # otherwise we skip the region
                        if self.year in self.iam_data.production_volumes.year.values:
                            prod_vol = self.iam_data.production_volumes.sel(
                                region=loc,
                                variables=["steel - primary", "steel - secondary"],
                                year=self.year,
                            ).sum(dim="variables")

                        else:
                            prod_vol = (
                                self.iam_data.production_volumes.sel(
                                    region=loc,
                                    variables=["steel - primary", "steel - secondary"],
                                )
                                .interp(year=self.year)
                                .sum(dim="variables")
                            )

                        if prod_vol <= 0:
                            continue

                        if self.system_model != "consequential":
                            try:
                                if (
                                    self.year
                                    in self.iam_data.production_volumes.coords[
                                        "year"
                                    ].values
                                ):
                                    primary_share = (
                                        self.iam_data.production_volumes.sel(
                                            region=loc,
                                            variables="steel - primary",
                                            year=self.year,
                                        ).values.item(0)
                                        / self.iam_data.production_volumes.sel(
                                            region=loc,
                                            variables=[
                                                "steel - primary",
                                                "steel - secondary",
                                            ],
                                            year=self.year,
                                        )
                                        .sum(dim="variables")
                                        .values.item(0)
                                    )
                                else:
                                    primary_share = (
                                        self.iam_data.production_volumes.sel(
                                            region=loc, variables="steel - primary"
                                        )
                                        .interp(year=self.year)
                                        .values.item(0)
                                        / self.iam_data.production_volumes.sel(
                                            region=loc,
                                            variables=[
                                                "steel - primary",
                                                "steel - secondary",
                                            ],
                                        )
                                        .interp(year=self.year)
                                        .sum(dim="variables")
                                        .values.item(0)
                                    )
                            except KeyError:
                                primary_share = 1
                        else:
                            primary_share = 1

                        secondary_share = 1 - primary_share

                        new_exc = [
                            {
                                "uncertainty type": 0,
                                "loc": primary_share,
                                "amount": primary_share,
                                "type": "technosphere",
                                "production volume": 1,
                                "product": steel_product,
                                "name": "steel production, converter, low-alloyed",
                                "unit": "kilogram",
                                "location": loc,
                            }
                        ]

                        if secondary_share > 0:
                            new_exc.append(
                                {
                                    "uncertainty type": 0,
                                    "loc": secondary_share,
                                    "amount": secondary_share,
                                    "type": "technosphere",
                                    "production volume": 1,
                                    "product": steel_product,
                                    "name": "steel production, electric, low-alloyed",
                                    "unit": "kilogram",
                                    "location": loc,
                                },
                            )

                        dataset["exchanges"] = [
                            e
                            for e in dataset["exchanges"]
                            if e["type"] == "production" or e["unit"] == "ton kilometer"
                        ]
                        dataset["exchanges"].extend(new_exc)

                        dataset["log parameters"] = {
                            "primary steel share": primary_share,
                            "secondary steel share": secondary_share,
                        }

                # check that the production volumes are positive
                # otherwise we remove the region dataset from steel_markets
                steel_markets = {
                    loc: dataset
                    for loc, dataset in steel_markets.items()
                    if (
                        self.iam_data.production_volumes.sel(
                            region=loc,
                            variables=["steel - primary", "steel - secondary"],
                        )
                        .interp(year=self.year)
                        .sum(dim="variables")
                        > 0
                        or loc == "World"
                    )
                }
            else:
                for loc, dataset in steel_markets.items():
                    if loc != "World":
                        name_ref = [
                            (e["name"], e.get("product"))
                            for e in dataset["exchanges"]
                            if "steel production" in e["name"]
                        ][0]
                        name, ref = name_ref

                        dataset["exchanges"] = [
                            e
                            for e in dataset["exchanges"]
                            if e["type"] == "production" or e["unit"] == "ton kilometer"
                        ]

                        dataset["exchanges"].append(
                            {
                                "uncertainty type": 0,
                                "loc": 1,
                                "amount": 1,
                                "type": "technosphere",
                                "production volume": 1,
                                "product": ref,
                                "name": name,
                                "unit": "kilogram",
                                "location": loc,
                            }
                        )

            # populate World dataset
            steel_markets["World"]["exchanges"] = [
                x
                for x in steel_markets["World"]["exchanges"]
                if x["type"] == "production"
            ]
            regions = [r for r in self.regions if r != "World"]

            for region in regions:
                try:
                    if (
                        self.year
                        in self.iam_data.production_volumes.coords["year"].values
                    ):
                        share = (
                            self.iam_data.production_volumes.sel(
                                variables=["steel - primary", "steel - secondary"],
                                region=region,
                                year=self.year,
                            ).sum(dim="variables")
                            / self.iam_data.production_volumes.sel(
                                variables=["steel - primary", "steel - secondary"],
                                region=[
                                    x
                                    for x in self.iam_data.production_volumes.region.values
                                    if x != "World"
                                ],
                                year=self.year,
                            ).sum(dim=["variables", "region"])
                        ).values.item(0)
                    else:
                        share = (
                            self.iam_data.production_volumes.sel(
                                variables=["steel - primary", "steel - secondary"],
                                region=region,
                            )
                            .interp(year=self.year)
                            .sum(dim="variables")
                            / self.iam_data.production_volumes.sel(
                                variables=["steel - primary", "steel - secondary"],
                                region=[
                                    x
                                    for x in self.iam_data.production_volumes.region.values
                                    if x != "World"
                                ],
                            )
                            .interp(year=self.year)
                            .sum(dim=["variables", "region"])
                        ).values.item(0)

                except KeyError:
                    # equal share to all regions
                    share = 1 / len(regions)

                if share > 0:
                    steel_markets["World"]["exchanges"].append(
                        {
                            "name": market,
                            "product": steel_product,
                            "amount": share,
                            "unit": "kilogram",
                            "type": "technosphere",
                            "location": region,
                        }
                    )

            # add to log
            for new_dataset in list(steel_markets.values()):
                self.write_log(new_dataset)
                self.add_to_index(new_dataset)

            list_new_steel_markets.extend(list(steel_markets.values()))

        # add new steel markets to database
        self.database.extend(list_new_steel_markets)

    def create_steel_production_activities(self):
        """
        Create steel production activities for different regions.

        """
        # Determine all steel activities in the database. Empty old datasets.

        d_act_primary_steel = {
            mat: self.fetch_proxies(
                name=mat[0],
                ref_prod=mat[1],
                production_variable=["steel - primary"],
            )
            for mat in zip(
                self.material_map["steel - primary"],
                ["steel"] * len(self.material_map["steel - primary"]),
            )
        }

        d_act_secondary_steel = {
            mat: self.fetch_proxies(
                name=mat[0],
                ref_prod=mat[1],
                production_variable=["steel - secondary"],
            )
            for mat in zip(
                self.material_map["steel - secondary"],
                ["steel"] * len(self.material_map["steel - secondary"]),
            )
        }

        # adjust efficiency of primary steel production
        # and add carbon capture and storage, if needed
        for _, steel in d_act_primary_steel.items():
            steel = self.adjust_process_efficiency(steel)
            # update the database with the modified datasets
            self.database.extend(list(steel.values()))

            # add to log
            for new_dataset in list(steel.values()):
                self.write_log(new_dataset)
                self.add_to_index(new_dataset)

        # adjust efficiency of secondary steel production
        # and add carbon capture and storage, if needed
        for _, steel in d_act_secondary_steel.items():
            steel = self.adjust_process_efficiency(steel)
            # update the database with the modified datasets
            self.database.extend(list(steel.values()))

            # add to log
            for new_dataset in list(steel.values()):
                self.write_log(new_dataset)
                self.add_to_index(new_dataset)

    def create_pig_iron_production_activities(self):
        """
        Create region-specific pig iron production activities.
        """

        # print("Create pig iron production datasets")

        pig_iron = self.fetch_proxies(
            name="pig iron production",
            ref_prod="pig iron",
            production_variable=["steel - primary"],
        )

        # adjust efficiency of pig iron production
        pig_iron = self.adjust_process_efficiency(pig_iron)

        self.database.extend(list(pig_iron.values()))

        # add to log
        for new_dataset in list(pig_iron.values()):
            self.write_log(new_dataset)
            self.add_to_index(new_dataset)

    def create_pig_iron_markets(self):
        """
        Create region-specific pig iron markets.
        Adds datasets to the database.
        """

        pig_iron_markets = self.fetch_proxies(
            name="market for pig iron",
            ref_prod="pig iron",
            production_variable=["steel - primary"],
        )
        self.database.extend(list(pig_iron_markets.values()))

        # add to log
        for new_dataset in list(pig_iron_markets.values()):
            self.write_log(new_dataset)
            self.add_to_index(new_dataset)

    def adjust_process_efficiency(self, datasets):
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

        for region, dataset in datasets.items():
            # Determine the sector based on the activity name
            if any(i in dataset["name"] for i in ["converter", "pig iron"]):
                sector = "steel - primary"
            else:
                sector = "steel - secondary"

            if sector in self.iam_data.steel_technology_efficiencies.variables.values:
                # Calculate the scaling factor based on the efficiency change from 2020 to the current year
                scaling_factor = 1 / self.find_iam_efficiency_change(
                    data=self.iam_data.steel_technology_efficiencies,
                    variable=sector,
                    location=dataset["location"],
                )

            else:
                scaling_factor = 1

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

                    scaling_factor = max(0.444 / electricity, scaling_factor)

                # if pig iron production, we want to make sure
                # that the scaling down will not bring energy consumption
                # below the minimum value of 9.0 MJ/kg
                # see Theoretical Minimum Energies To Produce Steel for Selected Conditions
                # US Department of Energy, 2000

                if dataset["name"] == "pig iron production":
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
                    f"region {region} in {self.year}, following the scenario {self.scenario}. "
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

        return datasets

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
