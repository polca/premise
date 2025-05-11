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


def group_dicts_by_keys(dicts: list, keys: list):
    groups = defaultdict(list)
    for d in dicts:
        group_key = tuple(d.get(k) for k in keys)
        groups[group_key].append(d)
    return list(groups.values())


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
        inv = InventorySet(self.database, self.version, self.model)
        self.steel_map = inv.generate_steel_map()

    def generate_activities(self):
        """
        This function generates new activities for primary and secondary steel
        production and add them to the wurst database.

        :return: Returns a modified database with newly added steel activities for the corresponding year
        """

        self.create_pig_iron_production_activities()
        self.create_pig_iron_markets()
        self.create_steel_production_activities()
        self.create_steel_markets()

    def create_steel_markets(self):
        """
        Create steel markets for different regions

        :return: Does not return anything. Adds new markets to database.
        """

        steel_markets = ws.get_many(
            self.database,
            ws.either(
                *[
                    ws.contains("name", market)
                    for market in [
                        "market for steel, low-alloyed",
                        "market for steel, unalloyed",
                        "market for steel, chromium steel 18/8"
                    ]
                ]
            )
        )

        steel_markets = group_dicts_by_keys(steel_markets, ["name", "location"])

        new_steel_markets, seen_datasets = [], []

        for steel_market in steel_markets:

            steel_market = [ds for ds in steel_market if ds["name"] not in seen_datasets]

            if not steel_market:
                continue

            regionalized_markets = self.fetch_proxies(
                datasets=steel_market,
                production_variable=self.iam_data.steel_technology_mix.variables.values,
            )

            # adjust share of primary and secondary steel
            if any(x in steel_market[0]["name"] for x in (
                "market for steel, unalloyed",
                "market for steel, low-alloyed",
            )):
                for region, dataset in regionalized_markets.items():

                    dataset["exchanges"] = [
                        e
                        for e in dataset["exchanges"]
                        if e["type"] == "production" or e["unit"] == "ton kilometer"
                    ]

                    if region != "World":
                        for steel_type, activities in self.steel_map.items():

                            if (
                                self.system_model == "consequential"
                                and steel_type == "steel - secondary"
                            ):
                                continue

                            activity = list(activities)[0]

                            if (
                                steel_type
                                in self.iam_data.steel_technology_mix.variables.values
                            ):
                                share = (
                                    self.iam_data.steel_technology_mix.sel(
                                        variables=steel_type,
                                        region=region,
                                    )
                                    .interp(year=self.year)
                                    .values.item(0)
                                )

                                if share > 0:

                                    dataset["exchanges"].append(
                                        {
                                            "name": activity["name"],
                                            "product": activity["reference product"],
                                            "amount": share,
                                            "unit": "kilogram",
                                            "type": "technosphere",
                                            "location": region,
                                        }
                                    )

                    # let's normalize shares to make sure them up to 1
                    total_share = sum(
                        exc["amount"]
                        for exc in dataset["exchanges"]
                        if exc["type"] == "technosphere" and exc["unit"] == "kilogram"
                    )

                    for exc in dataset["exchanges"]:
                        if exc["type"] == "technosphere" and exc["unit"] == "kilogram":
                            exc["amount"] /= total_share

            # populate World dataset
            regionalized_markets["World"]["exchanges"] = [
                x
                for x in regionalized_markets["World"]["exchanges"]
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
                                variables=self.iam_data.steel_technology_mix.variables.values,
                                region=region,
                                year=self.year,
                            ).sum(dim="variables")
                            / self.iam_data.production_volumes.sel(
                                variables=self.iam_data.steel_technology_mix.variables.values,
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
                                variables=self.iam_data.steel_technology_mix.variables.values,
                                region=region,
                            )
                            .interp(year=self.year)
                            .sum(dim="variables")
                            / self.iam_data.production_volumes.sel(
                                variables=self.iam_data.steel_technology_mix.variables.values,
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
                    regionalized_markets["World"]["exchanges"].append(
                        {
                            "name": steel_market[0]["name"],
                            "product": steel_market[0]["reference product"],
                            "amount": share,
                            "unit": "kilogram",
                            "type": "technosphere",
                            "location": region,
                        }
                    )

            new_steel_markets.extend(regionalized_markets.values())
            seen_datasets.extend([ds["name"] for ds in steel_market])

        # add to log
        for dataset in new_steel_markets:
            self.write_log(dataset)
            self.add_to_index(dataset)
            # add to database
            self.database.append(dataset)

    def create_steel_production_activities(self):
        """
        Create steel production activities for different regions.

        """
        # Determine all steel activities in the database.
        # Empty old datasets.

        processed_datasets, seen_datasets = [], []

        for steel_type, activities in self.steel_map.items():

            activities = [ds for ds in activities if ds["name"] not in seen_datasets]

            if not activities:
                continue

            activities = group_dicts_by_keys(activities, ["name", "reference product"])

            for activity in activities:

                activity = [ds for ds in activity if ds["name"] not in seen_datasets]
                if not activity:
                    continue

                regionalized_datasets = self.fetch_proxies(
                    datasets=activity
                )

                # adjust efficiency of steel production
                for dataset in regionalized_datasets.values():
                    self.adjust_process_efficiency(dataset, steel_type)

                processed_datasets.extend(regionalized_datasets.values())
                seen_datasets.extend([ds["name"] for ds in activity])

        # regionalize other steel datasets
        steel_datasets = ws.get_many(
            self.database,
            ws.contains("name", "steel production"),
            ws.contains("reference product", "steel"),
            ws.equals("unit", "kilogram"),
        )
        steel_datasets = [ds for ds in steel_datasets if ds["name"] not in set(seen_datasets)]

        steel_datasets = group_dicts_by_keys(steel_datasets, ["name", "location"])

        for dataset in steel_datasets:

            dataset = [ds for ds in dataset if ds["name"] not in seen_datasets]

            if not dataset:
                continue

            regionalized_datasets = self.fetch_proxies(
                datasets=dataset
            )

            processed_datasets.extend(regionalized_datasets.values())
            seen_datasets.extend([ds["name"] for ds in dataset])

        for dataset in processed_datasets:
            self.add_to_index(dataset)
            self.write_log(dataset, "created")
            self.database.append(dataset)

    def create_pig_iron_production_activities(self):
        """
        Create region-specific pig iron production activities.
        """

        pig_iron_datasets = [
            ds for ds in self.database
            if ds["name"].startswith("pig iron production")
            and ds["unit"] == "kilogram"
            and ds["reference product"] == "pig iron"
        ]

        pig_iron_datasets = group_dicts_by_keys(
            pig_iron_datasets, ["name", "location"]
        )

        new_datasets, seen_datasets = [], []

        for pig_iron_dataset in pig_iron_datasets:

            pig_iron_dataset = [ds for ds in pig_iron_dataset if ds["name"] not in seen_datasets]

            if not pig_iron_dataset:
                continue

            pig_iron = self.fetch_proxies(
                datasets=pig_iron_dataset,
                production_variable=[
                    p
                    for p in self.iam_data.steel_technology_mix.variables.values
                    if p != "steel - secondary"
                ],
            )

            new_datasets.extend(pig_iron.values())
            seen_datasets.extend([ds["name"] for ds in pig_iron_dataset])

        # add to log
        for new_dataset in new_datasets:
            self.write_log(new_dataset)
            self.add_to_index(new_dataset)
            # add to database
            self.database.append(new_dataset)

    def create_pig_iron_markets(self):
        """
        Create region-specific pig iron markets.
        Adds datasets to the database.
        """

        pig_iron_markets = ws.get_many(
            self.database,
            ws.contains("name", "market for pig iron"),
        )

        pig_iron_markets = group_dicts_by_keys(
            pig_iron_markets, ["name", "location"]
        )

        new_datasets, seen_datasets = [], []

        for pig_iron_market in pig_iron_markets:

            pig_iron_market = [ds for ds in pig_iron_market if ds["name"] not in seen_datasets]

            if not pig_iron_market:
                continue

            regionalized_markets = self.fetch_proxies(
                datasets=pig_iron_market,
                production_variable=[
                    p
                    for p in self.iam_data.steel_technology_mix.variables.values
                    if p != "steel - secondary"
                ],
            )

            new_datasets.extend(regionalized_markets.values())
            seen_datasets.extend([ds["name"] for ds in pig_iron_market])

        # add to log
        for new_dataset in new_datasets:
            self.write_log(new_dataset)
            self.add_to_index(new_dataset)
            # add to database
            self.database.append(new_dataset)

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
