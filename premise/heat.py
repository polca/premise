"""
Integrates projections regarding heat production and supply.
"""

from .activity_maps import InventorySet
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    List,
    ws,
    get_suppliers_of_a_region,
)
from .validation import HeatValidation
from .inventory_imports import get_biosphere_code
from .electricity import filter_technology

import copy
import uuid
import numpy as np
from collections import defaultdict
import xarray as xr

logger = create_logger("heat")


def _update_heat(scenario, version, system_model):

    heat = Heat(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    heat.fetch_fuel_market_co2_emissions()
    heat.regionalize_heat_production_datasets()

    if scenario["iam data"].residential_heating_mix is not None:
        heat.create_heat_markets(
            technologies=[
                tech
                for tech in heat.iam_data.residential_heating_mix.variables.values
                if "residential" in tech.lower()
            ],
            name="market for heat, residential",
            energy_use_volumes=heat.iam_data.residential_heating_mix,
            production_volumes=heat.iam_data.production_volumes,
        )
    else:
        print("No residential heat scenario data available -- skipping")

    if scenario["iam data"].daccs_energy_use is not None:
        heat.create_heat_markets(
            technologies=[
                tech for tech in heat.iam_data.daccs_energy_use.variables.values
            ],
            name="market for energy, for direct air capture and storage",
            energy_use_volumes=heat.iam_data.daccs_energy_use,
            production_volumes=heat.iam_data.production_volumes,
        )
    else:
        print("No DAC scenario data available -- skipping")

    if scenario["iam data"].ewr_energy_use is not None:
        heat.create_heat_markets(
            technologies=[
                tech for tech in heat.iam_data.ewr_energy_use.variables.values
            ],
            name="market for energy, for enhanced rock weathering",
            energy_use_volumes=heat.iam_data.ewr_energy_use,
            production_volumes=heat.iam_data.production_volumes,
        )
    else:
        print("No EWR scenario data available -- skipping")

    heat.relink_datasets()

    validate = HeatValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=heat.database,
        iam_data=scenario["iam data"],
    )

    validate.run_heat_checks()

    scenario["database"] = heat.database
    scenario["cache"] = heat.cache
    scenario["index"] = heat.index

    return scenario


class Heat(BaseTransformation):
    """
    Class that modifies fuel inventories and markets
    in ecoinvent based on IAM output data.
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
    ):
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

        self.carbon_intensity_markets = {}
        mapping = InventorySet(self.database)
        self.heat_techs = mapping.generate_heat_map()
        self.biosphere_flows = get_biosphere_code(self.version)

    def fetch_fuel_market_co2_emissions(self):
        """
        Fetch CO2 emissions from fuel markets.
        """
        fuel_markets = [
            "market for diesel, low-sulfur",
            "market for petrol, low-sulfur",
            "market for natural gas, high pressure",
        ]

        for dataset in ws.get_many(
            self.database,
            ws.either(*[ws.equals("name", n) for n in fuel_markets]),
        ):
            if "log parameters" in dataset:
                if "fossil CO2 per kg fuel" in dataset["log parameters"]:
                    self.carbon_intensity_markets[
                        (dataset["name"], dataset["location"])
                    ] = {"fossil": dataset["log parameters"]["fossil CO2 per kg fuel"]}
                if "non-fossil CO2 per kg fuel" in dataset["log parameters"]:
                    self.carbon_intensity_markets[
                        (dataset["name"], dataset["location"])
                    ].update(
                        {
                            "non-fossil": dataset["log parameters"][
                                "non-fossil CO2 per kg fuel"
                            ]
                        }
                    )

        # add "market for diesel" to self.carbon_intensity_markets
        # by duplicating the "market for low-sulfur" entries
        # add "market for natural gas, low pressure" to self.carbon_intensity_markets
        # by duplicating the "market for natural gas, high pressure" entries

        new_keys = {}
        for key, value in self.carbon_intensity_markets.items():
            if key[0] == "market for diesel, low-sulfur":
                new_keys[("market for diesel", key[1])] = value
                new_keys[("market group for diesel", key[1])] = value
                new_keys[("market group for diesel, low-sulfur", key[1])] = value
            if key[0] == "market for petrol, low-sulfur":
                new_keys[("market for petrol", key[1])] = value
                new_keys[("market for petrol, unleaded", key[1])] = value
            if key[0] == "market for natural gas, high pressure":
                new_keys[("market group for natural gas, high pressure", key[1])] = (
                    value
                )
                new_keys[("market for natural gas, low pressure", key[1])] = value

        self.carbon_intensity_markets.update(new_keys)

    def regionalize_heat_production_datasets(self):
        """
        Regionalize heat production.

        """

        created_datasets = []

        for heat_tech, heat_datasets in self.heat_techs.items():
            datasets = list(
                ws.get_many(
                    self.database,
                    ws.either(*[ws.equals("name", n) for n in heat_datasets]),
                    ws.equals("unit", "megajoule"),
                    ws.doesnt_contain_any("location", self.regions),
                )
            )

            for dataset in datasets:
                if dataset["name"] in created_datasets:
                    continue

                created_datasets.append(dataset["name"])

                geo_mapping = None
                if heat_tech == "heat, from natural gas (market)":
                    geo_mapping = {
                        r: "Europe without Switzerland" for r in self.regions
                    }

                new_ds = self.fetch_proxies(
                    name=dataset["name"],
                    ref_prod=dataset["reference product"],
                    exact_name_match=True,
                    exact_product_match=True,
                    subset=datasets,
                    geo_mapping=geo_mapping,
                )

                if len(new_ds) == 0:
                    continue

                for ds in new_ds.values():

                    fossil_co2, non_fossil_co2 = 0.0, 0.0

                    for exc in ws.technosphere(ds):
                        if (
                            exc["name"],
                            exc["location"],
                        ) in self.carbon_intensity_markets:
                            fossil_co2 += (
                                exc["amount"]
                                * self.carbon_intensity_markets[
                                    (exc["name"], exc["location"])
                                ]["fossil"]
                            )

                            non_fossil_co2 += (
                                exc["amount"]
                                * self.carbon_intensity_markets[
                                    (exc["name"], exc["location"])
                                ]["non-fossil"]
                            )

                    if fossil_co2 + non_fossil_co2 > 0:

                        initial_fossil_co2 = sum(
                            [
                                exc["amount"]
                                for exc in ws.biosphere(ds)
                                if exc["name"] == "Carbon dioxide, fossil"
                            ]
                        )
                        initial_non_fossil_co2 = sum(
                            [
                                exc["amount"]
                                for exc in ws.biosphere(ds)
                                if exc["name"] == "Carbon dioxide, non-fossil"
                            ]
                        )

                        ds["exchanges"] = [
                            e
                            for e in ds["exchanges"]
                            if e["name"]
                            not in (
                                "Carbon dioxide, fossil",
                                "Carbon dioxide, non-fossil",
                            )
                        ]

                        if fossil_co2 > 0:
                            ds["exchanges"].append(
                                {
                                    "uncertainty type": 0,
                                    "loc": fossil_co2,
                                    "amount": fossil_co2,
                                    "name": "Carbon dioxide, fossil",
                                    "categories": ("air",),
                                    "type": "biosphere",
                                    "unit": "kilogram",
                                    "input": (
                                        "biosphere3",
                                        self.biosphere_flows[
                                            (
                                                "Carbon dioxide, fossil",
                                                "air",
                                                "unspecified",
                                                "kilogram",
                                            )
                                        ],
                                    ),
                                }
                            )

                        if non_fossil_co2 > 0:

                            ds["exchanges"].append(
                                {
                                    "uncertainty type": 0,
                                    "loc": non_fossil_co2,
                                    "amount": non_fossil_co2,
                                    "name": "Carbon dioxide, non-fossil",
                                    "categories": ("air",),
                                    "type": "biosphere",
                                    "unit": "kilogram",
                                    "input": (
                                        "biosphere3",
                                        self.biosphere_flows[
                                            (
                                                "Carbon dioxide, non-fossil",
                                                "air",
                                                "unspecified",
                                                "kilogram",
                                            )
                                        ],
                                    ),
                                }
                            )

                        if "log parameters" not in ds:
                            ds["log parameters"] = {}

                        ds["log parameters"][
                            "initial amount of fossil CO2"
                        ] = initial_fossil_co2
                        ds["log parameters"]["new amount of fossil CO2"] = float(
                            fossil_co2
                        )
                        ds["log parameters"][
                            "initial amount of biogenic CO2"
                        ] = initial_non_fossil_co2
                        ds["log parameters"]["new amount of biogenic CO2"] = float(
                            non_fossil_co2
                        )

                for new_dataset in list(new_ds.values()):
                    self.write_log(new_dataset)
                    # add it to list of created datasets
                    self.add_to_index(new_dataset)
                    self.database.append(new_dataset)

    def generate_world_market(
        self,
        dataset: dict,
        regions: List[str],
        production_volumes: xr.DataArray,
    ) -> dict:
        """
        Generate the world market for a given dataset and product variables.

        :param dataset: The dataset for which to generate the world market.
        :param regions: A dictionary of activity datasets, keyed by region.

        This function generates the world market exchanges for a given dataset and set of product variables.
        It first filters out non-production exchanges from the dataset, and then calculates the total production
        volume for the world using the given product variables. For each region, it calculates the share of the
        production volume and adds a technosphere exchange to the dataset with the appropriate share.

        """

        # rename location
        dataset["location"] = "World"
        dataset["code"] = str(uuid.uuid4().hex)

        # remove existing production exchange if any
        dataset["exchanges"] = [
            exc for exc in dataset["exchanges"] if exc["type"] != "production"
        ]

        if self.year in production_volumes.coords["year"].values:
            production_volume = (
                production_volumes.sel(
                    region=regions,
                    variables=production_volumes.variables.values,
                    year=self.year,
                )
                .sum(dim=["region", "variables"])
                .values.item(0)
            )
        else:
            production_volume = (
                production_volumes.sel(
                    region=regions,
                    variables=production_volumes.variables.values,
                )
                .interp(year=self.year)
                .sum(dim=["region", "variables"])
                .values.item(0)
            )

        # add production exchange
        dataset["exchanges"].append(
            {
                "uncertainty type": 0,
                "loc": 1,
                "amount": 1,
                "type": "production",
                "production volume": production_volume,
                "product": dataset["reference product"],
                "name": dataset["name"],
                "unit": dataset["unit"],
                "location": "World",
            }
        )

        # Filter out non-production exchanges
        dataset["exchanges"] = [
            e for e in dataset["exchanges"] if e["type"] == "production"
        ]

        # Calculate share of production volume for each region
        for r in regions:
            if r == "World":
                continue

            if self.year in production_volumes.coords["year"].values:
                share = (
                    production_volumes.sel(
                        region=r,
                        variables=production_volumes.variables.values,
                        year=self.year,
                    ).sum(dim="variables")
                    / production_volumes.sel(
                        region=[
                            x for x in production_volumes.region.values if x != "World"
                        ],
                        variables=production_volumes.variables.values,
                        year=self.year,
                    ).sum(dim=["variables", "region"])
                ).values
            else:
                share = (
                    (
                        production_volumes.sel(
                            region=r,
                            variables=production_volumes.variables.values,
                        ).sum(dim="variables")
                        / production_volumes.sel(
                            region=[
                                x
                                for x in production_volumes.region.values
                                if x != "World"
                            ],
                            variables=production_volumes.variables.values,
                        ).sum(dim=["variables", "region"])
                    )
                    .interp(
                        year=self.year,
                        kwargs={"fill_value": "extrapolate"},
                    )
                    .values
                )

            if np.isnan(share):
                print("Incorrect market share for", dataset["name"], "in", r)

            if share > 0:
                # Add exchange for the region
                exchange = {
                    "uncertainty type": 0,
                    "amount": share,
                    "type": "technosphere",
                    "product": dataset["reference product"],
                    "name": dataset["name"],
                    "unit": dataset["unit"],
                    "location": r,
                }
                dataset["exchanges"].append(exchange)

        return dataset

    def create_heat_markets(
        self, technologies, name, energy_use_volumes, production_volumes
    ):

        # Get the possible names of ecoinvent datasets
        ecoinvent_technologies = {
            technology: self.heat_techs[technology] for technology in technologies
        }

        generic_dataset = {
            "name": name,
            "reference product": "heat, central or small-scale",
            "unit": "megajoule",
            "database": self.database[1]["database"],
            "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
            f" using the pathway {self.scenario} for the year {self.year}.",
            "exchanges": [],
        }

        def generate_regional_markets(
            region: str, period: int, subset: list, production_volumes: xr.DataArray
        ) -> dict:

            new_dataset = copy.deepcopy(generic_dataset)
            new_dataset["location"] = region
            new_dataset["code"] = str(uuid.uuid4().hex)

            # Fetch ecoinvent regions contained in the IAM region
            ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)

            # Fetch electricity-producing technologies contained in the IAM region
            # if they cannot be found for the ecoinvent locations concerned
            # we widen the scope to EU-based datasets, and RoW, and finally Switzerland

            possible_locations = [
                [region],
                ecoinvent_regions,
                ["RER"],
                ["RoW"],
                ["CH"],
                list(self.ecoinvent_to_iam_loc.keys()),
            ]

            tech_suppliers = defaultdict(list)

            for technology in ecoinvent_technologies:
                suppliers, counter = [], 0

                try:
                    while len(suppliers) == 0:
                        suppliers = list(
                            get_suppliers_of_a_region(
                                database=subset,
                                locations=possible_locations[counter],
                                names=ecoinvent_technologies[technology],
                                reference_prod="heat",
                                unit="megajoule",
                                exact_match=True,
                            )
                        )
                        counter += 1

                    tech_suppliers[technology] = suppliers

                except IndexError as exc:
                    if self.system_model == "consequential":
                        continue
                    raise IndexError(
                        f"Couldn't find suppliers for {technology} when looking for {ecoinvent_technologies[technology]}."
                        f"Ony found: {[(x['name'], x['reference product'], x['location']) for x in self.database if x['name'] in ecoinvent_technologies[technology]]}"
                    ) from exc

            if self.system_model == "consequential":
                heat_mix = dict(
                    zip(
                        production_volumes.variables.values,
                        production_volumes.sel(region=region, year=self.year).values,
                    )
                )

            else:
                heat_mix = dict(
                    zip(
                        production_volumes.variables.values,
                        production_volumes.sel(
                            region=region,
                        )
                        .interp(
                            year=np.arange(self.year, self.year + period + 1),
                            kwargs={"fill_value": "extrapolate"},
                        )
                        .mean(dim="year")
                        .values,
                    )
                )

            # normalize the mix to 1
            total = sum(heat_mix.values())
            heat_mix = {tech: heat_mix[tech] / total for tech in heat_mix}

            # fetch production volume
            if self.year in production_volumes.coords["year"].values:
                production_volume = production_volumes.sel(
                    region=region,
                    variables=production_volumes.variables.values,
                    year=self.year,
                ).values.item(0)
            else:
                production_volume = (
                    production_volumes.sel(
                        region=region,
                        variables=production_volumes.variables.values,
                    )
                    .interp(year=self.year)
                    .values.item(0)
                )

            # First, add the reference product exchange
            new_exchanges = [
                {
                    "uncertainty type": 0,
                    "loc": 1,
                    "amount": 1,
                    "type": "production",
                    "production volume": float(production_volume),
                    "product": new_dataset["reference product"],
                    "name": new_dataset["name"],
                    "unit": new_dataset["unit"],
                    "location": region,
                }
            ]

            if period != 0:
                # this dataset is for a period of time
                new_dataset["name"] += f", {period}-year period"
                new_dataset["comment"] += (
                    f" Average heat mix over a {period}"
                    f"-year period {self.year}-{self.year + period}."
                )
                new_exchanges[0]["name"] = new_dataset["name"]

            for technology in technologies:
                # If the given technology contributes to the mix
                if heat_mix[technology] > 0:
                    # Contribution in supply
                    amount = heat_mix[technology]

                    for supplier in tech_suppliers[technology]:
                        new_exchanges.append(
                            {
                                "uncertainty type": 0,
                                "loc": amount,
                                "amount": amount,
                                "type": "technosphere",
                                "product": supplier["reference product"],
                                "name": supplier["name"],
                                "unit": supplier["unit"],
                                "location": supplier["location"],
                            }
                        )

            new_dataset["exchanges"] = new_exchanges

            if "log parameters" not in new_dataset:
                new_dataset["log parameters"] = {}

            return new_dataset

        if self.system_model == "consequential":
            periods = [
                0,
            ]
        else:
            periods = [0, 20, 40, 60]

        # Using a list comprehension to process all technologies
        subset = filter_technology(
            dataset_names=[
                item for subset in ecoinvent_technologies.values() for item in subset
            ],
            database=self.database,
            unit="megajoule",
        )

        new_datasets = [
            generate_regional_markets(region, period, subset, energy_use_volumes)
            for period in periods
            for region in self.regions
            if region != "World"
            and energy_use_volumes.sel(region=region, year=self.year).sum() > 0
        ]

        self.database.extend(new_datasets)

        for ds in new_datasets:
            self.write_log(ds)
            self.add_to_index(ds)

        if energy_use_volumes.sel(year=self.year).sum() > 0:
            new_world_dataset = self.generate_world_market(
                dataset=copy.deepcopy(generic_dataset),
                regions=self.regions,
                production_volumes=production_volumes,
            )
            self.database.append(new_world_dataset)
            self.write_log(new_world_dataset)

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('initial amount of fossil CO2')}|"
            f"{dataset.get('log parameters', {}).get('new amount of fossil CO2')}|"
            f"{dataset.get('log parameters', {}).get('initial amount of biogenic CO2')}|"
            f"{dataset.get('log parameters', {}).get('new amount of biogenic CO2')}"
        )
