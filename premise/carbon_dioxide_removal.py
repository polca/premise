"""
Integrates projections regarding direct air capture and storage.
"""

import copy

import yaml
import numpy as np
from collections import defaultdict
import xarray as xr

from .filesystem_constants import DATA_DIR
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    InventorySet,
    List,
    uuid,
    ws,
    get_suppliers_of_a_region,
)
from .electricity import filter_technology
from .utils import rescale_exchanges

logger = create_logger("dac")

CDR_ACTIVITIES = DATA_DIR / "cdr" / "cdr_activities.yaml"


def fetch_mapping(filepath: str) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, "r", encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


def _update_cdr(scenario, version, system_model):

    if scenario["iam data"].cdr_technology_mix is None:
        print("No CDR scenario data available -- skipping")
        return scenario

    cdr = CarbonDioxideRemoval(
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

    if scenario["iam data"].cdr_technology_mix is not None:
        cdr.regionalize_cdr_activities()
        cdr.create_cdr_markets()
        cdr.relink_datasets()
        scenario["database"] = cdr.database
        scenario["cache"] = cdr.cache
        scenario["index"] = cdr.index
    else:
        print("No DAC information found in IAM data. Skipping.")

    return scenario


class CarbonDioxideRemoval(BaseTransformation):
    """
    Class that modifies DAC and DACCS inventories and markets
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
        self.database = database
        self.iam_data = iam_data
        self.model = model
        self.pathway = pathway
        self.year = year
        self.version = version
        self.system_model = system_model
        mapping = InventorySet(self.database)
        self.cdr_plants = mapping.generate_cdr_map()
        self.cdr_activities = fetch_mapping(CDR_ACTIVITIES)
        self.carbon_storage = mapping.generate_carbon_storage_map()

    def regionalize_cdr_activities(self) -> None:
        """
        Generates regional variants of the direct air capture process with varying heat sources.

        This function fetches the original datasets for the direct air capture process and creates regional variants
        with different heat sources. The function loops through the heat sources defined in the `HEAT_SOURCES` mapping,
        modifies the original datasets to include the heat source, and adds the modified datasets to the database.

        """

        # get original dataset
        for technology, datasets in self.cdr_activities.items():
            for ds_name in datasets:
                new_ds = self.fetch_proxies(
                    name=ds_name,
                    ref_prod="",
                )

                # relink to energy mix for CDR plant, if available
                if any(
                    x in technology
                    for x in ("direct air capture", "enhanced rock weathering")
                ):

                    energy_dataset_name = None
                    if technology == "direct air capture" and not any(
                        x in ds_name for x in ("industrial", "pump", "waste")
                    ):
                        energy_dataset_name = (
                            "market for energy, for direct air capture and storage"
                        )

                    if technology == "enhanced rock weathering":
                        energy_dataset_name = (
                            "market for energy, for enhanced rock weathering"
                        )

                    if energy_dataset_name is not None:
                        for region, dataset in new_ds.items():
                            try:
                                energy_supply = ws.get_one(
                                    self.database,
                                    ws.equals(
                                        "name",
                                        energy_dataset_name,
                                    ),
                                    ws.equals("location", region),
                                    ws.equals("unit", "megajoule"),
                                )
                            except ws.NoResults:
                                continue

                            energy_input = sum(
                                e["amount"]
                                for e in dataset["exchanges"]
                                if e["type"] == "technosphere"
                                and e["unit"] == "megajoule"
                            )
                            energy_input += sum(
                                e["amount"] * 3.6
                                for e in dataset["exchanges"]
                                if e["type"] == "technosphere"
                                and e["unit"] == "kilowatt hour"
                            )
                            dataset["exchanges"] = [
                                e
                                for e in dataset["exchanges"]
                                if e["unit"] not in ["megajoule", "kilowatt hour"]
                            ]
                            dataset["exchanges"].append(
                                {
                                    "name": energy_supply["name"],
                                    "location": region,
                                    "amount": energy_input,
                                    "uncertainty type": 0,
                                    "unit": "megajoule",
                                    "type": "technosphere",
                                    "product": energy_supply["reference product"],
                                }
                            )

                for k, dataset in new_ds.items():
                    # Add created dataset to cache
                    self.add_new_entry_to_cache(
                        location=dataset["location"],
                        exchange=dataset,
                        allocated=[dataset],
                        shares=[
                            1.0,
                        ],
                    )

                    # add it to list of created datasets
                    self.write_log(dataset)
                    # add it to list of created datasets
                    self.add_to_index(dataset)

                self.database.extend(new_ds.values())

    def generate_world_market(
        self,
        dataset: dict,
        regions: List[str],
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

        if self.year in self.iam_data.cdr_technology_mix.coords["year"].values:
            production_volume = (
                self.iam_data.cdr_technology_mix.sel(
                    region=regions,
                    variables=self.iam_data.cdr_technology_mix.variables.values,
                    year=self.year,
                )
                .sum(dim=["region", "variables"])
                .values.item(0)
            )
        else:
            production_volume = (
                self.iam_data.cdr_technology_mix.sel(
                    region=regions,
                    variables=self.iam_data.cdr_technology_mix.variables.values,
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
        for region in regions:
            if region == "World":
                continue

            if self.year in self.iam_data.production_volumes.coords["year"].values:
                share = (
                    self.iam_data.production_volumes.sel(
                        region=region,
                        variables=self.iam_data.cdr_technology_mix.variables.values,
                        year=self.year,
                    ).sum(dim="variables")
                    / self.iam_data.production_volumes.sel(
                        region=[
                            x
                            for x in self.iam_data.cdr_technology_mix.region.values
                            if x != "World"
                        ],
                        variables=self.iam_data.cdr_technology_mix.variables.values,
                        year=self.year,
                    ).sum(dim=["variables", "region"])
                ).values
            else:
                share = (
                    (
                        self.iam_data.production_volumes.sel(
                            region=region,
                            variables=self.iam_data.cdr_technology_mix.variables.values,
                        ).sum(dim="variables")
                        / self.iam_data.production_volumes.sel(
                            region=[
                                x
                                for x in self.iam_data.cdr_technology_mix.region.values
                                if x != "World"
                            ],
                            variables=self.iam_data.cdr_technology_mix.variables.values,
                        ).sum(dim=["variables", "region"])
                    )
                    .interp(
                        year=self.year,
                        kwargs={"fill_value": "extrapolate"},
                    )
                    .values
                )

            if np.isnan(share):
                print("Incorrect market share for", dataset["name"], "in", region)

            if share > 0:
                # Add exchange for the region
                exchange = {
                    "uncertainty type": 0,
                    "amount": share,
                    "type": "technosphere",
                    "product": dataset["reference product"],
                    "name": dataset["name"],
                    "unit": dataset["unit"],
                    "location": region,
                }
                dataset["exchanges"].append(exchange)

        return dataset

    def create_cdr_markets(
        self,
    ):

        # Get the possible names of ecoinvent datasets
        technologies = self.cdr_plants

        generic_dataset = {
            "name": "market for carbon dioxide removal",
            "reference product": "carbon dioxide, captured and stored",
            "unit": "megajoule",
            "database": self.database[1]["database"],
            "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
            f" using the pathway {self.scenario} for the year {self.year}.",
            "exchanges": [],
        }

        def generate_regional_markets(
            region: str,
            subset: list,
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

            for technology in technologies:
                suppliers, counter = [], 0

                try:
                    while len(suppliers) == 0:
                        suppliers = list(
                            get_suppliers_of_a_region(
                                database=subset,
                                locations=possible_locations[counter],
                                names=technologies[technology],
                                reference_prod="carbon dioxide",
                                unit="kilogram",
                                exact_match=True,
                            )
                        )
                        counter += 1

                    tech_suppliers[technology] = suppliers

                except IndexError as exc:
                    if self.system_model == "consequential":
                        continue
                    raise IndexError(
                        f"Couldn't find suppliers for {technology} when looking for {technologies[technology]}."
                        f"Ony found: {[(x['name'], x['reference product'], x['location']) for x in self.database if x['name'] in technologies[technology]]}"
                    ) from exc

            cdr_mix = dict(
                zip(
                    self.iam_data.cdr_technology_mix.variables.values,
                    self.iam_data.cdr_technology_mix.sel(
                        region=region, year=self.year
                    ).values,
                )
            )

            # normalize the mix to 1
            total = sum(cdr_mix.values())
            cdr_mix = {tech: cdr_mix[tech] / total for tech in cdr_mix}

            # fetch production volume
            if self.year in self.iam_data.cdr_technology_mix.coords["year"].values:
                production_volume = self.iam_data.cdr_technology_mix.sel(
                    region=region,
                    variables=self.iam_data.cdr_technology_mix.variables.values,
                    year=self.year,
                ).values.item(0)
            else:
                production_volume = (
                    self.iam_data.cdr_technology_mix.sel(
                        region=region,
                        variables=self.iam_data.cdr_technology_mix.variables.values,
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

            for technology, amount in cdr_mix.items():
                # If the given technology contributes to the mix
                if amount > 0:
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

        # Using a list comprehension to process all technologies
        subset = filter_technology(
            dataset_names=[item for subset in technologies.values() for item in subset],
            database=self.database,
            unit="kilogram",
        )

        new_datasets = [
            generate_regional_markets(region, subset)
            for region in self.regions
            if region != "World"
            and self.iam_data.cdr_technology_mix.sel(
                region=region, year=self.year
            ).sum()
            > 0
        ]

        self.database.extend(new_datasets)

        for ds in new_datasets:
            self.write_log(ds)
            self.add_to_index(ds)

        if self.iam_data.cdr_technology_mix.sel(year=self.year).sum() > 0:
            new_world_dataset = self.generate_world_market(
                dataset=copy.deepcopy(generic_dataset),
                regions=self.regions,
            )
            self.database.append(new_world_dataset)
            self.write_log(new_world_dataset)

    def adjust_dac_efficiency(self, datasets):
        """
        Fetch the cumulated deployment of DAC from IAM file.
        Apply a learning rate -- see Qiu et al., 2022.
        """

        for region, dataset in datasets.items():
            if self.iam_data.dac_electricity_efficiencies is not None:
                if (
                    region
                    in self.iam_data.dac_electricity_efficiencies.coords[
                        "region"
                    ].values
                ):
                    if (
                        self.year
                        in self.iam_data.dac_electricity_efficiencies.coords[
                            "year"
                        ].values
                    ):
                        scaling_factor = float(
                            1
                            / self.iam_data.dac_electricity_efficiencies.sel(
                                region=region, year=self.year
                            ).values.item()
                        )
                    else:
                        scaling_factor = float(
                            1
                            / self.iam_data.dac_electricity_efficiencies.sel(
                                region=region
                            )
                            .interp(year=self.year)
                            .values
                        )

                    # bound the scaling factor to 1.5 and 0.5
                    scaling_factor = max(0.5, min(1.5, scaling_factor))

                    if scaling_factor != 1:
                        rescale_exchanges(
                            dataset,
                            scaling_factor,
                            technosphere_filters=[ws.equals("unit", "kilowatt hour")],
                        )

                        # add in comments the scaling factor applied
                        dataset["comment"] += (
                            f" The electrical efficiency of the system has been "
                            f"adjusted to match the efficiency of the "
                            f"average DAC plant in {self.year}."
                        )

                        if "log parameters" not in dataset:
                            dataset["log parameters"] = {}

                        dataset["log parameters"].update(
                            {
                                "electricity scaling factor": scaling_factor,
                            }
                        )

            if self.iam_data.cdr_technology_efficiencies is not None:
                if (
                    region
                    in self.iam_data.cdr_technology_efficiencies.coords["region"].values
                ):
                    if (
                        self.year
                        in self.iam_data.cdr_technology_efficiencies.coords[
                            "year"
                        ].values
                    ):
                        scaling_factor = float(
                            1
                            / self.iam_data.cdr_technology_efficiencies.sel(
                                region=region, year=self.year
                            ).values.item()
                        )
                    else:
                        scaling_factor = float(
                            1
                            / self.iam_data.cdr_technology_efficiencies.sel(
                                region=region
                            )
                            .interp(year=self.year)
                            .values
                        )

                    # bound the scaling factor to 1.5 and 0.5
                    scaling_factor = max(0.5, min(1.5, scaling_factor))

                    if scaling_factor != 1:

                        rescale_exchanges(
                            dataset,
                            scaling_factor,
                            technosphere_filters=[ws.equals("unit", "megajoule")],
                        )

                        # add in comments the scaling factor applied
                        dataset["comment"] += (
                            f" The thermal efficiency of the system has been "
                            f"adjusted to match the efficiency of the "
                            f"average DAC plant in {self.year}."
                        )

                        if "log parameters" not in dataset:
                            dataset["log parameters"] = {}

                        dataset["log parameters"].update(
                            {
                                "heat scaling factor": scaling_factor,
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
            f"{dataset.get('log parameters', {}).get('electricity scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('heat scaling factor', '')}"
        )
