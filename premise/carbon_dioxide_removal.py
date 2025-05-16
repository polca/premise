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


def group_dicts_by_keys(dicts: list, keys: list):
    groups = defaultdict(list)
    for d in dicts:
        group_key = tuple(d.get(k) for k in keys)
        groups[group_key].append(d)
    return list(groups.values())


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
        self.mapping = InventorySet(self.database)
        self.cdr_map = self.mapping.generate_cdr_map()

    def regionalize_cdr_activities(self) -> None:
        """
        Generates regional variants of the direct air capture process with varying heat sources.

        This function fetches the original datasets for the direct air capture process and creates regional variants
        with different heat sources. The function loops through the heat sources defined in the `HEAT_SOURCES` mapping,
        modifies the original datasets to include the heat source, and adds the modified datasets to the database.

        """

        self.process_and_add_activities(
            efficiency_adjustment_fn=self.adjust_cdr_efficiency,
            mapping=self.cdr_map,
        )

       for technology in (
            "direct air capture (solvent) with storage",
            "direct air capture (sorbent) with storage",
            "enhanced rock weathering"
       ):
        processed_datasets = []
        # get original dataset
        for technology, datasets in self.cdr_map.items():

            datasets = group_dicts_by_keys(
                datasets,
                ["name", "reference product"],
            )

            for dataset in datasets:
                # fetch the original dataset
                new_ds = self.fetch_proxies(
                    datasets=dataset,
                )

                # relink to energy mix for CDR plant, if available
                if any(
                    x in technology
                    for x in ("direct air capture", "enhanced rock weathering")
                ):

                    energy_dataset_name = None
                    if technology == "direct air capture" and not any(
                        x in y
                        for y in datasets
                        for x in ("industrial", "pump", "waste")
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

                processed_datasets.extend(new_ds.values())

        for new_dataset in processed_datasets:
            # Add created dataset to cache
            self.add_new_entry_to_cache(
                location=new_dataset["location"],
                exchange=new_dataset,
                allocated=[new_dataset],
                shares=[
                    1.0,
                ],
            )

            # add it to list of created datasets
            self.write_log(new_dataset)
            # add it to list of created datasets
            self.add_to_index(new_dataset)

            self.database.append(new_dataset)


    def create_cdr_markets(
        self,
    ):

        self.process_and_add_markets(
            name="market for carbon dioxide removal",
            reference_product="carbon dioxide, captured and stored",
            unit="kilogram",
            mapping=self.cdr_map,
            production_volumes=self.iam_data.production_volumes,
            system_model=self.system_model,
        )


    def adjust_cdr_efficiency(self, dataset, technology):
        """
        Fetch the cumulated deployment of DAC from IAM file.
        Apply a learning rate -- see Qiu et al., 2022.
        """


        region = dataset["location"]

        efficiencies = None
        if technology in self.iam_data.cdr_technology_efficiencies.coords["variables"].values:
            if region in self.iam_data.cdr_technology_efficiencies.coords["region"].values:
                if self.year in self.iam_data.cdr_technology_efficiencies.coords["year"].values:
                    efficiencies = self.iam_data.cdr_technology_efficiencies.sel(
                        region=region, year=self.year, variables=technology
                    )
                else:
                    efficiencies = self.iam_data.cdr_technology_efficiencies.sel(
                        region=region, variables=technology
                    ).interp(year=self.year)
                print(f"Found efficiency for {technology} in IAM data for {region}.")

        if efficiencies is None:
            print(f"No efficiency found for {technology} in IAM data for {region}.")
            return dataset

        scaling_factor = float(
            1
            / efficiencies.values.item(0)
        )

        # bound the scaling factor to 1.5 and 0.5
        scaling_factor = max(0.5, min(1.5, scaling_factor))

        if scaling_factor != 1:
            rescale_exchanges(
                dataset,
                scaling_factor,
            )

            # add in comments the scaling factor applied
            dataset["comment"] += (
                f" The efficiency of the system has been "
                f"adjusted to match the efficiency of the "
                f"average CDR plant in {self.year}."
            )

            dataset.setdefault("log parameters", {}).update(
                {
                    "electricity scaling factor": scaling_factor,
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
            f"{dataset.get('log parameters', {}).get('electricity scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('heat scaling factor', '')}"
        )
