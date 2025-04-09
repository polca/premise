"""
This module contains the class to create final energy use
datasets based on IAM output data.
"""

from typing import List

from wurst import searching as ws

from .data_collection import IAMDataCollection
from .filesystem_constants import DATA_DIR
from .inventory_imports import DefaultInventory
from .transformation import BaseTransformation, InventorySet


def _update_final_energy(
    scenario,
    version,
    system_model,
):

    if scenario["iam data"].final_energy_use is None:
        print("No final energy scenario data available -- skipping")
        return scenario

    final_energy = FinalEnergy(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
    )

    final_energy.regionalize_heating_datasets()

    final_energy.relink_datasets()
    scenario["database"] = final_energy.database
    scenario["index"] = final_energy.index
    scenario["cache"] = final_energy.cache

    return scenario


class FinalEnergy(BaseTransformation):
    """
    Class that creates heating datasets based on IAM output data.

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
        )
        self.version = version

        mapping = InventorySet(database=database, version=version, model=model)
        self.final_energy_map = mapping.generate_final_energy_map()

    def regionalize_heating_datasets(self):

        new_datasets, processed_datasets = [], []

        for dataset in ws.get_many(
            self.database,
            ws.either(
                *[
                    ws.contains("name", name)
                    for name in list(
                        set(
                            [
                                ", ".join(sorted(s))
                                for s in self.final_energy_map.values()
                            ]
                        )
                    )
                ]
            ),
        ):

            if dataset["name"] in processed_datasets:
                continue
            if dataset["location"] in self.regions:
                continue

            datasets = self.fetch_proxies(
                name=dataset["name"],
                ref_prod=dataset["reference product"],
            )
            new_datasets.append(datasets)

            processed_datasets.append(dataset["name"])

        for datasets in new_datasets:
            self.database.extend(datasets.values())
            self.add_to_index(datasets.values())
