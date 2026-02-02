"""
This module contains the class to create final energy use
datasets based on IAM output data.
"""

from typing import List

from wurst import searching as ws

from .data_collection import IAMDataCollection
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

    final_energy.regionalize_energy_carrier_inputs()
    final_energy.regionalize_heating_datasets()

    final_energy.relink_datasets()
    scenario["database"] = final_energy.database
    scenario["index"] = final_energy.index
    scenario["cache"] = final_energy.cache

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["final energy"] = final_energy.final_energy_map

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
        self.fuel_map = mapping.generate_fuel_map(model=self.model)

    def regionalize_heating_datasets(self):

        self.process_and_add_activities(
            mapping=self.final_energy_map,
            production_volumes=self.iam_data.production_volumes,
        )

    def regionalize_energy_carrier_inputs(self):
        energy_products = {
            ds["reference product"]
            for acts in self.fuel_map.values()
            for ds in acts
            if ds.get("reference product")
        }
        energy_products.update(
            {
                "electricity, low voltage",
                "electricity, medium voltage",
                "electricity, high voltage",
                "heat",
                "heat, district or industrial",
                "heat, district or industrial, natural gas",
            }
        )

        def is_energy_product(product: str) -> bool:
            if product in energy_products:
                return True
            lowered = product.lower()
            return lowered.startswith("electricity") or lowered.startswith("heat")

        supplier_keys = set()
        for acts in self.final_energy_map.values():
            for dataset in acts:
                for exc in ws.technosphere(dataset):
                    if exc.get("amount", 0) == 0:
                        continue
                    product = exc.get("product")
                    if not product:
                        continue
                    if is_energy_product(product):
                        supplier_keys.add((exc["name"], product))

        if not supplier_keys:
            return

        mapping = {}
        for name, product in supplier_keys:
            activities = list(
                ws.get_many(
                    self.database,
                    ws.equals("name", name),
                    ws.equals("reference product", product),
                )
            )
            if activities:
                mapping[f"{name}::{product}"] = activities

        if mapping:
            self.process_and_add_activities(mapping=mapping)
