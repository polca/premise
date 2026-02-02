"""
This module contains the class to create final energy use
datasets based on IAM output data.
"""

from typing import List

from wurst import searching as ws

from .data_collection import IAMDataCollection
from .fuels.config import (
    REGION_BIODIESEL_FEEDSTOCK_MAP,
    REGION_BIOETHANOL_FEEDSTOCK_MAP,
    REGION_CLIMATE_MAP,
)
from .fuels.utils import fetch_mapping, get_crops_properties
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

    final_energy.regionalize_biodiesel_supply_for_final_energy()
    final_energy.regionalize_bioethanol_supply_for_final_energy()
    final_energy.regionalize_energy_carrier_inputs()
    final_energy.regionalize_heating_datasets()
    final_energy.adjust_final_energy_feedstock_inputs()

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
        self.region_to_climate = fetch_mapping(REGION_CLIMATE_MAP).get(self.model, {})
        self.region_to_oil_feedstock = fetch_mapping(
            REGION_BIODIESEL_FEEDSTOCK_MAP
        ).get(self.model, {})
        self.region_to_bioethanol_feedstock = fetch_mapping(
            REGION_BIOETHANOL_FEEDSTOCK_MAP
        ).get(self.model, {})
        self.crops_props = get_crops_properties()

    def regionalize_heating_datasets(self):

        self.process_and_add_activities(
            mapping=self.final_energy_map,
            production_volumes=self.iam_data.production_volumes,
            efficiency_adjustment_fn=[
                self.adjust_biodiesel_feedstock_inputs,
                self.adjust_bioethanol_feedstock_inputs,
            ],
        )

    def adjust_final_energy_feedstock_inputs(self):
        target_keys = {
            (ds.get("name"), ds.get("reference product"))
            for acts in self.final_energy_map.values()
            for ds in acts
            if ds.get("name") and ds.get("reference product")
        }

        for ds in self.database:
            if (
                (ds.get("name"), ds.get("reference product")) in target_keys
                and ds.get("location") in self.regions
            ):
                self.adjust_biodiesel_feedstock_inputs(ds, "")
                self.adjust_bioethanol_feedstock_inputs(ds, "")

    def regionalize_biodiesel_supply_for_final_energy(self):
        activities = self.fuel_map.get("biodiesel, from oil crops", [])
        if not activities:
            return

        def detect_feedstock(name: str) -> str:
            lowered = name.lower()
            if "rapeseed" in lowered:
                return "rapeseed"
            if "soybean" in lowered or "soya" in lowered or "soy" in lowered:
                return "soybean"
            if "palm" in lowered:
                return "palm oil"
            return ""

        feedstock_to_activities = {}
        for ds in activities:
            feedstock = detect_feedstock(ds["name"])
            if feedstock:
                feedstock_to_activities.setdefault(feedstock, []).append(ds)

        if not feedstock_to_activities:
            return

        climate_to_default = self.crops_props["oil"]["crop_type"].get(self.model, {})
        region_to_feedstock = {}
        for region, climate in self.region_to_climate.items():
            region_to_feedstock[region] = self.region_to_oil_feedstock.get(
                region, climate_to_default.get(climate)
            )

        feedstock_to_regions = {}
        for region, feedstock in region_to_feedstock.items():
            if not feedstock:
                continue
            feedstock_to_regions.setdefault(feedstock, []).append(region)

        for feedstock, regions in feedstock_to_regions.items():
            ds_list = feedstock_to_activities.get(feedstock, [])
            if not ds_list:
                continue
            mapping = {f"biodiesel::{feedstock}": ds_list}
            self.process_and_add_activities(mapping=mapping, regions=regions)

    def regionalize_bioethanol_supply_for_final_energy(self):
        crop_types = {
            "sugar": "bioethanol, from sugar",
            "grass": "bioethanol, from grass",
            "wood": "bioethanol, from wood",
            "grain": "bioethanol, from grain",
        }

        def detect_feedstock(crop_type: str, name: str) -> str:
            lowered = name.lower()
            if crop_type == "sugar":
                if "sugarbeet" in lowered or "sugar beet" in lowered:
                    return "sugarbeet"
                if "sugarcane" in lowered:
                    return "sugarcane"
            elif crop_type == "grass":
                if "switchgrass" in lowered:
                    return "switchgrass"
                if "miscanthus" in lowered:
                    return "miscanthus"
                if "sorghum" in lowered:
                    return "sorghum"
            elif crop_type == "wood":
                if "poplar" in lowered:
                    return "poplar"
                if "eucalyptus" in lowered:
                    return "eucalyptus"
            elif crop_type == "grain":
                if "corn" in lowered or "maize" in lowered:
                    return "corn"
                if "wheat" in lowered or "rye" in lowered:
                    return "wheat_rye"
            return ""

        for crop_type, variable in crop_types.items():
            activities = self.fuel_map.get(variable, [])
            if not activities:
                continue

            feedstock_to_activities = {}
            for ds in activities:
                feedstock = detect_feedstock(crop_type, ds["name"])
                if feedstock:
                    feedstock_to_activities.setdefault(feedstock, []).append(ds)

            if not feedstock_to_activities:
                continue

            climate_to_default = self.crops_props[crop_type]["crop_type"].get(
                self.model, {}
            )
            region_mapping = self.region_to_bioethanol_feedstock.get(crop_type, {})
            region_to_feedstock = {}
            for region, climate in self.region_to_climate.items():
                region_to_feedstock[region] = region_mapping.get(
                    region, climate_to_default.get(climate)
                )

            feedstock_to_regions = {}
            for region, feedstock in region_to_feedstock.items():
                if not feedstock:
                    continue
                feedstock_to_regions.setdefault(feedstock, []).append(region)

            for feedstock, regions in feedstock_to_regions.items():
                ds_list = feedstock_to_activities.get(feedstock, [])
                if not ds_list:
                    continue
                mapping = {f"bioethanol::{crop_type}::{feedstock}": ds_list}
                self.process_and_add_activities(mapping=mapping, regions=regions)

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
            self.process_and_add_activities(
                mapping=mapping,
                efficiency_adjustment_fn=[
                    self.adjust_biodiesel_feedstock_inputs,
                    self.adjust_bioethanol_feedstock_inputs,
                ],
            )

    def adjust_biodiesel_feedstock_inputs(self, dataset: dict, _tech: str) -> dict:
        region = dataset.get("location")
        if not region:
            return dataset

        biodiesel_excs = []
        for exc in ws.technosphere(dataset):
            product = exc.get("product", "")
            name = exc.get("name", "")
            if "biodiesel" in product.lower() or "biodiesel" in name.lower():
                biodiesel_excs.append(exc)

        if not biodiesel_excs:
            return dataset

        # Only adjust oil-based biodiesel (transesterification/esterification).
        # Skip FT/cellulosic routes.
        def is_oil_based(name: str) -> bool:
            lowered = name.lower()
            return (
                "transesterification" in lowered
                or "esterification" in lowered
                or "oil" in lowered
            )

        def is_ft_route(name: str) -> bool:
            lowered = name.lower()
            return "fischer-tropsch" in lowered or "ft " in lowered or "ft," in lowered

        if any(is_ft_route(exc.get("name", "")) for exc in biodiesel_excs):
            return dataset

        if not any(is_oil_based(exc.get("name", "")) for exc in biodiesel_excs):
            return dataset

        climate_to_default = self.crops_props["oil"]["crop_type"].get(self.model, {})
        desired_feedstock = self.region_to_oil_feedstock.get(
            region, climate_to_default.get(self.region_to_climate.get(region))
        )
        if not desired_feedstock:
            return dataset

        def matches_feedstock(name: str, feedstock: str) -> bool:
            lowered = name.lower()
            if feedstock == "rapeseed":
                return "rapeseed" in lowered
            if feedstock == "soybean":
                return "soybean" in lowered or "soya" in lowered or "soy" in lowered
            if feedstock == "palm oil":
                return "palm" in lowered
            return False

        suppliers = list(
            ws.get_many(
                self.database,
                ws.equals("location", region),
                ws.either(
                    ws.equals("reference product", "biodiesel"),
                    ws.contains("reference product", "biodiesel"),
                    ws.equals("reference product", "fatty acid methyl ester"),
                    ws.contains("name", "esterification of soybean oil"),
                ),
            )
        )

        def is_esterification(ds_name: str) -> bool:
            return "esterification" in ds_name.lower()

        def keep_supplier(ds: dict) -> bool:
            if not matches_feedstock(ds["name"], desired_feedstock):
                return False
            if desired_feedstock != "soybean" and is_esterification(ds["name"]):
                return False
            return True

        suppliers = [s for s in suppliers if keep_supplier(s)]

        if not suppliers:
            suppliers = [
                s
                for s in ws.get_many(
                    self.database,
                    ws.either(
                        ws.equals("reference product", "biodiesel"),
                        ws.contains("reference product", "biodiesel"),
                        ws.equals("reference product", "fatty acid methyl ester"),
                        ws.contains("name", "esterification of soybean oil"),
                    ),
                )
                if keep_supplier(s)
            ]
            if not suppliers:
                return dataset

        def supplier_rank(ds: dict) -> tuple:
            name = ds.get("name", "").lower()
            ref = ds.get("reference product", "").lower()
            return (
                0 if "biodiesel production" in name else 1,
                0 if "biodiesel" in ref else 1,
                0 if not is_esterification(name) else 1,
            )

        suppliers.sort(key=supplier_rank)
        supplier = suppliers[0]

        for exc in ws.technosphere(dataset):
            product = exc.get("product")
            if not product:
                continue
            product_lower = product.lower()
            if (
                "biodiesel" not in product_lower
                and product_lower != "fatty acid methyl ester"
                and "biodiesel" not in exc.get("name", "").lower()
            ):
                continue
            if not matches_feedstock(exc.get("name", ""), desired_feedstock):
                exc["name"] = supplier["name"]
                exc["product"] = supplier["reference product"]
                exc["location"] = supplier["location"]
                exc["unit"] = supplier["unit"]

        return dataset

    def adjust_bioethanol_feedstock_inputs(self, dataset: dict, _tech: str) -> dict:
        region = dataset.get("location")
        if not region:
            return dataset

        def detect_category_and_feedstock(name: str) -> tuple[str, str]:
            lowered = name.lower()
            if "sugarbeet" in lowered or "sugar beet" in lowered or "sugarcane" in lowered:
                if "sugarbeet" in lowered or "sugar beet" in lowered:
                    return "sugar", "sugarbeet"
                return "sugar", "sugarcane"
            if "switchgrass" in lowered or "miscanthus" in lowered or "sorghum" in lowered:
                if "switchgrass" in lowered:
                    return "grass", "switchgrass"
                if "miscanthus" in lowered:
                    return "grass", "miscanthus"
                return "grass", "sorghum"
            if "poplar" in lowered or "eucalyptus" in lowered:
                if "poplar" in lowered:
                    return "wood", "poplar"
                return "wood", "eucalyptus"
            if "corn" in lowered or "maize" in lowered or "wheat" in lowered or "rye" in lowered:
                if "corn" in lowered or "maize" in lowered:
                    return "grain", "corn"
                return "grain", "wheat_rye"
            return "", ""

        def desired_feedstock_for(category: str) -> str:
            if not category:
                return ""
            climate_to_default = self.crops_props[category]["crop_type"].get(
                self.model, {}
            )
            return self.region_to_bioethanol_feedstock.get(category, {}).get(
                region, climate_to_default.get(self.region_to_climate.get(region))
            )

        def matches_feedstock(name: str, feedstock: str) -> bool:
            lowered = name.lower()
            if feedstock == "sugarbeet":
                return "sugarbeet" in lowered or "sugar beet" in lowered
            if feedstock == "sugarcane":
                return "sugarcane" in lowered
            if feedstock == "switchgrass":
                return "switchgrass" in lowered
            if feedstock == "miscanthus":
                return "miscanthus" in lowered
            if feedstock == "sorghum":
                return "sorghum" in lowered
            if feedstock == "poplar":
                return "poplar" in lowered
            if feedstock == "eucalyptus":
                return "eucalyptus" in lowered
            if feedstock == "corn":
                return "corn" in lowered or "maize" in lowered
            if feedstock == "wheat_rye":
                return "wheat" in lowered or "rye" in lowered
            return False

        for exc in ws.technosphere(dataset):
            product = exc.get("product", "")
            if product != "ethanol" and "ethanol" not in product.lower():
                continue

            category, _current = detect_category_and_feedstock(exc.get("name", ""))
            desired_feedstock = desired_feedstock_for(category)
            if not desired_feedstock:
                continue

            suppliers = list(
                ws.get_many(
                    self.database,
                    ws.equals("location", region),
                    ws.contains("reference product", "ethanol"),
                )
            )
            suppliers = [
                s for s in suppliers if matches_feedstock(s["name"], desired_feedstock)
            ]

            if not suppliers:

                continue

            supplier = suppliers[0]
            if not matches_feedstock(exc.get("name", ""), desired_feedstock):

                exc["name"] = supplier["name"]
                exc["product"] = supplier["reference product"]
                exc["location"] = supplier["location"]
                exc["unit"] = supplier["unit"]

        return dataset
