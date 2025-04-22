"""
biomass.py contains the class `Biomass`, which inherits from `BaseTransformation`.
This class transforms the biomass markets that feed heât and power plants,
based on projections from the IAM scenario.
It eventually re-links all the biomass-consuming activities of the wurst database to
the newly created biomass markets.

"""

import yaml
from collections import defaultdict

from .export import biosphere_flows_dictionary
from .filesystem_constants import VARIABLES_DIR
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    List,
    get_shares_from_production_volume,
    np,
    uuid,
    ws,
)
from .activity_maps import InventorySet
from .validation import BiomassValidation

IAM_BIOMASS_VARS = VARIABLES_DIR / "biomass.yaml"

logger = create_logger("biomass")


def _update_biomass(scenario, version, system_model):

    if scenario["iam data"].biomass_mix is None:
        print("No biomass scenario data available -- skipping")
        return scenario

    biomass = Biomass(
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

    biomass.regionalize_wood_chips_activities()
    if scenario["iam data"].biomass_mix is not None:
        biomass.create_biomass_markets()

    biomass.relink_datasets()

    validate = BiomassValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=biomass.database,
        iam_data=scenario["iam data"],
    )

    validate.run_biomass_checks()

    scenario["database"] = biomass.database
    scenario["index"] = biomass.index
    scenario["cache"] = biomass.cache

    return scenario


def _group_datasets_by_keys(datasets: list, keys: list):
    from collections import defaultdict

    grouped = defaultdict(list)
    for d in datasets:
        group_key = tuple(d.get(k) for k in keys)
        grouped[group_key].append(d)
    return list(grouped.values())


class Biomass(BaseTransformation):
    """
    Class that modifies biomass markets in the database based on IAM output data.
    Inherits from `transformation.BaseTransformation`.

    :ivar database: wurst database, which is a list of dictionaries
    :vartype database: list
    :ivar iam_data: IAM data
    :vartype iam_data: xarray.DataArray
    :ivar model: name of the IAM model (e.g., "remind", "image")
    :vartype model: str
    :vartype pathway: str
    :ivar year: year of the pathway (e.g., 2030)
    :vartype year: int

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
        self.system_model = system_model
        self.biosphere_dict = biosphere_flows_dictionary(self.version)
        mapping = InventorySet(database=database, version=version, model=model)
        self.biomass_map = mapping.generate_biomass_map()

    def _wood_chips_activity_names(self):
        return [
            "softwood forestry, spruce, sustainable forest management",
            "softwood forestry, pine, sustainable forest management",
            "hardwood forestry, birch, sustainable forest management",
            "hardwood forestry, oak, sustainable forest management",
            "hardwood forestry, beech, sustainable forest management",
            "hardwood forestry, mixed species, sustainable forest management",
            "market for sawlog and veneer log, softwood, measured as solid wood under bark",
            "market for sawlog and veneer log, hardwood, measured as solid wood under bark",
            "sawing, softwood",
            "sawing, hardwood",
            "wood chips production, hardwood, at sawmill",
            "wood chips production, softwood, at sawmill",
            "willow production, short rotation coppice",
            "hardwood forestry, eucalyptus ssp., planted forest management",
        ]

    def _add_new_datasets(self, proxy_dict: dict):
        for dataset in proxy_dict.values():
            self.log_and_index(dataset)
        self.database.extend(proxy_dict.values())

    def regionalize_wood_chips_activities(self):
        """
        Regionalize wood chips and forestry-related activities, which are currently only
        available in RER, CA and RoW.
        """
        target_names = self._wood_chips_activity_names()
        matching_datasets = [ds for ds in self.database if ds["name"] in target_names]

        for group in _group_datasets_by_keys(
            matching_datasets, ["name", "reference product"]
        ):
            proxies = self.fetch_proxies(datasets=group)
            self._add_new_datasets(proxies)

    def create_biomass_markets(self) -> None:
        self._create_forest_residue_datasets()
        self._create_regional_biomass_markets()
        self._replace_biomass_inputs()

    def _create_forest_residue_datasets(self):
        for datasets in self.biomass_map["biomass - residual"]:
            proxies = self.fetch_proxies(
                datasets=datasets,
                production_variable="biomass - residual",
            )

            self.database.extend(proxies.values())
            for ds in proxies.values():
                self.log_and_index(ds)

    def _create_regional_biomass_markets(self):
        for region in self.regions:
            dataset = self._create_market_dataset(region)

            for biomass_type, biomass_datasets in self.biomass_map.items():
                if (
                    biomass_type
                    not in self.iam_data.biomass_mix.coords["variables"].values
                ):
                    continue

                share = self._get_biomass_share(biomass_type, region)

                if (
                    self.system_model == "consequential"
                    and biomass_type == "biomass - residual"
                ):
                    share = 0

                if share > 0:
                    suppliers = self.get_suppliers_with_fallback(
                        biomass_datasets, region
                    )
                    for supplier, supply_share in suppliers.items():
                        amount = supply_share * share
                        dataset["exchanges"].append(
                            {
                                "type": "technosphere",
                                "product": supplier[2],
                                "name": supplier[0],
                                "unit": supplier[-1],
                                "location": supplier[1],
                                "amount": amount,
                                "uncertainty type": 0,
                            }
                        )

                dataset["log parameters"]["biomass share"] = share

            if not any(exc["type"] == "technosphere" for exc in dataset["exchanges"]):
                raise ValueError(
                    f"Dataset {dataset['name']} has no technosphere exchanges."
                )

            self.database.append(dataset)
            self.log_and_index(dataset)

    def _create_market_dataset(self, region):
        return {
            "name": "market for biomass, used as fuel",
            "reference product": "biomass, used as fuel",
            "location": region,
            "comment": (
                f"Biomass market, created by `premise`, to align with projections "
                f"for the region {region} in {self.year}. "
                "Calculated for an average energy input (LHV) of 19 MJ/kg, dry basis. "
                "Sum of inputs can be superior to 1, as inputs of wood chips, wet-basis, "
                "have been multiplied by a factor 2.5, to reach a LHV of 19 MJ "
                "(they have a LHV of 7.6 MJ, wet basis)."
            ),
            "unit": "kilogram",
            "database": "premise",
            "code": str(uuid.uuid4().hex),
            "exchanges": [
                {
                    "name": "market for biomass, used as fuel",
                    "product": "biomass, used as fuel",
                    "amount": 1,
                    "unit": "kilogram",
                    "location": region,
                    "uncertainty type": 0,
                    "type": "production",
                }
            ],
            "log parameters": {},
        }

    def _get_biomass_share(self, biomass_type, region):
        if self.year in self.iam_data.biomass_mix.coords["year"].values:
            return self.iam_data.biomass_mix.sel(
                variables=biomass_type,
                region=region,
                year=self.year,
            ).values.item(0)
        else:
            return (
                self.iam_data.biomass_mix.sel(variables=biomass_type, region=region)
                .interp(year=self.year)
                .values.item(0)
            )

    def get_suppliers_with_fallback(self, biomass_datasets, region):
        ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)
        fallback_locs = [
            region,
            *ecoinvent_regions,
            "RER",
            "Europe without Switzerland",
            "RoW",
            "GLO",
        ]

        for loc in fallback_locs:
            suppliers = [ds for ds in biomass_datasets if ds["location"] == loc]
            if suppliers:
                return get_shares_from_production_volume(suppliers)

        raise ValueError(f"No suppliers found for biomass in location {region}")

    def _replace_biomass_inputs(self):
        for dataset in ws.get_many(
            self.database,
            ws.either(*[ws.equals("unit", u) for u in ["kilowatt hour", "megajoule"]]),
            ws.either(
                *[ws.contains("name", n) for n in ["electricity", "heat", "power"]]
            ),
        ):
            for exc in ws.technosphere(
                dataset,
                ws.contains("name", "market for wood chips"),
                ws.equals("unit", "kilogram"),
            ):
                exc["name"] = "market for biomass, used as fuel"
                exc["product"] = "biomass, used as fuel"
                exc["location"] = (
                    dataset["location"]
                    if dataset["location"] in self.regions
                    else self.ecoinvent_to_iam_loc.get(dataset["location"], "GLO")
                )

    def log_and_index(self, dataset):
        self.write_log(dataset)
        self.add_to_index(dataset)

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('biomass share', '')}"
        )
