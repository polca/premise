"""
biomass.py contains the class `Biomass`, which inherits from `BaseTransformation`.
This class transforms the biomass markets that feed heât and power plants,
based on projections from the IAM scenario.
It eventually re-links all the biomass-consuming activities of the wurst database to
the newly created biomass markets.

"""

import yaml

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

IAM_BIOMASS_VARS = VARIABLES_DIR / "biomass_variables.yaml"

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

    def create_biomass_markets(self) -> None:

        for activity in self.biomass_map["biomass - residual"]:
            # create region-specific "Supply of forest residue" datasets
            forest_residues_ds = self.fetch_proxies(
                name=activity,
                ref_prod="",
                production_variable="biomass - residual",
            )

            # add them to the database
            self.database.extend(forest_residues_ds.values())

            # add log
            for dataset in list(forest_residues_ds.values()):
                self.write_log(dataset=dataset)
                self.add_to_index(dataset)

        for region in self.regions:
            dataset = {
                "name": "market for biomass, used as fuel",
                "reference product": "biomass, used as fuel",
                "location": region,
                "comment": f"Biomass market, created by `premise`, "
                f"to align with projections for the region {region} in {self.year}. "
                "Calculated for an average energy input (LHV) of 19 MJ/kg, dry basis. "
                "Sum of inputs can be superior to 1, as "
                "inputs of wood chips, wet-basis, have been multiplied by a factor 2.5, "
                "to reach a LHV of 19 MJ (they have a LHV of 7.6 MJ, wet basis).",
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
            }

            for biomass_type, biomass_act in self.biomass_map.items():

                if (
                    biomass_type
                    not in self.iam_data.biomass_mix.coords["variables"].values
                ):
                    continue

                if self.year in self.iam_data.biomass_mix.coords["year"].values:
                    share = self.iam_data.biomass_mix.sel(
                        variables=biomass_type,
                        region=region,
                        year=self.year,
                    ).values.item(0)
                else:
                    share = (
                        self.iam_data.biomass_mix.sel(
                            variables=biomass_type, region=region
                        )
                        .interp(year=self.year)
                        .values.item(0),
                    )

                if (
                    self.system_model == "consequential"
                    and biomass_type == "biomass - residual"
                ):
                    share = 0

                if share > 0:
                    ecoinvent_regions = self.geo.iam_to_ecoinvent_location(
                        dataset["location"]
                    )
                    possible_locations = [
                        dataset["location"],
                        *ecoinvent_regions,
                        "RER",
                        "Europe without Switzerland",
                        "RoW",
                        "GLO",
                    ]
                    possible_names = list(biomass_act)

                    suppliers, counter = [], 0

                    while not suppliers:
                        suppliers = list(
                            ws.get_many(
                                self.database,
                                ws.either(
                                    *[
                                        ws.contains("name", possible_name)
                                        for possible_name in possible_names
                                    ]
                                ),
                                ws.equals("location", possible_locations[counter]),
                                ws.either(
                                    *[
                                        ws.contains(
                                            "reference product", possible_product
                                        )
                                        for possible_product in ("chips", "residue")
                                    ]
                                ),
                                ws.equals("unit", "kilogram"),
                                ws.doesnt_contain_any(
                                    "name", ["willow", "post-consumer"]
                                ),
                            )
                        )
                        counter += 1

                    suppliers = get_shares_from_production_volume(suppliers)

                    for supplier, supply_share in suppliers.items():
                        multiplication_factor = 1.0
                        amount = supply_share * share * multiplication_factor

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

                if "log parameters" not in dataset:
                    dataset["log parameters"] = {}

                dataset["log parameters"].update(
                    {
                        "biomass share": share,
                    }
                )

            # check that dataset has exchanges
            number_tech_exchanges = len(
                [exc for exc in dataset["exchanges"] if exc["type"] == "technosphere"]
            )
            if number_tech_exchanges == 0:
                raise ValueError(
                    f"Dataset {dataset['name']} has no technosphere exchanges."
                )

            self.database.append(dataset)

            # add log
            self.write_log(dataset=dataset)
            self.add_to_index(dataset)

        # replace biomass inputs
        for dataset in ws.get_many(
            self.database,
            ws.either(
                *[ws.equals("unit", unit) for unit in ["kilowatt hour", "megajoule"]]
            ),
            ws.either(
                *[
                    ws.contains("name", name)
                    for name in ["electricity", "heat", "power"]
                ]
            ),
        ):
            for exc in ws.technosphere(
                dataset,
                ws.contains("name", "market for wood chips"),
                ws.equals("unit", "kilogram"),
            ):
                exc["name"] = "market for biomass, used as fuel"
                exc["product"] = "biomass, used as fuel"

                if dataset["location"] in self.regions:
                    exc["location"] = dataset["location"]
                else:
                    exc["location"] = self.ecoinvent_to_iam_loc[dataset["location"]]

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('biomass share', '')}"
        )
