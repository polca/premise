"""
biomass.py contains the class `Biomass`, which inherits from `BaseTransformation`.
This class transforms the biomass markets that feed heât and power plants,
based on projections from the IAM scenario.
It eventually re-links all the biomass-consuming activities of the wurst database to
the newly created biomass markets.

"""

import yaml

from .export import biosphere_flows_dictionary
from .filesystem_constants import VARIABLES_DIR, DATA_DIR
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
from .activity_maps import InventorySet, act_fltr, get_mapping
from .validation import BiomassValidation

IAM_BIOMASS_VARS = VARIABLES_DIR / "biomass.yaml"
BIOMASS_ACTIVITIES = DATA_DIR / "biomass" / "biomass_activities.yaml"

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
        biomass.create_regional_biomass_markets()
        biomass.replace_biomass_inputs()

    biomass.relink_datasets()

    validate = BiomassValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=biomass.database,
        iam_data=scenario["iam data"],
        system_model=system_model,
    )

    validate.run_biomass_checks()

    scenario["database"] = biomass.database
    scenario["index"] = biomass.index
    scenario["cache"] = biomass.cache
    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["biomass"] = biomass.biomass_activities

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
        self.mapping = InventorySet(database=database, version=version, model=model)
        self.biomass_map = self.mapping.generate_biomass_map()
        self.biomass_activities = self.mapping.generate_map(
            get_mapping(filepath=BIOMASS_ACTIVITIES, var="ecoinvent_aliases")
        )

    def _build_ordered_biomass_activity_mapping(self) -> dict:
        """Build biomass activities mapping in the exact order listed in YAML."""
        with open(BIOMASS_ACTIVITIES, "r", encoding="utf-8") as stream:
            config = yaml.safe_load(stream)

        ordered_mapping = {}

        for entry in config.values():
            aliases = entry.get("ecoinvent_aliases", {})
            fltr = aliases.get("fltr", {})
            mask = aliases.get("mask")

            if not isinstance(fltr, dict):
                fltr = {"name": fltr}

            names = fltr.get("name")
            if isinstance(names, list):
                ordered_names = names
            elif names is None:
                ordered_names = [None]
            else:
                ordered_names = [names]

            ordered_activities = []
            seen = set()
            for name in ordered_names:
                current_fltr = dict(fltr)
                if name is not None:
                    current_fltr["name"] = name

                for activity in act_fltr(self.database, current_fltr, mask):
                    key = (
                        activity["name"],
                        activity["reference product"],
                        activity["location"],
                    )
                    if key not in seen:
                        ordered_activities.append(activity)
                        seen.add(key)

            for activity in ordered_activities:
                activity_key = (activity["name"], activity["reference product"])
                ordered_mapping.setdefault(activity_key, []).append(activity)

        return ordered_mapping

    def regionalize_wood_chips_activities(self):
        """
        Regionalize wood chips and forestry-related activities,
        which are currently only
        available in RER, CA and RoW.
        """

        # Build mapping in YAML order so regionalization is deterministic and user-defined.
        self.biomass_activities = self._build_ordered_biomass_activity_mapping()

        self.process_and_add_activities(
            mapping=self.biomass_activities,
            production_volumes=self.iam_data.production_volumes,
        )

        self.process_and_add_activities(
            mapping={
                k: v
                for k, v in self.biomass_map.items()
                if k in self.iam_data.production_volumes.variables.values
            },
            production_volumes=self.iam_data.production_volumes,
        )

        # Second pass: now that all candidate suppliers have been regionalized,
        # relink biomass activities again to replace temporary RoW links.
        activity_keys = set(self.biomass_activities.keys())
        for dataset in self.database:
            if (
                dataset.get("regionalized", False)
                and dataset.get("location") in self.regions
                and (dataset["name"], dataset["reference product"]) in activity_keys
            ):
                # Invalidate cached linking decisions from the first pass (often RoW),
                # so links can be recalculated with newly regionalized providers.
                if dataset["location"] in self.cache:
                    self.cache[dataset["location"]].pop(self.model, None)
                self.relink_technosphere_exchanges(dataset)

    def create_regional_biomass_markets(self):

        self.process_and_add_markets(
            name="market for lignocellulosic biomass, used as fuel",
            reference_product="lignocellulosic biomass",
            unit="kilogram",
            mapping=self.biomass_map,
            production_volumes=self.iam_data.biomass_mix,
            system_model=self.system_model,
            blacklist={
                "consequential": [
                    "biomass - residual",
                ]
            },
        )

    def replace_biomass_inputs(self):

        new_candidate = {
            "name": "market for lignocellulosic biomass, used as fuel",
            "reference product": "lignocellulosic biomass",
            "unit": "kilogram",
        }

        for dataset in ws.get_many(
            self.database,
            ws.either(
                *[
                    ws.equals("unit", u)
                    for u in ["kilowatt hour", "megajoule", "kilogram", "cubic meter"]
                ]
            ),
            ws.either(
                *[
                    ws.contains("name", n)
                    for n in [
                        "electricity",
                        "heat",
                        "power",
                        "hydrogen production",
                        "biomethane production",
                        "ethanol production",
                        "synthetic gas",
                    ]
                ]
            ),
            ws.doesnt_contain_any("name", ["logs", " residual ", "oil"]),
        ):

            for exc in ws.technosphere(
                dataset,
                ws.either(
                    *[
                        ws.contains("name", n)
                        for n in ["market for wood chips", "market for wood pellet"]
                    ]
                ),
                ws.equals("unit", "kilogram"),
            ):

                if dataset["location"] in self.regions:
                    location = dataset["location"]
                else:
                    location = self.ecoinvent_to_iam_loc.get(dataset["location"], "GLO")

                if self.is_in_index(new_candidate, location):
                    exc["name"] = "market for lignocellulosic biomass, used as fuel"
                    exc["product"] = "lignocellulosic biomass"
                    exc["location"] = location

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('biomass share', '')}"
        )
