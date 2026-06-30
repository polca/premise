"""
Integrates projections regarding carbon dioxide removal.
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

logger = create_logger("cdr")

CDR_ACTIVITIES = DATA_DIR / "cdr" / "cdr_activities.yaml"


def fetch_mapping() -> dict:
    """Returns a dictionary from a YML file"""

    with open(CDR_ACTIVITIES, "r", encoding="utf-8") as stream:
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
        print("No CDR information found in IAM data. Skipping.")

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["cdr"] = cdr.cdr_map

    return scenario


class CarbonDioxideRemoval(BaseTransformation):
    """
    Class that modifies CDR inventories and markets
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

    def regionalize_cdr_activities(self) -> None:
        """
        Generates regional variants of mapped carbon dioxide removal activities.
        """

        # regionalize support activities
        filters = fetch_mapping()

        self.cdr_support_activities = self.mapping.generate_sets_from_filters(filters)

        self.cdr_support_activities = {
            x["name"]: v for v in self.cdr_support_activities.values() for x in v
        }

        if self.cdr_support_activities:
            self.process_and_add_activities(mapping=self.cdr_support_activities)

        self.cdr_map = self.mapping.generate_cdr_map(model=self.model)

        self.process_and_add_activities(
            efficiency_adjustment_fn=self.adjust_cdr_efficiency,
            mapping=self.cdr_map,
            production_volumes=self.iam_data.production_volumes,
        )

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

    def _get_cdr_efficiency(self, technology, region, carrier):
        efficiencies = getattr(self.iam_data, "cdr_technology_efficiencies", None)
        if efficiencies is None:
            return None

        if technology not in efficiencies.coords["variables"].values:
            return None

        selector = {"variables": technology}

        if "carrier" in efficiencies.coords:
            if carrier not in efficiencies.coords["carrier"].values:
                return None
            selector["carrier"] = carrier

        if "region" in efficiencies.coords:
            if region not in efficiencies.coords["region"].values:
                return None
            selector["region"] = region

        if "year" in efficiencies.coords:
            if self.year in efficiencies.coords["year"].values:
                selector["year"] = self.year
                efficiency = efficiencies.sel(**selector)
            else:
                efficiency = efficiencies.sel(**selector).interp(year=self.year)
        else:
            efficiency = efficiencies.sel(**selector)

        efficiency = float(efficiency.values.item(0))
        if not np.isfinite(efficiency) or efficiency == 0:
            return None

        return efficiency

    @staticmethod
    def _bounded_scaling_factor(efficiency):
        if efficiency is None:
            return 1.0
        return max(0.5, min(1.5, float(1 / efficiency)))

    def adjust_cdr_efficiency(self, dataset, technology):
        """
        Scale energy exchanges using IAM CDR efficiency changes.
        """

        region = dataset["location"]

        electricity_scaling_factor = self._bounded_scaling_factor(
            self._get_cdr_efficiency(technology, region, "electricity")
        )
        heat_scaling_factor = self._bounded_scaling_factor(
            self._get_cdr_efficiency(technology, region, "heat")
        )

        electricity_filter = ws.either(
            ws.contains("name", "electricity"),
            ws.contains("product", "electricity"),
            ws.equals("unit", "kilowatt hour"),
        )
        heat_filter = ws.either(
            *[
                ws.contains(field, term)
                for field in ("name", "product")
                for term in (
                    "heat",
                    "steam",
                    "diesel",
                    "natural gas",
                    "hydrogen",
                    "fuel",
                )
            ]
        )
        no_biosphere_filter = [ws.equals("name", "__no_cdr_biosphere_scaling__")]

        if electricity_scaling_factor != 1:
            rescale_exchanges(
                ds=dataset,
                value=electricity_scaling_factor,
                technosphere_filters=[
                    ws.exclude(ws.contains("name", "carbon dioxide")),
                    electricity_filter,
                ],
                biosphere_filters=no_biosphere_filter,
            )

        if heat_scaling_factor != 1:
            rescale_exchanges(
                ds=dataset,
                value=heat_scaling_factor,
                technosphere_filters=[
                    ws.exclude(ws.contains("name", "carbon dioxide")),
                    ws.exclude(electricity_filter),
                    heat_filter,
                ],
                biosphere_filters=no_biosphere_filter,
            )

        if electricity_scaling_factor != 1 or heat_scaling_factor != 1:
            # add in comments the scaling factor applied
            if "comment" not in dataset:
                dataset["comment"] = (
                    f"The electricity and heat/fuel efficiency of the system has been "
                    f"adjusted to match the efficiency of the average CDR plant in "
                    f"{self.year}."
                )
            else:
                dataset["comment"] += (
                    f" The electricity and heat/fuel efficiency of the system has been "
                    f"adjusted to match the efficiency of the average CDR plant in "
                    f"{self.year}."
                )

        dataset.setdefault("log parameters", {}).update(
            {
                "electricity efficiency scaling factor": electricity_scaling_factor,
                "heat efficiency scaling factor": heat_scaling_factor,
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
            f"{dataset.get('log parameters', {}).get('electricity efficiency scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('heat efficiency scaling factor', '')}"
        )
