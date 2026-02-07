"""
Integrates projections regarding emissions of hot pollutants
from GAINS.
"""

from functools import lru_cache
from typing import Union

import numpy as np
import wurst
import xarray as xr
import yaml
from numpy import ndarray

from .filesystem_constants import DATA_DIR
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    InventorySet,
    List,
)

logger = create_logger("emissions")

EI_POLLUTANTS = DATA_DIR / "GAINS_emission_factors" / "GAINS_ei_pollutants.yaml"


def fetch_mapping(filepath: str) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, "r", encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


def _update_emissions(scenario, version, system_model, gains_scenario):

    if scenario["iam data"].gains_data_IAM is None:
        print("No pollutant emissions scenario data available -- skipping")
        return scenario

    emissions = Emissions(
        database=scenario["database"],
        year=scenario["year"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        iam_data=scenario["iam data"],
        version=version,
        system_model=system_model,
        gains_scenario=gains_scenario,
    )

    emissions.update_emissions_in_database()
    scenario["database"] = emissions.database

    return scenario


class Emissions(BaseTransformation):
    """
    Class that modifies emissions of hot pollutants
    according to GAINS projections.
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
        gains_scenario: str,
    ):
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
        )

        self.version = version
        self.gains_IAM = self.prepare_data(iam_data.gains_data_IAM)
        self.ei_pollutants = fetch_mapping(EI_POLLUTANTS)
        self.gains_pollutant = {v: k for k, v in self.ei_pollutants.items()}
        self.gains_scenario = gains_scenario

        mapping = InventorySet(self.database)
        self.gains_map = mapping.generate_gains_mapping()
        self.rev_gains_map = {}

        for s in self.gains_map:
            for t in self.gains_map[s]:
                self.rev_gains_map[t["name"]] = s

    def prepare_data(self, data):

        def _safe_divide(x):
            return xr.where((np.isnan(x)) | (x == 0), 1, x)

        base = data.sel(year=2020)

        if self.year in data.coords["year"]:
            year_slice = data.sel(year=self.year)
        else:
            year_slice = data.interp(year=self.year)

        data = year_slice / _safe_divide(base)

        # replace 0 values with 1
        data = xr.where((np.isnan(data)) | (data == 0), 1, data)

        return data

    def update_emissions_in_database(self):
        for ds in self.database:
            name = ds["name"]
            loc = ds["location"]

            if name in self.rev_gains_map:
                iam_loc = self.ecoinvent_to_iam_loc.get(loc)
                if iam_loc and iam_loc in self.gains_IAM.coords["region"]:
                    sector = self.rev_gains_map[name]
                    self.update_pollutant_emissions(
                        ds,
                        sector,
                        regions=self.gains_IAM.region.values,
                    )
                    self.write_log(ds, status="updated")

    def update_pollutant_emissions(
        self, dataset: dict, sector: str, regions: list
    ) -> dict:
        """
        Update pollutant emissions based on GAINS data.
        We apply a correction factor equal to the relative
        change in emissions compared to 2020

        :param dataset: dataset to adjust non-CO2 emission for
        :param sector: GAINS industrial sector to look up
        :return: Does not return anything. Modified in place.
        """

        # Update biosphere exchanges according to GAINS emission values
        relevant = set(self.ei_pollutants)
        biosphere_excs = [
            exc
            for exc in dataset["exchanges"]
            if exc["type"] == "biosphere" and exc["name"] in relevant
        ]

        for exc in biosphere_excs:
            gains_pollutant = self.ei_pollutants[exc["name"]]
            scaling_factor = self.find_gains_emissions_change(
                pollutant=gains_pollutant,
                location=(
                    dataset["location"]
                    if dataset["location"] in regions
                    else self.ecoinvent_to_iam_loc[dataset["location"]]
                ),
                sector=sector,
            )

            if 1 > scaling_factor > 0:
                if gains_pollutant not in dataset.get("log parameters", {}):
                    wurst.rescale_exchange(
                        exc, scaling_factor, remove_uncertainty=False
                    )

                    logp = dataset.setdefault("log parameters", {})
                    if "GAINS sector" not in logp:
                        logp["GAINS sector"] = sector
                    logp[gains_pollutant] = scaling_factor

        return dataset

    @lru_cache
    def find_gains_emissions_change(
        self, pollutant: str, location: str, sector: str
    ) -> Union[ndarray, float]:
        """
        Return the relative change in emissions compared to 2020
        for a given pollutant, location and sector.
        :param pollutant: name of pollutant
        :param sector: name of technology/sector
        :param location: location of emitting dataset
        :model: GAINS model
        :return: a
        """

        data = self.gains_IAM

        sf = data.loc[dict(region=location, pollutant=pollutant, sector=sector)].item()

        if np.isnan(sf) or sf == 0.0:
            return 1.0

        return float(sf)

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        if "GAINS sector" in dataset.get("log parameters", {}):
            logger.info(
                f"{status}|{self.model}|{self.scenario}|{self.year}|"
                f"{dataset['name']}|{dataset['location']}|"
                f"{dataset.get('log parameters', {}).get('GAINS sector', '')}|"
                f"{dataset.get('log parameters', {}).get('CH4', '')}|"
                f"{dataset.get('log parameters', {}).get('N2O', '')}|"
                f"{dataset.get('log parameters', {}).get('NH3', '')}|"
                f"{dataset.get('log parameters', {}).get('NOx', '')}|"
                f"{dataset.get('log parameters', {}).get('PM1', '')}|"
                f"{dataset.get('log parameters', {}).get('PM10', '')}|"
                f"{dataset.get('log parameters', {}).get('PM25', '')}|"
                f"{dataset.get('log parameters', {}).get('SO2', '')}|"
                f"{dataset.get('log parameters', {}).get('VOC', '')}"
            )
