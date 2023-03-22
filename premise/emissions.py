"""
Integrates projections regarding emissions of hot pollutants
from GAINS.
"""

import logging.config

import numpy as np
import xarray as xr
import yaml

from .transformation import (
    BaseTransformation,
    Dict,
    IAMDataCollection,
    InventorySet,
    List,
    Set,
    ws,
    wurst,
)
from .utils import DATA_DIR

EI_POLLUTANTS = DATA_DIR / "GAINS_emission_factors" / "GAINS_ei_pollutants.yaml"
GAINS_SECTORS = DATA_DIR / "GAINS_emission_factors" / "GAINS_EU_sectors_mapping.yaml"
LOG_CONFIG = DATA_DIR / "utils" / "logging" / "logconfig.yaml"

with open(LOG_CONFIG, "r") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger("emissions")


def fetch_mapping(filepath: str) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, "r", encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


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
            database, iam_data, model, pathway, year, version, system_model
        )

        self.version = version
        self.gains_EU = iam_data.gains_data_EU
        self.gains_IAM = iam_data.gains_data_IAM
        self.ei_pollutants = fetch_mapping(EI_POLLUTANTS)
        self.gains_pollutant = {v: k for k, v in self.ei_pollutants.items()}
        self.gains_sectors = fetch_mapping(GAINS_SECTORS)
        self.gains_scenario = gains_scenario

        mapping = InventorySet(self.database)
        self.gains_map_EU: Dict[str, Set] = mapping.generate_gains_mapping()
        self.gains_map_IAM: Dict[str, Set] = mapping.generate_gains_mapping_IAM()
        self.rev_gains_map_EU, self.rev_gains_map_IAM = {}, {}

        for s in self.gains_map_EU:
            for t in self.gains_map_EU[s]:
                self.rev_gains_map_EU[t] = s

        for s in self.gains_map_IAM:
            for t in self.gains_map_IAM[s]:
                self.rev_gains_map_IAM[t] = s

    def update_emissions_in_database(self):
        print("Integrating GAINS EU emission factors.")
        for ds in self.database:
            if (
                ds["name"] in self.rev_gains_map_EU
                and ds["location"] in self.gains_EU.coords["region"]
            ):
                gains_sector = self.rev_gains_map_EU[ds["name"]]
                self.update_pollutant_emissions(
                    ds, gains_sector, self.gains_EU, model="GAINS-EU"
                )
                self.write_log(ds, status="updated")

        print("Integrating GAINS IAM emission factors.")
        for ds in self.database:
            if (
                ds["name"] in self.rev_gains_map_IAM
                and self.ecoinvent_to_iam_loc[ds["location"]]
                in self.gains_IAM.coords["region"]
            ):
                gains_sector = self.rev_gains_map_IAM[ds["name"]]
                self.update_pollutant_emissions(
                    ds, gains_sector, self.gains_IAM, model="GAINS-IAM"
                )

                self.write_log(ds, status="updated")

    def update_pollutant_emissions(
        self, dataset: dict, sector: str, gains_data: xr.DataArray, model: str
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
        for exc in ws.biosphere(
            dataset, ws.either(*[ws.equals("name", x) for x in self.ei_pollutants])
        ):
            gains_pollutant = self.ei_pollutants[exc["name"]]
            scaling_factor = self.find_gains_emissions_change(
                pollutant=gains_pollutant,
                location=(
                    dataset["location"]
                    if dataset["location"] in gains_data.region.values
                    else self.ecoinvent_to_iam_loc[dataset["location"]]
                ),
                sector=sector,
                data=gains_data,
            )

            if scaling_factor != 1.0:
                if f"{gains_pollutant} scaling factor" not in dataset.get(
                    "log parameters", {}
                ):
                    wurst.rescale_exchange(exc, scaling_factor)

                    if "log parameters" not in dataset:
                        dataset["log parameters"] = {}

                    if "GAINS model" not in dataset["log parameters"]:
                        dataset["log parameters"]["GAINS model"] = model

                    if "GAINS sector" not in dataset["log parameters"]:
                        dataset["log parameters"]["GAINS sector"] = sector

                    dataset["log parameters"].update(
                        {f"{gains_pollutant} scaling factor": scaling_factor}
                    )

        return dataset

    def find_gains_emissions_change(
        self, pollutant: str, location: str, sector: str, data: xr.DataArray
    ) -> float:
        """
        Return the relative change in emissions compared to 2020
        for a given pollutant, location and sector.
        :param pollutant: name of pollutant
        :param sector: name of technology/sector
        :param location: location of emitting dataset
        :return: a scaling factor
        """

        _ = lambda x: np.where((np.isnan(x)) | (x == 0), 1, x)

        try:
            scaling_factor = _(
                data.loc[
                    dict(
                        region=location,
                        pollutant=pollutant,
                        sector=sector,
                    )
                ].interp(year=self.year)
            ) / _(
                data.loc[
                    dict(
                        region=location,
                        pollutant=pollutant,
                        sector=sector,
                        year=2020,
                    )
                ]
            )
        except KeyError:
            scaling_factor = 1.0

        if self.year < 2020:
            scaling_factor = np.clip(
                scaling_factor,
                1,
                None,
            )
        else:
            scaling_factor = np.clip(
                scaling_factor,
                0,
                1,
            )

        if not np.isnan(scaling_factor) and scaling_factor != 1.0:
            return scaling_factor.astype(float)
        else:
            return 1.0

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        if "log parameters" in dataset:
            if "GAINS model" in dataset["log parameters"]:
                logger.info(
                    f"{status}|{self.model}|{self.scenario}|{self.year}|"
                    f"{dataset['name']}|{dataset['location']}|"
                    f"{dataset.get('log parameters', {}).get('GAINS model', '')}|"
                    f"{dataset.get('log parameters', {}).get('GAINS sector', '')}|"
                    f"{dataset.get('log parameters', {}).get('CH4 scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('N2O scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('NH3 scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('NOx scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('PM1 scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('PM10 scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('PM25 scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('SO2 scaling factor', '')}|"
                    f"{dataset.get('log parameters', {}).get('VOC scaling factor', '')}"
                )
