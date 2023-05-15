"""
data_collection.py contains the IAMDataCollection class which collects a number of data,
mostly from the IAM file. This class will have offer market shares, efficiency
and emission values for different sectors, carbon capture rates, etc.
"""

import copy
import csv
import itertools
from functools import lru_cache
from io import StringIO
from itertools import chain
from pathlib import Path
from typing import Dict, List, Union

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from cryptography.fernet import Fernet

from . import DATA_DIR, VARIABLES_DIR
from .marginal_mixes import consequential_method

IAM_ELEC_VARS = VARIABLES_DIR / "electricity_variables.yaml"
IAM_FUELS_VARS = VARIABLES_DIR / "fuels_variables.yaml"
IAM_BIOMASS_VARS = VARIABLES_DIR / "biomass_variables.yaml"
IAM_CROPS_VARS = VARIABLES_DIR / "crops_variables.yaml"
IAM_CEMENT_VARS = VARIABLES_DIR / "cement_variables.yaml"
IAM_STEEL_VARS = VARIABLES_DIR / "steel_variables.yaml"
IAM_DAC_VARS = VARIABLES_DIR / "direct_air_capture_variables.yaml"
IAM_OTHER_VARS = VARIABLES_DIR / "other_variables.yaml"
FILEPATH_FLEET_COMP = (
    DATA_DIR / "iam_output_files" / "fleet_files" / "fleet_all_vehicles.csv"
)
FILEPATH_IMAGE_TRUCKS_FLEET_COMP = (
    DATA_DIR / "iam_output_files" / "fleet_files" / "image_fleet_trucks.csv"
)
VEHICLES_MAP = DATA_DIR / "transport" / "vehicles_map.yaml"
IAM_CARBON_CAPTURE_VARS = VARIABLES_DIR / "carbon_capture_variables.yaml"
CROPS_PROPERTIES = VARIABLES_DIR / "crops_variables.yaml"
GAINS_GEO_MAP = VARIABLES_DIR / "gains_regions_mapping.yaml"


def get_delimiter(data=None, filepath=None):
    sniffer = csv.Sniffer()
    if filepath:
        data = open(filepath, "r").readline()
    delimiter = sniffer.sniff(data).delimiter
    return delimiter


def get_crops_properties() -> dict:
    """
    Return a dictionary with crop names as keys and IAM labels as values
    relating to land use change CO2 per crop type
    :return: dict
    """
    with open(CROPS_PROPERTIES, "r", encoding="utf-8") as stream:
        crop_props = yaml.safe_load(stream)

    return crop_props


@lru_cache
def get_gains_IAM_data(model, gains_scenario):
    filepath = Path(
        DATA_DIR / "GAINS_emission_factors" / "iam_data" / gains_scenario
    ).glob("*")

    list_arrays = []

    for file in filepath:
        df = pd.read_csv(
            file, sep=get_delimiter(filepath=file), encoding="utf-8", low_memory=False
        )
        df = df.rename(columns={"Region": "region", "EMF30 Sector": "sector"})
        df = df.rename(columns={str(v): int(v) for v in range(1990, 2055, 5)})

        df["pollutant"] = file.stem

        array = (
            df.melt(
                id_vars=["region", "sector", "pollutant"],
                value_vars=range(1990, 2055, 5),
                var_name="year",
                value_name="value",
            )
            .groupby(["region", "sector", "year", "pollutant"])["value"]
            .mean()
            .to_xarray()
        )

        array = array.interpolate_na(
            dim="year", method="nearest", fill_value="extrapolate"
        )
        array = array.bfill(dim="year")
        array = array.ffill(dim="year")

        list_arrays.append(array)

    arr = xr.concat(list_arrays, dim="pollutant")

    with open(GAINS_GEO_MAP, "r", encoding="utf-8") as stream:
        geo_map = yaml.safe_load(stream)

    arr.coords["region"] = [geo_map[v][model] for v in arr.region.values]
    arr = arr.drop_duplicates(dim="region")

    return arr


@lru_cache
def get_gains_EU_data() -> xr.DataArray:
    """
    Read the GAINS emissions csv file and return an `xarray` with dimensions:

    * region
    * pollutant
    * sector
    * year

    :return: a multidimensional array with GAINS emissions data

    """

    filename = "GAINS_emission_factors_EU.csv"
    filepath = DATA_DIR / "GAINS_emission_factors" / filename

    gains_emi_EU = pd.read_csv(
        filepath,
        delimiter=get_delimiter(filepath=filepath),
        low_memory=False,
        dtype={
            "Region": str,
            "Sector": str,
            "Activity": str,
            "variable": str,
            "value": float,
            "year": int,
            "substance": str,
            "Activity_long": str,
        },
        encoding="utf-8",
    )
    gains_emi_EU["sector"] = gains_emi_EU["Sector"] + gains_emi_EU["Activity"]
    gains_emi_EU.drop(
        [
            "Sector",
            "Activity",
        ],
        axis=1,
    )

    gains_emi_EU = gains_emi_EU[~gains_emi_EU["value"].isna()]

    gains_emi_EU = gains_emi_EU.rename(
        columns={"Region": "region", "substance": "pollutant"}
    )

    array = (
        gains_emi_EU.groupby(["region", "pollutant", "year", "sector"])["value"]
        .mean()
        .to_xarray()
    )

    array = array.interpolate_na(dim="year", method="nearest", fill_value="extrapolate")
    array = array.bfill(dim="year")
    array = array.ffill(dim="year")

    return array


def get_vehicle_fleet_composition(model, vehicle_type) -> Union[xr.DataArray, None]:
    """
    Read the fleet composition csv file and return an `xarray` with dimensions:
    "region", "year", "powertrain", "construction_year", "size"
    :param model: the model to get the fleet composition for
    :param vehicle_type: the type of vehicle to get the fleet composition for
    :return: a multidimensional array with fleet composition data
    """

    if not FILEPATH_FLEET_COMP.is_file():
        raise FileNotFoundError("The fleet composition file could not be found.")

    if model == "remind":
        dataframe = pd.read_csv(
            FILEPATH_FLEET_COMP, sep=get_delimiter(filepath=FILEPATH_FLEET_COMP)
        )
    else:
        dataframe = pd.read_csv(
            FILEPATH_IMAGE_TRUCKS_FLEET_COMP,
            sep=get_delimiter(filepath=FILEPATH_FLEET_COMP),
        )

    dataframe = dataframe.loc[~dataframe["region"].isnull()]

    with open(VEHICLES_MAP, "r", encoding="utf-8") as stream:
        size_ftr = yaml.safe_load(stream)[vehicle_type]["sizes"]

    dataframe = dataframe.loc[dataframe["size"].isin(size_ftr)]

    if len(dataframe) > 0:
        arr = (
            dataframe.groupby(
                ["region", "year", "powertrain", "construction_year", "size"]
            )
            .sum()["vintage_demand_vkm"]
            .to_xarray()
        )
        arr = arr.fillna(0)

        return arr

    return None


def fix_efficiencies(data: xr.DataArray) -> xr.DataArray:
    # If we are looking at a year post 2020
    # and the ratio in efficiency change is inferior to 1
    # we correct it to 1, as we do not accept
    # that efficiency degrades over time
    data.loc[dict(year=[y for y in data.year.values if y > 2020])] = np.clip(
        data.loc[dict(year=[y for y in data.year.values if y > 2020])],
        1,
        None,
    )

    # Inversely, if we are looking at a year prior to 2020
    # and the ratio in efficiency change is superior to 1
    # we correct it to 1, as we do not accept
    # that efficiency in the past was higher than now
    data.loc[dict(year=[y for y in data.year.values if y < 2020])] = np.clip(
        data.loc[dict(year=[y for y in data.year.values if y < 2020])],
        None,
        1,
    )

    # ensure that efficiency can not decrease over time
    while data.diff(dim="year").min().values < 0:
        diff = data.diff(dim="year")
        diff = xr.concat([data.sel(year=2005), diff], dim="year")
        diff.values[diff.values > 0] = 0
        diff *= -1
        data += diff

    # convert NaNs to ones
    data = data.fillna(1)

    # only consider efficiency change between
    # 50% and 300% relative to 2020
    data.values = np.clip(data, 0.5, None)

    return data


class IAMDataCollection:
    """
    :var model: name of the IAM model (e.g., "remind")
    :var pathway: name of the IAM scenario (e.g., "SSP2-Base")
    :var year: year to produce the database for
    :var system_model: "cutoff" or "consequential".
    """

    def __init__(
        self,
        model: str,
        pathway: str,
        year: int,
        filepath_iam_files: Path,
        key: bytes,
        external_scenarios: dict = None,
        system_model: str = "cutoff",
        system_model_args: dict = None,
        gains_scenario: str = "CLE",
    ) -> None:
        self.model = model
        self.pathway = pathway
        self.year = year
        self.external_scenarios = external_scenarios
        self.system_model_args = system_model_args
        key = key or None

        prod_vars = self.__get_iam_variable_labels(
            IAM_ELEC_VARS, variable="iam_aliases"
        )
        eff_vars = self.__get_iam_variable_labels(IAM_ELEC_VARS, variable="eff_aliases")

        prod_vars.update(
            self.__get_iam_variable_labels(IAM_FUELS_VARS, variable="iam_aliases")
        )
        eff_vars.update(
            self.__get_iam_variable_labels(IAM_FUELS_VARS, variable="eff_aliases")
        )
        prod_vars.update(
            self.__get_iam_variable_labels(IAM_CEMENT_VARS, variable="iam_aliases")
        )
        eff_vars.update(
            self.__get_iam_variable_labels(
                IAM_CEMENT_VARS, variable="energy_use_aliases"
            )
        )
        eff_vars.update(
            self.__get_iam_variable_labels(IAM_CEMENT_VARS, variable="eff_aliases")
        )
        energy_use_vars = self.__get_iam_variable_labels(
            IAM_CEMENT_VARS, variable="energy_use_aliases"
        )
        prod_vars.update(
            self.__get_iam_variable_labels(IAM_STEEL_VARS, variable="iam_aliases")
        )
        eff_vars.update(
            self.__get_iam_variable_labels(
                IAM_STEEL_VARS, variable="energy_use_aliases"
            )
        )
        energy_use_vars.update(
            self.__get_iam_variable_labels(
                IAM_STEEL_VARS, variable="energy_use_aliases"
            )
        )
        prod_vars.update(
            self.__get_iam_variable_labels(IAM_DAC_VARS, variable="iam_aliases")
        )

        prod_vars.update(
            self.__get_iam_variable_labels(IAM_BIOMASS_VARS, variable="iam_aliases")
        )
        eff_vars.update(
            self.__get_iam_variable_labels(IAM_BIOMASS_VARS, variable="eff_aliases")
        )
        land_use_vars = self.__get_iam_variable_labels(
            IAM_CROPS_VARS, variable="land_use"
        )
        land_use_change_vars = self.__get_iam_variable_labels(
            IAM_CROPS_VARS, variable="land_use_change"
        )

        carbon_capture_vars = self.__get_iam_variable_labels(
            IAM_CARBON_CAPTURE_VARS, variable="iam_aliases"
        )

        other_vars = self.__get_iam_variable_labels(
            IAM_OTHER_VARS, variable="iam_aliases"
        )

        variables = list(prod_vars.values())
        variables.extend(eff_vars.values())
        variables.extend(energy_use_vars.values())
        variables.extend(land_use_vars.values())
        variables.extend(land_use_change_vars.values())
        variables.extend(carbon_capture_vars.values())
        variables.extend(other_vars.values())
        new_vars = []
        for variable in variables:
            if isinstance(variable, list):
                for sub_var in variable:
                    new_vars.append(sub_var)
            else:
                new_vars.append(variable)

        data = self.__get_iam_data(
            key=key,
            filedir=filepath_iam_files,
            variables=new_vars,
        )

        self.regions = data.region.values.tolist()
        self.system_model = system_model

        self.gains_data_EU = get_gains_EU_data()
        self.gains_data_IAM = get_gains_IAM_data(
            self.model, gains_scenario=gains_scenario
        )

        self.electricity_markets = self.__get_iam_electricity_markets(data=data)
        self.fuel_markets = self.__get_iam_fuel_markets(data=data)

        self.production_volumes = self.__get_iam_production_volumes(
            prod_vars, data=data
        )
        self.carbon_capture_rate = self.__get_carbon_capture_rate(
            dict_vars=self.__get_iam_variable_labels(
                IAM_CARBON_CAPTURE_VARS, variable="iam_aliases"
            ),
            data=data,
        )

        self.other_vars = self.__get_other_iam_vars(data=data)

        electricity_efficiencies = self.__get_iam_electricity_efficiencies(data=data)
        cement_efficiencies = self.__get_iam_cement_efficiencies(data=data)
        steel_efficiencies = self.__get_iam_steel_efficiencies(data=data)
        fuel_efficiencies = self.__get_iam_fuel_efficiencies(data=data)

        self.efficiency = xr.concat(
            [
                electricity_efficiencies,
                steel_efficiencies,
                cement_efficiencies,
                fuel_efficiencies,
            ],
            dim="variables",
        )

        if self.model == "image":
            self.land_use = self.__get_iam_land_use(data=data)
            self.land_use_change = self.__get_iam_land_use_change_emissions(data=data)
        else:
            self.land_use = None
            self.land_use_change = None

        self.trsp_cars = get_vehicle_fleet_composition(self.model, vehicle_type="car")
        self.trsp_trucks = get_vehicle_fleet_composition(
            self.model, vehicle_type="truck"
        )
        self.trsp_buses = get_vehicle_fleet_composition(self.model, vehicle_type="bus")

    def __get_iam_variable_labels(
        self, filepath: Path, variable: str
    ) -> Dict[str, Union[str, List[str]]]:
        """
        Loads a csv file into a dictionary.
        This dictionary contains common terminology to ``premise``
        (fuel names, electricity production technologies, etc.) and its
        equivalent variable name in the IAM file.
        :return: dictionary that contains fuel production names equivalence
        """

        dict_vars = {}

        with open(filepath, "r", encoding="utf-8") as stream:
            out = yaml.safe_load(stream)

        for key, values in out.items():
            if variable in values:
                if self.model in values[variable]:
                    if values[variable][self.model] is not None:
                        dict_vars[key] = values[variable][self.model]

        return dict_vars

    def __get_iam_data(
        self, key: bytes, filedir: Path, variables: List
    ) -> xr.DataArray:
        """
        Read the IAM result file and return an `xarray` with dimensions:

        * region
        * variable
        * year

        :param key: encryption key, if provided by user
        :param filepath: file path to IAM file

        :return: a multidimensional array with IAM data

        """

        file_ext = self.model + "_" + self.pathway + ".csv"
        filepath = Path(filedir) / file_ext

        if key is None:
            # Uses a non-encrypted file
            try:
                with open(filepath, "rb") as file:
                    # read the encrypted data
                    encrypted_data = file.read()
            except FileNotFoundError:
                file_ext = self.model + "_" + self.pathway + ".mif"
                filepath = Path(filedir) / file_ext
                with open(filepath, "rb") as file:
                    # read the encrypted data
                    encrypted_data = file.read()

            # create a temp csv-like file to pass to pandas.read_csv()
            data = StringIO(str(encrypted_data, "latin-1"))

        else:
            # Uses an encrypted file
            fernet_obj = Fernet(key)
            with open(filepath, "rb") as file:
                # read the encrypted data
                encrypted_data = file.read()

            # decrypt data
            decrypted_data = fernet_obj.decrypt(encrypted_data)
            data = StringIO(str(decrypted_data, "latin-1"))

        dataframe = pd.read_csv(
            data,
            sep=get_delimiter(data=copy.copy(data).readline()),
            encoding="latin-1",
        )

        # if a column name can be an integer
        # we convert it to an integer
        new_cols = {c: int(c) if c.isdigit() else c for c in dataframe.columns}
        dataframe = dataframe.rename(columns=new_cols)

        # remove any column that is a string
        # and that is not any of "Region", "Variable", "Unit"
        for col in dataframe.columns:
            if isinstance(col, str) and col not in ["Region", "Variable", "Unit"]:
                dataframe = dataframe.drop(col, axis=1)

        dataframe = dataframe.reset_index()

        # remove "index" column
        if "index" in dataframe.columns:
            dataframe = dataframe.drop("index", axis=1)

        dataframe = dataframe.loc[dataframe["Variable"].isin(variables)]

        dataframe = dataframe.rename(
            columns={"Region": "region", "Variable": "variables", "Unit": "unit"}
        )

        array = (
            dataframe.melt(
                id_vars=["region", "variables", "unit"],
                var_name="year",
                value_name="value",
            )[["region", "variables", "year", "value"]]
            .groupby(["region", "variables", "year"])["value"]
            .mean()
            .to_xarray()
        )

        return array

    def __get_other_iam_vars(self, data: xr.DataArray) -> xr.DataArray:
        """
        Returns various IAM variables.

        :return: array containing various IAM variables
        """

        labels = self.__get_iam_variable_labels(IAM_OTHER_VARS, variable="iam_aliases")

        list_vars = list(labels.values())
        list_vars = [l for p in list_vars for l in p]

        data_to_return = data.loc[:, list_vars, :]

        return data_to_return

    def __get_iam_electricity_markets(self, data: xr.DataArray) -> xr.DataArray:
        """
        This method retrieves the market share for each electricity-producing technology,
        or a specified year, for each region provided by the IAM.

        :return: a multidimensional array with electricity technologies market share
        for a given year, for all regions.

        """

        labels = self.__get_iam_variable_labels(IAM_ELEC_VARS, variable="iam_aliases")

        list_technologies = list(labels.values())

        # If the year specified is not contained within the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        # Finally, if the specified year falls in between two periods provided by the IAM
        # Interpolation between two periods

        try:
            data_to_return = data.loc[:, list_technologies, :]
        except KeyError as exc:
            list_missing_vars = [
                var for var in list_technologies if var not in data.variables.values
            ]
            print(
                f"The following variables cannot be found in the IAM file: {list_missing_vars}"
            )
            if len(list_technologies) - len(list_missing_vars) > 0:
                available_vars = [
                    var for var in list_technologies if var in data.variables.values
                ]
                print(
                    "The process continues with the remaining variables, "
                    "but certain transformation functions may not work."
                )
                list_technologies = available_vars
                data_to_return = data.loc[:, list_technologies, :]
            else:
                raise SystemExit from exc

        # give the array premise labels
        list_vars = [k for k, v in labels.items() if v in list_technologies]

        data_to_return.coords["variables"] = list_vars

        if self.system_model == "consequential":
            data_to_return = consequential_method(
                data_to_return, self.year, self.system_model_args
            )

        else:
            data_to_return /= (
                data.loc[:, list_technologies, :].groupby("region").sum(dim="variables")
            )

        return data_to_return

    def __get_iam_electricity_efficiencies(self, data: xr.DataArray) -> xr.DataArray:
        """
        This method retrieves efficiency values for
        electricity-producing technology,
        for a specified year, for each region
        provided by the IAM.
        Electricity production from hydrogen can
        be removed from the mix
        (unless specified, it is removed).

        :return: a multidimensional array with electricity
        technologies market share for a given year, for all regions.

        """

        labels = self.__get_iam_variable_labels(IAM_ELEC_VARS, variable="eff_aliases")

        list_technologies = list(labels.values())

        # If the year specified is not contained within the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        # Finally, if the specified year falls in between two periods provided by the IAM
        # Interpolation between two periods

        try:
            data_to_return = data.loc[:, list_technologies, :]
        except KeyError as exc:
            list_missing_vars = [
                var for var in list_technologies if var not in data.variables.values
            ]
            print(
                f"The following variables cannot be found in the IAM file: {list_missing_vars}"
            )
            if len(list_technologies) - len(list_missing_vars) > 0:
                available_vars = [
                    var for var in list_technologies if var in data.variables.values
                ]
                print(
                    "The process continues with the remaining variables, "
                    "but certain transformation functions may not work."
                )
                list_technologies = available_vars
                data_to_return = data.loc[:, list_technologies, :]
            else:
                raise SystemExit from exc

        data_to_return /= data_to_return.sel(year=2020)

        # fix efficiencies
        data_to_return = fix_efficiencies(data_to_return)

        data_to_return.coords["variables"] = [
            k for k, v in labels.items() if v in list_technologies
        ]

        return data_to_return

    def __get_iam_cement_efficiencies(self, data: xr.DataArray) -> xr.DataArray:
        """
        This method retrieves specific energy use values for cement-producing technology,
        for a specified year, for each region provided by the IAM.

        :return: a multidimensional array with electricity technologies market share
        for a given year, for all regions.

        """

        # If the year specified is not contained within
        # the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        if (
            len(self.__get_iam_variable_labels(IAM_CEMENT_VARS, variable="eff_aliases"))
            > 0
        ):
            eff = self.__get_iam_variable_labels(
                IAM_CEMENT_VARS, variable="eff_aliases"
            )

            data_to_return = xr.DataArray(dims=["variables"], coords={"variables": []})

            for k, v in eff.items():
                if v not in data.variables.values:
                    print("No efficiency variables is given for the cement sector.")
                    d = xr.ones_like(data)
                else:
                    d = 1 / data.loc[:, [v], :]
                data_to_return = xr.concat([data_to_return, d], dim="variables")

            # back-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.ffill(dim="variables")
            )
            # forward-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.bfill(dim="variables")
            )

            data_to_return.coords["variables"] = list(eff.keys())

        else:
            prod = self.__get_iam_variable_labels(
                IAM_CEMENT_VARS, variable="iam_aliases"
            )
            energy = self.__get_iam_variable_labels(
                IAM_CEMENT_VARS, variable="energy_use_aliases"
            )

            data_to_return = xr.DataArray(dims=["variables"], coords={"variables": []})

            for k, v in prod.items():
                # check that each element of energy.values() is in data.variables.values
                # knowing that energy.values() is a list of lists
                # and that each element of prod.values() is in data.variables.values
                if (
                    all(var in data.variables.values for var in energy[k])
                    and v in data.variables.values
                ):
                    d = 1 / (
                        data.loc[:, energy[k], :].sum(dim="variables")
                        / data.loc[:, v, :]
                    )

                else:
                    print("No efficiency variables is given for the cement sector.")
                    d = xr.ones_like(data)

                data_to_return = xr.concat([data_to_return, d], dim="variables")

            # back-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.ffill(dim="variables")
            )
            # forward-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.bfill(dim="variables")
            )

            data_to_return.coords["variables"] = list(prod.keys())

        data_to_return = data_to_return / data_to_return.sel(year=2020)
        # fix efficiencies
        data_to_return = fix_efficiencies(data_to_return)
        return data_to_return

    def __get_iam_steel_efficiencies(self, data: xr.DataArray) -> xr.DataArray:
        """
        This method retrieves specific energy use values for steel-producing technology,
        for a specified year, for each region provided by the IAM.

        :return: a multi-dimensional array with electricity technologies market share
        for a given year, for all regions.

        """

        # If the year specified is not contained within the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        if (
            len(self.__get_iam_variable_labels(IAM_STEEL_VARS, variable="eff_aliases"))
            > 0
        ):
            eff = self.__get_iam_variable_labels(IAM_STEEL_VARS, variable="eff_aliases")

            data_to_return = xr.DataArray(dims=["variables"], coords={"variables": []})

            for k, v in eff.items():
                if v not in data.variables.values:
                    print("No efficiency variables is given for the steel sector.")
                    d = xr.ones_like(data)
                else:
                    d = 1 / data.loc[:, [v], :]
                data_to_return = xr.concat([data_to_return, d], dim="variables")

            # back-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.ffill(dim="variables")
            )
            # forward-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.bfill(dim="variables")
            )

            data_to_return.coords["variables"] = list(eff.keys())

        else:
            prod = self.__get_iam_variable_labels(
                IAM_STEEL_VARS, variable="iam_aliases"
            )
            energy = self.__get_iam_variable_labels(
                IAM_STEEL_VARS, variable="energy_use_aliases"
            )

            data_to_return = xr.DataArray(dims=["variables"], coords={"variables": []})

            for k, v in prod.items():
                # check that each element of energy.values() is in data.variables.values
                # knowing that energy.values() is a list of lists
                # and that each element of prod.values() is in data.variables.values

                if (
                    all(var in data.variables.values for var in energy[k])
                    and v in data.variables.values
                ):
                    d = 1 / (
                        data.loc[:, energy[k], :].sum(dim="variables")
                        / data.loc[:, v, :]
                    )

                else:
                    print("No efficiency variables is given for the steel sector.")
                    d = xr.ones_like(data)

                data_to_return = xr.concat([data_to_return, d], dim="variables")

            # back-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.ffill(dim="variables")
            )
            # forward-fill missing values with nearest available
            data_to_return = data_to_return.fillna(
                data_to_return.bfill(dim="variables")
            )

            data_to_return.coords["variables"] = list(prod.keys())

        data_to_return = data_to_return / data_to_return.sel(year=2020)
        # fix efficiencies
        data_to_return = fix_efficiencies(data_to_return)

        return data_to_return

    def __get_iam_fuel_markets(self, data: xr.DataArray) -> xr.DataArray:
        """
        This method retrieves the market share
        for each fuel-producing technology,
        for a specified year, for each
        region provided by the IAM.

        :return: a multidimensional array with
        electricity technologies market share
        for a given year, for all regions.

        """

        labels = self.__get_iam_variable_labels(IAM_FUELS_VARS, variable="iam_aliases")

        list_technologies = list(labels.values())

        # If the year specified is not contained within the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        # Finally, if the specified year falls in between
        # two periods provided by the IAM
        # sometimes, the World region is either neglected
        # or wrongly evaluated, so we fix that here

        try:
            data.loc[dict(region="World", variables=list_technologies)] = data.loc[
                dict(
                    region=[r for r in data.coords["region"].values if r != "World"],
                    variables=list_technologies,
                )
            ].sum(dim="region")

        except KeyError as exc:
            list_missing_vars = [
                var for var in list_technologies if var not in data.variables.values
            ]
            print(
                f"The following variables cannot be found in the IAM file: {list_missing_vars}"
            )

            if len(list_technologies) - len(list_missing_vars) > 0:
                available_vars = [
                    var for var in list_technologies if var in data.variables.values
                ]
                print(
                    "The process continues with the remaining variables, "
                    "but certain transformation functions may not work."
                )
                list_technologies = available_vars
            else:
                raise SystemExit from exc

        # Interpolation between two periods
        data_to_return = data.loc[:, list_technologies, :]

        data_to_return.coords["variables"] = [
            k for k, v in labels.items() if v in list_technologies
        ]

        if self.system_model == "consequential":
            # If the system model is consequential,
            # we need to identify marginal suppliers
            # but we need first to separate fuel types

            fuel_types = {
                "diesel": [
                    "diesel",
                    "diesel, synthetic, from electrolysis",
                    "diesel, synthetic, from coal",
                    "diesel, synthetic, from coal, with CCS",
                    "diesel, synthetic, from wood",
                    "diesel, synthetic, from wood, with CCS",
                    "biodiesel, oil",
                ],
                "gasoline": [
                    "gasoline",
                    "petrol, synthetic, from electrolysis",
                    "petrol, synthetic, from coal",
                    "petrol, synthetic, from coal, with CCS",
                    "bioethanol, wood",
                    "bioethanol, wood, with CCS",
                    "bioethanol, grass",
                    "bioethanol, sugar",
                ],
                "natural gas": [
                    "natural gas",
                    "biomethane",
                ],
                "hydrogen": [
                    "hydrogen, electrolysis",
                    "hydrogen, biomass",
                    "hydrogen, biomass, with CCS",
                    "hydrogen, coal",
                    "hydrogen, nat. gas",
                    "hydrogen, nat. gas, with CCS",
                ],
            }

            for fuel_type, fuel_list in fuel_types.items():
                data_to_return.loc[
                    dict(
                        variables=[
                            f
                            for f in fuel_list
                            if f in data_to_return.coords["variables"].values
                        ],
                        year=[self.year],
                    )
                ] = consequential_method(
                    data_to_return.loc[
                        dict(
                            variables=[
                                f
                                for f in fuel_list
                                if f in data_to_return.coords["variables"].values
                            ]
                        )
                    ],
                    self.year,
                    self.system_model_args,
                )

        return data_to_return

    def __get_iam_land_use(self, data):
        """
        Only provided by IMAGE at the moment. Those are land footprint
        associated with growing a given crop type, in hectares per GJ of that crop,
        for each region and year. This land occupation is added to the LCI
        for crop farming in fuels.py.

        :param data: IAM data
        :return: a multidimensional array with land use
        for different crops types, for all years, for all regions.
        """

        crops_vars = get_crops_properties()
        labels = list(crops_vars.keys())
        list_vars = [x["land_use"][self.model] for x in crops_vars.values()]

        try:
            data_to_return = data.loc[:, list_vars, :]

        except KeyError:
            list_missing_vars = [
                var for var in list_vars if var not in data.variables.values
            ]
            raise KeyError(
                f"The following variables cannot be found in the IAM file: {list_missing_vars}"
            )

        data_to_return.coords["variables"] = labels

        return data_to_return

    def __get_iam_land_use_change_emissions(self, data: xr.DataArray) -> xr.DataArray:
        """
        Only provided by IMAGE at the moment. Those are CO2-eq. emissions
        associated with growing a given crop type, per GJ of that crop,
        for each region and year. Such LUC emissions are added to the LCI
        for crop farming in fuels.py.

        :param data: IAM data
        :return: a multi-dimensional array with land use change CO2 emissions
        for different crops types, for all years, for all regions.
        """

        crops_vars = get_crops_properties()
        labels = list(crops_vars.keys())
        list_vars = [x["land_use_change"][self.model] for x in crops_vars.values()]

        try:
            data_to_return = data.loc[:, list_vars, :]

        except KeyError:
            list_missing_vars = [
                var for var in list_vars if var not in data.variables.values
            ]
            raise KeyError(
                f"The following variables cannot be found in the IAM file: {list_missing_vars}"
            )

        data_to_return.coords["variables"] = labels

        return data_to_return

    def __get_iam_fuel_efficiencies(self, data: xr.DataArray) -> xr.DataArray:
        """
        This method retrieves the change in fuel production efficiency
        between the year in question and 2020, for each region provided by the IAM.
        Because we assume that the fuel conversion efficiency in ecoinvent or imported
        inventories are current (hence, representative of 2020).
        If the efficiency drops after 2020, we ignore it and keep the change
        in efficiency ratio to 1.

        :return: a multidimensional array with electricity technologies market
        share for a given year, for all regions.
        """

        labels = self.__get_iam_variable_labels(IAM_FUELS_VARS, variable="eff_aliases")

        list_technologies = list(labels.values())

        # If the year specified is not contained within the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        # Finally, if the specified year falls in between two periods provided by the IAM
        # Interpolation between two periods
        try:
            data_to_return = data.loc[:, list_technologies, :]
        except KeyError as exc:
            list_missing_vars = [
                var for var in list_technologies if var not in data.variables.values
            ]
            print(
                f"The following variables cannot be found in the IAM file: {list_missing_vars}"
            )
            if len(list_technologies) - len(list_missing_vars) > 0:
                available_vars = [
                    var for var in list_technologies if var in data.variables.values
                ]
                print(
                    "The process continues with the remaining variables, "
                    "but certain transformation functions may not work."
                )
                list_technologies = available_vars
                data_to_return = data.loc[:, list_technologies, :]
            else:
                raise SystemExit from exc

        data_to_return /= data_to_return.sel(year=2020)

        # fix efficiencies
        data_to_return = fix_efficiencies(data_to_return)

        data_to_return.coords["variables"] = [
            k for k, v in labels.items() if v in list_technologies
        ]

        return data_to_return

    def __get_carbon_capture_rate(
        self, dict_vars: Dict[str, str], data: xr.DataArray
    ) -> xr.DataArray:
        """
        Returns a xarray with carbon capture rates for steel and cement production.

        :param dict_vars: dictionary that contains AIM variables to search for
        :param data: IAM data
        :return: a xarray with carbon capture rates, for each year and region
        """

        # If the year specified is not contained within the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        # Finally, if the specified year falls in between two periods provided by the IAM
        # Interpolation between two periods

        cement_rate = data.loc[:, dict_vars["cement - cco2"], :].sum(
            dim=["variables"]
        ) / data.loc[:, dict_vars["cement - co2"], :].sum(dim=["variables"])

        cement_rate.coords["variables"] = "cement"

        steel_rate = data.loc[:, dict_vars["steel - cco2"], :].sum(
            dim="variables"
        ) / data.loc[:, dict_vars["steel - co2"], :].sum(dim="variables")
        steel_rate.coords["variables"] = "steel"

        rate = xr.concat([cement_rate, steel_rate], dim="variables")

        # forward fill missing values
        rate = rate.ffill(dim="year")

        rate = rate.fillna(0)

        # we need to fix the rate for "World"
        # as it is sometimes neglected in the
        # IAM files

        rate.loc[dict(region="World", variables="cement")] = (
            data.loc[
                dict(
                    region=[r for r in self.regions if r != "World"],
                    variables=dict_vars["cement - cco2"],
                )
            ]
            .sum(dim=["variables", "region"])
            .values
            / data.loc[
                dict(
                    region=[r for r in self.regions if r != "World"],
                    variables=dict_vars["cement - co2"],
                )
            ]
            .sum(dim=["variables", "region"])
            .values
        )

        rate.loc[dict(region="World", variables="steel")] = data.loc[
            dict(
                region=[r for r in self.regions if r != "World"],
                variables=dict_vars["steel - cco2"],
            )
        ].sum(dim=["variables", "region"]) / data.loc[
            dict(
                region=[r for r in self.regions if r != "World"],
                variables=dict_vars["steel - co2"],
            )
        ].sum(
            dim=["variables", "region"]
        )

        # we ensure that the rate can only be between 0 and 1
        rate.values = np.clip(rate, 0, 1)

        return rate

    def __get_iam_production_volumes(self, dict_products, data) -> xr.DataArray:
        """
        Returns n xarray with production volumes for different sectors:
        electricity, steel, cement, fuels.
        This is used to build markets: we use
        the production volumes of each region for example,
        to build the World market.
        :param dict_products: a dictionary that contains
        common labels as keys, and IAM labels as values.
        :param data: IAM data
        :return: a xarray with production volumes for
        different commodities (electricity, cement, etc.)
        """

        list_products = list(dict_products.values())

        # If the year specified is not contained within the range of years given by the IAM
        if self.year < data.year.values.min() or self.year > data.year.values.max():
            raise KeyError(
                f"{self.year} is outside of the boundaries "
                f"of the IAM file: {data.year.values.min()}-{data.year.values.max()}"
            )

        # Finally, if the specified year falls in between
        # two periods provided by the IAM
        # Interpolation between two periods

        try:
            # flatten list
            for var in list_products:
                if isinstance(var, list):
                    list_products.extend(var)
                    list_products.remove(var)
            data_to_return = data.loc[:, list_products, :]

        except KeyError as exc:
            list_missing_vars = [
                var for var in list_products if var not in data.variables.values
            ]
            print(
                f"The following variables cannot be found in the IAM file: {list_missing_vars}"
            )
            if len(list_products) - len(list_missing_vars) > 0:
                available_vars = [
                    var for var in list_products if var in data.variables.values
                ]
                print(
                    "The process continues with the remaining variables, "
                    "but certain transformation functions may not work."
                )
                list_products = available_vars
                data_to_return = data.loc[:, list_products, :]
            else:
                raise SystemExit from exc

        data_to_return.coords["variables"] = [
            k for k, v in dict_products.items() if v in list_products
        ]

        return data_to_return

    def get_external_data(self, datapackages):
        data = {}

        for i, dp in enumerate(datapackages):
            data[i] = {}

            resource = dp.get_resource("scenario_data")
            scenario_data = resource.read()
            scenario_headers = resource.headers
            df = pd.DataFrame(scenario_data, columns=scenario_headers)

            resource = dp.get_resource("config")
            config_file = yaml.safe_load(resource.raw_read())

            if "production pathways" in config_file:
                variables = {}
                for k, v in config_file["production pathways"].items():
                    try:
                        variables[k] = v["production volume"]["variable"]
                    except KeyError:
                        continue

                subset = df.loc[
                    (df["model"] == self.model)
                    & (df["pathway"] == self.pathway)
                    & (df["scenario"] == self.external_scenarios[i])
                    & (df["variables"].isin(variables.values())),
                    "region":,
                ]

                array = (
                    subset.melt(
                        id_vars=["region", "variables", "unit"],
                        var_name="year",
                        value_name="value",
                    )[["region", "variables", "year", "value"]]
                    .groupby(["region", "variables", "year"])["value"]
                    .mean()
                    .to_xarray()
                )

                array.coords["year"] = [int(y) for y in array.coords["year"]]

                data[i]["production volume"] = array
                regions = subset["region"].unique().tolist()
                data[i]["regions"] = regions

                variables = {}
                if "production pathways" in config_file:
                    for k, v in config_file["production pathways"].items():
                        try:
                            variables[k] = [e["variable"] for e in v["efficiency"]]
                        except KeyError:
                            continue

                if "markets" in config_file:
                    for m, market in enumerate(config_file["markets"]):
                        try:
                            variables[f"market {m}"] = [
                                e["variable"] for e in market["efficiency"]
                            ]
                        except KeyError:
                            continue

                if len(variables) > 0:
                    subset = df.loc[
                        (df["model"] == self.model)
                        & (df["pathway"] == self.pathway)
                        & (df["scenario"] == self.external_scenarios[i])
                        & (df["variables"].isin(list(chain(*variables.values())))),
                        "region":,
                    ]

                    array = (
                        subset.melt(
                            id_vars=["region", "variables", "unit"],
                            var_name="year",
                            value_name="value",
                        )[["region", "variables", "year", "value"]]
                        .groupby(["region", "variables", "year"])["value"]
                        .mean()
                        .to_xarray()
                    )
                    array.coords["year"] = [int(y) for y in array.coords["year"]]

                    ref_years = {}

                    if "production pathways" in config_file:
                        for v in config_file["production pathways"].values():
                            for e, f in v.items():
                                if e == "efficiency":
                                    for x in f:
                                        ref_years[x["variable"]] = x.get(
                                            "reference year", None
                                        )

                    if "markets" in config_file:
                        for market in config_file["markets"]:
                            for e, f in market.items():
                                if f == "efficiency":
                                    for x in f["efficiency"]:
                                        ref_years[x["variable"]] = x.get(
                                            "reference year", None
                                        )

                    for y, ref_year in ref_years.items():
                        if ref_year is None:
                            # use the earliest year in `array`
                            ref_years[y] = array.year.values.min()

                    for v, y in ref_years.items():
                        array.loc[dict(variables=v)] = array.loc[
                            dict(variables=v)
                        ] / array.loc[dict(variables=v)].sel(year=int(y))

                    # convert NaNs to ones
                    array = array.fillna(1)

                    data[i]["efficiency"] = array

        return data
