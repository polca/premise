"""
ecoinvent_modification.py exposes methods to create a database, perform transformations on it,
as well as export it back.

"""

import copy
import os
import pickle
import sys
from datetime import date
from pathlib import Path
from typing import List, Union
import xarray as xr
import numpy as np

import wurst

from . import DATA_DIR, INVENTORY_DIR
from .clean_datasets import DatabaseCleaner
from .data_collection import IAMDataCollection
from .electricity import Electricity
from .export import (
    export_scenario_difference_file,
    export_scenario_factor_file,
    generate_scenario_difference_file,
)
from .inventory_imports import AdditionalInventory, DefaultInventory, VariousVehicles
from .scenario_report import generate_summary_report
from .utils import (
    c,
    convert_db_to_dataframe,
    convert_df_to_dict,
    create_scenario_label,
    eidb_label,
    s,
    print_version,
    warning_about_biogenic_co2,
    info_on_utils_functions,
    hide_messages,
    HiddenPrints
)

SUPPORTED_EI_VERSIONS = ["3.5", "3.6", "3.7", "3.7.1", "3.8"]
LIST_REMIND_REGIONS = [
    "CAZ",
    "CHA",
    "EUR",
    "IND",
    "JPN",
    "LAM",
    "MEA",
    "NEU",
    "OAS",
    "REF",
    "SSA",
    "USA",
    "World",
]

LIST_IMAGE_REGIONS = [
    "BRA",
    "CAN",
    "CEU",
    "CHN",
    "EAF",
    "INDIA",
    "INDO",
    "JAP",
    "KOR",
    "ME",
    "MEX",
    "NAF",
    "OCE",
    "RCAM",
    "RSAF",
    "RSAM",
    "RSAS",
    "RUS",
    "SAF",
    "SEAS",
    "STAN",
    "TUR",
    "UKR",
    "USA",
    "WAF",
    "WEU",
    "World",
]


DIR_CACHED_DB = DATA_DIR / "cache"

FILEPATH_OIL_GAS_INVENTORIES = INVENTORY_DIR / "lci-ESU-oil-and-gas.xlsx"
FILEPATH_CARMA_INVENTORIES = INVENTORY_DIR / "lci-Carma-CCS.xlsx"
FILEPATH_CHP_INVENTORIES = INVENTORY_DIR / "lci-combined-heat-power-plant-CCS.xlsx"
FILEPATH_DAC_INVENTORIES = INVENTORY_DIR / "lci-direct-air-capture.xlsx"
FILEPATH_BIOFUEL_INVENTORIES = INVENTORY_DIR / "lci-biofuels.xlsx"
FILEPATH_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-biogas.xlsx"

FILEPATH_CARBON_FIBER_INVENTORIES = INVENTORY_DIR / "lci-carbon-fiber.xlsx"
FILEPATH_HYDROGEN_DISTRI_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-distribution.xlsx"

FILEPATH_HYDROGEN_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-electrolysis.xlsx"

FILEPATH_HYDROGEN_BIOGAS_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-smr-atr-biogas.xlsx"
)
FILEPATH_HYDROGEN_NATGAS_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-smr-atr-natgas.xlsx"
)
FILEPATH_HYDROGEN_WOODY_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-wood-gasification.xlsx"
)
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-coal-gasification.xlsx"
)
FILEPATH_SYNFUEL_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-electrolysis.xlsx"
)

FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-wood-gasification.xlsx"
)
FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_WITH_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-wood-gasification-with-CCS.xlsx"
)
FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-coal-gasification.xlsx"
)
FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_WITH_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-coal-gasification-with-CCS.xlsx"
)

FILEPATH_SYNFUEL_FROM_BIOMASS_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-biomass-CCS.xlsx"
)
FILEPATH_SYNGAS_INVENTORIES = INVENTORY_DIR / "lci-syngas.xlsx"
FILEPATH_SYNGAS_FROM_COAL_INVENTORIES = INVENTORY_DIR / "lci-syngas-from-coal.xlsx"
FILEPATH_GEOTHERMAL_HEAT_INVENTORIES = INVENTORY_DIR / "lci-geothermal.xlsx"
FILEPATH_METHANOL_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-electrolysis.xlsx"
)
FILEPATH_METHANOL_CEMENT_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-cement-plant.xlsx"
)
FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-coal.xlsx"
)
FILEPATH_METHANOL_FROM_COAL_FUELS_WITH_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-coal-with-CCS.xlsx"
)
FILEPATH_METHANOL_FROM_BIOMASS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biomass.xlsx"
)
FILEPATH_METHANOL_FROM_BIOGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biogas.xlsx"
)
FILEPATH_METHANOL_FROM_NATGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-natural-gas.xlsx"
)
FILEPATH_LITHIUM = INVENTORY_DIR / "lci-lithium.xlsx"
FILEPATH_BATTERIES = INVENTORY_DIR / "lci-batteries.xlsx"
FILEPATH_PHOTOVOLTAICS = INVENTORY_DIR / "lci-PV.xlsx"
FILEPATH_BIGCC = INVENTORY_DIR / "lci-BIGCC.xlsx"


SUPPORTED_MODELS = ["remind", "image"]
SUPPORTED_PATHWAYS = [
    "SSP2-Base",
    "SSP2-NDC",
    "SSP2-NPi",
    "SSP2-PkBudg1150",
    "SSP2-PkBudg500",
    "SSP2-RCP26",
    "SSP2-RCP19",
    "static",
]


LIST_TRANSF_FUNC = [
    "update_electricity",
]

# Disable printing
def blockPrint():
    sys.stdout = open(os.devnull, "w")


# Restore printing
def enablePrint():
    sys.stdout = sys.__stdout__


def check_ei_filepath(filepath: str) -> Path:
    """Check for the existence of the file path."""

    if not Path(filepath).is_dir():
        raise FileNotFoundError(
            f"The directory for ecospold files {filepath} could not be found."
        )
    return Path(filepath)


def check_model_name(name: str) -> str:
    """Check for the validity of the IAM model name."""
    if name.lower() not in SUPPORTED_MODELS:
        raise ValueError(
            f"Only {SUPPORTED_MODELS} are currently supported, not {name}."
        )
    return name.lower()


def check_pathway_name(name: str, filepath: Path, model: str) -> str:
    """Check the pathway name"""

    if name not in SUPPORTED_PATHWAYS:
        # If the pathway name is not a default one,
        # check that the filepath + pathway name
        # leads to an actual file

        if model.lower() not in name:
            name_check = "_".join((model.lower(), name))
        else:
            name_check = name

        if (filepath / name_check).with_suffix(".mif").is_file():
            return name
        if (filepath / name_check).with_suffix(".xlsx").is_file():
            return name
        if (filepath / name_check).with_suffix(".csv").is_file():
            return name
        raise ValueError(
            f"Only {SUPPORTED_PATHWAYS} are currently supported, not {name}."
        )

    if model.lower() not in name:
        name_check = "_".join((model.lower(), name))
    else:
        name_check = name

    if (filepath / name_check).with_suffix(".mif").is_file():
        return name
    if (filepath / name_check).with_suffix(".xlsx").is_file():
        return name
    if (filepath / name_check).with_suffix(".csv").is_file():
        return name

    raise ValueError(
        f"Cannot find the IAM scenario file at this location: {filepath / name_check}."
    )


def check_year(year: [int, float]) -> int:
    """Check for the validity of the year passed."""
    try:
        year = int(year)
    except ValueError as err:
        raise Exception(f"{year} is not a valid year.") from err

    try:
        assert 2005 <= year <= 2100
    except AssertionError as err:
        raise Exception(f"{year} must be comprised between 2005 and 2100.") from err

    return year


def check_filepath(path: str) -> Path:
    """
    Check for the existence of the file.
    """
    if not Path(path).is_dir():
        raise FileNotFoundError(f"The IAM output directory {path} could not be found.")
    return Path(path)


def check_exclude(list_exc: List[str]) -> List[str]:
    """
    Check for the validity of the list of excluded functions.
    """

    if not isinstance(list_exc, list):
        raise TypeError("`exclude` should be a sequence of strings.")

    if not set(list_exc).issubset(LIST_TRANSF_FUNC):
        raise ValueError(
            "One or several of the transformation that you wish to exclude is not recognized."
        )
    return list_exc


def check_additional_inventories(inventories_list: List[dict]) -> List[dict]:
    """
    Check that any additional inventories that need to be imported are properly listed.
    :param inventories_list: list of dictionaries
    :return: list of dictionaries
    """

    if not isinstance(inventories_list, list):
        raise TypeError(
            "Inventories to import need to be in a sequence of dictionaries like so:"
            "["
            "{'inventories': 'a file path', 'ecoinvent version: '3.6'},"
            " {'inventories': 'a file path', 'ecoinvent version: '3.6'}"
            "]"
        )

    for inventory in inventories_list:
        if not isinstance(inventory, dict):
            raise TypeError(
                "Inventories to import need to be in a sequence of dictionaries like so:"
                "["
                "{'inventories': 'a file path', 'ecoinvent version: '3.6'},"
                " {'inventories': 'a file path', 'ecoinvent version: '3.6'}"
                "]"
            )

        if not all(
            i for i in inventory.keys() if i in ["inventories", "ecoinvent version"]
        ):
            raise TypeError(
                "Both `inventories` and `ecoinvent version` "
                "must be present in the list of inventories to import."
            )

        if not Path(inventory["inventories"]).is_file():
            raise FileNotFoundError(
                f"Cannot find the inventory file: {inventory['inventories']}."
            )
        inventory["inventories"] = Path(inventory["inventories"])

        if inventory["ecoinvent version"] not in ["3.7", "3.7.1", "3.8"]:
            raise ValueError(
                "A lot of trouble will be avoided if the additional "
                f"inventories to import are ecoinvent 3.7 or 3.8-compliant, not {inventory['ecoinvent version']}."
            )

    return inventories_list


def check_db_version(version: [str, float]) -> str:
    """
    Check that the ecoinvent database version is supported
    :param version:
    :return:
    """
    version = str(version)
    if version not in SUPPORTED_EI_VERSIONS:
        raise ValueError(
            f"Only {SUPPORTED_EI_VERSIONS} are currently supported, not {version}."
        )
    return version


def check_scenarios(scenario: dict, key: bytes) -> dict:
    """
    Check that the scenarios are properly formatted and that
    all the necessary info is given.
    """

    if not all(name in scenario for name in ["model", "pathway", "year"]):
        raise ValueError(
            f"Missing parameters in {scenario}. Needs to include at least `model`,"
            f"`pathway` and `year`."
        )

    if "filepath" in scenario:
        filepath = scenario["filepath"]
        scenario["filepath"] = check_filepath(filepath)
    else:
        if key is not None:
            scenario["filepath"] = DATA_DIR / "iam_output_files"
        else:
            raise PermissionError(
                "You will need to provide a decryption key "
                "if you want to use the IAM scenario files included "
                "in premise. If you do not have a key, "
                "please contact the developers."
            )

    scenario["model"] = check_model_name(scenario["model"])
    scenario["pathway"] = check_pathway_name(
        scenario["pathway"], scenario["filepath"], scenario["model"]
    )
    scenario["year"] = check_year(scenario["year"])

    if "exclude" in scenario:
        scenario["exclude"] = check_exclude(scenario["exclude"])

    return scenario


def check_system_model(system_model: str) -> str:
    """
    Check that the system model is valid.
    """

    if not isinstance(system_model, str):
        raise TypeError(
            "The argument `system_model` must be a string"
            "('attributional', 'consequential')."
        )

    if system_model not in ("attributional", "consequential"):
        raise ValueError(
            "The argument `system_model` must be one of the two values:"
            "'attributional', 'consequential'."
        )

    return system_model



class IAMData:
    """
    Class that contains all the IAM data needed to perform
    subsequent operations, for every scenario.

    :var list_data: list of data packages returned by IAMDataCollection
    """

    def __init__(self, list_data: List[IAMDataCollection]):

        self.electricity_markets = xr.concat(
            [d.electricity_markets for d in list_data], dim="scenario"
        )
        self.electricity_markets = self.electricity_markets.interpolate_na(dim="year", method="linear")
        self.production_volumes = xr.concat(
            [d.production_volumes for d in list_data], dim="scenario"
        )
        self.production_volumes = self.production_volumes.interpolate_na(dim="year", method="linear")
        self.fuel_markets = xr.concat(
            [d.fuel_markets for d in list_data], dim="scenario"
        )
        self.fuel_markets = self.fuel_markets.interpolate_na(dim="year", method="linear")
        self.gnr_data = list_data[0].gnr_data
        self.carbon_capture_rate = xr.concat(
            [d.carbon_capture_rate for d in list_data], dim="scenario"
        )
        self.carbon_capture_rate = self.carbon_capture_rate.interpolate_na(dim="year", method="linear")
        self.efficiency = xr.concat([d.efficiency for d in list_data], dim="scenario")
        self.efficiency = self.efficiency.interpolate_na(dim="year", method="linear")
        self.emissions = xr.concat([d.emissions for d in list_data], dim="scenario")
        self.other_vars = xr.concat([d.other_vars for d in list_data], dim="scenario")
        self.other_vars = self.other_vars.interpolate_na(dim="year", method="linear")
        self.trsp_cars = xr.concat([d.trsp_cars for d in list_data if d.trsp_cars is not None], dim="scenario")
        self.trsp_trucks = xr.concat([d.trsp_trucks for d in list_data if d.trsp_trucks is not None], dim="scenario")
        self.trsp_buses = xr.concat([d.trsp_buses for d in list_data if d.trsp_buses is not None], dim="scenario")


class NewDatabase:
    """
    Class that represents a new wurst inventory database, modified according to IAM data.

    :ivar source_type: the source of the ecoinvent database. Can be `brigthway` or `ecospold`.
    :vartype source_type: str
    :ivar source_db: name of the ecoinvent source database
    :vartype source_db: str
    :ivar source_version: version of the ecoinvent source database.
        Currently works with ecoinvent cut-off 3.5, 3.6, 3.7, 3.7.1 and 3.8.
    :vartype source_version: str
    :ivar direct_import: If True, appends pickled inventories to database.
    If False, import inventories via bw2io importer.
    :vartype direct_import: bool
    :ivar system_model: Can be `attributional` (default) or `consequential`.
    :vartype system_model: str

    """

    def __init__(
        self,
        scenarios: List[dict],
        source_version: str = "3.8",
        source_type: str = "brightway",
        key: bytes = None,
        source_db: str = None,
        source_file_path: str = None,
        additional_inventories: List[dict] = None,
        system_model: str = "attributional",
        system_args: dict = None,
        use_cached_inventories: bool = True,
        use_cached_database: bool = True,
        quiet=False,
        keep_uncertainty_data=False,
    ) -> None:

        self.source = source_db
        self.version = check_db_version(source_version)
        self.source_type = source_type
        self.system_model = check_system_model(system_model)
        self.system_model_args = system_args

        if self.source_type == "ecospold":
            self.source_file_path = check_ei_filepath(source_file_path)
        else:
            self.source_file_path = None

        self.scenarios = [check_scenarios(scenario, key) for scenario in scenarios]

        # print some info
        if not quiet:
            print_version()
            warning_about_biogenic_co2()
            info_on_utils_functions()
            hide_messages()

        if additional_inventories:
            self.additional_inventories = check_additional_inventories(
                additional_inventories
            )
        else:
            self.additional_inventories = None

        print("\n////////////////////// EXTRACTING SOURCE DATABASE //////////////////")

        if use_cached_database:
            self.database = self.__find_cached_db(
                source_db, keep_uncertainty_data=keep_uncertainty_data
            )
            print("Done!")
        else:
            self.database = self.__clean_database(
                keep_uncertainty_data=keep_uncertainty_data
            )

        print("\n////////////////// IMPORTING DEFAULT INVENTORIES ///////////////////")
        if use_cached_inventories:
            data = self.__find_cached_inventories(source_db)
            if data is not None:
                self.database.extend(data)
                print("Done!")
        else:
            self.__import_inventories()
            print("No cache of inventories created.")

        if self.additional_inventories:
            data = self.__import_additional_inventories(self.additional_inventories)
            self.database.extend(data)

        self.database = convert_db_to_dataframe(self.database)

        print("\n///////////////////////// EXTRACTING IAM DATA //////////////////////")

        list_data = []
        for scenario in self.scenarios:
            data = IAMDataCollection(
                model=scenario["model"],
                pathway=scenario["pathway"],
                year=scenario["year"],
                filepath_iam_files=scenario["filepath"],
                key=key,
                system_model=self.system_model,
                system_model_args=self.system_model_args,
            )
            list_data.append(data)

            # add additional columns to host pathway-specific data
            scenario_label = create_scenario_label(
                model=scenario["model"],
                pathway=scenario["pathway"],
                year=scenario["year"],
            )

            for col in [c.cons_prod_vol, c.amount, c.efficiency]:
                self.database[(scenario_label, col)] = np.nan

            self.database[(scenario_label, c.comment)] = ""

        self.iam_data = IAMData(list_data)

        print("Done!")

    def __find_cached_db(self, db_name: str, keep_uncertainty_data: bool) -> List[dict]:
        """
        If `use_cached_db` = True, then we look for a cached database.
        If cannot be found, we create a cache for next time.
        :param db_name: database name
        :return: database
        """
        # check that directory exists, otherwise create it
        Path(DIR_CACHED_DB).mkdir(parents=True, exist_ok=True)
        # build file path
        file_name = Path(DIR_CACHED_DB / f"cached_{db_name.strip().lower()}.pickle")

        # check that file path leads to an existing file
        if file_name.exists():
            # return the cached database
            return pickle.load(open(file_name, "rb"))

        # extract the database, pickle it for next time and return it
        print("Cannot find cached database. Will create one now for next time...")
        database = self.__clean_database(keep_uncertainty_data=keep_uncertainty_data)
        pickle.dump(database, open(file_name, "wb"))
        return database

    def __find_cached_inventories(self, db_name: str) -> Union[None, List[dict]]:
        """
        If `use_cached_inventories` = True, then we look for a cached inventories.
        If cannot be found, we create a cache for next time.
        :param db_name: database name
        :return: database
        """
        # check that directory exists, otherwise create it
        Path(DIR_CACHED_DB).mkdir(parents=True, exist_ok=True)
        # build file path
        file_name = Path(
            DIR_CACHED_DB / f"cached_{db_name.strip().lower()}_inventories.pickle"
        )

        # check that file path leads to an existing file
        if file_name.exists():
            # return the cached database
            return pickle.load(open(file_name, "rb"))

        # else, extract the database, pickle it for next time and return it
        print("Cannot find cached inventories. Will create them now for next time...")
        data = self.__import_inventories()
        pickle.dump(data, open(file_name, "wb"))
        print(
            "Data cached. It is advised to restart your workflow at this point.\n"
            "This allows premise to use the cached data instead, which results in\n"
            "a faster workflow."
        )
        return None

    def __clean_database(self, keep_uncertainty_data) -> List[dict]:
        """
        Extracts the ecoinvent database, loads it into a dictionary and does a little bit of housekeeping
        (adds missing locations, reference products, etc.).
        :return:
        """
        return DatabaseCleaner(
            self.source, self.source_type, self.source_file_path
        ).prepare_datasets(keep_uncertainty_data)

    def __import_inventories(self) -> List[dict]:
        """
        This method will trigger the import of a number of pickled inventories
        and merge them into the database dictionary.
        """

        print("Importing default inventories...\n")

        with HiddenPrints():
            # Manual import
            # file path and original ecoinvent version
            data = []
            filepaths = [
                (FILEPATH_OIL_GAS_INVENTORIES, "3.7"),
                (FILEPATH_CARMA_INVENTORIES, "3.5"),
                (FILEPATH_CHP_INVENTORIES, "3.5"),
                (FILEPATH_DAC_INVENTORIES, "3.7"),
                (FILEPATH_BIOGAS_INVENTORIES, "3.6"),
                (FILEPATH_CARBON_FIBER_INVENTORIES, "3.7"),
                (FILEPATH_LITHIUM, "3.8"),
                (FILEPATH_BATTERIES, "3.8"),
                (FILEPATH_PHOTOVOLTAICS, "3.7"),
                (FILEPATH_HYDROGEN_DISTRI_INVENTORIES, "3.7"),
                (FILEPATH_HYDROGEN_INVENTORIES, "3.7"),
                (FILEPATH_HYDROGEN_BIOGAS_INVENTORIES, "3.7"),
                (FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES, "3.7"),
                (FILEPATH_HYDROGEN_NATGAS_INVENTORIES, "3.7"),
                (FILEPATH_HYDROGEN_WOODY_INVENTORIES, "3.7"),
                (FILEPATH_SYNGAS_INVENTORIES, "3.6"),
                (FILEPATH_SYNGAS_FROM_COAL_INVENTORIES, "3.7"),
                (FILEPATH_BIOFUEL_INVENTORIES, "3.7"),
                (FILEPATH_SYNFUEL_INVENTORIES, "3.7"),
                (
                    FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_INVENTORIES,
                    "3.7",
                ),
                (
                    FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_WITH_CCS_INVENTORIES,
                    "3.7",
                ),
                (
                    FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_INVENTORIES,
                    "3.7",
                ),
                (
                    FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_WITH_CCS_INVENTORIES,
                    "3.7",
                ),
                (FILEPATH_GEOTHERMAL_HEAT_INVENTORIES, "3.6"),
                (FILEPATH_METHANOL_FUELS_INVENTORIES, "3.7"),
                (FILEPATH_METHANOL_CEMENT_FUELS_INVENTORIES, "3.7"),
                (FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES, "3.7"),
                (FILEPATH_METHANOL_FROM_COAL_FUELS_WITH_CCS_INVENTORIES, "3.7"),
                (FILEPATH_BIGCC, "3.8"),
            ]
            for filepath in filepaths:
                inventory = DefaultInventory(
                    database=self.database,
                    version_in=filepath[1],
                    version_out=self.version,
                    path=filepath[0],
                    system_model=self.system_model,
                )
                datasets = inventory.merge_inventory()
                data.extend(datasets)

                self.database.extend(datasets)

        print("Done!\n")
        return data

    def __import_additional_inventories(self):

        print("\n//////////////// IMPORTING USER-DEFINED INVENTORIES ////////////////")

        data = []

        for file in self.additional_inventories:

            if file["inventories"] != "":
                additional = AdditionalInventory(
                    database=self.database,
                    version_in=file["ecoinvent version"]
                    if "ecoinvent version" in file
                    else "3.8",
                    version_out=self.version,
                    path=file["inventories"],
                )
                additional.prepare_inventory()

                # if the inventories are to be duplicated
                # to be made specific to each IAM region
                # we flag them
                if "region_duplicate" in file:
                    if file["region_duplicate"]:
                        for dataset in additional.import_db:
                            dataset["duplicate"] = True

                data.extend(additional.merge_inventory())

        return data

    def update_electricity(self):

        print("\n///////////////////////////// ELECTRICITY //////////////////////////")

        electricity = Electricity(
            database=self.database,
            iam_data=self.iam_data,
            scenarios=self.scenarios,
            system_model=self.system_model,
        )

        electricity.create_region_specific_power_plants()
        electricity.update_electricity_efficiency()
        electricity.update_efficiency_of_solar_pv()
        electricity.update_electricity_markets()
        self.database = electricity.database

    def update_all(self):
        """
        Shortcut method to execute all transformation functions.
        """

        self.update_electricity()

    def generate_scenario_factor_file(self, name=None, filepath=None):

        if name is None:
            name = f"super_db_{self.version}_{date.today().isoformat()}"

        if filepath is not None:
            filepath = Path(filepath)
        else:
            filepath = DATA_DIR / "export" / "scenario factor files"

        if not Path(filepath).exists():
            Path(filepath).mkdir(parents=True, exist_ok=True)

        filepath = filepath / f"{name}.xlsx"

        export_scenario_factor_file(self.database, name, self.scenarios, filepath)

    def write_scenario_difference_file(self, filepath=None, name=None) -> None:

        if filepath is not None:
            filepath = Path(filepath)
        else:
            filepath = DATA_DIR / "export" / "scenario diff files"

        if not Path(filepath).exists():
            Path(filepath).mkdir(parents=True, exist_ok=True)

        if name is None:
            name = f"super_db_{self.version}_{date.today().isoformat()}"

        filepath = filepath / f"{name}.xlsx"

        file = generate_scenario_difference_file(
            database=self.database.copy(), db_name=name
        )

        export_scenario_difference_file(file, self.scenarios, filepath)


    def write_superstructure_db_to_brightway(self, name=None, filepath=None):

        """
        Register a super-structure database, according to https://github.com/dgdekoning/brightway-superstructure
        :return: filepath of the "scenarios difference file"
        """

        self.write_scenario_difference_file(filepath, name)

        # FIXME: REVIEW It might be a good idea to start thinking about refactoring all prints into a logging library based approach.
        #        That way we can control the amount of output that is generated via log levels.
        print(f"Exporting {name}...")

        wurst.write_brightway2_database(
            next(convert_df_to_dict(self.database, db_type="super")),
            name,
        )

        print("Done!")

    def write_db_to_brightway(self, name=None):
        """
        Register the new database into an open brightway2 project.
        :param name: to give a (list) of custom name(s) to the database.
        Should either be a string if there's only one database to export.
        Or a list of strings if there are several databases.
        :type name: str
        """

        # FIXME: remember to add the original ecoinvent's comments

        if name:
            if isinstance(name, str):
                name = [name]
            elif isinstance(name, list):
                if not all(isinstance(item, str) for item in name):
                    raise TypeError(
                        "`name` should be a string or a sequence of strings."
                    )
            else:
                raise TypeError("`name` should be a string or a sequence of strings.")
        else:
            name = [
                eidb_label(scenario["model"], scenario["pathway"], scenario["year"])
                for scenario in self.scenarios
            ]

        if len(name) != len(self.scenarios):
            raise ValueError(
                "The number of databases does not match the number of `name` given."
            )

        # we ensure first the absence of duplicate datasets
        # FIXME: some duplicates are legit! Example: electricity losses in markets.
        # check_for_duplicates(self.database)

        print("Write new database(s) to Brightway2.")
        for scen, scenario in enumerate(convert_df_to_dict(self.database)):

            wurst.write_brightway2_database(
                scenario,
                name[scen],
            )

    def generate_scenario_report(
        self,
        filepath: [str, Path] = None,
        name: str = f"scenario_report_{date.today()}.xlsx",
    ):
        """
        Generate a report of the scenarios.
        """

        print("Generate scenario report.")

        if filepath is not None:
            if isinstance(filepath, str):
                filepath = Path(filepath)
        else:
            filepath = Path(DATA_DIR / "export" / "scenario_report")

        if not os.path.exists(filepath):
            os.makedirs(filepath)

        name = Path(name)
        if name.suffix != ".xlsx":
            name = name.with_suffix(".xlsx")

        generate_summary_report(self.iam_data, filepath / name)

        print(f"Report saved under {filepath}.")
