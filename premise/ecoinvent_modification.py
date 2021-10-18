import sys, os, copy, pickle
from pathlib import Path
from datetime import date
import wurst
from prettytable import PrettyTable
from . import DATA_DIR, INVENTORY_DIR
from .clean_datasets import DatabaseCleaner
from .data_collection import IAMDataCollection
from .electricity import Electricity
from .renewables import SolarPV
from .inventory_imports import (
    DefaultInventory,
    CarculatorInventory,
    TruckInventory,
    VariousVehicles,
    AdditionalInventory,
    PassengerCars,
    Trucks,
)
from .cement import Cement
from .steel import Steel
from .fuels import Fuels
from .export import Export
from .utils import eidb_label, add_modified_tags, build_superstructure_db


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
FILEPATH_METHANOL_FROM_BIOMASS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biomass.xlsx"
)
FILEPATH_METHANOL_FROM_BIOGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biogas.xlsx"
)
FILEPATH_METHANOL_FROM_NATGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-natural-gas.xlsx"
)
FILEPATH_TWO_WHEELERS = INVENTORY_DIR / "lci-two_wheelers.xlsx"
FILE_PATH_INVENTORIES_EI_38 = INVENTORY_DIR / "inventory_data_ei_38.pickle"
FILE_PATH_INVENTORIES_EI_37 = INVENTORY_DIR / "inventory_data_ei_37.pickle"
FILE_PATH_INVENTORIES_EI_36 = INVENTORY_DIR / "inventory_data_ei_36.pickle"
FILE_PATH_INVENTORIES_EI_35 = INVENTORY_DIR / "inventory_data_ei_35.pickle"

SUPPORTED_EI_VERSIONS = ["3.5", "3.6", "3.7", "3.7.1", "3.8"]
SUPPORTED_MODELS = ["remind", "image"]
SUPPORTED_PATHWAYS = [
    "SSP2-Base",
    "SSP2-NDC",
    "SSP2-NPi",
    "SSP2-PkBudg900",
    "SSP2-PkBudg1100",
    "SSP2-PkBudg1300",
    "SSP2-PkBudg900_Elec",
    "SSP2-PkBudg1100_Elec",
    "SSP2-PkBudg1300_Elec",
    "SSP2-RCP26",
    "SSP2-RCP19",
    "static",
]

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
    "ESC",
    "ECE",
    "DEU",
    "ESW",
    "FRA",
    "UKI",
    "ENC",
    "EWN",
    "ECS",
    "NEN",
    "NES",
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

LIST_TRANSF_FUNC = [
    "update_electricity",
    "update_cement",
    "update_steel",
    "update_cars",
    "update_two_wheelers",
    "update_trucks",
    "update_solar_pv",
    "update_fuels",
]

# Disable printing
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore printing
def enablePrint():
    sys.stdout = sys.__stdout__

def check_for_duplicates(database):
    """ Check for the absence of duplicates before export """

    db_names = [
        (x["name"].lower(), x["reference product"].lower(), x["location"])
        for x in database
    ]

    if len(db_names) == len(set(db_names)):
        return database


    print("One or multiple duplicates detected. Removing them...")
    seen = set()
    return [
        x
        for x in database
        if (x["name"].lower(), x["reference product"].lower(), x["location"])
           not in seen
           and not seen.add(
            (x["name"].lower(), x["reference product"].lower(), x["location"])
        )
    ]


def check_ei_filepath(filepath):
    """ Check for the existence of the file path."""

    if not Path(filepath).is_dir():
        raise FileNotFoundError(
            f"The directory for ecospold files {filepath} could not be found."
        )
    return Path(filepath)


def check_model_name(name):
    """ Check for the validity of the IAM model name."""
    if name.lower() not in SUPPORTED_MODELS:
        raise ValueError(
            f"Only {SUPPORTED_MODELS} are currently supported, not {name}."
        )
    return name.lower()


def check_pathway_name(name, filepath, model):
    """ Check the pathway name"""

    if name not in SUPPORTED_PATHWAYS:
        # If the pathway name is not a default one, check that the filepath + pathway name
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
    else:
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


def check_year(year):
    """ Check for the validity of the year passed. """
    try:
        year = int(year)
    except ValueError as err:
        raise Exception(f"{year} is not a valid year.") from err

    try:
        assert 2005 <= year < 2100
    except AssertionError as err:
        raise Exception(f"{year} must be comprised between 2005 and 2100.") from err

    return year


def check_filepath(path):
    if not Path(path).is_dir():
        raise FileNotFoundError(f"The IAM output directory {path} could not be found.")
    return Path(path)


def check_exclude(list_exc):

    if not isinstance(list_exc, list):
        raise TypeError("`exclude` should be a sequence of strings.")

    if not set(list_exc).issubset(LIST_TRANSF_FUNC):
        raise ValueError(
            "One or several of the transformation that you wish to exclude is not recognized."
        )
    return list_exc


def check_fleet(fleet, model, vehicle_type):
    if "fleet file" not in fleet:
        print(
            f"No fleet composition file is provided for {vehicle_type}.\n"
            "Fleet average vehicles will be built using default fleet projection."
        )

        fleet["fleet file"] = (
            DATA_DIR
            / "iam_output_files"
            / "fleet_files"
            / model
            / vehicle_type
            / "fleet_file.csv"
        )
    else:
        filepath = fleet["fleet file"]
        if not Path(filepath).is_file():
            raise FileNotFoundError(f"The fleet file {filepath} could not be found.")

    if "regions" in fleet:
        if isinstance(fleet["regions"], str):
            fleet["regions"] = list(fleet["regions"])

        if model == "remind":
            if not set(fleet["regions"]).issubset(LIST_REMIND_REGIONS):
                raise ValueError(
                    "One or several regions specified for the fleet "
                    "of passenger_cars is invalid."
                )

        if model == "image":
            if not set(fleet["regions"]).issubset(LIST_IMAGE_REGIONS):
                raise ValueError(
                    "One or several regions specified for the fleet "
                    "of passenger_cars is invalid."
                )
    else:
        if model == "remind":
            fleet["regions"] = LIST_REMIND_REGIONS
        if model == "image":
            fleet["regions"] = LIST_IMAGE_REGIONS

    if "filters" not in fleet:
        fleet["filters"] = None
    else:
        if isinstance(fleet["fleet"], str):
            fleet["filters"] = list(fleet["filters"])

    return fleet


def check_additional_inventories(inventories_list):

    if not isinstance(inventories_list, list):
        raise TypeError(
            "Inventories to import need to be in a sequence of dictionaries like so:"
            "["
            "{'filepath': 'a file path', 'ecoinvent version: '3.6'},"
            " {'filepath': 'a file path', 'ecoinvent version: '3.6'}"
            "]"
        )

    for inventory in inventories_list:
        if not isinstance(inventory, dict):
            raise TypeError(
                "Inventories to import need to be in a sequence of dictionaries like so:"
                "["
                "{'filepath': 'a file path', 'ecoinvent version: '3.6'},"
                " {'filepath': 'a file path', 'ecoinvent version: '3.6'}"
                "]"
            )

        if not all(
            i for i in inventory.keys() if i in ["filepath", "ecoinvent version"]
        ):
            raise TypeError(
                "Both `filepath` and `ecoinvent version` must be present in the list of inventories to import."
            )

        if not Path(inventory["filepath"]).is_file():
            raise FileNotFoundError(
                f"Cannot find the inventory file: {inventory['filepath']}."
            )
        inventory["filepath"] = Path(inventory["filepath"])

        if inventory["ecoinvent version"] not in ["3.7", "3.7.1", "3.8"]:
            raise ValueError(
                "A lot of trouble will be avoided if the additional inventories to import are ecoinvent 3.7 or 3.8-compliant."
            )

    return inventories_list


def check_db_version(version):
    version = str(version)
    if version not in SUPPORTED_EI_VERSIONS:
        raise ValueError(
            f"Only {SUPPORTED_EI_VERSIONS} are currently supported, not {version}."
        )
    return version


def check_scenarios(scenario, key):

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

    if "passenger_cars" in scenario:
        scenario["passenger_cars"] = check_fleet(
            scenario["passenger_cars"], scenario["model"], "passenger_cars"
        )
    else:
        scenario["passenger_cars"] = False

    if "trucks" in scenario:
        scenario["trucks"] = check_fleet(
            scenario["trucks"], scenario["model"], "trucks"
        )
    else:
        scenario["trucks"] = False

    return scenario


def check_system_model(system_model):

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


def check_time_horizon(th):

    if th is None:
        print(
            "`time_horizon`, used to identify marginal suppliers, is not specified. "
            "It is therefore set to 20 years."
        )
        th = 20

    try:
        int(th)
    except ValueError as err:
        raise Exception(
            "`time_horizon` must be an integer or float with a value between 5 and 50 years."
        ) from err

    if th < 5 or th > 50:
        raise ValueError(
            "`time_horizon` must be an integer or float with a value between 5 and 50 years."
        )

    return int(th)

def warming_about_biogenic_co2():
    """
    Prints a simple warning about characterizing biogenic CO2 flows.
    :return: Does not return anything.
    """
    t = PrettyTable(["Warning"])
    t.add_row(["Because some of the scenarios can yield LCI databases\n"
               "containing net negative emission technologies (NET),\n"
               "it is advised to account for biogenic CO2 flows when calculating\n"
               "Global Warming potential indicators.\n"
               "`premise_gwp` provides characterization factors for such flows.\n\n"
               "Install it via\n"
               "pip install premise_gwp\n"
               "or\n"
               "conda install -c romainsacchi premise_gwp\n\n"
               "Within in your bw2 project:\n"
               "from premise_gwp import add_premise_gwp\n"
                "add_premise_gwp()"])
    # align text to the left
    t.align = "l"
    print(t)


class HiddenPrints:
    """
    From https://stackoverflow.com/questions/8391411/how-to-block-calls-to-print
    """
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

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
        scenarios,
        source_version="3.8",
        source_type="brightway",
        key=None,
        source_db=None,
        source_file_path=None,
        additional_inventories=None,
        direct_import=True,
        system_model="attributional",
        time_horison=None,
    ):

        self.source = source_db
        self.version = check_db_version(source_version)
        self.source_type = source_type
        self.system_model = check_system_model(system_model)
        self.time_horizon = (
            check_time_horizon(time_horison)
            if system_model == "consequential"
            else None
        )

        if self.source_type == "ecospold":
            self.source_file_path = check_ei_filepath(source_file_path)
        else:
            self.source_file_path = None

        self.scenarios = [check_scenarios(scenario, key) for scenario in scenarios]

        # warning about biogenic CO2
        warming_about_biogenic_co2()

        if additional_inventories:
            self.additional_inventories = check_additional_inventories(
                additional_inventories
            )
        else:
            self.additional_inventories = None

        print(
            "\n////////////////////// EXTRACTING SOURCE DATABASE ///////////////////////"
        )
        self.database = self.__clean_database()
        print(
            "\n/////////////////// IMPORTING DEFAULT INVENTORIES ////////////////////"
        )
        self.__import_inventories(direct_import)

        for scenario in self.scenarios:
            scenario["external data"] = IAMDataCollection(
                model=scenario["model"],
                pathway=scenario["pathway"],
                year=scenario["year"],
                filepath_iam_files=scenario["filepath"],
                key=key,
                system_model=self.system_model,
                time_horizon=self.time_horizon,
            )
            scenario["database"] = copy.deepcopy(self.database)

    def __clean_database(self):
        """
        Extracts the ecoinvent database, loads it into a dictionary and does a little bit of housekeeping
        (adds missing locations, reference products, etc.).
        :return:
        """
        return DatabaseCleaner(
            self.source, self.source_type, self.source_file_path
        ).prepare_datasets()

    def __import_inventories(self, direct_import):
        """
        This method will trigger the import of a number of pickled inventories
        and merge them into the database dictionary.
        """

        print("Importing necessary inventories...\n")

        if direct_import:

            # we unpickle inventories here
            # and append them directly to the end of the database
            if self.version == "3.8":
                filepath = FILE_PATH_INVENTORIES_EI_38
            elif self.version in ["3.7", "3.7.1"]:
                self.version = "3.7"
                filepath = FILE_PATH_INVENTORIES_EI_37
            elif self.version == "3.6":
                filepath = FILE_PATH_INVENTORIES_EI_36
            else:
                filepath = FILE_PATH_INVENTORIES_EI_35

            with open(filepath, "rb") as handle:
                data = check_for_duplicates(pickle.load(handle))
                self.database.extend(data)

        else:
            with HiddenPrints():
                # Manual import
                # file path and original ecoinvent version
                filepaths = [
                    (FILEPATH_OIL_GAS_INVENTORIES, "3.7"),
                    (FILEPATH_CARMA_INVENTORIES, "3.5"),
                    (FILEPATH_CHP_INVENTORIES, "3.5"),
                    (FILEPATH_DAC_INVENTORIES, "3.7"),
                    (FILEPATH_BIOGAS_INVENTORIES, "3.6"),
                    (FILEPATH_CARBON_FIBER_INVENTORIES, "3.7"),
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
                    (FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_INVENTORIES, "3.7"),
                    (
                        FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_WITH_CCS_INVENTORIES,
                        "3.7",
                    ),
                    (FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_INVENTORIES, "3.7"),
                    (FILEPATH_GEOTHERMAL_HEAT_INVENTORIES, "3.6"),
                    (FILEPATH_METHANOL_FUELS_INVENTORIES, "3.7"),
                    (FILEPATH_METHANOL_CEMENT_FUELS_INVENTORIES, "3.7"),
                    (FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES, "3.7"),
                ]
                for filepath in filepaths:
                    inventory = DefaultInventory(
                        database=self.database,
                        version_in=filepath[1],
                        version_out=self.version,
                        path=filepath[0],
                    )
                    inventory.merge_inventory()

        print("Done!\n")

        if self.additional_inventories:

            print(
                "\n/////////////////// IMPORTING USER-DEFINED INVENTORIES ////////////////////"
            )

            for file in self.additional_inventories:
                additional = AdditionalInventory(
                    database=self.database,
                    version_in=file["ecoinvent version"],
                    version_out=self.version,
                    path=file["filepath"],
                )
                additional.prepare_inventory()
                additional.merge_inventory()

            print("Done!\n")

    def update_electricity(self):

        print("\n/////////////////// ELECTRICITY ////////////////////")

        for scenario in self.scenarios:
            if (
                "exclude" not in scenario
                or "update_electricity" not in scenario["exclude"]
            ):
                electricity = Electricity(
                    database=scenario["database"],
                    iam_data=scenario["external data"],
                    model=scenario["model"],
                    pathway=scenario["pathway"],
                    year=scenario["year"],
                )
                scenario["database"] = electricity.update_electricity_markets()
                scenario["database"] = electricity.update_electricity_efficiency()

    def update_fuels(self):
        print("\n/////////////////// FUELS ////////////////////")

        for scenario in self.scenarios:

            if "exclude" not in scenario or "update_fuels" not in scenario["exclude"]:

                fuels = Fuels(
                    db=scenario["database"],
                    original_db=self.database,
                    model=scenario["model"],
                    pathway=scenario["pathway"],
                    iam_data=scenario["external data"],
                    year=scenario["year"],
                    regions=scenario["fuels"]["regions"]
                    if "fuels" in scenario
                    else None,
                )

                scenario["database"] = fuels.generate_fuel_markets()

    def update_cement(self):
        print("\n/////////////////// CEMENT ////////////////////")

        for scenario in self.scenarios:
            if (
                    "exclude" not in scenario
                    or "update_cement" not in scenario["exclude"]
                ):

                cement = Cement(
                    database=scenario["database"],
                    model=scenario["model"],
                    pathway=scenario["pathway"],
                    iam_data=scenario["external data"],
                    year=scenario["year"],
                    version=self.version,
                )

                scenario["database"] = cement.add_datasets_to_database()

    def update_steel(self):
        print("\n/////////////////// STEEL ////////////////////")

        for scenario in self.scenarios:

            if (
                "exclude" not in scenario
                or "update_steel" not in scenario["exclude"]
            ):

                steel = Steel(
                    db=scenario["database"],
                    model=scenario["model"],
                    iam_data=scenario["external data"],
                    year=scenario["year"],
                )
                scenario["database"] = steel.generate_activities()

    def update_cars(self):
        print("\n/////////////////// PASSENGER CARS ////////////////////")

        for scenario in self.scenarios:
            if "exclude" not in scenario or "update_cars" not in scenario["exclude"]:

                if scenario["passenger_cars"]:
                    # Load fleet-specific inventories
                    # Import `carculator` inventories if wanted
                    cars = CarculatorInventory(
                        database=scenario["database"],
                        version=self.version,
                        fleet_file=scenario["passenger_cars"]["fleet file"],
                        model=scenario["model"],
                        year=scenario["year"],
                        regions=scenario["passenger_cars"]["regions"],
                        filters=scenario["passenger_cars"]["filters"],
                        iam_data=scenario["external data"].data,
                    )

                else:
                    # Load fleet default inventories
                    cars = PassengerCars(
                        database=scenario["database"],
                        version_in="3.7",
                        version_out=self.version,
                        model=scenario["model"],
                        year=scenario["year"],
                        regions=scenario["external data"]
                        .data.coords["region"]
                        .values.tolist(),
                        iam_data=scenario["external data"].data,
                    )

                scenario["database"] = cars.merge_inventory()

    def update_two_wheelers(self):
        print("\n/////////////////// TWO-WHEELERS ////////////////////")

        for scenario in self.scenarios:
            if (
                "exclude" not in scenario
                or "update_two_wheelers" not in scenario["exclude"]
            ):

                various_veh = VariousVehicles(
                    database=scenario["database"],
                    version_in="3.7",
                    version_out=self.version,
                    path=FILEPATH_TWO_WHEELERS,
                    year=scenario["year"],
                    regions=scenario["external data"]
                    .data.coords["region"]
                    .values.tolist(),
                    model=scenario["model"],
                )
                scenario["database"] = various_veh.merge_inventory()

    def update_trucks(self):

        print("\n/////////////////// MEDIUM AND HEAVY DUTY TRUCKS ////////////////////")

        for scenario in self.scenarios:
            if "exclude" not in scenario or "update_trucks" not in scenario["exclude"]:
                if scenario["trucks"]:

                    # Load fleet-specific inventories
                    # Import `carculator_truck` inventories if wanted

                    trucks = TruckInventory(
                        database=scenario["database"],
                        version_in="3.7",
                        version_out=self.version,
                        fleet_file=scenario["trucks"]["fleet file"],
                        model=scenario["model"],
                        year=scenario["year"],
                        regions=scenario["trucks"]["regions"],
                        filters=scenario["trucks"]["filters"],
                        iam_data=scenario["external data"].data,
                    )

                else:
                    # Load default trucks inventories
                    trucks = Trucks(
                        database=scenario["database"],
                        version_in="3.7",
                        version_out=self.version,
                        model=scenario["model"],
                        year=scenario["year"],
                        regions=scenario["external data"]
                        .data.coords["region"]
                        .values.tolist(),
                        iam_data=scenario["external data"].data,
                    )

                scenario["database"] = trucks.merge_inventory()

    def update_solar_pv(self):
        print("\n/////////////////// SOLAR PV ////////////////////")

        for scenario in self.scenarios:
            if (
                "exclude" not in scenario
                or "update_solar_pv" not in scenario["exclude"]
            ):
                solar_pv = SolarPV(db=scenario["database"], year=scenario["year"])
                print("Update efficiency of solar PVs.\n")
                scenario["database"] = solar_pv.update_efficiency_of_solar_pv()

    def update_all(self):
        """
        Shortcut method to execute all transformation functions.
        """

        self.update_two_wheelers()
        self.update_cars()
        self.update_trucks()
        self.update_electricity()
        self.update_solar_pv()
        self.update_cement()
        self.update_steel()
        self.update_fuels()

    def write_superstructure_db_to_brightway(
        self, name=f"super_db_{date.today()}", filepath=None
    ):
        """
        Register a super-structure database, according to https://github.com/dgdekoning/brightway-superstructure
        :return: filepath of the "scenarios difference file"
        """

        self.database = build_superstructure_db(
            self.database, self.scenarios, db_name=name, fp=filepath
        )

        print("Done!")

        wurst.write_brightway2_database(
            self.database, name,
        )

    def write_db_to_brightway(self, name=None):
        """
        Register the new database into an open brightway2 project.
        :param name: to give a (list) of custom name(s) to the database.
        Should either be a string if there's only one database to export.
        Or a list of strings if there are several databases.
        :type name: str
        """

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
                eidb_label(scenario["model"], scenario["pathway"], scenario["year"]) for scenario in self.scenarios
            ]

        if len(name) != len(self.scenarios):
            raise ValueError(
                "The number of databases does not match the number of `name` given."
            )

        print("Write new database(s) to Brightway2.")
        for scen, scenario in enumerate(self.scenarios):

            # we ensure first the absence of duplicate datasets
            scenario["database"] = check_for_duplicates(scenario["database"])

            wurst.write_brightway2_database(
                scenario["database"], name[scen],
            )

    def write_db_to_matrices(self, filepath=None):
        """

        Exports the new database as a sparse matrix representation in csv files.

        :param filepath: path provided by the user to store the exported matrices.
        If it is a string, the path is used as main directory from which
        "iam model" / "pathway" / "year" subdirectories will be created.
        If it is a sequence of strings, each string becomes the directory
        under which the set of matrices is saved. If `filepath` is not provided,
        "iam model" / "pathway" / "year" subdirectories are created under
        "premise" / "data" / "export".
        :type filepath: str or list

        """

        if filepath is not None:
            if isinstance(filepath, str):
                filepath = [
                    (Path(filepath) / s["model"] / s["pathway"] / str(s["year"]))
                    for s in self.scenarios
                ]
            elif isinstance(filepath, list):
                filepath = [Path(f) for f in filepath]
            else:
                raise TypeError(
                    f"Expected a string or a sequence of strings for `filepath`, not {type(filepath)}."
                )
        else:
            filepath = [
                (DATA_DIR / "export" / s["model"] / s["pathway"] / str(s["year"]))
                for s in self.scenarios
            ]

        print("Write new database(s) to matrix.")
        for scen, scenario in enumerate(self.scenarios):

            # we ensure first the absence of duplicate datasets
            scenario["database"] = check_for_duplicates(scenario["database"])

            Export(
                scenario["database"],
                scenario["model"],
                scenario["pathway"],
                scenario["year"],
                filepath[scen],
            ).export_db_to_matrices()

    def write_db_to_simapro(self, filepath=None):
        """
        Exports database as a CSV file to be imported in Simapro 9.x

        :param filepath: path provided by the user to store the exported import file
        :type filepath: str

        """

        filepath = filepath or Path(DATA_DIR / "export" / "simapro")

        if not os.path.exists(filepath):
            os.makedirs(filepath)

        print("Write Simapro import file(s).")
        for scenario in self.scenarios:

            # we ensure first the absence of duplicate datasets
            scenario["database"] = check_for_duplicates(scenario["database"])

            Export(
                scenario["database"],
                scenario["model"],
                scenario["pathway"],
                scenario["year"],
                filepath,
            ).export_db_to_simapro()

    def write_db_to_brightway25(self, name=None):
        """
        Register the new database into the current brightway2.5 project.
        """

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
                eidb_label(s["model"], s["pathway"], s["year"]) for s in self.scenarios
            ]

        if len(name) != len(self.scenarios):
            raise ValueError(
                "The number of databases does not match the number of `name` given."
            )

        print("Write new database to Brightway2.5")
        # We first need to check for differences between the source database
        # and the new ones
        # We add a `modified` label to any new activity or any new or modified exchange
        self.scenarios = add_modified_tags(self.database, self.scenarios)
        for scen, scenario in enumerate(self.scenarios):

            # we ensure first the absence of duplicate datasets
            scenario["database"] = check_for_duplicates(scenario["database"])

            wurst.write_brightway25_database(scenario["database"], name[scen], self.source)