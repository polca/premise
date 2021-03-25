from . import DATA_DIR, INVENTORY_DIR
from .clean_datasets import DatabaseCleaner
from .data_collection import IAMDataCollection
from .electricity import Electricity
from .renewables import SolarPV
from .inventory_imports import (
    CarmaCCSInventory,
    BiofuelInventory,
    DACInventory,
    HydrogenInventory,
    BiogasInventory,
    SynfuelInventory,
    SyngasInventory,
    GeothermalInventory,
    LPGInventory,
    CarculatorInventory,
    TruckInventory,
    VariousVehicles,
    AdditionalInventory
)
from .cement import Cement
from .steel import Steel
from .cars import Cars
from .export import Export
from .utils import eidb_label, add_modified_tags
import wurst
from pathlib import Path
import copy
import os
import contextlib
import pickle


FILEPATH_CARMA_INVENTORIES = INVENTORY_DIR / "lci-Carma-CCS.xlsx"
FILEPATH_CHP_INVENTORIES = INVENTORY_DIR / "lci-combined-heat-power-plant-CCS.xlsx"
FILEPATH_DAC_INVENTORIES = INVENTORY_DIR / "lci-direct-air-capture.xlsx"
FILEPATH_BIOFUEL_INVENTORIES = INVENTORY_DIR / "lci-biofuels.xlsx"
FILEPATH_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-biogas.xlsx"
FILEPATH_HYDROGEN_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-electrolysis.xlsx"
FILEPATH_HYDROGEN_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-smr-atr-biogas.xlsx"
FILEPATH_HYDROGEN_NATGAS_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-smr-atr-natgas.xlsx"
FILEPATH_HYDROGEN_WOODY_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-wood-gasification.xlsx"
)
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-coal-gasification.xlsx"
)
FILEPATH_SYNFUEL_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-electrolysis.xlsx"
)
FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-coal.xlsx"
)
FILEPATH_SYNFUEL_FROM_BIOGAS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-biogas.xlsx"
)
FILEPATH_SYNFUEL_FROM_NAT_GAS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-natural-gas.xlsx"
)
FILEPATH_SYNFUEL_FROM_NAT_GAS_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-natural-gas-CCS.xlsx"
)
FILEPATH_SYNFUEL_FROM_PETROLEUM_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-petroleum.xlsx"
)
FILEPATH_SYNFUEL_FROM_BIOMASS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-biomass.xlsx"
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
FILEPATH_VARIOUS_VEHICLES = INVENTORY_DIR / "lci-various_vehicles.xlsx"
FILE_PATH_INVENTORIES_EI_37 = INVENTORY_DIR / "inventory_data_ei_37.pickle"
FILE_PATH_INVENTORIES_EI_36 = INVENTORY_DIR / "inventory_data_ei_36.pickle"
FILE_PATH_INVENTORIES_EI_35 = INVENTORY_DIR / "inventory_data_ei_35.pickle"

SUPPORTED_EI_VERSIONS = ["3.5", "3.6", "3.7", "3.7.1"]
SUPPORTED_MODELS = ["remind", "image", "static"]
SUPPORTED_PATHWAYS = [
    "SSP2-Base",
    "SSP2-NDC",
    "SSP2-NPi",
    "SSP2-PkBudg900",
    "SSP2-PkBudg1100",
    "SSP2-PkBudg1300",
    "static",
]

LIST_REMIND_REGIONS = [
    'CAZ',
    'CHA',
    'EUR',
    'IND',
    'JPN',
    'LAM',
    'MEA',
    'NEU',
    'OAS',
    'REF',
    'SSA',
    'USA',
    'World'
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
]

LIST_TRANSF_FUNC = [
    "update_electricity",
    "update_cement",
    "update_steel",
    "update_cars",
    "update_trucks",
    "update_solar_PV"
]


def check_ei_filepath(filepath):

    if not Path(filepath).is_dir():
        raise FileNotFoundError(
            f"The directory for ecospold files {filepath} could not be found."
        )
    else:
        return Path(filepath)


def check_model_name(name):
    if name.lower() not in SUPPORTED_MODELS:
        raise ValueError(
            f"Only {SUPPORTED_MODELS} are currently supported, not {name}."
        )
    else:
        return name.lower()


def check_pathway_name(name, filepath, model):
    """ Check the pathway name"""

    if name not in SUPPORTED_PATHWAYS:
        # If the pathway name is not a default one, check that the filepath + pathway name
        # leads to an actual file

        if model.lower() not in name:
            name_check = '_'.join((model.lower(), name))
        else:
            name_check = name

        if (filepath / name_check).with_suffix(".mif").is_file():
            return name
        elif (filepath / name_check).with_suffix(".xls").is_file():
            return name
        else:
            raise ValueError(
                f"Only {SUPPORTED_PATHWAYS} are currently supported, not {name}."
            )
    else:
        if model.lower() not in name:
            name_check = '_'.join((model.lower(), name))
        else:
            name_check = name

        if (filepath / name_check).with_suffix(".mif").is_file():
            return name
        elif (filepath / name_check).with_suffix(".xlsx").is_file():
            return name
        else:
            raise ValueError(
                f"Cannot find the IAM scenario file at this location: {filepath / name_check}."
            )


def check_year(year):
    try:
        year = int(year)
    except ValueError:
        raise ValueError(f"{year} is not a valid year.")

    try:
        assert 2005 <= year < 2100
    except AssertionError:
        raise AssertionError(f"{year} must be comprised between 2005 and 2100.")

    return year


def check_filepath(path):
    if not Path(path).is_dir():
        raise FileNotFoundError(f"The IAM output directory {path} could not be found.")
    else:
        return Path(path)

def check_exclude(list_exc):

    if not isinstance(list_exc, list):
        raise TypeError("`exclude` should be a sequence of strings.")

    if not set(list_exc).issubset(LIST_TRANSF_FUNC):
        raise ValueError(
            "One or several of the transformation that you wish to exclude is not recognized."
        )
    else:
        return list_exc



def check_fleet(fleet, model, vehicle_type):
    if "fleet file" not in fleet:
        print(
            f"No fleet composition file is provided for {vehicle_type}."
            "Fleet average vehicles will be built using default fleet projection."
        )

        fleet["fleet file"] = (
            DATA_DIR / "iam_output_files" / "fleet_files" / model / vehicle_type / "fleet_file.csv"
        )
    else:
        filepath = fleet["fleet file"]
        if not Path(filepath).is_file():
            raise FileNotFoundError(
                f"The fleet file {filepath} could not be found."
            )

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
        raise TypeError("Inventories to import need to be in a sequence of dictionaries like so:"
                  "["
                  "{'filepath': 'a file path', 'ecoinvent version: '3.6'},"
                  " {'filepath': 'a file path', 'ecoinvent version: '3.6'}"
                  "]")

    for inventory in inventories_list:
        if not isinstance(inventory, dict):
            raise TypeError("Inventories to import need to be in a sequence of dictionaries like so:"
                  "["
                  "{'filepath': 'a file path', 'ecoinvent version: '3.6'},"
                  " {'filepath': 'a file path', 'ecoinvent version: '3.6'}"
                  "]")

        if not all(i for i in inventory.keys() if i in ["filepath", "ecoinvent version"]):
            raise TypeError("Both `filepath` and `ecoinvent version` must be present in the list of inventories to import.")

        if not Path(inventory["filepath"]).is_file():
            raise FileNotFoundError(f"Cannot find the inventory file: {inventory['filepath']}.")
        else:
            inventory["filepath"] = Path(inventory["filepath"])

        if inventory["ecoinvent version"] not in ["3.7", "3.7.1"]:
            raise ValueError(
                f"A lot of trouble will be avoided if the additional inventories to import are ecoinvent 3.7 or 3.7.1-compliant."
            )

    return inventories_list

def check_db_version(version):
    version = str(version)
    if version not in SUPPORTED_EI_VERSIONS:
        raise ValueError(
            f"Only {SUPPORTED_EI_VERSIONS} are currently supported, not {version}."
        )
    else:
        return version


def check_scenarios(scenario):

    if not all(name in scenario for name in ["model", "pathway", "year"]):
        raise ValueError(
            f"Missing parameters in {scenario}. Needs to include at least `model`,"
            f"`pathway` and `year`."
        )

    if "filepath" in scenario:
        filepath = scenario["filepath"]
        scenario["filepath"] = check_filepath(filepath)
    else:
        scenario["filepath"] = DATA_DIR / "iam_output_files"

    scenario["model"] = check_model_name(scenario["model"])
    scenario["pathway"] = check_pathway_name(scenario["pathway"], scenario["filepath"], scenario["model"])
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


class NewDatabase:
    """
    Class that represents a new wurst inventory database, modified according to IAM data.

    :ivar source_type: the source of the ecoinvent database. Can be `brigthway` or `ecospold`.
    :vartype source_type: str
    :ivar source_db: name of the ecoinvent source database
    :vartype source_db: str
    :ivar source_version: version of the ecoinvent source database. Currently works with ecoinvent 3.5, 3.6, 3.7, 3.7.1.
    :vartype source_version: str

    """

    def __init__(
        self,
        scenarios,
        source_db=None,
        source_version="3.7.1",
        source_type="brightway",
        source_file_path=None,
        additional_inventories=None,
        direct_import=True
    ):

        self.source = source_db
        self.version = check_db_version(source_version)
        self.source_type = source_type

        if self.source_type == "ecospold":
            self.source_file_path = check_ei_filepath(source_file_path)
        else:
            self.source_file_path = None

        self.scenarios = [check_scenarios(scenario) for scenario in scenarios]

        if additional_inventories:
            self.additional_inventories = check_additional_inventories(additional_inventories)
        else:
            self.additional_inventories = None

        print(
            "\n////////////////////// EXTRACTING SOURCE DATABASE ///////////////////////"
        )
        self.db = self.clean_database()
        print(
            "\n/////////////////// IMPORTING DEFAULT INVENTORIES ////////////////////"
        )
        self.import_inventories(direct_import)

        for scenario in self.scenarios:
            scenario["external data"] = IAMDataCollection(
                model=scenario["model"],
                pathway=scenario["pathway"],
                year=scenario["year"],
                filepath_iam_files=scenario["filepath"],
            )
            scenario["database"] = copy.deepcopy(self.db)

    def clean_database(self):
        """
        Extracts the ecoinvent database, loads it into a dictionary and does a little bit of housekeeping
        (adds missing locations, reference products, etc.).
        :return:
        """
        return DatabaseCleaner(
            self.source, self.source_type, self.source_file_path
        ).prepare_datasets()

    def import_inventories(self, direct_import):
        """
        This method will trigger the import of a number of inventories
        and merge them into the database dictionary.
        """

        print("Importing necessary inventories...\n")

        if direct_import:

            # we unpickle inventories here
            # and append them directly to the end of the database

            if self.version in ["3.7", "3.7.1"]:
                fp = FILE_PATH_INVENTORIES_EI_37
            elif self.version == "3.6":
                fp = FILE_PATH_INVENTORIES_EI_36
            else:
                fp = FILE_PATH_INVENTORIES_EI_35

            with open(fp, 'rb') as handle:
                data = pickle.load(handle)
                self.db.extend(data)

        else:
            # Manual import
            for file in (FILEPATH_CARMA_INVENTORIES, FILEPATH_CHP_INVENTORIES):
                carma = CarmaCCSInventory(self.db, self.version, file)
                carma.merge_inventory()

            dac = DACInventory(self.db, self.version, FILEPATH_DAC_INVENTORIES)
            dac.merge_inventory()

            biogas = BiogasInventory(self.db, self.version, FILEPATH_BIOGAS_INVENTORIES)
            biogas.merge_inventory()

            for file in (
                FILEPATH_HYDROGEN_INVENTORIES,
                FILEPATH_HYDROGEN_BIOGAS_INVENTORIES,
                FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES,
                FILEPATH_HYDROGEN_NATGAS_INVENTORIES,
                FILEPATH_HYDROGEN_WOODY_INVENTORIES,
            ):

                hydro = HydrogenInventory(self.db, self.version, file)
                hydro.merge_inventory()

            for file in (FILEPATH_SYNGAS_INVENTORIES, FILEPATH_SYNGAS_FROM_COAL_INVENTORIES):
                syngas = SyngasInventory(self.db, self.version, file)
                syngas.merge_inventory()

            bio = BiofuelInventory(self.db, self.version, FILEPATH_BIOFUEL_INVENTORIES)
            bio.merge_inventory()


            for file in (
                FILEPATH_SYNFUEL_INVENTORIES,
                FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES,
                FILEPATH_SYNFUEL_FROM_BIOGAS_INVENTORIES,
                FILEPATH_SYNFUEL_FROM_BIOMASS_INVENTORIES,
                FILEPATH_SYNFUEL_FROM_BIOMASS_CCS_INVENTORIES,
                FILEPATH_SYNFUEL_FROM_NAT_GAS_INVENTORIES,
                FILEPATH_SYNFUEL_FROM_NAT_GAS_CCS_INVENTORIES,
                FILEPATH_SYNFUEL_FROM_PETROLEUM_INVENTORIES,
            ):
                synfuel = SynfuelInventory(self.db, self.version, file)
                synfuel.merge_inventory()

            geo_heat = GeothermalInventory(
                self.db, self.version, FILEPATH_GEOTHERMAL_HEAT_INVENTORIES
            )
            geo_heat.merge_inventory()

            for file in (
                FILEPATH_METHANOL_FUELS_INVENTORIES,
                FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES,
                FILEPATH_METHANOL_FROM_BIOMASS_FUELS_INVENTORIES,
                FILEPATH_METHANOL_FROM_BIOGAS_FUELS_INVENTORIES,
                FILEPATH_METHANOL_FROM_NATGAS_FUELS_INVENTORIES,
            ):

                lpg = LPGInventory(self.db, self.version, file)
                lpg.merge_inventory()

            various_veh = VariousVehicles(self.db, self.version, FILEPATH_VARIOUS_VEHICLES)
            various_veh.merge_inventory()

        print("Done!\n")

        if self.additional_inventories:

            print(
                "\n/////////////////// IMPORTING USER-DEFINED INVENTORIES ////////////////////"
            )

            for file in self.additional_inventories:
                additional = AdditionalInventory(self.db, self.version, file["filepath"])
                additional.merge_inventory()

            print("Done!\n")



    def update_electricity(self):

        print("\n/////////////////// ELECTRICITY ////////////////////")

        for scenario in self.scenarios:
            if "exclude" not in scenario or "update_electricity" not in scenario["exclude"]:
                electricity = Electricity(
                    db=scenario["database"],
                    iam_data=scenario["external data"],
                    model=scenario["model"],
                    pathway=scenario["pathway"],
                    year=scenario["year"],
                )
                scenario["database"] = electricity.update_electricity_markets()
                scenario["database"] = electricity.update_electricity_efficiency()

    def update_cement(self):
        print("\n/////////////////// CEMENT ////////////////////")

        for scenario in self.scenarios:
            if "exclude" not in scenario or "update_cement" not in scenario["exclude"]:

                cement = Cement(
                    db=scenario["database"],
                    model=scenario["model"],
                    scenario=scenario["pathway"],
                    iam_data=scenario["external data"],
                    year=scenario["year"],
                    version=self.version,
                )

                scenario["database"] = cement.add_datasets_to_database()

    def update_steel(self):
        print("\n/////////////////// STEEL ////////////////////")

        if (
            len(
                [
                    v
                    for v in self.scenarios[0]["external data"].data.variables.values
                    if "steel" in v.lower() and "production" in v.lower()
                ]
            )
            > 0
        ):

            for scenario in self.scenarios:
                if "exclude" not in scenario or "update_steel" not in scenario["exclude"]:

                    steel = Steel(
                        db=scenario["database"],
                        model=scenario["model"],
                        iam_data=scenario["external data"],
                        year=scenario["year"],
                    )
                    scenario["database"] = steel.generate_activities()
        else:
            print(
                "The IAM pathway chosen does not contain any data related to the steel sector.\n"
                "The creation of IAM region-specific steel production activities and markets will be skipped."
                "But we will nevertheless adjust hot pollutant emissions and the expected share of recycled steel."
            )
            for scenario in self.scenarios:
                if "exclude" not in scenario or "update_steel" not in scenario["exclude"]:

                    steel = Steel(
                        db=scenario["database"],
                        model=scenario["model"],
                        iam_data=scenario["external data"],
                        year=scenario["year"],
                    )
                    scenario["database"] = steel.generate_activities(industry_module_present=False)

    def update_cars(self):
        print("\n/////////////////// PASSENGER CARS ////////////////////")

        for scenario in self.scenarios:
            if "exclude" not in scenario or "update_cars" not in scenario["exclude"]:

                if scenario["passenger_cars"]:

                    # Import `carculator` inventories if wanted
                    cars = CarculatorInventory(
                        database=scenario["database"],
                        version=self.version,
                        path=scenario["filepath"],
                        fleet_file=scenario["passenger_cars"]["fleet file"],
                        model=scenario["model"],
                        pathway=scenario["pathway"],
                        year=scenario["year"],
                        regions=scenario["passenger_cars"]["regions"],
                        filters=scenario["passenger_cars"]["filters"],
                    )
                    scenario["database"] = cars.merge_inventory()

                    crs = Cars(
                        db=scenario["database"],
                        iam_data=scenario["external data"],
                        pathway=scenario["pathway"],
                        year=scenario["year"],
                        model=scenario["model"],
                    )
                    scenario["database"] = crs.update_cars()



    def update_trucks(self):

        print("\n/////////////////// MEDIUM AND HEAVY DUTY TRUCKS ////////////////////")

        for scenario in self.scenarios:
            if "exclude" not in scenario or "update_trucks" not in scenario["exclude"]:
                if scenario["trucks"]:

                    # Import `carculator_truck` inventories if wanted

                    trucks = TruckInventory(
                        database=scenario["database"],
                        version=self.version,
                        path=scenario["filepath"],
                        fleet_file=scenario["trucks"]["fleet file"],
                        model=scenario["model"],
                        pathway=scenario["pathway"],
                        year=scenario["year"],
                        regions=scenario["trucks"]["regions"],
                        filters=scenario["trucks"]["filters"],
                       )
                    scenario["database"] = trucks.merge_inventory()


    def update_solar_PV(self):
        print("\n/////////////////// SOLAR PV ////////////////////")

        for scenario in self.scenarios:
            if "exclude" not in scenario or "update_solar_PV" not in scenario["exclude"]:
                solar_PV = SolarPV(db=scenario["database"], year=scenario["year"])
                print("Update efficiency of solar PVs.\n")
                scenario["database"] = solar_PV.update_efficiency_of_solar_PV()

    def update_all(self):
        """
        Shortcut method to execute all transformation functions.
        """

        self.update_cars()
        self.update_trucks()
        self.update_electricity()
        self.update_solar_PV()
        self.update_cement()
        self.update_steel()


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
                    raise TypeError("`name` should be a string or a sequence of strings.")
            else:
                raise TypeError("`name` should be a string or a sequence of strings.")
        else:
            name = [eidb_label(s["model"], s["pathway"], s["year"]) for s in self.scenarios]

        if len(name) != len(self.scenarios):
            raise ValueError("The number of databases does not match the number of `name` given.")

        print("Write new database(s) to Brightway2.")
        for s, scenario in enumerate(self.scenarios):
            wurst.write_brightway2_database(
                scenario["database"],
                name[s],
            )

    def write_db_to_matrices(self, filepath=None):
        """

        Exports the new database as a sparse matrix representation in csv files.


        :param filepath: path provided by the user to store the exported matrices
        :type filepath: str

        """
        print("Write new database(s) to matrix.")
        for scenario in self.scenarios:
            Export(
                scenario["database"],
                scenario["model"],
                scenario["pathway"],
                scenario["year"],
                filepath,
            ).export_db_to_matrices()

    def write_db_to_simapro(self, filepath=None):
        """
        Exports database as a CSV file to be imported in Simapro 9.x

        :param filepath: path provided by the user to store the exported import file
        :type filepath: str

        """

        print("Write Simapro import file(s).")
        for scenario in self.scenarios:
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
                    raise TypeError("`name` should be a string or a sequence of strings.")
            else:
                raise TypeError("`name` should be a string or a sequence of strings.")
        else:
            name = [eidb_label(s["model"], s["pathway"], s["year"]) for s in self.scenarios]

        if len(name) != len(self.scenarios):
            raise ValueError("The number of databases does not match the number of `name` given.")

        print('Write new database to Brightway2.5')
        # We first need to check for differences between the source database
        # and the new ones
        # We add a `modified` label to any new activity or any new or modified exchange
        self.scenarios = add_modified_tags(self.db, self.scenarios)
        for s, scenario in enumerate(self.scenarios):
            wurst.write_brightway25_database(scenario["database"],
                                             name[s],
                                             self.source
                                             )