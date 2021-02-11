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
)
from .cement import Cement
from .steel import Steel
from .cars import Cars
from .export import Export
from .utils import eidb_label
import wurst
import wurst.searching as ws
from pathlib import Path
import copy


FILEPATH_CARMA_INVENTORIES = INVENTORY_DIR / "lci-Carma-CCS.xls"
FILEPATH_CHP_INVENTORIES = INVENTORY_DIR / "lci-combined-heat-power-plant-CCS.xls"
FILEPATH_DAC_INVENTORIES = INVENTORY_DIR / "lci-direct-air-capture.xls"
FILEPATH_BIOFUEL_INVENTORIES = INVENTORY_DIR / "lci-biofuels.xls"
FILEPATH_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-biogas.xls"
FILEPATH_HYDROGEN_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-electrolysis.xls"
FILEPATH_HYDROGEN_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-smr-atr-biogas.xls"
FILEPATH_HYDROGEN_NATGAS_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-smr-atr-natgas.xls"
FILEPATH_HYDROGEN_WOODY_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-wood-gasification.xls"
)
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-coal-gasification.xls"
)
FILEPATH_SYNFUEL_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-electrolysis.xls"
)
FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-coal.xls"
)
FILEPATH_SYNFUEL_FROM_BIOGAS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-biogas.xls"
)
FILEPATH_SYNFUEL_FROM_NAT_GAS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-natural-gas.xls"
)
FILEPATH_SYNFUEL_FROM_NAT_GAS_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-natural-gas-CCS.xls"
)
FILEPATH_SYNFUEL_FROM_PETROLEUM_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-petroleum.xls"
)
FILEPATH_SYNFUEL_FROM_BIOMASS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-biomass.xls"
)
FILEPATH_SYNFUEL_FROM_BIOMASS_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-biomass-CCS.xls"
)
FILEPATH_SYNGAS_INVENTORIES = INVENTORY_DIR / "lci-syngas.xls"
FILEPATH_SYNGAS_FROM_COAL_INVENTORIES = INVENTORY_DIR / "lci-syngas-from-coal.xls"
FILEPATH_GEOTHERMAL_HEAT_INVENTORIES = INVENTORY_DIR / "lci-geothermal.xls"
FILEPATH_METHANOL_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-electrolysis.xls"
)
FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-coal.xls"
)
FILEPATH_METHANOL_FROM_BIOMASS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biomass.xls"
)
FILEPATH_METHANOL_FROM_BIOGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biogas.xls"
)
FILEPATH_METHANOL_FROM_NATGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-natural-gas.xls"
)

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
    "LAM",
    "CAZ",
    "EUR",
    "CHA",
    "SSA",
    "IND",
    "OAS",
    "JPN",
    "OAS",
    "REF",
    "MEA",
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


def check_pathway_name(name):
    if name not in SUPPORTED_PATHWAYS:
        raise ValueError(
            f"Only {SUPPORTED_PATHWAYS} are currently supported, not {name}."
        )
    else:
        return name


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


def check_fleet(fleet, model, vehicle_type):
    if "fleet file" not in fleet:
        print(
            "No fleet composition file is provided, hence fleet average vehicles will be built using "
            "fleet projection from the pathway provided."
        )
        fleet["fleet file"] = (
            DATA_DIR / "iam_output_files" / "fleet files" / model / vehicle_type / "fleet_file_pc.csv"
        )
    else:
        filepath = fleet["fleet file"]
        if not Path(filepath).is_dir():
            raise FileNotFoundError(
                f"The fleet file directory {filepath} could not be found."
            )

    if "region" in fleet:
        if isinstance(fleet["region"], str):
            fleet["region"] = list(fleet["region"])


        if model == "remind":
            if not set(fleet["region"]).issubset(LIST_REMIND_REGIONS):
                raise ValueError(
                    "One or several regions specified for the fleet "
                    "of passenger cars is invalid."
                )

        if model == "image":
            if not set(fleet["region"]).issubset(LIST_IMAGE_REGIONS):
                raise ValueError(
                    "One or several regions specified for the fleet "
                    "of passenger cars is invalid."
                )
    else:
        if model == "remind":
            fleet["region"] = LIST_REMIND_REGIONS
        if model == "image":
            fleet["region"] = LIST_IMAGE_REGIONS

    if "filters" not in fleet:
        fleet["filters"] = None
    else:
        if isinstance(fleet["fleet"], str):
            fleet["filters"] = list(fleet["filters"])


    return fleet


def check_scenarios(scenario):

    if not all(name in scenario for name in ["model", "pathway", "year"]):
        raise ValueError(
            f"Missing parameters in {scenario}. Needs to include at least `model`,"
            f"`pathway` and `year`."
        )

    scenario["model"] = check_model_name(scenario["model"])
    scenario["pathway"] = check_pathway_name(scenario["pathway"])
    scenario["year"] = check_year(scenario["year"])

    if "filepath" in scenario:
        filepath = scenario["filepath"]
        scenario["filepath"] = check_filepath(filepath)
    else:
        scenario["filepath"] = DATA_DIR / "iam_output_files"

    if "passenger cars" in scenario:
        scenario["passenger cars"] = check_fleet(
            scenario["passenger cars"], scenario["model"], "passenger cars"
        )
    else:
        scenario["passenger cars"] = False

    if "trucks" in scenario:
        scenario["trucks"] = check_fleet(
            scenario["trucks"], scenario["model"], "trucks"
        )
    else:
        scenario["trucks"] = False

    return scenario


def check_db_version(version):
    version = str(version)
    if version not in SUPPORTED_EI_VERSIONS:
        raise ValueError(
            f"Only {SUPPORTED_EI_VERSIONS} are currently supported, not {version}."
        )
    else:
        return version


class NewDatabase:
    """
    Class that represents a new wurst inventory database, modified according to IAM data.

    :ivar model: name of teh IAM model. Can be `remind` or `image`.
    :vartype model: str
    :ivar pathway: name of the IAM pathway, e.g., 'SSP2-Base', 'SSP2/NDC', etc..
    :vartype pathway: str
    :ivar year: year of the IAM pathway to consider, between 2005 and 2150.
    :vartype year: int
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
    ):

        self.source = source_db
        self.version = check_db_version(source_version)
        self.source_type = source_type

        if self.source_type == "ecospold":
            self.source_file_path = check_ei_filepath(source_file_path)
        else:
            self.source_file_path = None

        self.scenarios = [check_scenarios(scenario) for scenario in scenarios]

        print(
            "\n////////////////////// EXTRACTING SOURCE DATABASE ///////////////////////"
        )
        self.db = self.clean_database()
        print(
            "\n/////////////////// IMPORTING ADDITIONAL INVENTORIES ////////////////////"
        )
        self.import_inventories()

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

    def import_inventories(self):
        """
        This method will trigger the import of a number of inventories
        and merge them into the database dictionary.
        If `add_passenger_cars` and `add_trucks` have been set to `True`,
        or if they have been passed as dictionaries, corresponding inventories will be
        imported as well, otherwise, they will not.
        """

        # Add Carma CCS inventories
        for f in (FILEPATH_CARMA_INVENTORIES, FILEPATH_CHP_INVENTORIES):
            carma = CarmaCCSInventory(self.db, self.version, f)
            carma.merge_inventory()

        dac = DACInventory(self.db, self.version, FILEPATH_DAC_INVENTORIES)
        dac.merge_inventory()

        biogas = BiogasInventory(self.db, self.version, FILEPATH_BIOGAS_INVENTORIES)
        biogas.merge_inventory()

        for f in (
            FILEPATH_HYDROGEN_INVENTORIES,
            FILEPATH_HYDROGEN_BIOGAS_INVENTORIES,
            FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES,
            FILEPATH_HYDROGEN_NATGAS_INVENTORIES,
            FILEPATH_HYDROGEN_WOODY_INVENTORIES,
        ):

            hydro = HydrogenInventory(self.db, self.version, f)
            hydro.merge_inventory()

        for f in (FILEPATH_SYNGAS_INVENTORIES, FILEPATH_SYNGAS_FROM_COAL_INVENTORIES):
            syngas = SyngasInventory(self.db, self.version, f)
            syngas.merge_inventory()

        bio = BiofuelInventory(self.db, self.version, FILEPATH_BIOFUEL_INVENTORIES)
        bio.merge_inventory()

        for f in (
            FILEPATH_SYNFUEL_INVENTORIES,
            FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES,
            FILEPATH_SYNFUEL_FROM_BIOGAS_INVENTORIES,
            FILEPATH_SYNFUEL_FROM_BIOMASS_INVENTORIES,
            FILEPATH_SYNFUEL_FROM_BIOMASS_CCS_INVENTORIES,
            FILEPATH_SYNFUEL_FROM_NAT_GAS_INVENTORIES,
            FILEPATH_SYNFUEL_FROM_NAT_GAS_CCS_INVENTORIES,
            FILEPATH_SYNFUEL_FROM_PETROLEUM_INVENTORIES,
        ):
            synfuel = SynfuelInventory(self.db, self.version, f)
            synfuel.merge_inventory()

        geo_heat = GeothermalInventory(
            self.db, self.version, FILEPATH_GEOTHERMAL_HEAT_INVENTORIES
        )
        geo_heat.merge_inventory()

        for f in (
            FILEPATH_METHANOL_FUELS_INVENTORIES,
            FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES,
            FILEPATH_METHANOL_FROM_BIOMASS_FUELS_INVENTORIES,
            FILEPATH_METHANOL_FROM_BIOGAS_FUELS_INVENTORIES,
            FILEPATH_METHANOL_FROM_NATGAS_FUELS_INVENTORIES,
        ):

            lpg = LPGInventory(self.db, self.version, f)
            lpg.merge_inventory()

    def update_electricity_to_iam_data(self):
        print("\n/////////////////// ELECTRICITY ////////////////////")

        for scenario in self.scenarios:

            electricity = Electricity(
                db=scenario["database"],
                iam_data=scenario["external data"],
                model=scenario["model"],
                pathway=scenario["pathway"],
                year=scenario["year"],
            )
            scenario["database"] = electricity.update_electricity_markets()
            scenario["database"] = electricity.update_electricity_efficiency()

    def update_cement_to_iam_data(self):
        print("\n/////////////////// CEMENT ////////////////////")

        for scenario in self.scenarios:

            cement = Cement(
                db=scenario["database"],
                model=scenario["model"],
                scenario=scenario["pathway"],
                iam_data=scenario["external data"],
                year=scenario["year"],
                version=self.version,
            )

            scenario["database"] = cement.add_datasets_to_database()

    def update_steel_to_iam_data(self):

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
            print("\n/////////////////// STEEL ////////////////////")
            for scenario in self.scenarios:

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
                "Transformations related to the steel sector will be skipped."
            )

    def update_cars(self):

        try:
            next(
                ws.get_many(
                    self.db,
                    ws.equals("name", "market group for electricity, low voltage"),
                )
            )

            for scenario in self.scenarios:
                if scenario["passenger cars"]:
                    print("\n/////////////////// PASSENGER CARS ////////////////////")


                    # Import `carculator` inventories if wanted
                    cars = CarculatorInventory(
                        database=scenario["database"],
                        version=self.version,
                        path=scenario["filepath"],
                        fleet_file=scenario["passenger cars"]["fleet file"],
                        model=scenario["model"],
                        pathway=scenario["pathway"],
                        year=scenario["year"],
                        regions=scenario["passenger cars"]["region"],
                        filters=scenario["passenger cars"]["filters"],
                    )
                    cars.merge_inventory()

                    crs = Cars(
                        db=scenario["database"],
                        iam_data=scenario["external data"],
                        pathway=scenario["pathway"],
                        year=scenario["year"],
                        model=scenario["model"],
                    )
                    scenario["database"] = crs.update_cars()

        except StopIteration:
            raise (
                (
                    "No updated electricity markets found. Please update "
                    "electricity markets before updating upstream fuel "
                    "inventories for electricity powered vehicles"
                )
            )

    def update_trucks(self):

        try:
            next(
                ws.get_many(
                    self.db,
                    ws.equals("name", "market group for electricity, low voltage"),
                )
            )

            for scenario in self.scenarios:
                if scenario["passenger cars"]:
                    print("\n/////////////////// MEDIUM AND HEAVY DUTY TRUCKS ////////////////////")

                    # Import `carculator_truck` inventories if wanted

                    trucks = TruckInventory(
                        database=scenario["database"],
                        version=self.version,
                        path=scenario["filepath"],
                        fleet_file=scenario["passenger cars"]["fleet file"],
                        model=scenario["model"],
                        pathway=scenario["pathway"],
                        year=scenario["year"],
                        regions=scenario["trucks"]["region"],
                        filters=scenario["trucks"]["filters"],
                       )
                    trucks.merge_inventory()

        except StopIteration:
            raise (
                (
                    "No updated electricity markets found. Please update "
                    "electricity markets before updating upstream fuel "
                    "inventories for electricity powered vehicles"
                )
            )




    def update_solar_PV(self):
        print("\n/////////////////// SOLAR PV ////////////////////")

        for scenario in self.scenarios:
            solar_PV = SolarPV(db=scenario["database"], year=scenario["year"])
            print("Update efficiency of solar PVs.\n")
            scenario["database"] = solar_PV.update_efficiency_of_solar_PV()

    def update_all(self):
        """
        Shortcut method to execute all transformation functions.
        """
        self.update_electricity_to_iam_data()
        self.update_solar_PV()
        self.update_cement_to_iam_data()
        self.update_steel_to_iam_data()
        self.update_cars()

    def write_db_to_brightway(self):
        """
        Register the new database into an open brightway2 project.
        """
        print("Write new database(s) to Brightway2.")
        for scenario in self.scenarios:
            wurst.write_brightway2_database(
                scenario["database"],
                eidb_label(scenario["model"], scenario["pathway"], scenario["year"]),
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
