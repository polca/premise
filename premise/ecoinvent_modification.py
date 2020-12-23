from . import DATA_DIR, INVENTORY_DIR
from .clean_datasets import DatabaseCleaner
from .data_collection import IAMDataCollection
from .electricity import Electricity
from .inventory_imports import CarmaCCSInventory, \
    BiofuelInventory, \
    DACInventory, \
    HydrogenInventory, \
    BiogasInventory, \
    SynfuelInventory, \
    SyngasInventory, \
    HydrogenCoalInventory, \
    HydrogenWoodyInventory, \
    GeothermalInventory, \
    SyngasCoalInventory, \
    SynfuelCoalInventory, \
    LPGInventory, \
    CarculatorInventory, \
    TruckInventory
from .cement import Cement
from .steel import Steel
from .cars import Cars
from .export import Export
from .utils import eidb_label
import wurst
import wurst.searching as ws
from pathlib import Path


FILEPATH_CARMA_INVENTORIES = (INVENTORY_DIR / "lci-Carma-CCS.xls")
FILEPATH_CHP_INVENTORIES = (INVENTORY_DIR / "lci-combined-heat-power-plant-CCS.xls")
FILEPATH_DAC_INVENTORIES = (INVENTORY_DIR / "lci-direct-air-capture.xls")
FILEPATH_BIOFUEL_INVENTORIES = (INVENTORY_DIR / "lci-biofuels.xls")
FILEPATH_BIOGAS_INVENTORIES = (INVENTORY_DIR / "lci-biogas.xls")
FILEPATH_HYDROGEN_INVENTORIES = (INVENTORY_DIR / "lci-hydrogen-electrolysis.xls")
FILEPATH_HYDROGEN_BIOGAS_INVENTORIES = (INVENTORY_DIR / "lci-hydrogen-smr-atr-biogas.xls")
FILEPATH_HYDROGEN_NATGAS_INVENTORIES = (INVENTORY_DIR / "lci-hydrogen-smr-atr-natgas.xls")
FILEPATH_HYDROGEN_WOODY_INVENTORIES = (INVENTORY_DIR / "lci-hydrogen-wood-gasification.xls")
FILEPATH_SYNFUEL_INVENTORIES = (INVENTORY_DIR / "lci-synfuels-from-FT.xls")
FILEPATH_SYNGAS_INVENTORIES = (INVENTORY_DIR / "lci-syngas.xls")
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (INVENTORY_DIR / "lci-hydrogen-coal-gasification.xls")
FILEPATH_GEOTHERMAL_HEAT_INVENTORIES = (INVENTORY_DIR / "lci-geothermal.xls")
FILEPATH_SYNGAS_FROM_COAL_INVENTORIES = (INVENTORY_DIR / "lci-syngas-from-coal.xls")
FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES = (INVENTORY_DIR / "lci-synfuel-from-coal.xls")
FILEPATH_METHANOL_FUELS_INVENTORIES = (INVENTORY_DIR / "lci-synfuels-from-methanol.xls")

SUPPORTED_EI_VERSIONS = [3.5, 3.6, 3.7]
SUPPORTED_SCENARIOS = [
    "SSP2-Base",
    "SSP2-NDC",
    "SSP2-NPi",
    "SSP2-PkBudg900",
    "SSP2-PkBudg1100",
    "SSP2-PkBudg1300",
]

class NewDatabase:
    """
    Class that represents a new wurst inventory database, modified according to IAM data.

    :ivar model: name of teh IAM model. Can be `remind` or `image`.
    :vartype model: str
    :ivar scenario: name of the IAM scenario, e.g., 'SSP2-Base', 'SSP2/NDC', etc..
    :vartype scenario: str
    :ivar year: year of the IAM scenario to consider, between 2005 and 2150.
    :vartype year: int
    :ivar source_type: the source of the ecoinvent database. Can be `brigthway` or `ecospold`.
    :vartype source_type: str
    :ivar source_db: name of the ecoinvent source database
    :vartype source_db: str
    :ivar source_version: version of the ecoinvent source database. Currently works with ecoinvent 3.5, 3.6 and 3.7.
    :vartype source_version: float
    :ivar filepath_to_iam_files: Filepath to the directory that contains IAM output files.
    :vartype filepath_to_iam_files: str
    :ivar add_passenger_cars: Whether or not to include inventories of future passenger cars. If
    a dictionary is passed, it will look inside to collect arguments such as a specified fleet file,
    or a list of regions to generate inventories for. If a boolean is passed, inventories are
    generated for all regions.
    :vartype add_passenger_cars: bool or dict
    :ivar add_trucks: Whether or not to include inventories of future medium and heavy duty trucks. If
    a dictionary is passed, it will look inside to collect arguments such as a specified fleet file,
    or a list of regions to generate inventories for. If a boolean is passed, inventories are
    generated for all regions.
    :vartype add_passenger_cars: bool or dict

    """

    def __init__(self,
                 year,
                 source_db=None,
                 model="remind",
                 scenario=None,
                 source_version=3.7,
                 source_type='brightway',
                 source_file_path=None,
                 filepath_to_iam_files=None,
                 add_passenger_cars=None,
                 add_trucks=None):

        if model.lower() not in ["remind", "image"]:
            raise ValueError("Only REMIND and IMAGE model scenarios are currently supported.")

        if filepath_to_iam_files is None:
            if model.lower() == "remind":
                if scenario not in [
                    "SSP2-Base",
                    "SSP2-NDC",
                    "SSP2-NPi",
                    "SSP2-PkBudg900",
                    "SSP2-PkBudg1100",
                    "SSP2-PkBudg1300",
                ]:
                    print(('Warning: The scenario chosen is not any of '
                           '"SSP2-Base", "SSP2-NDC", "SSP2-NPi", "SSP2-PkBudg900", '
                           '"SSP2-PkBudg1100", "SSP2-PkBudg1300".'))

            if model.lower() == "image":
                if scenario not in [
                    "SSP2-Base",
                ]:
                    print(('Warning: The scenario chosen is not any of '
                           '"SSP2-Base".'))

        # If we produce fleet average vehicles,
        # fleet compositions and electricity mixes must be provided
        if add_passenger_cars:
            if isinstance(add_passenger_cars, dict):
                if "fleet file" not in add_passenger_cars:
                    print("No fleet composition file is provided, hence fleet average vehicles inventories will not be produced.")
                if "source file" not in add_passenger_cars:
                    add_passenger_cars["source file"] = (filepath_to_iam_files or DATA_DIR / "iam_output_files")
            elif isinstance(add_passenger_cars, bool):

                # IAM output file extension differs between REMIND and IMAGE
                add_passenger_cars = {"region": "all",
                                        "source file":(filepath_to_iam_files or DATA_DIR / "iam_output_files")
                                      }

        # If we produce fleet average trucks,
        # fleet compositions and electricity mixes must be provided
        if add_trucks:
            if isinstance(add_trucks, dict):
                if "fleet file" not in add_trucks:
                    print(
                        "No fleet composition file is provided, hence fleet average truck inventories will not be produced.")
                if "source file" not in add_trucks:
                    add_trucks["source file"] = (filepath_to_iam_files or DATA_DIR / "iam_output_files")
            elif isinstance(add_trucks, bool):

                # IAM output file extension differs between REMIND and IMAGE
                add_trucks = {"region": "all",
                                      "source file": (
                                                  filepath_to_iam_files or DATA_DIR / "iam_output_files")
                                      }

        if scenario is None:
            raise ValueError("Missing scenario name.")
        else:
            self.scenario = scenario

        try:
            source_version = float(source_version)
        except ValueError:
            raise ValueError(f"Provided ecoinvent version ({source_version}) is not valid.\n")

        if float(source_version) not in SUPPORTED_EI_VERSIONS:
            raise ValueError(
                (
                    f"Provided ecoinvent version ({source_version}) is not supported.\n"
                    f"Please use one of the following: {SUPPORTED_EI_VERSIONS}"
                )
            )

        self.year = year
        self.source = source_db
        self.model = model.lower()
        self.version = source_version
        self.source_type = source_type
        self.source_file_path = source_file_path
        self.filepath_to_iam_files = Path(filepath_to_iam_files or DATA_DIR / "iam_output_files")
        self.add_passenger_cars = add_passenger_cars
        self.add_trucks = add_trucks

        if not self.filepath_to_iam_files.is_dir():
            raise FileNotFoundError(
                "The IAM output directory could not be found."
            )
        self.rdc = IAMDataCollection(self.model, self.scenario, self.year, self.filepath_to_iam_files)

        print("\n////////////////////// EXTRACTING SOURCE DATABASE ///////////////////////")
        self.db = self.clean_database()
        print("\n/////////////////// IMPORTING ADDITIONAL INVENTORIES ////////////////////")
        self.import_inventories()


    def clean_database(self):
        """
        Extracts the ecoinvent database, loads it into a dictionary and does a little bit of housekeeping
        (adds missing locations, reference products, etc.).
        :return:
        """
        return DatabaseCleaner(
            self.source,
            self.source_type,
            self.source_file_path
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
        carma = CarmaCCSInventory(self.db, self.version, FILEPATH_CARMA_INVENTORIES)
        carma.merge_inventory()

        carma = CarmaCCSInventory(self.db, self.version, FILEPATH_CHP_INVENTORIES)
        carma.merge_inventory()

        dac = DACInventory(self.db, self.version, FILEPATH_DAC_INVENTORIES)
        dac.merge_inventory()

        biogas = BiogasInventory(self.db, self.version, FILEPATH_BIOGAS_INVENTORIES)
        biogas.merge_inventory()

        hydro = HydrogenInventory(self.db, self.version, FILEPATH_HYDROGEN_INVENTORIES)
        hydro.merge_inventory()

        hydro = HydrogenInventory(self.db, self.version, FILEPATH_HYDROGEN_NATGAS_INVENTORIES)
        hydro.merge_inventory()

        hydro = HydrogenInventory(self.db, self.version, FILEPATH_HYDROGEN_BIOGAS_INVENTORIES)
        hydro.merge_inventory()

        hydro = HydrogenWoodyInventory(self.db, self.version, FILEPATH_HYDROGEN_WOODY_INVENTORIES)
        hydro.merge_inventory()

        syngas = SyngasInventory(self.db, self.version, FILEPATH_SYNGAS_INVENTORIES)
        syngas.merge_inventory()

        bio = BiofuelInventory(self.db, self.version, FILEPATH_BIOFUEL_INVENTORIES)
        bio.merge_inventory()

        synfuel = SynfuelInventory(self.db, self.version, FILEPATH_SYNFUEL_INVENTORIES)
        synfuel.merge_inventory()

        hydrogen_coal = HydrogenCoalInventory(self.db, self.version, FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES)
        hydrogen_coal.merge_inventory()

        geo_heat = GeothermalInventory(self.db, self.version, FILEPATH_GEOTHERMAL_HEAT_INVENTORIES)
        geo_heat.merge_inventory()

        syngas_coal = SyngasCoalInventory(self.db, self.version, FILEPATH_SYNGAS_FROM_COAL_INVENTORIES)
        syngas_coal.merge_inventory()

        synfuel_coal = SynfuelCoalInventory(self.db, self.version, FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES)
        synfuel_coal.merge_inventory()

        lpg = LPGInventory(self.db, self.version, FILEPATH_METHANOL_FUELS_INVENTORIES)
        lpg.merge_inventory()

        # Import `carculator` inventories if wanted
        if self.add_passenger_cars:
            cars = CarculatorInventory(database=self.db,
                                       version=self.version,
                                       path=Path(""),
                                       model=self.model,
                                       scenario=self.scenario,
                                       year=self.year,
                                       regions=self.rdc.regions,
                                       vehicles=self.add_passenger_cars
                                       )
            cars.merge_inventory()

        # Import `carculator_truck` inventories if wanted
        if self.add_trucks:
            trucks = TruckInventory(database=self.db,
                                       version=self.version,
                                       path=Path(""),
                                       model=self.model,
                                       scenario=self.scenario,
                                       year=self.year,
                                       regions=self.rdc.regions,
                                       vehicles=self.add_passenger_cars
                                       )
            trucks.merge_inventory()

    def update_electricity_to_iam_data(self):
        print("\n/////////////////// ELECTRICITY ////////////////////")
        electricity = Electricity(self.db, self.rdc, self.model, self.scenario, self.year)
        self.db = electricity.update_electricity_markets()
        self.db = electricity.update_electricity_efficiency()

    def update_cement_to_iam_data(self):
        print("\n/////////////////// CEMENT ////////////////////")
        cement = Cement(db=self.db,
                        model=self.model,
                        scenario=self.scenario,
                        rmd=self.rdc,
                        year=self.year,
                        version=self.version
                        )
        self.db = cement.add_datasets_to_database()

    def update_steel_to_iam_data(self):
        print("\n/////////////////// STEEL ////////////////////")
        if len([v for v in self.rdc.data.variables.values
                if "steel" in v.lower() and "production" in v.lower()]) > 0:
            steel = Steel(db=self.db, model=self.model, rmd=self.rdc, year=self.year)
            self.db = steel.generate_activities()
        else:
            print("The IAM scenario chosen does not contain any data related to the steel sector.\n"
                  "Transformations related to the steel sector will be skipped.")

    def update_cars(self):
        print("\n/////////////////// VEHICLES ////////////////////")
        try:
            next(ws.get_many(
                self.db,
                ws.equals("name", "market group for electricity, low voltage")))
            crs = Cars(self.db, self.rdc, self.scenario, self.year, self.model)
            crs.update_cars()
        except StopIteration:
            raise(("No updated electricity markets found. Please update "
                   "electricity markets before updating upstream fuel "
                   "inventories for electricity powered vehicles"))

    def update_all(self):
        """
        Shortcut method to execute all transformation functions.
        """
        self.update_electricity_to_iam_data()
        self.update_cement_to_iam_data()
        self.update_steel_to_iam_data()
        if self.add_passenger_cars:
            self.update_cars()

    def write_db_to_brightway(self):
        """
        Register the new database into an open brightway2 project.
        """
        print('Write new database to Brightway2.')
        wurst.write_brightway2_database(self.db, eidb_label(self.model, self.scenario, self.year))

    def write_db_to_matrices(self, filepath=None):
        """
        :param filepath: path provided by the user to store the exported matrices
        :type filepath: str
        Exports the new database as a sparse matrix representation in csv files.
        """
        print("Write new database to matrix.")
        Export(self.db, self.model, self.scenario, self.year, filepath).export_db_to_matrices()
