from . import DATA_DIR, INVENTORY_DIR
from .clean_datasets import DatabaseCleaner
from .data_collection import IAMDataCollection
from .electricity import Electricity
from .inventory_imports import CarmaCCSInventory, \
    BiofuelInventory, \
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
                 source_db,
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
                if add_passenger_cars == True:

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
                if add_trucks == True:

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

        self.db = self.clean_database()
        self.import_inventories()


    def clean_database(self):
        return DatabaseCleaner(
            self.source,
            self.source_type,
            self.source_file_path
        ).prepare_datasets()

    def import_inventories(self):

        # Add Carma CCS inventories
        print("Add inventories for conventional power plants with CCS")
        carma = CarmaCCSInventory(self.db, self.version, FILEPATH_CARMA_INVENTORIES)
        carma.merge_inventory()

        print("Add inventories for CHP power plants with CCS")
        carma = CarmaCCSInventory(self.db, self.version, FILEPATH_CHP_INVENTORIES)
        carma.merge_inventory()

        print("Add Biogas inventories")
        biogas = BiogasInventory(self.db, self.version, FILEPATH_BIOGAS_INVENTORIES)
        biogas.merge_inventory()

        print("Add Electrolysis Hydrogen inventories")
        hydro = HydrogenInventory(self.db, self.version, FILEPATH_HYDROGEN_INVENTORIES)
        hydro.merge_inventory()

        print("Add Natural Gas SMR and ATR Hydrogen inventories")
        hydro = HydrogenInventory(self.db, self.version, FILEPATH_HYDROGEN_NATGAS_INVENTORIES)
        hydro.merge_inventory()

        print("Add Biogas SMR and ATR Hydrogen inventories")
        hydro = HydrogenInventory(self.db, self.version, FILEPATH_HYDROGEN_BIOGAS_INVENTORIES)
        hydro.merge_inventory()

        print("Add Woody biomass gasification Hydrogen inventories")
        hydro = HydrogenWoodyInventory(self.db, self.version, FILEPATH_HYDROGEN_WOODY_INVENTORIES)
        hydro.merge_inventory()

        print("Add Synthetic gas inventories")
        syngas = SyngasInventory(self.db, self.version, FILEPATH_SYNGAS_INVENTORIES)
        syngas.merge_inventory()

        print("Add Biofuel inventories")
        bio = BiofuelInventory(self.db, self.version, FILEPATH_BIOFUEL_INVENTORIES)
        bio.merge_inventory()

        print("Add Fischer-Tropsh-based synthetic fuels inventories")
        synfuel = SynfuelInventory(self.db, self.version, FILEPATH_SYNFUEL_INVENTORIES)
        synfuel.merge_inventory()

        print("Add Hydrogen from coal gasification inventories")
        hydrogen_coal = HydrogenCoalInventory(self.db, self.version, FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES)
        hydrogen_coal.merge_inventory()

        print("Add Geothermal heat inventories")
        geo_heat = GeothermalInventory(self.db, self.version, FILEPATH_GEOTHERMAL_HEAT_INVENTORIES)
        geo_heat.merge_inventory()

        print("Add Syngas from coal gasification inventories")
        syngas_coal = SyngasCoalInventory(self.db, self.version, FILEPATH_SYNGAS_FROM_COAL_INVENTORIES)
        syngas_coal.merge_inventory()

        print("Add Synfuel from coal gasification inventories")
        synfuel_coal = SynfuelCoalInventory(self.db, self.version, FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES)
        synfuel_coal.merge_inventory()

        print("Add methanol-based synthetic fuel inventories")
        lpg = LPGInventory(self.db, self.version, FILEPATH_METHANOL_FUELS_INVENTORIES)
        lpg.merge_inventory()

        # Import `carculator` inventories if wanted
        if self.add_passenger_cars:
            print("Add passenger cars inventories")
            cars = CarculatorInventory(self.db, self.model, self.scenario, self.year, self.version, self.rdc.regions,
                                       self.add_passenger_cars)
            cars.merge_inventory()

        # Import `carculator_truck` inventories if wanted
        if self.add_trucks:
            print("Add medium and heavy duty truck inventories")
            trucks = TruckInventory(self.db, self.model, self.scenario, self.year, self.version,
                                       self.rdc.regions,
                                       self.add_trucks)
            trucks.merge_inventory()

    def update_electricity_to_iam_data(self):
        electricity = Electricity(self.db, self.rdc, self.model, self.scenario, self.year)
        self.db = electricity.update_electricity_markets()
        self.db = electricity.update_electricity_efficiency()

    def update_cement_to_iam_data(self):
        if len([v for v in self.rdc.data.variables.values
                if "cement" in v.lower() and "production" in v.lower()])>0:
            cement = Cement(self.db, self.rdc, self.year, self.version)
            self.db = cement.add_datasets_to_database()
        else:
            print("The IAM scenario chosen does not contain any data related to the cement sector.\n"
                  "Transformations related to the cement sector will be skipped.")

    def update_steel_to_iam_data(self):
        if len([v for v in self.rdc.data.variables.values
                if "steel" in v.lower() and "production" in v.lower()]) > 0:
            steel = Steel(self.db, self.rdc, self.year)
            self.db = steel.generate_activities()
        else:
            print("The IAM scenario chosen does not contain any data related to the steel sector.\n"
                  "Transformations related to the steel sector will be skipped.")

    def update_cars(self):
        try:
            next(ws.get_many(
                self.db,
                ws.equals("name", "market group for electricity, low voltage")))
            crs = Cars(self.db, self.rdc, self.scenario, self.year, self.model)
            crs.update_cars()
        except StopIteration as e:
            print(("No updated electricity markets found. Please update "
                   "electricity markets before updating upstream fuel "
                   "inventories for electricity powered vehicles"))

    def update_all(self):
        self.update_electricity_to_iam_data()
        self.update_cement_to_iam_data()
        self.update_steel_to_iam_data()
        if self.add_passenger_cars:
            self.update_cars()

    def write_db_to_brightway(self):
        print('Write new database to Brightway2.')
        wurst.write_brightway2_database(self.db, eidb_label(self.model, self.scenario, self.year))

    def write_db_to_matrices(self):
        print("Write new database to matrix.")
        Export(self.db, self.scenario, self.year).export_db_to_matrices()
