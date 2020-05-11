from . import DATA_DIR, INVENTORY_DIR
from .clean_datasets import DatabaseCleaner
from .data_collection import RemindDataCollection
from .electricity import Electricity
from .inventory_imports import CarmaCCSInventory, \
    BiofuelInventory, \
    HydrogenInventory, \
    BiogasInventory, \
    SynfuelInventory, \
    SyngasInventory, \
    HydrogenCoalInventory, \
    GeothermalInventory, \
    SyngasCoalInventory, \
    SynfuelCoalInventory, \
    LPGInventory
from .cement import Cement
from .steel import Steel

from .export import Export
from .utils import eidb_label
import wurst

FILEPATH_CARMA_INVENTORIES = (INVENTORY_DIR / "lci-Carma-CCS.xlsx")
FILEPATH_BIOFUEL_INVENTORIES = (INVENTORY_DIR / "lci-biofuels.xlsx")
FILEPATH_BIOGAS_INVENTORIES = (INVENTORY_DIR / "lci-biogas.xlsx")
FILEPATH_HYDROGEN_INVENTORIES = (INVENTORY_DIR / "lci-hydrogen.xlsx")
FILEPATH_SYNFUEL_INVENTORIES = (INVENTORY_DIR / "lci-synfuel.xlsx")
FILEPATH_SYNGAS_INVENTORIES = (INVENTORY_DIR / "lci-syngas.xlsx")
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (INVENTORY_DIR / "lci-hydrogen-coal-gasification.xlsx")
FILEPATH_GEOTHERMAL_HEAT_INVENTORIES = (INVENTORY_DIR / "lci-geothermal.xlsx")
FILEPATH_SYNGAS_FROM_COAL_INVENTORIES = (INVENTORY_DIR / "lci-syngas-from-coal.xlsx")
FILEPATH_SYNFUEL_FROM_COAL_INVENTORIES = (INVENTORY_DIR / "lci-synfuel-from-coal.xlsx")
FILEPATH_LPG_INVENTORIES = (INVENTORY_DIR / "lci-lpg-from-methanol.xlsx")


class NewDatabase:
    """
    Class that represents a new wurst inventory database, modified according to IAM data.

    :ivar scenario: name of the REMIND scenario, e.g., 'BAU', 'SCP26'.
    :vartype scenario: str
    :ivar year: year of the REMIND scenario to consider, between 2005 and 2150.
    :vartype year: int
    :ivar source_db: name of the ecoinvent source database
    :vartype source_db: str
    :ivar source_version: version of the ecoinvent source database. Currently works with ecoinvent 3.5 and 3.6.
    :vartype source_version: float
    :ivar filepath_to_remind_files: Filepath to the directory that contains REMIND output files.
    :vartype filepath_to_remind_file: pathlib.Path

    """

    def __init__(self, scenario, year, source_db,
                 source_version=3.5,
                 source_type='brightway',
                 source_file_path=None,
                 filepath_to_remind_files=None):

        if scenario not in [
            "SSP2-Base",
            "SSP2-NDC",
            "SSP2-NPi",
            "SSP2-PkBudg900",
            "SSP2-PkBudg1100",
            "SSP2-PkBudg1300",
        ]:
            raise NameError('The scenario chosen is not any of "SSP2-Base", "SSP2-NDC", "SSP2-NPi", "SSP2-PkBudg900", "SSP2-PkBudg1100", "SSP2-PkBudg1300".')
        else:
            self.scenario = scenario

        self.year = year
        self.source = source_db
        self.version = source_version
        self.source_type = source_type
        self.source_file_path = source_file_path
        self.db = self.clean_database()
        self.import_inventories()
        self.filepath_to_remind_files = (filepath_to_remind_files or DATA_DIR / "remind_output_files")

        self.rdc = RemindDataCollection(self.scenario, self.year, self.filepath_to_remind_files)

    def clean_database(self):
        return DatabaseCleaner(self.source,
                               self.source_type,
                               self.source_file_path
                               ).prepare_datasets()

    def import_inventories(self):
        # Add Carma CCS inventories
        print("Add Carma CCS inventories")
        carma = CarmaCCSInventory(self.db, self.version, FILEPATH_CARMA_INVENTORIES)
        carma.merge_inventory()

        print("Add Biofuel inventories")
        bio = BiofuelInventory(self.db, self.version, FILEPATH_BIOFUEL_INVENTORIES)
        bio.merge_inventory()

        print("Add Hydrogen inventories")
        hydro = HydrogenInventory(self.db, self.version, FILEPATH_HYDROGEN_INVENTORIES)
        hydro.merge_inventory()

        print("Add Biogas inventories")
        biogas = BiogasInventory(self.db, self.version, FILEPATH_BIOGAS_INVENTORIES)
        biogas.merge_inventory()

        print("Add Synthetic gas inventories")
        syngas = SyngasInventory(self.db, self.version, FILEPATH_SYNGAS_INVENTORIES)
        syngas.merge_inventory()

        print("Add Synthetic fuels inventories")
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

        print("Add LPG inventories")
        lpg = LPGInventory(self.db, self.version, FILEPATH_LPG_INVENTORIES)
        lpg.merge_inventory()

    def update_electricity_to_remind_data(self):
        electricity = Electricity(self.db, self.rdc, self.scenario, self.year)
        self.db = electricity.update_electricity_markets()
        self.db = electricity.update_electricity_efficiency()

    def update_cement_to_remind_data(self):
        cement = Cement(self.db, self.rdc, self.year, self.version)
        self.db = cement.add_datasets_to_database()

    def update_steel_to_remind_data(self):
        steel = Steel(self.db, self.rdc, self.year)
        self.db = steel.generate_activities()

    def update_all(self):
        electricity = Electricity(self.db, self.rdc, self.scenario, self.year)
        self.db = electricity.update_electricity_markets()
        self.db = electricity.update_electricity_efficiency()
        cement = Cement(self.db, self.rdc, self.year, self.version)
        self.db = cement.add_datasets_to_database()
        steel = Steel(self.db, self.rdc, self.year)
        self.db = steel.generate_activities()

    def write_db_to_brightway(self):
        print('Write new database to Brightway2.')
        wurst.write_brightway2_database(self.db, eidb_label(self.scenario, self.year))

    def write_db_to_matrices(self):
        print("Write new database to matrix.")
        Export(self.db, self.scenario, self.year).export_db_to_matrices()
