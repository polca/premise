from . import DATA_DIR
from .clean_datasets import DatabaseCleaner
from .data_collection import RemindDataCollection
from .electricity import Electricity
from .inventory_imports import CarmaCCSInventory, BiofuelInventory, HydrogenInventory, BiogasInventory, SynfuelInventory, SyngasInventory
from .export import Export
from .utils import eidb_label
import wurst


FILEPATH_CARMA_INVENTORIES = (DATA_DIR / "lci-Carma-CCS.xlsx")
FILEPATH_BIOFUEL_INVENTORIES = (DATA_DIR / "lci-biofuels.xlsx")
FILEPATH_BIOGAS_INVENTORIES = (DATA_DIR / "lci-biogas.xlsx")
FILEPATH_HYDROGEN_INVENTORIES = (DATA_DIR / "lci-hydrogen.xlsx")
FILEPATH_SYNFUEL_INVENTORIES = (DATA_DIR / "lci-synfuel.xlsx")
FILEPATH_SYNGAS_INVENTORIES = (DATA_DIR / "lci-syngas.xlsx")


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
                 source_file_path = None,
                 filepath_to_remind_files=None):
        self.scenario = scenario
        self.year = year
        self.source = source_db
        self.version = source_version
        self.source_type = source_type
        self.source_file_path = source_file_path
        self.db = self.clean_database()
        self.import_inventories()
        self.filepath_to_remind_files = (filepath_to_remind_files or DATA_DIR / "Remind output files")

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

    def update_electricity_to_remind_data(self):
        rdc = RemindDataCollection(self.scenario, self.year, self.filepath_to_remind_files)
        El = Electricity(self.db, rdc, self.scenario, self.year)
        self.db = El.update_electricity_markets()
        self.db = El.update_electricity_efficiency()

    def write_db_to_brightway(self):
        print('Write new database to Brightway2.')
        wurst.write_brightway2_database(self.db, eidb_label(self.scenario, self.year))

    def write_db_to_matrices(self):
        print("Write new database to matrix.")
        Export(self.db, self.scenario, self.year).export_db_to_matrices()

