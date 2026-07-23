import os

if os.name == "nt":
    # openpyxl auto-enables lxml when installed. lxml 6.x can hard-crash
    # some Windows kernels while reading/writing Premise Excel workbooks.
    os.environ.setdefault("OPENPYXL_LXML", "False")

__all__ = (
    "NewDatabase",
    "IncrementalDatabase",
    "PathwaysDataPackage",
    "clear_cache",
    "clear_inventory_cache",
    "get_regions_definition",
)
__version__ = (2, 4, 7)


from premise.new_database import NewDatabase
from premise.incremental import IncrementalDatabase
from premise.pathways import PathwaysDataPackage
from premise.utils import clear_cache, clear_inventory_cache, get_regions_definition
import premise.scenario_downloader
