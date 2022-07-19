__all__ = ("NewDatabase", "clear_cache", "get_regions_definition")
__version__ = (1, 2, 0)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
INVENTORY_DIR = Path(__file__).resolve().parent / "data" / "additional_inventories"

from premise.ecoinvent_modification import NewDatabase
from premise.utils import clear_cache, get_regions_definition
