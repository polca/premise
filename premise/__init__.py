__all__ = ("NewDatabase", "clear_cache", "get_regions_definition")
__version__ = (1, 4, 0)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
INVENTORY_DIR = Path(__file__).resolve().parent / "data" / "additional_inventories"

from .ecoinvent_modification import NewDatabase
from .utils import clear_cache, get_regions_definition
