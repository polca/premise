__all__ = ("NewDatabase", "clear_cache")
__version__ = (1, 0, 1)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
INVENTORY_DIR = Path(__file__).resolve().parent / "data" / "additional_inventories"

from .ecoinvent_modification import NewDatabase, clear_cache
