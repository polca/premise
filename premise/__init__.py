__all__ = ("NewDatabase",)
__version__ = (0, 4, 6)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
INVENTORY_DIR = Path(__file__).resolve().parent / "data" / "additional_inventories"

from .ecoinvent_modification import NewDatabase
