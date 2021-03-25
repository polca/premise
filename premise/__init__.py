__all__ = (
    "NewDatabase",
    "Geomap",
    "DATA_DIR",
    "INVENTORY_DIR"
)
__version__ = (0, 2, 5)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
INVENTORY_DIR = Path(__file__).resolve().parent / "data" / "additional_inventories"

from .ecoinvent_modification import NewDatabase
from .geomap import Geomap
