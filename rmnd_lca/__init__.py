__all__ = (
    "InventorySet",
    "DatabaseCleaner",
    "RemindDataCollection",
    "NewDatabase",
    "Electricity",
)
__version__ = (0, 0, 1)

from pathlib import Path
DATA_DIR = Path(__file__).resolve().parent / "data"

from .activity_maps import InventorySet
from .clean_datasets import DatabaseCleaner
from .data_collection import RemindDataCollection
from .ecoinvent_modification import NewDatabase
from .electricity import Electricity
from .database import EcoinventDatabase
