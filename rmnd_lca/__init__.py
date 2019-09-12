
__all__ = (
    'generate_sets_from_filters',
    'DatabaseCleaner',
    'RemindDataCollection'
)
__version__ = (0, 0, 1)

# For relative imports to work in Python 3.6
import os, sys;
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

from .activity_maps import generate_sets_from_filters
from .clean_datasets import DatabaseCleaner
from .data_collection import RemindDataCollection

