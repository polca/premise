import os

if os.name == "nt":
    # openpyxl auto-enables lxml when installed. lxml 6.x can hard-crash
    # some Windows kernels while reading/writing Premise Excel workbooks.
    os.environ.setdefault("OPENPYXL_LXML", "False")

__all__ = (
    "NewDatabase",
    "IncrementalDatabase",
    "PathwaysDataPackage",
    "InventoryStore",
    "LegacyInventoryStore",
    "CompactInventoryStore",
    "InventoryStoreBuilder",
    "ChangeReportArtifacts",
    "PremiseValidationError",
    "ValidationIssue",
    "ValidationPhaseResult",
    "ValidationReport",
    "ValidationRuleResult",
    "clear_cache",
    "clear_inventory_cache",
    "get_regions_definition",
)
__version__ = (3, 0, 0)


from premise.new_database import NewDatabase
from premise.change_report import ChangeReportArtifacts
from premise.incremental import IncrementalDatabase
from premise.pathways import PathwaysDataPackage
from premise.inventory_store import (
    CompactInventoryStore,
    InventoryStore,
    InventoryStoreBuilder,
    LegacyInventoryStore,
)
from premise.validation_framework import (
    PremiseValidationError,
    ValidationIssue,
    ValidationPhaseResult,
    ValidationReport,
    ValidationRuleResult,
)
from premise.utils import clear_cache, clear_inventory_cache, get_regions_definition
import premise.scenario_downloader
