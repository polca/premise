import os
from importlib import import_module

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
__version__ = (2, 5, 2)


_EXPORT_MODULES = {
    "NewDatabase": "new_database",
    "ChangeReportArtifacts": "change_report",
    "IncrementalDatabase": "incremental",
    "PathwaysDataPackage": "pathways",
    **dict.fromkeys(
        (
            "CompactInventoryStore",
            "InventoryStore",
            "InventoryStoreBuilder",
            "LegacyInventoryStore",
        ),
        "inventory_store",
    ),
    **dict.fromkeys(
        (
            "PremiseValidationError",
            "ValidationIssue",
            "ValidationPhaseResult",
            "ValidationReport",
            "ValidationRuleResult",
        ),
        "validation_framework",
    ),
    **dict.fromkeys(
        ("clear_cache", "clear_inventory_cache", "get_regions_definition"), "utils"
    ),
}


def __getattr__(name):
    """Load build dependencies only when their public API is requested.

    In particular, isolated report workers need no Brightway project, log
    handlers, IAM transformations, or scenario download machinery.
    """
    if name in _EXPORT_MODULES:
        value = getattr(import_module(f".{_EXPORT_MODULES[name]}", __name__), name)
    elif name == "scenario_downloader":
        value = import_module(".scenario_downloader", __name__)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__) | {"scenario_downloader"})
