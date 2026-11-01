import os
from importlib import import_module
from ._compat import MODULE_ALIASES, install as _install_module_aliases

_install_module_aliases()

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
__version__ = (2, 5, 5)


_EXPORT_MODULES = {
    "NewDatabase": "database.new_database",
    "ChangeReportArtifacts": "reporting.change_report",
    "IncrementalDatabase": "database.incremental",
    "PathwaysDataPackage": "iam.pathways",
    **dict.fromkeys(
        (
            "CompactInventoryStore",
            "InventoryStore",
            "InventoryStoreBuilder",
            "LegacyInventoryStore",
        ),
        "inventory.inventory_store",
    ),
    **dict.fromkeys(
        (
            "PremiseValidationError",
            "ValidationIssue",
            "ValidationPhaseResult",
            "ValidationReport",
            "ValidationRuleResult",
        ),
        "validation.validation_framework",
    ),
    **dict.fromkeys(
        ("clear_cache", "clear_inventory_cache", "get_regions_definition"), "core.utils"
    ),
}


def __getattr__(name):
    """Load build dependencies only when their public API is requested.

    In particular, isolated report workers need no Brightway project, log
    handlers, IAM transformations, or scenario download machinery.
    """
    if name in _EXPORT_MODULES:
        value = getattr(import_module(f".{_EXPORT_MODULES[name]}", __name__), name)
    elif "premise." + name in MODULE_ALIASES:
        value = import_module(MODULE_ALIASES["premise." + name])
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(
        set(globals())
        | set(__all__)
        | {name.removeprefix("premise.") for name in MODULE_ALIASES}
    )
