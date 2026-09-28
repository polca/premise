"""
new_database.py exposes methods to create a database, perform transformations on it,
as well as export it back.

"""

import gc
import hashlib
import inspect
import json
import logging
import os
import pickle
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import List, Literal, Union

import bw2data
import datapackage
from bw2io import ExcelImporter
from packaging.version import Version
from tqdm import tqdm

from . import __version__
from .cache_cleanup import with_cache_session
from .battery import _update_battery
from .biomass import _update_biomass
from .cement import _update_cement
from .clean_datasets import DatabaseCleaner
from .data_collection import IAMDataCollection
from .carbon_dioxide_removal import _update_cdr
from .change_report import (
    ChangeReportArtifacts,
    ChangeReportCacheEntry,
    ReportScenario,
    generate_structured_change_report,
    generate_validation_diagnostic_workbook,
)
from .electricity import _update_electricity
from .emissions import _update_emissions
from .final_energy import _update_final_energy
from .export import (
    Export,
    _build_superstructure_db,
    _prepare_database,
    build_datapackage,
    generate_scenario_factor_file,
    generate_superstructure_db,
    prepare_db_for_export,
    prepare_db_for_fast_export,
)
from .external import _update_external_scenarios
from .external_data_validation import check_external_scenarios
from .filesystem_constants import (
    DIR_CACHED_DB,
    DIR_CACHED_FILES,
    IAM_OUTPUT_DIR,
    INVENTORY_DIR,
)
from .fuels.base import _update_fuels
from .heat import _update_heat
from .inventory_imports import (
    AdditionalInventory,
    BaseInventoryImport,
    DefaultInventory,
)
from .inventory_store import (
    CompactInventoryStore,
    IndexedInventoryList,
    InventoryStore,
    InventoryStoreCorruptionError,
    InventoryStoreError,
    InventoryStoreVersionError,
    ReadOnlyInventoryStore,
    STORE_SCHEMA_VERSION,
    _compact_scenario_mapping,
    _hydrate_scenario_mapping,
    create_inventory_store,
)
from .metals import _update_metals
from .mining import _update_mining
from .provenance import ProvenanceCollector, record_change_event
from .report import generate_summary_report
from .scenario_array import (
    _load_scenario_array_dependencies,
    _write_scenario_array_datapackage,
)
from .sector_validation import validate_sector_contract
from .steel import _update_steel
from .transformation import _SCENARIO_GIS_CACHE_KEY, _SCENARIO_ROW_CACHE_KEY
from .transport import _update_vehicles
from .validation_framework import (
    VALIDATION_RULESET_VERSION,
    InventoryGraphValidator,
    PremiseValidationError,
    ValidationCertificate,
    ValidationIntent,
    ValidationPhaseResult,
    ValidationReport,
    ValidationRuleResult,
    inventory_baseline_snapshot,
)
from .validation import (
    normalize_exact_deterministic_exchange_duplicates,
    normalize_inventory_numeric_types,
    normalize_inventory_uncertainty,
)
from .utils import (
    CACHE_SCHEMA_VERSION,
    cache_ref_fingerprint,
    cache_ref_exists,
    database_metadata,
    clear_existing_cache,
    clear_runtime_caches,
    create_scenario_list,
    delete_all_pickles,
    eidb_label,
    hide_messages,
    info_on_utils_functions,
    load_constants,
    load_cached_database,
    load_database,
    print_version,
    resolve_cache_ref,
    warning_about_biogenic_co2,
    end_of_process,
    create_cache,
    restore_cached_classifications,
    scenario_metadata,
)
from .renewables import _update_wind_turbines

logger = logging.getLogger("module")


def _normalize_inventory_before_certification(database, provenance_owner=None):
    """Apply idempotent historical normalizations before baseline capture."""

    def on_change(dataset, action):
        if provenance_owner is None:
            return
        record_change_event(
            provenance_owner,
            dataset,
            "normalized",
            sector="normalization",
            reason_code=f"normalization.{action}",
            explanation=(
                "Certification normalization applied the "
                f"{action.replace('_', ' ')} rule."
            ),
        )

    normalize_inventory_numeric_types(database, on_change=on_change)
    normalize_inventory_uncertainty(database, on_change=on_change)
    normalize_exact_deterministic_exchange_duplicates(database, on_change=on_change)
    return database


if int(bw2data.__version__[0]) >= 4:
    from .brightway25 import write_brightway_database

else:
    from .brightway2 import write_brightway_database


FILEPATH_OIL_GAS_INVENTORIES = INVENTORY_DIR / "lci-ESU-oil-and-gas.xlsx"
FILEPATH_CARMA_INVENTORIES = INVENTORY_DIR / "lci-Carma-CCS.xlsx"
FILEPATH_CO_FIRING_INVENTORIES = INVENTORY_DIR / "lci-co-firing-power-plants.xlsx"
FILEPATH_CHP_INVENTORIES = INVENTORY_DIR / "lci-combined-heat-power-plant-CCS.xlsx"
FILEPATH_CC_INVENTORIES = INVENTORY_DIR / "lci-carbon-capture.xlsx"
FILEPATH_AFFORESTATION_INVENTORIES = INVENTORY_DIR / "lci-afforestation.xlsx"
FILEPATH_BIOFUEL_INVENTORIES = INVENTORY_DIR / "lci-biofuels.xlsx"
FILEPATH_BIOGAS_INVENTORIES = INVENTORY_DIR / "lci-biogas.xlsx"
FILEPATH_WASTE_CHP_INVENTORIES = INVENTORY_DIR / "lci-waste-CHP.xlsx"

FILEPATH_CARBON_FIBER_INVENTORIES = INVENTORY_DIR / "lci-carbon-fiber.xlsx"
FILEPATH_HYDROGEN_DISTRI_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-distribution.xlsx"

FILEPATH_HYDROGEN_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-electrolysis.xlsx"
FILEPATH_HYDROGEN_SOLAR_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-thermochemical-water-splitting.xlsx"
)
FILEPATH_HYDROGEN_PYROLYSIS_INVENTORIES = INVENTORY_DIR / "lci-hydrogen-pyrolysis.xlsx"

FILEPATH_HYDROGEN_BIOGAS_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-smr-atr-biogas.xlsx"
)
FILEPATH_HYDROGEN_NATGAS_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-smr-atr-natgas.xlsx"
)
FILEPATH_HYDROGEN_WOODY_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-wood-gasification.xlsx"
)
FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-coal-gasification.xlsx"
)
FILEPATH_HYDROGEN_COAL_GASIFICATION_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-hydrogen-coal-gasification_CCS.xlsx"
)
FILEPATH_HYDROGEN_OIL = INVENTORY_DIR / "lci-hydrogen-oil.xlsx"
FILEPATH_SYNFUEL_AVG_INVENTORIES = INVENTORY_DIR / "lci-synfuels-from-FT.xlsx"
FILEPATH_SYNFUEL_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-electrolysis.xlsx"
)

FILEPATH_SYNFUEL_INVENTORIES_FT_FROM_NG = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-natural-gas.xlsx"
)

FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-wood-gasification.xlsx"
)
FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_WITH_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-wood-gasification-with-CCS.xlsx"
)
FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-coal-gasification.xlsx"
)
FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_WITH_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-coal-gasification-with-CCS.xlsx"
)

FILEPATH_SYNFUEL_FROM_BIOMASS_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-FT-from-biomass-CCS.xlsx"
)
FILEPATH_SYNGAS_INVENTORIES = INVENTORY_DIR / "lci-syngas.xlsx"
FILEPATH_SYNGAS_FROM_COAL_INVENTORIES = INVENTORY_DIR / "lci-syngas-from-coal.xlsx"
FILEPATH_GEOTHERMAL_HEAT_INVENTORIES = INVENTORY_DIR / "lci-geothermal.xlsx"
FILEPATH_METHANOL_AVG_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol.xlsx"
)
FILEPATH_METHANOL_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-electrolysis.xlsx"
)
FILEPATH_METHANOL_FROM_WOOD = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-wood.xlsx"
)
FILEPATH_METHANOL_CEMENT_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-cement-plant.xlsx"
)
FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-coal.xlsx"
)
FILEPATH_METHANOL_FROM_COAL_FUELS_WITH_CCS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-coal-with-CCS.xlsx"
)
FILEPATH_METHANOL_FROM_BIOMASS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biomass.xlsx"
)
FILEPATH_METHANOL_FROM_BIOGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-biogas.xlsx"
)
FILEPATH_METHANOL_FROM_NATGAS_FUELS_INVENTORIES = (
    INVENTORY_DIR / "lci-synfuels-from-methanol-from-natural-gas.xlsx"
)
FILEPATH_AMMONIA = INVENTORY_DIR / "lci-ammonia.xlsx"
FILEPATH_LITHIUM = INVENTORY_DIR / "lci-lithium.xlsx"
FILEPATH_COBALT = INVENTORY_DIR / "lci-cobalt.xlsx"
FILEPATH_GRAPHITE = INVENTORY_DIR / "lci-graphite.xlsx"
FILEPATH_BATTERIES_NMC_NCA_LFP = INVENTORY_DIR / "lci-batteries-NMC111-811-NCA-LFP.xlsx"
FILEPATH_BATTERIES_NMC622_532 = INVENTORY_DIR / "lci-batteries-NMC622-NMC532.xlsx"
FILEPATH_BATTERIES_NMC955_LTO = INVENTORY_DIR / "lci-batteries-NMC955-LTO.xlsx"
FILEPATH_LIO2_BATTERY = INVENTORY_DIR / "lci-batteries-LiO2.xlsx"
FILEPATH_LIS_BATTERY = INVENTORY_DIR / "lci-batteries-LiS.xlsx"
FILEPATH_PHOTOVOLTAICS = INVENTORY_DIR / "lci-PV.xlsx"
FILEPATH_PHOTOVOLTAICS_2026 = INVENTORY_DIR / "lci-PV-2026.xlsx"
FILEPATH_PHOTOVOLTAICS_2026_ELECTRICITY = INVENTORY_DIR / "lci-PV-2026-electricity.xlsx"
FILEPATH_PHOTOVOLTAICS_CIGS = INVENTORY_DIR / "lci-PV-CIGS.xlsx"
FILEPATH_BIGCC = INVENTORY_DIR / "lci-BIGCC.xlsx"
FILEPATH_NUCLEAR_EPR = INVENTORY_DIR / "lci-nuclear_EPR.xlsx"
FILEPATH_NUCLEAR_SMR = INVENTORY_DIR / "lci-nuclear_SMR.xlsx"
FILEPATH_WAVE = INVENTORY_DIR / "lci-wave_energy.xlsx"
FILEPATH_FUEL_CELL = INVENTORY_DIR / "lci-fuel_cell.xlsx"
FILEPATH_CSP = INVENTORY_DIR / "lci-concentrating-solar-power.xlsx"
FILEPATH_HOME_STORAGE_BATTERIES = INVENTORY_DIR / "lci-home-batteries.xlsx"
FILEPATH_VANADIUM = INVENTORY_DIR / "lci-batteries-vanadium.xlsx"
FILEPATH_VANADIUM_REDOX_BATTERY = (
    INVENTORY_DIR / "lci-batteries-vanadium-redox-flow.xlsx"
)
FILEPATH_ORGANIC_REDOX_BATTERY = (
    INVENTORY_DIR / "lci-batteries-organic-and-hybrid-redox-flow.xlsx"
)
FILEPATH_SIB_BATTERY = INVENTORY_DIR / "lci-batteries-SIB.xlsx"
FILEPATH_HYDROGEN_TURBINE = INVENTORY_DIR / "lci-hydrogen-turbine.xlsx"
FILEPATH_HYDROGEN_HEATING = INVENTORY_DIR / "lci-hydrogen-heating.xlsx"
FILEPATH_METHANOL_HEATING = INVENTORY_DIR / "lci-methanol-heating.xlsx"
FILEPATH_ELECTRIC_HEATING = INVENTORY_DIR / "lci-electric-heating.xlsx"
FILEPATH_GERMANIUM = INVENTORY_DIR / "lci-germanium.xlsx"
FILEPATH_RHENIUM = INVENTORY_DIR / "lci-rhenium.xlsx"
FILEPATH_PGM = INVENTORY_DIR / "lci-PGM.xlsx"
FILEPATH_TWO_WHEELERS = INVENTORY_DIR / "lci-two_wheelers.xlsx"
FILEPATH_TRUCKS = INVENTORY_DIR / "lci-trucks.xlsx"
FILEPATH_BUSES = INVENTORY_DIR / "lci-buses.xlsx"
FILEPATH_PASS_CARS = INVENTORY_DIR / "lci-pass_cars.xlsx"
FILEPATH_RAIL_FREIGHT = INVENTORY_DIR / "lci-rail-freight.xlsx"
FILEPATH_PV_GAAS = INVENTORY_DIR / "lci-PV-GaAs.xlsx"
FILEPATH_PV_PEROVSKITE = INVENTORY_DIR / "lci-PV-perovskite.xlsx"
FILEPATH_BATTERY_CAPACITY = INVENTORY_DIR / "lci-battery-capacity.xlsx"
FILEPATH_BIOCHAR = INVENTORY_DIR / "lci-biochar-spruce.xlsx"
FILEPATH_ENHANCED_WEATHERING = INVENTORY_DIR / "lci-coastal-enhanced-weathering.xlsx"
FILEPATH_OCEAN_LIMING = INVENTORY_DIR / "lci-ocean-liming.xlsx"
FILEPATH_FINAL_ENERGY = INVENTORY_DIR / "lci-final-energy.xlsx"
FILEPATH_SULFIDIC_TAILINGS = INVENTORY_DIR / "lci-sulfidic-tailings.xlsx"
FILEPATH_SHIPS = INVENTORY_DIR / "lci-ships.xlsx"
FILEPATH_STEEL = INVENTORY_DIR / "lci-steel.xlsx"
FILEPATH_IND_HEAT_PUMP = INVENTORY_DIR / "lci-heat-pump-high-temp.xlsx"
FILEPATH_IND_ELECTRIC_BOILER = INVENTORY_DIR / "lci-electric-boiler-industrial.xlsx"
FILEPATH_NUCLEAR_HEAT = INVENTORY_DIR / "lci-nuclear-heat.xlsx"

config = load_constants()


def check_ei_filepath(filepath: str) -> Path:
    """Check for the existence of the file path."""

    if not Path(filepath).is_dir():
        raise FileNotFoundError(
            f"The directory for ecospold files {filepath} could not be found."
        )
    return Path(filepath)


def check_model_name(name: str) -> str:
    """Check for the validity of the IAM model name."""
    if name.lower() not in config["SUPPORTED_MODELS"]:
        raise ValueError(
            f"Only {config['SUPPORTED_MODELS']} are currently supported, not {name}."
        )
    return name.lower()


def check_pathway_name(name: str, filepath: Path, model: str) -> str:
    """Check the pathway name"""

    if model.lower() not in name:
        name_check = "_".join((model.lower(), name))
    else:
        name_check = name

    local_paths = [
        (filepath / name_check).with_suffix(extension)
        for extension in (".mif", ".xlsx", ".csv")
    ]
    if any(path.is_file() for path in local_paths):
        return name

    if name in config.get("UNAVAILABLE_PATHWAYS", ()):
        raise ValueError(
            f"IAM pathway '{model.lower()} - {name}' is not available for "
            "automatic download: the current Zenodo IAM archive does not "
            f"contain '{name_check}.csv'. Choose a pathway listed in "
            "SUPPORTED_PATHWAYS or provide the scenario locally as "
            f"'{name_check}.csv', '{name_check}.xlsx', or '{name_check}.mif' "
            f"in '{filepath}'."
        )

    if name not in config["SUPPORTED_PATHWAYS"]:
        raise ValueError(
            f"Only {config['SUPPORTED_PATHWAYS']} are currently supported, not {name}."
        )

    print(
        f"Cannot find the IAM scenario file at {filepath / name_check}. "
        f"Will check online."
    )
    return name


def check_year(year: [int, float]) -> int:
    """Check for the validity of the year passed."""
    try:
        year = int(year)
    except (TypeError, ValueError) as err:
        raise ValueError(f"{year} is not a valid year.") from err

    if not 2005 <= year <= 2100:
        raise ValueError(f"{year} must be comprised between 2005 and 2100.")

    return year


def check_filepath(path: str) -> Path:
    """
    Check for the existence of the file.
    """
    if not Path(path).is_dir():
        raise FileNotFoundError(f"The filepath {path} could not be found.")
    return Path(path)


def check_exclude(list_exc: List[str]) -> List[str]:
    """
    Check for the validity of the list of excluded functions.
    """

    if not isinstance(list_exc, list):
        raise TypeError("`exclude` should be a sequence of strings.")

    if not set(list_exc).issubset(config["LIST_TRANSF_FUNC"]):
        raise ValueError(
            "One or several of the transformation that you wish to exclude is not recognized."
        )
    return list_exc


def check_additional_inventories(inventories_list: List[dict]) -> List[dict]:
    """
    Check that any additional inventories that need to be imported are properly listed.
    :param inventories_list: list of dictionaries
    :return: list of dictionaries
    """

    if not isinstance(inventories_list, list):
        raise TypeError(
            "Inventories to import need to be in a sequence of dictionaries like so:"
            "["
            "{'inventories': 'a file path', 'ecoinvent version: '3.6'},"
            " {'inventories': 'a file path', 'ecoinvent version: '3.6'}"
            "]"
        )

    for inventory in inventories_list:
        if not isinstance(inventory, dict):
            raise TypeError(
                "Inventories to import need to be in a sequence of dictionaries like so:"
                "["
                "{'inventories': 'a file path', 'ecoinvent version: '3.6'},"
                " {'inventories': 'a file path', 'ecoinvent version: '3.6'}"
                "]"
            )
        if "region_duplicate" in inventory:
            if not isinstance(inventory["region_duplicate"], bool):
                raise TypeError(
                    "`region_duplicate`must be a boolean (`True`` `False`.)"
                )

        if not all(
            i for i in inventory.keys() if i in ["filepath", "ecoinvent version"]
        ):
            raise TypeError(
                "Both `filepath` and `ecoinvent version` "
                "must be present in the list of inventories to import."
            )

        if not Path(inventory["filepath"]).is_file():
            raise FileNotFoundError(
                f"Cannot find the inventory file: {inventory['filepath']}."
            )

        if inventory["ecoinvent version"] not in config["SUPPORTED_EI_VERSIONS"]:
            raise ValueError(
                "A lot of trouble will be avoided if the additional "
                f"inventories to import are ecoinvent 3.6, 3.7, 3-8 or 3.9-compliant, not {inventory['ecoinvent version']}."
            )

    return inventories_list


def check_db_version(version: [str, float]) -> str:
    """
    Check that the ecoinvent database version is supported
    :param version:
    :return: str
    """
    version = str(version)
    if version not in config["SUPPORTED_EI_VERSIONS"]:
        raise ValueError(
            f"Only {config['SUPPORTED_EI_VERSIONS']} are currently supported, not {version}."
        )

    # convert "3.7.1" to "3.7"
    if version == "3.7.1":
        version = "3.7"

    if version == "3.9.1":
        version = "3.9"

    return version


def check_scenarios(scenario: dict, key: bytes) -> dict:
    """
    Check that the scenarios are properly formatted and that
    all the necessary info is given.
    """

    if not all(name in scenario for name in ["model", "pathway", "year"]):
        raise ValueError(
            f"Missing parameters in {scenario}. Needs to include at least `model`,"
            f"`pathway` and `year`."
        )

    if "filepath" in scenario:
        filepath = scenario["filepath"]
        scenario["filepath"] = check_filepath(filepath)
    else:
        # Note: A directory path, not a file path
        scenario["filepath"] = IAM_OUTPUT_DIR
        if key is None:
            print("Reading unencrypted IAM output files.")
        else:
            # make sure that the key is 44 bytes long
            if len(key) != 44:
                raise ValueError(
                    f"The key must be 44 bytes long, not {len(key)} bytes."
                )

    scenario["model"] = check_model_name(scenario["model"])
    scenario["pathway"] = check_pathway_name(
        scenario["pathway"], scenario["filepath"], scenario["model"]
    )
    scenario["year"] = check_year(scenario["year"])

    if "external scenarios" in scenario:
        if not isinstance(scenario["external scenarios"], list):
            raise TypeError("external scenarios must be provided as a list.")

        # ensure both keys `data` and `scenario` are present
        for external_scenario in scenario["external scenarios"]:
            if not isinstance(external_scenario, dict) or not all(
                key in external_scenario for key in ("data", "scenario")
            ):
                raise ValueError(
                    "Each external scenario requires 'data' and 'scenario' fields."
                )

        scenario["external scenarios"] = check_external_scenarios(
            scenario["external scenarios"]
        )

    return scenario


def check_system_model(system_model: str) -> str:
    """
    Check that the system model is valid.
    """

    if not isinstance(system_model, str):
        raise TypeError(
            "The argument `system_model` must be a string"
            "('consequential', 'cutoff')."
        )

    if system_model not in ("consequential", "cutoff"):
        raise ValueError(
            "The argument `system_model` must be one of the two values:"
            "'consequential', 'cutoff'."
        )

    return system_model


def check_time_horizon(time_horizon: int) -> int:
    """
    Check the validity of the time horizon provided (in years).
    :param time_horizon: time horizon (in years), to determine marginal mixes for consequential modelling.
    :return: time horizon (in years)
    """

    if time_horizon is None:
        print(
            "`time_horizon`, used to identify marginal suppliers, is not specified. "
            "It is therefore set to 20 years."
        )
        time_horizon = 20

    try:
        int(time_horizon)
    except ValueError as err:
        raise Exception(
            "`time_horizon` must be an integer with a value between 5 and 50 years."
        ) from err

    if time_horizon < 5 or time_horizon > 50:
        raise ValueError(
            "`time_horizon` must be an integer with a value between 5 and 50 years."
        )

    return int(time_horizon)


def _export_to_matrices(obj):
    obj.export_db_to_matrices()


def _export_to_simapro(obj):
    obj.export_db_to_simapro()


def _export_to_olca(obj):
    obj.export_db_to_simapro(olca_compartments=True)


def check_presence_biosphere_database(biosphere_name: str) -> str:
    """
    Check that the biosphere database is present in the current project.

    This validation is only required when exporting to Brightway.
    """

    if biosphere_name not in bw2data.databases:
        current_project = getattr(bw2data.projects, "current", None)
        project_message = (
            f" in the current Brightway project '{current_project}'"
            if current_project
            else " in the current Brightway project"
        )
        raise ValueError(
            "Brightway export requires a biosphere database "
            f"named '{biosphere_name}'{project_message}. "
            f"Available databases: {list(bw2data.databases)}."
        )

    return biosphere_name


def _extract_default_inventory_importers(filepaths):
    """Extract workbook payloads concurrently while retaining input order."""

    with ThreadPoolExecutor(max_workers=min(4, len(filepaths) or 1)) as executor:
        return list(
            executor.map(
                ExcelImporter,
                (filepath for filepath, _ in filepaths),
            )
        )


class NewDatabase:
    """
    Build prospective life-cycle inventories from source databases and scenario data.

    :ivar source_type: the source of the ecoinvent database. Can be `brightway` or `ecospold`.
    :vartype source_type: str
    :vartype source_db: str
    :ivar system_model: Can be `cutoff` (default) or `consequential`.
    :vartype system_model: str
    :ivar system_model_args: arguments for the system model.
    :vartype system_model_args: dict
    :ivar version: ecoinvent database version.
    :vartype version: str
    :ivar biosphere_name: name to use for biosphere exchanges during export.
    :vartype biosphere_name: str
    :ivar generate_reports: whether to generate change and summary reports.
    :vartype generate_reports: bool

    """

    @with_cache_session
    def __init__(
        self,
        scenarios: List[dict],
        source_version: str = "3.12",
        source_type: str = "brightway",
        key: Union[bytes, str] = None,
        source_db: str = None,
        source_file_path: str = None,
        additional_inventories: List[dict] = None,
        system_model: str = "cutoff",
        system_args: dict = None,
        use_cached_inventories: bool = True,
        use_cached_database: bool = True,
        external_scenarios: list = None,
        quiet=False,
        keep_imports_uncertainty=True,
        keep_source_db_uncertainty=False,
        gains_scenario="CLE",
        use_absolute_efficiency=False,
        biosphere_name: str = "biosphere3",
        generate_reports: bool = True,
        inventory_backend: Literal["compact", "legacy"] = "legacy",
        cleanup_expired_caches: bool = True,
    ) -> None:
        """
        Initialize the NewDatabase class.

        :param scenarios: list of IAM scenarios to use.
        :param source_version: ecoinvent database version. Default is "3.12".
        :param source_type: source of the ecoinvent database. Can be `brightway` or `ecospold`. Default is `brightway`.
        :param key: decryption key for encrypted IAM data files. Default is None.
        :param source_db: name of the source ecoinvent database in the current project. Default is None.
        :param source_file_path: file path to the ecospold files, if source_type is `ecospold`. Default is None.
        :param additional_inventories: list of additional inventories to import. Default is None.
        :param system_model: system model to use. Can be `cutoff` (default) or `consequential`. Default is `cutoff`.
        :param system_args: arguments for the system model. Default is None.
        :param use_cached_inventories: whether to use cached inventories. Default is True.
        :param use_cached_database: whether to use a cached database. Default is True.
        :param external_scenarios: list of external scenarios to use. Default is None.
        :param quiet: whether to suppress output messages. Default is False.
        :param keep_imports_uncertainty: whether to keep uncertainty in imported inventories. Default is True.
        :param keep_source_db_uncertainty: whether to keep uncertainty in the source database. Default is False.
        :param gains_scenario: gains scenario to use. Can be either 'CLE' or 'MFR'. Default is 'CLE'.
        :param use_absolute_efficiency: whether to use absolute efficiency values. Default is False.
        :param biosphere_name: name to use for biosphere exchanges during export.
            It must match a biosphere database in the current Brightway project
            only when exporting to Brightway. Default is "biosphere3".
        :param generate_reports: whether to generate change and summary reports. Default is True.
        :param inventory_backend: inventory storage implementation. ``"compact"``
            is the production and certification-performance path; ``"legacy"``
            remains available as a compatibility and differential-testing oracle.
        :param cleanup_expired_caches: Remove managed scenario checkpoints unused
            for more than 24 hours at startup, only when no other NewDatabase
            instance is alive. Unmarked legacy caches are never expired.
        """
        self._inventory_api_active = False
        self.sector_update_methods = None
        self.source = source_db
        self.version = check_db_version(source_version)
        self.source_type = source_type
        self.system_model = check_system_model(system_model)
        self.system_model_args = system_args
        self.use_absolute_efficiency = use_absolute_efficiency
        self.keep_imports_uncertainty = keep_imports_uncertainty
        self.keep_source_db_uncertainty = keep_source_db_uncertainty
        self.biosphere_name = biosphere_name
        self.generate_reports = generate_reports
        self.build_id = uuid.uuid4().hex
        self._provenance_collector = ProvenanceCollector(self.build_id)
        self._change_report_cache: ChangeReportCacheEntry | None = None
        self._last_change_report_artifacts: ChangeReportArtifacts | None = None
        self._automatic_report_in_progress = False
        if inventory_backend not in {"compact", "legacy"}:
            raise ValueError("inventory_backend must be either 'compact' or 'legacy'.")
        self.inventory_backend = inventory_backend
        # Critical methodological certification has no public off switch. The
        # compact backend is the production/performance path; legacy remains a
        # compatibility and output-equivalence oracle under the same contract.
        self._validation_enabled = True
        self._validation_reports = {}
        self._validation_iam_fingerprints = {}
        self._source_inventory_store = None
        self._compact_source_checkpoint = None
        self.database_cache_filepath = None
        self.inventories_cache_filepath = None
        self._database_is_complete = False
        self._reload_original_database_from_cache_for_update = False
        self._shared_geography_caches = {}

        # if version is anything other than 3.8 or 3.9
        # and system_model is "consequential"
        # raise an error
        if (
            self.version
            not in ["3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]
            and self.system_model == "consequential"
        ):
            raise ValueError(
                "Consequential system model is only available for ecoinvent 3.8, 3.9, 3.10, 3.11, 3.12."
            )

        if gains_scenario not in ["CLE", "MFR"]:
            raise ValueError("gains_scenario must be either 'CLE' or 'MFR'")
        self.gains_scenario = gains_scenario

        if self.source_type == "ecospold":
            self.source_file_path = check_ei_filepath(source_file_path)
        else:
            self.source_file_path = None

        self.scenarios = [check_scenarios(scenario, key) for scenario in scenarios]

        # print some info
        if not quiet:
            print_version()
            warning_about_biogenic_co2()
            info_on_utils_functions()
            hide_messages()

        if additional_inventories:
            self.additional_inventories = check_additional_inventories(
                additional_inventories
            )
        else:
            self.additional_inventories = None

        # unlink all files in the cache directory
        delete_all_pickles()

        if external_scenarios:
            print(
                "External scenarios should now be given as part of the scenarios list. "
                "E.g., {'external scenarios': ['scenario': 'A', 'data': datapackage]}"
            )

        def _fetch_iam_data(scenario):
            data = IAMDataCollection(
                model=scenario["model"],
                pathway=scenario["pathway"],
                year=scenario["year"],
                external_scenarios=scenario.get("external scenarios"),
                filepath_iam_files=scenario["filepath"],
                key=key,
                system_model=self.system_model,
                system_model_args=self.system_model_args,
                gains_scenario=self.gains_scenario,
                use_absolute_efficiency=self.use_absolute_efficiency,
            )
            scenario["iam data"] = data

            if "external scenarios" in scenario:
                scenario["external data"] = data.get_external_data(
                    scenario["external scenarios"]
                )

        compact_cache_hit = None
        compact_cache_eligible = (
            self.inventory_backend == "compact"
            and use_cached_database
            and use_cached_inventories
            and self.additional_inventories is None
        )
        if compact_cache_eligible:
            compact_cache_hit = self._find_compact_source_checkpoint(source_db)

        imported_inventory_data = False
        if compact_cache_hit is not None:
            checkpoint, cache_refs = compact_cache_hit
            print("- Opening compact source database")
            self.database_cache_filepath = cache_refs["source"]
            self.database_metadata_cache_filepath = cache_refs["source_metadata"]
            self.inventories_cache_filepath = cache_refs["inventories"]
            self.inventories_metadata_cache_filepath = cache_refs[
                "inventories_metadata"
            ]
            for scenario in self.scenarios:
                scenario["database metadata cache filepath"] = (
                    self.database_metadata_cache_filepath
                )
                scenario["inventories metadata cache filepath"] = (
                    self.inventories_metadata_cache_filepath
                )
            self._database = None
            self._database_is_complete = True
            self._compact_source_checkpoint = checkpoint
        else:
            print("- Extracting source database")
            if use_cached_database:
                self._database = self.__find_cached_db(source_db)
                for scenario in self.scenarios:
                    scenario["database metadata cache filepath"] = (
                        self.database_metadata_cache_filepath
                    )
            else:
                self._database = self.__clean_database()

            print("- Extracting inventories")
            if use_cached_inventories:
                data = self.__find_cached_inventories(source_db)
                for scenario in self.scenarios:
                    scenario["inventories metadata cache filepath"] = (
                        self.inventories_metadata_cache_filepath
                    )
                if data is not None:
                    self._database.extend(data)
                else:
                    imported_inventory_data = True
                # A cache miss imports inventories directly into ``self._database``
                # before replacing the imported tail with the trimmed cached
                # representation, so the inventory coverage is complete in both the
                # hit and miss cases here and the original form can be reloaded from
                # cache when needed.
                self._database_is_complete = True
            else:
                self.__import_inventories()
                imported_inventory_data = True
                self._database_is_complete = True

        if self.additional_inventories:
            print("- Importing additional inventories")
            data = self.__import_additional_inventories(self.additional_inventories)
            self._database.extend(data)
            imported_inventory_data = True

        if imported_inventory_data:
            self._clear_inventory_importer_state()

        print("- Fetching IAM data")
        for scenario in self.scenarios:
            _fetch_iam_data(scenario)

        if self._compact_source_checkpoint is not None:
            try:
                self._source_inventory_store = InventoryStore.open(
                    self._compact_source_checkpoint
                )
            except (InventoryStoreCorruptionError, InventoryStoreVersionError):
                # The versioned pickle caches remain the source of truth. A
                # partial or corrupt compact derivative is rebuilt wholesale.
                if compact_cache_hit is None:  # pragma: no cover - invariant guard
                    raise InventoryStoreError(
                        "A compact checkpoint was selected without cache references."
                    )
                _, cache_refs = compact_cache_hit
                self._database = self._load_compact_source_cache_payload(cache_refs)
                _normalize_inventory_before_certification(self._database)
                self._source_inventory_store = create_inventory_store(
                    self._database,
                    backend="compact",
                    scenario_identity="source",
                    take_ownership=True,
                    compute_fingerprints=True,
                )
                self._compact_source_checkpoint = self._write_compact_source_checkpoint(
                    source_db, self._source_inventory_store
                )
                self._source_inventory_store = None
                self._database = None
                gc.collect()
                self._source_inventory_store = InventoryStore.open(
                    self._compact_source_checkpoint
                )
        else:
            _normalize_inventory_before_certification(self._database)
            self._source_inventory_store = create_inventory_store(
                self._database,
                backend=self.inventory_backend,
                scenario_identity="source",
                take_ownership=True,
                compute_fingerprints=True,
            )
            if compact_cache_eligible:
                self._compact_source_checkpoint = self._write_compact_source_checkpoint(
                    source_db, self._source_inventory_store
                )
                if self._compact_source_checkpoint is not None:
                    self._source_inventory_store = None
                    self._database = None
                    gc.collect()
                    self._source_inventory_store = InventoryStore.open(
                        self._compact_source_checkpoint
                    )
        # The mutable extraction list is an implementation detail only.  From
        # this point onward all scenario ownership goes through InventoryStore.
        self._database = None
        self._inventory_api_active = True

        print("Done!")

    @property
    def database(self):
        """The historical mutable inventory attribute has been removed."""

        if not getattr(self, "_inventory_api_active", False):
            return getattr(self, "_database", None)
        raise AttributeError(
            "NewDatabase.database was removed in premise 2.5.0. Use "
            "get_inventory_store() for immutable access or "
            "materialize_inventory() when a real list[dict] is unavoidable "
            "(materialization has a substantial memory cost)."
        )

    @database.setter
    def database(self, value):
        # A compatibility hook for narrowly constructed internal/test objects.
        # Fully initialised premise 2.5.0 instances reject both reads and writes.
        if getattr(self, "_inventory_api_active", False):
            raise AttributeError(
                "NewDatabase.database was removed in premise 2.5.0; inventory "
                "mutation must use get_inventory_store(writable=True)."
            )
        self._database = value

    def _cache_database_name(self, db_name: str | None) -> str:
        if db_name is None and self.source_type == "ecospold":
            db_name = f"ecospold_{self.system_model}_{self.version}"
        if db_name is None:
            raise ValueError("A source database name is required for caching.")
        return db_name.strip().lower()

    def _database_cache_path(
        self, db_name: str | None, *, inventories: bool = False
    ) -> Path:
        uncertainty = (
            self.keep_imports_uncertainty
            if inventories
            else self.keep_source_db_uncertainty
        )
        uncertainty_label = "w_uncertainty" if uncertainty else "wo_uncertainty"
        inventory_label = "_inventories" if inventories else ""
        if inventories:
            digest = hashlib.sha256()
            for path in (
                FILEPATH_PHOTOVOLTAICS_2026,
                FILEPATH_PHOTOVOLTAICS_2026_ELECTRICITY,
                FILEPATH_PHOTOVOLTAICS_CIGS,
            ):
                digest.update(path.name.encode())
                digest.update(path.read_bytes())
            inventory_label += f"_pv2026_{digest.hexdigest()[:16]}"
        return (
            DIR_CACHED_DB
            / f"cached_{''.join(tuple(map(str, __version__)))}_v{CACHE_SCHEMA_VERSION}_"
            f"{self._cache_database_name(db_name)}_{uncertainty_label}"
            f"{inventory_label}.pickle"
        )

    @staticmethod
    def _metadata_cache_path(cache_path: Path) -> Path:
        return Path(str(cache_path).replace(".pickle", " (metadata).pickle"))

    def _compact_source_cache_references(
        self, db_name: str | None
    ) -> dict[str, Path] | None:
        source = self._database_cache_path(db_name)
        inventories = self._database_cache_path(db_name, inventories=True)
        candidates = {
            "source": source,
            "source_metadata": self._metadata_cache_path(source),
            "inventories": inventories,
            "inventories_metadata": self._metadata_cache_path(inventories),
        }
        if not all(cache_ref_exists(path) for path in candidates.values()):
            return None
        return {name: resolve_cache_ref(path) for name, path in candidates.items()}

    def _compact_source_checkpoint_path(self, db_name: str | None) -> Path:
        source = self._database_cache_path(db_name)
        return source.with_name(
            f"{source.stem}_with_inventories_store_v{STORE_SCHEMA_VERSION}"
            ".inventory-store"
        )

    @staticmethod
    def _compact_source_signature(cache_refs: dict[str, Path]) -> dict[str, str]:
        return {
            name: cache_ref_fingerprint(cache_ref)
            for name, cache_ref in sorted(cache_refs.items())
        }

    def _find_compact_source_checkpoint(
        self, db_name: str | None
    ) -> tuple[Path, dict[str, Path]] | None:
        cache_refs = self._compact_source_cache_references(db_name)
        if cache_refs is None:
            return None
        checkpoint = self._compact_source_checkpoint_path(db_name)
        marker = checkpoint / "source-cache.json"
        if not checkpoint.is_dir() or not marker.is_file():
            return None
        try:
            recorded = json.loads(marker.read_text(encoding="utf-8"))
            current = self._compact_source_signature(cache_refs)
        except (OSError, ValueError, TypeError):
            return None
        if recorded.get("cache_fingerprints") != current:
            return None
        return checkpoint, cache_refs

    def _write_compact_source_checkpoint(
        self,
        db_name: str | None,
        store: CompactInventoryStore,
    ) -> Path | None:
        cache_refs = self._compact_source_cache_references(db_name)
        if cache_refs is None:
            return None
        checkpoint = store.checkpoint(self._compact_source_checkpoint_path(db_name))
        marker = {
            "store_schema_version": STORE_SCHEMA_VERSION,
            "cache_fingerprints": self._compact_source_signature(cache_refs),
        }
        (checkpoint / "source-cache.json").write_text(
            json.dumps(marker, sort_keys=True, indent=2), encoding="utf-8"
        )
        return checkpoint

    @staticmethod
    def _load_compact_source_cache_payload(cache_refs: dict[str, Path]) -> list[dict]:
        database = load_cached_database(cache_refs["source"])
        restore_cached_classifications(database, cache_refs["source_metadata"])
        inventories = load_cached_database(cache_refs["inventories"])
        restore_cached_classifications(inventories, cache_refs["inventories_metadata"])
        database.extend(inventories)
        return database

    def __find_cached_db(self, db_name: str) -> List[dict]:
        """
        If `use_cached_db` = True, then we look for a cached database.
        If cannot be found, we create a cache for next time.
        :param db_name: database name
        :return: database
        """
        file_name = self._database_cache_path(db_name)

        # check that file path leads to an existing file
        if cache_ref_exists(file_name):
            # return the cached database
            self.database_cache_filepath = resolve_cache_ref(file_name)
            self.database_metadata_cache_filepath = resolve_cache_ref(
                Path(str(file_name).replace(".pickle", " (metadata).pickle"))
            )
            database = load_cached_database(self.database_cache_filepath)
            return restore_cached_classifications(
                database, self.database_metadata_cache_filepath
            )

        # extract the database, pickle it for next time and return it
        print("Cannot find cached database. Will create one now for next time...")
        clear_existing_cache()
        database = self.__clean_database()
        database, metadata_cache_filepath = create_cache(database, file_name)
        self.database_cache_filepath = resolve_cache_ref(file_name)
        self.database_metadata_cache_filepath = metadata_cache_filepath
        self._reload_original_database_from_cache_for_update = True
        return database

    def __find_cached_inventories(self, db_name: str) -> Union[None, List[dict]]:
        """
        If `use_cached_inventories` = True, then we look for a cached inventories.
        If cannot be found, we create a cache for next time.
        :param db_name: database name
        :return: database
        """
        file_name = self._database_cache_path(db_name, inventories=True)

        # check that file path leads to an existing file
        if cache_ref_exists(file_name):
            # return the cached database
            self.inventories_cache_filepath = resolve_cache_ref(file_name)
            self.inventories_metadata_cache_filepath = resolve_cache_ref(
                Path(str(file_name).replace(".pickle", " (metadata).pickle"))
            )
            data = load_cached_database(self.inventories_cache_filepath)
            return restore_cached_classifications(
                data, self.inventories_metadata_cache_filepath
            )

        # else, extract the database, pickle it for next time and return it
        print("Cannot find cached inventories. Will create them now for next time...")
        inventory_start = len(self._database)
        import_inventories = self.__import_inventories
        if "collect_data" in inspect.signature(import_inventories).parameters:
            import_inventories(collect_data=False)
        else:
            import_inventories()

        trimmed_inventories, inventories_metadata_cache_filepath = create_cache(
            self._database[inventory_start:], file_name
        )
        self._database[inventory_start:] = trimmed_inventories
        self.inventories_cache_filepath = resolve_cache_ref(file_name)
        self.inventories_metadata_cache_filepath = inventories_metadata_cache_filepath
        self._reload_original_database_from_cache_for_update = True
        print(
            "Data cached. Continuing with the cached inventory representation for\n"
            "the rest of this workflow."
        )
        return None

    def __clean_database(self) -> List[dict]:
        """
        Extracts the ecoinvent database, loads it into a dictionary and does a little bit of housekeeping
        (adds missing locations, reference products, etc.).
        :return:
        """
        return DatabaseCleaner(
            self.source, self.source_type, self.source_file_path, self.version
        ).prepare_datasets(self.keep_source_db_uncertainty)

    def __import_inventories(self, collect_data: bool = True) -> List[dict]:
        """
        This method will trigger the import of a number of pickled inventories
        and merge them into the database dictionary.
        """

        print("Importing default inventories...\n")

        # with HiddenPrints():
        # Manual import
        # file path and original ecoinvent version
        data, unlinked = [], []
        filepaths = [
            (FILEPATH_OIL_GAS_INVENTORIES, "3.7"),
            (FILEPATH_CARMA_INVENTORIES, "3.5"),
            (FILEPATH_CO_FIRING_INVENTORIES, "3.5"),
            (FILEPATH_CHP_INVENTORIES, "3.5"),
            (FILEPATH_CC_INVENTORIES, "3.9"),
            (FILEPATH_BIOGAS_INVENTORIES, "3.6"),
            (FILEPATH_WASTE_CHP_INVENTORIES, "3.10"),
            (FILEPATH_CARBON_FIBER_INVENTORIES, "3.9"),
            (FILEPATH_LITHIUM, "3.8"),
            (FILEPATH_COBALT, "3.8"),
            (FILEPATH_GRAPHITE, "3.8"),
            (FILEPATH_BATTERIES_NMC_NCA_LFP, "3.8"),
            (FILEPATH_BATTERIES_NMC622_532, "3.8"),
            (FILEPATH_BATTERIES_NMC955_LTO, "3.8"),
            (FILEPATH_LIS_BATTERY, "3.9"),
            (FILEPATH_LIO2_BATTERY, "3.9"),
            (FILEPATH_VANADIUM, "3.9"),
            (FILEPATH_VANADIUM_REDOX_BATTERY, "3.9"),
            (FILEPATH_ORGANIC_REDOX_BATTERY, "3.9"),
            (FILEPATH_SIB_BATTERY, "3.9"),
            (FILEPATH_BATTERY_CAPACITY, "3.10"),
            (FILEPATH_HOME_STORAGE_BATTERIES, "3.9"),
            (FILEPATH_IND_HEAT_PUMP, "3.11"),
            (FILEPATH_IND_ELECTRIC_BOILER, "3.10"),
            (FILEPATH_PHOTOVOLTAICS, "3.7"),
            (FILEPATH_PGM, "3.8"),
            (FILEPATH_HYDROGEN_INVENTORIES, "3.9"),
            (FILEPATH_HYDROGEN_SOLAR_INVENTORIES, "3.9"),
            (FILEPATH_HYDROGEN_PYROLYSIS_INVENTORIES, "3.9"),
            (FILEPATH_METHANOL_FUELS_INVENTORIES, "3.7"),
            (FILEPATH_METHANOL_AVG_FUELS_INVENTORIES, "3.7"),
            (FILEPATH_METHANOL_CEMENT_FUELS_INVENTORIES, "3.7"),
            (FILEPATH_HYDROGEN_COAL_GASIFICATION_INVENTORIES, "3.7"),
            (FILEPATH_HYDROGEN_COAL_GASIFICATION_CCS_INVENTORIES, "3.7"),
            (FILEPATH_METHANOL_FROM_COAL_FUELS_INVENTORIES, "3.7"),
            (FILEPATH_METHANOL_FROM_COAL_FUELS_WITH_CCS_INVENTORIES, "3.7"),
            (FILEPATH_HYDROGEN_DISTRI_INVENTORIES, "3.7"),
            (FILEPATH_HYDROGEN_BIOGAS_INVENTORIES, "3.7"),
            (FILEPATH_HYDROGEN_NATGAS_INVENTORIES, "3.7"),
            (FILEPATH_HYDROGEN_WOODY_INVENTORIES, "3.7"),
            (FILEPATH_HYDROGEN_OIL, "3.10"),
            (FILEPATH_HYDROGEN_TURBINE, "3.9"),
            (FILEPATH_SYNGAS_INVENTORIES, "3.9"),
            (FILEPATH_METHANOL_FROM_WOOD, "3.7"),
            (FILEPATH_AMMONIA, "3.9"),
            (FILEPATH_SYNGAS_FROM_COAL_INVENTORIES, "3.7"),
            (FILEPATH_BIOFUEL_INVENTORIES, "3.7"),
            (FILEPATH_SYNFUEL_INVENTORIES, "3.7"),
            (FILEPATH_SYNFUEL_AVG_INVENTORIES, "3.7"),
            (FILEPATH_SYNFUEL_INVENTORIES_FT_FROM_NG, "3.7"),
            (
                FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_INVENTORIES,
                "3.7",
            ),
            (
                FILEPATH_SYNFUEL_FROM_FT_FROM_WOOD_GASIFICATION_WITH_CCS_INVENTORIES,
                "3.7",
            ),
            (
                FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_INVENTORIES,
                "3.7",
            ),
            (
                FILEPATH_SYNFUEL_FROM_FT_FROM_COAL_GASIFICATION_WITH_CCS_INVENTORIES,
                "3.7",
            ),
            (FILEPATH_GEOTHERMAL_HEAT_INVENTORIES, "3.6"),
            (FILEPATH_BIGCC, "3.8"),
            (FILEPATH_NUCLEAR_EPR, "3.8"),
            # Nuclear heat links to the EPR activity imported immediately above.
            (FILEPATH_NUCLEAR_HEAT, "3.10"),
            (FILEPATH_NUCLEAR_SMR, "3.8"),
            (FILEPATH_WAVE, "3.8"),
            (FILEPATH_FUEL_CELL, "3.10"),
            (FILEPATH_CSP, "3.9"),
            (FILEPATH_HYDROGEN_HEATING, "3.9"),
            (FILEPATH_METHANOL_HEATING, "3.10"),
            (FILEPATH_ELECTRIC_HEATING, "3.10"),
            (FILEPATH_GERMANIUM, "3.9"),
            (FILEPATH_RHENIUM, "3.9"),
            (FILEPATH_TWO_WHEELERS, "3.7"),
            (FILEPATH_TRUCKS, "3.7"),
            (FILEPATH_BUSES, "3.7"),
            (FILEPATH_PASS_CARS, "3.7"),
            (FILEPATH_RAIL_FREIGHT, "3.9"),
            (FILEPATH_PV_GAAS, "3.10"),
            (FILEPATH_PV_PEROVSKITE, "3.10"),
            (FILEPATH_BIOCHAR, "3.10"),
            (FILEPATH_OCEAN_LIMING, "3.10"),
            (FILEPATH_ENHANCED_WEATHERING, "3.10"),
            (FILEPATH_FINAL_ENERGY, "3.10"),
            (FILEPATH_SULFIDIC_TAILINGS, "3.8"),
            (FILEPATH_SHIPS, "3.10"),
            (FILEPATH_STEEL, "3.9"),
        ]
        if Version(self.version) >= Version("3.11"):
            # These two re/afforestation datasets use suppliers first available
            # in ecoinvent 3.11. Their workbook contains 3.12 identifiers so
            # that premise can migrate them backwards when building with 3.11.
            filepaths.append((FILEPATH_AFFORESTATION_INVENTORIES, "3.12"))

        selected_filepaths = []
        for filepath in filepaths:
            if filepath[0] == FILEPATH_PHOTOVOLTAICS:
                selected_filepaths.extend(
                    [
                        (FILEPATH_PHOTOVOLTAICS_2026, "3.12"),
                        (FILEPATH_PHOTOVOLTAICS_2026_ELECTRICITY, "3.12"),
                        (FILEPATH_PHOTOVOLTAICS_CIGS, "3.7"),
                    ]
                )
                continue
            # make an exception for FILEPATH_OIL_GAS_INVENTORIES
            # ecoinvent version is 3.9
            if filepath[0] in [
                FILEPATH_OIL_GAS_INVENTORIES,
                FILEPATH_BATTERIES_NMC_NCA_LFP,
            ] and self.version in ["3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]:
                continue

            if filepath[0] in [
                FILEPATH_BATTERIES_NMC622_532,
                FILEPATH_GRAPHITE,
            ] and self.version in ["3.11", "3.12"]:
                continue

            selected_filepaths.append(filepath)

        # Excel extraction is independent and read-only. All migrations,
        # linking, duplicate checks, and merges below remain sequential in the
        # original workbook order.
        preloaded_importers = _extract_default_inventory_importers(selected_filepaths)

        for filepath, preloaded_importer in zip(
            selected_filepaths, preloaded_importers
        ):
            inventory = DefaultInventory(
                database=self._database,
                version_in=filepath[1],
                version_out=self.version,
                path=filepath[0],
                system_model=self.system_model,
                keep_uncertainty_data=self.keep_imports_uncertainty,
                preloaded_importer=preloaded_importer,
            )
            datasets = inventory.merge_inventory()
            if collect_data:
                data.extend(datasets)
            self._database.extend(datasets)
            unlinked.extend(inventory.list_unlinked)

        if len(unlinked) > 0:
            raise ValueError("Fix the unlinked exchanges before proceeding")

        return data

    @staticmethod
    def _clear_inventory_importer_state() -> None:
        """Release importer instances retained by method-level caches after import."""

        BaseInventoryImport.correct_product_field.cache_clear()
        gc.collect()

    def _can_reload_original_database(self) -> bool:
        return (
            getattr(self, "database_cache_filepath", None) is not None
            and getattr(self, "inventories_cache_filepath", None) is not None
            and getattr(self, "additional_inventories", None) is None
        )

    @staticmethod
    def _load_pickled_database(filepath: Path) -> List[dict]:
        return load_cached_database(filepath)

    def _load_original_database(self) -> List[dict]:
        source_store = getattr(self, "_source_inventory_store", None)
        if source_store is not None:
            return source_store.materialize(restore_metadata=True)

        if self._database is not None and self._database_is_complete:
            return self._database

        if self._can_reload_original_database():
            database = self._load_pickled_database(self.database_cache_filepath)
            database.extend(
                self._load_pickled_database(self.inventories_cache_filepath)
            )
            return database

        if self._database is None:
            raise ValueError(
                "The original database is not available in memory and cannot be "
                "reloaded from cache."
            )

        return self._database

    @staticmethod
    def _scenario_identity(scenario: dict) -> tuple:
        external = tuple(
            item.get("scenario")
            for item in scenario.get("external scenarios", ())
            if isinstance(item, dict)
        )
        return (
            scenario.get("model"),
            scenario.get("pathway"),
            scenario.get("year"),
            external,
        )

    def _get_provenance_collector(self) -> ProvenanceCollector:
        collector = getattr(self, "_provenance_collector", None)
        if collector is None:
            build_id = getattr(self, "build_id", None) or uuid.uuid4().hex
            self.build_id = build_id
            collector = self._provenance_collector = ProvenanceCollector(build_id)
        return collector

    def _restore_scenario_provenance(
        self, scenario: dict, store: InventoryStore
    ) -> None:
        identity = self._scenario_identity(scenario)
        underlying = getattr(store, "_store", store)
        payload = scenario.get("_provenance") or getattr(
            underlying, "_provenance_payload", None
        )
        if isinstance(payload, dict):
            scenario["_provenance"] = payload
            self._get_provenance_collector().restore(identity, payload)

    def _ensure_scenario_store(
        self, scenario: dict, *, materialize_source: bool = False
    ) -> InventoryStore:
        store = scenario.get("_inventory_store")
        if store is not None:
            self._restore_scenario_provenance(scenario, store)
            return store

        # Public inspection and subsequent updates use the durable checkpoint.
        # The canonical handoff is intentionally private to immediate export
        # because checking it out transfers ownership and empties the store.
        scenario.pop("_inventory_export_handoff", None)

        checkpoint = scenario.get("_inventory_checkpoint")
        if checkpoint is not None:
            try:
                store = InventoryStore.open(checkpoint)
            except (InventoryStoreCorruptionError, InventoryStoreVersionError):
                # Never partially load an invalid compact bundle.  A source
                # graph can safely recreate an unmodified scenario; callers
                # with a transformed corrupt scenario receive the original
                # validation error because its overlay cannot be reconstructed.
                if scenario.get("applied functions"):
                    raise
                scenario.pop("_inventory_checkpoint", None)
                checkpoint = None
                store = None
        else:
            store = None

        if store is None:
            source_store = getattr(self, "_source_inventory_store", None)
            if source_store is None and materialize_source:
                source_store = getattr(self, "_update_source_store", None)
            if source_store is None:
                compact_checkpoint = getattr(self, "_compact_source_checkpoint", None)
                if (
                    getattr(self, "inventory_backend", "compact") == "compact"
                    and compact_checkpoint is not None
                ):
                    source_store = InventoryStore.open(compact_checkpoint)
            if source_store is None:
                # Supports narrowly constructed NewDatabase instances in tools
                # and tests without reintroducing a public mutable attribute.
                source_store = create_inventory_store(
                    _normalize_inventory_before_certification(
                        self._load_original_database()
                    ),
                    backend=getattr(self, "inventory_backend", "compact"),
                    scenario_identity="source",
                    take_ownership=True,
                    compute_fingerprints=True,
                )
                self._source_inventory_store = source_store
            scenario_identity = self._scenario_identity(scenario)
            if isinstance(source_store, CompactInventoryStore):
                try:
                    store = source_store.fresh_columnar_view(scenario_identity)
                except InventoryStoreError:
                    store = source_store.fork(scenario_identity)
            else:
                if materialize_source:
                    # The caller immediately makes an isolated mutable copy.
                    # Forking first would copy the same graph twice.
                    if hasattr(self, "_update_source_store"):
                        self._update_source_store = source_store
                    self._restore_scenario_provenance(scenario, source_store)
                    return source_store
                store = source_store.fork(scenario_identity)
        scenario["_inventory_store"] = store
        self._restore_scenario_provenance(scenario, store)
        return store

    def get_inventory_store(
        self,
        scenario: int = 0,
        *,
        writable: bool = False,
    ) -> InventoryStore:
        """Return the inventory store for a scenario.

        Read-only access is the default.  Even writable stores can only be
        changed through ``store.transaction(...)``; snapshots returned by read
        methods are immutable.
        """

        if not isinstance(scenario, int):
            raise TypeError("scenario must be an integer scenario position.")
        if scenario < 0 or scenario >= len(self.scenarios):
            raise IndexError(
                f"scenario position {scenario} is outside 0..{len(self.scenarios) - 1}."
            )
        store = self._ensure_scenario_store(self.scenarios[scenario])
        return store if writable else ReadOnlyInventoryStore(store)

    def materialize_inventory(
        self,
        scenario: int = 0,
        *,
        restore_metadata: bool = True,
    ) -> List[dict]:
        """Return a real ``list[dict]`` for an integration that requires one.

        Materialization duplicates the complete graph in Python and can have a
        substantial memory cost.  Prefer :meth:`get_inventory_store` for
        inspection and transformation code.
        """

        return self.get_inventory_store(scenario).materialize(
            restore_metadata=restore_metadata
        )

    def _validation_source_fingerprint(self) -> str:
        """Fingerprint source/cache identity without materialising the graph."""

        checkpoint = getattr(self, "_compact_source_checkpoint", None)
        if checkpoint is not None:
            manifest_path = Path(checkpoint) / "manifest.json"
            if manifest_path.is_file():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    manifest = {}
                fingerprint = manifest.get("source_fingerprint")
                if fingerprint:
                    return str(fingerprint)

        payload = {
            "source": getattr(self, "source", None),
            "version": getattr(self, "version", None),
            "system_model": getattr(self, "system_model", None),
            "database_cache": str(getattr(self, "database_cache_filepath", None)),
            "inventories_cache": str(getattr(self, "inventories_cache_filepath", None)),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def _ensure_validation_baseline_snapshot(self) -> tuple:
        """Capture source fingerprints and cycles once for fresh scenarios."""

        cached = getattr(self, "_validation_source_baseline_snapshot", None)
        if cached is not None:
            return cached
        source_store = getattr(self, "_source_inventory_store", None)
        if source_store is None:
            checkpoint = getattr(self, "_compact_source_checkpoint", None)
            if checkpoint is not None:
                source_store = InventoryStore.open(checkpoint)
        if source_store is None:
            cached = ({}, frozenset())
        else:
            source_fingerprint = self._validation_source_fingerprint()
            cache_path = DIR_CACHED_FILES / (
                f"validation-baseline-r{VALIDATION_RULESET_VERSION}-"
                f"s{STORE_SCHEMA_VERSION}-{source_fingerprint}.pkl"
            )
            try:
                payload = pickle.loads(cache_path.read_bytes())
                fingerprints = payload["fingerprints"]
                cycles = payload["cycles"]
                if len(fingerprints) != len(source_store):
                    raise ValueError("baseline activity count changed")
                cached = (fingerprints, cycles)
            except (
                OSError,
                EOFError,
                AttributeError,
                KeyError,
                TypeError,
                ValueError,
                pickle.PickleError,
            ):
                # Source checkpoints persist fingerprints computed while the
                # import dictionaries are in hand.  Recompute once after the
                # columnar checkpoint is reopened so the baseline and the
                # transformed scenario use the same scalar/mapping
                # representation.  The resulting audit is cached below.
                underlying = getattr(source_store, "_store", source_store)
                state = getattr(underlying, "_state", None)
                if state is not None:
                    state.activity_fingerprints.clear()
                fingerprints, cycles = inventory_baseline_snapshot(source_store)
                cached = (dict(fingerprints), cycles)
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = cache_path.with_name(
                    f".{cache_path.name}.tmp-{uuid.uuid4().hex}"
                )
                temporary.write_bytes(
                    pickle.dumps(
                        {"fingerprints": cached[0], "cycles": cached[1]},
                        protocol=pickle.HIGHEST_PROTOCOL,
                    )
                )
                temporary.replace(cache_path)
        self._validation_source_baseline_snapshot = cached
        return cached

    def _ensure_validation_baseline_cycles(self) -> frozenset:
        """Return source cycles retained for compatibility with internal callers."""

        return self._ensure_validation_baseline_snapshot()[1]

    def _validation_iam_fingerprint(self, scenario: dict) -> str:
        """Fingerprint IAM identity and source bytes without hashing repeatedly."""

        external = [
            item.get("scenario")
            for item in scenario.get("external scenarios", ())
            if isinstance(item, dict)
        ]
        source = Path(str(scenario.get("filepath", "")))
        candidates = []
        if source.is_file():
            candidates = [source]
        elif source.is_dir():
            stem = f"{scenario.get('model')}_{scenario.get('pathway')}"
            candidates = sorted(
                candidate
                for candidate in source.glob(f"{stem}.*")
                if candidate.is_file()
            )
        signature = tuple(
            (
                str(candidate.resolve()),
                candidate.stat().st_size,
                candidate.stat().st_mtime_ns,
            )
            for candidate in candidates
        )
        cache = getattr(self, "_validation_iam_fingerprints", None)
        if cache is None:
            cache = self._validation_iam_fingerprints = {}
        source_hashes = cache.get(signature)
        if source_hashes is None:
            source_hashes = []
            for candidate in candidates:
                digest = hashlib.sha256()
                with candidate.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                source_hashes.append((str(candidate.resolve()), digest.hexdigest()))
            source_hashes = tuple(source_hashes)
            cache[signature] = source_hashes

        payload = {
            "model": scenario.get("model"),
            "pathway": scenario.get("pathway"),
            "year": scenario.get("year"),
            "sources": source_hashes,
            "external": external,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def _certify_scenario_store(
        self,
        scenario: dict,
        store: InventoryStore,
        *,
        intent: ValidationIntent | None = None,
        raise_on_error: bool = True,
        exhaustive: bool = False,
    ) -> ValidationCertificate | None:
        """Certify one exact store generation and attach its immutable report.

        Production updates combine the low-cost sector contracts recorded while
        transformations still own their targets.  A complete graph traversal is
        reserved for explicit diagnostics, inventories without sector coverage,
        and stores mutated after their production certificate was issued.
        """

        if not getattr(self, "_validation_enabled", False):
            return None
        started = time.perf_counter()
        sector_phases = []
        for phase_payload in scenario.get("_validation_phase_results", ()):
            try:
                phase = ValidationPhaseResult.from_dict(phase_payload)
            except (KeyError, TypeError, ValueError):
                continue
            if phase.kind == "sector":
                sector_phases.append(phase)
        incremental = bool(sector_phases) and not exhaustive
        if intent is None:
            intent = self._scenario_validation_intent(
                scenario, store=None if incremental else store
            )
        validator = InventoryGraphValidator(
            store,
            scenario_identity=self._scenario_identity(scenario),
            source_fingerprint=self._validation_source_fingerprint(),
            iam_fingerprint=self._validation_iam_fingerprint(scenario),
            system_model=getattr(self, "system_model", "cutoff"),
            version=getattr(self, "version", "unknown"),
            intent=intent,
            baseline_cycles=(
                ()
                if incremental
                else (
                    intent.baseline_cycles
                    if intent is not None and intent.baseline_cycles
                    else self._ensure_validation_baseline_cycles()
                )
            ),
        )

        if not incremental:
            certificate = validator.certify(raise_on_error=False)
            report = certificate.report
            for phase in sector_phases:
                report = report.with_phase(phase)
            certificate = replace(certificate, report=report)
        else:
            checked = sum(
                result.checked_object_count
                for phase in sector_phases
                for result in phase.rule_results
            )
            coverage = ValidationRuleResult(
                rule_id="GRAPH.INCREMENTAL_SCOPE",
                severity="error",
                applicability="applicable",
                checked_object_count=checked,
                expected="all applied sector contracts pass",
                actual={
                    "mode": "incremental",
                    "sector_phases": [phase.phase_id for phase in sector_phases],
                },
            )
            graph_phase = ValidationPhaseResult(
                phase_id="graph:incremental",
                kind="graph",
                rule_results=(coverage,),
                elapsed_seconds=time.perf_counter() - started,
            )
            incremental_key = hashlib.sha256(
                f"{validator.cache_key}:incremental-v1".encode("utf-8")
            ).hexdigest()
            report = ValidationReport(
                scenario_identity=self._scenario_identity(scenario),
                store_generation=store.generation,
                ruleset_version=VALIDATION_RULESET_VERSION,
                certificate_key=incremental_key,
                rule_results=(coverage,),
                phase_results=(graph_phase, *sector_phases),
            )
            certificate = ValidationCertificate(
                cache_key=incremental_key,
                store_generation=store.generation,
                ruleset_version=VALIDATION_RULESET_VERSION,
                scenario_identity=self._scenario_identity(scenario),
                source_fingerprint=validator.source_fingerprint,
                iam_fingerprint=validator.iam_fingerprint,
                system_model=validator.system_model,
                version=validator.version,
                report=report,
            )
        underlying = getattr(store, "_store", store)
        underlying._validation_certificate_payload = certificate.to_dict()
        scenario["_validation_report"] = report.to_dict()
        reports = getattr(self, "_validation_reports", None)
        if reports is None:
            reports = self._validation_reports = {}
        previous = reports.get(self._scenario_identity(scenario))
        if previous is not None:
            for phase in previous.phase_results:
                if phase.kind == "export":
                    report = report.with_phase(phase)
        reports[self._scenario_identity(scenario)] = report
        for warning in report.warnings:
            logger.warning("%s: %s", warning.rule_id, warning.message)
        if raise_on_error:
            report.raise_for_errors()
        return certificate

    def _scenario_validation_intent(
        self, scenario: dict, *, store: InventoryStore | None = None
    ) -> ValidationIntent | None:
        """Combine applied sector declarations for one full-graph pass."""

        payloads = scenario.get("_validation_intents", {})
        intents = []
        for payload in payloads.values() if isinstance(payloads, dict) else ():
            try:
                intents.append(ValidationIntent.from_dict(payload))
            except (KeyError, TypeError, ValueError):
                continue
        if not intents:
            return None
        applicable = [
            intent for intent in intents if intent.applicability == "applicable"
        ]
        affected_keys = frozenset(
            key for intent in applicable for key in intent.affected_activity_keys
        )
        current_affected_keys = affected_keys
        affected_ids = frozenset(
            activity_id
            for intent in applicable
            for activity_id in intent.affected_activity_ids
        )
        intended_suppliers = {
            target: entries
            for intent in applicable
            for target, entries in intent.intended_suppliers.items()
        }
        transformations = tuple(intent.transformation for intent in applicable)
        baseline_fingerprints = dict(
            scenario.get("_validation_baseline_fingerprints", {})
        )
        target_semantics = {(key[0], key[1]) for key in affected_keys}
        baseline_semantic_targets = {
            key for key in baseline_fingerprints if (key[0], key[1]) in target_semantics
        }
        affected_keys = frozenset(set(affected_keys) | baseline_semantic_targets)
        resolved_target_count = None
        if store is not None:
            underlying = getattr(store, "_store", store)
            iterator = getattr(underlying, "_iter_storage_activities", None)
            if iterator is not None:
                current_keys_by_id = {
                    activity_id: (
                        payload.get("name"),
                        payload.get("reference product", payload.get("product")),
                        payload.get("location"),
                    )
                    for activity_id, payload, _ in iterator()
                }
                current_keys = set(current_keys_by_id.values())
                resolved_target_count = sum(
                    key in current_keys for key in affected_keys
                ) + sum(
                    activity_id in current_keys_by_id
                    and current_keys_by_id[activity_id] not in affected_keys
                    for activity_id in affected_ids
                )
        algorithm = None
        if {"electricity", "fuels"}.intersection(transformations):
            algorithm = (
                "marginal mix"
                if self.system_model == "consequential"
                else "average production-volume mix"
            )
        return ValidationIntent(
            transformation="scenario",
            targeted=False,
            scope_complete=bool(applicable)
            and all(intent.scope_complete for intent in applicable),
            affected_activity_ids=affected_ids,
            affected_activity_keys=affected_keys,
            allowed_added_keys=frozenset(
                key for intent in applicable for key in intent.allowed_added_keys
            ),
            allowed_removed_keys=frozenset(baseline_semantic_targets),
            baseline_fingerprints=baseline_fingerprints,
            expected_match_count=(
                resolved_target_count
                if resolved_target_count is not None
                else len(affected_ids) + len(current_affected_keys)
            ),
            expected_regions=tuple(
                sorted(
                    {
                        region
                        for intent in applicable
                        for region in intent.expected_regions
                    }
                )
            ),
            expected_technologies=tuple(
                sorted(
                    {
                        technology
                        for intent in applicable
                        for technology in intent.expected_technologies
                    }
                )
            ),
            algorithm=algorithm,
            intended_suppliers=intended_suppliers,
            computed_target_values={
                "transformations": transformations,
                "sector_target_counts": {
                    intent.transformation: len(intent.affected_activity_keys)
                    for intent in applicable
                },
            },
            baseline_cycles=frozenset(
                frozenset(tuple(key) for key in cycle)
                for cycle in scenario.get("_validation_baseline_cycles", ())
            ),
            tolerance=min((intent.tolerance for intent in applicable), default=1e-9),
        )

    def _record_export_validation_phase(
        self, scenario_definition: dict, runtime_scenario: dict, exporter: str
    ) -> None:
        """Keep exporter schema checks in memory without changing checkpoints."""

        payload = runtime_scenario.pop("_export_validation_phase", None)
        if not isinstance(payload, dict):
            return
        phase = ValidationPhaseResult.from_dict(payload)
        phase = replace(phase, phase_id=f"export:{exporter}", kind="export")
        identity = self._scenario_identity(scenario_definition)
        reports = getattr(self, "_validation_reports", None)
        if reports is None:
            reports = self._validation_reports = {}
        report = reports.get(identity)
        if report is None:
            stored = scenario_definition.get("_validation_report")
            if isinstance(stored, dict):
                report = ValidationReport.from_dict(stored)
        if report is not None:
            reports[identity] = report.with_phase(phase)

    def _handle_export_validation_error(
        self,
        scenario_definition: dict,
        error: PremiseValidationError,
        exporter: str,
        runtime_scenario: dict | None = None,
    ) -> None:
        """Attach an exporter phase and diagnose the invalid runtime inventory."""

        identity = self._scenario_identity(scenario_definition)
        reports = getattr(self, "_validation_reports", None)
        if reports is None:
            reports = self._validation_reports = {}
        report = reports.get(identity)
        if report is None:
            stored = scenario_definition.get("_validation_report")
            if isinstance(stored, dict):
                report = ValidationReport.from_dict(stored)
        if report is None:
            report = error.report
        for phase in error.report.phase_results:
            if phase.kind == "export":
                report = report.with_phase(
                    replace(phase, phase_id=f"export:{exporter}", kind="export")
                )
        reports[identity] = report
        error.report = report
        diagnostic_scenario = runtime_scenario or scenario_definition
        database = diagnostic_scenario.get("database")
        if database is not None:
            store = create_inventory_store(
                database,
                backend=diagnostic_scenario.get("_inventory_backend")
                or getattr(self, "inventory_backend", "compact"),
                scenario_identity=identity,
                take_ownership=False,
            )
        else:
            store = self._ensure_scenario_store(scenario_definition)
        self._generate_validation_diagnostic(error, diagnostic_scenario, store)

    def _try_automatic_failed_report(self) -> None:
        """Best-effort report for a non-framework exporter anomaly."""

        if not getattr(self, "generate_reports", False):
            return
        try:
            self._generate_change_report(status="failed")
        except Exception as reporting_error:
            logger.warning(
                "Failed to generate exporter diagnostic report: %s",
                reporting_error,
                exc_info=True,
            )

    def _ensure_semantic_certification(self, scenario: dict) -> ValidationReport | None:
        """Reuse certification or recertify after an inventory mutation."""

        if not getattr(self, "_validation_enabled", False):
            return None
        stored_report = scenario.get("_validation_report")
        store = scenario.get("_inventory_store")
        report = None
        if isinstance(stored_report, dict):
            try:
                report = ValidationReport.from_dict(stored_report)
            except (KeyError, TypeError, ValueError):
                report = None
            if (
                report is not None
                and report.ruleset_version != VALIDATION_RULESET_VERSION
            ):
                report = None
            if report is not None and (
                store is None or report.store_generation == store.generation
            ):
                try:
                    report.raise_for_errors()
                except PremiseValidationError as error:
                    diagnostic_store = store or self._ensure_scenario_store(scenario)
                    self._generate_validation_diagnostic(
                        error, scenario, diagnostic_store
                    )
                    raise
                return report.with_reuse(True)
        store = self._ensure_scenario_store(scenario)
        exhaustive = report is not None and report.store_generation != store.generation
        try:
            certificate = self._certify_scenario_store(
                scenario, store, exhaustive=exhaustive
            )
        except PremiseValidationError as error:
            self._generate_validation_diagnostic(error, scenario, store)
            raise
        return certificate.report if certificate is not None else None

    def get_validation_report(
        self, scenario: int = 0, *, exhaustive: bool = False
    ) -> ValidationReport:
        """Return the completed immutable report for a scenario.

        If a writable store has changed since its last certificate, this method
        performs a complete new read-only pass.  Set ``exhaustive=True`` to run
        that full graph pass explicitly for diagnostics; ordinary production
        updates return the incremental sector certificate required by exports.
        """

        if not isinstance(scenario, int):
            raise TypeError("scenario must be an integer scenario position.")
        if scenario < 0 or scenario >= len(self.scenarios):
            raise IndexError(
                f"scenario position {scenario} is outside 0..{len(self.scenarios) - 1}."
            )
        definition = self.scenarios[scenario]
        if exhaustive:
            store = self._ensure_scenario_store(definition)
            certificate = self._certify_scenario_store(
                definition, store, exhaustive=True
            )
            report = certificate.report if certificate is not None else None
        else:
            report = self._ensure_semantic_certification(definition)
        if report is None:
            # The accessor never creates a public validation-off switch.
            store = self._ensure_scenario_store(definition)
            certificate = InventoryGraphValidator(
                store,
                scenario_identity=self._scenario_identity(definition),
                source_fingerprint=self._validation_source_fingerprint(),
                iam_fingerprint=self._validation_iam_fingerprint(definition),
                system_model=getattr(self, "system_model", "cutoff"),
                version=getattr(self, "version", "unknown"),
            ).certify(raise_on_error=False)
            definition["_validation_report"] = certificate.report.to_dict()
            report = certificate.report
        cached = getattr(self, "_validation_reports", {}).get(
            self._scenario_identity(definition)
        )
        if cached is not None:
            for phase in cached.phase_results:
                if phase.kind == "export":
                    report = report.with_phase(phase)
        return report

    def _attach_shared_geography_cache(self, runtime_scenario: dict) -> None:
        """Share immutable geographic topology across compact scenario-years."""

        if getattr(self, "inventory_backend", "legacy") != "compact":
            return
        topology_key = self._geography_topology_key(runtime_scenario)
        shared_geography_caches = getattr(self, "_shared_geography_caches", None)
        if shared_geography_caches is None:
            shared_geography_caches = self._shared_geography_caches = {}
        shared_cache = shared_geography_caches.setdefault(
            topology_key,
            {
                _SCENARIO_GIS_CACHE_KEY: {},
                _SCENARIO_ROW_CACHE_KEY: {},
            },
        )
        scenario_cache = runtime_scenario.get("cache")
        if not isinstance(scenario_cache, dict):
            scenario_cache = {}
            runtime_scenario["cache"] = scenario_cache
        scenario_cache[_SCENARIO_GIS_CACHE_KEY] = shared_cache[_SCENARIO_GIS_CACHE_KEY]
        scenario_cache[_SCENARIO_ROW_CACHE_KEY] = shared_cache[_SCENARIO_ROW_CACHE_KEY]

    @staticmethod
    def _geography_topology_key(scenario: dict) -> tuple[str, tuple]:
        regions = tuple(getattr(scenario.get("iam data"), "regions", ()))
        return scenario["model"], regions

    def _release_shared_geography_cache(
        self, scenario: dict, scenario_position: int
    ) -> None:
        """Release a topology cache after its final matching scenario."""

        if getattr(self, "inventory_backend", "legacy") != "compact":
            return
        topology_key = self._geography_topology_key(scenario)
        if any(
            self._geography_topology_key(future) == topology_key
            for future in self.scenarios[scenario_position + 1 :]
        ):
            return
        shared_geography_caches = getattr(self, "_shared_geography_caches", None)
        if shared_geography_caches is not None:
            shared_geography_caches.pop(topology_key, None)

    @contextmanager
    def _reuse_update_source(self):
        """Retain one pristine legacy source only for the duration of update.

        Each scenario still receives an isolated working graph. Keeping this
        private reference avoids rebuilding the source from caches for every
        scenario-year, and releases it before update returns or raises.
        """
        self._update_source_store = None
        try:
            yield
        finally:
            del self._update_source_store

    def _load_scenario_database_for_update(
        self, scenario: dict, scenario_position: int
    ) -> dict:
        if not hasattr(self, "inventory_backend") and not hasattr(
            self, "_source_inventory_store"
        ):
            if (
                scenario_position == 0
                and self._database is not None
                and self._database_is_complete
                and self._can_reload_original_database()
            ):
                if getattr(
                    self, "_reload_original_database_from_cache_for_update", False
                ):
                    self._database = None
                    scenario["database"] = self._load_original_database()
                    self._reload_original_database_from_cache_for_update = False
                    return scenario
                scenario["database"] = self._database
                self._database = None
                return scenario
            scenario["database"] = self._load_original_database()
            return scenario

        runtime_scenario = scenario.copy()
        runtime_scenario.pop("_inventory_store", None)
        runtime_scenario.pop("_inventory_export_handoff", None)
        runtime_scenario.pop("_inventory_checkpoint", None)
        self._attach_shared_geography_cache(runtime_scenario)
        store = self._ensure_scenario_store(scenario, materialize_source=True)
        can_transfer_source = self._can_reload_original_database()
        has_mapping = bool(runtime_scenario.get("mapping"))
        activity_ids = tuple(store.iter_activity_ids()) if has_mapping else ()
        if isinstance(store, CompactInventoryStore) and can_transfer_source:
            working_copy = store._checkout_materialized(discard_shared_state=True)
        else:
            working_copy = IndexedInventoryList(
                store.materialize(restore_metadata=True),
                inventory_backend=store.backend_name,
            )
        runtime_scenario["_inventory_backend"] = store.backend_name
        runtime_scenario["_inventory_working_copy"] = working_copy
        if has_mapping:
            runtime_scenario["mapping"] = _hydrate_scenario_mapping(
                runtime_scenario["mapping"],
                dict(zip(activity_ids, working_copy)),
            )
        # A reloadable source store only inflates the high-water mark after its
        # graph has been transferred to a scenario. The next scenario can take
        # ownership of a fresh graph from the versioned source/inventory caches.
        scenario.pop("_inventory_store", None)
        if can_transfer_source and not getattr(store, "_shares_source_storage", False):
            self._source_inventory_store = None
            del store
            clear_runtime_caches()
            gc.collect()
        return runtime_scenario

    def _store_updated_scenario(
        self,
        scenario_definition: dict,
        runtime_scenario: dict,
        *,
        persist: bool,
    ) -> InventoryStore:
        store = runtime_scenario.pop("_inventory_store", None)
        if store is None:
            database = runtime_scenario.pop("_inventory_working_copy")
            store = create_inventory_store(
                database,
                backend=runtime_scenario.get("_inventory_backend")
                or getattr(self, "inventory_backend", "compact"),
                scenario_identity=self._scenario_identity(runtime_scenario),
                take_ownership=True,
                scenario_cache_compatibility=not persist,
                build_indexes=not persist,
            )
        else:
            runtime_scenario.pop("_inventory_working_copy", None)
        if persist:
            store._scenario_cache_compatibility = True
        provenance_payload = self._get_provenance_collector().payload_for(
            self._scenario_identity(runtime_scenario)
        )
        runtime_scenario["_provenance"] = provenance_payload
        underlying = getattr(store, "_store", store)
        underlying._provenance_payload = provenance_payload
        # Certification deliberately happens before the scenario definition is
        # replaced and before a checkpoint can be written.  Invalid builds
        # therefore leave the last known-good scenario state untouched.
        try:
            self._certify_scenario_store(runtime_scenario, store)
        except PremiseValidationError as error:
            self._generate_validation_diagnostic(error, runtime_scenario, store)
            raise
        runtime_scenario.pop("_inventory_backend", None)
        runtime_scenario.pop("_inventory_defer_indexes", None)
        runtime_scenario.pop("_validation_baseline_fingerprints", None)
        runtime_scenario.pop("_validation_baseline_cycles", None)
        scenario_definition.clear()
        scenario_definition.update(runtime_scenario)
        if persist:
            checkpoint = DIR_CACHED_FILES / f"{uuid.uuid4().hex}.inventory-store"
            checkpoint = store.checkpoint(checkpoint)
            scenario_definition["_inventory_checkpoint"] = checkpoint
            if isinstance(store, CompactInventoryStore) and scenario_definition.get(
                "mapping"
            ):
                scenario_definition["mapping"] = _compact_scenario_mapping(
                    scenario_definition["mapping"], store, checkpoint
                )
            if self._can_retain_export_handoff(store):
                # The private store was canonicalized before certification and
                # checkpointing, so it is the exact single-use export handoff.
                for scenario in self.scenarios:
                    if scenario is not scenario_definition:
                        scenario.pop("_inventory_export_handoff", None)
                scenario_definition["_inventory_export_handoff"] = store
        else:
            scenario_definition["_inventory_store"] = store
        return store

    def _can_retain_export_handoff(self, store: InventoryStore) -> bool:
        """Allow one private single-use checkpoint-to-export handoff."""

        if not isinstance(store, CompactInventoryStore):
            return False
        return getattr(store, "_state", None) is not None

    @staticmethod
    def _clear_scenario_runtime_state(scenario: dict) -> None:
        scenario.pop("_transport_index_ready", None)

        if "cache" in scenario:
            scenario["cache"] = {}

        if "index" in scenario:
            scenario["index"] = {}

        clear_runtime_caches()
        gc.collect()

    def __import_additional_inventories(
        self, data_package: [datapackage.DataPackage, list]
    ) -> List[dict]:
        """
        This method will trigger the import of a number of inventories
        and merge them into the database dictionary.

        :param data_package: datapackage.DataPackage or list of file paths
        :return: list of dictionaries

        """
        print("\n//////////////// IMPORTING USER-DEFINED INVENTORIES ////////////////")

        data = []

        if isinstance(data_package, list):
            # this is a list of file paths
            for file_path in data_package:
                additional = AdditionalInventory(
                    database=self._database,
                    version_in=file_path["ecoinvent version"],
                    version_out=self.version,
                    path=file_path["filepath"],
                    system_model=self.system_model,
                )
                additional.prepare_inventory()
                data.extend(additional.merge_inventory())

        elif isinstance(data_package, datapackage.DataPackage):
            if data_package.get_resource("inventories"):
                additional = AdditionalInventory(
                    database=self._database,
                    version_in=data_package.descriptor["ecoinvent"]["version"],
                    version_out=self.version,
                    path=data_package.get_resource("inventories").source,
                    system_model=self.system_model,
                )
                data.extend(additional.merge_inventory())
        else:
            raise TypeError("Unknown data type for datapackage.")

        return data

    def update(
        self,
        sectors: [str, list, None] = None,
        *,
        persist: bool = True,
    ) -> None:
        """
        Update a specific sector by name.

        :param persist: checkpoint the scenario store after the update. Set to
            ``False`` to retain it in memory for immediate inspection or export.
        """
        self.sector_update_methods = {
            "biomass": {
                "func": _update_biomass,
                "args": (self.version, self.system_model),
            },
            "electricity": {
                "func": _update_electricity,
                "args": (self.version, self.system_model, self.use_absolute_efficiency),
            },
            "cement": {
                "func": _update_cement,
                "args": (self.version, self.system_model),
            },
            "steel": {"func": _update_steel, "args": (self.version, self.system_model)},
            "fuels": {"func": _update_fuels, "args": (self.version, self.system_model)},
            "renewable": {
                "func": _update_wind_turbines,
                "args": (self.version, self.system_model),
            },
            "metals": {
                "func": _update_metals,
                "args": (self.version, self.system_model),
            },
            "mining": {
                "func": _update_mining,
                "args": (self.version, self.system_model),
            },
            "heat": {"func": _update_heat, "args": (self.version, self.system_model)},
            "cdr": {"func": _update_cdr, "args": (self.version, self.system_model)},
            "battery": {
                "func": _update_battery,
                "args": (self.version, self.system_model),
            },
            "cars": {
                "func": _update_vehicles,
                "args": ("car", self.version, self.system_model),
            },
            "two_wheelers": {
                "func": _update_vehicles,
                "args": ("two-wheeler", self.version, self.system_model),
            },
            "trucks": {
                "func": _update_vehicles,
                "args": ("truck", self.version, self.system_model),
            },
            "ships": {
                "func": _update_vehicles,
                "args": ("ship", self.version, self.system_model),
            },
            "buses": {
                "func": _update_vehicles,
                "args": ("bus", self.version, self.system_model),
            },
            "trains": {
                "func": _update_vehicles,
                "args": ("train", self.version, self.system_model),
            },
            "final energy": {
                "func": _update_final_energy,
                "args": (self.version, self.system_model),
            },
            "external": {
                "func": _update_external_scenarios,
                "args": (
                    self.version,
                    self.system_model,
                ),
            },
            "emissions": {
                "func": _update_emissions,
                "args": (self.version, self.system_model, self.gains_scenario),
            },
        }

        if isinstance(sectors, str):
            description = f"Processing scenarios for sector '{sectors}'"
            sectors = [
                sectors,
            ]
        elif isinstance(sectors, list):
            description = f"Processing scenarios for {len(sectors)} sectors"
        elif sectors is None:
            description = "Processing scenarios for all sectors"
            sectors = [s for s in list(self.sector_update_methods.keys())]

        if not isinstance(sectors, list) or not all(
            isinstance(item, str) for item in sectors
        ):
            raise TypeError("sectors must be a string or a list of strings.")
        unknown_sectors = [
            item for item in sectors if item not in self.sector_update_methods
        ]
        if unknown_sectors:
            raise ValueError(f"Unknown resource name(s): {unknown_sectors}")

        with (
            self._reuse_update_source(),
            tqdm(total=len(self.scenarios), desc=description, ncols=70) as pbar_outer,
        ):
            for position, scenario_definition in enumerate(self.scenarios):
                # Once another scenario starts updating, the previous scenario
                # remains checkpoint-backed and must not keep a second complete
                # store resident alongside the new working graph.
                for other_scenario in self.scenarios:
                    if other_scenario is not scenario_definition:
                        other_scenario.pop("_inventory_export_handoff", None)
                gc.collect()
                scenario = self._load_scenario_database_for_update(
                    scenario=scenario_definition, scenario_position=position
                )
                collector = self._get_provenance_collector()

                for sector in sectors:
                    if sector in scenario.get("applied functions", []):
                        print(
                            f"Function to update {sector} already applied to scenario."
                        )
                        continue

                    if sector not in {
                        "cars",
                        "two_wheelers",
                        "trucks",
                        "ships",
                        "buses",
                        "trains",
                    }:
                        scenario.pop("_transport_index_ready", None)

                    # Prepare the function and arguments
                    update_func = self.sector_update_methods[sector]["func"]
                    fixed_args = self.sector_update_methods[sector]["args"]
                    if sector == "emissions" and "_inventory_store" not in scenario:
                        database = scenario.pop("_inventory_working_copy")
                        # These indexes are neither serialized nor used by
                        # emissions. Persisted stores are reopened with their
                        # own indexes; diagnostics still build them on demand.
                        scenario["_inventory_defer_indexes"] = persist
                        scenario["_inventory_store"] = create_inventory_store(
                            database,
                            backend=scenario.get("_inventory_backend")
                            or getattr(self, "inventory_backend", "compact"),
                            scenario_identity=self._scenario_identity(scenario),
                            take_ownership=True,
                            scenario_cache_compatibility=not persist,
                            build_indexes=not persist,
                        )
                    with collector.session(self._scenario_identity(scenario), sector):
                        scenario = update_func(scenario, *fixed_args)
                    contract_phase = validate_sector_contract(scenario, sector)
                    if contract_phase.errors:
                        try:
                            ValidationReport(
                                scenario_identity=self._scenario_identity(scenario),
                                store_generation=0,
                                ruleset_version=VALIDATION_RULESET_VERSION,
                                certificate_key=f"sector:{sector}:incremental",
                                rule_results=contract_phase.rule_results,
                                phase_results=(contract_phase,),
                            ).raise_for_errors()
                        except PremiseValidationError as error:
                            self._generate_validation_diagnostic(error, scenario)
                            raise

                    if "applied functions" not in scenario:
                        scenario["applied functions"] = []
                    scenario["applied functions"].append(sector)

                self._clear_scenario_runtime_state(scenario)
                self._store_updated_scenario(
                    scenario_definition,
                    scenario,
                    persist=persist,
                )
                self._release_shared_geography_cache(scenario_definition, position)
                # Manually update the outer progress bar after each sector is completed
                pbar_outer.update()

        print("Done!\n")

    def update_and_write(
        self,
        name: [str, List[str]] = None,
        sectors: [str, list, None] = None,
    ) -> None:
        """Update scenarios in memory and immediately write them to Brightway.

        Keeping the scenario stores in memory avoids the historical scenario
        dump/reload cycle between :meth:`update` and
        :meth:`write_db_to_brightway`.
        """

        self.update(sectors=sectors, persist=False)
        self.write_db_to_brightway(name=name)

    def write_superstructure_db_to_brightway(
        self,
        name: str = f"super_db_{datetime.now().strftime('%d-%m-%Y')}",
        filepath: str = None,
        file_format: str = "csv",
        preserve_original_column: bool = False,
    ) -> None:
        """
        Register a super-structure database,
        according to https://github.com/dgdekoning/brightway-superstructure
        :param name: name of the super-structure database
        :param filepath: filepath of the "scenarios difference file"
        :param file_format: format of the "scenarios difference file" export. Can be "excel", "csv" or "feather".
        :param preserve_original_column: if True, the original column names are preserved in the super-structure database.
        :return: filepath of the "scenarios difference file"
        """

        self._prepare_superstructure_export(
            name=name,
            filepath=filepath,
            file_format=file_format,
            preserve_original_column=preserve_original_column,
        )

        write_brightway_database(
            data=self._database,
            name=name,
            fast=True,
            check_internal=False,
            metadata=database_metadata(
                self.scenarios,
                version=getattr(self, "version", None),
                system_model=getattr(self, "system_model", None),
            ),
        )

        self._finalize_superstructure_export()

    def write_scenario_array_db_to_brightway(
        self,
        name: str = f"scenario_array_db_{datetime.now():%d-%m-%Y}",
        filepath: str | Path | None = None,
    ) -> Path:
        """Write a union database and deterministic Brightway scenario arrays.

        ``filepath`` is the complete destination ZIP path. The returned package
        is project-specific because its matrix indices refer to IDs assigned
        when ``name`` is written in the active Brightway project.
        """

        version = bw2data.__version__
        major_version = (
            int(version[0])
            if isinstance(version, (tuple, list))
            else Version(str(version)).major
        )
        if major_version < 4:
            raise NotImplementedError(
                "Scenario-array export requires modern Brightway (bw2data >= 4)."
            )

        self._validate_superstructure_export_prerequisites()

        destination = Path(filepath).expanduser() if filepath is not None else None
        if destination is not None and destination.suffix.lower() != ".zip":
            raise ValueError(
                "Scenario-array filepath must be the complete destination path "
                "with a '.zip' suffix."
            )

        scenario_labels = create_scenario_list(self.scenarios)
        duplicates = sorted(
            {label for label in scenario_labels if scenario_labels.count(label) > 1}
        )
        if duplicates:
            raise ValueError(
                "Scenario labels must be unique for scenario-array export. "
                f"Duplicate label(s): {duplicates}."
            )

        dependencies = _load_scenario_array_dependencies()
        bw_processing = dependencies[0]
        if destination is None:
            sanitized_name = bw_processing.clean_datapackage_name(name) or "database"
            destination = (
                Path.cwd()
                / "export"
                / "scenario arrays"
                / f"scenario_array_{sanitized_name}.zip"
            )

        scenario_labels, dataframe = self._prepare_superstructure_export(
            name=name,
            scenario_array=True,
            prerequisites_validated=True,
        )

        write_brightway_database(
            data=self._database,
            name=name,
            fast=True,
            check_internal=False,
            metadata=database_metadata(
                self.scenarios,
                version=getattr(self, "version", None),
                system_model=getattr(self, "system_model", None),
            ),
        )

        ordered_labels = ["original", *scenario_labels]
        project_name = getattr(bw2data.projects, "current", None)
        metadata = {
            "database_name": name,
            "brightway_project": project_name,
            "source_database": getattr(self, "source", None),
            "ecoinvent_version": self.version,
            "premise_version": ".".join(map(str, __version__)),
            "scenario_count": len(ordered_labels),
            "scenario_labels": ordered_labels,
        }
        destination = _write_scenario_array_datapackage(
            dataframe=dataframe,
            scenario_labels=ordered_labels,
            filepath=destination,
            name=name,
            metadata=metadata,
            dependencies=dependencies,
        )

        self._finalize_superstructure_export()
        return destination

    def _prepare_superstructure_export(
        self,
        *,
        name: str,
        filepath: str | Path | None = None,
        file_format: str = "csv",
        preserve_original_column: bool = False,
        scenario_array: bool = False,
        prerequisites_validated: bool = False,
    ) -> tuple[list[str], object]:
        """Run the common preparation path for both superstructure exporters."""

        if not prerequisites_validated:
            self._validate_superstructure_export_prerequisites()

        original_database = self._load_original_database()

        prepared_scenarios = []
        for scenario_definition in self.scenarios:
            self._ensure_semantic_certification(scenario_definition)
            scenario = load_database(
                scenario=scenario_definition,
                original_database=original_database,
                load_metadata=True,
            )

            try:
                _prepare_database(
                    scenario=scenario,
                    db_name=name,
                    original_database=original_database,
                    biosphere_name=self.biosphere_name,
                    version=self.version,
                )
            except PremiseValidationError as error:
                self._handle_export_validation_error(
                    scenario_definition,
                    error,
                    "scenario-array" if scenario_array else "superstructure",
                    scenario,
                )
                raise
            except ValueError:
                self._try_automatic_failed_report()
                raise ValueError(
                    "The database is not ready for export: MAJOR anomalies found. Check the change report."
                )
            self._record_export_validation_phase(
                scenario_definition,
                scenario,
                "scenario-array" if scenario_array else "superstructure",
            )
            prepared_scenarios.append(scenario)

        scenario_labels = create_scenario_list(self.scenarios)
        scenario_payloads = (
            prepared_scenarios
            if getattr(self, "_inventory_api_active", False)
            else self.scenarios
        )
        dataframe = None
        if scenario_array:
            self._database, dataframe = _build_superstructure_db(
                origin_db=original_database,
                scenarios=scenario_payloads,
                db_name=name,
                biosphere_name=self.biosphere_name,
                version=self.version,
                scenario_list=scenario_labels,
            )
        else:
            self._database = generate_superstructure_db(
                origin_db=original_database,
                scenarios=scenario_payloads,
                db_name=name,
                biosphere_name=self.biosphere_name,
                filepath=filepath,
                version=self.version,
                file_format=file_format,
                scenario_list=scenario_labels,
                preserve_original_column=preserve_original_column,
            )

        tmp_scenario = self.scenarios[0].copy()
        # The union graph is a new inventory generation and cannot reuse the
        # first constituent scenario's semantic certificate.
        tmp_scenario.pop("_validation_report", None)
        tmp_scenario["database"] = self._database
        additional_regions = sorted(
            {
                region
                for scenario in self.scenarios
                for region in getattr(scenario.get("iam data"), "regions", [])
            }
        )
        if additional_regions:
            tmp_scenario["additional valid regions"] = additional_regions

        try:
            self._database = prepare_db_for_export(
                scenario=tmp_scenario,
                name=name,
                original_database=original_database,
                biosphere_name=self.biosphere_name,
                version=self.version,
            )
        except PremiseValidationError as error:
            self._handle_export_validation_error(
                self.scenarios[0],
                error,
                "scenario-array-union" if scenario_array else "superstructure-union",
                tmp_scenario,
            )
            raise
        except ValueError:
            self._try_automatic_failed_report()
            raise

        return scenario_labels, dataframe

    def _validate_superstructure_export_prerequisites(self) -> None:
        """Validate prerequisites shared by both superstructure exporters."""

        if len(self.scenarios) < 2:
            raise ValueError(
                "At least two scenarios are needed to "
                "create a super-structure database."
            )

        check_presence_biosphere_database(self.biosphere_name)

    def _finalize_superstructure_export(self) -> None:
        """Generate reports and release scenario export state once."""

        self._run_automatic_reports()

        for scenario in self.scenarios:
            end_of_process(scenario, preserve_applied_functions=True)

        delete_all_pickles()

    def write_db_to_brightway(self, name: [str, List[str]] = None):
        """
        Register the new database into an open brightway project.
        :param name: to give a (list) of custom name(s) to the database.
        Should either be a string if there's only one database to export.
        Or a list of strings if there are several databases.
        :type name: str
        """

        if name:
            if isinstance(name, str):
                name = [name]
            elif isinstance(name, list):
                if not all(isinstance(item, str) for item in name):
                    raise TypeError(
                        "`name` should be a string or a sequence of strings."
                    )
            else:
                raise TypeError("`name` should be a string or a sequence of strings.")
        else:
            name = [
                eidb_label(
                    scenario,
                    version=self.version,
                    system_model=self.system_model,
                )
                for scenario in self.scenarios
            ]

        if len(name) != len(self.scenarios):
            raise ValueError(
                "The number of databases does not match the number of `name` given."
            )

        check_presence_biosphere_database(self.biosphere_name)

        print("Write new database(s) to Brightway.")

        for s, scenario in enumerate(self.scenarios):
            self._ensure_semantic_certification(scenario)
            can_use_fast_export = (
                scenario.get("_inventory_store") is not None
                or scenario.get("_inventory_export_handoff") is not None
                or "_inventory_checkpoint" in scenario
                or scenario.get("database") is not None
                or "database filepath" in scenario
            )

            if can_use_fast_export:
                consume_compact = (
                    "_inventory_checkpoint" in scenario or not self.generate_reports
                )
                scenario = load_database(
                    scenario=scenario,
                    original_database=[],
                    load_metadata=True,
                    warning=False,
                    consume_compact=consume_compact,
                )
                if consume_compact:
                    # The runtime exporter owns the checked-out graph. Durable
                    # checkpoints remain available for reports and later
                    # exporters without retaining an emptied store object.
                    self.scenarios[s].pop("_inventory_store", None)
                    self.scenarios[s].pop("_inventory_export_handoff", None)
                try:
                    scenario["database"] = prepare_db_for_fast_export(
                        scenario=scenario,
                        name=name[s],
                        biosphere_name=self.biosphere_name,
                        version=self.version,
                    )
                except PremiseValidationError as error:
                    self._handle_export_validation_error(
                        self.scenarios[s], error, "brightway", scenario
                    )
                    raise
                except ValueError:
                    self._try_automatic_failed_report()
                    raise ValueError(
                        "The database is not ready for export: MAJOR anomalies found. Check the change report."
                    )

                self._record_export_validation_phase(
                    self.scenarios[s], scenario, "brightway"
                )

                scenario["database name"] = name[s]
                write_brightway_database(
                    scenario["database"],
                    name[s],
                    fast=True,
                    # The combined fast-export session has already resolved
                    # and checked every internal provider in the same pass.
                    check_internal=False,
                    metadata=scenario_metadata(
                        scenario,
                        version=getattr(self, "version", None),
                        system_model=getattr(self, "system_model", None),
                    ),
                )
                end_of_process(scenario)
                continue

            original_database = self._load_original_database()
            scenario = load_database(
                scenario=scenario,
                original_database=original_database,
                load_metadata=True,
            )

            try:
                _prepare_database(
                    scenario=scenario,
                    db_name=name[s],
                    original_database=original_database,
                    biosphere_name=self.biosphere_name,
                    version=self.version,
                )
            except PremiseValidationError as error:
                self._handle_export_validation_error(
                    self.scenarios[s], error, "brightway", scenario
                )
                raise
            except ValueError:
                self._try_automatic_failed_report()
                raise ValueError(
                    "The database is not ready for export: MAJOR anomalies found. Check the change report."
                )

            self._record_export_validation_phase(
                self.scenarios[s], scenario, "brightway"
            )

            scenario["database name"] = name[s]
            write_brightway_database(
                scenario["database"],
                name[s],
                metadata=scenario_metadata(
                    scenario,
                    version=getattr(self, "version", None),
                    system_model=getattr(self, "system_model", None),
                ),
            )

            end_of_process(scenario)

        self._run_automatic_reports()
        delete_all_pickles()

    def write_db_to_matrices(self, filepath: str = None):
        """

        Exports the new database as a sparse matrix representation in csv files.

        :param filepath: path provided by the user to store the exported matrices.
            If it is a string, the path is used as main directory from which
            "iam model" / "pathway" / "year" subdirectories will be created.
            If it is a sequence of strings, each string becomes the directory
            under which the set of matrices is saved. If `filepath` is not provided,
            "iam model" / "pathway" / "year" subdirectories are created under
            the working directory.
        :type filepath: str or list

        """

        def scenario_name(scenario):
            name = scenario["pathway"]

            if "external scenarios" in scenario:
                for external in scenario["external scenarios"]:
                    name += f"-{external['scenario']}"
            return name

        if filepath is not None:
            if isinstance(filepath, str):
                filepath = [
                    (Path(filepath) / s["model"] / scenario_name(s) / str(s["year"]))
                    for s in self.scenarios
                ]
            elif isinstance(filepath, list):
                filepath = [Path(f) for f in filepath]
            else:
                raise TypeError(
                    f"Expected a string or a sequence of "
                    f"strings for `filepath`, not {type(filepath)}."
                )
        else:
            filepath = [
                (Path.cwd() / "export" / s["model"] / s["pathway"] / str(s["year"]))
                for s in self.scenarios
            ]

        print("Write new database(s) to matrix.")
        original_database = self._load_original_database()

        for s, scenario in enumerate(self.scenarios):
            self._ensure_semantic_certification(scenario)
            scenario = load_database(
                scenario=scenario,
                original_database=original_database,
                load_metadata=True,
            )

            try:
                scenario = _prepare_database(
                    scenario=scenario,
                    db_name="database",
                    original_database=original_database,
                    biosphere_name=self.biosphere_name,
                    version=self.version,
                )
            except PremiseValidationError as error:
                self._handle_export_validation_error(
                    self.scenarios[s], error, "matrices", scenario
                )
                raise
            except ValueError:
                self._try_automatic_failed_report()
                raise ValueError(
                    "The database is not ready for export: MAJOR anomalies found. Check the change report."
                )

            self._record_export_validation_phase(
                self.scenarios[s], scenario, "matrices"
            )

            Export(
                scenario=scenario,
                filepath=filepath[s],
                version=self.version,
                system_model=self.system_model,
            ).export_db_to_matrices()

            end_of_process(scenario)

        self._run_automatic_reports()
        delete_all_pickles()

    def write_db_to_simapro(self, filepath: str = None):
        """
        Exports database as a CSV file to be imported in Simapro 9.x

        :param filepath: path provided by the user to store the exported import file
        :type filepath: str

        """

        filepath = filepath or Path(Path.cwd() / "export" / "simapro")

        if not os.path.exists(filepath):
            os.makedirs(filepath)

        print("Write Simapro import file(s).")
        original_database = self._load_original_database()

        for scenario_definition in self.scenarios:
            self._ensure_semantic_certification(scenario_definition)
            scenario = load_database(
                scenario=scenario_definition,
                original_database=original_database,
                load_metadata=True,
            )

            try:
                _prepare_database(
                    scenario=scenario,
                    db_name="database",
                    original_database=original_database,
                    biosphere_name=self.biosphere_name,
                    version=self.version,
                )
            except PremiseValidationError as error:
                self._handle_export_validation_error(
                    scenario_definition, error, "simapro", scenario
                )
                raise
            except ValueError:
                self._try_automatic_failed_report()
                raise ValueError(
                    "The database is not ready for export: MAJOR anomalies found. Check the change report."
                )
            self._record_export_validation_phase(
                scenario_definition, scenario, "simapro"
            )
            export = Export(
                scenario=scenario,
                filepath=filepath,
                version=self.version,
                system_model=self.system_model,
            )
            export.export_db_to_simapro()

            if len(export.unmatched_category_flows) > 0:
                scenario["unmatched category flows"] = export.unmatched_category_flows

            end_of_process(scenario)
            # Export keeps a reference to the complete prepared inventory.
            # Release it before loading the next scenario or building reports.
            del export

        if (
            getattr(self, "generate_reports", False)
            and getattr(self, "_source_inventory_store", None) is None
            and getattr(self, "_compact_source_checkpoint", None) is None
            and getattr(self, "_report_source_view", None) is None
        ):
            from ._report_inputs import DeferredReportSource

            # Export preparation reads the original inventory as a reference;
            # the same source can be normalized by the report's readers.
            self._report_source_view = DeferredReportSource(
                original_database, backend=getattr(self, "inventory_backend", "compact")
            )
        del original_database
        self._run_automatic_reports()
        delete_all_pickles()

    def write_db_to_olca(self, filepath: str = None):
        """
        Exports database as a Simapro CSV file to be imported in OpenLCA

        :param filepath: path provided by the user to store the exported import file
        :type filepath: str

        """

        filepath = filepath or Path(Path.cwd() / "export" / "olca")

        if not os.path.exists(filepath):
            os.makedirs(filepath)

        print("Write Simapro import file(s) for OpenLCA.")
        original_database = self._load_original_database()

        for scenario_definition in self.scenarios:
            self._ensure_semantic_certification(scenario_definition)
            scenario = load_database(
                scenario=scenario_definition,
                original_database=original_database,
                load_metadata=True,
            )

            try:
                _prepare_database(
                    scenario=scenario,
                    db_name="database",
                    original_database=original_database,
                    biosphere_name=self.biosphere_name,
                    version=self.version,
                )
            except PremiseValidationError as error:
                self._handle_export_validation_error(
                    scenario_definition, error, "openlca", scenario
                )
                raise
            except ValueError:
                self._try_automatic_failed_report()
                raise ValueError(
                    "The database is not ready for export: MAJOR anomalies found. Check the change report."
                )

            self._record_export_validation_phase(
                scenario_definition, scenario, "openlca"
            )

            Export(
                scenario=scenario,
                filepath=filepath,
                version=self.version,
                system_model=self.system_model,
            ).export_db_to_simapro(olca_compartments=True)

            end_of_process(scenario)

        self._run_automatic_reports()
        delete_all_pickles()

    def write_datapackage(
        self,
        name: str = f"datapackage_{datetime.now().strftime('%d-%m-%Y')} (v.{str(__version__)})",
    ):
        if not isinstance(name, str):
            raise TypeError("`name` should be a string.")

        cached_inventories = self.__find_cached_inventories(self.source)

        if not cached_inventories:
            cache_fp = DIR_CACHED_DB / f"cached_{self.source}_inventories.pickle"
            raise ValueError(f"No cached inventories found at {cache_fp}.")

        original_database = self._load_original_database()

        prepared_scenarios = []
        for scenario_definition in self.scenarios:
            self._ensure_semantic_certification(scenario_definition)
            scenario = load_database(
                scenario=scenario_definition,
                original_database=original_database,
                load_metadata=True,
            )

            try:
                _prepare_database(
                    scenario=scenario,
                    db_name=name,
                    original_database=original_database,
                    biosphere_name=self.biosphere_name,
                    version=self.version,
                )
            except PremiseValidationError as error:
                self._handle_export_validation_error(
                    scenario_definition, error, "datapackage", scenario
                )
                raise
            except ValueError:
                self._try_automatic_failed_report()
                raise ValueError(
                    "The database is not ready for export: MAJOR anomalies found. Check the change report."
                )
            self._record_export_validation_phase(
                scenario_definition, scenario, "datapackage"
            )
            prepared_scenarios.append(scenario)

        list_scenarios = create_scenario_list(self.scenarios)

        df, extra_inventories = generate_scenario_factor_file(
            origin_db=original_database,
            scenarios=prepared_scenarios,
            db_name=name,
            biosphere_name=self.biosphere_name,
            version=self.version,
            scenario_list=list_scenarios,
        )

        for scenario in prepared_scenarios:
            end_of_process(scenario)

        cached_inventories.extend(extra_inventories)

        list_scenarios = ["original"] + list_scenarios

        build_datapackage(
            df=df,
            inventories=cached_inventories,
            list_scenarios=list_scenarios,
            ei_version=self.version,
            name=name,
        )

        self._run_automatic_reports()

    def generate_scenario_report(
        self,
        filepath: [str, Path] = None,
        name: str = f"scenario_report_{datetime.now().strftime('%d-%m-%Y')}.xlsx",
    ):
        """
        Generate a report of the scenarios.
        """

        print("Generate scenario report.")

        if filepath is not None:
            if isinstance(filepath, str):
                filepath = Path(filepath)
        else:
            filepath = Path(Path.cwd() / "export" / "scenario_report")

        if not os.path.exists(filepath):
            os.makedirs(filepath)

        name = Path(name)
        if name.suffix != ".xlsx":
            name = name.with_suffix(".xlsx")

        generate_summary_report(self.scenarios, filepath / name)

        print(f"Report saved under {filepath}.")

    def _report_source_store(self) -> InventoryStore:
        store = getattr(self, "_source_inventory_store", None)
        if store is None:
            checkpoint = getattr(self, "_compact_source_checkpoint", None)
            if checkpoint is not None:
                store = InventoryStore.open_for_reporting(checkpoint)
        if store is None:
            cached_view = getattr(self, "_report_source_view", None)
            if cached_view is not None:
                return cached_view
            try:
                database = self._load_original_database()
            except (AttributeError, OSError, ValueError) as error:
                raise RuntimeError(
                    "The normalized source inventory is unavailable."
                ) from error
            from ._report_inputs import DeferredReportSource

            self._report_source_view = DeferredReportSource(
                database, backend=getattr(self, "inventory_backend", "compact")
            )
            return self._report_source_view
        return ReadOnlyInventoryStore(store)

    def _report_scenarios(
        self,
        override: tuple[dict, InventoryStore, ValidationReport] | None = None,
    ) -> tuple[ReportScenario, ...]:
        override_identity = (
            self._scenario_identity(override[0]) if override is not None else None
        )
        report_scenarios = []
        pending = []
        with ThreadPoolExecutor(
            max_workers=min(4, max(1, len(self.scenarios)))
        ) as readers:
            for definition in self.scenarios:
                identity = self._scenario_identity(definition)
                if override is not None and identity == override_identity:
                    runtime, store, report = override
                    pending.append((identity, runtime, report, store, None))
                    continue
                if not definition.get("applied functions"):
                    continue
                # Certification and resident-store ownership remain ordered on
                # the calling thread; immutable checkpoint reads can overlap.
                report = self._ensure_semantic_certification(definition)
                cached = getattr(self, "_validation_reports", {}).get(identity)
                if cached is not None:
                    report = cached
                checkpoint = definition.get("_inventory_checkpoint")
                if (
                    definition.get("_inventory_store") is None
                    and checkpoint is not None
                ):
                    future = readers.submit(
                        InventoryStore.open_for_reporting, checkpoint
                    )
                    pending.append((identity, definition, report, None, future))
                else:
                    store = self._ensure_scenario_store(definition)
                    pending.append((identity, definition, report, store, None))
            for identity, definition, report, store, future in pending:
                if future is not None:
                    store = future.result()
                underlying = getattr(store, "_store", store)
                provenance = definition.get("_provenance") or getattr(
                    underlying, "_provenance_payload", None
                )
                report_scenarios.append(
                    ReportScenario(
                        identity=identity,
                        store=ReadOnlyInventoryStore(store),
                        validation_report=report,
                        provenance_payload=provenance,
                        definition=definition,
                    )
                )
        return tuple(report_scenarios)

    def _generate_change_report(
        self,
        *,
        filepath: str | Path | None = None,
        name: str | None = None,
        status: Literal["passed", "failed"] = "passed",
        override: tuple[dict, InventoryStore, ValidationReport] | None = None,
    ) -> ChangeReportArtifacts:
        scenarios = self._report_scenarios(override=override)
        if not scenarios:
            raise RuntimeError(
                "Cannot generate a change report before any scenario has been updated. "
                "Call update() first."
            )
        build_id = getattr(self, "build_id", None) or uuid.uuid4().hex
        self.build_id = build_id
        generated = generate_structured_change_report(
            source_store=self._report_source_store(),
            scenarios=scenarios,
            build_id=build_id,
            source_fingerprint=self._validation_source_fingerprint(),
            status=status,
            filepath=filepath,
            name=name,
            source_database=getattr(self, "source", None),
            source_type=getattr(self, "source_type", None),
            version=getattr(self, "version", None),
            system_model=getattr(self, "system_model", None),
            premise_version=".".join(map(str, __version__)),
            # An exporter failure is represented by a temporary read-only
            # store which can legitimately share a generation number with the
            # certified scenario store. Never reuse or replace the certified
            # audit cache for that diagnostic graph.
            cache_entry=(
                None
                if override is not None
                else getattr(self, "_change_report_cache", None)
            ),
        )
        if override is None:
            self._change_report_cache = generated.cache_entry
        self._last_change_report_artifacts = generated.artifacts
        return generated.artifacts

    def _generate_validation_diagnostic(
        self,
        error: PremiseValidationError,
        scenario: dict,
        store: InventoryStore | None = None,
    ) -> None:
        if not getattr(self, "generate_reports", False) or getattr(
            self, "_automatic_report_in_progress", False
        ):
            return
        try:
            self._automatic_report_in_progress = True
            if store is None:
                store = scenario.get("_inventory_store")
            if store is None:
                database = scenario.get("_inventory_working_copy")
                if database is None:
                    return
                store = create_inventory_store(
                    database,
                    backend=scenario.get("_inventory_backend")
                    or getattr(self, "inventory_backend", "compact"),
                    scenario_identity=self._scenario_identity(scenario),
                    take_ownership=False,
                )
            artifacts = self._generate_change_report(
                status="failed",
                override=(scenario, store, error.report),
            )
            error.attach_report_artifacts(artifacts)
        except Exception as reporting_error:  # preserve the validation failure
            logger.warning(
                "Failed to generate validation diagnostic report: %s",
                reporting_error,
                exc_info=True,
            )
            try:
                underlying = getattr(store, "_store", store)
                provenance = scenario.get("_provenance") or getattr(
                    underlying, "_provenance_payload", None
                )
                artifacts = generate_validation_diagnostic_workbook(
                    scenarios=(
                        ReportScenario(
                            identity=self._scenario_identity(scenario),
                            store=ReadOnlyInventoryStore(store),
                            validation_report=error.report,
                            provenance_payload=provenance,
                            definition=scenario,
                        ),
                    ),
                    build_id=getattr(self, "build_id", uuid.uuid4().hex),
                    source_fingerprint=self._validation_source_fingerprint(),
                    source_database=getattr(self, "source", None),
                    source_type=getattr(self, "source_type", None),
                    version=getattr(self, "version", None),
                    system_model=getattr(self, "system_model", None),
                    premise_version=".".join(map(str, __version__)),
                )
                error.attach_report_artifacts(artifacts)
            except Exception as fallback_error:
                logger.warning(
                    "Failed to generate validation-only workbook: %s",
                    fallback_error,
                    exc_info=True,
                )
        finally:
            self._automatic_report_in_progress = False

    def _run_automatic_reports(self) -> None:
        if not getattr(self, "generate_reports", False):
            return
        try:
            self.generate_scenario_report()
        except Exception as error:
            logger.warning(
                "Automatic scenario report generation failed: %s", error, exc_info=True
            )
        try:
            self.generate_change_report()
        except Exception as error:
            logger.warning(
                "Automatic change report generation failed: %s", error, exc_info=True
            )

    def generate_change_report(
        self,
        filepath: str | Path | None = None,
        name: str | None = None,
    ) -> ChangeReportArtifacts:
        """Generate a structured V2 workbook and detailed Parquet audit.

        This explicit entry point is available after :meth:`update` regardless
        of the ``generate_reports`` constructor setting. Reporting errors are
        intentionally propagated to the caller.
        """

        print("Generate change report.")
        artifacts = self._generate_change_report(filepath=filepath, name=name)
        print(f"Report saved under {artifacts.workbook_path}.")
        return artifacts
