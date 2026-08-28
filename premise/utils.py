"""
Various utils functions.
"""

import json
import hashlib
import math
import os
import pickle
import sys
import uuid
from collections.abc import MutableMapping
from datetime import datetime
from functools import lru_cache
from numbers import Number
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import xarray as xr
import yaml
from country_converter import CountryConverter
from prettytable import PrettyTable
from wurst.searching import biosphere, equals, get_many, technosphere
import numpy as np

from . import __version__
from .data_collection import get_delimiter
from .filesystem_constants import (
    DATA_DIR,
    DIR_CACHED_DB,
    DIR_CACHED_FILES,
    VARIABLES_DIR,
)
from .geomap import Geomap

FUELS_PROPERTIES = VARIABLES_DIR / "fuels.yaml"
EFFICIENCY_RATIO_SOLAR_PV = DATA_DIR / "renewables" / "efficiency_solar_PV.csv"
CACHE_MANIFEST_SUFFIX = ".manifest.json"
CACHE_SCHEMA_VERSION = 5


def rescale_exchange(
    exchange: MutableMapping[str, Any],
    value: Number,
    remove_uncertainty: bool = True,
) -> MutableMapping[str, Any]:
    """Rescale a dictionary-compatible exchange and its uncertainty fields.

    This preserves Wurst's numerical behavior while accepting compact
    copy-on-write exchange mappings in addition to concrete dictionaries.
    """

    assert isinstance(exchange, MutableMapping), "Must pass exchange mapping"
    assert isinstance(value, Number), "Constant factor ``value`` must be a number"

    exchange["amount"] *= value

    if not remove_uncertainty and "uncertainty type" in exchange:
        uncertainty_type = exchange["uncertainty type"]
        if uncertainty_type in {1, 2, 3, 4, 5}:
            if "loc" in exchange and uncertainty_type == 2:
                exchange["loc"] += math.log(value)
            elif "loc" in exchange:
                exchange["loc"] *= value

            if "scale" in exchange and uncertainty_type != 2:
                exchange["scale"] *= abs(value)

            for bound in ("minimum", "maximum"):
                if bound in exchange:
                    exchange[bound] *= value
    elif remove_uncertainty:
        exchange["uncertainty type"] = 0
        exchange["loc"] = exchange["amount"]
        for field in ("scale", "minimum", "maximum"):
            if field in exchange:
                del exchange[field]

    return exchange


def rescale_exchanges(
    ds: Dict[str, Any],
    value: Number,
    technosphere_filters: Optional[Sequence[Any]] = None,
    biosphere_filters: Optional[Sequence[Any]] = None,
    remove_uncertainty: bool = False,
) -> Dict[str, Any]:
    """Scale exchanges in a dataset by a constant factor.

    This function is adapted from :mod:`wurst`'s
    ``change_exchanges_by_constant_factor`` but maintains the option to keep
    uncertainty data attached to exchanges.

    :param ds: Dataset dictionary that contains the exchanges to rescale.
    :type ds: dict
    :param value: Factor used to scale each selected exchange.
    :type value: numbers.Number
    :param technosphere_filters: Filters passed to :func:`wurst.searching.technosphere`.
    :type technosphere_filters: Sequence, optional
    :param biosphere_filters: Filters passed to :func:`wurst.searching.biosphere`.
    :type biosphere_filters: Sequence, optional
    :param remove_uncertainty: Whether to drop the uncertainty information when scaling.
    :type remove_uncertainty: bool
    :return: The updated dataset with the scaled exchanges.
    :rtype: dict
    """

    assert isinstance(ds, dict), "Must pass dataset dictionary document"
    assert isinstance(value, Number), "Constant factor ``value`` must be a number"

    for exc in technosphere(ds, *(technosphere_filters or [])):
        rescale_exchange(exc, value, remove_uncertainty)

    for exc in biosphere(ds, *(biosphere_filters or [])):
        rescale_exchange(exc, value, remove_uncertainty)

    return ds


# Disable printing
def blockPrint() -> None:
    """Redirect ``stdout`` to ``os.devnull``.

    This helper can be used to silence output temporarily when a context
    manager is not required.
    """

    with open(os.devnull, "w") as devnull:
        sys.stdout = devnull


# Restore printing
def enablePrint() -> None:
    """Restore ``stdout`` to the original stream."""

    sys.stdout = sys.__stdout__


class HiddenPrints:
    """Context manager to silence ``print`` statements temporarily.

    Adapted from a recipe shared on StackOverflow_.

    .. _StackOverflow: https://stackoverflow.com/questions/8391411/
       how-to-block-calls-to-print
    """

    def __init__(self):
        """Initialise the context manager state."""

        self._original_stdout = None

    def __enter__(self):
        """Replace ``stdout`` with ``os.devnull``.

        :return: The context manager instance so it can be reused if needed.
        :rtype: HiddenPrints
        """

        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w", encoding="utf-8")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore the original ``stdout`` stream."""

        sys.stdout.close()
        sys.stdout = self._original_stdout


def eidb_label(
    scenario: Dict[str, Any],
    version: str,
    system_model: str = "cutoff",
) -> str:
    """Build a readable label for an ecoinvent scenario.

    :param scenario: Scenario metadata containing the IAM model, pathway and year.
    :type scenario: dict
    :param version: Target ecoinvent version.
    :type version: str
    :param system_model: Ecoinvent system model (``"cutoff"`` or ``"consequential"``).
    :type system_model: str
    :return: A descriptive label, including optional external scenarios and timestamp.
    :rtype: str
    """

    name = (
        f"ei_{system_model}_{version}_{scenario['model']}"
        f"_{scenario['pathway']}_{scenario['year']}"
    )

    if "external scenarios" in scenario:
        for ext_scenario in scenario["external scenarios"]:
            name += f"_{ext_scenario['scenario']}"

    # add date and time
    name += f" {datetime.now().strftime('%Y-%m-%d')}"

    return name


def scenario_metadata(
    scenario: Dict[str, Any],
    version: str = None,
    system_model: str = None,
) -> Dict[str, Any]:
    """Describe the scenario a database represents.

    The returned mapping is meant to be attached to the metadata of an exported
    database (e.g., ``bw2data.databases[name]``), so that the point in time and
    the IAM scenario it represents can be retrieved later on.

    :param scenario: Scenario dictionary, with `model`, `pathway` and `year` keys.
    :type scenario: dict
    :param version: Ecoinvent version the database is based on.
    :type version: str
    :param system_model: Ecoinvent system model ("cutoff" or "consequential").
    :type system_model: str
    :return: JSON-serializable scenario metadata.
    :rtype: dict
    """

    year = int(scenario["year"])

    metadata = {
        "premise_version": ".".join(str(item) for item in __version__),
        "iam_model": scenario["model"],
        "pathway": scenario["pathway"],
        # ISO 8601 point in time the database is representative of
        "representative_time": datetime(year, 1, 1).isoformat(),
    }

    if version is not None:
        metadata["ecoinvent_version"] = str(version)

    if system_model is not None:
        metadata["system_model"] = system_model

    external_scenarios = scenario.get("external scenarios")
    if external_scenarios:
        metadata["external_scenarios"] = [
            ext["scenario"] for ext in external_scenarios if "scenario" in ext
        ]

    return metadata


def database_metadata(
    scenarios: List[Dict[str, Any]],
    version: str = None,
    system_model: str = None,
) -> Dict[str, Any]:
    """Describe the scenario(s) a database represents.

    Databases holding a single scenario get a flat description
    (see :func:`scenario_metadata`). Databases holding several scenarios
    (super-structure or scenario-array databases) get the list of scenarios
    under `scenarios`, plus the point in time they all share, if any.

    :param scenarios: List of scenario dictionaries.
    :type scenarios: list
    :param version: Ecoinvent version the database is based on.
    :type version: str
    :param system_model: Ecoinvent system model ("cutoff" or "consequential").
    :type system_model: str
    :return: JSON-serializable database metadata.
    :rtype: dict
    """

    entries = [
        scenario_metadata(scenario, version=version, system_model=system_model)
        for scenario in scenarios
    ]

    if len(entries) == 1:
        return entries[0]

    for entry in entries:
        # reported once, at the database level
        entry.pop("premise_version", None)

    metadata = {
        "premise_version": ".".join(str(item) for item in __version__),
        "scenarios": entries,
    }

    if version is not None:
        metadata["ecoinvent_version"] = str(version)

    if system_model is not None:
        metadata["system_model"] = system_model

    times = {entry["representative_time"] for entry in entries}
    if len(times) == 1:
        metadata["representative_time"] = times.pop()

    return metadata


@lru_cache(maxsize=1)
def load_constants() -> Dict[str, Any]:
    """Load global constants from ``constants.yaml``.

    :return: Mapping of constant names to their values.
    :rtype: dict
    """
    with open(VARIABLES_DIR / "constants.yaml", "r", encoding="utf-8") as stream:
        constants = yaml.safe_load(stream)

    return constants


@lru_cache
def get_fuel_properties() -> Dict[str, Any]:
    """Retrieve physical properties for the supported fuel mix.

    The information originates from ecoinvent and
    `<https://www.engineeringtoolbox.com/fuels-higher-calorific-values-d_169.html>`_.

    :return: Mapping of fuel names to their properties.
    :rtype: dict
    """

    with open(FUELS_PROPERTIES, "r", encoding="utf-8") as stream:
        fuel_props = yaml.safe_load(stream)

    return fuel_props


def get_water_consumption_factors() -> Dict[str, Any]:
    """Return water-consumption correction factors for hydropower datasets.

    :return: Mapping of hydropower technologies to correction factors.
    :rtype: dict
    """
    with open(
        DATA_DIR / "renewables" / "hydropower.yaml", "r", encoding="utf-8"
    ) as stream:
        water_consumption_factors = yaml.safe_load(stream)

    return water_consumption_factors


def get_efficiency_solar_photovoltaics() -> xr.DataArray:
    """Return PV module efficiencies across time and technology.

    :return: Efficiencies indexed by year, technology and statistic type.
    :rtype: xarray.DataArray
    """

    dataframe = pd.read_csv(
        EFFICIENCY_RATIO_SOLAR_PV, sep=get_delimiter(filepath=EFFICIENCY_RATIO_SOLAR_PV)
    )

    dataframe = dataframe.melt(
        id_vars=["technology", "year"],
        value_vars=["mean", "min", "max"],
        var_name="efficiency_type",
        value_name="efficiency",
    )

    # Convert the DataFrame to an xarray Dataset
    array = dataframe.set_index(["year", "technology", "efficiency_type"])[
        "efficiency"
    ].to_xarray()
    array = array.interpolate_na(dim="year", method="linear")

    return array


def default_global_location(
    database: Iterable[Dict[str, Any]],
) -> Iterable[Dict[str, Any]]:
    """Ensure that each dataset has a location set.

    Missing locations are defaulted to ``"GLO"``.

    :param database: Collection of dataset dictionaries to inspect.
    :type database: collections.abc.Iterable
    :return: The updated database, provided for convenience to support chaining.
    :rtype: collections.abc.Iterable
    """

    for dataset in get_many(database, *[equals("location", None)]):
        dataset["location"] = "GLO"
    return database


def get_regions_definition(model: str) -> None:
    """Print a table describing the IAM regions and their countries.

    :param model: IAM model name, e.g. ``"remind"`` or ``"image"``.
    :type model: str
    """
    table = PrettyTable(["Region", "Countries"])

    geo = Geomap(model)
    country_converter = CountryConverter()

    for region in geo.iam_regions:
        list_countries = []
        for iso_2 in geo.iam_to_ecoinvent_location(region):
            if iso_2 in country_converter.ISO2["ISO2"].values:
                country_name = country_converter.convert(iso_2, to="name")
            else:
                country_name = iso_2

            list_countries.append(country_name)

        table.add_row([region, list_countries])

    table._max_width = {"Region": 50, "Countries": 125}

    print(table)


def clear_existing_cache(
    all_versions: bool = False, filter: Optional[str] = None
) -> None:
    """Delete cached databases except for the active version.

    :param all_versions: Whether to delete cached files for every version.
    :type all_versions: bool
    :param filter: Optional substring that cached filenames must contain.
    :type filter: str, optional
    """

    [
        f.unlink()
        for f in DIR_CACHED_DB.glob("*")
        if f.is_file()
        and (all_versions or "".join(tuple(map(str, __version__))) not in f.name)
        and (filter is None or filter in f.name)
    ]


# clear the cache folder
def clear_cache() -> None:
    """Remove all cached database files."""

    clear_existing_cache(all_versions=True)
    print("Cache folder cleared!")


def clear_inventory_cache() -> None:
    """Remove cached inventory data only."""

    clear_existing_cache(
        all_versions=True,
        filter="inventories",
    )
    print("Inventory cache cleared!")


def clear_runtime_caches() -> None:
    """Clear runtime caches that can retain large transformation objects."""

    from .electricity import Electricity
    from .emissions import Emissions
    from .export import exc_codes, fetch_exchange_code
    from .external import ExternalScenario
    from .inventory_imports import BaseInventoryImport
    from .metals import Metals
    from .runtime_cache import clear_constructor_caches
    from .transformation import BaseTransformation

    cached_functions = (
        Geomap.iam_to_ecoinvent_location,
        Geomap.ecoinvent_to_iam_location,
        BaseInventoryImport.correct_product_field,
        BaseTransformation.get_gis_match,
        Electricity.get_production_per_tech_dict,
        Emissions.find_gains_emissions_change,
        ExternalScenario.add_additional_exchanges,
        Metals.get_metal_market_dataset,
        fetch_exchange_code,
    )

    for cached_function in cached_functions:
        cache_clear = getattr(cached_function, "cache_clear", None)
        if cache_clear is not None:
            cache_clear()

    exc_codes.clear()
    clear_constructor_caches()


def print_version():
    """Display the installed ``premise`` version."""

    print(f"premise v.{__version__}")


def info_on_utils_functions():
    """Print a summary table of helper utilities."""

    table = PrettyTable(["Utils functions", "Description"])
    table.add_row(
        [
            "clear_cache()",
            (
                "Clears the cache folder. "
                "Useful when updating `premise`"
                "or encountering issues with "
                "inventories."
            ),
        ]
    )
    table.add_row(
        [
            "get_regions_definition(model)",
            "Retrieves the list of countries for each region of the model.",
        ]
    )
    table.add_row(
        [
            "ndb.NewDatabase(...)\nndb.generate_scenario_report()",
            "Generates a summary of the most important scenarios' variables.",
        ]
    )
    # align text to the left
    table.align = "l"
    table._max_width = {"Utils functions": 50, "Description": 32}
    print(table)


def warning_about_biogenic_co2() -> None:
    """Explain why biogenic CO₂ flows should be characterised explicitly."""
    table = PrettyTable(["Warning"])
    table.add_row(
        [
            "Because some of the scenarios can yield LCI databases\n"
            "containing net negative emission technologies (NET),\n"
            "it is advised to account for biogenic CO2 flows when calculating\n"
            "Global Warming potential indicators.\n"
            "`premise_gwp` provides characterization factors for such flows.\n"
            "It also provides factors for hydrogen emissions to air.\n\n"
            "Within your Brightway project:\n"
            "from premise_gwp import add_premise_gwp\n"
            "add_premise_gwp()"
        ]
    )
    # align text to the left
    table.align = "l"
    print(table)


def hide_messages():
    """Print guidance on suppressing console output programmatically."""

    print("Keep uncertainty data?")
    print(
        "NewDatabase(..., keep_source_db_uncertainty=True), keep_imports_uncertainty=True)"
    )
    print("")
    print("Hide these messages?")
    print("NewDatabase(..., quiet=True)")


def reset_all_codes(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Assign new UUID codes to datasets and their exchanges.

    :param data: Database composed of dataset dictionaries.
    :type data: list
    :return: Updated database with refreshed codes.
    :rtype: list
    """
    for ds in data:
        ds["code"] = str(uuid.uuid4())
        for exc in ds["exchanges"]:
            if exc["type"] in ["production", "technosphere"]:
                if "input" in exc:
                    del exc["input"]

    return data


def delete_log() -> None:
    """Delete the ``premise.log`` file in the current working directory."""
    log_path = Path.cwd() / "premise.log"
    if log_path.exists():
        log_path.unlink()


def create_scenario_list(scenarios: List[Dict[str, Any]]) -> List[str]:
    """Create human-readable names for IAM scenarios.

    :param scenarios: List of scenario metadata dictionaries.
    :type scenarios: list
    :return: Readable scenario names that include IAM model, pathway and year.
    :rtype: list
    """

    list_scenarios = []

    for scenario in scenarios:
        name = f"{scenario['model']} - {scenario['pathway']} - {scenario['year']}"

        if "external scenarios" in scenario:
            for ext_scenario in scenario["external scenarios"]:
                name += f" - {ext_scenario['scenario']}"

        list_scenarios.append(name)

    return list_scenarios


def dump_database(scenario: Dict[str, Any]) -> Dict[str, Any]:
    """Persist a scenario database to disk.

    :param scenario: Scenario dictionary which may contain a ``"database"`` key.
    :type scenario: dict
    :return: The input scenario with the ``"database"`` replaced by a file reference.
    :rtype: dict
    """

    if scenario.get("database") is None:
        return scenario

    # generate random name
    name = f"{uuid.uuid4().hex}.pickle"
    database_cache_ref, metadata_cache_ref = create_scenario_cache(
        scenario["database"], DIR_CACHED_FILES / name
    )
    scenario["database filepath"] = database_cache_ref
    scenario["database metadata filepath"] = metadata_cache_ref
    del scenario["database"]

    return scenario


def get_cache_manifest_path(file_name: Path) -> Path:
    """Return the manifest path corresponding to a cache reference."""

    file_name = Path(file_name)
    if str(file_name).endswith(CACHE_MANIFEST_SUFFIX):
        return file_name

    return Path(f"{file_name}{CACHE_MANIFEST_SUFFIX}")


def resolve_cache_ref(file_name: Path) -> Path:
    """Resolve a cache reference to either a legacy pickle or a manifest file."""

    file_name = Path(file_name)
    if file_name.exists():
        return file_name

    manifest_path = get_cache_manifest_path(file_name)
    if manifest_path.exists():
        return manifest_path

    return file_name


def cache_ref_exists(file_name: Path) -> bool:
    """Return ``True`` if a legacy cache file or a manifest-backed cache exists."""

    file_name = Path(file_name)
    return file_name.exists() or get_cache_manifest_path(file_name).exists()


def cache_ref_fingerprint(file_name: Path) -> str:
    """Return a cheap invalidation fingerprint for a cache and its shards."""

    cache_ref = resolve_cache_ref(Path(file_name))
    paths = [cache_ref]
    if _is_cache_manifest(cache_ref):
        paths.extend(_iter_cache_bundle_paths(cache_ref))
    records = []
    for path in paths:
        stat = path.stat()
        records.append((str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns)))
    encoded = json.dumps(records, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _is_cache_manifest(file_name: Path) -> bool:
    return str(file_name).endswith(CACHE_MANIFEST_SUFFIX)


def _load_cache_manifest(file_name: Path) -> Dict[str, Any]:
    with open(file_name, encoding="utf-8") as file:
        manifest = json.load(file)

    if not isinstance(manifest, dict):
        raise TypeError(f"Cache manifest {file_name} must contain a JSON object.")

    return manifest


def _iter_cache_bundle_paths(file_name: Path) -> Iterable[Path]:
    manifest = _load_cache_manifest(file_name)
    entries = manifest.get("files", manifest.get("shards"))

    if not isinstance(entries, list) or len(entries) == 0:
        raise ValueError(f"Cache manifest {file_name} does not define any shard files.")

    for entry in entries:
        shard_path = entry.get("path") if isinstance(entry, dict) else entry

        if not isinstance(shard_path, str) or not shard_path:
            raise ValueError(
                f"Cache manifest {file_name} contains an invalid shard entry: {entry!r}"
            )

        shard_file = Path(shard_path)
        if not shard_file.is_absolute():
            shard_file = file_name.parent / shard_file

        yield shard_file


def delete_cache_ref(cache_ref: Path) -> None:
    """Delete a legacy cache file or all files referenced by a manifest-backed cache."""

    cache_ref = resolve_cache_ref(cache_ref)

    if _is_cache_manifest(cache_ref):
        for shard_file in _iter_cache_bundle_paths(cache_ref):
            if shard_file.exists():
                shard_file.unlink()
        if cache_ref.exists():
            cache_ref.unlink()
        return

    if cache_ref.exists():
        cache_ref.unlink()


def load_cached_database(cache_ref: Path) -> List[Dict[str, Any]]:
    """Load a cached database from a legacy pickle or manifest-backed shard set."""

    cache_ref = resolve_cache_ref(cache_ref)

    if _is_cache_manifest(cache_ref):
        database: List[Dict[str, Any]] = []
        for shard_file in _iter_cache_bundle_paths(cache_ref):
            with open(shard_file, "rb") as file:
                shard = pickle.load(file)

            if not isinstance(shard, list):
                raise TypeError(
                    f"Database shard {shard_file} must contain a list of datasets."
                )

            database.extend(shard)

        return database

    with open(cache_ref, "rb") as file:
        return pickle.load(file)


def iter_cached_metadata(cache_ref: Path) -> Iterable[Dict[tuple, Dict[str, Any]]]:
    """Yield metadata chunks from a legacy pickle or manifest-backed shard set."""

    cache_ref = resolve_cache_ref(cache_ref)

    if _is_cache_manifest(cache_ref):
        for shard_file in _iter_cache_bundle_paths(cache_ref):
            with open(shard_file, "rb") as file:
                metadata = pickle.load(file)

            if not isinstance(metadata, dict):
                raise TypeError(
                    f"Metadata shard {shard_file} must contain a dictionary."
                )

            yield metadata

        return

    with open(cache_ref, "rb") as file:
        metadata = pickle.load(file)

    if not isinstance(metadata, dict):
        raise TypeError(f"Metadata cache {cache_ref} must contain a dictionary.")

    yield metadata


def restore_cached_classifications(
    database: List[Dict[str, Any]], metadata_cache_filepath: Path
) -> List[Dict[str, Any]]:
    """Hydrate classifications from old metadata sidecars into cached datasets.

    New caches retain ``classifications`` in the runtime payload. This helper
    keeps older caches compatible without restoring all bulky metadata fields.
    """

    if not metadata_cache_filepath or not cache_ref_exists(metadata_cache_filepath):
        return database

    datasets_by_key: Dict[tuple, List[Dict[str, Any]]] = {}
    for dataset in database:
        if dataset.get("classifications"):
            continue

        key = (
            dataset.get("name"),
            dataset.get("reference product"),
            dataset.get("location"),
        )
        datasets_by_key.setdefault(key, []).append(dataset)

    if not datasets_by_key:
        return database

    for metadata in iter_cached_metadata(metadata_cache_filepath):
        for key, metadata_values in metadata.items():
            if key not in datasets_by_key or "classifications" not in metadata_values:
                continue

            classifications = metadata_values["classifications"]
            for dataset in datasets_by_key[key]:
                if not dataset.get("classifications"):
                    dataset["classifications"] = pickle.loads(
                        pickle.dumps(classifications, -1)
                    )

    return database


def load_database(
    scenario: Dict[str, Any],
    original_database: List[Dict[str, Any]],
    delete: bool = True,
    load_metadata: bool = True,
    warning: bool = True,
    consume_compact: bool = False,
) -> Dict[str, Any]:
    """Load a cached database back into memory.

    :param scenario: Scenario definition potentially referencing a cached database.
    :type scenario: dict
    :param original_database: In-memory reference database used as a fallback copy.
    :type original_database: list
    :param delete: Remove the cached pickle after loading when ``True``.
    :type delete: bool
    :param load_metadata: Reload the metadata cache alongside the database.
    :type load_metadata: bool
    :param warning: Display a warning when reusing the unmodified original database.
    :type warning: bool
    :param consume_compact: Transfer ownership of compact columnar mappings to a
        short-lived exporter instead of building a complete list of dictionaries.
        The source store is emptied when this is enabled.
    :type consume_compact: bool
    :return: Scenario dictionary with the ``"database"`` entry populated in memory.
    :rtype: dict
    """

    if scenario.get("database") is not None:
        return scenario

    # InventoryStore-backed scenarios never expose a mutable database payload.
    # Exporters receive a short-lived copy so the active scenario definition
    # continues to contain only private store/checkpoint references.
    store = scenario.get("_inventory_store")
    export_handoff = scenario.get("_inventory_export_handoff")
    using_export_handoff = (
        store is None and consume_compact and export_handoff is not None
    )
    if using_export_handoff:
        store = export_handoff
    checkpoint = scenario.get("_inventory_checkpoint")
    if store is not None or checkpoint is not None:
        if store is None:
            from .inventory_store import InventoryStore

            store = InventoryStore.open(checkpoint)
        materialized = scenario.copy()
        from .inventory_store import CompactInventoryStore

        if isinstance(store, CompactInventoryStore) and consume_compact:
            materialized["database"] = store._checkout_materialized(
                discard_shared_state=True
            )
            if using_export_handoff:
                # A retained checkpoint handoff still contains lazy columnar
                # activities. Brightway's streaming writer would otherwise
                # prepare every exchange once for SQL and again for vectors.
                # Converting only the lightweight activity shells keeps the
                # exchange mappings private and lets the existing one-pass
                # compact writer reuse its prepared payload.
                for position, dataset in enumerate(materialized["database"]):
                    materialize_dataset = getattr(dataset, "_premise_materialize", None)
                    if materialize_dataset is None:
                        continue
                    payload = materialize_dataset()
                    exchanges = dataset["exchanges"]
                    payload["exchanges"] = exchanges
                    for exchange in exchanges:
                        if hasattr(exchange, "_validation_owner"):
                            exchange._validation_owner = payload
                    list.__setitem__(materialized["database"], position, payload)
        else:
            materialized["database"] = store.materialize(restore_metadata=load_metadata)
        # Match the legacy cache-loading path: source activities can omit
        # storage identifiers, but every exporter requires one.
        for dataset in materialized["database"]:
            common_value = getattr(dataset, "_premise_common_value", None)
            code = (
                common_value("code")
                if common_value is not None
                else dataset.get("code")
            )
            if not code:
                dataset["code"] = uuid.uuid4().hex
        return materialized

    if "database filepath" not in scenario:
        if warning:
            print("WARNING: loading unmodified database!")
        scenario["database"] = pickle.loads(pickle.dumps(original_database, -1))

    else:
        filepath = scenario["database filepath"]
        scenario["database"] = load_cached_database(filepath)
        if not load_metadata and "database metadata filepath" in scenario:
            restore_cached_classifications(
                scenario["database"], scenario["database metadata filepath"]
            )

        # delete the file
        if delete:
            delete_cache_ref(filepath)

    if load_metadata:
        if "database metadata filepath" in scenario:
            filepaths = [scenario["database metadata filepath"]]
        else:
            filepaths = [
                scenario["database metadata cache filepath"],
                scenario["inventories metadata cache filepath"],
            ]
        datasets_by_key = {
            (ds["name"], ds["reference product"], ds["location"]): ds
            for ds in scenario["database"]
        }

        # check if metadata files exist
        for filepath_metadata in filepaths:
            if not cache_ref_exists(filepath_metadata):
                raise FileNotFoundError(
                    f"Metadata file {filepath_metadata} does not exist."
                )
            for metadata in iter_cached_metadata(filepath_metadata):
                for key, metadata_values in metadata.items():
                    ds = datasets_by_key.get(key)
                    if ds is None:
                        continue

                    exchange_metadata = metadata_values.get("__exchange_metadata__")

                    for k, v in metadata_values.items():
                        if k == "__exchange_metadata__":
                            continue
                        if k in [
                            "code",
                            "worksheet name",
                            "database",
                        ]:
                            continue

                        if v is None or v == "None" or v == "nan" or not v:
                            # skip None or empty values
                            continue

                        if k not in ds:
                            ds[k] = v

                        elif ds[k] is None:
                            ds[k] = v

                        else:
                            # if the key already exists, concatenate the values
                            if isinstance(ds[k], list):
                                ds[k].extend(v)

                            elif isinstance(ds[k], str):
                                try:
                                    if len(ds[k]) != len(v):
                                        ds[k] = f"{ds[k]}. {v}"
                                except Exception as exc:
                                    raise ValueError(
                                        f"Failed to merge metadata for {ds.get('name')}: "
                                        f"key={k}, existing={ds[k]!r}, new={v!r}, error={exc}"
                                    ) from exc

                            elif isinstance(ds[k], dict):
                                ds[k].update(v)

                    if exchange_metadata:
                        for exchange, exchange_values in zip(
                            ds.get("exchanges", []), exchange_metadata
                        ):
                            for k, v in exchange_values.items():
                                if v is None or v == "None" or v == "nan" or not v:
                                    continue

                                if k not in exchange:
                                    exchange[k] = v
                                elif exchange[k] is None:
                                    exchange[k] = v
                                elif isinstance(exchange[k], list):
                                    exchange[k].extend(v)
                                elif isinstance(exchange[k], str):
                                    try:
                                        if len(exchange[k]) != len(v):
                                            exchange[k] = f"{exchange[k]}. {v}"
                                    except Exception as exc:
                                        raise ValueError(
                                            f"Failed to merge exchange metadata for {ds.get('name')}: "
                                            f"key={k}, existing={exchange[k]!r}, new={v!r}, error={exc}"
                                        ) from exc
                                elif isinstance(exchange[k], dict):
                                    exchange[k].update(v)

    # scenario caches can preserve dataset codes directly; fall back to
    # generating new identifiers only when a dataset has none.
    for ds in scenario["database"]:
        if not ds.get("code"):
            ds["code"] = str(uuid.uuid4().hex)

    if "database filepath" in scenario:
        del scenario["database filepath"]

    return scenario


def delete_all_pickles(filepath: Optional[Path] = None) -> None:
    """Remove cached pickle files from the cache directory.

    :param filepath: Specific pickle file to delete. When ``None``, delete all.
    :type filepath: pathlib.Path, optional
    """

    if filepath is not None:
        resolved = resolve_cache_ref(filepath)
        if resolved.exists():
            print(f"File {resolved} deleted.")
        delete_cache_ref(resolved)
        metadata_ref = resolve_cache_ref(
            Path(str(filepath).replace(".pickle", " (metadata).pickle"))
        )
        if metadata_ref.exists():
            delete_cache_ref(metadata_ref)
        return

    for manifest in DIR_CACHED_FILES.glob(f"*{CACHE_MANIFEST_SUFFIX}"):
        delete_cache_ref(manifest)

    for file in DIR_CACHED_FILES.glob("*.pickle"):
        if file.exists():
            file.unlink()


def end_of_process(
    scenario: Dict[str, Any],
    *,
    preserve_applied_functions: bool = False,
) -> Dict[str, Any]:
    """Release cached information stored in a scenario definition.

    :param scenario: Scenario dictionary to clean up.
    :type scenario: dict
    :param preserve_applied_functions: Keep transformation metadata when the
        scenario checkpoint will be reused by another exporter.
    :type preserve_applied_functions: bool
    :return: Scenario stripped of database information and caches.
    :rtype: dict
    """

    # delete the database from the scenario
    scenario.pop("database", None)

    if not preserve_applied_functions and "applied functions" in scenario:
        del scenario["applied functions"]

    if "cache" in scenario:
        scenario["cache"] = {}
    if "index" in scenario:
        scenario["index"] = {}

    return scenario


def downcast_value(val: Any) -> Any:
    """Convert ``float`` values to ``float32`` for smaller cache size."""

    if isinstance(val, float):
        return np.float32(val)
    return val


def trim_exchanges(exc: Dict[str, Any]) -> Dict[str, Any]:
    """Filter an exchange dictionary to retain serialisable keys only.

    :param exc: Exchange dictionary from a dataset.
    :type exc: dict
    :return: Sanitised exchange containing only supported keys.
    :rtype: dict
    """

    # only keep certain keys and remove None or NaN values

    return {
        k: downcast_value(v)
        for k, v in exc.items()
        if k
        in [
            "uncertainty type",
            "loc",
            "scale",
            "amount",
            "type",
            "production volume",
            "product",
            "name",
            "unit",
            "location",
            "shape",
            "minimum",
            "maximum",
            "categories",
            "comment",
        ]
        and pd.notna(v)
    }


_CACHE_TRIMMED_DATASET_FIELDS = {
    "name",
    "reference product",
    "location",
    "unit",
    "exchanges",
    "comment",
    "classifications",
}

_CACHE_METADATA_EXCLUDED_FIELDS = {
    "name",
    "reference product",
    "location",
    "unit",
    "exchanges",
    "type",
    "comment",
    "classifications",
}

_SCENARIO_TRIMMED_DATASET_FIELDS = {
    "database",
    "code",
    "name",
    "reference product",
    "location",
    "unit",
    "type",
    "regionalized",
    "exchanges",
    "classifications",
}

_SCENARIO_TRIMMED_EXCHANGE_FIELDS = {
    "input",
    "amount",
    "type",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
    "production volume",
    "product",
    "name",
    "unit",
    "location",
    "categories",
}

_SCENARIO_METADATA_EXCLUDED_FIELDS = {
    "name",
    "reference product",
    "location",
    "unit",
    "type",
    "exchanges",
    "database",
    "code",
    "classifications",
}


def _has_cache_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, np.floating):
        return not np.isnan(value)

    value_type = type(value)
    if value_type is str:
        return value not in {"None", "nan", ""}
    if value_type in {list, tuple, dict, set}:
        return True
    if value_type in {bool, int}:
        return True
    if value_type is float:
        return not np.isnan(value)

    # Preserve support for subclasses and the complete NumPy integer family
    # after the exact built-in hot paths above.
    if isinstance(value, str):
        return value not in {"None", "nan", ""}
    if isinstance(value, (list, tuple, dict, set)):
        return True
    if isinstance(value, (bool, int, np.integer)):
        return True
    try:
        return bool(pd.notna(value))
    except Exception:
        return True


def _scenario_metadata_value_is_restored(value: Any) -> bool:
    """Return whether the legacy scenario-cache loader restores ``value``.

    Scenario payloads historically passed through ``create_scenario_cache``
    followed by ``load_database`` before callers could inspect or export them.
    The writer drops null-like values, while the loader additionally skips
    false and empty metadata values.  Compact scenario stores must reproduce
    that observable boundary without weakening the lossless generic store
    contract.
    """

    if not _has_cache_value(value):
        return False
    try:
        return bool(value)
    except (TypeError, ValueError):
        # The legacy loader cannot truth-test arbitrary vector values. Keep
        # them here so scenario sealing remains lossless for custom metadata.
        return True


def _normalize_scenario_cache_activity(
    dataset: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Apply legacy post-cache field-presence semantics to one activity."""

    for field, value in list(dataset.items()):
        if (
            field not in _SCENARIO_TRIMMED_DATASET_FIELDS
            and not _scenario_metadata_value_is_restored(value)
        ):
            del dataset[field]
    return dataset


def _scenario_cache_exchange_field_is_restored(field: str, value: Any) -> bool:
    """Return whether one exchange field survives a legacy cache round trip."""

    if field in _SCENARIO_TRIMMED_EXCHANGE_FIELDS:
        return _has_cache_value(value)
    return _scenario_metadata_value_is_restored(value)


def _normalize_scenario_cache_exchange(
    exchange: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Apply legacy post-cache field-presence semantics to one exchange."""

    for field, value in list(exchange.items()):
        if not _scenario_cache_exchange_field_is_restored(field, value):
            del exchange[field]
    return exchange


def _metadata_for_cache_dataset(ds: Dict[str, Any]) -> Tuple[tuple, Dict[str, Any]]:
    key = (ds["name"], ds["reference product"], ds["location"])
    metadata = {
        field: value
        for field, value in ds.items()
        if field not in _CACHE_METADATA_EXCLUDED_FIELDS
        and value is not None
        and value != "None"
        and value != "nan"
    }
    return key, metadata


def _trim_cache_dataset_in_place(ds: Dict[str, Any]) -> Dict[str, Any]:
    trimmed_dataset = {
        field: value
        for field, value in ds.items()
        if field in _CACHE_TRIMMED_DATASET_FIELDS
    }
    trimmed_dataset["exchanges"] = [
        trim_exchanges(exchange) for exchange in trimmed_dataset["exchanges"]
    ]
    ds.clear()
    ds.update(trimmed_dataset)
    return ds


def _trim_scenario_exchange(
    exchange: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    compact_exchange: Dict[str, Any] = {}
    exchange_metadata: Dict[str, Any] = {}

    for field, value in exchange.items():
        if not _has_cache_value(value):
            continue

        target = (
            compact_exchange
            if field in _SCENARIO_TRIMMED_EXCHANGE_FIELDS
            else exchange_metadata
        )
        target[field] = value

    return compact_exchange, exchange_metadata


def _metadata_for_scenario_dataset(ds: Dict[str, Any]) -> Tuple[tuple, Dict[str, Any]]:
    key = (ds["name"], ds["reference product"], ds["location"])
    metadata = {
        field: value
        for field, value in ds.items()
        if field not in _SCENARIO_METADATA_EXCLUDED_FIELDS and _has_cache_value(value)
    }

    exchange_metadata = []
    for exchange in ds.get("exchanges", []):
        _, compact_exchange_metadata = _trim_scenario_exchange(exchange)
        exchange_metadata.append(compact_exchange_metadata)

    if any(exchange_metadata):
        metadata["__exchange_metadata__"] = exchange_metadata

    return key, metadata


def _trim_scenario_dataset_in_place(ds: Dict[str, Any]) -> Dict[str, Any]:
    trimmed_dataset = {
        field: value
        for field, value in ds.items()
        if field in _SCENARIO_TRIMMED_DATASET_FIELDS
    }
    trimmed_dataset["exchanges"] = [
        _trim_scenario_exchange(exchange)[0]
        for exchange in trimmed_dataset["exchanges"]
    ]
    ds.clear()
    ds.update(trimmed_dataset)
    return ds


def _compact_scenario_dataset_in_place(
    ds: Dict[str, Any],
) -> Tuple[tuple, Dict[str, Any]]:
    """Compact one scenario dataset and collect sidecar metadata in one pass."""

    key = (ds["name"], ds["reference product"], ds["location"])
    metadata = {
        field: value
        for field, value in ds.items()
        if field not in _SCENARIO_METADATA_EXCLUDED_FIELDS and _has_cache_value(value)
    }

    compact_exchanges = []
    exchange_metadata = []
    for exchange in ds.get("exchanges", []):
        compact_exchange, compact_exchange_metadata = _trim_scenario_exchange(exchange)
        compact_exchanges.append(compact_exchange)
        exchange_metadata.append(compact_exchange_metadata)

    if any(exchange_metadata):
        metadata["__exchange_metadata__"] = exchange_metadata

    compact_dataset = {
        field: value
        for field, value in ds.items()
        if field in _SCENARIO_TRIMMED_DATASET_FIELDS
    }
    compact_dataset["exchanges"] = compact_exchanges
    ds.clear()
    ds.update(compact_dataset)
    return key, metadata


def _chunk_sequence(
    sequence: Sequence[Any], chunk_size: int
) -> Iterable[Sequence[Any]]:
    for start in range(0, len(sequence), chunk_size):
        yield sequence[start : start + chunk_size]


def _chunk_mapping(
    mapping: Dict[Any, Any], chunk_size: int
) -> Iterable[Dict[Any, Any]]:
    items = list(mapping.items())
    for chunk in _chunk_sequence(items, chunk_size):
        yield dict(chunk)


def _write_cache_manifest(
    cache_ref: Path, shard_paths: Sequence[Path], payload_kind: str
) -> Path:
    manifest_path = get_cache_manifest_path(cache_ref)

    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(
            {
                "cache_format": 1,
                "storage": "pickle-shards",
                "kind": payload_kind,
                "files": [shard_path.name for shard_path in shard_paths],
            },
            file,
        )

    return manifest_path


def _write_cache_shards(
    cache_ref: Path, chunks: Iterable[Any], payload_kind: str
) -> Path:
    shard_paths = []

    for index, chunk in enumerate(chunks):
        shard_path = cache_ref.with_name(f"{cache_ref.name}.part-{index:04d}.pickle")
        with open(shard_path, "wb") as file:
            pickle.dump(chunk, file)
        shard_paths.append(shard_path)

    if not shard_paths:
        shard_path = cache_ref.with_name(f"{cache_ref.name}.part-0000.pickle")
        with open(shard_path, "wb") as file:
            pickle.dump([] if payload_kind == "database" else {}, file)
        shard_paths.append(shard_path)

    return _write_cache_manifest(cache_ref, shard_paths, payload_kind)


def create_cache(
    database: List[Dict[str, Any]], file_name: Path
) -> Tuple[List[Dict[str, Any]], Path]:
    """Persist a database and its metadata into cache files.

    :param database: Database to cache.
    :type database: list
    :param file_name: Target file that will store the database pickle.
    :type file_name: pathlib.Path
    :return: Tuple with the trimmed database and the metadata cache path.
    :rtype: tuple
    """

    # make sure the directory exists
    DIR_CACHED_DB.mkdir(parents=True, exist_ok=True)

    metadata_cache_file = Path(str(file_name).replace(".pickle", " (metadata).pickle"))
    metadata_chunk_size = 5_000
    metadata_shard_paths = []
    metadata_chunk: Dict[tuple, Dict[str, Any]] = {}

    for dataset in database:
        key, metadata = _metadata_for_cache_dataset(dataset)
        if metadata:
            metadata_chunk[key] = metadata

        _trim_cache_dataset_in_place(dataset)

        if len(metadata_chunk) >= metadata_chunk_size:
            shard_path = metadata_cache_file.with_name(
                f"{metadata_cache_file.name}.part-{len(metadata_shard_paths):04d}.pickle"
            )
            with open(shard_path, "wb") as file:
                pickle.dump(metadata_chunk, file)
            metadata_shard_paths.append(shard_path)
            metadata_chunk = {}

    if metadata_chunk:
        shard_path = metadata_cache_file.with_name(
            f"{metadata_cache_file.name}.part-{len(metadata_shard_paths):04d}.pickle"
        )
        with open(shard_path, "wb") as file:
            pickle.dump(metadata_chunk, file)
        metadata_shard_paths.append(shard_path)
    elif not metadata_shard_paths:
        shard_path = metadata_cache_file.with_name(
            f"{metadata_cache_file.name}.part-0000.pickle"
        )
        with open(shard_path, "wb") as file:
            pickle.dump({}, file)
        metadata_shard_paths.append(shard_path)

    with open(file_name, "wb") as file:
        pickle.dump(database, file)

    metadata_cache_ref = _write_cache_manifest(
        metadata_cache_file, metadata_shard_paths, "metadata"
    )

    return database, metadata_cache_ref


def create_scenario_cache(
    database: List[Dict[str, Any]], file_name: Path
) -> Tuple[Path, Path]:
    """Persist a post-update scenario database in compact shard-backed files."""

    DIR_CACHED_FILES.mkdir(parents=True, exist_ok=True)

    metadata_cache_file = Path(str(file_name).replace(".pickle", " (metadata).pickle"))
    metadata_chunk_size = 1_000
    metadata_shard_paths = []
    metadata_chunk: Dict[tuple, Dict[str, Any]] = {}

    for dataset in database:
        key, metadata = _compact_scenario_dataset_in_place(dataset)
        if metadata:
            metadata_chunk[key] = metadata

        if len(metadata_chunk) >= metadata_chunk_size:
            shard_path = metadata_cache_file.with_name(
                f"{metadata_cache_file.name}.part-{len(metadata_shard_paths):04d}.pickle"
            )
            with open(shard_path, "wb") as file:
                pickle.dump(metadata_chunk, file)
            metadata_shard_paths.append(shard_path)
            metadata_chunk = {}

    if metadata_chunk:
        shard_path = metadata_cache_file.with_name(
            f"{metadata_cache_file.name}.part-{len(metadata_shard_paths):04d}.pickle"
        )
        with open(shard_path, "wb") as file:
            pickle.dump(metadata_chunk, file)
        metadata_shard_paths.append(shard_path)
    elif not metadata_shard_paths:
        shard_path = metadata_cache_file.with_name(
            f"{metadata_cache_file.name}.part-0000.pickle"
        )
        with open(shard_path, "wb") as file:
            pickle.dump({}, file)
        metadata_shard_paths.append(shard_path)

    database_cache_ref = _write_cache_shards(
        file_name, _chunk_sequence(database, 2_500), "database"
    )
    metadata_cache_ref = _write_cache_manifest(
        metadata_cache_file, metadata_shard_paths, "metadata"
    )

    return database_cache_ref, metadata_cache_ref


def load_metadata(file_name: Path) -> Dict[str, Any]:
    """Load metadata stored alongside a cached database.

    :param file_name: Cache file containing the database pickle.
    :type file_name: pathlib.Path
    :return: Metadata dictionary indexed by dataset identifiers.
    :rtype: dict
    """
    cache_file = resolve_cache_ref(
        Path(str(file_name).replace(".pickle", " (metadata).pickle"))
    )

    if not cache_ref_exists(cache_file):
        raise FileNotFoundError(f"Cache file {cache_file} does not exist.")

    metadata: Dict[str, Any] = {}
    for chunk in iter_cached_metadata(cache_file):
        metadata.update(chunk)

    return metadata


def get_uuids(db: Iterable[Dict[str, Any]]) -> Dict[tuple, str]:
    """Create a mapping between dataset identifiers and random UUIDs.

    :param db: Iterable of dataset dictionaries.
    :type db: collections.abc.Iterable
    :return: Mapping from dataset identity tuple to UUID string.
    :rtype: dict
    """

    return {
        (ds["name"], ds["reference product"], ds["location"]): str(uuid.uuid4().hex)
        for ds in db
    }
