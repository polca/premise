"""
Various utils functions.
"""

import json
import os
import pickle
import sys
import uuid
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
from wurst import rescale_exchange
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
    # dump as pickle
    with open(DIR_CACHED_FILES / name, "wb") as f:
        pickle.dump(scenario["database"], f)
    scenario["database filepath"] = DIR_CACHED_FILES / name
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


def load_database(
    scenario: Dict[str, Any],
    original_database: List[Dict[str, Any]],
    delete: bool = True,
    load_metadata: bool = True,
    warning: bool = True,
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
    :return: Scenario dictionary with the ``"database"`` entry populated in memory.
    :rtype: dict
    """

    if scenario.get("database") is not None:
        return scenario

    if "database filepath" not in scenario:
        if warning:
            print("WARNING: loading unmodified database!")
        scenario["database"] = pickle.loads(pickle.dumps(original_database, -1))

    else:
        filepath = scenario["database filepath"]

        # load pickle
        with open(filepath, "rb") as f:
            scenario["database"] = pickle.load(f)

        # delete the file
        if delete:
            filepath.unlink()

    if load_metadata:

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

                    for k, v in metadata_values.items():
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

    # re-attribute a code to every dataset
    uuids = get_uuids(scenario["database"])
    for ds in scenario["database"]:
        key = (ds["name"], ds["reference product"], ds["location"])
        if key in uuids:
            ds["code"] = uuids[key]
        else:
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
        for file in DIR_CACHED_FILES.glob("*.pickle"):
            if file == filepath:
                print(f"File {file} deleted.")
                file.unlink()
    else:
        for file in DIR_CACHED_FILES.glob("*.pickle"):
            file.unlink()


def end_of_process(scenario: Dict[str, Any]) -> Dict[str, Any]:
    """Release cached information stored in a scenario definition.

    :param scenario: Scenario dictionary to clean up.
    :type scenario: dict
    :return: Scenario stripped of database information and caches.
    :rtype: dict
    """

    # delete the database from the scenario
    del scenario["database"]

    if "applied functions" in scenario:
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

    metadata = {
        (ds["name"], ds["reference product"], ds["location"]): {
            k: v
            for k, v in ds.items()
            if k
            not in [
                "name",
                "reference product",
                "location",
                "unit",
                "exchanges",
                "type",
                "comment",
            ]
            and v is not None
            and v != "None"
            and v != "nan"
        }
        for ds in database
    }

    database = [
        {
            k: v
            for k, v in ds.items()
            if k
            in [
                "name",
                "reference product",
                "location",
                "unit",
                "exchanges",
                "comment",
            ]
        }
        for ds in database
    ]

    for ds in database:
        ds["exchanges"] = [trim_exchanges(exc) for exc in ds["exchanges"]]

    # make sure the directory exists
    DIR_CACHED_DB.mkdir(parents=True, exist_ok=True)

    metadata_cache_file = Path(str(file_name).replace(".pickle", " (metadata).pickle"))
    metadata_cache_ref = _write_cache_shards(
        metadata_cache_file,
        _chunk_mapping(metadata, chunk_size=5_000),
        payload_kind="metadata",
    )

    with open(file_name, "wb") as file:
        pickle.dump(database, file)

    return database, metadata_cache_ref


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
