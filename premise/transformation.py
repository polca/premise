"""
transformation.py contains the base class TransformationBase,
used by other classes (e.g. Transport, Electricity, Steel, Cement, etc.).
It provides basic methods usually used for electricity, cement, steel sector transformation
on the wurst database.
"""

import copy
import logging.config
import math
import uuid
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Mapping, ValuesView
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from itertools import groupby, product
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Sequence, Set, Tuple, Union

import numpy as np
import xarray as xr
import yaml
from _operator import itemgetter
from constructive_geometries import resolved_row
from wurst import reference_product
from wurst import searching as ws
from wurst import transformations as wt
from xarray import DataArray

from .activity_maps import InventorySet
from .data_collection import IAMDataCollection
from .filesystem_constants import DATA_DIR
from .geomap import Geomap
from .inventory_copy import clone_inventory_dataset
from .inventory_store import IndexedInventoryList, compact_exchange_payload
from .provenance import record_change_event
from .utils import get_fuel_properties, rescale_exchange

LOG_CONFIG = DATA_DIR / "utils" / "logging" / "logconfig.yaml"
# directory for log files
DIR_LOG_REPORT = Path.cwd() / "export" / "logs"
# if DIR_LOG_REPORT folder does not exist
# we create it
if not Path(DIR_LOG_REPORT).exists():
    Path(DIR_LOG_REPORT).mkdir(parents=True, exist_ok=True)

with open(LOG_CONFIG, encoding="utf-8") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger("module")

_SCENARIO_GIS_CACHE_KEY = "__premise_gis_match_v1__"
_SCENARIO_ROW_CACHE_KEY = "__premise_resolved_row_v1__"
_VALIDATION_RELINKED_TARGETS_KEY = "__premise_validation_relinked_targets_v1__"
_VALIDATION_ADDED_TARGETS_KEY = "__premise_validation_added_targets_v1__"


class RelinkingInvariantError(RuntimeError):
    """Raised when relinking does not preserve a product's demanded amount."""


def _exclude_exchange_objects(exchanges, excluded):
    """Return exchanges whose object identities are not selected."""

    excluded_ids = {id(exchange) for exchange in excluded}
    return [exchange for exchange in exchanges if id(exchange) not in excluded_ids]


def _exchange_type_for_relinking(exchange):
    """Read only the exchange type before decoding other relinking fields."""

    accessor = getattr(exchange, "_premise_exchange_type", None)
    return accessor() if accessor is not None else exchange["type"]


def _technosphere_relink_fields(exchange):
    """Return name, effective product, location, unit, and amount once."""

    accessor = getattr(exchange, "_premise_relink_technosphere_fields", None)
    if accessor is not None:
        return accessor()
    product = (
        exchange["reference product"]
        if "reference product" in exchange
        else exchange["product"]
    )
    return (
        exchange["name"],
        product,
        exchange["location"],
        exchange["unit"],
        exchange["amount"],
    )


def _iter_generic_relink_candidates(exchanges, provider_semantics):
    """Yield generic candidates while decoding technosphere fields once."""

    for exchange in exchanges:
        if _exchange_type_for_relinking(exchange) != "technosphere":
            continue
        fields = _technosphere_relink_fields(exchange)
        name, effective_product, location, unit, amount = fields
        if (
            name,
            effective_product,
            location,
        ) not in provider_semantics and amount != 0:
            # Match the established two-step semantics: reference-product
            # precedence decides eligibility, while relinking itself groups by
            # the concrete product field.
            yield exchange, (
                name,
                exchange["product"],
                location,
                unit,
                amount,
            )


@dataclass(frozen=True, slots=True, eq=False)
class _ProviderRecord(Mapping[str, Any]):
    """Compact mapping-compatible provider snapshot used by activity indexes."""

    name: str
    reference_product: str
    location: str
    unit: str
    production_volume: Any

    _FIELDS = (
        "name",
        "reference product",
        "location",
        "unit",
        "production volume",
    )

    def __getitem__(self, key: str) -> Any:
        if key == "name":
            return self.name
        if key == "reference product":
            return self.reference_product
        if key == "location":
            return self.location
        if key == "unit":
            return self.unit
        if key == "production volume":
            return self.production_volume
        raise KeyError(key)

    def __iter__(self):
        return iter(self._FIELDS)

    def __len__(self) -> int:
        return len(self._FIELDS)


def _provider_record(dataset: Mapping[str, Any]) -> _ProviderRecord:
    production = None
    for exchange in dataset["exchanges"]:
        if exchange["type"] == "production":
            production = exchange
            break
    if production is None:
        # Preserve the historical failure from ``list(ws.production(...))[0]``.
        raise IndexError("list index out of range") from None
    return _ProviderRecord(
        name=dataset["name"],
        reference_product=dataset["reference product"],
        location=dataset["location"],
        unit=dataset["unit"],
        production_volume=production.get("production volume", 0),
    )


def redefine_uncertainty_params(old_exc, new_exc):
    """
    Returns "loc", "scale", "minimum" and "maximum" and "negative" values for a given exchange.
    """

    try:
        if old_exc.get("amount") in (0, None):
            raise ZeroDivisionError(
                "Cannot rescale uncertainty parameters with zero amount."
            )

        if old_exc.get("uncertainty type") in [
            0,
            1,
        ]:
            return (
                new_exc["amount"],
                None,
                None,
                None,
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 2:
            return (
                (
                    math.log(new_exc["amount"] * -1)
                    if new_exc["amount"] < 0
                    else math.log(new_exc["amount"])
                ),
                old_exc.get("scale"),
                None,
                None,
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 3:
            return (
                new_exc["amount"],
                old_exc.get("scale"),
                None,
                None,
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 4:
            return (
                None,
                None,
                old_exc.get("minimum", 0) * (new_exc["amount"] / old_exc["amount"]),
                old_exc.get("maximum") * (new_exc["amount"] / old_exc["amount"]),
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 5:
            return (
                new_exc["amount"],
                None,
                old_exc.get("minimum", 0) * (new_exc["amount"] / old_exc["amount"]),
                old_exc.get("maximum") * (new_exc["amount"] / old_exc["amount"]),
                True if new_exc["amount"] < 0 else False,
            )

        else:
            return None, None, None, None, None
    except Exception as exc:
        raise ValueError(
            f"Failed to redefine uncertainty params for {old_exc} -> {new_exc}: {exc}"
        ) from exc


def group_dicts_by_keys(dicts: list, keys: list):
    groups = defaultdict(list)
    for d in dicts:
        group_key = tuple(d.get(k) for k in keys)
        groups[group_key].append(d)
    return list(groups.values())


def get_suppliers_of_a_region(
    database: List[dict],
    locations: List[str],
    names: List[str],
    reference_prod: str,
    unit: str,
    exclude: List[str] = None,
    exact_match: bool = False,
) -> filter:
    """
    Return a list of datasets, for which the location, name,
    reference product and unit correspond to the region and name
    given, respectively.

    :param database: database to search
    :param locations: list of locations
    :param names: names of datasets
    :param unit: unit of dataset
    :param reference_prod: reference product of dataset
    :return: list of wurst datasets
    :param exclude: list of terms to exclude
    """

    if exact_match is True:
        filters = [
            ws.either(*[ws.equals("name", supplier) for supplier in names]),
        ]
    else:
        filters = [
            ws.either(*[ws.contains("name", supplier) for supplier in names]),
        ]

    filters += [
        ws.either(*[ws.equals("location", loc) for loc in locations]),
        ws.contains("reference product", reference_prod),
        ws.equals("unit", unit),
    ]

    if exclude:
        filters.append(ws.doesnt_contain_any("name", exclude))

    return ws.get_many(
        database,
        *filters,
    )


def get_shares_from_production_volume(
    ds_list: Union[Dict[str, Any], List[Dict[str, Any]]],
) -> list:
    """
    Return shares of supply of each dataset in `ds_list`
    based on respective production volumes
    :param ds_list: list of datasets
    :return: dictionary with (dataset name, dataset location, ref prod, unit) as keys, shares as values. Shares total 1.
    """

    if not isinstance(ds_list, list):
        ds_list = [ds_list]

    suppliers = []
    total_production_volume = 0

    for act in ds_list:
        production_volume = 0

        if "production volume" in act:
            production_volume = float(act["production volume"])
            if not np.isfinite(production_volume):
                production_volume = 1e-9
            production_volume = max(production_volume, 1e-9)
        else:
            for exc in ws.production(act):
                # even if non-existent, we set a minimum value of 1e-9
                # because if not, we risk dividing by zero!!!
                production_volume = float(exc.get("production volume", 1e-9))
                if not np.isfinite(production_volume):
                    production_volume = 1e-9
                production_volume = max(production_volume, 1e-9)

        suppliers.append(
            {
                "name": act["name"],
                "reference product": act["reference product"],
                "location": act["location"],
                "unit": act["unit"],
                "production volume": production_volume,
            }
        )
        total_production_volume += production_volume

    def nonzero(x):
        return x if x != 0.0 else 1.0

    for supplier in suppliers:
        supplier["share"] = supplier["production volume"] / nonzero(
            total_production_volume
        )

    return suppliers


def get_tuples_from_database(database: List[dict]) -> List[Tuple[str, str, str]]:
    """
    Return a list of tuples (name, reference product, location)
    for each dataset in database.
    :param database: wurst database
    :return: a list of tuples
    """
    return [
        (dataset["name"], dataset["reference product"], dataset["location"])
        for dataset in database
        if "has_downstream_consumer" not in dataset
    ]


def remove_exchanges(datasets_dict: Dict[str, dict], list_exc: List) -> Dict[str, dict]:
    """
    Returns the same `datasets_dict`, where the list of exchanges in these datasets
    has been filtered out: unwanted exchanges has been removed.

    :param datasets_dict: a dictionary with IAM regions as keys, datasets as value
    :param list_exc: list of names (e.g., ["coal", "lignite"]) which are checked against exchanges' names in the dataset
    :return: returns `datasets_dict` without the exchanges whose names check with `list_exc`
    """

    for region in datasets_dict:
        datasets_dict[region]["exchanges"] = [
            exc
            for exc in datasets_dict[region]["exchanges"]
            if not any(ele in exc.get("product", []) for ele in list_exc)
        ]

    return datasets_dict


def new_exchange(exc, location, factor):
    copied_exc = deepcopy(exc)
    copied_exc["location"] = location
    return rescale_exchange(copied_exc, factor, remove_uncertainty=False)


def _relinked_exchange(exc, location, factor=1.0, *, name=None, product=None):
    """Return only fields consumed by the relinking aggregation pass."""

    return {
        "name": exc["name"] if name is None else name,
        "product": exc["product"] if product is None else product,
        "unit": exc["unit"],
        "location": location,
        "type": "technosphere",
        "amount": exc["amount"] * factor,
    }


def allocate_inputs(exc, lst, exchange_factory=new_exchange):
    """
    Allocate the input exchanges in ``lst`` to ``exc``,
    using production volumes where possible, and equal splitting otherwise.
    Always uses equal splitting if ``RoW`` is present.
    """
    pvs = [o.get("production volume", 0) for o in lst]

    if any((x > 0 for x in pvs)):
        # Allocate using production volume
        total = sum(pvs)
    else:
        # Allocate evenly
        total = len(lst)
        pvs = [1 for _ in range(total)]

    if lst[0]["name"] != exc["name"]:
        exc["name"] = lst[0]["name"]

    return (
        [
            exchange_factory(exc, obj["location"], factor / total)
            for obj, factor in zip(lst, pvs)
            if factor > 0
        ],
        [p / total for p in pvs if p > 0],
    )


def filter_out_results(
    item_to_look_for: str, results: List[dict], field_to_look_at: str
) -> List[dict]:
    """Filters a list of results by a given field"""
    return [r for r in results if item_to_look_for not in r[field_to_look_at]]


def filter_technosphere_exchanges(exchanges: list):
    return filter(lambda x: x["type"] == "technosphere", exchanges)


def calculate_input_energy(
    fuel_name: str,
    fuel_amount: float,
    fuel_unit: str,
    fuels_specs: dict,
    fuel_map_reverse: dict,
) -> float:
    """
    Returns the amount of energy entering the conversion process, in MJ
    :param fuel_name: name of the liquid, gaseous or solid fuel
    :param fuel_amount: amount of fuel input
    :param fuel_unit: unit of fuel
    :return: amount of fuel energy, in MJ
    """

    def _sanitize_fuel_name(name: str) -> str:
        """Sanitize fuel name by removing market prefixes."""
        items_to_remove = [
            "market for ",
            "market group for ",
            ", high pressure",
            ", low pressure",
            # ", used as fuel",
        ]
        for item in items_to_remove:
            name = name.replace(item, "")
        return name

    # if fuel input other than MJ
    if fuel_unit in ["kilogram", "cubic meter"]:
        fuel_name = _sanitize_fuel_name(fuel_name)

        if fuel_name in fuel_map_reverse:
            lhv = fuels_specs[fuel_map_reverse[fuel_name]]["lhv"]["value"]
        elif any(fuel_name.startswith(x) for x in fuels_specs.keys()):
            fuels = [x for x in fuels_specs.keys() if fuel_name.startswith(x)]
            lhv = fuels_specs[fuels[0]]["lhv"]["value"]
        elif any(
            fuel_name.startswith(x.replace("market for ", ""))
            for x in fuel_map_reverse.keys()
        ):
            fuels = [
                x
                for x in fuel_map_reverse.keys()
                if fuel_name.startswith(x.replace("market for ", ""))
            ]
            lhv = fuels_specs[fuel_map_reverse[fuels[0]]]["lhv"]["value"]
        else:
            print(f"Warning: LHV for {fuel_name} not found in fuel specifications.")
            print()
            print(f"Available fuel specs keys: {list(fuels_specs.keys())}.")
            print()
            print(f"Available fuel map reverse keys: {list(fuel_map_reverse.keys())}.")
            print()
            print("fuel map reverse keys without `market for`")
            print([x.replace("market for ", "") for x in fuel_map_reverse.keys()])
            print(
                any(
                    fuel_name.startswith(x.replace("market for ", ""))
                    for x in fuel_map_reverse.keys()
                )
            )
            print()
            raise ValueError(f"LHV for {fuel_name} not found in fuel specifications.")
    elif fuel_unit == "kilowatt hour":
        lhv = 3.6
    else:
        lhv = 1

    # if already in MJ
    return fuel_amount * lhv


_FUEL_NAME_PARTS_TO_REMOVE = (
    "market for ",
    "market group for ",
    ", high pressure",
    ", low pressure",
    ", used as fuel",
)


@lru_cache(maxsize=8192)
def _sanitize_fuel_filter_name(name: str) -> str:
    """Return the canonical name used to identify fuel exchanges."""
    for item in _FUEL_NAME_PARTS_TO_REMOVE:
        name = name.replace(item, "")
    return name


class FuelFilterIndex(NamedTuple):
    """Immutable prefix index for the fuel filters used by transformations."""

    filters: Tuple[str, ...]
    sorted_filters: Tuple[str, ...]

    def matches(self, exchange_name: str) -> bool:
        """Return whether a filter starts with the sanitized exchange name."""
        exchange_name = _sanitize_fuel_filter_name(exchange_name)
        position = bisect_left(self.sorted_filters, exchange_name)
        return position < len(self.sorted_filters) and self.sorted_filters[
            position
        ].startswith(exchange_name)


@lru_cache(maxsize=64)
def _prepare_fuel_filter_tuple(fuel_filters: Tuple[str, ...]) -> FuelFilterIndex:
    sanitized = tuple(_sanitize_fuel_filter_name(name) for name in fuel_filters)
    return FuelFilterIndex(
        filters=sanitized,
        sorted_filters=tuple(sorted(set(sanitized))),
    )


def prepare_fuel_filters(fuel_filters: Sequence[str]) -> FuelFilterIndex:
    """Compile fuel filters once for repeated efficiency calculations."""
    return _prepare_fuel_filter_tuple(tuple(fuel_filters))


def find_fuel_efficiency(
    dataset: dict,
    energy_out: float,
    fuel_specs: dict,
    fuel_map_reverse: dict,
    fuel_filters: Union[Sequence[str], FuelFilterIndex] = None,
) -> float:
    """
    This method calculates the efficiency value set initially, in case it is not specified in the parameter
    field of the dataset. In Carma datasets, fuel inputs are expressed in megajoules instead of kilograms.

    :param dataset: a wurst dataset of an electricity-producing technology
    :param fuel_filters: wurst filter to filter fuel input exchanges
    :param energy_out: the amount of energy expect as output, in MJ
    :return: the efficiency value set initially
    """

    if fuel_filters is None:
        fuel_filter_index = prepare_fuel_filters(tuple(fuel_map_reverse))
    elif isinstance(fuel_filters, FuelFilterIndex):
        if not fuel_filters.filters:
            raise ValueError(
                "No fuel filters configured for "
                f"{dataset['name']!r} in {dataset['location']!r}."
            )
        fuel_filter_index = fuel_filters
    else:
        if not fuel_filters:
            raise ValueError(
                "No fuel filters configured for "
                f"{dataset['name']!r} in {dataset['location']!r}."
            )
        fuel_filter_index = prepare_fuel_filters(fuel_filters)

    sanitized_fuel_filters = fuel_filter_index.filters

    energy_input = np.sum(
        np.sum(
            np.asarray(
                [
                    calculate_input_energy(
                        exc["name"],
                        exc["amount"],
                        exc["unit"],
                        fuel_specs,
                        fuel_map_reverse,
                    )
                    for exc in dataset["exchanges"]
                    if fuel_filter_index.matches(exc["name"])
                    and exc["type"] == "technosphere"
                    and exc["amount"] > 0.0
                ]
            )
        )
    )

    if energy_input == 0:
        # try to see if we find instead direct energy flows in "megajoule" or "kilowatt hour"
        energy_input = np.sum(
            np.asarray(
                [
                    exc["amount"] if exc["unit"] == "megajoule" else exc["amount"] * 3.6
                    for exc in dataset["exchanges"]
                    if exc["type"] == "technosphere"
                    and exc["unit"] in ["megajoule", "kilowatt hour"]
                ]
            )
        )
        if energy_input == 0 and not any(
            x in dataset["name"] for x in ("waste", "treatment")
        ):
            technosphere_inputs = [
                f"{exc['name']} ({exc['amount']} {exc['unit']})"
                for exc in dataset["exchanges"]
                if exc["type"] == "technosphere" and exc.get("amount", 0) > 0
            ]
            raise ValueError(
                "No fuel input found for "
                f"{dataset['name']!r} in {dataset['location']!r}. "
                f"Fuel filters: {list(sanitized_fuel_filters)}. "
                f"Technosphere inputs: {technosphere_inputs}."
            )

    if energy_input != 0 and float(energy_out) != 0:
        current_efficiency = float(energy_out) / energy_input
    else:
        current_efficiency = np.nan

    if np.isnan(current_efficiency) or np.isinf(current_efficiency):
        current_efficiency = 1

    if "parameters" in dataset:
        dataset["parameters"]["efficiency"] = current_efficiency
    else:
        dataset["parameters"] = {"efficiency": current_efficiency}

    return current_efficiency


class BaseTransformation:
    """
    Base transformation class.

    :ivar database: wurst database
    :ivar iam_data: IAMDataCollection object_
    :ivar model: IAM model
    :ivar year: database year
    """

    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        cache: dict = None,
        index: dict = None,
    ) -> None:
        self.mapping = None
        self.database: List[dict] = database
        self.iam_data: IAMDataCollection = iam_data
        self.model: str = model
        self.regions: List[str] = iam_data.regions
        self.geo: Geomap = Geomap(model=model)
        self.scenario: str = pathway
        self.year: int = year
        self.version: str = version
        self.fuels_specs: dict = get_fuel_properties()

        self.system_model: str = system_model
        self.cache: dict = cache if cache is not None else {}
        self._gis_match_cache: dict[tuple, tuple] = self.cache.setdefault(
            _SCENARIO_GIS_CACHE_KEY, {}
        )
        self._resolved_row_faces_cache: dict[tuple, set] = self.cache.setdefault(
            _SCENARIO_ROW_CACHE_KEY, {}
        )
        self._validation_relinked_targets: dict[int, dict] = {}
        self.cache[_VALIDATION_RELINKED_TARGETS_KEY] = self._validation_relinked_targets
        storage = getattr(self.database, "_validation_columnar_storage", None)
        if storage is None and self.database:
            storage = getattr(self.database[0], "_storage", None)
        if storage is not None:
            storage._validation_modified_targets = self._validation_relinked_targets
        self._validation_added_targets: dict[int, dict] = {}
        self.cache[_VALIDATION_ADDED_TARGETS_KEY] = self._validation_added_targets
        if isinstance(self.database, IndexedInventoryList):
            self.database.track_validation_additions(self._validation_added_targets)
        # Some sector checks need a short-lived exchange label to distinguish
        # IAM technologies which resolve to the same provider.  Keep the exact
        # owning activities so cleanup is proportional to the generated
        # markets, not to the complete ecoinvent graph.
        self._validation_provenance_targets: dict[str, dict[int, dict]] = {}
        self.ecoinvent_to_iam_loc: Dict[str, str] = {
            loc: self.geo.ecoinvent_to_iam_location(loc)
            for loc in self.get_ecoinvent_locs()
        }
        self.iam_to_ecoinvent_loc = defaultdict(list)
        for key, value in self.ecoinvent_to_iam_loc.items():
            self.iam_to_ecoinvent_loc[value].append(key)

        if index is None:
            self.index = self.create_index()
        elif isinstance(index, defaultdict):
            self.index = index
        else:
            self.index = defaultdict(list, index)
        self._provider_index_generation = 0
        self._provider_group_cache: dict[tuple[int, tuple[str, str]], tuple] = {}
        self._provider_location_cache: dict[tuple[int, tuple[str, str]], set[str]] = {}
        self._provider_semantic_index: dict[tuple[str, str, str], int] | None = None

    def create_index(self):
        idx = defaultdict(list)
        for ds in self.database:
            key = (ds["name"], ds["reference product"])
            idx[key].append(_provider_record(ds))
        return idx

    def add_to_index(self, ds: [dict, list, ValuesView]):
        if isinstance(ds, ValuesView):
            ds = list(ds)

        if isinstance(ds, dict):
            ds = [ds]

        provider_semantics = self._get_provider_semantic_index()
        for d in ds:
            key = (copy.deepcopy(d["name"]), copy.deepcopy(d["reference product"]))
            semantic_key = key[0], key[1], d["location"]
            if semantic_key not in provider_semantics and hasattr(
                self, "_validation_added_targets"
            ):
                # ``add_to_index`` is the common creation boundary used by sector
                # transformations.  Recording the exact object here gives scope
                # validation an independent declaration of intended additions;
                # arbitrary list appends which bypass the creation boundary remain
                # visible as undeclared collateral changes.
                self._validation_added_targets[id(d)] = d
            self.index[key].append(_provider_record(d))
            provider_semantics[semantic_key] = (
                provider_semantics.get(semantic_key, 0) + 1
            )
        self._invalidate_provider_group_cache(preserve_semantic_index=True)

    def remove_from_index(self, ds):
        provider_semantics = self._get_provider_semantic_index()
        key = (copy.deepcopy(ds["name"]), copy.deepcopy(ds["reference product"]))
        available_locations = [k["location"] for k in self.index[key]]
        if ds["location"] in available_locations:
            ds_to_remove = [
                d for d in self.index[key] if d["location"] == ds["location"]
            ][0]
            self.index[key].remove(ds_to_remove)
            semantic_key = key[0], key[1], ds_to_remove["location"]
            if provider_semantics[semantic_key] == 1:
                del provider_semantics[semantic_key]
            else:
                provider_semantics[semantic_key] -= 1
            self._invalidate_provider_group_cache(preserve_semantic_index=True)

    def track_validation_provenance(self, dataset: dict, field_name: str) -> None:
        """Register an activity carrying a transient validation-only field."""

        targets = getattr(self, "_validation_provenance_targets", None)
        if targets is None:
            targets = self._validation_provenance_targets = {}
        targets.setdefault(field_name, {})[id(dataset)] = dataset

    def clear_validation_provenance_field(self, field_name: str) -> None:
        """Remove one transient field from its registered activities only."""

        registry = getattr(self, "_validation_provenance_targets", {})
        targets = registry.pop(field_name, {})
        for dataset in targets.values():
            for exchange in dataset.get("exchanges", ()):
                exchange.pop(field_name, None)

    def _invalidate_provider_group_cache(
        self, *, preserve_semantic_index: bool = False
    ) -> None:
        """Invalidate provider groupings after a mutation to ``self.index``."""

        self._provider_index_generation = (
            getattr(self, "_provider_index_generation", 0) + 1
        )
        self._provider_group_cache = {}
        self._provider_location_cache = {}
        if not preserve_semantic_index:
            self._provider_semantic_index = None

    def _get_provider_semantic_index(self) -> dict[tuple[str, str, str], int]:
        """Return reference-counted provider name, product, and location keys."""

        cached = getattr(self, "_provider_semantic_index", None)
        if cached is not None:
            return cached

        providers: dict[tuple[str, str, str], int] = {}
        for key, datasets in self.index.items():
            for dataset in datasets:
                semantic_key = key[0], key[1], dataset["location"]
                providers[semantic_key] = providers.get(semantic_key, 0) + 1
        self._provider_semantic_index = providers
        return providers

    def _get_provider_locations(
        self,
        key: tuple[str, str],
        possible_datasets: list[dict] | None = None,
    ) -> set[str]:
        """Return provider locations for an exact index key and generation."""

        generation = getattr(self, "_provider_index_generation", 0)
        cache = getattr(self, "_provider_location_cache", None)
        if cache is None:
            cache = self._provider_location_cache = {}
        cache_key = (generation, key)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        if possible_datasets is None:
            possible_datasets = self.index[key]
        locations = {dataset["location"] for dataset in possible_datasets}
        cache[cache_key] = locations
        return locations

    def _get_provider_groups(self, exchange: dict) -> tuple:
        """Return ordered provider candidates and their location groupings.

        Group construction is independent of the consumer location but used for
        every exchange resolution. Cache entries are tied to the provider-index
        generation so proxy additions and removals cannot leave stale supplier
        sets behind. The returned list and location order exactly follow the
        legacy index.
        """

        key = (exchange["name"], exchange["product"])
        possible_datasets = self.index[key]
        if len(possible_datasets) == 0 and "market for" in exchange["name"]:
            key = (
                exchange["name"].replace("market for", "market group for"),
                exchange["product"],
            )
            possible_datasets = self.index[key]

        generation = getattr(self, "_provider_index_generation", 0)
        cache = getattr(self, "_provider_group_cache", None)
        if cache is None:
            cache = self._provider_group_cache = {}
        cache_key = (generation, key)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        possible_locations = [dataset["location"] for dataset in possible_datasets]
        locations_set = self._get_provider_locations(key, possible_datasets)
        by_location = defaultdict(list)
        for dataset in possible_datasets:
            by_location[dataset["location"]].append(dataset)
        grouped = (
            key,
            possible_datasets,
            possible_locations,
            locations_set,
            by_location,
        )
        cache[cache_key] = grouped
        return grouped

    def is_in_index(self, ds, location=None):
        if "reference product" in ds:
            product = ds["reference product"]
        elif "product" in ds:
            product = ds["product"]
        else:
            raise KeyError(
                f"Dataset {ds['name']} does not have neither 'reference product' nor 'product' keys."
            )

        target_location = ds["location"] if location is None else location
        cached = getattr(self, "_provider_semantic_index", None)
        providers = self._get_provider_semantic_index() if cached is None else cached
        return (
            ds["name"],
            product,
            target_location,
        ) in providers

    def get_ecoinvent_locs(self) -> List[str]:
        """
        Rerun a list of unique locations in ecoinvent

        :return: list of locations
        :rtype: list
        """

        locs = list(set(a["location"] for a in self.database))

        # add Laos
        if "LA" not in locs:
            locs.append("LA")

        # add Fiji
        if "FJ" not in locs:
            locs.append("FJ")

        # add Guinea
        if "GN" not in locs:
            locs.append("GN")

        # add Guyana
        if "GY" not in locs:
            locs.append("GY")

        # add Sierra Leone
        if "SL" not in locs:
            locs.append("SL")

        # add Solomon Islands
        if "SB" not in locs:
            locs.append("SB")

        # add Uganda
        if "UG" not in locs:
            locs.append("UG")

        # add Afghanistan
        if "AF" not in locs:
            locs.append("AF")

        return locs

    def update_ecoinvent_efficiency_parameter(
        self, dataset: dict, old_ei_eff: float, new_eff: float
    ) -> None:
        """
        Update the old efficiency value in the ecoinvent dataset by the newly calculated one.
        :param dataset: dataset
        :param old_ei_eff: conversion efficiency of the original ecoinvent dataset
        :param new_eff: new conversion efficiency
        :return: nothing. Modifies the `comment` and `parameters` fields of the dataset.
        """
        parameters = dataset["parameters"]
        possibles = ["efficiency", "efficiency_oil_country", "efficiency_electrical"]

        if any(i in dataset for i in possibles):
            for key in possibles:
                if key in parameters:
                    dataset["parameters"][key] = new_eff
        else:
            dataset["parameters"]["efficiency"] = new_eff

        if dataset["location"] in self.regions:
            iam_region = dataset["location"]
        else:
            iam_region = self.ecoinvent_to_iam_loc[dataset["location"]]

        new_txt = (
            f" 'premise' has modified the efficiency of this dataset, from an original "
            f"{int(old_ei_eff * 100)}% to {int(new_eff * 100)}%, according to IAM model {self.model.upper()}, scenario {self.scenario} "
            f"for the region {iam_region}."
        )

        if "comment" in dataset:
            dataset["comment"] += new_txt
        else:
            dataset["comment"] = new_txt

    def get_iam_mapping(
        self, activity_map: dict, fuels_map: dict, technologies: list
    ) -> Dict[str, Any]:
        """
        Define filter functions that decide which wurst datasets to modify.
        :param activity_map: a dictionary that contains 'technologies' as keys and activity names as values.
        :param fuels_map: a dictionary that contains 'technologies' as keys and fuel names as values.
        :param technologies: a list of IAM technologies.
        :return: dictionary that contains filters and functions
        :rtype: dict
        """

        return {
            tech: {
                "IAM_eff_func": self.find_iam_efficiency_change,
                "current_eff_func": find_fuel_efficiency,
                "technology filters": activity_map[tech],
                "fuel filters": fuels_map[tech],
            }
            for tech in technologies
        }

    def get_technology_and_regional_production_shares(
        self, production_volumes: xr.DataArray, mapping: dict
    ) -> (
        tuple[None, dict[tuple[Any, str], float], dict[str, float]]
        | tuple[DataArray, dict[tuple[Any, Any], Any], dict[Any, Any]]
    ):

        regions = [region for region in self.regions if region != "World"]
        year = self.year
        if self.year < production_volumes.year.values.min():
            year = production_volumes.year.values.min()
        if self.year > production_volumes.year.values.max():
            year = production_volumes.year.values.max()

        available_variables = set(production_volumes.variables.values.tolist())
        mapped_variables = [v for v in mapping.keys() if v in available_variables]

        if not mapped_variables:
            return (
                None,
                {(var, reg): 0.0 for var in mapping.keys() for reg in regions},
                {reg: 1 / len(regions) for reg in regions},
            )

        try:
            variables = mapped_variables
            if year in production_volumes.year.values:
                production_volumes = production_volumes.sel(
                    variables=variables, region=regions, year=year
                )
            else:
                production_volumes = production_volumes.sel(
                    variables=variables, region=regions
                ).interp(year=year)
        except KeyError:
            raise KeyError(
                f"The variable(s) {[v for v in list(mapping.keys()) if v not in production_volumes.variables.values]} not "
                f"found in production volumes data. "
                f"Available variables: {sorted(list(production_volumes.variables.values))}."
            )
        regional_totals = production_volumes.sum(dim="variables")

        regional_shares = (production_volumes / regional_totals).fillna(0)
        world_shares = (regional_totals / regional_totals.sum()).fillna(0)

        # The reductions above define the numerical semantics. Extract their
        # already-computed values in coordinate order rather than issuing one
        # scalar xarray selection per variable and region (thousands of calls
        # for final-energy mappings).
        variables = regional_shares.variables.values
        share_regions = regional_shares.region.values
        share_values = regional_shares.transpose("variables", "region").values
        technology_shares_dict = {
            (var, reg): share_values[var_idx, region_idx].item()
            for var_idx, var in enumerate(variables)
            for region_idx, reg in enumerate(share_regions)
        }

        world_regions = world_shares.region.values
        world_values = world_shares.transpose("region").values
        regional_shares_dict = {
            reg: world_values[region_idx].item()
            for region_idx, reg in enumerate(world_regions)
        }

        return production_volumes, technology_shares_dict, regional_shares_dict

    def add_geo_definition_metadata(self, dataset):

        if dataset["location"] in self.regions:
            geo_coverage = self.iam_to_ecoinvent_loc[dataset["location"]]

            if "comment" in dataset:
                dataset[
                    "comment"
                ] += f" This IAM region covers the following ecoinvent location: {geo_coverage}"
            else:
                dataset["comment"] = (
                    f"This IAM region covers the following ecoinvent location: {geo_coverage}"
                )

        return dataset

    @staticmethod
    def is_treatment_supplier(dataset: dict) -> bool:
        """Return whether a supplier represents a waste-treatment activity."""
        return dataset.get("name", "").strip().lower().startswith("treatment")

    def process_and_add_markets(
        self,
        name,
        reference_product,
        unit,
        mapping,
        production_volumes=None,
        technology_shares=None,
        additional_exchanges_fn=None,
        system_model="cut-off",
        blacklist=None,
        conversion_factor=None,
        flip_treatment_supplier_sign=False,
        retain_validation_technology=False,
    ):
        """
        Generalized method to create and add regionalized market datasets.

        Parameters
        ----------
        production_volumes : xarray.DataArray, optional
            Absolute production volumes. These determine production-volume
            metadata and regional weights, and supply the technology shares
            unless ``technology_shares`` is provided.
        technology_shares : xarray.DataArray, optional
            Separate technology mix used for the market's supplier shares.
            Consequential markets use this to retain marginal mixes while
            keeping absolute production volumes for regional weighting.
        retain_validation_technology : bool, optional
            Retain a transient IAM-technology label on supplier exchanges so
            an incremental validator can distinguish technologies sharing the
            same provider. Callers must remove these labels after validation.
        additional_exchanges_fn : callable, optional
            Function to add extra exchanges to the market dataset (e.g., transport, losses).
        """

        conversion_factor = conversion_factor or {}
        blacklist = blacklist or {}

        if production_volumes is not None:
            regions = [region for region in self.regions if region != "World"]
            production_volumes, technology_shares_dict, regional_shares_dict = (
                self.get_technology_and_regional_production_shares(
                    production_volumes=production_volumes,
                    mapping=mapping,
                )
            )
            if technology_shares is not None:
                _, technology_shares_dict, _ = (
                    self.get_technology_and_regional_production_shares(
                        production_volumes=technology_shares,
                        mapping=mapping,
                    )
                )

        else:
            regions = self.regions
            technology_shares_dict = {
                (var, reg): 0.0 for var in mapping.keys() for reg in regions
            }
            regional_shares_dict = {reg: 1 / len(regions) for reg in regions}

        preserved_exchanges = self.extract_market_ancillary_exchanges(
            name=name,
            reference_product=reference_product,
            market_unit=unit,
        )

        for region in regions:
            if production_volumes is not None:
                production_volume = float(
                    production_volumes.sel(
                        region=region,
                    )
                    .sum(dim="variables")
                    .values.item(0)
                )

                if production_volume == 0:
                    continue
            else:
                production_volume = 0.0

            production_exchange = {
                "name": name,
                "product": reference_product,
                "location": region,
                "amount": 1.0,
                "unit": unit,
                "uncertainty type": 0,
                "type": "production",
            }

            if production_volumes is not None:
                production_exchange["production volume"] = production_volume

            market_dataset = {
                "name": name,
                "reference product": reference_product,
                "location": region,
                "unit": unit,
                "regionalized": True,
                "code": str(uuid.uuid4().hex),
                "database": "",
                "comment": f"Market dataset for {name} in {region} for {self.year}.",
                "exchanges": [
                    production_exchange,
                ],
            }

            # add geographical coverage definition
            self.add_geo_definition_metadata(market_dataset)

            for technology, activities in mapping.items():
                if (technology, region) in technology_shares_dict:
                    share = technology_shares_dict.get((technology, region), 0)
                    if share <= 0:
                        continue

                    suppliers = [ds for ds in activities if ds["location"] == region]
                    if len(suppliers) == 0:
                        suppliers = [
                            ds
                            for ds in activities
                            if ds["location"] in self.iam_to_ecoinvent_loc[region]
                        ]
                    if len(suppliers) == 0:
                        suppliers = [ds for ds in activities if ds["location"] == "RoW"]
                    if len(suppliers) == 0:
                        raise ValueError(
                            f"No activity found for technology {technology} in region {region}. "
                            f"Available activities: {[(a['name'], a['location']) for a in activities]}."
                        )

                    suppliers = self.deduplicate_market_suppliers(suppliers)

                    if len(suppliers) > 1:
                        suppliers = get_shares_from_production_volume(suppliers)

                    if technology not in blacklist.get(system_model, []):
                        factor = conversion_factor.get(
                            (technology, region),
                            conversion_factor.get(technology, 1.0),
                        )
                        for supplier in suppliers:
                            exchange = {
                                "name": supplier["name"],
                                "product": supplier["reference product"],
                                "location": supplier["location"],
                                "amount": share * factor * supplier.get("share", 1.0),
                                "unit": supplier["unit"],
                                "uncertainty type": 0,
                                "type": "technosphere",
                            }
                            if retain_validation_technology:
                                exchange["premise market technology"] = technology
                            market_dataset["exchanges"].append(exchange)

            market_dataset["exchanges"] = self.summarize_market_exchanges(
                market_dataset["exchanges"]
            )

            # normalize the shares
            total_share = sum(
                exc["amount"]
                for exc in market_dataset["exchanges"]
                if exc["type"] == "technosphere"
            )
            if total_share > 0:
                for exc in market_dataset["exchanges"]:
                    if exc["type"] == "technosphere":
                        exc["amount"] /= total_share
            else:
                continue

            if flip_treatment_supplier_sign and system_model == "cutoff":
                for exc in market_dataset["exchanges"]:
                    if exc["type"] == "technosphere" and self.is_treatment_supplier(
                        exc
                    ):
                        exc["amount"] = -abs(exc["amount"])

            if additional_exchanges_fn:
                additional_exchanges_fn(market_dataset)

            # Preserve ancillary legacy exchanges:
            # - all biosphere exchanges
            # - technosphere exchanges with a unit different from market unit
            source_location = [
                loc
                for loc in preserved_exchanges.keys()
                if loc in self.iam_to_ecoinvent_loc[region]
            ]

            if len(source_location) == 0:
                source_location = [
                    loc for loc in preserved_exchanges.keys() if loc == "RoW"
                ]

            if len(source_location) == 0:
                source_location = [
                    loc for loc in preserved_exchanges.keys() if loc == "GLO"
                ]

            if len(source_location) > 0:
                for exc in preserved_exchanges[source_location[0]]:
                    new_exc = copy.deepcopy(exc)
                    # These links are rebuilt at export/relink time.
                    new_exc.pop("input", None)
                    market_dataset["exchanges"].append(new_exc)

            if retain_validation_technology:
                self.track_validation_provenance(
                    market_dataset, "premise market technology"
                )

            self.database.append(market_dataset)
            self.add_to_index(market_dataset)
            self.write_log(market_dataset, "created")

        if production_volumes is not None:
            if (
                "World" not in regions
                and production_volumes.sel(
                    region=[reg for reg in regions if reg != "World"]
                )
                .sum(dim="region")
                .sum()
                .values.item(0)
                > 0
            ):

                # create the World market
                world_market = {
                    "name": name,
                    "reference product": reference_product,
                    "location": "World",
                    "unit": unit,
                    "regionalized": True,
                    "code": str(uuid.uuid4().hex),
                    "database": "",
                    "comment": f"Market dataset for {name} in World for {self.year}.",
                    "exchanges": [
                        {
                            "name": name,
                            "product": reference_product,
                            "location": "World",
                            "amount": 1.0,
                            "unit": unit,
                            "uncertainty type": 0,
                            "type": "production",
                        }
                    ],
                }

                candidate = {
                    "name": name,
                    "reference product": reference_product,
                    "unit": unit,
                }
                for region in regions:
                    share = regional_shares_dict.get(region, 0)

                    if share > 0:
                        if self.is_in_index(candidate, region):
                            # add the regional market shares
                            world_market["exchanges"].append(
                                {
                                    "name": name,
                                    "product": reference_product,
                                    "location": region,
                                    "amount": share,
                                    "unit": unit,
                                    "uncertainty type": 0,
                                    "type": "technosphere",
                                }
                            )

                self.database.append(world_market)
                self.add_to_index(world_market)
                self.write_log(world_market, "created")

        datasets = list(
            ws.get_many(
                self.database,
                ws.equals("name", name),
                ws.equals("reference product", reference_product),
            )
        )
        datasets = [ds for ds in datasets if ds.get("regionalized", False) is False]

        self.empty_original_datasets(
            datasets=datasets,
            loc_map={
                x["location"]: self.geo.ecoinvent_to_iam_location(x["location"])
                for x in datasets
            },
            production_shares=regional_shares_dict,
            regions=regions,
        )

    @staticmethod
    def deduplicate_market_suppliers(suppliers: List[dict]) -> List[dict]:
        """
        Return one supplier per linkable market key.

        Some mappings can yield the same regionalized supplier more than once. If
        those duplicates are kept, production-volume weighting overrepresents that
        supplier before market shares are written.
        """

        unique_suppliers = []
        seen = set()

        for supplier in suppliers:
            key = (
                supplier["name"],
                supplier["reference product"],
                supplier["location"],
                supplier["unit"],
            )
            if key in seen:
                continue
            seen.add(key)
            unique_suppliers.append(supplier)

        return unique_suppliers

    @staticmethod
    def summarize_market_exchanges(exchanges: List[dict]) -> List[dict]:
        """
        Sum duplicate technosphere exchanges in generated market datasets.

        This keeps the market matrix compact when several IAM technologies map to
        the same supplier dataset, while preserving production exchanges as
        separate rows.
        """

        summarized = []
        technosphere_by_key = {}

        for exchange in exchanges:
            if exchange.get("type") != "technosphere":
                summarized.append(exchange)
                continue

            key = (
                exchange.get("name"),
                exchange.get("product"),
                exchange.get("location"),
                exchange.get("unit"),
                exchange.get("premise market technology"),
            )
            if key not in technosphere_by_key:
                technosphere_by_key[key] = copy.deepcopy(exchange)
                summarized.append(technosphere_by_key[key])
                continue

            technosphere_by_key[key]["amount"] += exchange["amount"]

        return summarized

    def extract_market_ancillary_exchanges(
        self,
        name: str,
        reference_product: str,
        market_unit: str,
    ) -> Dict[str, List[dict]]:
        datasets = list(
            ws.get_many(
                self.database,
                ws.equals("name", name),
                ws.equals("reference product", reference_product),
            )
        )

        preserved_exchanges = defaultdict(list)

        for dataset in datasets:
            for exc in ws.biosphere(dataset):
                preserved_exchanges[dataset["location"]].append(exc)

            for exc in ws.technosphere(dataset):
                # Keep ancillary market inputs (e.g., transport, losses, electricity),
                # while market-supply links (same unit as market) are rebuilt from IAM shares.
                if exc.get("unit") != market_unit:
                    preserved_exchanges[dataset["location"]].append(exc)

        return preserved_exchanges

    def process_and_add_activities(
        self,
        mapping,
        production_volumes=None,
        efficiency_adjustment_fn=None,
        regions=None,
        scaling_factors=None,
    ):
        """
        Generalized processing of activities and adding them to the database.

        Parameters
        ----------
        mapping : dict or pd.DataFrame
            Mapping containing the relevant activity data.
        proxy_selection_fn : callable
            Function to fetch proxies based on the mapping.
        efficiency_adjustment_fn : callable, optional
            Function to adjust process efficiency. If None, no adjustment is done.
        scaling_factors : dict, optional
            Dictionary with scaling factors for inputs.
        log_message : str, optional
            Message to log after processing.
        add_to_index : bool, optional
            Whether to add new activities to the index.
        """

        # Some callers build several mapping keys from the same source list. This
        # method mutates mapping values to expose regionalized datasets to later
        # market construction, so only duplicated list objects need a defensive copy.
        mapping_value_ids = defaultdict(int)
        for activities in mapping.values():
            mapping_value_ids[id(activities)] += 1

        shared_mapping_value_ids = {
            value_id for value_id, count in mapping_value_ids.items() if count > 1
        }
        for technology, activities in list(mapping.items()):
            if id(activities) in shared_mapping_value_ids:
                mapping[technology] = list(activities)

        def dataset_identity(dataset):
            return (
                dataset.get("name"),
                dataset.get("reference product"),
                dataset.get("location"),
            )

        def append_unique_datasets(target, additions):
            existing = {dataset_identity(dataset) for dataset in target}
            for dataset in additions:
                identity = dataset_identity(dataset)
                if identity not in existing:
                    target.append(dataset)
                    existing.add(identity)

        if production_volumes is not None:
            regions = regions or [
                region for region in self.regions if region != "World"
            ]
            production_volumes, _, regional_shares_dict = (
                self.get_technology_and_regional_production_shares(
                    production_volumes=production_volumes,
                    mapping=mapping,
                )
            )

        else:
            regions = regions or self.regions
            regional_shares_dict = {reg: 1 / len(regions) for reg in regions}

        processed_datasets, seen_datasets, processed_by_key = [], set(), {}

        # resize production volumes to the keys available in mapping
        if production_volumes is not None:
            production_volumes = production_volumes.sel(
                variables=[
                    v
                    for v in list(mapping.keys())
                    if v in production_volumes.variables.values
                ]
            )

        for technology, grouped_activities in mapping.items():
            reused_regionalized_datasets = []
            for ds in grouped_activities:
                dataset_key = (ds["name"], ds["reference product"])
                reused_regionalized_datasets.extend(
                    processed_by_key.get(dataset_key, [])
                )

            if reused_regionalized_datasets:
                append_unique_datasets(
                    mapping[technology], reused_regionalized_datasets
                )

            grouped_activities = [
                ds
                for ds in mapping[technology]
                if (ds["name"], ds["reference product"]) not in seen_datasets
            ]

            if not grouped_activities:
                continue

            grouped_activities = group_dicts_by_keys(
                grouped_activities, ["name", "reference product"]
            )

            for activities in grouped_activities:
                if not activities:
                    continue

                existing_regionalized_locs = {
                    ds["location"] for ds in activities if ds.get("regionalized", False)
                }
                if existing_regionalized_locs and all(
                    region in existing_regionalized_locs for region in regions
                ):
                    # if any of the datasets in the activity
                    # is already regionalized, skip it
                    append_unique_datasets(
                        mapping[technology],
                        [ds for ds in activities if ds.get("regionalized", True)],
                    )
                    continue

                regions_to_process = [
                    r for r in regions if r not in existing_regionalized_locs
                ]
                if not regions_to_process:
                    continue

                prod_vol = None
                if production_volumes is not None:
                    if technology in production_volumes.coords["variables"].values:
                        prod_vol = production_volumes.sel(variables=technology)

                datasets_to_regionalize = [
                    ds for ds in activities if not ds.get("regionalized", False)
                ]

                if len(datasets_to_regionalize) > 0:

                    regionalized_datasets = self.fetch_proxies(
                        # datasets=activities,
                        datasets=[
                            ds for ds in activities if not ds.get("regionalized", False)
                        ],
                        production_volumes=prod_vol,
                        regions=regions_to_process,
                    )

                    # add geographical coverage definition
                    for ds in regionalized_datasets.values():
                        self.add_geo_definition_metadata(ds)

                    # adjust efficiency of steel production
                    if efficiency_adjustment_fn:
                        for dataset in regionalized_datasets.values():
                            if isinstance(efficiency_adjustment_fn, list):
                                for fn in efficiency_adjustment_fn:
                                    fn(dataset, technology)
                            else:
                                efficiency_adjustment_fn(dataset, technology)

                    regionalized_datasets = list(regionalized_datasets.values())
                    # Original datasets can only be emptied once their IAM
                    # replacements are visible to the provider index.
                    self.add_to_index(regionalized_datasets)
                    processed_datasets.extend(regionalized_datasets)
                    seen_datasets.update(
                        (ds["name"], ds["reference product"]) for ds in activities
                    )
                    for ds in activities:
                        processed_by_key[(ds["name"], ds["reference product"])] = (
                            regionalized_datasets
                        )
                    append_unique_datasets(mapping[technology], regionalized_datasets)

                    datasets = list(
                        ws.get_many(
                            self.database,
                            ws.equals("name", activities[0]["name"]),
                            ws.equals(
                                "reference product", activities[0]["reference product"]
                            ),
                        )
                    )
                    datasets = [
                        ds for ds in datasets if ds.get("regionalized", False) is False
                    ]

                    self.empty_original_datasets(
                        datasets=datasets,
                        loc_map={
                            x["location"]: self.geo.ecoinvent_to_iam_location(
                                x["location"]
                            )
                            for x in datasets
                        },
                        production_shares=regional_shares_dict,
                        regions=regions,
                    )

        for dataset in processed_datasets:
            self.write_log(dataset, "created")
            self.database.append(dataset)

    def region_to_proxy_dataset_mapping(
        self, datasets: list[dict], regions: List[str] = None
    ) -> Dict[str, dict]:
        d_map = {
            self.ecoinvent_to_iam_loc[d["location"]]: d
            for d in datasets
            if d["location"] not in self.regions
        }

        if not regions:
            regions = self.regions

        locs = {x["location"]: x for x in datasets}

        if "RoW" in locs:
            fallback_dataset = locs["RoW"]
        else:
            if "GLO" in locs:
                fallback_dataset = locs["GLO"]
            else:
                fallback_dataset = list(locs.values())[0]

        return {region: d_map.get(region, fallback_dataset) for region in regions}

    def fetch_proxies(
        self,
        datasets: List[dict],
        production_volumes: xr.DataArray = None,
        relink=True,
        regions=None,
        geo_mapping: dict = None,
        delete_original_datasets=False,
        unlist=True,
    ) -> Dict[str, dict]:
        """
        Fetch dataset proxies, given a dataset `name` and `reference product`.
        Store a copy for each IAM region.
        If a fitting ecoinvent location cannot be found for a given IAM region,
        fetch a dataset with a "RoW" location.
        Delete original datasets from the database.

        :param production_variable: name of variable in IAM data that refers to production volume
        :param relink: if `relink`, exchanges from the datasets will be relinked to
        the most geographically-appropriate providers from the database. This is computer-intensive.
        :param regions: regions to create proxy datasets for. if None, all regions are considered.
        :param delete_original_datasets: if True, delete original datasets from the database.
        :param empty_original_activity: if True, empty original activities from exchanges.
        :param unlist: if True, remove original datasets from the index.
        :return: dictionary with IAM regions as keys, proxy datasets as values.
        """

        if not isinstance(datasets, list):
            datasets = [datasets]

        d_iam_to_eco = geo_mapping or self.region_to_proxy_dataset_mapping(
            datasets=datasets, regions=regions
        )

        production_volume_by_region = {}
        world_production_volume = None
        if production_volumes is not None:
            production_regions = production_volumes.region.values
            region_axis = production_volumes.get_axis_num("region")
            production_values = np.moveaxis(
                production_volumes.values, region_axis, 0
            ).reshape(len(production_regions), -1)
            production_volume_by_region = {
                region: production_values[position].item(0)
                for position, region in enumerate(production_regions)
            }
            world_production_volume = production_volumes.sum(dim="region").values.item(
                0
            )

        d_act = {}
        for region, dataset in d_iam_to_eco.items():

            if self.is_in_index(dataset, region):
                # delete original dataset from the database
                self.database[:] = [
                    d
                    for d in self.database
                    if (d["name"], d["reference product"], d["location"])
                    != (dataset["name"], dataset["reference product"], region)
                ]

            dataset = clone_inventory_dataset(dataset)
            dataset["location"] = region
            dataset["code"] = str(uuid.uuid4().hex)
            dataset["regionalized"] = True

            for exc in ws.production(dataset):
                if "input" in exc:
                    del exc["input"]
                if "location" in exc:
                    exc["location"] = region

            if "input" in dataset:
                del dataset["input"]

            for prod in ws.production(dataset):
                prod["location"] = region
                if production_volumes is not None:
                    # Add `production volume` field
                    if region in production_volume_by_region:
                        prod["production volume"] = float(
                            production_volume_by_region[region]
                        )
                    else:
                        if region == "World":
                            # If the region is "World", use the total production volume
                            prod["production volume"] = float(world_production_volume)
                        else:
                            raise KeyError(
                                f"Region {region} not found in production volumes data."
                            )
                else:
                    prod["production volume"] = 0.0

            if relink:
                d_act[region] = self.relink_technosphere_exchanges(dataset)

        if unlist:
            for dataset in datasets:
                self.remove_from_index(dataset)

        if delete_original_datasets is True:
            # remove the dataset from `self.database`
            self.database[:] = [ds for ds in self.database if ds not in datasets]

        return d_act

    def empty_original_datasets(
        self,
        datasets: list[dict],
        production_shares: dict,
        loc_map: Dict[str, str],
        regions: List[str] = None,
    ) -> None:
        """
        Empty original ecoinvent datasets and replace them with IAM-based inputs.
        """
        regions = regions or self.regions

        def build_exchange(dataset, location, amount):
            return {
                "name": dataset["name"],
                "product": dataset["reference product"],
                "amount": amount,
                "unit": dataset["unit"],
                "uncertainty type": 0,
                "location": location,
                "type": "technosphere",
            }

        for dataset in datasets:
            if dataset.get("regionalized", False) is True:
                print(
                    f"Skipping {dataset['name']} in {dataset['location']}, already regionalized."
                )
                continue

            ecoinvent_location = dataset["location"]
            iam_location = loc_map[ecoinvent_location]

            if not self.is_in_index(dataset, iam_location):
                iam_location = "World"

            if iam_location == "World":
                iam_location = [
                    r for r in regions if r != "World" and self.is_in_index(dataset, r)
                ]

            if not iam_location:
                continue

            if isinstance(iam_location, str):
                iam_location = [iam_location]

            # Clean dataset
            dataset["has_downstream_consumer"] = False
            dataset["exchanges"] = [
                e for e in dataset["exchanges"] if e["type"] == "production"
            ]

            # Empty production volume
            for prod in ws.production(dataset):
                prod["production volume"] = 0.0

            dataset["emptied"] = True
            dataset.pop("adjust efficiency", None)

            if not dataset["exchanges"]:
                print(
                    f"ISSUE: no exchanges found in {dataset['name']} in {ecoinvent_location}"
                )

            # Add new exchanges
            if len(iam_location) == 1:
                dataset["exchanges"].append(
                    build_exchange(dataset, iam_location[0], 1.0)
                )

            else:
                dataset["exchanges"].extend(
                    [
                        build_exchange(dataset, loc, production_shares.get(loc, 0))
                        for loc in iam_location
                    ]
                )

            self.write_log(dataset=dataset, status="empty")
            self.remove_from_index(dataset)

    def relink_datasets(self, excludes_datasets=None, alt_names=None):
        """
        For a given exchange name, product, and unit, change its location to an IAM location,
        to effectively link to the newly built market(s)/activity(ies).
        :param excludes_datasets: list of datasets to exclude from relinking
        :param alt_names: list of alternative names to use for relinking
        """

        # Simplify default arguments
        alt_names = alt_names or []
        excludes_datasets = excludes_datasets or []

        provider_semantics = self._get_provider_semantic_index()
        for act in ws.get_many(
            self.database, ws.doesnt_contain_any("name", excludes_datasets)
        ):
            # Filter out exchanges to relink. Compact inventories can decode
            # unchanged typed columns and sparse overlays without generic
            # mapping dispatch; generic/third-party inventories retain the
            # established ordered loop.
            candidate_iterator = getattr(
                self.database, "_premise_relink_candidates", None
            )
            candidates = (
                candidate_iterator(act, provider_semantics)
                if candidate_iterator is not None
                else None
            )
            if candidates is None:
                candidates = _iter_generic_relink_candidates(
                    act["exchanges"], provider_semantics
                )
            selected_candidates = list(candidates)
            excs_to_relink = [exchange for exchange, _ in selected_candidates]

            if len(excs_to_relink) == 0:
                continue

            old_uncertainty = {}

            for exc, fields in selected_candidates:
                name, effective_product, _location, unit, amount = fields
                if exc.get("uncertainty type", 0) != 0:
                    old_uncertainty[(name, effective_product, unit)] = {
                        "uncertainty type": exc.get("uncertainty type", 0),
                        "amount": amount,
                        "loc": exc.get("loc"),
                        "scale": exc.get("scale"),
                        "minimum": exc.get("minimum", 0),
                        "maximum": exc.get("maximum", 0),
                    }

            # make a dictionary with the names and amounts
            # of the technosphere exchanges to relink
            # to compare with the new exchanges
            excs_to_relink_dict = defaultdict(float)
            relink_amounts = defaultdict(float)
            for _, fields in selected_candidates:
                name, effective_product, location, unit, amount = fields
                excs_to_relink_dict[effective_product] += amount
                relink_amounts[(name, effective_product, location, unit)] += amount

            # Create a set of unique exchanges to relink
            # turn this into a list of dictionaries
            unique_excs_to_relink = [
                dict(items)
                for items in {
                    (
                        ("name", fields[0]),
                        ("product", fields[1]),
                        ("location", fields[2]),
                        ("unit", fields[3]),
                    )
                    for _, fields in selected_candidates
                }
            ]

            # Process exchanges to relink
            new_exchanges = self.process_exchanges_to_relink(
                act,
                unique_excs_to_relink,
                alt_names,
                relink_amounts=relink_amounts,
            )

            # apply uncertainties, if any
            if old_uncertainty:
                for exc in new_exchanges:
                    key = (exc["name"], exc["product"], exc["unit"])
                    if key in old_uncertainty:
                        exc["uncertainty type"] = old_uncertainty[key][
                            "uncertainty type"
                        ]
                        loc, scale, minimum, maximum, negative = (
                            redefine_uncertainty_params(old_uncertainty[key], exc)
                        )

                        if loc:
                            exc["loc"] = float(loc)

                        if scale:
                            exc["scale"] = float(scale)

                        if minimum:
                            exc["minimum"] = float(minimum)

                        if maximum:
                            exc["maximum"] = float(maximum)

                        if negative:
                            exc["negative"] = float(negative)

            # ``excs_to_relink`` contains the exact objects selected from this
            # exchange list. Identity membership avoids quadratic full-mapping
            # equality checks, which are especially expensive for lazy compact
            # exchange views. The selection predicate is content-based, so
            # equal duplicate exchanges are selected together.
            act["exchanges"] = _exclude_exchange_objects(
                act["exchanges"], excs_to_relink
            )
            # Update act["exchanges"] by adding new exchanges
            act["exchanges"].extend(new_exchanges)
            self._validation_relinked_targets[id(act)] = act

            new_exchanges_dict = defaultdict(float)
            for exc in new_exchanges:
                new_exchanges_dict[exc["product"]] += exc["amount"]

            # compare with the original exchanges
            # if the amount is different, add a log
            for key in excs_to_relink_dict:
                if key not in new_exchanges_dict:
                    raise RelinkingInvariantError(
                        f"Product {key!r} disappeared while relinking "
                        f"{act['name']!r} in {act['location']!r}."
                    )
                if not np.isclose(
                    excs_to_relink_dict[key],
                    new_exchanges_dict[key],
                    rtol=0.001,
                ):
                    raise RelinkingInvariantError(
                        f"Relinking {act['name']!r} in {act['location']!r} "
                        f"changed {key!r} from {excs_to_relink_dict[key]!r} to "
                        f"{new_exchanges_dict[key]!r}."
                    )

    def process_exchanges_to_relink(
        self,
        act,
        unique_excs_to_relink,
        alt_names,
        relink_amounts=None,
    ):
        new_exchanges = []
        for exc in unique_excs_to_relink:
            amount = None
            if relink_amounts is not None:
                amount = relink_amounts[
                    (
                        exc["name"],
                        exc["product"],
                        exc["location"],
                        exc["unit"],
                    )
                ]
            entries, amount = self.find_new_exchange_entries(
                act,
                exc,
                alt_names,
                amount=amount,
            )
            if amount != 0:
                new_exchanges.extend(self.create_new_exchanges(entries, amount))
        # Make exchanges unique and sum amounts for duplicates
        return self.summarize_exchanges(new_exchanges)

    def get_exchange_from_cache(self, exc, loc):
        key = (
            exc["name"],
            exc["product"],
            exc["location"],
            exc["unit"],
        )

        return self.cache.get(loc, {}).get(self.model, {}).get(key)

    def find_alternative_locations(self, act, exc, alt_names):
        """
        Find alternative locations for an exchange, trying "market for" and "market group for"
        only if the initial search is unsuccessful.

        :param act: The activity dictionary.
        :param exc: A tuple representing the exchange (name, product, location, unit).
        :param alt_names: A list of alternative names to use for relinking.
        :return: A list of new exchange entries or an empty list if none are found.
        """
        names_to_look_for = [exc["name"]] + alt_names

        def allocate_exchanges(lst):
            """
            Allocate the input exchanges in ``lst`` to ``exc``,
            using production volumes where possible, and equal splitting otherwise.
            Always uses equal splitting if ``RoW`` is present.
            """

            pvs = []
            for o in lst:
                try:
                    ds = ws.get_one(
                        self.database,
                        ws.equals("name", o[0]),
                        ws.equals("reference product", o[1]),
                        ws.equals("location", o[2]),
                    )

                except ws.NoResults:
                    raise ws.NoResults(
                        f"Can't find {o[0]} {o[1]} {o[2]} in the database"
                    )

                for exc in ds["exchanges"]:
                    if exc["type"] == "production":
                        pvs.append(exc.get("production volume", 0))

            if any((x > 0 for x in pvs)):
                # Allocate using production volume
                total = sum(pvs)
            else:
                # Allocate evenly
                total = len(lst)
                pvs = [1 for _ in range(total)]

            return [p / total for p in pvs]

        # Function to search for new exchanges
        def search_for_new_exchanges(names):
            entries = []
            for name_to_look_for, alt_loc in product(
                set(names), set(alternative_locations)
            ):
                if (name_to_look_for, alt_loc) != (act["name"], act["location"]):
                    if self.is_in_index(
                        {
                            "name": name_to_look_for,
                            "product": exc["product"],
                            "location": alt_loc,
                            "unit": exc["unit"],
                        }
                    ):
                        entries.append(
                            (
                                name_to_look_for,
                                exc["product"],
                                alt_loc,
                                exc["unit"],
                                1.0,
                            )
                        )

            if len(entries) > 1 and any(
                x in ["World", "GLO", "RoW"] for x in [e[2] for e in entries]
            ):
                entries = [e for e in entries if e[2] not in ["World", "GLO", "RoW"]]

            if len(entries) > 1:
                shares = allocate_exchanges(entries)
                entries = [(e[0], e[1], e[2], e[3], s) for e, s in zip(entries, shares)]
                # remove entries that have a share of 0
                entries = [e for e in entries if e[-1] > 0]

            return entries

        # Start with the activity's location
        alternative_locations = [
            act["location"],
        ]

        # Add alternative locations based on mapping
        if act["location"] in self.ecoinvent_to_iam_loc:
            alternative_locations.append(self.ecoinvent_to_iam_loc[act["location"]])

        # Always include 'RoW', 'World', and 'GLO' as last resort options
        alternative_locations.extend(["RoW", "World", "GLO"])

        # Initial search with the provided names
        new_entries = search_for_new_exchanges(names_to_look_for)
        # check if the location of one of the entries matches with
        # the location of the activity to relink

        if len(new_entries) > 1:
            if any(e[2] == act["location"] for e in new_entries):
                new_entries = [e for e in new_entries if e[2] == act["location"]]
                # re-normalize the shares
                sum_shares = sum(e[-1] for e in new_entries)
                new_entries = [
                    (e[0], e[1], e[2], e[3], e[-1] / sum_shares) for e in new_entries
                ]

        if new_entries:
            return new_entries

        # If initial search fails, try with "market for" and "market group for"
        for prefix in ["market for", "market group for"]:
            if exc["name"].startswith(prefix):
                modified_name = exc["name"].replace(
                    prefix,
                    (
                        "market for"
                        if prefix == "market group for"
                        else "market group for"
                    ),
                )
                names_to_look_for.append(modified_name)

        # Second search with modified names
        return search_for_new_exchanges(names_to_look_for)

    def find_new_exchange_entries(self, act, exc, alt_names, amount=None):
        entries = None

        if self.is_exchange_in_cache(exc, act["location"]):
            entries = self.get_exchange_from_cache(exc, act["location"])

        if not entries:
            entries = self.find_alternative_locations(act, exc, alt_names)

        if not entries:
            entries = [
                (exc["name"], exc["product"], exc["location"], exc["unit"]) + (1.0,)
            ]

        if amount is None:
            amount = sum(
                e["amount"]
                for e in ws.technosphere(act)
                if (e["name"], e["product"], e["location"], e["unit"])
                == (exc["name"], exc["product"], exc["location"], exc["unit"])
            )

        return entries, amount

    def create_new_exchanges(self, entries, amount):
        return [
            {
                "name": e[0],
                "product": e[1],
                "amount": amount * e[-1],
                "type": "technosphere",
                "unit": e[3],
                "location": e[2],
            }
            for e in entries
        ]

    def summarize_exchanges(self, new_exchanges):
        grouped_exchanges = groupby(
            sorted(
                new_exchanges, key=itemgetter("name", "product", "location", "unit")
            ),
            key=itemgetter("name", "product", "location", "unit"),
        )
        summarized = (
            {
                "name": name,
                "product": prod,
                "location": loc,
                "unit": unit,
                "type": "technosphere",
                "amount": sum(e["amount"] for e in excs),
            }
            for (name, prod, loc, unit), excs in grouped_exchanges
        )
        return [compact_exchange_payload(exchange) for exchange in summarized]

    def find_iam_efficiency_change(
        self,
        data: xr.DataArray,
        variable: Union[str, list],
        location: str,
    ) -> float:
        """
        Return the relative change in efficiency for `variable` in `location`
        relative to 2020.
        :param variable: IAM variable name
        :param location: IAM region
        :return: relative efficiency change (e.g., 1.05)
        """

        if self.year in data.coords["year"].values:
            scaling_factor = data.sel(
                region=location, variables=variable, year=self.year
            ).values.item(0)
        else:
            scaling_factor = (
                data.sel(region=location, variables=variable)
                .interp(year=self.year)
                .values.item(0)
            )

        if np.isnan(scaling_factor) or np.isinf(scaling_factor):
            scaling_factor = 1

        return scaling_factor

    def write_log(self, dataset, status="created"):
        """Record a structured transformation provenance event."""

        record_change_event(self, dataset, status)

    def add_new_entry_to_cache(
        self,
        location: str,
        exchange: dict,
        allocated: List[dict],
        shares: List[float],
    ) -> None:
        """
        Add an entry to the cache.
        :param location: The location to which the cache entry corresponds.
        :param exchange: The exchange dictionary containing the data to cache.
        :param allocated: A list of dictionaries containing allocated exchanges.
        :param shares: A list of floats representing the shares for each allocated exchange.
        """
        # Ensure 'product' key is present in exchange.
        exchange.setdefault("product", exchange.get("reference product"))

        # Create a key for the cache entry.
        exc_key = (
            exchange["name"],
            exchange["product"],
            exchange["location"],
            exchange["unit"],
        )

        # Create the cache entry.
        entry = [
            (
                e.get("name", e.get("reference product")),
                e.get("product", e.get("reference product")),
                e["location"],
                e["unit"],
                s,
            )
            for e, s in zip(allocated, shares)
        ]

        # Initialize cache dictionary levels with setdefault.
        location_cache = self.cache.setdefault(location, {})
        model_cache = location_cache.setdefault(self.model, {})

        # Add the new entry to the cache.
        model_cache[exc_key] = entry

    def is_exchange_in_cache(self, exchange: dict, dataset_location: str) -> bool:
        """
        Check if an exchange is in the cache.
        :param exchange: The exchange dictionary to check.
        :param dataset_location: The location of the dataset.
        :return: True if the exchange is in the cache, False otherwise.

        """
        return (
            exchange["name"],
            exchange["product"],
            exchange["location"],
            exchange["unit"],
        ) in self.cache.get(dataset_location, {}).get(self.model, {})

    def process_cached_exchange(
        self, exchange: dict, dataset: dict, new_exchanges: list
    ) -> None:
        """
        Process a cached exchange. Adds the new exchanges to the list of new exchanges.
        :param exchange: The exchange dictionary to process.
        :param dataset: The dataset dictionary.
        :param new_exchanges: The list of new exchanges to add to the dataset.

        """
        exchanges = self.cache[dataset["location"]][self.model][
            (
                exchange["name"],
                exchange["product"],
                exchange["location"],
                exchange["unit"],
            )
        ]

        if isinstance(exchanges, tuple):
            exchanges = [exchanges]

        _ = lambda x: 0 if x is None else x

        for i, e in enumerate(exchanges):

            new_exc = {
                "name": e[0],
                "product": e[1],
                "unit": exchange["unit"],
                "location": e[2],
                "type": "technosphere",
                "amount": exchange["amount"] * e[-1],
                "uncertainty type": exchange.get("uncertainty type", 0),
            }

            for key in ["loc", "scale", "negative", "minimum", "maximum"]:
                if key in exchange:
                    if isinstance(exchange[key], float):
                        new_exc[key] = exchange[key]

            new_exchanges.append(new_exc)

    def process_uncached_exchange(
        self,
        exchange: dict,
        dataset: dict,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ):
        """
        Process an uncached exchange. Adds the new exchanges to the list of new exchanges.
        :param exchange: The exchange dictionary to process.
        :param dataset: The dataset dictionary.
        :param new_exchanges: The list of new exchanges to add to the dataset.
        """

        # This function needs to handle the logic when
        # an exchange is not in the cache.
        (
            key,
            possible_datasets,
            possible_locations,
            locations_set,
            by_location,
        ) = self._get_provider_groups(exchange)

        if len(possible_datasets) == 0:
            # search self.database for possible datasets
            possible_datasets = [
                ds
                for ds in self.database
                if ds["name"] == exchange["name"]
                and ds["reference product"] == exchange["product"]
            ]

            if len(possible_datasets) > 0:
                # repopulate self.index
                for ds in possible_datasets:
                    self.add_to_index(ds)

        if len(possible_datasets) == 0:
            print(
                f"No possible datasets found for {key} in {dataset['name']} {dataset['location']}"
            )

            exc = {
                "name": exchange["name"],
                "product": exchange["product"],
                "unit": exchange["unit"],
                "location": dataset["location"],
                "type": "technosphere",
                "amount": exchange["amount"],
                "uncertainty type": exchange.get("uncertainty type", 0),
            }

            for key in ["loc", "scale", "negative", "minimum", "maximum"]:
                if key in exchange:
                    exc[key] = exchange[key]

            return [exc]

        if len(possible_datasets) == 1:
            self.handle_single_possible_dataset(
                exchange, possible_datasets, new_exchanges
            )

        else:
            # A fallback database scan returns full datasets rather than the
            # lightweight index records cached above. Preserve that first-call
            # behavior while later calls use the freshly populated index.
            if not possible_locations:
                possible_locations = [ds["location"] for ds in possible_datasets]
                locations_set = set(possible_locations)
                by_location = defaultdict(list)
                for ds in possible_datasets:
                    by_location[ds["location"]].append(ds)

            self.handle_multiple_possible_datasets(
                exchange,
                dataset,
                possible_datasets,
                possible_locations,
                locations_set,
                by_location,
                new_exchanges,
                exclusive,
                biggest_first,
                contained,
            )

    def handle_single_possible_dataset(
        self, exchange, possible_datasets, new_exchanges
    ):
        # If there's only one possible dataset, we can just use it
        single_dataset = possible_datasets[0]

        assert (
            single_dataset.get("reference product") == exchange["product"]
        ), f"Candidate: {single_dataset}, exchange: {exchange}"

        new_exc = _relinked_exchange(
            exchange,
            single_dataset["location"],
            name=single_dataset["name"],
            product=single_dataset["reference product"],
        )

        new_exchanges.append(new_exc)

    def new_exchange(self, exchange, location, amount_multiplier):
        # Create a new exchange dictionary with the modified location and amount

        exc = {
            "name": exchange["name"],
            "product": exchange["product"],
            "unit": exchange["unit"],
            "location": location,
            "type": "technosphere",
            "amount": exchange["amount"] * amount_multiplier,
            "uncertainty type": exchange.get("uncertainty type", 0),
        }

        for key in ["loc", "scale", "negative", "minimum", "maximum"]:
            if key in exchange:
                exc[key] = exchange[key]

        return exc

    def handle_multiple_possible_datasets(
        self,
        exchange: dict,
        dataset: dict,
        possible_datasets: list,
        possible_locations: list,
        locations_set: set,
        by_location: dict,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ) -> None:
        # First, check if the dataset location itself is a possible match
        if dataset["location"] in locations_set:
            candidate = by_location[dataset["location"]][0]

            new_exc = _relinked_exchange(
                exchange,
                candidate["location"],
                name=candidate["name"],
                product=candidate["reference product"],
            )

            self.add_new_entry_to_cache(
                dataset["location"],
                exchange,
                [new_exc],
                [1.0],
            )

            new_exchanges.append(new_exc)
        else:
            # If more complex GIS matching or allocation is required,
            # we delegate to another function
            self.process_complex_matching_and_allocation(
                exchange,
                dataset,
                possible_datasets,
                possible_locations,
                locations_set,
                by_location,
                new_exchanges,
                exclusive,
                biggest_first,
                contained,
            )

    def process_complex_matching_and_allocation(
        self,
        exchange: dict,
        dataset: dict,
        possible_datasets: list,
        possible_locations: list,
        locations_set: set,
        by_location: dict,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ) -> None:
        # Check if the location of the dataset is within IAM regions
        if dataset["location"] in self.geo.iam_regions:
            self.handle_iam_region(
                exchange,
                dataset,
                possible_datasets,
                locations_set,
                by_location,
                new_exchanges,
            )

        elif dataset["location"] in ["GLO", "RoW", "World"]:
            # Handle global or rest-of-world scenarios
            self.handle_global_and_row_scenarios(
                exchange,
                dataset,
                possible_datasets,
                locations_set,
                by_location,
                new_exchanges,
            )

        else:
            # After the above checks, perform GIS matching if necessary
            self.perform_gis_matching(
                exchange,
                dataset,
                possible_datasets,
                possible_locations,
                by_location,
                new_exchanges,
                exclusive,
                biggest_first,
                contained,
            )

        # If there's still no match found, consider the default option
        self.handle_default_option(
            exchange,
            dataset,
            new_exchanges,
            possible_datasets,
            locations_set,
            by_location,
        )

    def handle_iam_region(
        self,
        exchange,
        dataset,
        possible_datasets,
        locations_set,
        by_location,
        new_exchanges,
    ):
        # In IAM regions, we need to look for possible local datasets
        locs = [
            iloc
            for iloc in self.iam_to_ecoinvent_loc[dataset["location"]]
            if iloc in locations_set
        ]

        if locs:
            kept = [ds for loc in locs for ds in by_location[loc]]
            if dataset["location"] == "World" and "GLO" in locs:
                kept = [ds for ds in kept if ds["location"] == "GLO"]

            allocated, share = allocate_inputs(
                exchange, kept, exchange_factory=_relinked_exchange
            )

            new_exchanges.extend(allocated)
            self.add_new_entry_to_cache(dataset["location"], exchange, allocated, share)

    def handle_global_and_row_scenarios(
        self,
        exchange,
        dataset,
        possible_datasets,
        locations_set,
        by_location,
        new_exchanges,
    ):
        # Handle scenarios where the location is 'GLO' or 'RoW'
        if locations_set.intersection({"GLO", "RoW", "World"}):
            kept = [
                ds for loc in ("GLO", "RoW", "World") for ds in by_location.get(loc, [])
            ]
            allocated, share = allocate_inputs(
                exchange, kept, exchange_factory=_relinked_exchange
            )
            new_exchanges.extend(allocated)
            self.add_new_entry_to_cache(dataset["location"], exchange, allocated, share)

    def perform_gis_matching(
        self,
        exchange: dict,
        dataset: dict,
        possible_datasets: list,
        possible_locations: list,
        by_location: dict,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ) -> None:
        """
        Perform GIS matching for a dataset with a non-IAM location.

        :param exchange: The exchange dictionary to process.
        :param dataset: The dataset dictionary.
        :param possible_datasets: The list of possible datasets.
        :param new_exchanges: The list of new exchanges to add to the dataset.
        :param exclusive: Bool, default is ``True``. Don't allow overlapping locations in input providers.
        :param biggest_first: Bool, default is ``False``. Determines search order when selecting provider locations. Only relevant if ``exclusive`` is ``True``.
        :param contained: Bool, default is ``True``. If true, only use providers whose location is completely within the ``dataset`` location; otherwise use all intersecting locations.

        """
        # Perform GIS-based matching for location
        location = dataset["location"]
        # if regions contained in posisble location
        # we need to turn them into tuples (model, region)
        possible_locations = tuple(possible_locations)

        gis_match = self.get_gis_match(
            location,
            possible_locations,
            contained,
            exclusive,
            biggest_first,
        )

        kept = [ds for loc in gis_match for ds in by_location.get(loc, [])]

        if kept:
            allocated, share = allocate_inputs(
                exchange, kept, exchange_factory=_relinked_exchange
            )
            new_exchanges.extend(allocated)
            self.add_new_entry_to_cache(dataset["location"], exchange, allocated, share)

    def handle_default_option(
        self,
        exchange,
        dataset,
        new_exchanges,
        possible_datasets,
        locations_set,
        by_location,
    ):
        new_exc = None
        # Handle the default case where no better candidate is found
        if not self.is_exchange_in_cache(exchange, dataset["location"]):
            for default_location in ["RoW", "GLO", "World"]:
                if default_location in locations_set:
                    default_dataset = by_location[default_location][0]

                    new_exc = _relinked_exchange(
                        exchange,
                        default_dataset["location"],
                        name=default_dataset["name"],
                        product=default_dataset["reference product"],
                    )
                    new_exchanges.append(new_exc)

                    break

        if new_exc is None and not self.is_exchange_in_cache(
            exchange, dataset["location"]
        ):
            new_exchanges.append(exchange)

    def find_candidates(
        self,
        dataset: dict,
        exclusive=True,
        biggest_first=False,
        contained=False,
        technosphere_exchanges=None,
    ):
        new_exchanges = []

        exchanges = (
            filter_technosphere_exchanges(dataset["exchanges"])
            if technosphere_exchanges is None
            else technosphere_exchanges
        )
        for exchange in exchanges:
            if self.is_exchange_in_cache(exchange, dataset["location"]):
                self.process_cached_exchange(exchange, dataset, new_exchanges)
            else:
                self.process_uncached_exchange(
                    exchange,
                    dataset,
                    new_exchanges,
                    exclusive,
                    biggest_first,
                    contained,
                )

        return new_exchanges

    def relink_technosphere_exchanges(
        self,
        dataset,
        exclusive=True,
        biggest_first=False,
        contained=False,
    ) -> dict:
        """Find new technosphere providers based on the location of the dataset.
        Designed to be used when the dataset's location changes, or when new datasets are added.
        Uses the name, reference product, and unit of the exchange to filter possible inputs. These must match exactly. Searches in the list of datasets ``data``.
        Will only search for providers contained within the location of ``dataset``, unless ``contained`` is set to ``False``, all providers whose location intersects the location of ``dataset`` will be used.
        A ``RoW`` provider will be added if there is a single topological face in the location of ``dataset`` which isn't covered by the location of any providing activity.
        If no providers can be found, `relink_technosphere_exchanes` will try to add a `RoW` or `GLO` providers, in that order, if available. If there are still no valid providers, a ``InvalidLink`` exception is raised, unless ``drop_invalid`` is ``True``, in which case the exchange will be deleted.
        Allocation between providers is done using ``allocate_inputs``; results seem strange if ``contained=False``, as production volumes for large regions would be used as allocation factors.
        Input arguments:
            * ``dataset``: The dataset whose technosphere exchanges will be modified.
            * ``data``: The list of datasets to search for technosphere product providers.
            * ``model``: The IAM model
            * ``exclusive``: Bool, default is ``True``. Don't allow overlapping locations in input providers.
            * ``drop_invalid``: Bool, default is ``False``. Delete exchanges for which no valid provider is available.
            * ``biggest_first``: Bool, default is ``False``. Determines search order when selecting provider locations. Only relevant if ``exclusive`` is ``True``.
            * ``contained``: Bool, default is ``True``. If true, only use providers whose location is completely within the ``dataset`` location; otherwise use all intersecting locations.
            * ``iam_regions``: List, lists IAM regions, if additional ones need to be defined.
        Modifies the dataset in place; returns the modified dataset."""

        exchanges = []
        exchange_amounts = []
        technosphere_exchanges = []
        non_technosphere_exchanges = []
        exchanges_before = defaultdict(float)
        old_uncertainty = {}
        missing_exchange_type = object()
        for exc in dataset.get("exchanges", []):
            exchange_type = exc.get("type", missing_exchange_type)
            if exchange_type == "technosphere":
                amount = exc.get("amount")
                if amount in (0, 0.0, None):
                    continue
                technosphere_exchanges.append(exc)
                product = exc["product"]
                exchanges_before[product] += amount

                uncertainty_type = exc.get("uncertainty type", 0)
                if uncertainty_type != 0:
                    uncertainty_key = (exc["name"], product, exc["unit"])
                    uncertainty = {
                        "uncertainty type": uncertainty_type,
                        "amount": amount,
                    }
                    for key in ["loc", "scale", "negative", "minimum", "maximum"]:
                        if key in exc:
                            uncertainty[key] = exc[key]
                    old_uncertainty[uncertainty_key] = uncertainty
            else:
                if exchange_type is missing_exchange_type:
                    # Preserve the legacy KeyError for malformed exchanges.
                    _ = exc["type"]
                amount = exc["amount"]
                non_technosphere_exchanges.append(exc)
            exchanges.append(exc)
            exchange_amounts.append(amount)

        dataset["exchanges"] = exchanges
        sum_before = sum(exchange_amounts)

        new_exchanges = self.find_candidates(
            dataset,
            exclusive=exclusive,
            biggest_first=biggest_first,
            contained=contained,
            technosphere_exchanges=technosphere_exchanges,
        )

        # make unique list of exchanges from new_exchanges
        # and sum the amounts of exchanges with the same name,
        # product, location and unit

        grouped_exchanges = defaultdict(float)
        for exc in new_exchanges:
            key = (
                exc["name"],
                exc["product"],
                exc["location"],
                exc["unit"],
            )
            grouped_exchanges[key] += exc["amount"]

        new_exchanges = [
            {
                "name": name,
                "product": prod,
                "location": location,
                "unit": unit,
                "type": "technosphere",
                "amount": amount,
            }
            for (name, prod, location, unit), amount in grouped_exchanges.items()
        ]

        # apply uncertainties, if any
        if old_uncertainty:
            for exc in new_exchanges:
                key = (exc["name"], exc["product"], exc["unit"])
                if key in old_uncertainty:
                    exc["uncertainty type"] = old_uncertainty[key]["uncertainty type"]
                    loc, scale, minimum, maximum, negative = (
                        redefine_uncertainty_params(old_uncertainty[key], exc)
                    )

                    if loc:
                        exc["loc"] = float(loc)

                    if scale:
                        exc["scale"] = float(scale)

                    if negative:
                        exc["negative"] = float(negative)

                    if minimum:
                        exc["minimum"] = float(minimum)

                    if maximum:
                        exc["maximum"] = float(maximum)

        dataset["exchanges"] = non_technosphere_exchanges + new_exchanges

        sum_after = sum(exc["amount"] for exc in dataset["exchanges"])

        assert np.allclose(sum_before, sum_after, rtol=1e-3), (
            f"Sum of exchanges before and after relinking is not the same: {sum_before} != {sum_after}"
            f"\n{dataset['name']}|{dataset['location']}"
        )

        # compare new exchanges with exchanges before
        exchanges_after = defaultdict(float)
        for exc in dataset["exchanges"]:
            if exc["type"] == "technosphere":
                exchanges_after[exc["product"]] += exc["amount"]

        assert set(exchanges_before.keys()) == set(exchanges_after.keys()), (
            f"Exchanges before and after relinking are not the same: {set(exchanges_before.keys())} != {set(exchanges_after.keys())}"
            f"\n{dataset['name']}|{dataset['location']}"
        )

        return dataset

    def get_gis_match(
        self,
        location,
        possible_locations,
        contained,
        exclusive,
        biggest_first,
    ):
        cache_key = (
            location,
            possible_locations,
            contained,
            exclusive,
            biggest_first,
        )
        cache = getattr(self, "_gis_match_cache", None)
        if cache is None:
            cache = self._gis_match_cache = {}
        if cache_key in cache:
            return cache[cache_key]

        # prepare locations in possible_locations
        # all locations in possible_locations that are an IAM region
        # need to be converted to tuples with (model.upper(), location)
        # and other locations longer than 2 characters (other than GLO)
        # are converted to tuples with ("ecoinvent", location).

        filtered_possible_locations = [
            (
                (self.model.upper(), loc)
                if loc in self.regions
                else (
                    ("ecoinvent", loc)
                    if (len(loc) > 2 and loc not in ["GLO", "RoW"])
                    else loc
                )
            )
            for loc in possible_locations
        ]

        filtered_possible_locations = [
            loc for loc in filtered_possible_locations if loc in self.geo.geo
        ]

        def match(geomatcher):
            func = geomatcher.contained if contained else geomatcher.intersects
            return func(
                location,
                include_self=True,
                exclusive=exclusive,
                biggest_first=biggest_first,
                only=filtered_possible_locations,
            )

        row_cache = getattr(self, "_resolved_row_faces_cache", None)
        if row_cache is None:
            row_cache = self._resolved_row_faces_cache = {}
        row_cache_key = (self.model, tuple(filtered_possible_locations))

        try:
            if row_cache_key not in row_cache:
                with resolved_row(filtered_possible_locations, self.geo.geo) as g:
                    row_cache[row_cache_key] = g["RoW"]
                    result = match(g)
            else:
                self.geo.geo["RoW"] = row_cache[row_cache_key]
                try:
                    result = match(self.geo.geo)
                finally:
                    del self.geo.geo["RoW"]
            cache[cache_key] = result
            return result
        except Exception as exc:
            raise ValueError(
                "GIS matching failed for "
                f"location={location}, possible_locations={possible_locations}, "
                f"filtered_possible_locations={filtered_possible_locations}: {exc}"
            ) from exc
