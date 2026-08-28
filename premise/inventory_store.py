"""Inventory storage abstractions used by :class:`premise.NewDatabase`.

The store is deliberately stricter than the historical ``list[dict]`` API:
readers receive immutable snapshots and writers must use a transaction.  The
compact implementation adds copy-on-write forks and a versioned, columnar
checkpoint representation.
"""

from __future__ import annotations

import copy
import hashlib
import json
import mmap
import os
import pickle
import shutil
import tempfile
import threading
from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Literal, TypeAlias

import numpy as np

try:  # PyArrow is a premise dependency, but keep source-only installs usable.
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc
except ImportError:  # pragma: no cover - exercised only in minimal environments
    pa = None
    pa_ipc = None


ActivityId: TypeAlias = int
ExchangeId: TypeAlias = int
STORE_SCHEMA_VERSION = 10
_UNHASHABLE = object()
_COLUMNAR_ACTIVITY_MISSING = object()

_ACTIVITY_COMMON_FIELDS = (
    "name",
    "reference product",
    "product",
    "location",
    "unit",
    "database",
    "code",
    "type",
)
_ACTIVITY_HOT_FIELD_ATTRIBUTES = {
    "database": "_database",
    "code": "_code",
    "type": "_type",
}
_EXCHANGE_STRING_FIELDS = (
    "name",
    "product",
    "location",
    "unit",
    "type",
)
_EXCHANGE_STRING_FIELD_SET = frozenset(_EXCHANGE_STRING_FIELDS)
_EXCHANGE_NUMERIC_FIELDS = ("amount", "uncertainty type")
_EXCHANGE_BOOLEAN_FIELDS = ()
_SCENARIO_CACHE_COMMON_FIELDS = (
    *_EXCHANGE_STRING_FIELDS,
    *_EXCHANGE_NUMERIC_FIELDS,
)
_SCENARIO_CACHE_STANDARD_SIDECAR_FIELDS = frozenset(
    {"uncertainty type", "loc", "production volume", "categories"}
)
_EXCHANGE_HOT_METADATA_FIELDS = (
    "categories",
    "reference product",
    "uncertainty type",
    "loc",
    "product",
    "scale",
    "negative",
    "minimum",
    "maximum",
    "input",
)
_EXCHANGE_HOT_METADATA_BITS = {
    field_name: 1 << position
    for position, field_name in enumerate(_EXCHANGE_HOT_METADATA_FIELDS)
}
_COMPACT_EXCHANGE_FIELDS = (
    "name",
    "product",
    "amount",
    "type",
    "unit",
    "location",
)
_COMPACT_EXCHANGE_FIELD_ATTRIBUTES = {
    field_name: f"_{field_name.replace(' ', '_')}"
    for field_name in _COMPACT_EXCHANGE_FIELDS
}
_COMPACT_EXCHANGE_FIELD_BITS = {
    field_name: 1 << position
    for position, field_name in enumerate(_COMPACT_EXCHANGE_FIELDS)
}
_NUMERIC_MISSING = 0
_NUMERIC_PYTHON_FLOAT = 1
_NUMERIC_PYTHON_INT = 2
_NUMERIC_FLOAT32 = 3
_NUMERIC_FLOAT64 = 4
_CandidatePositions: TypeAlias = set[int] | frozenset[int]


def _numeric_column_parts(value: Any) -> tuple[int, float | None, int | None]:
    if type(value) is float:
        return _NUMERIC_PYTHON_FLOAT, value, None
    if type(value) is int and -(2**63) <= value < 2**63:
        return _NUMERIC_PYTHON_INT, None, value
    if type(value) is np.float32:
        return _NUMERIC_FLOAT32, float(value), None
    if type(value) is np.float64:
        return _NUMERIC_FLOAT64, float(value), None
    return _NUMERIC_MISSING, None, None


def _decode_numeric_column(kind: int, float_value: Any, int_value: Any) -> Any:
    if kind == _NUMERIC_PYTHON_FLOAT:
        return float(float_value)
    if kind == _NUMERIC_PYTHON_INT:
        return int(int_value)
    if kind == _NUMERIC_FLOAT32:
        return np.float32(float_value)
    if kind == _NUMERIC_FLOAT64:
        return np.float64(float_value)
    raise ValueError(f"Unknown numeric exchange value kind: {kind}")


def _exchange_sidecar_metadata(
    payload: Mapping[str, Any],
    *,
    numeric_kinds: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Return only fields which cannot be restored losslessly from Arrow."""

    metadata = {}
    for key, value in payload.items():
        if key in _EXCHANGE_STRING_FIELDS and isinstance(value, str):
            continue
        if key in _EXCHANGE_NUMERIC_FIELDS:
            kind = (
                numeric_kinds[key]
                if numeric_kinds is not None and key in numeric_kinds
                else _numeric_column_parts(value)[0]
            )
            if kind != _NUMERIC_MISSING:
                continue
        if (
            key == "categories"
            and isinstance(value, tuple)
            and len(value) == 2
            and all(isinstance(item, str) for item in value)
        ):
            continue
        if key in _EXCHANGE_BOOLEAN_FIELDS and type(value) is bool:
            continue
        metadata[key] = value
    return metadata


def _activity_sidecar_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return activity fields not represented losslessly in Arrow columns."""

    return {
        key: value
        for key, value in payload.items()
        if key not in _ACTIVITY_COMMON_FIELDS or not isinstance(value, str)
    }


class InventoryStoreError(RuntimeError):
    """Base class for inventory-store failures."""


class InventoryStoreCorruptionError(InventoryStoreError):
    """Raised when a checkpoint checksum or payload is invalid."""


class InventoryStoreVersionError(InventoryStoreError):
    """Raised when a checkpoint uses an unsupported schema."""


class InventoryStoreReadOnlyError(InventoryStoreError):
    """Raised when mutation is requested through a read-only view."""


@dataclass(frozen=True, slots=True)
class _CompiledWurstFilter:
    operation: str
    field_name: str | None = None
    value: Any = None
    children: tuple["_CompiledWurstFilter", ...] = ()

    def __call__(self, payload: Mapping[str, Any]) -> bool:
        if self.operation == "equals":
            return payload.get(self.field_name) == self.value
        if self.operation == "contains":
            return self.value in payload.get(self.field_name)
        if self.operation == "startswith":
            return payload.get(self.field_name, "").startswith(self.value)
        if self.operation == "either":
            return any(child(payload) for child in self.children)
        if self.operation == "exclude":
            return not self.children[0](payload)
        if self.operation == "doesnt-contain-any":
            return all(
                value not in payload.get(self.field_name) for value in self.value
            )
        raise ValueError(f"Unknown compiled Wurst operation: {self.operation}")


class IndexedInventoryList(list):
    """List-compatible inventory with lazy, order-preserving query indexes.

    It is a migration bridge for transformation code which still calls Wurst.
    Structural list changes invalidate the indexes. Sector wrappers re-wrap the
    result at every boundary, which also captures direct changes to indexed
    dataset fields.
    """

    _INDEXED_FIELDS = (
        "name",
        "reference product",
        "product",
        "location",
        "unit",
        "type",
        "database",
        "code",
    )

    def __init__(self, iterable=(), *, inventory_backend: str | None = None):
        super().__init__(iterable)
        self._inventory_backend = inventory_backend or getattr(
            iterable, "_inventory_backend", None
        )
        self._query_indexes = None
        self._indexed_query_fields: set[str] = set()
        self._validation_added_targets: dict[int, Mapping[str, Any]] | None = None

    def track_validation_additions(
        self, sink: dict[int, Mapping[str, Any]] | None
    ) -> None:
        """Record structural additions made by one sector transformation.

        Initial contents are deliberately not recorded.  A fresh sink is attached
        by :class:`BaseTransformation` at each sector boundary, and the immutable
        validation intent retains the semantic keys after the sector completes.
        """

        self._validation_added_targets = sink

    def _record_validation_additions(self, additions: Iterable[Any]) -> None:
        sink = self._validation_added_targets
        if sink is None:
            return
        for item in additions:
            if isinstance(item, Mapping):
                sink[id(item)] = item

    def _invalidate(self) -> None:
        self._query_indexes = None
        self._indexed_query_fields = set()

    def _index_item(self, position: int, dataset: Mapping[str, Any]) -> None:
        if self._query_indexes is None:
            return
        exact, strings, predicate_cache = self._query_indexes
        predicate_cache.clear()
        for field_name in self._indexed_query_fields:
            value = dataset.get(field_name)
            hashable = _hashable(value)
            if hashable is not _UNHASHABLE:
                exact[field_name][hashable].add(position)
            if isinstance(value, str):
                strings[field_name][value].add(position)

    def _indexes(self, field_names: Iterable[str] = ()):
        if self._query_indexes is None:
            self._query_indexes = {}, {}, {}

        exact, strings, predicate_cache = self._query_indexes
        missing_fields = [
            field_name
            for field_name in field_names
            if field_name in self._INDEXED_FIELDS
            and field_name not in self._indexed_query_fields
        ]
        for field_name in missing_fields:
            field_exact: dict[Any, set[int]] = defaultdict(set)
            field_strings: dict[str, set[int]] = defaultdict(set)
            for position, dataset in enumerate(self):
                value = dataset.get(field_name)
                hashable = _hashable(value)
                if hashable is not _UNHASHABLE:
                    field_exact[hashable].add(position)
                if isinstance(value, str):
                    field_strings[value].add(position)
            exact[field_name] = field_exact
            strings[field_name] = field_strings
            self._indexed_query_fields.add(field_name)
        return self._query_indexes

    def _all_positions(self) -> frozenset[int]:
        _, _, predicate_cache = self._indexes()
        cache_key = ("all-positions", len(self))
        cached = predicate_cache.get(cache_key)
        if cached is None:
            cached = frozenset(range(len(self)))
            predicate_cache[cache_key] = cached
        return cached

    def _candidate_positions(
        self, expression: _CompiledWurstFilter
    ) -> _CandidatePositions | None:
        operation = expression.operation
        field_name = expression.field_name
        if operation == "equals" and field_name in self._INDEXED_FIELDS:
            exact, _, _ = self._indexes((field_name,))
            value = _hashable(expression.value)
            if value is _UNHASHABLE:
                return None
            # Query callers treat candidates as read-only. Returning the index
            # set avoids copying it for every exact predicate.
            return exact[field_name].get(value, frozenset())
        if (
            operation in {"contains", "startswith"}
            and field_name in self._INDEXED_FIELDS
        ):
            _, strings, predicate_cache = self._indexes((field_name,))
            cache_key = (operation, field_name, expression.value)
            cached = predicate_cache.get(cache_key)
            if cached is not None:
                return cached
            result: set[int] = set()
            for value, positions in strings[field_name].items():
                matches = (
                    expression.value in value
                    if operation == "contains"
                    else value.startswith(expression.value)
                )
                if matches:
                    result.update(positions)
            candidates = frozenset(result)
            predicate_cache[cache_key] = candidates
            return candidates
        if operation == "either":
            _, _, predicate_cache = self._indexes()
            cache_key = ("compiled", expression)
            try:
                cached = predicate_cache.get(cache_key)
            except TypeError:
                cache_key = None
                cached = None
            if cached is not None:
                return cached
            candidates = [
                self._candidate_positions(child) for child in expression.children
            ]
            if any(candidate is None for candidate in candidates):
                return None
            result: set[int] = set()
            for candidate in candidates:
                result.update(candidate)
            immutable_result = frozenset(result)
            if cache_key is not None:
                predicate_cache[cache_key] = immutable_result
            return immutable_result
        if operation == "exclude":
            _, _, predicate_cache = self._indexes()
            cache_key = ("compiled", expression)
            try:
                cached = predicate_cache.get(cache_key)
            except TypeError:
                cache_key = None
                cached = None
            if cached is not None:
                return cached
            candidates = self._candidate_positions(expression.children[0])
            if candidates is None:
                return None
            result = self._all_positions().difference(candidates)
            if cache_key is not None:
                predicate_cache[cache_key] = result
            return result
        if operation == "doesnt-contain-any" and field_name in self._INDEXED_FIELDS:
            _, strings, predicate_cache = self._indexes((field_name,))
            cache_key = (operation, field_name, expression.value)
            cached = predicate_cache.get(cache_key)
            if cached is not None:
                return cached
            excluded: set[int] = set()
            for value, positions in strings[field_name].items():
                if any(item in value for item in expression.value):
                    excluded.update(positions)
            result = self._all_positions().difference(excluded)
            predicate_cache[cache_key] = frozenset(result)
            return result
        return None

    def query_wurst(self, filters: tuple[_CompiledWurstFilter, ...]):
        candidate_groups: list[_CandidatePositions] = []
        for expression in filters:
            positions = self._candidate_positions(expression)
            if positions is None:
                return None
            candidate_groups.append(positions)
        if not candidate_groups:
            candidates: Iterable[int] = range(len(self))
        elif len(candidate_groups) == 1:
            candidates = candidate_groups[0]
        else:
            candidate_groups.sort(key=len)
            intersection = set(candidate_groups[0])
            for positions in candidate_groups[1:]:
                intersection.intersection_update(positions)
                if not intersection:
                    break
            candidates = intersection
        return (
            self[position]
            for position in sorted(candidates)
            if all(expression(self[position]) for expression in filters)
        )

    def _premise_relink_candidates(self, activity, provider_semantics):
        """Yield compact relinking candidates in exchange order when supported."""

        if self._inventory_backend != "compact":
            return None

        def candidates():
            for exchange in activity["exchanges"]:
                type_accessor = getattr(exchange, "_premise_exchange_type", None)
                exchange_type = (
                    type_accessor() if type_accessor is not None else exchange["type"]
                )
                if exchange_type != "technosphere":
                    continue
                fields_accessor = getattr(
                    exchange, "_premise_relink_technosphere_fields", None
                )
                if fields_accessor is None:
                    product = (
                        exchange["reference product"]
                        if "reference product" in exchange
                        else exchange["product"]
                    )
                    fields = (
                        exchange["name"],
                        product,
                        exchange["location"],
                        exchange["unit"],
                        exchange["amount"],
                    )
                else:
                    fields = fields_accessor()
                name, effective_product, location, unit, amount = fields
                if (
                    name,
                    effective_product,
                    location,
                ) not in provider_semantics and amount != 0:
                    # Historical relinking uses ``reference product`` only for
                    # candidate selection, then groups the selected exchanges
                    # by their stored ``product`` field.
                    yield exchange, (
                        name,
                        exchange["product"],
                        location,
                        unit,
                        amount,
                    )

        return candidates()

    def append(self, item) -> None:
        super().append(item)
        self._record_validation_additions((item,))
        self._index_item(len(self) - 1, item)

    def extend(self, iterable) -> None:
        additions = list(iterable)
        start = len(self)
        super().extend(additions)
        self._record_validation_additions(additions)
        for offset, item in enumerate(additions):
            self._index_item(start + offset, item)

    def insert(self, index, item) -> None:
        super().insert(index, item)
        self._record_validation_additions((item,))
        self._invalidate()

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        self._invalidate()

    def __delitem__(self, key) -> None:
        super().__delitem__(key)
        self._invalidate()

    def __iadd__(self, iterable):
        self.extend(iterable)
        return self

    def pop(self, index=-1):
        value = super().pop(index)
        self._invalidate()
        return value

    def remove(self, value) -> None:
        super().remove(value)
        self._invalidate()

    def clear(self) -> None:
        super().clear()
        self._invalidate()

    def reverse(self) -> None:
        super().reverse()
        self._invalidate()

    def sort(self, *args, **kwargs) -> None:
        super().sort(*args, **kwargs)
        self._invalidate()


def install_wurst_query_engine() -> bool:
    """Install metadata-carrying Wurst predicates and an indexed ``get_many``.

    Plain lists and dynamic predicates retain Wurst's original ordered scan.
    The patch is idempotent and affects no query results.
    """

    try:
        from wurst import searching as ws
    except ImportError:  # pragma: no cover - premise normally depends on Wurst
        return False
    if getattr(ws, "_premise_inventory_store_query_engine", False):
        return True

    original = {
        "equals": ws.equals,
        "contains": ws.contains,
        "startswith": ws.startswith,
        "either": ws.either,
        "exclude": ws.exclude,
        "doesnt_contain_any": ws.doesnt_contain_any,
        "get_many": ws.get_many,
    }

    def equals(field_name, value):
        return _CompiledWurstFilter("equals", field_name, value)

    def contains(field_name, value):
        return _CompiledWurstFilter("contains", field_name, value)

    def startswith(field_name, value):
        return _CompiledWurstFilter("startswith", field_name, value)

    def either(*filters):
        if all(isinstance(item, _CompiledWurstFilter) for item in filters):
            return _CompiledWurstFilter("either", children=tuple(filters))
        return original["either"](*filters)

    def exclude(filter_expression):
        if isinstance(filter_expression, _CompiledWurstFilter):
            return _CompiledWurstFilter("exclude", children=(filter_expression,))
        return original["exclude"](filter_expression)

    def doesnt_contain_any(field_name, values):
        return _CompiledWurstFilter("doesnt-contain-any", field_name, tuple(values))

    def get_many(data, *filters):
        if isinstance(data, IndexedInventoryList):
            if all(isinstance(item, _CompiledWurstFilter) for item in filters):
                result = data.query_wurst(tuple(filters))
                if result is not None:
                    ws._premise_inventory_store_query_diagnostics["indexed"] += 1
                    return result
            ws._premise_inventory_store_query_diagnostics["fallback"] += 1
        return original["get_many"](data, *filters)

    ws.equals = equals
    ws.contains = contains
    ws.startswith = startswith
    ws.either = either
    ws.exclude = exclude
    ws.doesnt_contain_any = doesnt_contain_any
    ws.get_many = get_many
    ws._premise_inventory_store_query_engine = True
    ws._premise_inventory_store_originals = original
    ws._premise_inventory_store_query_diagnostics = {
        "indexed": 0,
        "fallback": 0,
    }
    return True


install_wurst_query_engine()


def get_wurst_query_diagnostics(*, reset: bool = False) -> dict[str, int]:
    """Return counts of indexed and ordered-fallback Wurst scans."""

    from wurst import searching as ws

    diagnostics = dict(
        getattr(
            ws,
            "_premise_inventory_store_query_diagnostics",
            {"indexed": 0, "fallback": 0},
        )
    )
    if reset:
        ws._premise_inventory_store_query_diagnostics = {
            "indexed": 0,
            "fallback": 0,
        }
    return diagnostics


@dataclass(frozen=True, slots=True)
class ActivityKey:
    name: str
    product: str
    location: str


@dataclass(frozen=True, slots=True)
class ProviderKey:
    name: str
    product: str
    unit: str


@dataclass(frozen=True, slots=True)
class FilterExpression:
    """A serialisable activity predicate.

    ``operator`` accepts ``equals``/``exact``, ``contains``, ``startswith``,
    ``in``/``either``, ``all``, and their common negative variants.  A callable
    value is supported explicitly as an ordered fallback scan.
    """

    field: str
    value: Any
    operator: str = "equals"


@dataclass(frozen=True, slots=True)
class ActivityQuery:
    filters: tuple[FilterExpression, ...]
    masks: tuple[FilterExpression, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "filters", tuple(self.filters))
        object.__setattr__(self, "masks", tuple(self.masks))


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value


class _ImmutableRecord(Mapping[str, Any]):
    __slots__ = ("_data", "_raw")

    def __init__(self, data: Mapping[str, Any]) -> None:
        self._raw = copy.deepcopy(dict(data))
        self._data = _freeze(self._raw)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({dict(self._data)!r})"

    def to_dict(self) -> dict[str, Any]:
        """Return an independent mutable copy of this snapshot."""

        return copy.deepcopy(self._raw)


class ExchangeRecord(_ImmutableRecord):
    __slots__ = ("exchange_id", "activity_id")

    def __init__(
        self,
        exchange_id: ExchangeId,
        activity_id: ActivityId,
        data: Mapping[str, Any],
    ) -> None:
        super().__init__(data)
        self.exchange_id = exchange_id
        self.activity_id = activity_id

    @property
    def id(self) -> ExchangeId:
        return self.exchange_id


class ActivityRecord(_ImmutableRecord):
    __slots__ = ("activity_id", "exchange_ids")

    def __init__(
        self,
        activity_id: ActivityId,
        data: Mapping[str, Any],
        exchange_ids: Iterable[ExchangeId] = (),
    ) -> None:
        super().__init__(data)
        self.activity_id = activity_id
        self.exchange_ids = tuple(exchange_ids)

    @property
    def id(self) -> ActivityId:
        return self.activity_id


@dataclass
class _StoreState:
    activities: dict[ActivityId, dict[str, Any]] = field(default_factory=dict)
    activity_order: list[ActivityId] = field(default_factory=list)
    exchanges: Any = field(default_factory=dict)
    exchange_owner: dict[ExchangeId, ActivityId] = field(default_factory=dict)
    activity_exchanges: dict[ActivityId, list[ExchangeId] | range] = field(
        default_factory=dict
    )
    activity_fingerprints: dict[ActivityId, str] = field(default_factory=dict)
    next_activity_id: int = 0
    next_exchange_id: int = 0
    generation: int = 0
    transaction_log: list[str] = field(default_factory=list)
    field_index: dict[str, dict[Any, tuple[ActivityId, ...]]] = field(
        default_factory=dict
    )
    activity_key_index: dict[ActivityKey, tuple[ActivityId, ...]] = field(
        default_factory=dict
    )
    provider_index: dict[ProviderKey, tuple[ActivityId, ...]] = field(
        default_factory=dict
    )
    consumer_index: dict[ActivityId, tuple[ActivityId, ...]] = field(
        default_factory=dict
    )
    indexes_ready: bool = False


class _DenseExchangeTable:
    """Dense integer-ID exchange table with stable tombstoned positions."""

    __slots__ = ("_rows", "_length")

    def __init__(self) -> None:
        self._rows: list[Mapping[str, Any] | None] = []
        self._length = 0

    def __len__(self) -> int:
        return self._length

    def __contains__(self, exchange_id: object) -> bool:
        return (
            isinstance(exchange_id, int)
            and 0 <= exchange_id < len(self._rows)
            and self._rows[exchange_id] is not None
        )

    def __getitem__(self, exchange_id: ExchangeId) -> Mapping[str, Any]:
        if exchange_id not in self:
            raise KeyError(exchange_id)
        return self._rows[exchange_id]

    def __setitem__(self, exchange_id: ExchangeId, payload: Mapping[str, Any]) -> None:
        if exchange_id == len(self._rows):
            self._rows.append(payload)
            self._length += 1
            return
        if not 0 <= exchange_id < len(self._rows):
            raise IndexError(f"Non-contiguous exchange id: {exchange_id}")
        if self._rows[exchange_id] is None:
            self._length += 1
        self._rows[exchange_id] = payload

    def __delitem__(self, exchange_id: ExchangeId) -> None:
        if exchange_id not in self:
            raise KeyError(exchange_id)
        self._rows[exchange_id] = None
        self._length -= 1

    def shallow_copy(self) -> "_DenseExchangeTable":
        """Copy the row index while sharing immutable transaction payloads."""

        duplicate = type(self)()
        duplicate._rows = self._rows.copy()
        duplicate._length = self._length
        return duplicate


_COLUMNAR_DELETED = object()
_COLUMNAR_MISSING = object()


class _ColumnarExchangeStorage:
    """Compact immutable exchange columns plus lazy sidecar metadata.

    Arrow dictionaries are normalised into small NumPy integer columns when a
    checkpoint is opened.  This keeps repeated field access fast enough for the
    remaining list-based transformations without recreating a dictionary for
    every source exchange.  Arbitrary fields stay in the memory-mapped sidecar
    and are decoded one activity at a time through a bounded LRU cache.
    """

    _CACHE_SIZE = 128

    def __init__(
        self,
        checkpoint: Path,
        *,
        exchange_count: int,
        activity_ids: np.ndarray,
        exchange_starts: np.ndarray,
        exchange_counts: np.ndarray,
        activity_common_columns: Mapping[str, list[Any]],
        activity_offsets: Mapping[int, tuple[int, int]],
        exchange_metadata_offsets: Mapping[int, tuple[int, int]],
    ) -> None:
        if pa is None or pa_ipc is None:
            raise RuntimeError("PyArrow is required for lazy compact checkpoints.")
        self.checkpoint = checkpoint
        self.row_count = int(exchange_count)
        self.activity_ids = activity_ids
        self._activity_common_columns = dict(activity_common_columns)
        self._activity_position_by_id = (
            None
            if np.array_equal(
                activity_ids, np.arange(len(activity_ids), dtype=np.int64)
            )
            else {
                int(activity_id): position
                for position, activity_id in enumerate(activity_ids)
            }
        )
        self.exchange_starts = exchange_starts
        self.exchange_ends = exchange_starts + exchange_counts
        self._activity_position_by_row = np.repeat(
            np.arange(len(activity_ids), dtype=np.int32), exchange_counts
        )
        if len(self._activity_position_by_row) != self.row_count:
            raise InventoryStoreCorruptionError(
                "Activity exchange counts do not match the checkpoint manifest."
            )
        self.activity_offsets = dict(activity_offsets)
        self.exchange_metadata_offsets = dict(exchange_metadata_offsets)
        self._activity_cache: OrderedDict[int, dict[str, Any]] = OrderedDict()
        self._metadata_cache: OrderedDict[int, dict[int, dict[str, Any]]] = (
            OrderedDict()
        )
        self._metadata_hot_field_masks = np.full(self.row_count, -1, dtype=np.int16)
        self._scenario_cache_scanned_metadata_activities: set[int] = set()
        self._scenario_cache_filtered_metadata: dict[int, dict[int, dict[str, Any]]] = (
            {}
        )
        self._scenario_cache_deleted_metadata: dict[
            tuple[int, int], tuple[str, ...]
        ] = {}
        self._scenario_cache_deleted_fields_by_row: dict[int, tuple[str, ...]] = {}
        self._scenario_cache_scan_complete = not self.exchange_metadata_offsets
        self._scenario_cache_compatible_rows: np.ndarray | None = None
        self._validation_modified_targets: dict[int, Mapping[str, Any]] | None = None
        self._biosphere_category_masks: dict[
            tuple[tuple[str, str], str], tuple[np.ndarray, np.ndarray]
        ] = {}
        self._last_metadata_activity_id: int | None = None
        self._last_metadata_group: dict[int, dict[str, Any]] = {}
        self._last_scenario_metadata_activity_id: int | None = None
        self._last_scenario_metadata_group: dict[int, dict[str, Any]] = {}
        self._metadata_lock = threading.RLock()
        self._sidecar_file = (checkpoint / "metadata.bin").open("rb")
        self._sidecar = mmap.mmap(
            self._sidecar_file.fileno(), length=0, access=mmap.ACCESS_READ
        )
        self._string_values: list[str] = []
        self._string_ids: dict[str, int] = {}
        self._string_columns: dict[str, np.ndarray] = {
            field_name: np.full(self.row_count, -1, dtype=np.int32)
            for field_name in (
                *_EXCHANGE_STRING_FIELDS,
                "categories__0",
                "categories__1",
            )
        }
        self._numeric_kinds = {
            field_name: np.zeros(self.row_count, dtype=np.int8)
            for field_name in _EXCHANGE_NUMERIC_FIELDS
        }
        self._numeric_floats = {
            field_name: np.zeros(self.row_count, dtype=np.float64)
            for field_name in _EXCHANGE_NUMERIC_FIELDS
        }
        self._numeric_ints = {
            field_name: np.zeros(self.row_count, dtype=np.int64)
            for field_name in _EXCHANGE_NUMERIC_FIELDS
        }
        self._boolean_columns = {
            field_name: np.full(self.row_count, -1, dtype=np.int8)
            for field_name in _EXCHANGE_BOOLEAN_FIELDS
        }
        self.exchange_ids = np.empty(self.row_count, dtype=np.int64)
        self._load_exchange_columns(checkpoint / "exchanges.arrow")
        self._scenario_cache_string_values = [
            None if value in {"", "None", "nan"} else value
            for value in self._string_values
        ]
        self._scenario_cache_numeric_kinds = {}
        float_kinds = (
            _NUMERIC_PYTHON_FLOAT,
            _NUMERIC_FLOAT32,
            _NUMERIC_FLOAT64,
        )
        for field_name in _EXCHANGE_NUMERIC_FIELDS:
            compatible_kinds = self._numeric_kinds[field_name].copy()
            invalid = np.isin(compatible_kinds, float_kinds) & np.isnan(
                self._numeric_floats[field_name]
            )
            compatible_kinds[invalid] = _NUMERIC_MISSING
            self._scenario_cache_numeric_kinds[field_name] = compatible_kinds
        self._scenario_cache_invalid_common_masks = np.zeros(
            self.row_count, dtype=np.uint16
        )
        invalid_string_ids = np.asarray(
            [
                string_id
                for string_id, value in enumerate(self._scenario_cache_string_values)
                if value is None
            ],
            dtype=np.int32,
        )
        if len(invalid_string_ids):
            for position, field_name in enumerate(_EXCHANGE_STRING_FIELDS):
                invalid = np.isin(self._string_columns[field_name], invalid_string_ids)
                self._scenario_cache_invalid_common_masks[invalid] |= 1 << position
        offset = len(_EXCHANGE_STRING_FIELDS)
        for position, field_name in enumerate(_EXCHANGE_NUMERIC_FIELDS, offset):
            invalid = (
                self._numeric_kinds[field_name]
                != self._scenario_cache_numeric_kinds[field_name]
            )
            self._scenario_cache_invalid_common_masks[invalid] |= 1 << position
        expected = np.arange(self.row_count, dtype=np.int64)
        self._dense_ids = bool(np.array_equal(self.exchange_ids, expected))
        self._row_by_id = (
            None
            if self._dense_ids
            else {
                int(exchange_id): row
                for row, exchange_id in enumerate(self.exchange_ids)
            }
        )

    def _global_string_id(self, value: str) -> int:
        string_id = self._string_ids.get(value)
        if string_id is None:
            string_id = len(self._string_values)
            self._string_ids[value] = string_id
            self._string_values.append(value)
        return string_id

    def _copy_dictionary_column(
        self, array: Any, target: np.ndarray, start: int, stop: int
    ) -> None:
        if not pa.types.is_dictionary(array.type):
            array = array.dictionary_encode()
        local_values = array.dictionary.to_pylist()
        local_to_global = np.fromiter(
            (self._global_string_id(value) for value in local_values),
            dtype=np.int32,
            count=len(local_values),
        )
        if not local_values:
            target[start:stop] = -1
            return
        indices = array.indices.fill_null(0).to_numpy(zero_copy_only=False)
        target[start:stop] = local_to_global[indices]
        if array.null_count:
            nulls = array.is_null().to_numpy(zero_copy_only=False)
            target[start:stop][nulls] = -1

    def _load_exchange_columns(self, path: Path) -> None:
        cursor = 0
        try:
            with pa.memory_map(str(path), "r") as source:
                try:
                    reader = pa_ipc.open_stream(source)
                    batches = reader
                except pa.ArrowInvalid:
                    reader = pa_ipc.open_file(source)
                    batches = (
                        reader.get_batch(index)
                        for index in range(reader.num_record_batches)
                    )
                for batch in batches:
                    start = cursor
                    stop = start + batch.num_rows
                    if stop > self.row_count:
                        raise InventoryStoreCorruptionError(
                            "Exchange rows exceed the checkpoint manifest count."
                        )
                    names = {
                        name: index for index, name in enumerate(batch.schema.names)
                    }
                    self.exchange_ids[start:stop] = batch.column(
                        names["exchange_id"]
                    ).to_numpy(zero_copy_only=False)
                    for field_name, target in self._string_columns.items():
                        self._copy_dictionary_column(
                            batch.column(names[field_name]), target, start, stop
                        )
                    for field_name in _EXCHANGE_NUMERIC_FIELDS:
                        self._numeric_kinds[field_name][start:stop] = batch.column(
                            names[f"{field_name}__kind"]
                        ).to_numpy(zero_copy_only=False)
                        self._numeric_floats[field_name][start:stop] = (
                            batch.column(names[f"{field_name}__float"])
                            .fill_null(0)
                            .to_numpy(zero_copy_only=False)
                        )
                        self._numeric_ints[field_name][start:stop] = (
                            batch.column(names[f"{field_name}__int"])
                            .fill_null(0)
                            .to_numpy(zero_copy_only=False)
                        )
                    for field_name in _EXCHANGE_BOOLEAN_FIELDS:
                        values = batch.column(names[field_name])
                        target = self._boolean_columns[field_name]
                        target[start:stop] = values.fill_null(False).to_numpy(
                            zero_copy_only=False
                        )
                        if values.null_count:
                            target[start:stop][
                                values.is_null().to_numpy(zero_copy_only=False)
                            ] = -1
                    cursor = stop
        except (pa.ArrowInvalid, pa.ArrowIOError) as error:
            raise InventoryStoreCorruptionError(
                f"Cannot memory-map exchange columns from {path}."
            ) from error
        if cursor != self.row_count:
            raise InventoryStoreCorruptionError(
                "Exchange row count does not match the checkpoint manifest."
            )

    def row_for_id(self, exchange_id: ExchangeId) -> int:
        if self._dense_ids:
            if 0 <= exchange_id < self.row_count:
                return exchange_id
            raise KeyError(exchange_id)
        try:
            return self._row_by_id[exchange_id]
        except KeyError as error:
            raise KeyError(exchange_id) from error

    def exchange_id(self, row: int) -> ExchangeId:
        return int(self.exchange_ids[row])

    def activity_exchange_ids(self, activity_position: int) -> range | list[int]:
        start = int(self.exchange_starts[activity_position])
        stop = int(self.exchange_ends[activity_position])
        if self._dense_ids:
            return range(start, stop)
        return self.exchange_ids[start:stop].tolist()

    def _decode_sidecar_record(
        self,
        activity_id: ActivityId,
        offsets: Mapping[int, tuple[int, int]],
        record_kind: str,
    ) -> Any:
        try:
            offset, length = offsets[activity_id]
        except KeyError as error:
            raise InventoryStoreCorruptionError(
                f"Missing {record_kind} for activity {activity_id}."
            ) from error
        try:
            return pickle.loads(self._sidecar[offset : offset + length])
        except Exception as error:
            raise InventoryStoreCorruptionError(
                f"Invalid {record_kind} for activity {activity_id}."
            ) from error

    def activity_payload(self, activity_id: ActivityId) -> dict[str, Any]:
        with self._metadata_lock:
            payload = self._activity_cache.get(activity_id)
            if payload is None:
                payload = self._decode_sidecar_record(
                    activity_id, self.activity_offsets, "activity metadata"
                )
                if not isinstance(payload, dict):
                    raise InventoryStoreCorruptionError(
                        f"Invalid activity metadata for activity {activity_id}."
                    )
                self._activity_cache[activity_id] = payload
                if len(self._activity_cache) > self._CACHE_SIZE:
                    self._activity_cache.popitem(last=False)
            else:
                self._activity_cache.move_to_end(activity_id)
            return payload

    def base_activity_payload(self, activity_id: ActivityId) -> dict[str, Any]:
        """Materialize one immutable source activity without its exchanges."""

        position = self._activity_position(activity_id)
        payload = copy.deepcopy(self.activity_payload(activity_id))
        for field_name, values in self._activity_common_columns.items():
            value = values[position]
            if value is not None and field_name not in payload:
                payload[field_name] = value
        return payload

    def scenario_cache_base_activity_payload(
        self, activity_id: ActivityId
    ) -> dict[str, Any]:
        """Return a shallow base snapshot for read-only scenario diffing."""

        position = self._activity_position(activity_id)
        payload = self.activity_payload(activity_id).copy()
        for field_name, values in self._activity_common_columns.items():
            value = values[position]
            if value is not None and field_name not in payload:
                payload[field_name] = value
        return payload

    def _activity_position(self, activity_id: ActivityId) -> int:
        """Resolve an activity ID to its compact source-row position."""

        if self._activity_position_by_id is None:
            position = activity_id
            if not 0 <= position < len(self.activity_ids):
                raise KeyError(activity_id)
        else:
            try:
                position = self._activity_position_by_id[activity_id]
            except KeyError as error:
                raise KeyError(activity_id) from error
        return position

    def _activity_and_ordinal(self, row: int) -> tuple[ActivityId, int]:
        if not 0 <= row < self.row_count:
            raise KeyError(row)
        position = int(self._activity_position_by_row[row])
        return (
            int(self.activity_ids[position]),
            row - int(self.exchange_starts[position]),
        )

    def _metadata_for_activity(
        self, activity_id: ActivityId
    ) -> dict[int, dict[str, Any]]:
        with self._metadata_lock:
            metadata = self._metadata_cache.get(activity_id)
            if metadata is None:
                if activity_id in self.exchange_metadata_offsets:
                    records = self._decode_sidecar_record(
                        activity_id,
                        self.exchange_metadata_offsets,
                        "exchange metadata",
                    )
                    metadata = dict(records)
                else:
                    metadata = {}
                position = self._activity_position(activity_id)
                start = int(self.exchange_starts[position])
                stop = int(self.exchange_ends[position])
                masks = self._metadata_hot_field_masks
                masks[start:stop] = 0
                for ordinal, payload in metadata.items():
                    row = start + int(ordinal)
                    if not start <= row < stop:
                        raise InventoryStoreCorruptionError(
                            "Exchange metadata ordinal exceeds its activity block."
                        )
                    mask = 0
                    for field_name in payload:
                        mask |= _EXCHANGE_HOT_METADATA_BITS.get(field_name, 0)
                    masks[row] = mask
                self._metadata_cache[activity_id] = metadata
                if len(self._metadata_cache) > self._CACHE_SIZE:
                    self._metadata_cache.popitem(last=False)
            else:
                self._metadata_cache.move_to_end(activity_id)
        return metadata

    def metadata(self, row: int) -> Mapping[str, Any]:
        activity_id, ordinal = self._activity_and_ordinal(row)
        if activity_id == self._last_metadata_activity_id:
            metadata = self._last_metadata_group
        else:
            metadata = self._metadata_for_activity(activity_id)
            self._last_metadata_activity_id = activity_id
            self._last_metadata_group = metadata
        return metadata.get(ordinal, {})

    def _scenario_cache_metadata_for_activity(
        self, activity_id: ActivityId
    ) -> dict[int, dict[str, Any]]:
        """Return one activity's legacy-compatible exchange metadata."""

        metadata = self._metadata_for_activity(activity_id)
        with self._metadata_lock:
            if activity_id not in self._scenario_cache_scanned_metadata_activities:
                from .utils import (
                    _SCENARIO_TRIMMED_EXCHANGE_FIELDS,
                    _scenario_cache_exchange_field_is_restored,
                )

                for metadata_ordinal, payload in metadata.items():
                    if payload.keys() <= _SCENARIO_CACHE_STANDARD_SIDECAR_FIELDS:
                        uncertainty_type = payload.get(
                            "uncertainty type", _COLUMNAR_MISSING
                        )
                        loc = payload.get("loc", _COLUMNAR_MISSING)
                        production_volume = payload.get(
                            "production volume", _COLUMNAR_MISSING
                        )
                        categories = payload.get("categories", _COLUMNAR_MISSING)
                        if (
                            (
                                uncertainty_type is _COLUMNAR_MISSING
                                or type(uncertainty_type) is int
                            )
                            and (
                                loc is _COLUMNAR_MISSING
                                or (
                                    type(loc) in {np.float32, np.float64}
                                    and not np.isnan(loc)
                                )
                            )
                            and (
                                production_volume is _COLUMNAR_MISSING
                                or type(production_volume) is int
                                or (
                                    type(production_volume) in {np.float32, np.float64}
                                    and not np.isnan(production_volume)
                                )
                            )
                            and (
                                categories is _COLUMNAR_MISSING
                                or type(categories) is tuple
                            )
                        ):
                            continue

                    deleted_fields = []
                    for field_name, value in payload.items():
                        # Source sidecars overwhelmingly contain primitive
                        # uncertainty fields.  Reproduce the utility helpers'
                        # exact legacy semantics inline so compatible rows do
                        # not allocate a filtered dictionary or make millions
                        # of Python function calls during the first checkpoint.
                        value_type = type(value)
                        if field_name in _SCENARIO_TRIMMED_EXCHANGE_FIELDS:
                            if value is None:
                                restored = False
                            elif value_type is str:
                                restored = value not in {"", "None", "nan"}
                            elif value_type in {list, tuple, dict, set, bool, int}:
                                restored = True
                            elif value_type is float:
                                restored = value == value
                            elif value_type in {np.float32, np.float64}:
                                restored = not np.isnan(value)
                            else:
                                restored = _scenario_cache_exchange_field_is_restored(
                                    field_name, value
                                )
                        elif value is None:
                            restored = False
                        elif value_type is str:
                            restored = value not in {"", "None", "nan"}
                        elif value_type in {list, tuple, dict, set, bool, int}:
                            restored = bool(value)
                        elif value_type is float:
                            restored = value == value and bool(value)
                        elif value_type in {np.float32, np.float64}:
                            restored = not np.isnan(value) and bool(value)
                        else:
                            restored = _scenario_cache_exchange_field_is_restored(
                                field_name, value
                            )
                        if not restored:
                            deleted_fields.append(field_name)

                    if deleted_fields:
                        deleted_field_set = set(deleted_fields)
                        filtered = {
                            field_name: value
                            for field_name, value in payload.items()
                            if field_name not in deleted_field_set
                        }
                        compatible_metadata = (
                            self._scenario_cache_filtered_metadata.get(activity_id)
                        )
                        if compatible_metadata is None:
                            compatible_metadata = metadata.copy()
                            self._scenario_cache_filtered_metadata[activity_id] = (
                                compatible_metadata
                            )
                        compatible_metadata[metadata_ordinal] = filtered
                        deleted_fields = tuple(deleted_fields)
                        self._scenario_cache_deleted_metadata[
                            (activity_id, metadata_ordinal)
                        ] = deleted_fields
                        if self._activity_position_by_id is None:
                            activity_position = activity_id
                        else:
                            activity_position = self._activity_position_by_id[
                                activity_id
                            ]
                        row = (
                            int(self.exchange_starts[activity_position])
                            + metadata_ordinal
                        )
                        self._scenario_cache_deleted_fields_by_row[row] = deleted_fields
                self._scenario_cache_scanned_metadata_activities.add(activity_id)
                self._scenario_cache_scan_complete = len(
                    self._scenario_cache_scanned_metadata_activities
                ) >= len(self.exchange_metadata_offsets)
        return self._scenario_cache_filtered_metadata.get(activity_id, metadata)

    def scenario_cache_deleted_fields(self, row: int) -> tuple[str, ...]:
        """Return sidecar fields omitted by legacy scenario-cache loading."""

        if self._scenario_cache_scan_complete:
            return self._scenario_cache_deleted_fields_by_row.get(row, ())
        activity_id, ordinal = self._activity_and_ordinal(row)
        if activity_id not in self.exchange_metadata_offsets:
            return ()
        if activity_id not in self._scenario_cache_scanned_metadata_activities:
            self._scenario_cache_metadata_for_activity(activity_id)
        return self._scenario_cache_deleted_fields_by_row.get(row, ())

    def scenario_cache_compatible_rows(self) -> np.ndarray:
        """Return source rows needing no legacy compatibility deletions."""

        compatible = self._scenario_cache_compatible_rows
        if compatible is not None:
            return compatible

        for activity_id in self.exchange_metadata_offsets:
            if activity_id not in self._scenario_cache_scanned_metadata_activities:
                self._scenario_cache_metadata_for_activity(activity_id)

        compatible = self._scenario_cache_invalid_common_masks == 0
        if self._scenario_cache_deleted_fields_by_row:
            compatible[list(self._scenario_cache_deleted_fields_by_row)] = False
        compatible.flags.writeable = False
        self._scenario_cache_compatible_rows = compatible
        return compatible

    def scenario_cache_metadata(self, row: int) -> Mapping[str, Any]:
        """Return metadata with legacy scenario-cache semantics.

        Source-column metadata is immutable and shared by every scenario-year.
        Check each activity block only once, retaining filtered copies solely
        for the uncommon rows which contain values the legacy loader drops.
        """

        activity_id, ordinal = self._activity_and_ordinal(row)
        if activity_id == self._last_scenario_metadata_activity_id:
            metadata = self._last_scenario_metadata_group
        else:
            metadata = self._scenario_cache_metadata_for_activity(activity_id)
            self._last_scenario_metadata_activity_id = activity_id
            self._last_scenario_metadata_group = metadata
        return metadata.get(ordinal, {})

    def common_keys(self, row: int) -> Iterator[str]:
        for field_name in _EXCHANGE_STRING_FIELDS:
            if self._string_columns[field_name][row] >= 0:
                yield field_name
        if (
            self._string_columns["categories__0"][row] >= 0
            and self._string_columns["categories__1"][row] >= 0
        ):
            yield "categories"
        for field_name in _EXCHANGE_NUMERIC_FIELDS:
            if self._numeric_kinds[field_name][row] != _NUMERIC_MISSING:
                yield field_name
        for field_name in _EXCHANGE_BOOLEAN_FIELDS:
            if self._boolean_columns[field_name][row] >= 0:
                yield field_name

    def common_value(self, row: int, field_name: str) -> Any:
        if field_name in _EXCHANGE_STRING_FIELDS:
            string_id = int(self._string_columns[field_name][row])
            if string_id >= 0:
                return self._string_values[string_id]
            raise KeyError(field_name)
        if field_name == "categories":
            first = int(self._string_columns["categories__0"][row])
            second = int(self._string_columns["categories__1"][row])
            if first >= 0 and second >= 0:
                return self._string_values[first], self._string_values[second]
            raise KeyError(field_name)
        if field_name in _EXCHANGE_NUMERIC_FIELDS:
            kind = int(self._numeric_kinds[field_name][row])
            if kind != _NUMERIC_MISSING:
                return _decode_numeric_column(
                    kind,
                    self._numeric_floats[field_name][row],
                    self._numeric_ints[field_name][row],
                )
            raise KeyError(field_name)
        if field_name in _EXCHANGE_BOOLEAN_FIELDS:
            value = int(self._boolean_columns[field_name][row])
            if value >= 0:
                return bool(value)
            raise KeyError(field_name)
        raise KeyError(field_name)

    def biosphere_category_masks(
        self, categories: tuple[str, str], unit: str
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return exact-match and fully-columnar source-row masks."""

        cache_key = (categories, unit)
        masks = self._biosphere_category_masks.get(cache_key)
        if masks is not None:
            return masks

        string_ids = tuple(
            self._string_ids.get(value)
            for value in ("biosphere", categories[0], categories[1], unit)
        )
        type_column = self._string_columns["type"]
        encoded_predicates = (
            (self._string_columns["categories__0"] >= 0)
            & (self._string_columns["categories__1"] >= 0)
            & (self._string_columns["unit"] >= 0)
        )
        if any(string_id is None for string_id in string_ids):
            mask = np.zeros(self.row_count, dtype=np.bool_)
        else:
            type_id, first_id, second_id, unit_id = string_ids
            mask = (
                (type_column == type_id)
                & (self._string_columns["categories__0"] == first_id)
                & (self._string_columns["categories__1"] == second_id)
                & (self._string_columns["unit"] == unit_id)
            )
        biosphere_type_id = self._string_ids.get("biosphere")
        if biosphere_type_id is None:
            decisive = type_column >= 0
        else:
            decisive = (type_column >= 0) & (
                (type_column != biosphere_type_id) | encoded_predicates
            )
        mask.flags.writeable = False
        decisive.flags.writeable = False
        masks = mask, decisive
        self._biosphere_category_masks[cache_key] = masks
        return masks

    def close(self) -> None:
        sidecar = getattr(self, "_sidecar", None)
        if sidecar is not None:
            sidecar.close()
            self._sidecar = None
        sidecar_file = getattr(self, "_sidecar_file", None)
        if sidecar_file is not None:
            sidecar_file.close()
            self._sidecar_file = None

    def __del__(self) -> None:  # pragma: no cover - interpreter shutdown varies
        try:
            self.close()
        except Exception:
            pass


class _CompactExchangeMapping(MutableMapping[str, Any]):
    """Low-overhead mutable mapping for exchanges created during a build.

    Relinking creates hundreds of thousands of exchanges with the same six
    fields. A regular dictionary allocates a hash table for every one of them;
    this mapping keeps those common values in slots and allocates an overflow
    dictionary only if uncommon metadata is added later.
    """

    __slots__ = (
        "_name",
        "_product",
        "_amount",
        "_type",
        "_unit",
        "_location",
        "_present",
        "_extra",
    )
    _premise_compact_exchange = True

    def __init__(self, payload: Mapping[str, Any] | None = None) -> None:
        self._name = None
        self._product = None
        self._amount = None
        self._type = None
        self._unit = None
        self._location = None
        self._present = 0
        self._extra: dict[Any, Any] | None = None
        if payload is not None:
            for key, value in payload.items():
                self[key] = value

    def __getitem__(self, key: str) -> Any:
        field_bit = _COMPACT_EXCHANGE_FIELD_BITS.get(key)
        if field_bit is not None:
            if not self._present & field_bit:
                raise KeyError(key)
            return getattr(self, _COMPACT_EXCHANGE_FIELD_ATTRIBUTES[key])
        if self._extra is None:
            raise KeyError(key)
        return self._extra[key]

    def get(self, key: str, default: Any = None) -> Any:
        field_bit = _COMPACT_EXCHANGE_FIELD_BITS.get(key)
        if field_bit is not None:
            if self._present & field_bit:
                return getattr(self, _COMPACT_EXCHANGE_FIELD_ATTRIBUTES[key])
            return default
        if self._extra is None:
            return default
        return self._extra.get(key, default)

    def __setitem__(self, key: str, value: Any) -> None:
        field_bit = _COMPACT_EXCHANGE_FIELD_BITS.get(key)
        if field_bit is not None:
            setattr(self, _COMPACT_EXCHANGE_FIELD_ATTRIBUTES[key], value)
            self._present |= field_bit
            return
        if self._extra is None:
            self._extra = {}
        self._extra[key] = value

    def __delitem__(self, key: str) -> None:
        field_bit = _COMPACT_EXCHANGE_FIELD_BITS.get(key)
        if field_bit is not None:
            if not self._present & field_bit:
                raise KeyError(key)
            self._present &= ~field_bit
            setattr(self, _COMPACT_EXCHANGE_FIELD_ATTRIBUTES[key], None)
            return
        if self._extra is None:
            raise KeyError(key)
        del self._extra[key]
        if not self._extra:
            self._extra = None

    def __iter__(self) -> Iterator[str]:
        for field_name in _COMPACT_EXCHANGE_FIELDS:
            if self._present & _COMPACT_EXCHANGE_FIELD_BITS[field_name]:
                yield field_name
        if self._extra is not None:
            yield from self._extra

    def __len__(self) -> int:
        return self._present.bit_count() + (
            len(self._extra) if self._extra is not None else 0
        )

    def copy(self) -> dict[str, Any]:
        return dict(self.items())

    def _premise_relink_fields(self) -> tuple[Any, Any, Any, Any, Any, Any]:
        """Return the hot semantic fields used by relinking in one dispatch."""

        extra = self._extra or {}
        product = (
            extra["reference product"]
            if "reference product" in extra
            else self._product
        )
        return (
            self._type,
            self._name,
            product,
            self._location,
            self._unit,
            self._amount,
        )

    def _premise_exchange_type(self) -> Any:
        """Return the resident type without allocating or mapping dispatch."""

        if not self._present & _COMPACT_EXCHANGE_FIELD_BITS["type"]:
            raise KeyError("type")
        return self._type

    def _premise_relink_technosphere_fields(
        self,
    ) -> tuple[Any, Any, Any, Any, Any]:
        """Return relinking fields after the caller has checked the type."""

        extra = self._extra or {}
        product = (
            extra["reference product"]
            if "reference product" in extra
            else self._product
        )
        return self._name, product, self._location, self._unit, self._amount

    def _premise_export_fields(
        self,
    ) -> tuple[Any, Any, Any, Any, Any, Any, Any, Any]:
        """Return fields needed by fast export normalization and validation."""

        extra = self._extra or {}
        return (
            self._type,
            self._name,
            self._product,
            self._location,
            self._unit,
            self._amount,
            extra.get("categories"),
            extra.get("input"),
        )

    def _premise_fast_export_payload(self) -> dict[str, Any]:
        """Materialize an export row without generic mapping iteration."""

        payload = {}
        for field_name in _COMPACT_EXCHANGE_FIELDS:
            if self._present & _COMPACT_EXCHANGE_FIELD_BITS[field_name]:
                payload[field_name] = getattr(
                    self, _COMPACT_EXCHANGE_FIELD_ATTRIBUTES[field_name]
                )
        if self._extra is not None:
            payload.update(self._extra)
        return payload

    def _premise_materialize(self) -> dict[str, Any]:
        return self.copy()

    def _premise_clone(self, memo: dict[int, Any]) -> "_CompactExchangeMapping":
        duplicate = type(self)()
        memo[id(self)] = duplicate
        duplicate._name = copy.deepcopy(self._name, memo)
        duplicate._product = copy.deepcopy(self._product, memo)
        duplicate._amount = copy.deepcopy(self._amount, memo)
        duplicate._type = copy.deepcopy(self._type, memo)
        duplicate._unit = copy.deepcopy(self._unit, memo)
        duplicate._location = copy.deepcopy(self._location, memo)
        duplicate._present = self._present
        if self._extra is not None:
            duplicate._extra = copy.deepcopy(self._extra, memo)
        return duplicate

    def __copy__(self) -> "_CompactExchangeMapping":
        return self._premise_clone({})

    def __deepcopy__(self, memo: dict[int, Any]) -> "_CompactExchangeMapping":
        existing = memo.get(id(self))
        if existing is not None:
            return existing
        return self._premise_clone(memo)

    def __repr__(self) -> str:
        return repr(self.copy())

    def __reduce_ex__(self, protocol: int):
        del protocol
        return type(self), (self.copy(),)


def compact_exchange_payload(payload: Mapping[str, Any]) -> MutableMapping[str, Any]:
    """Return a compact mutable exchange without changing mapping semantics."""

    if isinstance(payload, _CompactExchangeMapping):
        return payload
    return _CompactExchangeMapping(payload)


class _ColumnarExchangeMapping(MutableMapping[str, Any]):
    """Mutable copy-on-write view over one compact exchange row."""

    __slots__ = ("_storage", "_row", "_changes", "_validation_owner")

    def __init__(self, storage: _ColumnarExchangeStorage, row: int) -> None:
        self._storage = storage
        self._row = row
        self._changes: tuple[str, Any] | list[Any] | None = None
        self._validation_owner: _ColumnarActivityMapping | None = None

    def _changed_value(self, key: str) -> Any:
        if self._changes is None:
            return _COLUMNAR_MISSING
        if isinstance(self._changes, tuple):
            return self._changes[1] if self._changes[0] == key else _COLUMNAR_MISSING
        for position in range(0, len(self._changes), 2):
            if self._changes[position] == key:
                return self._changes[position + 1]
        return _COLUMNAR_MISSING

    def _set_change(self, key: str, value: Any) -> None:
        owner = self._validation_owner
        if owner is not None:
            recorder = getattr(owner, "_record_validation_modification", None)
            if recorder is not None:
                recorder()
            else:
                # Scenario-delta checkpoints reconstruct activity overlays as
                # ordinary dictionaries while retaining columnar exchange
                # views. Keep scope tracking functional for those overlays;
                # exporter normalization normally has no active sink.
                sink = self._storage._validation_modified_targets
                if sink is not None:
                    sink[id(owner)] = owner
        if self._changes is None:
            self._changes = (key, value)
        elif isinstance(self._changes, tuple):
            previous_key, previous_value = self._changes
            if previous_key == key:
                self._changes = (key, value)
            else:
                self._changes = [previous_key, previous_value, key, value]
        else:
            for position in range(0, len(self._changes), 2):
                if self._changes[position] == key:
                    self._changes[position + 1] = value
                    break
            else:
                self._changes.extend((key, value))

    def _iter_changes(self) -> Iterator[tuple[str, Any]]:
        if self._changes is None:
            return
        if isinstance(self._changes, tuple):
            yield self._changes
        else:
            for position in range(0, len(self._changes), 2):
                yield self._changes[position], self._changes[position + 1]

    def _metadata_value(self, key: str, default: Any = _COLUMNAR_MISSING) -> Any:
        """Read one sidecar value, retaining copy-on-write isolation."""

        storage = self._storage
        row = self._row
        field_bit = _EXCHANGE_HOT_METADATA_BITS.get(key)
        metadata_mask = int(storage._metadata_hot_field_masks[row])
        if (
            field_bit is not None
            and metadata_mask >= 0
            and not metadata_mask & field_bit
        ):
            if default is not _COLUMNAR_MISSING:
                return default
            raise KeyError(key)
        metadata = storage.metadata(row)
        if key not in metadata:
            if default is not _COLUMNAR_MISSING:
                return default
            raise KeyError(key)
        value = metadata[key]
        if isinstance(value, (dict, list, set)):
            value = copy.deepcopy(value)
            self._set_change(key, value)
        return value

    def _checkpoint_value(self, key: str, metadata: Mapping[str, Any]) -> Any:
        """Return one effective value without generic mapping iteration."""

        value = self._changed_value(key)
        if value is _COLUMNAR_DELETED:
            return _UNHASHABLE
        if value is not _COLUMNAR_MISSING:
            return value
        try:
            return self._storage.common_value(self._row, key)
        except KeyError:
            return metadata.get(key, _UNHASHABLE)

    def _premise_append_checkpoint_payload(
        self,
        columns: Mapping[str, list[Any]],
        collect_string: Callable[[Any], None] | None,
        scenario_cache_compatibility: bool = False,
    ) -> Mapping[str, Any]:
        """Append this row directly to checkpoint columns.

        The generic mapping path repeatedly reconstructs all keys and consults
        the sidecar for every field. A columnar row already knows which fields
        are resident, so one metadata lookup is sufficient.
        """

        storage = self._storage
        row = self._row
        if self._changes is None:
            string_values = (
                storage._scenario_cache_string_values
                if scenario_cache_compatibility
                else storage._string_values
            )
            for field_name in _EXCHANGE_STRING_FIELDS:
                string_id = int(storage._string_columns[field_name][row])
                value = string_values[string_id] if string_id >= 0 else None
                columns[field_name].append(value)
                if collect_string is not None:
                    collect_string(value)

            first = int(storage._string_columns["categories__0"][row])
            second = int(storage._string_columns["categories__1"][row])
            columns["categories__0"].append(
                storage._string_values[first] if first >= 0 else None
            )
            columns["categories__1"].append(
                storage._string_values[second] if second >= 0 else None
            )
            if collect_string is not None and first >= 0 and second >= 0:
                collect_string(storage._string_values[first])
                collect_string(storage._string_values[second])

            for field_name in _EXCHANGE_NUMERIC_FIELDS:
                kinds = (
                    storage._scenario_cache_numeric_kinds[field_name]
                    if scenario_cache_compatibility
                    else storage._numeric_kinds[field_name]
                )
                kind = int(kinds[row])
                float_value = (
                    float(storage._numeric_floats[field_name][row])
                    if kind
                    in {
                        _NUMERIC_PYTHON_FLOAT,
                        _NUMERIC_FLOAT32,
                        _NUMERIC_FLOAT64,
                    }
                    else None
                )
                columns[f"{field_name}__kind"].append(kind)
                columns[f"{field_name}__float"].append(float_value)
                columns[f"{field_name}__int"].append(
                    int(storage._numeric_ints[field_name][row])
                    if kind == _NUMERIC_PYTHON_INT
                    else None
                )

            for field_name in _EXCHANGE_BOOLEAN_FIELDS:
                value = int(storage._boolean_columns[field_name][row])
                columns[field_name].append(bool(value) if value >= 0 else None)

            if scenario_cache_compatibility:
                return storage.scenario_cache_metadata(row)
            return storage.metadata(row)

        metadata = dict(storage.metadata(row))
        for field_name in _EXCHANGE_STRING_FIELDS:
            value = self._checkpoint_value(field_name, metadata)
            if (
                scenario_cache_compatibility
                and isinstance(value, str)
                and value in {"", "None", "nan"}
            ):
                value = None
            columns[field_name].append(value if isinstance(value, str) else None)
            if collect_string is not None:
                collect_string(value)

        categories = self._checkpoint_value("categories", metadata)
        valid_categories = (
            isinstance(categories, tuple)
            and len(categories) == 2
            and all(isinstance(item, str) for item in categories)
        )
        columns["categories__0"].append(categories[0] if valid_categories else None)
        columns["categories__1"].append(categories[1] if valid_categories else None)
        if collect_string is not None and valid_categories:
            collect_string(categories[0])
            collect_string(categories[1])

        numeric_kinds = {}
        for field_name in _EXCHANGE_NUMERIC_FIELDS:
            value = self._checkpoint_value(field_name, metadata)
            kind, float_value, int_value = _numeric_column_parts(value)
            if (
                scenario_cache_compatibility
                and float_value is not None
                and float_value != float_value
            ):
                kind, float_value, int_value = _NUMERIC_MISSING, None, None
            numeric_kinds[field_name] = kind
            columns[f"{field_name}__kind"].append(kind)
            columns[f"{field_name}__float"].append(float_value)
            columns[f"{field_name}__int"].append(int_value)

        for field_name in _EXCHANGE_BOOLEAN_FIELDS:
            value = self._checkpoint_value(field_name, metadata)
            columns[field_name].append(value if type(value) is bool else None)

        for key, value in self._iter_changes():
            if value is _COLUMNAR_DELETED:
                metadata.pop(key, None)
                continue
            field_metadata = _exchange_sidecar_metadata(
                {key: value},
                numeric_kinds=(numeric_kinds if key in numeric_kinds else None),
            )
            if field_metadata:
                metadata[key] = value
            else:
                metadata.pop(key, None)
        if scenario_cache_compatibility:
            from .utils import _normalize_scenario_cache_exchange

            _normalize_scenario_cache_exchange(metadata)
        return metadata

    def __getitem__(self, key: str) -> Any:
        if self._changes is not None:
            value = self._changed_value(key)
            if value is not _COLUMNAR_MISSING:
                if value is _COLUMNAR_DELETED:
                    raise KeyError(key)
                return value

        storage = self._storage
        row = self._row
        if key in _EXCHANGE_STRING_FIELD_SET:
            string_id = int(storage._string_columns[key][row])
            if string_id >= 0:
                return storage._string_values[string_id]
        elif key in _EXCHANGE_NUMERIC_FIELDS:
            kind = int(storage._numeric_kinds[key][row])
            if kind != _NUMERIC_MISSING:
                return _decode_numeric_column(
                    kind,
                    storage._numeric_floats[key][row],
                    storage._numeric_ints[key][row],
                )
        elif key == "categories":
            first = int(storage._string_columns["categories__0"][row])
            second = int(storage._string_columns["categories__1"][row])
            if first >= 0 and second >= 0:
                return storage._string_values[first], storage._string_values[second]
        elif key in _EXCHANGE_BOOLEAN_FIELDS:
            boolean = int(storage._boolean_columns[key][row])
            if boolean >= 0:
                return bool(boolean)
        return self._metadata_value(key)

    def get(self, key: str, default: Any = None) -> Any:
        if self._changes is not None:
            value = self._changed_value(key)
            if value is _COLUMNAR_DELETED:
                return default
            if value is not _COLUMNAR_MISSING:
                return value

        storage = self._storage
        row = self._row
        if key in _EXCHANGE_STRING_FIELD_SET:
            string_id = int(storage._string_columns[key][row])
            if string_id >= 0:
                return storage._string_values[string_id]
        elif key in _EXCHANGE_NUMERIC_FIELDS:
            kind = int(storage._numeric_kinds[key][row])
            if kind != _NUMERIC_MISSING:
                return _decode_numeric_column(
                    kind,
                    storage._numeric_floats[key][row],
                    storage._numeric_ints[key][row],
                )
        elif key == "categories":
            first = int(storage._string_columns["categories__0"][row])
            second = int(storage._string_columns["categories__1"][row])
            if first >= 0 and second >= 0:
                return storage._string_values[first], storage._string_values[second]
        elif key in _EXCHANGE_BOOLEAN_FIELDS:
            boolean = int(storage._boolean_columns[key][row])
            if boolean >= 0:
                return bool(boolean)
        return self._metadata_value(key, default)

    def _premise_relink_fields(self) -> tuple[Any, Any, Any, Any, Any, Any]:
        """Decode the hot relinking tuple without generic mapping dispatch."""

        return (
            self._premise_exchange_type(),
            *self._premise_relink_technosphere_fields(),
        )

    def _premise_exchange_type(self) -> Any:
        """Decode only the resident type column and any sparse override."""

        changed = self._changed_value("type")
        if changed is _COLUMNAR_DELETED:
            raise KeyError("type")
        if changed is not _COLUMNAR_MISSING:
            return changed
        string_id = int(self._storage._string_columns["type"][self._row])
        if string_id < 0:
            raise KeyError("type")
        return self._storage._string_values[string_id]

    def _premise_relink_technosphere_fields(
        self,
    ) -> tuple[Any, Any, Any, Any, Any]:
        """Decode relinking columns after a technosphere type check."""

        storage = self._storage
        row = self._row

        def string_value(field_name: str) -> Any:
            changed = self._changed_value(field_name)
            if changed is _COLUMNAR_DELETED:
                return None
            if changed is not _COLUMNAR_MISSING:
                return changed
            string_id = int(storage._string_columns[field_name][row])
            return storage._string_values[string_id] if string_id >= 0 else None

        changed_amount = self._changed_value("amount")
        if changed_amount is _COLUMNAR_DELETED:
            amount = None
        elif changed_amount is not _COLUMNAR_MISSING:
            amount = changed_amount
        else:
            kind = int(storage._numeric_kinds["amount"][row])
            amount = (
                _decode_numeric_column(
                    kind,
                    storage._numeric_floats["amount"][row],
                    storage._numeric_ints["amount"][row],
                )
                if kind != _NUMERIC_MISSING
                else None
            )

        changed_reference_product = self._changed_value("reference product")
        if changed_reference_product is _COLUMNAR_DELETED:
            reference_product_present = False
            reference_product = None
        elif changed_reference_product is not _COLUMNAR_MISSING:
            reference_product_present = True
            reference_product = changed_reference_product
        else:
            reference_product_present = False
            reference_product = None
            field_bit = _EXCHANGE_HOT_METADATA_BITS["reference product"]
            metadata_mask = int(storage._metadata_hot_field_masks[row])
            if metadata_mask < 0 or metadata_mask & field_bit:
                metadata = storage.metadata(row)
                if "reference product" in metadata:
                    reference_product_present = True
                    reference_product = metadata["reference product"]

        return (
            string_value("name"),
            reference_product if reference_product_present else string_value("product"),
            string_value("location"),
            string_value("unit"),
            amount,
        )

    def _premise_export_fields(
        self,
    ) -> tuple[Any, Any, Any, Any, Any, Any, Any, Any]:
        """Return fields needed by fast export normalization and validation."""

        storage = self._storage
        row = self._row

        def string_value(field_name: str) -> Any:
            changed = self._changed_value(field_name)
            if changed is _COLUMNAR_DELETED:
                return None
            if changed is not _COLUMNAR_MISSING:
                return changed
            string_id = int(storage._string_columns[field_name][row])
            return storage._string_values[string_id] if string_id >= 0 else None

        changed_amount = self._changed_value("amount")
        if changed_amount is _COLUMNAR_DELETED:
            amount = None
        elif changed_amount is not _COLUMNAR_MISSING:
            amount = changed_amount
        else:
            kind = int(storage._numeric_kinds["amount"][row])
            amount = (
                _decode_numeric_column(
                    kind,
                    storage._numeric_floats["amount"][row],
                    storage._numeric_ints["amount"][row],
                )
                if kind != _NUMERIC_MISSING
                else None
            )

        return (
            string_value("type"),
            string_value("name"),
            string_value("product"),
            string_value("location"),
            string_value("unit"),
            amount,
            self.get("categories"),
            self.get("input"),
        )

    def __setitem__(self, key: str, value: Any) -> None:
        self._set_change(key, value)

    def __delitem__(self, key: str) -> None:
        try:
            self[key]
        except KeyError:
            raise KeyError(key) from None
        self._set_change(key, _COLUMNAR_DELETED)

    def __iter__(self) -> Iterator[str]:
        yielded = set()
        for key in self._storage.common_keys(self._row):
            if self._changed_value(key) is not _COLUMNAR_DELETED:
                yielded.add(key)
                yield key
        for key in self._storage.metadata(self._row):
            if key in yielded:
                continue
            if self._changed_value(key) is not _COLUMNAR_DELETED:
                yielded.add(key)
                yield key
        for key, value in self._iter_changes():
            if key not in yielded and value is not _COLUMNAR_DELETED:
                yield key

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def copy(self) -> dict[str, Any]:
        payload = {
            key: self._storage.common_value(self._row, key)
            for key in self._storage.common_keys(self._row)
        }
        for key, value in self._storage.metadata(self._row).items():
            changed = self._changed_value(key)
            if changed is _COLUMNAR_DELETED:
                continue
            if changed is not _COLUMNAR_MISSING:
                payload[key] = changed
                continue
            if isinstance(value, (dict, list, set)):
                value = copy.deepcopy(value)
                self._set_change(key, value)
            payload[key] = value
        for key, value in self._iter_changes():
            if value is _COLUMNAR_DELETED:
                payload.pop(key, None)
            else:
                payload[key] = value
        return payload

    def _premise_fast_export_payload(self) -> dict[str, Any]:
        """Return the effective row with one sidecar lookup.

        Export payloads are serialized immediately and never mutate nested
        metadata. Avoiding ``MutableMapping.items`` here removes repeated
        column and sidecar lookups for every field of every exchange.
        """

        storage = self._storage
        row = self._row
        payload = {}
        string_columns = storage._string_columns
        string_values = storage._string_values
        for field_name in _EXCHANGE_STRING_FIELDS:
            string_id = int(string_columns[field_name][row])
            if string_id >= 0:
                payload[field_name] = string_values[string_id]

        first = int(string_columns["categories__0"][row])
        second = int(string_columns["categories__1"][row])
        if first >= 0 and second >= 0:
            payload["categories"] = string_values[first], string_values[second]

        for field_name in _EXCHANGE_NUMERIC_FIELDS:
            kind = int(storage._numeric_kinds[field_name][row])
            if kind != _NUMERIC_MISSING:
                payload[field_name] = _decode_numeric_column(
                    kind,
                    storage._numeric_floats[field_name][row],
                    storage._numeric_ints[field_name][row],
                )

        for field_name in _EXCHANGE_BOOLEAN_FIELDS:
            value = int(storage._boolean_columns[field_name][row])
            if value >= 0:
                payload[field_name] = bool(value)

        payload.update(storage.metadata(row))
        for key, value in self._iter_changes():
            if value is _COLUMNAR_DELETED:
                payload.pop(key, None)
            else:
                payload[key] = value
        return payload

    def __copy__(self) -> dict[str, Any]:
        return self.copy()

    def __deepcopy__(self, memo: dict[int, Any]) -> "_ColumnarExchangeMapping":
        existing = memo.get(id(self))
        if existing is not None:
            return existing
        return self._premise_clone(memo)

    def _premise_clone(self, memo: dict[int, Any]) -> "_ColumnarExchangeMapping":
        """Clone an overlay while retaining the immutable source row."""

        duplicate = type(self)(self._storage, self._row)
        memo[id(self)] = duplicate
        duplicate._validation_owner = self._validation_owner
        if isinstance(self._changes, tuple):
            key, value = self._changes
            duplicate._changes = (
                copy.deepcopy(key, memo),
                (
                    _COLUMNAR_DELETED
                    if value is _COLUMNAR_DELETED
                    else copy.deepcopy(value, memo)
                ),
            )
        elif self._changes is not None:
            duplicate._changes = []
            for position in range(0, len(self._changes), 2):
                key = self._changes[position]
                value = self._changes[position + 1]
                duplicate._changes.extend(
                    (
                        copy.deepcopy(key, memo),
                        (
                            _COLUMNAR_DELETED
                            if value is _COLUMNAR_DELETED
                            else copy.deepcopy(value, memo)
                        ),
                    )
                )
        return duplicate

    def __repr__(self) -> str:
        return repr(self.copy())


def filter_biosphere_category(
    exchanges: Iterable[Mapping[str, Any]],
    categories: tuple[str, ...],
    unit: str,
) -> list[Mapping[str, Any]]:
    """Filter biosphere exchanges while preserving ordered mapping semantics."""

    matches = []
    append = matches.append
    encoded_categories = len(categories) == 2
    source_storage = None
    match_mask = None
    decisive_mask = None
    for exchange in exchanges:
        if (
            encoded_categories
            and isinstance(exchange, _ColumnarExchangeMapping)
            and exchange._changes is None
        ):
            storage = exchange._storage
            if storage is not source_storage:
                match_mask, decisive_mask = storage.biosphere_category_masks(
                    categories, unit
                )
                source_storage = storage
            if match_mask[exchange._row]:
                append(exchange)
                continue
            if decisive_mask[exchange._row]:
                continue

        if (
            exchange.get("type") == "biosphere"
            and tuple(exchange.get("categories", ())) == categories
            and exchange.get("unit") == unit
        ):
            append(exchange)
    return matches


class _ColumnarActivityMapping(dict[str, Any]):
    """Dictionary-compatible activity with lazy uncommon metadata."""

    __slots__ = (
        "_storage",
        "_activity_id",
        "_deleted",
        "_database",
        "_code",
        "_type",
    )

    def __init__(
        self,
        storage: _ColumnarExchangeStorage,
        activity_id: ActivityId,
        common: Mapping[str, Any],
    ) -> None:
        resident = dict(common)
        self._database = resident.pop("database", _COLUMNAR_ACTIVITY_MISSING)
        self._code = resident.pop("code", _COLUMNAR_ACTIVITY_MISSING)
        self._type = resident.pop("type", _COLUMNAR_ACTIVITY_MISSING)
        super().__init__(resident)
        self._storage = storage
        self._activity_id = activity_id
        self._deleted: set[str] = set()

    def _metadata(self) -> Mapping[str, Any]:
        return self._storage.activity_payload(self._activity_id)

    def _record_validation_modification(self) -> None:
        sink = self._storage._validation_modified_targets
        if sink is not None:
            sink[id(self)] = self

    def _premise_common_value(self, key: str, default: Any = None) -> Any:
        """Read an Arrow-resident field without touching the metadata sidecar."""

        if key in self._deleted:
            return default
        hot_attribute = _ACTIVITY_HOT_FIELD_ATTRIBUTES.get(key)
        if hot_attribute is not None:
            value = getattr(self, hot_attribute)
            return default if value is _COLUMNAR_ACTIVITY_MISSING else value
        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        return default

    def _premise_prepare_fast_export(self) -> None:
        """Seed missing hot fields without decoding uncommon metadata."""

        if self._type is _COLUMNAR_ACTIVITY_MISSING:
            self._deleted.discard("type")
            self._type = None

    @staticmethod
    def _requires_private_copy(value: Any) -> bool:
        return not isinstance(
            value,
            (
                type(None),
                bool,
                int,
                float,
                complex,
                str,
                bytes,
                tuple,
                frozenset,
                np.generic,
            ),
        )

    def __getitem__(self, key: str) -> Any:
        hot_attribute = _ACTIVITY_HOT_FIELD_ATTRIBUTES.get(key)
        if hot_attribute is not None:
            if key in self._deleted:
                raise KeyError(key)
            value = getattr(self, hot_attribute)
            if value is not _COLUMNAR_ACTIVITY_MISSING:
                return value
        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        if key in self._deleted:
            raise KeyError(key)
        metadata = self._metadata()
        if key not in metadata:
            raise KeyError(key)
        value = metadata[key]
        if self._requires_private_copy(value):
            value = copy.deepcopy(value)
            if hot_attribute is not None:
                setattr(self, hot_attribute, value)
            else:
                dict.__setitem__(self, key, value)
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        self._record_validation_modification()
        self._deleted.discard(key)
        hot_attribute = _ACTIVITY_HOT_FIELD_ATTRIBUTES.get(key)
        if hot_attribute is not None:
            setattr(self, hot_attribute, value)
            return
        dict.__setitem__(self, key, value)

    def __delitem__(self, key: str) -> None:
        if not self.__contains__(key):
            raise KeyError(key)
        self._record_validation_modification()
        hot_attribute = _ACTIVITY_HOT_FIELD_ATTRIBUTES.get(key)
        if hot_attribute is not None:
            setattr(self, hot_attribute, _COLUMNAR_ACTIVITY_MISSING)
        if dict.__contains__(self, key):
            dict.__delitem__(self, key)
        self._deleted.add(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str) or key in self._deleted:
            return False
        hot_attribute = _ACTIVITY_HOT_FIELD_ATTRIBUTES.get(key)
        if (
            hot_attribute is not None
            and getattr(self, hot_attribute) is not _COLUMNAR_ACTIVITY_MISSING
        ):
            return True
        if dict.__contains__(self, key):
            return True
        return key in self._metadata()

    def __iter__(self) -> Iterator[str]:
        yielded = set()
        for key in dict.__iter__(self):
            if key not in self._deleted:
                yielded.add(key)
                yield key
        for key, hot_attribute in _ACTIVITY_HOT_FIELD_ATTRIBUTES.items():
            if (
                key not in yielded
                and key not in self._deleted
                and getattr(self, hot_attribute) is not _COLUMNAR_ACTIVITY_MISSING
            ):
                yielded.add(key)
                yield key
        for key in self._metadata():
            if key not in yielded and key not in self._deleted:
                yield key

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return MutableMapping.keys(self)

    def items(self):
        return MutableMapping.items(self)

    def values(self):
        return MutableMapping.values(self)

    def pop(self, key: str, *default: Any) -> Any:
        if len(default) > 1:
            raise TypeError(f"pop expected at most 2 arguments, got {len(default) + 1}")
        try:
            value = self[key]
        except KeyError:
            if default:
                return default[0]
            raise
        del self[key]
        return value

    def setdefault(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def clear(self) -> None:
        self._deleted.update(self)
        self._database = _COLUMNAR_ACTIVITY_MISSING
        self._code = _COLUMNAR_ACTIVITY_MISSING
        self._type = _COLUMNAR_ACTIVITY_MISSING
        dict.clear(self)

    def copy(self) -> dict[str, Any]:
        return dict(self.items())

    def _premise_materialize(self) -> dict[str, Any]:
        return self.copy()

    def _premise_scenario_cache_payload(self) -> dict[str, Any]:
        """Merge the overlay for read-only checkpoint diffing without cloning."""

        payload = {
            key: value for key, value in dict.items(self) if key not in self._deleted
        }
        for key, hot_attribute in _ACTIVITY_HOT_FIELD_ATTRIBUTES.items():
            value = getattr(self, hot_attribute)
            if (
                key not in self._deleted
                and value is not _COLUMNAR_ACTIVITY_MISSING
                and key not in payload
            ):
                payload[key] = value
        for key, value in self._metadata().items():
            if key not in self._deleted and key not in payload:
                payload[key] = value
        return payload

    def _premise_clone(self, memo: dict[int, Any]) -> "_ColumnarActivityMapping":
        """Clone resident overlays while sharing immutable columnar storage."""

        duplicate = type(self)(self._storage, self._activity_id, {})
        memo[id(self)] = duplicate
        duplicate._deleted = self._deleted.copy()
        duplicate._database = (
            _COLUMNAR_ACTIVITY_MISSING
            if self._database is _COLUMNAR_ACTIVITY_MISSING
            else copy.deepcopy(self._database, memo)
        )
        duplicate._code = (
            _COLUMNAR_ACTIVITY_MISSING
            if self._code is _COLUMNAR_ACTIVITY_MISSING
            else copy.deepcopy(self._code, memo)
        )
        duplicate._type = (
            _COLUMNAR_ACTIVITY_MISSING
            if self._type is _COLUMNAR_ACTIVITY_MISSING
            else copy.deepcopy(self._type, memo)
        )
        for key, value in dict.items(self):
            if key == "exchanges" and type(value) is list:
                cloned_value = memo.get(id(value))
                if cloned_value is None:
                    cloned_value = value.copy()
                    memo[id(value)] = cloned_value
                    for position, exchange in enumerate(value):
                        compact_clone = getattr(exchange, "_premise_clone", None)
                        cloned_value[position] = (
                            compact_clone(memo)
                            if compact_clone is not None
                            else copy.deepcopy(exchange, memo)
                        )
                        if isinstance(cloned_value[position], _ColumnarExchangeMapping):
                            cloned_value[position]._validation_owner = duplicate
            else:
                cloned_value = copy.deepcopy(value, memo)
            dict.__setitem__(
                duplicate,
                key if isinstance(key, str) else copy.deepcopy(key, memo),
                cloned_value,
            )
        return duplicate

    def __copy__(self) -> dict[str, Any]:
        return self.copy()

    def __deepcopy__(self, memo: dict[int, Any]) -> "_ColumnarActivityMapping":
        existing = memo.get(id(self))
        if existing is not None:
            return existing
        return self._premise_clone(memo)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Mapping):
            return False
        return self.copy() == dict(other.items())

    def __repr__(self) -> str:
        return repr(self.copy())

    def __reduce_ex__(self, protocol: int):
        del protocol
        return dict, (self.copy(),)


class _ColumnarExchangeTable:
    """Dense exchange-ID facade backed by compact checkpoint columns."""

    __slots__ = ("_storage", "_overrides", "_tombstones", "_length")

    def __init__(self, storage: _ColumnarExchangeStorage) -> None:
        self._storage = storage
        self._overrides: dict[int, Mapping[str, Any]] = {}
        self._tombstones: set[int] = set()
        self._length = storage.row_count

    def __len__(self) -> int:
        return self._length

    def __contains__(self, exchange_id: object) -> bool:
        if not isinstance(exchange_id, int) or exchange_id in self._tombstones:
            return False
        if exchange_id in self._overrides:
            return True
        try:
            self._storage.row_for_id(exchange_id)
            return True
        except KeyError:
            return False

    def __getitem__(self, exchange_id: ExchangeId) -> Mapping[str, Any]:
        if exchange_id not in self:
            raise KeyError(exchange_id)
        override = self._overrides.get(exchange_id)
        if override is not None:
            return override
        return _ColumnarExchangeMapping(
            self._storage, self._storage.row_for_id(exchange_id)
        )

    def __setitem__(self, exchange_id: ExchangeId, payload: Mapping[str, Any]) -> None:
        existed = exchange_id in self
        if not existed:
            largest = max(
                int(self._storage.exchange_ids[-1]) if self._storage.row_count else -1,
                max(self._overrides, default=-1),
            )
            if exchange_id != largest + 1:
                raise IndexError(f"Non-contiguous exchange id: {exchange_id}")
            self._length += 1
        self._tombstones.discard(exchange_id)
        self._overrides[exchange_id] = payload

    def __delitem__(self, exchange_id: ExchangeId) -> None:
        if exchange_id not in self:
            raise KeyError(exchange_id)
        self._overrides.pop(exchange_id, None)
        self._tombstones.add(exchange_id)
        self._length -= 1

    def shallow_copy(self) -> "_ColumnarExchangeTable":
        duplicate = type(self)(self._storage)
        duplicate._overrides = self._overrides.copy()
        duplicate._tombstones = self._tombstones.copy()
        duplicate._length = self._length
        return duplicate


class _DeltaExchangeTable:
    """Lazy final exchange IDs mapped onto immutable source rows."""

    _MISSING = -2
    _OVERRIDE = -1

    def __init__(self, storage: _ColumnarExchangeStorage, exchange_count: int) -> None:
        self._storage = storage
        self._rows = np.full(exchange_count, self._MISSING, dtype=np.int64)
        self._overrides: dict[int, Mapping[str, Any]] = {}
        self._length = 0

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[ExchangeId]:
        for exchange_id, row in enumerate(self._rows):
            if row != self._MISSING:
                yield exchange_id

    def __contains__(self, exchange_id: object) -> bool:
        return (
            isinstance(exchange_id, int)
            and 0 <= exchange_id < len(self._rows)
            and self._rows[exchange_id] != self._MISSING
        )

    def __getitem__(self, exchange_id: ExchangeId) -> Mapping[str, Any]:
        if exchange_id not in self:
            raise KeyError(exchange_id)
        row = int(self._rows[exchange_id])
        if row == self._OVERRIDE:
            return self._overrides[exchange_id]
        return _ColumnarExchangeMapping(self._storage, row)

    def __setitem__(self, exchange_id: ExchangeId, payload: Mapping[str, Any]) -> None:
        if exchange_id < 0:
            raise IndexError(f"Negative exchange id: {exchange_id}")
        if exchange_id >= len(self._rows):
            expanded = np.full(exchange_id + 1, self._MISSING, dtype=np.int64)
            expanded[: len(self._rows)] = self._rows
            self._rows = expanded
        if self._rows[exchange_id] == self._MISSING:
            self._length += 1
        self._rows[exchange_id] = self._OVERRIDE
        self._overrides[exchange_id] = payload

    def set_source_range(
        self, exchange_id: ExchangeId, source_row: int, length: int
    ) -> None:
        stop = exchange_id + length
        if exchange_id < 0 or stop > len(self._rows):
            raise InventoryStoreCorruptionError(
                "Invalid scenario delta exchange range."
            )
        if np.any(self._rows[exchange_id:stop] != self._MISSING):
            raise InventoryStoreCorruptionError("Overlapping scenario delta ranges.")
        self._rows[exchange_id:stop] = np.arange(
            source_row, source_row + length, dtype=np.int64
        )
        self._length += length

    def __delitem__(self, exchange_id: ExchangeId) -> None:
        if exchange_id not in self:
            raise KeyError(exchange_id)
        self._rows[exchange_id] = self._MISSING
        self._overrides.pop(exchange_id, None)
        self._length -= 1

    def shallow_copy(self) -> "_DeltaExchangeTable":
        duplicate = type(self)(self._storage, len(self._rows))
        duplicate._rows = self._rows.copy()
        duplicate._overrides = self._overrides.copy()
        duplicate._length = self._length
        return duplicate


def _product(payload: Mapping[str, Any]) -> Any:
    return payload.get("reference product", payload.get("product"))


def _hashable(value: Any) -> Any:
    try:
        hash(value)
    except TypeError:
        return _UNHASHABLE
    return value


def _activity_structural_fingerprint(
    payload: Mapping[str, Any],
    exchanges: Iterable[Mapping[str, Any]],
    *,
    canonical_order: bool = False,
) -> str:
    """Hash one activity independent of top-level mapping insertion order.

    Compact columnar round-trips reconstruct hot fields in a different order
    from imported dictionaries.  Ingestion canonicalizes those top-level maps
    once, allowing its common path to be serialized directly in C.  The slower
    fallback is used only after transaction mutations or by foreign stores.
    """

    if canonical_order:
        activity = payload
        exchange_rows = tuple(exchanges)
    else:
        activity = {key: payload[key] for key in sorted(payload)}
        exchange_rows = tuple(
            {key: exchange[key] for key in sorted(exchange)} for exchange in exchanges
        )
    encoded = pickle.dumps((activity, exchange_rows), protocol=pickle.HIGHEST_PROTOCOL)
    return hashlib.blake2b(encoded, digest_size=16).hexdigest()


def _canonicalize_top_level_mapping(payload: MutableMapping[str, Any]) -> None:
    """Put string-keyed inventory fields in deterministic insertion order."""

    items = sorted(payload.items())
    payload.clear()
    payload.update(items)


def _normalise_query(
    query: ActivityQuery | FilterExpression | Mapping[str, Any] | Callable | None,
) -> ActivityQuery | Callable | None:
    if query is None or isinstance(query, ActivityQuery) or callable(query):
        return query
    if isinstance(query, FilterExpression):
        return ActivityQuery((query,))
    if isinstance(query, Mapping):
        return ActivityQuery(
            tuple(
                FilterExpression(field_name, value)
                for field_name, value in query.items()
            )
        )
    raise TypeError(
        "query must be an ActivityQuery, FilterExpression, mapping, callable, or None"
    )


def _matches_expression(
    payload: Mapping[str, Any], expression: FilterExpression
) -> bool:
    actual = payload.get(expression.field)
    expected = expression.value
    operator = expression.operator.lower().replace("_", "-")

    if callable(expected):
        result = bool(expected(actual))
    elif operator in {"equals", "equal", "exact", "=="}:
        result = actual == expected
    elif operator in {"contains", "contain"}:
        if actual is None:
            result = False
        elif isinstance(expected, (tuple, list, set, frozenset)):
            result = any(item in actual for item in expected)
        else:
            result = expected in actual
    elif operator in {"startswith", "starts-with", "prefix"}:
        result = isinstance(actual, str) and actual.startswith(expected)
    elif operator in {"in", "either", "one-of"}:
        result = actual in expected
    elif operator == "all":
        result = actual is not None and all(item in actual for item in expected)
    elif operator in {"not-equals", "not-equal", "!=", "exclusion", "exclude"}:
        result = actual != expected
    elif operator in {"not-contains", "doesnt-contain", "does-not-contain"}:
        result = actual is None or expected not in actual
    elif operator in {"not-in", "neither"}:
        result = actual not in expected
    else:
        raise ValueError(f"Unsupported filter operator: {expression.operator!r}")
    return result


class InventoryStore(ABC):
    """Abstract ordered inventory graph."""

    backend_name: str

    @property
    @abstractmethod
    def generation(self) -> int: ...

    @property
    @abstractmethod
    def scenario_identity(self) -> Any: ...

    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def iter_activity_ids(self) -> Iterator[ActivityId]: ...

    @abstractmethod
    def iter_activities(
        self,
        query: (
            ActivityQuery | FilterExpression | Mapping[str, Any] | Callable | None
        ) = None,
    ) -> Iterator[ActivityRecord]: ...

    @abstractmethod
    def activity(self, activity_id: ActivityId) -> ActivityRecord: ...

    @abstractmethod
    def exchange(self, exchange_id: ExchangeId) -> ExchangeRecord: ...

    @abstractmethod
    def find(
        self, query: ActivityQuery | FilterExpression | Mapping[str, Any] | Callable
    ) -> tuple[ActivityRecord, ...]: ...

    @abstractmethod
    def find_one(
        self, query: ActivityQuery | FilterExpression | Mapping[str, Any] | Callable
    ) -> ActivityRecord: ...

    @abstractmethod
    def providers(
        self, provider_key: ProviderKey, consumer_location: str | None = None
    ) -> tuple[ActivityRecord, ...]: ...

    @abstractmethod
    def consumers(self, activity_id: ActivityId) -> tuple[ActivityId, ...]: ...

    @abstractmethod
    def contains(self, activity_key: ActivityKey) -> bool: ...

    @abstractmethod
    def transaction(self, label: str) -> "InventoryTransaction": ...

    @abstractmethod
    def fork(self, scenario_identity: Any = None) -> "InventoryStore": ...

    @abstractmethod
    def checkpoint(self, path: str | Path) -> Path: ...

    @abstractmethod
    def iter_materialized(
        self, restore_metadata: bool = True
    ) -> Iterator[dict[str, Any]]: ...

    def materialize(self, restore_metadata: bool = True) -> list[dict[str, Any]]:
        """Materialise the complete graph.

        This is intentionally explicit: for a production ecoinvent graph the
        returned Python dictionaries can require several gigabytes of memory.
        """

        return list(self.iter_materialized(restore_metadata=restore_metadata))

    @classmethod
    def open(cls, path: str | Path) -> "InventoryStore":
        """Open and validate a versioned inventory checkpoint."""

        return _open_checkpoint(Path(path))


class _InMemoryInventoryStore(InventoryStore):
    backend_name = "compact"
    eager_indexes = False
    eager_exchange_owners = False
    dense_exchange_table = True

    def __init__(
        self,
        database: Iterable[Mapping[str, Any]] = (),
        *,
        scenario_identity: Any = None,
        take_ownership: bool = False,
        scenario_cache_compatibility: bool = False,
        compute_fingerprints: bool = False,
    ) -> None:
        self._state = self._new_state()
        self._scenario_identity = scenario_identity
        self._lock = threading.RLock()
        self._active_transaction = False
        self._shared_state = False
        self._scenario_cache_compatibility = scenario_cache_compatibility
        self._ingest(
            database,
            take_ownership=take_ownership,
            scenario_cache_compatibility=scenario_cache_compatibility,
            compute_fingerprints=compute_fingerprints,
        )

    def _new_state(self) -> _StoreState:
        state = _StoreState()
        if self.dense_exchange_table:
            state.exchanges = _DenseExchangeTable()
        return state

    def _normalize_scenario_activity(
        self, payload: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        if self._scenario_cache_compatibility:
            from .utils import _normalize_scenario_cache_activity

            _normalize_scenario_cache_activity(payload)
        return payload

    def _normalize_scenario_exchange(
        self, payload: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        if self._scenario_cache_compatibility:
            from .utils import _normalize_scenario_cache_exchange

            _normalize_scenario_cache_exchange(payload)
        return payload

    def _activity_fingerprint(self, activity_id: ActivityId) -> str:
        fingerprint = self._state.activity_fingerprints.get(activity_id)
        if fingerprint is None:
            fingerprint = _activity_structural_fingerprint(
                self._state.activities[activity_id],
                (
                    self._state.exchanges[exchange_id]
                    for exchange_id in self._state.activity_exchanges[activity_id]
                    if exchange_id in self._state.exchanges
                ),
            )
            self._state.activity_fingerprints[activity_id] = fingerprint
        return fingerprint

    def _ensure_activity_fingerprints(self) -> dict[ActivityId, str]:
        for activity_id in self.iter_activity_ids():
            self._activity_fingerprint(activity_id)
        return self._state.activity_fingerprints

    def _invalidate_activity_fingerprint(self, activity_id: ActivityId) -> None:
        self._state.activity_fingerprints.pop(activity_id, None)

    @property
    def generation(self) -> int:
        return self._state.generation

    @property
    def scenario_identity(self) -> Any:
        return self._scenario_identity

    def _ingest(
        self,
        database: Iterable[Mapping[str, Any]],
        *,
        take_ownership: bool,
        scenario_cache_compatibility: bool,
        compute_fingerprints: bool,
    ) -> None:
        normalize_activity = None
        normalize_exchange = None
        if scenario_cache_compatibility:
            from .utils import (
                _normalize_scenario_cache_activity,
                _normalize_scenario_cache_exchange,
            )

            normalize_activity = _normalize_scenario_cache_activity
            normalize_exchange = _normalize_scenario_cache_exchange

        for dataset in database:
            payload_is_plain = type(dataset) is dict
            payload = (
                dataset
                if take_ownership and isinstance(dataset, dict)
                else copy.deepcopy(dict(dataset))
            )
            if normalize_activity is not None:
                normalize_activity(payload)
            exchanges = payload.pop("exchanges", [])
            if payload_is_plain:
                _canonicalize_top_level_mapping(payload)
            activity_id = self._state.next_activity_id
            self._state.next_activity_id += 1
            self._state.activities[activity_id] = payload
            self._state.activity_order.append(activity_id)
            exchanges_are_plain = self.dense_exchange_table
            if self.dense_exchange_table:
                exchange_start = self._state.next_exchange_id
                exchanges_are_plain = True
                for exchange in exchanges:
                    exchange_is_plain = type(exchange) is dict
                    exchanges_are_plain = exchanges_are_plain and exchange_is_plain
                    stored_exchange = (
                        exchange
                        if take_ownership and isinstance(exchange, Mapping)
                        else copy.deepcopy(dict(exchange))
                    )
                    if normalize_exchange is not None:
                        normalize_exchange(stored_exchange)
                    if exchange_is_plain:
                        _canonicalize_top_level_mapping(stored_exchange)
                    exchange_id = self._state.next_exchange_id
                    self._state.next_exchange_id += 1
                    self._state.exchanges[exchange_id] = stored_exchange
                self._state.activity_exchanges[activity_id] = range(
                    exchange_start, self._state.next_exchange_id
                )
            else:
                self._state.activity_exchanges[activity_id] = []
                for exchange in exchanges:
                    if normalize_exchange is not None:
                        exchange = normalize_exchange(
                            exchange
                            if take_ownership and isinstance(exchange, MutableMapping)
                            else copy.deepcopy(dict(exchange))
                        )
                    self._add_exchange_unchecked(
                        activity_id, exchange, take_ownership=take_ownership
                    )
            if compute_fingerprints:
                self._state.activity_fingerprints[activity_id] = (
                    _activity_structural_fingerprint(
                        payload,
                        (
                            self._state.exchanges[exchange_id]
                            for exchange_id in self._state.activity_exchanges[
                                activity_id
                            ]
                        ),
                        canonical_order=payload_is_plain and exchanges_are_plain,
                    )
                )
        if self.eager_indexes:
            self._rebuild_indexes()

    def _ensure_owned_state(self) -> None:
        if self._shared_state:
            self._state = self._copy_compact_state()
            self._shared_state = False

    def _copy_compact_state(self) -> _StoreState:
        """Copy graph structure while retaining immutable payload snapshots."""

        state = self._state
        duplicate = copy.copy(state)
        duplicate.activities = state.activities.copy()
        duplicate.activity_order = state.activity_order.copy()
        duplicate.exchanges = state.exchanges.shallow_copy()
        duplicate.exchange_owner = state.exchange_owner.copy()
        duplicate.activity_exchanges = state.activity_exchanges.copy()
        duplicate.activity_fingerprints = state.activity_fingerprints.copy()
        duplicate.transaction_log = state.transaction_log.copy()
        return duplicate

    def _transaction_snapshot(self) -> _StoreState:
        """Return a rollback snapshot without copying every compact payload.

        Transaction mutation methods replace activity and exchange dictionaries
        instead of changing existing payloads in place. A shallow structural
        snapshot is therefore sufficient and keeps a small sector transaction
        from duplicating the complete inventory graph.
        """
        return self._copy_compact_state()

    def _invalidate_indexes(self) -> None:
        self._state.field_index = {}
        self._state.activity_key_index = {}
        self._state.provider_index = {}
        self._state.consumer_index = {}
        self._state.indexes_ready = False

    def _ensure_indexes(self) -> None:
        if self._state.indexes_ready:
            return
        with self._lock:
            if not self._state.indexes_ready:
                self._rebuild_indexes()

    def _ensure_exchange_owners(self) -> None:
        if len(self._state.exchange_owner) == len(self._state.exchanges):
            return
        with self._lock:
            if len(self._state.exchange_owner) == len(self._state.exchanges):
                return
            self._state.exchange_owner = {
                exchange_id: activity_id
                for activity_id in self.iter_activity_ids()
                for exchange_id in self._state.activity_exchanges[activity_id]
                if exchange_id in self._state.exchanges
            }

    def __len__(self) -> int:
        return len(self._state.activities)

    def iter_activity_ids(self) -> Iterator[ActivityId]:
        for activity_id in self._state.activity_order:
            if activity_id in self._state.activities:
                yield activity_id

    def _activity_payload(self, activity_id: ActivityId) -> dict[str, Any]:
        try:
            payload = self._state.activities[activity_id]
        except KeyError as error:
            raise KeyError(f"Unknown activity id: {activity_id}") from error
        record = copy.deepcopy(payload)
        record["exchanges"] = [
            copy.deepcopy(self._state.exchanges[exchange_id])
            for exchange_id in self._state.activity_exchanges[activity_id]
            if exchange_id in self._state.exchanges
        ]
        return record

    def _iter_storage_activities(
        self,
    ) -> Iterator[tuple[ActivityId, Mapping[str, Any], tuple[ExchangeId, ...]]]:
        """Yield read-only activity metadata for package-native kernels.

        This private interface avoids materialising exchange dictionaries when
        a migrated sector only needs activity metadata and stable exchange IDs.
        Mutations still have to go through :class:`InventoryTransaction`.
        """

        for activity_id in self.iter_activity_ids():
            yield (
                activity_id,
                MappingProxyType(self._state.activities[activity_id]),
                tuple(self._state.activity_exchanges[activity_id]),
            )

    def _storage_exchange(self, exchange_id: ExchangeId) -> Mapping[str, Any]:
        """Return a read-only exchange mapping for a package-native kernel."""

        if exchange_id not in self._state.exchanges:
            raise KeyError(f"Unknown exchange id: {exchange_id}")
        return MappingProxyType(self._state.exchanges[exchange_id])

    def _iter_validation_storage(self):
        """Yield direct payloads to the package's strictly read-only validator.

        This is intentionally private: avoiding 1.6 million transient
        ``MappingProxyType`` wrappers materially reduces certification time,
        while all public store readers remain immutable.
        """

        for activity_id in self.iter_activity_ids():
            yield (
                activity_id,
                self._state.activities[activity_id],
                tuple(
                    (exchange_id, self._state.exchanges[exchange_id])
                    for exchange_id in self._state.activity_exchanges[activity_id]
                    if exchange_id in self._state.exchanges
                ),
            )

    def activity(self, activity_id: ActivityId) -> ActivityRecord:
        return ActivityRecord(
            activity_id,
            self._activity_payload(activity_id),
            self._state.activity_exchanges[activity_id],
        )

    def exchange(self, exchange_id: ExchangeId) -> ExchangeRecord:
        self._ensure_exchange_owners()
        try:
            payload = self._state.exchanges[exchange_id]
            owner = self._state.exchange_owner[exchange_id]
        except KeyError as error:
            raise KeyError(f"Unknown exchange id: {exchange_id}") from error
        return ExchangeRecord(exchange_id, owner, copy.deepcopy(payload))

    def _candidate_ids(self, query: ActivityQuery) -> Iterable[ActivityId]:
        self._ensure_indexes()
        candidate: set[ActivityId] | None = None
        for expression in query.filters:
            if expression.operator.lower() not in {"equals", "equal", "exact", "=="}:
                continue
            value = _hashable(expression.value)
            if value is _UNHASHABLE:
                continue
            indexed = set(
                self._state.field_index.get(expression.field, {}).get(value, ())
            )
            candidate = (
                indexed if candidate is None else candidate.intersection(indexed)
            )
        if candidate is None:
            return self.iter_activity_ids()
        return (
            activity_id
            for activity_id in self.iter_activity_ids()
            if activity_id in candidate
        )

    def iter_activities(
        self,
        query: (
            ActivityQuery | FilterExpression | Mapping[str, Any] | Callable | None
        ) = None,
    ) -> Iterator[ActivityRecord]:
        normalised = _normalise_query(query)
        if callable(normalised):
            for activity_id in self.iter_activity_ids():
                record = self.activity(activity_id)
                if normalised(record):
                    yield record
            return

        if normalised is None:
            for activity_id in self.iter_activity_ids():
                yield self.activity(activity_id)
            return

        for activity_id in self._candidate_ids(normalised):
            payload = self._state.activities[activity_id]
            if not all(
                _matches_expression(payload, expression)
                for expression in normalised.filters
            ):
                continue
            if any(
                _matches_expression(payload, expression)
                for expression in normalised.masks
            ):
                continue
            yield self.activity(activity_id)

    def find(
        self,
        query: ActivityQuery | FilterExpression | Mapping[str, Any] | Callable,
    ) -> tuple[ActivityRecord, ...]:
        return tuple(self.iter_activities(query))

    def find_one(
        self,
        query: ActivityQuery | FilterExpression | Mapping[str, Any] | Callable,
    ) -> ActivityRecord:
        records = self.find(query)
        if len(records) != 1:
            raise ValueError(f"Expected exactly one activity, found {len(records)}.")
        return records[0]

    def providers(
        self, provider_key: ProviderKey, consumer_location: str | None = None
    ) -> tuple[ActivityRecord, ...]:
        self._ensure_indexes()
        activity_ids = list(self._state.provider_index.get(provider_key, ()))
        if consumer_location is not None:
            location_rank = {
                consumer_location: 0,
                "RER": 1,
                "RoW": 2,
                "GLO": 3,
            }
            activity_ids.sort(
                key=lambda activity_id: (
                    location_rank.get(
                        self._state.activities[activity_id].get("location"), 4
                    ),
                    self._state.activity_order.index(activity_id),
                )
            )
        return tuple(self.activity(activity_id) for activity_id in activity_ids)

    def consumers(self, activity_id: ActivityId) -> tuple[ActivityId, ...]:
        if activity_id not in self._state.activities:
            raise KeyError(f"Unknown activity id: {activity_id}")
        self._ensure_indexes()
        return self._state.consumer_index.get(activity_id, ())

    def contains(self, activity_key: ActivityKey) -> bool:
        self._ensure_indexes()
        return bool(self._state.activity_key_index.get(activity_key))

    def transaction(self, label: str) -> "InventoryTransaction":
        return InventoryTransaction(self, label)

    def fork(self, scenario_identity: Any = None) -> "InventoryStore":
        child = object.__new__(type(self))
        child._state = copy.deepcopy(self._state)
        child._scenario_identity = scenario_identity
        child._lock = threading.RLock()
        child._active_transaction = False
        child._shared_state = False
        child._scenario_cache_compatibility = self._scenario_cache_compatibility
        return child

    def iter_materialized(
        self, restore_metadata: bool = True
    ) -> Iterator[dict[str, Any]]:
        del restore_metadata  # All metadata is retained losslessly in this backend.
        for activity_id in self.iter_activity_ids():
            yield self._activity_payload(activity_id)

    def checkpoint(self, path: str | Path) -> Path:
        return _write_checkpoint(self, Path(path))

    def _add_activity_unchecked(self, payload: Mapping[str, Any]) -> ActivityId:
        data = copy.deepcopy(dict(payload))
        self._normalize_scenario_activity(data)
        exchanges = data.pop("exchanges", [])
        activity_id = self._state.next_activity_id
        self._state.next_activity_id += 1
        self._state.activities[activity_id] = data
        self._state.activity_order.append(activity_id)
        self._state.activity_exchanges[activity_id] = []
        for exchange in exchanges:
            self._add_exchange_unchecked(activity_id, exchange)
        self._activity_fingerprint(activity_id)
        return activity_id

    def _add_exchange_unchecked(
        self,
        activity_id: ActivityId,
        payload: Mapping[str, Any],
        *,
        take_ownership: bool = False,
    ) -> ExchangeId:
        if activity_id not in self._state.activities:
            raise KeyError(f"Unknown activity id: {activity_id}")
        exchange_id = self._state.next_exchange_id
        self._state.next_exchange_id += 1
        self._state.exchanges[exchange_id] = (
            payload
            if take_ownership and type(payload) is dict
            else copy.deepcopy(dict(payload))
        )
        self._normalize_scenario_exchange(self._state.exchanges[exchange_id])
        if self.eager_exchange_owners:
            self._state.exchange_owner[exchange_id] = activity_id
        exchange_ids = self._state.activity_exchanges[activity_id]
        exchange_ids = list(exchange_ids)
        exchange_ids.append(exchange_id)
        self._state.activity_exchanges[activity_id] = exchange_ids
        self._invalidate_activity_fingerprint(activity_id)
        return exchange_id

    def _remove_exchange_unchecked(self, exchange_id: ExchangeId) -> None:
        if exchange_id not in self._state.exchanges:
            raise KeyError(f"Unknown exchange id: {exchange_id}")
        self._ensure_exchange_owners()
        owner = self._state.exchange_owner.pop(exchange_id)
        exchange_ids = list(self._state.activity_exchanges[owner])
        exchange_ids.remove(exchange_id)
        self._state.activity_exchanges[owner] = exchange_ids
        del self._state.exchanges[exchange_id]
        self._invalidate_activity_fingerprint(owner)

    def _rebuild_indexes(self) -> None:
        fields: dict[str, dict[Any, list[ActivityId]]] = defaultdict(
            lambda: defaultdict(list)
        )
        activity_keys: dict[ActivityKey, list[ActivityId]] = defaultdict(list)
        provider_keys: dict[ProviderKey, list[ActivityId]] = defaultdict(list)
        identifiers: dict[tuple[Any, Any], ActivityId] = {}

        for activity_id in self.iter_activity_ids():
            payload = self._state.activities[activity_id]
            for field_name, value in payload.items():
                value = _hashable(value)
                if value is not _UNHASHABLE:
                    fields[field_name][value].append(activity_id)
            name = payload.get("name")
            product = _product(payload)
            location = payload.get("location")
            unit = payload.get("unit")
            if None not in (name, product, location):
                activity_keys[ActivityKey(name, product, location)].append(activity_id)
            if None not in (name, product, unit):
                provider_keys[ProviderKey(name, product, unit)].append(activity_id)
            if payload.get("database") is not None and payload.get("code") is not None:
                identifiers[(payload["database"], payload["code"])] = activity_id

        consumers: dict[ActivityId, list[ActivityId]] = defaultdict(list)
        for consumer_id in self.iter_activity_ids():
            for exchange_id in self._state.activity_exchanges[consumer_id]:
                exchange = self._state.exchanges[exchange_id]
                provider_id = None
                exchange_input = exchange.get("input")
                if (
                    isinstance(exchange_input, (tuple, list))
                    and len(exchange_input) == 2
                ):
                    provider_id = identifiers.get(tuple(exchange_input))
                if provider_id is None and exchange.get("type") == "technosphere":
                    key = ActivityKey(
                        exchange.get("name"),
                        _product(exchange),
                        exchange.get("location"),
                    )
                    matches = activity_keys.get(key, ())
                    if len(matches) == 1:
                        provider_id = matches[0]
                if (
                    provider_id is not None
                    and consumer_id not in consumers[provider_id]
                ):
                    consumers[provider_id].append(consumer_id)

        self._state.field_index = {
            field_name: {value: tuple(ids) for value, ids in values.items()}
            for field_name, values in fields.items()
        }
        self._state.activity_key_index = {
            key: tuple(ids) for key, ids in activity_keys.items()
        }
        self._state.provider_index = {
            key: tuple(ids) for key, ids in provider_keys.items()
        }
        self._state.consumer_index = {key: tuple(ids) for key, ids in consumers.items()}
        self._state.indexes_ready = True


class LegacyInventoryStore(_InMemoryInventoryStore):
    """Dictionary-backed compatibility and differential-testing store."""

    backend_name = "legacy"
    eager_indexes = True
    eager_exchange_owners = True
    dense_exchange_table = False


class CompactInventoryStore(_InMemoryInventoryStore):
    """Copy-on-write inventory store with columnar checkpoints.

    The first implementation intentionally shares the battle-tested mutation
    engine with the legacy oracle.  Forks share state until the first write;
    checkpoints split activities and exchanges and dictionary-encode common
    string columns when PyArrow is available.
    """

    backend_name = "compact"
    eager_indexes = False
    eager_exchange_owners = False
    dense_exchange_table = True

    def fresh_columnar_view(
        self, scenario_identity: Any = None
    ) -> "CompactInventoryStore":
        """Create an isolated source view while sharing immutable columns.

        A multi-scenario build previously reopened and decoded the same source
        checkpoint for every scenario. A pristine columnar store can instead
        recreate only its lightweight activity overlays and graph indexes. The
        returned payload mappings are independent, while their Arrow/NumPy
        storage remains read-only and shared with this source template.
        """

        with self._lock:
            if self._active_transaction:
                raise InventoryStoreError(
                    "Cannot create a source view during an active transaction."
                )
            exchange_table = self._state.exchanges
            if not isinstance(exchange_table, _ColumnarExchangeTable):
                raise InventoryStoreError(
                    "Fresh source views require a columnar checkpoint store."
                )
            if exchange_table._overrides or exchange_table._tombstones:
                raise InventoryStoreError(
                    "Fresh source views require an unmodified exchange table."
                )

            storage = exchange_table._storage
            child = object.__new__(type(self))
            state = child._new_state()
            state.exchanges = _ColumnarExchangeTable(storage)
            for activity_id in self._state.activity_order:
                payload = self._state.activities.get(activity_id)
                if not isinstance(payload, _ColumnarActivityMapping):
                    raise InventoryStoreError(
                        "Fresh source views require columnar activity mappings."
                    )
                if payload._storage is not storage:
                    raise InventoryStoreError(
                        "Source activities do not share one columnar storage."
                    )
                state.activities[activity_id] = payload._premise_clone({})
                state.activity_order.append(activity_id)
                exchange_ids = self._state.activity_exchanges[activity_id]
                state.activity_exchanges[activity_id] = (
                    exchange_ids
                    if isinstance(exchange_ids, range)
                    else exchange_ids.copy()
                )
                fingerprint = self._state.activity_fingerprints.get(activity_id)
                if fingerprint is not None:
                    state.activity_fingerprints[activity_id] = fingerprint

            state.next_activity_id = self._state.next_activity_id
            state.next_exchange_id = self._state.next_exchange_id
            state.generation = self._state.generation
            child._state = state
            child._scenario_identity = scenario_identity
            child._lock = threading.RLock()
            child._active_transaction = False
            child._shared_state = False
            child._scenario_cache_compatibility = self._scenario_cache_compatibility
            child._shares_source_storage = True
            return child

    def _checkout_materialized(
        self, *, discard_shared_state: bool = False
    ) -> IndexedInventoryList:
        """Transfer graph ownership to the private transformation bridge.

        This is intentionally internal. It is safe only when the caller owns
        the final reference to the source graph (or explicitly discards all
        other references), because returned dictionaries are mutable.
        """

        with self._lock:
            if self._active_transaction:
                raise InventoryStoreError(
                    "Cannot check out a store during an active transaction."
                )
            if self._shared_state and not discard_shared_state:
                raise InventoryStoreError(
                    "Cannot check out shared compact state without discarding "
                    "every other reference."
                )
            state = self._state
            database = IndexedInventoryList(inventory_backend=self.backend_name)
            # Bypass IndexedInventoryList.append: no query index exists yet,
            # and building it incrementally would only add overhead here.
            append = list.append
            for activity_id in state.activity_order:
                if activity_id not in state.activities:
                    continue
                payload = state.activities[activity_id]
                payload["exchanges"] = [
                    state.exchanges[exchange_id]
                    for exchange_id in state.activity_exchanges[activity_id]
                    if exchange_id in state.exchanges
                ]
                for exchange in payload["exchanges"]:
                    if isinstance(exchange, _ColumnarExchangeMapping):
                        exchange._validation_owner = payload
                append(database, payload)
            self._state = self._new_state()
            self._shared_state = False
            return database

    def fork(self, scenario_identity: Any = None) -> "CompactInventoryStore":
        child = object.__new__(type(self))
        child._state = self._state
        child._scenario_identity = scenario_identity
        child._lock = threading.RLock()
        child._active_transaction = False
        child._shared_state = True
        child._scenario_cache_compatibility = self._scenario_cache_compatibility
        self._shared_state = True
        return child


_SCENARIO_MAPPING_MISSING = object()
_SCENARIO_MAPPING_RESIDENT_FIELDS = (
    "name",
    "reference product",
    "product",
    "location",
    "unit",
    "lhv",
)
_SCENARIO_MAPPING_RESIDENT_ATTRIBUTES = tuple(
    field_name.replace(" ", "_") for field_name in _SCENARIO_MAPPING_RESIDENT_FIELDS
)
_SCENARIO_MAPPING_RESIDENT_FIELD_BITS = {
    field_name: 1 << position
    for position, field_name in enumerate(_SCENARIO_MAPPING_RESIDENT_FIELDS)
}


class _CheckpointActivityResolver:
    """Load uncommon scenario-mapping metadata only when it is requested."""

    __slots__ = ("checkpoint", "_store", "_lock")

    def __init__(self, checkpoint: Path) -> None:
        self.checkpoint = checkpoint
        self._store: InventoryStore | None = None
        self._lock = threading.Lock()

    def __getstate__(self) -> dict[str, Path]:
        return {"checkpoint": self.checkpoint}

    def __setstate__(self, state: Mapping[str, Path]) -> None:
        self.checkpoint = state["checkpoint"]
        self._store = None
        self._lock = threading.Lock()

    def _payload(self, activity_id: ActivityId) -> Mapping[str, Any]:
        if self._store is None:
            with self._lock:
                if self._store is None:
                    self._store = InventoryStore.open(self.checkpoint)
        try:
            return self._store._state.activities[activity_id]
        except KeyError as error:
            raise KeyError(f"Unknown checkpoint activity id: {activity_id}") from error

    def value(self, activity_id: ActivityId, field_name: str) -> Any:
        payload = self._payload(activity_id)
        if field_name not in payload:
            raise KeyError(field_name)
        return copy.deepcopy(payload[field_name])

    def keys(self, activity_id: ActivityId) -> tuple[str, ...]:
        return tuple(self._payload(activity_id))

    def contains(self, activity_id: ActivityId, field_name: object) -> bool:
        return field_name in self._payload(activity_id)


class _ScenarioActivityReference(Mapping[str, Any]):
    """Small activity mapping backed by a versioned inventory checkpoint."""

    __slots__ = (
        "_resolver",
        "_activity_id",
        "_resident_mask",
        *_SCENARIO_MAPPING_RESIDENT_ATTRIBUTES,
    )

    def __init__(
        self,
        resolver: _CheckpointActivityResolver,
        activity_id: ActivityId,
        payload: Mapping[str, Any],
    ) -> None:
        self._resolver = resolver
        self._activity_id = activity_id
        resident_mask = 0
        for field_name in _SCENARIO_MAPPING_RESIDENT_FIELDS:
            if field_name in payload:
                resident_mask |= _SCENARIO_MAPPING_RESIDENT_FIELD_BITS[field_name]
                value = copy.deepcopy(payload[field_name])
            else:
                value = None
            setattr(
                self,
                field_name.replace(" ", "_"),
                value,
            )
        self._resident_mask = resident_mask

    def _resident_value(self, field_name: str) -> Any:
        field_bit = _SCENARIO_MAPPING_RESIDENT_FIELD_BITS.get(field_name)
        if field_bit is None or not self._resident_mask & field_bit:
            return _SCENARIO_MAPPING_MISSING
        return getattr(self, field_name.replace(" ", "_"))

    def __getitem__(self, field_name: str) -> Any:
        resident = self._resident_value(field_name)
        if resident is not _SCENARIO_MAPPING_MISSING:
            return copy.deepcopy(resident)
        return self._resolver.value(self._activity_id, field_name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._resolver.keys(self._activity_id))

    def __len__(self) -> int:
        return len(self._resolver.keys(self._activity_id))

    def __contains__(self, field_name: object) -> bool:
        if isinstance(field_name, str):
            resident = self._resident_value(field_name)
            if resident is not _SCENARIO_MAPPING_MISSING:
                return True
            if field_name in _SCENARIO_MAPPING_RESIDENT_FIELDS:
                return False
        return self._resolver.contains(self._activity_id, field_name)


def _compact_scenario_mapping(
    mapping: Mapping[str, Mapping[str, Iterable[Mapping[str, Any]]]],
    store: CompactInventoryStore,
    checkpoint: Path,
) -> dict[str, dict[str, list[Mapping[str, Any]]]]:
    """Replace store-owned mapping payloads with shared lazy references."""

    activity_ids = {
        id(payload): activity_id
        for activity_id, payload in store._state.activities.items()
    }
    resolver = _CheckpointActivityResolver(checkpoint)
    references: dict[ActivityId, _ScenarioActivityReference] = {}
    compacted: dict[str, dict[str, list[Mapping[str, Any]]]] = {}

    for sector, sector_mapping in mapping.items():
        compacted[sector] = {}
        for variable, activities in sector_mapping.items():
            compacted_activities: list[Mapping[str, Any]] = []
            for activity in activities:
                activity_id = activity_ids.get(id(activity))
                if activity_id is None:
                    compacted_activities.append(activity)
                    continue
                reference = references.get(activity_id)
                if reference is None:
                    reference = _ScenarioActivityReference(
                        resolver, activity_id, activity
                    )
                    references[activity_id] = reference
                compacted_activities.append(reference)
            compacted[sector][variable] = compacted_activities

    return compacted


def _hydrate_scenario_mapping(
    mapping: Mapping[str, Mapping[str, Iterable[Mapping[str, Any]]]],
    activities_by_id: Mapping[ActivityId, dict[str, Any]],
) -> dict[str, dict[str, list[Mapping[str, Any]]]]:
    """Rebind lazy mapping entries to an incremental update's working graph."""

    hydrated: dict[str, dict[str, list[Mapping[str, Any]]]] = {}
    for sector, sector_mapping in mapping.items():
        hydrated[sector] = {}
        for variable, activities in sector_mapping.items():
            hydrated[sector][variable] = [
                (
                    activities_by_id.get(activity._activity_id, activity)
                    if isinstance(activity, _ScenarioActivityReference)
                    else activity
                )
                for activity in activities
            ]
    return hydrated


class ReadOnlyInventoryStore(InventoryStore):
    """Read-only facade returned by ``NewDatabase.get_inventory_store``."""

    def __init__(self, store: InventoryStore) -> None:
        self._store = store
        self.backend_name = store.backend_name

    def __len__(self) -> int:
        return len(self._store)

    @property
    def generation(self) -> int:
        return self._store.generation

    @property
    def scenario_identity(self) -> Any:
        return self._store.scenario_identity

    def iter_activity_ids(self) -> Iterator[ActivityId]:
        return self._store.iter_activity_ids()

    def iter_activities(self, query=None) -> Iterator[ActivityRecord]:
        return self._store.iter_activities(query)

    def activity(self, activity_id: ActivityId) -> ActivityRecord:
        return self._store.activity(activity_id)

    def exchange(self, exchange_id: ExchangeId) -> ExchangeRecord:
        return self._store.exchange(exchange_id)

    def find(self, query) -> tuple[ActivityRecord, ...]:
        return self._store.find(query)

    def find_one(self, query) -> ActivityRecord:
        return self._store.find_one(query)

    def providers(self, provider_key, consumer_location=None):
        return self._store.providers(provider_key, consumer_location)

    def consumers(self, activity_id: ActivityId) -> tuple[ActivityId, ...]:
        return self._store.consumers(activity_id)

    def contains(self, activity_key: ActivityKey) -> bool:
        return self._store.contains(activity_key)

    def transaction(self, label: str) -> "InventoryTransaction":
        del label
        raise InventoryStoreReadOnlyError(
            "This inventory-store view is read-only; request writable=True."
        )

    def fork(self, scenario_identity: Any = None) -> InventoryStore:
        return self._store.fork(scenario_identity)

    def checkpoint(self, path: str | Path) -> Path:
        return self._store.checkpoint(path)

    def iter_materialized(self, restore_metadata: bool = True):
        return self._store.iter_materialized(restore_metadata)


class InventoryTransaction:
    """Atomic mutation scope for an in-memory store."""

    def __init__(self, store: _InMemoryInventoryStore, label: str) -> None:
        self.store = store
        self.label = str(label)
        self._snapshot: _StoreState | None = None
        self._entered = False

    def __enter__(self) -> "InventoryTransaction":
        self.store._lock.acquire()
        if self.store._active_transaction:
            self.store._lock.release()
            raise InventoryStoreError(
                "Nested inventory transactions are not supported."
            )
        self.store._active_transaction = True
        self.store._ensure_owned_state()
        self._snapshot = self.store._transaction_snapshot()
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        try:
            if exc_type is not None:
                if self._snapshot is not None:
                    self.store._state = self._snapshot
            else:
                self.store._state.generation += 1
                self.store._state.transaction_log.append(self.label)
                if self.store.eager_indexes:
                    self.store._rebuild_indexes()
        finally:
            self.store._active_transaction = False
            self._entered = False
            self.store._lock.release()
        return False

    def _require_entered(self) -> None:
        if not self._entered:
            raise InventoryStoreError(
                "Transaction mutations must be called inside a with block."
            )

    def add_activity(self, payload: Mapping[str, Any]) -> ActivityId:
        self._require_entered()
        activity_id = self.store._add_activity_unchecked(payload)
        self.store._invalidate_indexes()
        return activity_id

    def clone_activity(
        self,
        source_id: ActivityId,
        activity_updates: Mapping[str, Any] | None = None,
        exchange_updates: (
            Mapping[int, Mapping[str, Any]] | Iterable[Mapping[str, Any]] | None
        ) = None,
    ) -> ActivityId:
        self._require_entered()
        payload = self.store._activity_payload(source_id)
        payload.update(copy.deepcopy(dict(activity_updates or {})))
        exchanges = payload.get("exchanges", [])
        if isinstance(exchange_updates, Mapping):
            for position, updates in exchange_updates.items():
                index = int(position)
                if index < 0 or index >= len(exchanges):
                    raise IndexError(f"Unknown source exchange position: {position}")
                exchanges[index].update(copy.deepcopy(dict(updates)))
        elif exchange_updates is not None:
            updates_list = list(exchange_updates)
            if len(updates_list) != len(exchanges):
                raise ValueError(
                    "Positional exchange_updates must match the source exchange count."
                )
            for exchange, updates in zip(exchanges, updates_list):
                exchange.update(copy.deepcopy(dict(updates)))
        activity_id = self.store._add_activity_unchecked(payload)
        self.store._invalidate_indexes()
        return activity_id

    def patch_activity(
        self,
        activity_id: ActivityId,
        updates: Mapping[str, Any],
        delete_fields: Iterable[str] = (),
    ) -> None:
        self._require_entered()
        if activity_id not in self.store._state.activities:
            raise KeyError(f"Unknown activity id: {activity_id}")
        changes = copy.deepcopy(dict(updates))
        exchanges = changes.pop("exchanges", None)
        payload = copy.deepcopy(self.store._state.activities[activity_id])
        payload.update(changes)
        for field_name in delete_fields:
            payload.pop(field_name, None)
        self.store._normalize_scenario_activity(payload)
        self.store._state.activities[activity_id] = payload
        self.store._invalidate_activity_fingerprint(activity_id)
        if exchanges is not None:
            self.replace_exchanges(activity_id, exchanges)
        else:
            self.store._invalidate_indexes()

    def remove_activity(self, activity_id: ActivityId) -> None:
        self._require_entered()
        if activity_id not in self.store._state.activities:
            raise KeyError(f"Unknown activity id: {activity_id}")
        for exchange_id in list(self.store._state.activity_exchanges[activity_id]):
            self.store._remove_exchange_unchecked(exchange_id)
        del self.store._state.activity_exchanges[activity_id]
        del self.store._state.activities[activity_id]
        self.store._state.activity_fingerprints.pop(activity_id, None)
        self.store._invalidate_indexes()

    def add_exchange(
        self, activity_id: ActivityId, payload: Mapping[str, Any]
    ) -> ExchangeId:
        self._require_entered()
        exchange_id = self.store._add_exchange_unchecked(activity_id, payload)
        self.store._invalidate_indexes()
        return exchange_id

    def patch_exchange(
        self,
        exchange_id: ExchangeId,
        updates: Mapping[str, Any],
        delete_fields: Iterable[str] = (),
        *,
        activity_id: ActivityId | None = None,
    ) -> None:
        self._require_entered()
        if exchange_id not in self.store._state.exchanges:
            raise KeyError(f"Unknown exchange id: {exchange_id}")
        payload = copy.deepcopy(self.store._state.exchanges[exchange_id])
        payload.update(copy.deepcopy(dict(updates)))
        for field_name in delete_fields:
            payload.pop(field_name, None)
        self.store._normalize_scenario_exchange(payload)
        self.store._state.exchanges[exchange_id] = payload
        if activity_id is None:
            self.store._ensure_exchange_owners()
            activity_id = self.store._state.exchange_owner[exchange_id]
        elif exchange_id not in self.store._state.activity_exchanges.get(
            activity_id, ()
        ):
            raise ValueError(
                f"Exchange {exchange_id} does not belong to activity {activity_id}."
            )
        self.store._invalidate_activity_fingerprint(activity_id)
        self.store._invalidate_indexes()

    def remove_exchange(self, exchange_id: ExchangeId) -> None:
        self._require_entered()
        self.store._remove_exchange_unchecked(exchange_id)
        self.store._invalidate_indexes()

    def replace_exchanges(
        self, activity_id: ActivityId, exchanges: Iterable[Mapping[str, Any]]
    ) -> None:
        self._require_entered()
        if activity_id not in self.store._state.activities:
            raise KeyError(f"Unknown activity id: {activity_id}")
        for exchange_id in list(self.store._state.activity_exchanges[activity_id]):
            self.store._remove_exchange_unchecked(exchange_id)
        for exchange in exchanges:
            self.store._add_exchange_unchecked(activity_id, exchange)
        self.store._invalidate_indexes()


class InventoryStoreBuilder:
    """Incremental source-graph builder."""

    def __init__(self, backend: Literal["compact", "legacy"] = "compact") -> None:
        if backend not in {"compact", "legacy"}:
            raise ValueError("inventory backend must be 'compact' or 'legacy'")
        self.backend = backend
        self._store = create_inventory_store((), backend=backend)
        self._sealed = False

    def append(self, dataset: Mapping[str, Any]) -> None:
        if self._sealed:
            raise InventoryStoreError("Cannot append to a sealed builder.")
        self._store._add_activity_unchecked(dataset)

    def extend(self, datasets: Iterable[Mapping[str, Any]]) -> None:
        for dataset in datasets:
            self.append(dataset)

    def seal(self, scenario_identity: Any = None) -> InventoryStore:
        if self._sealed:
            raise InventoryStoreError(
                "InventoryStoreBuilder.seal() can only be called once."
            )
        self._sealed = True
        self._store._scenario_identity = scenario_identity
        if self._store.eager_indexes:
            self._store._rebuild_indexes()
        return self._store


def create_inventory_store(
    database: Iterable[Mapping[str, Any]],
    *,
    backend: Literal["compact", "legacy"] = "compact",
    scenario_identity: Any = None,
    take_ownership: bool = False,
    scenario_cache_compatibility: bool = False,
    compute_fingerprints: bool = False,
) -> InventoryStore:
    store_class = {
        "compact": CompactInventoryStore,
        "legacy": LegacyInventoryStore,
    }.get(backend)
    if store_class is None:
        raise ValueError("inventory_backend must be either 'compact' or 'legacy'.")
    return store_class(
        database,
        scenario_identity=scenario_identity,
        take_ownership=take_ownership,
        scenario_cache_compatibility=scenario_cache_compatibility,
        compute_fingerprints=compute_fingerprints,
    )


def get_scenario_inventory(scenario: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the private mutable working inventory for a sector wrapper.

    This bridge keeps the historical transformation implementations functional
    while preventing the mutable payload from being exposed as
    ``scenario["database"]``.  New transformation code should operate on the
    store contract directly.
    """

    if "_inventory_working_copy" in scenario:
        database = scenario["_inventory_working_copy"]
        if not isinstance(database, IndexedInventoryList):
            database = IndexedInventoryList(
                database,
                inventory_backend=scenario.get("_inventory_backend"),
            )
            scenario["_inventory_working_copy"] = database
        return database
    if "database" in scenario:  # Compatibility for standalone legacy callers.
        return scenario["database"]
    store = scenario.get("_inventory_store")
    if store is None and scenario.get("_inventory_checkpoint") is not None:
        store = InventoryStore.open(scenario["_inventory_checkpoint"])
        scenario["_inventory_store"] = store
    if store is None:
        raise InventoryStoreError(
            "Scenario has no inventory store or private working inventory."
        )
    if isinstance(store, CompactInventoryStore):
        working_copy = store._checkout_materialized()
        scenario.pop("_inventory_store", None)
    else:
        working_copy = IndexedInventoryList(
            store.materialize(restore_metadata=True),
            inventory_backend=store.backend_name,
        )
    scenario["_inventory_backend"] = store.backend_name
    scenario["_inventory_working_copy"] = working_copy
    return working_copy


def replace_scenario_inventory(
    scenario: dict[str, Any], database: Iterable[Mapping[str, Any]]
) -> None:
    """Commit a sector wrapper's resulting inventory without a public payload."""

    if "database" in scenario:
        scenario["database"] = database
        return
    store = scenario.get("_inventory_store")
    if store is None:
        current = scenario.get("_inventory_working_copy")
        inventory_backend = (
            getattr(database, "_inventory_backend", None)
            or getattr(current, "_inventory_backend", None)
            or scenario.get("_inventory_backend")
        )
        scenario["_inventory_backend"] = inventory_backend
        scenario["_inventory_working_copy"] = IndexedInventoryList(
            database,
            inventory_backend=inventory_backend,
        )
        return
    scenario["_inventory_store"] = create_inventory_store(
        database,
        backend=store.backend_name,
        scenario_identity=getattr(store, "scenario_identity", None),
    )
    scenario.pop("_inventory_working_copy", None)
    scenario.pop("_inventory_checkpoint", None)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_activity_fingerprints(
    path: Path, activity_ids: Iterable[ActivityId]
) -> dict[ActivityId, str]:
    try:
        fingerprints = pickle.loads((path / "activity-fingerprints.pkl").read_bytes())
    except Exception as error:
        raise InventoryStoreCorruptionError(
            "Invalid activity fingerprint checkpoint payload."
        ) from error
    expected_ids = {int(activity_id) for activity_id in activity_ids}
    if not isinstance(fingerprints, dict) or not set(fingerprints).issubset(
        expected_ids
    ):
        raise InventoryStoreCorruptionError(
            "Activity fingerprint IDs are not checkpoint activities."
        )
    if not all(
        isinstance(value, str) and len(value) == 32 for value in fingerprints.values()
    ):
        raise InventoryStoreCorruptionError(
            "Activity fingerprint checkpoint values are invalid."
        )
    return {int(key): value for key, value in fingerprints.items()}


def _write_table(path: Path, columns: Mapping[str, list[Any]]) -> None:
    if pa is None:
        with path.open("wb") as stream:
            pickle.dump(dict(columns), stream, protocol=pickle.HIGHEST_PROTOCOL)
        return
    arrays = {}
    for name, values in columns.items():
        array = pa.array(values)
        if pa.types.is_string(array.type):
            array = array.dictionary_encode()
        arrays[name] = array
    table = pa.table(arrays)
    with pa.OSFile(str(path), "wb") as sink:
        with pa_ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)


def _read_table(path: Path) -> dict[str, list[Any]]:
    if pa is None:
        with path.open("rb") as stream:
            return pickle.load(stream)
    for open_reader in (pa_ipc.open_file, pa_ipc.open_stream):
        try:
            with pa.memory_map(str(path), "r") as source:
                table = open_reader(source).read_all()
            return {name: table[name].to_pylist() for name in table.column_names}
        except (pa.ArrowInvalid, pa.ArrowIOError):
            continue
    # A checkpoint produced in a source-only environment uses the pickle
    # fallback despite retaining the stable bundle filenames.
    with path.open("rb") as stream:
        return pickle.load(stream)


def _iter_table_rows(path: Path) -> Iterator[dict[str, Any]]:
    """Yield Arrow rows batch-wise, with support for source-only fallbacks."""

    if pa is not None:
        try:
            with pa.memory_map(str(path), "r") as source:
                reader = pa_ipc.open_file(source)
                for batch_index in range(reader.num_record_batches):
                    batch = reader.get_batch(batch_index)
                    names = batch.schema.names
                    columns = [
                        batch.column(index).to_pylist() for index in range(len(names))
                    ]
                    for values in zip(*columns):
                        yield dict(zip(names, values))
            return
        except (pa.ArrowInvalid, pa.ArrowIOError):
            pass
        try:
            with pa.memory_map(str(path), "r") as source:
                reader = pa_ipc.open_stream(source)
                for batch in reader:
                    names = batch.schema.names
                    columns = [
                        batch.column(index).to_pylist() for index in range(len(names))
                    ]
                    for values in zip(*columns):
                        yield dict(zip(names, values))
            return
        except (pa.ArrowInvalid, pa.ArrowIOError):
            pass
    columns = _read_table(path)
    names = tuple(columns)
    for values in zip(*(columns[name] for name in names)):
        yield dict(zip(names, values))


def _exchange_from_arrow_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = {}
    for field_name in _EXCHANGE_STRING_FIELDS:
        value = row.get(field_name)
        if value is not None:
            payload[field_name] = value
    category_0 = row.get("categories__0")
    category_1 = row.get("categories__1")
    if category_0 is not None and category_1 is not None:
        payload["categories"] = (category_0, category_1)
    for field_name in _EXCHANGE_NUMERIC_FIELDS:
        kind = int(row.get(f"{field_name}__kind") or _NUMERIC_MISSING)
        if kind != _NUMERIC_MISSING:
            payload[field_name] = _decode_numeric_column(
                kind,
                row.get(f"{field_name}__float"),
                row.get(f"{field_name}__int"),
            )
    for field_name in _EXCHANGE_BOOLEAN_FIELDS:
        value = row.get(field_name)
        if value is not None:
            payload[field_name] = bool(value)
    return payload


def _write_checkpoint_payloads(
    store: _InMemoryInventoryStore,
    temporary: Path,
    *,
    batch_size: int = 65_536,
) -> tuple[dict[str, list[Any]], dict[str, list[Any]], list[str]]:
    """Serialize metadata and Arrow rows in one ordered graph traversal."""

    scenario_cache_compatibility = getattr(
        store, "_scenario_cache_compatibility", False
    )
    normalize_activity_metadata = None
    normalize_exchange_metadata = None
    exchange_field_is_restored = None
    if scenario_cache_compatibility:
        from .utils import (
            _normalize_scenario_cache_activity,
            _normalize_scenario_cache_exchange,
            _scenario_cache_exchange_field_is_restored,
        )

        normalize_activity_metadata = _normalize_scenario_cache_activity
        normalize_exchange_metadata = _normalize_scenario_cache_exchange
        exchange_field_is_restored = _scenario_cache_exchange_field_is_restored

    id_columns = ("exchange_id", "activity_id", "exchange_ordinal")
    category_columns = ("categories__0", "categories__1")
    numeric_columns = tuple(
        column
        for field_name in _EXCHANGE_NUMERIC_FIELDS
        for column in (
            f"{field_name}__kind",
            f"{field_name}__float",
            f"{field_name}__int",
        )
    )
    column_names = (
        *id_columns,
        *_EXCHANGE_STRING_FIELDS,
        *category_columns,
        *numeric_columns,
        *_EXCHANGE_BOOLEAN_FIELDS,
    )

    strings: list[str] = []
    seen_strings: set[str] = set()

    def collect_string(value: Any) -> None:
        if isinstance(value, str) and value not in seen_strings:
            strings.append(value)
            seen_strings.add(value)

    def append_payload(columns, payload) -> dict[str, Any]:
        append_columnar = getattr(payload, "_premise_append_checkpoint_payload", None)
        if append_columnar is not None:
            metadata = append_columnar(
                columns,
                collect_string if pa is None else None,
                scenario_cache_compatibility,
            )
            if scenario_cache_compatibility:
                return metadata
        else:
            for field_name in _EXCHANGE_STRING_FIELDS:
                value = payload.get(field_name)
                if (
                    scenario_cache_compatibility
                    and isinstance(value, str)
                    and value in {"", "None", "nan"}
                ):
                    value = None
                columns[field_name].append(value if isinstance(value, str) else None)
                if pa is None:
                    collect_string(value)
            categories = payload.get("categories")
            valid_categories = (
                isinstance(categories, tuple)
                and len(categories) == 2
                and all(isinstance(item, str) for item in categories)
            )
            columns["categories__0"].append(categories[0] if valid_categories else None)
            columns["categories__1"].append(categories[1] if valid_categories else None)
            if pa is None and valid_categories:
                collect_string(categories[0])
                collect_string(categories[1])
            numeric_kinds = {}
            for field_name in _EXCHANGE_NUMERIC_FIELDS:
                kind, float_value, int_value = _numeric_column_parts(
                    payload.get(field_name, _UNHASHABLE)
                )
                if (
                    scenario_cache_compatibility
                    and float_value is not None
                    and float_value != float_value
                ):
                    kind, float_value, int_value = _NUMERIC_MISSING, None, None
                numeric_kinds[field_name] = kind
                columns[f"{field_name}__kind"].append(kind)
                columns[f"{field_name}__float"].append(float_value)
                columns[f"{field_name}__int"].append(int_value)
            for field_name in _EXCHANGE_BOOLEAN_FIELDS:
                value = payload.get(field_name)
                columns[field_name].append(value if type(value) is bool else None)
            metadata = _exchange_sidecar_metadata(payload, numeric_kinds=numeric_kinds)

        if scenario_cache_compatibility:
            if (
                exchange_field_is_restored is None
                or normalize_exchange_metadata is None
            ):
                raise InventoryStoreError(
                    "Scenario-compatible exchange metadata hooks are unavailable."
                )
            if any(
                not exchange_field_is_restored(field_name, value)
                for field_name, value in metadata.items()
            ):
                metadata = dict(metadata)
                normalize_exchange_metadata(metadata)
        return metadata

    schema = None
    if pa is not None:
        dictionary_type = pa.dictionary(pa.int32(), pa.string())
        fields = [
            pa.field("exchange_id", pa.int64()),
            pa.field("activity_id", pa.int64()),
            pa.field("exchange_ordinal", pa.int64()),
        ]
        fields.extend(
            pa.field(field_name, dictionary_type)
            for field_name in (*_EXCHANGE_STRING_FIELDS, *category_columns)
        )
        fields.extend(
            pa.field(
                column,
                (
                    pa.int8()
                    if column.endswith("__kind")
                    else pa.int64() if column.endswith("__int") else pa.float64()
                ),
            )
            for column in numeric_columns
        )
        fields.extend(
            pa.field(field_name, pa.bool_()) for field_name in _EXCHANGE_BOOLEAN_FIELDS
        )
        schema = pa.schema(fields)
    columns = {name: [] for name in column_names}

    def flush(writer) -> None:
        if not columns["exchange_id"]:
            return
        if schema is None:
            raise InventoryStoreError("Arrow exchange schema is unavailable.")
        arrays = []
        for arrow_field in schema:
            values = columns[arrow_field.name]
            if pa.types.is_dictionary(arrow_field.type):
                array = pa.array(values, type=pa.string()).dictionary_encode()
                for value in array.dictionary.to_pylist():
                    collect_string(value)
                arrays.append(array)
            else:
                arrays.append(pa.array(values, type=arrow_field.type))
        writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
        for values in columns.values():
            values.clear()

    offsets = {"kind": [], "id": [], "offset": [], "length": []}
    activities: dict[str, list[Any]] = defaultdict(list)

    def write_sidecar_record(sidecar, kind: str, record_id: int, payload: Any) -> None:
        encoded = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        offset = sidecar.tell()
        sidecar.write(encoded)
        offsets["kind"].append(kind)
        offsets["id"].append(record_id)
        offsets["offset"].append(offset)
        offsets["length"].append(len(encoded))

    def serialize(activity_sidecar, exchange_sidecar, writer=None) -> None:
        exchange_cursor = 0
        for ordinal, activity_id in enumerate(store.iter_activity_ids()):
            payload = store._state.activities[activity_id]
            exchange_ids = store._state.activity_exchanges[activity_id]
            exchange_metadata = []
            for exchange_ordinal, exchange_id in enumerate(exchange_ids):
                exchange_payload = store._state.exchanges[exchange_id]
                columns["exchange_id"].append(exchange_id)
                columns["activity_id"].append(activity_id)
                columns["exchange_ordinal"].append(exchange_ordinal)
                metadata = append_payload(columns, exchange_payload)
                if metadata:
                    exchange_metadata.append((exchange_ordinal, metadata))
                if writer is not None and len(columns["exchange_id"]) >= batch_size:
                    flush(writer)

            materialize = getattr(payload, "_premise_materialize", None)
            activity_metadata = (
                materialize() if materialize is not None else dict(payload)
            )
            activity_metadata = _activity_sidecar_metadata(activity_metadata)
            if normalize_activity_metadata is not None:
                normalize_activity_metadata(activity_metadata)
            write_sidecar_record(
                activity_sidecar,
                "activity",
                activity_id,
                activity_metadata,
            )
            if exchange_metadata:
                write_sidecar_record(
                    exchange_sidecar,
                    "exchange-metadata",
                    activity_id,
                    exchange_metadata,
                )
            activities["activity_id"].append(activity_id)
            activities["source_ordinal"].append(ordinal)
            exchange_count = len(exchange_ids)
            activities["exchange_start"].append(exchange_cursor)
            activities["exchange_count"].append(exchange_count)
            exchange_cursor += exchange_count
            for field_name in _ACTIVITY_COMMON_FIELDS:
                value = payload.get(field_name)
                activities[field_name].append(value if isinstance(value, str) else None)
                collect_string(value)

        if writer is not None:
            flush(writer)

    activity_metadata_path = temporary / ".activity-metadata.bin"
    exchange_metadata_path = temporary / ".exchange-metadata.bin"
    with (
        activity_metadata_path.open("wb") as activity_sidecar,
        exchange_metadata_path.open("wb") as exchange_sidecar,
    ):
        if pa is None:
            serialize(activity_sidecar, exchange_sidecar)
        else:
            with pa.OSFile(str(temporary / "exchanges.arrow"), "wb") as sink:
                # IPC streams permit independent dictionaries per bounded batch,
                # avoiding a second graph traversal to build a global dictionary.
                with pa_ipc.new_stream(sink, schema) as writer:
                    serialize(activity_sidecar, exchange_sidecar, writer)

    activity_metadata_size = activity_metadata_path.stat().st_size
    for position, kind in enumerate(offsets["kind"]):
        if kind == "exchange-metadata":
            offsets["offset"][position] += activity_metadata_size
    with (temporary / "metadata.bin").open("wb") as combined:
        for metadata_path in (activity_metadata_path, exchange_metadata_path):
            with metadata_path.open("rb") as source:
                shutil.copyfileobj(source, combined, length=1024 * 1024)
            metadata_path.unlink()

    if pa is None:
        _write_table(temporary / "exchanges.arrow", columns)

    return offsets, dict(activities), strings


def _scenario_delta_base_storage(
    store: _InMemoryInventoryStore,
) -> _ColumnarExchangeStorage | None:
    """Return the single immutable source storage eligible for a delta."""

    exchange_table = store._state.exchanges
    if isinstance(exchange_table, _DenseExchangeTable):
        if len(exchange_table._rows) != len(exchange_table):
            return None
        payloads = exchange_table._rows
    else:
        payloads = (
            exchange_table[exchange_id]
            for exchange_id in range(store._state.next_exchange_id)
            if exchange_id in exchange_table
        )
        if store._state.next_exchange_id != len(exchange_table):
            return None
    storage = None
    for payload in payloads:
        if payload is None:
            return None
        if not isinstance(payload, _ColumnarExchangeMapping):
            continue
        if storage is None:
            storage = payload._storage
        elif payload._storage is not storage:
            return None
    return storage


def _scenario_delta_exchange_changes(
    payload: _ColumnarExchangeMapping,
) -> tuple[tuple[str, bool, Any], ...]:
    """Return sparse changes including legacy compatibility deletions."""

    from .utils import _scenario_cache_exchange_field_is_restored

    storage = payload._storage
    row = payload._row
    changes: list[tuple[str, bool, Any]] = []
    changed_fields = set()
    for field_name, value in payload._iter_changes():
        changed_fields.add(field_name)
        deleted = value is _COLUMNAR_DELETED or not (
            value is not _COLUMNAR_DELETED
            and _scenario_cache_exchange_field_is_restored(field_name, value)
        )
        changes.append(
            (
                field_name,
                deleted,
                None if deleted else copy.deepcopy(value),
            )
        )

    invalid_common_fields = int(storage._scenario_cache_invalid_common_masks[row])
    if invalid_common_fields:
        for position, field_name in enumerate(_SCENARIO_CACHE_COMMON_FIELDS):
            if (
                invalid_common_fields & (1 << position)
                and field_name not in changed_fields
            ):
                changes.append((field_name, True, None))

    for field_name in storage.scenario_cache_deleted_fields(row):
        if field_name not in changed_fields:
            changes.append((field_name, True, None))
    return tuple(changes)


def _scenario_delta_values_equal(left: Any, right: Any) -> bool:
    """Compare metadata values without changing their represented types."""

    try:
        result = left == right
    except Exception:
        return False
    if isinstance(result, (bool, np.bool_)):
        return bool(result)
    if isinstance(result, np.ndarray):
        try:
            return bool(np.all(result))
        except (TypeError, ValueError):
            return False
    return False


def _write_scenario_delta_checkpoint_v1(
    store: _InMemoryInventoryStore,
    path: Path,
    storage: _ColumnarExchangeStorage,
) -> Path:
    """Persist a scenario as ordered source-row ranges and sparse overlays."""

    from .utils import (
        _normalize_scenario_cache_activity,
        _normalize_scenario_cache_exchange,
    )

    path = path.expanduser().resolve()
    if path == Path(path.anchor):
        raise ValueError("Refusing to use a filesystem root as a checkpoint path.")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{path.name}.tmp-", dir=path.parent))
    backup: Path | None = None
    delta_stream = None
    try:
        delta_path = temporary / "scenario-delta.pkl"
        delta_stream = delta_path.open("wb")
        pickle.dump(
            ("scenario-delta-stream-v1", len(store)),
            delta_stream,
            protocol=pickle.HIGHEST_PROTOCOL,
        )
        compatible_source_rows = storage.scenario_cache_compatible_rows()
        exchange_table = store._state.exchanges
        dense_exchange_rows = (
            exchange_table._rows
            if isinstance(exchange_table, _DenseExchangeTable)
            else None
        )
        for activity_id in store.iter_activity_ids():
            payload = store._state.activities[activity_id]
            materialize = getattr(
                payload,
                "_premise_scenario_cache_payload",
                getattr(payload, "_premise_materialize", None),
            )
            activity_payload = (
                materialize() if materialize is not None else dict(payload)
            )
            _normalize_scenario_cache_activity(activity_payload)
            if (
                isinstance(payload, _ColumnarActivityMapping)
                and payload._storage is storage
            ):
                source_activity_id = payload._activity_id
                base_activity_payload = storage.scenario_cache_base_activity_payload(
                    source_activity_id
                )
                _normalize_scenario_cache_activity(base_activity_payload)
                activity_updates = {
                    field_name: copy.deepcopy(value)
                    for field_name, value in activity_payload.items()
                    if field_name not in base_activity_payload
                    or not _scenario_delta_values_equal(
                        value, base_activity_payload[field_name]
                    )
                }
                activity_deletions = tuple(
                    field_name
                    for field_name in base_activity_payload
                    if field_name not in activity_payload
                )
                activity_descriptor = (
                    "p",
                    activity_id,
                    source_activity_id,
                    activity_updates,
                    activity_deletions,
                )
            else:
                activity_descriptor = ("v", activity_id, activity_payload)

            segments = []
            run_exchange_id = None
            run_row = None
            run_length = 0

            def flush_run() -> None:
                nonlocal run_exchange_id, run_row, run_length
                if run_length:
                    segments.append(("r", run_exchange_id, run_row, run_length))
                    run_exchange_id = None
                    run_row = None
                    run_length = 0

            for exchange_id in store._state.activity_exchanges[activity_id]:
                exchange = (
                    dense_exchange_rows[exchange_id]
                    if dense_exchange_rows is not None
                    else exchange_table[exchange_id]
                )
                if exchange is None:
                    raise KeyError(exchange_id)
                if (
                    isinstance(exchange, _ColumnarExchangeMapping)
                    and exchange._storage is storage
                ):
                    row = exchange._row
                    if exchange._changes is None and compatible_source_rows[row]:
                        if (
                            run_length
                            and exchange_id == run_exchange_id + run_length
                            and row == run_row + run_length
                        ):
                            run_length += 1
                        else:
                            flush_run()
                            run_exchange_id = exchange_id
                            run_row = row
                            run_length = 1
                        continue
                    changes = _scenario_delta_exchange_changes(exchange)
                    if not changes:
                        if (
                            run_length
                            and exchange_id == run_exchange_id + run_length
                            and row == run_row + run_length
                        ):
                            run_length += 1
                        else:
                            flush_run()
                            run_exchange_id = exchange_id
                            run_row = row
                            run_length = 1
                        continue
                    flush_run()
                    segments.append(("p", exchange_id, row, changes))
                    continue

                flush_run()
                materialize_exchange = getattr(exchange, "_premise_materialize", None)
                exchange_payload = (
                    materialize_exchange()
                    if materialize_exchange is not None
                    else dict(exchange)
                )
                _normalize_scenario_cache_exchange(exchange_payload)
                segments.append(("v", exchange_id, exchange_payload))
            flush_run()
            pickle.dump(
                (*activity_descriptor, segments),
                delta_stream,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        delta_stream.close()
        delta_stream = None
        (temporary / "activity-fingerprints.pkl").write_bytes(
            pickle.dumps(
                dict(store._state.activity_fingerprints),
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        )

        try:
            from . import __version__ as premise_version

            premise_version_text = ".".join(map(str, premise_version))
        except (ImportError, TypeError):  # pragma: no cover - defensive fallback
            premise_version_text = "unknown"

        base_checkpoint = storage.checkpoint.resolve()
        manifest = {
            "schema_version": STORE_SCHEMA_VERSION,
            "premise_version": premise_version_text,
            "backend": store.backend_name,
            "scenario_identity": store.scenario_identity,
            "generation": store.generation,
            "activity_count": len(store),
            "exchange_count": len(store._state.exchanges),
            "row_counts": {
                "activities": len(store),
                "exchanges": len(store._state.exchanges),
            },
            "columnar_format": "scenario-delta-v1",
            "metadata_layout": "scenario-delta-v1",
            "base_checkpoint": os.path.relpath(base_checkpoint, path.parent),
            "base_checksum_fingerprint": _sha256(base_checkpoint / "checksums.json"),
        }
        validation_certificate = getattr(store, "_validation_certificate_payload", None)
        if validation_certificate is not None:
            manifest["validation_certificate"] = validation_certificate
        provenance = getattr(store, "_provenance_payload", None)
        if provenance is not None:
            manifest["provenance"] = provenance
        try:
            manifest_text = json.dumps(manifest, sort_keys=True, indent=2)
        except TypeError:
            manifest["scenario_identity"] = repr(store.scenario_identity)
            manifest_text = json.dumps(manifest, sort_keys=True, indent=2)
        (temporary / "manifest.json").write_text(manifest_text, encoding="utf-8")
        checksums = {
            name: _sha256(temporary / name)
            for name in (
                "manifest.json",
                "scenario-delta.pkl",
                "activity-fingerprints.pkl",
            )
        }
        (temporary / "checksums.json").write_text(
            json.dumps(checksums, sort_keys=True, indent=2), encoding="utf-8"
        )

        if path.exists():
            backup = path.with_name(f".{path.name}.old-{os.getpid()}")
            if backup.exists():
                if backup.is_dir():
                    shutil.rmtree(backup)
                else:
                    backup.unlink()
            os.replace(path, backup)
        os.replace(temporary, path)
        if backup is not None:
            if backup.is_dir():
                shutil.rmtree(backup)
            else:
                backup.unlink()
        return path
    except Exception:
        if delta_stream is not None:
            delta_stream.close()
        if temporary.exists():
            shutil.rmtree(temporary)
        if backup is not None and backup.exists() and not path.exists():
            os.replace(backup, path)
        raise


def _iter_scenario_delta_records_v2(
    store: _InMemoryInventoryStore,
    storage: _ColumnarExchangeStorage,
) -> Iterator[tuple[tuple[Any, ...], list[tuple[Any, ...]]]]:
    """Yield V2 activity descriptors and ordered exchange segments."""

    from .utils import (
        _normalize_scenario_cache_activity,
        _normalize_scenario_cache_exchange,
    )

    compatible_source_rows = storage.scenario_cache_compatible_rows()
    exchange_table = store._state.exchanges
    dense_exchange_rows = (
        exchange_table._rows
        if isinstance(exchange_table, _DenseExchangeTable)
        else None
    )
    for activity_id in store.iter_activity_ids():
        payload = store._state.activities[activity_id]
        materialize = getattr(
            payload,
            "_premise_scenario_cache_payload",
            getattr(payload, "_premise_materialize", None),
        )
        activity_payload = materialize() if materialize is not None else dict(payload)
        activity_fields = frozenset(activity_payload)
        _normalize_scenario_cache_activity(activity_payload)
        removed_activity_fields = activity_fields.difference(activity_payload)
        if removed_activity_fields:
            if isinstance(payload, _ColumnarActivityMapping):
                payload._deleted.update(removed_activity_fields)
            else:
                for field_name in removed_activity_fields:
                    payload.pop(field_name, None)
        if (
            isinstance(payload, _ColumnarActivityMapping)
            and payload._storage is storage
        ):
            source_activity_id = payload._activity_id
            base_activity_payload = storage.scenario_cache_base_activity_payload(
                source_activity_id
            )
            _normalize_scenario_cache_activity(base_activity_payload)
            activity_updates = {
                field_name: copy.deepcopy(value)
                for field_name, value in activity_payload.items()
                if field_name not in base_activity_payload
                or not _scenario_delta_values_equal(
                    value, base_activity_payload[field_name]
                )
            }
            activity_deletions = tuple(
                field_name
                for field_name in base_activity_payload
                if field_name not in activity_payload
            )
            activity_descriptor = (
                "p",
                activity_id,
                source_activity_id,
                (activity_updates, activity_deletions),
            )
        else:
            activity_descriptor = ("v", activity_id, None, activity_payload)

        segments: list[tuple[Any, ...]] = []
        run_exchange_id = None
        run_row = None
        run_length = 0

        def flush_run() -> None:
            nonlocal run_exchange_id, run_row, run_length
            if run_length:
                segments.append(("r", run_exchange_id, run_row, run_length, None))
                run_exchange_id = None
                run_row = None
                run_length = 0

        for exchange_id in store._state.activity_exchanges[activity_id]:
            exchange = (
                dense_exchange_rows[exchange_id]
                if dense_exchange_rows is not None
                else exchange_table[exchange_id]
            )
            if exchange is None:
                raise KeyError(exchange_id)
            if (
                isinstance(exchange, _ColumnarExchangeMapping)
                and exchange._storage is storage
            ):
                row = exchange._row
                if exchange._changes is None and compatible_source_rows[row]:
                    if (
                        run_length
                        and exchange_id == run_exchange_id + run_length
                        and row == run_row + run_length
                    ):
                        run_length += 1
                    else:
                        flush_run()
                        run_exchange_id = exchange_id
                        run_row = row
                        run_length = 1
                    continue
                changes = _scenario_delta_exchange_changes(exchange)
                if not changes:
                    if (
                        run_length
                        and exchange_id == run_exchange_id + run_length
                        and row == run_row + run_length
                    ):
                        run_length += 1
                    else:
                        flush_run()
                        run_exchange_id = exchange_id
                        run_row = row
                        run_length = 1
                    continue
                for field_name, deleted, _value in changes:
                    if deleted:
                        exchange._set_change(field_name, _COLUMNAR_DELETED)
                flush_run()
                segments.append(("p", exchange_id, row, None, changes))
                continue

            flush_run()
            materialize_exchange = getattr(exchange, "_premise_materialize", None)
            exchange_payload = (
                materialize_exchange()
                if materialize_exchange is not None
                else dict(exchange)
            )
            exchange_fields = frozenset(exchange_payload)
            _normalize_scenario_cache_exchange(exchange_payload)
            for field_name in exchange_fields.difference(exchange_payload):
                try:
                    del exchange[field_name]
                except KeyError:
                    pass
            segments.append(("v", exchange_id, None, None, exchange_payload))
        flush_run()
        yield activity_descriptor, segments


def _write_scenario_delta_v2_tables(
    store: _InMemoryInventoryStore,
    temporary: Path,
    storage: _ColumnarExchangeStorage,
    *,
    batch_size: int = 8_192,
) -> None:
    """Write bounded Arrow activity and segment streams in one traversal."""

    activity_columns = {
        "activity_id": [],
        "record_kind": [],
        "source_activity_id": [],
        "payload": [],
    }
    segment_columns = {
        "activity_ordinal": [],
        "segment_ordinal": [],
        "segment_kind": [],
        "exchange_id": [],
        "source_row": [],
        "range_length": [],
        "payload": [],
    }
    activity_schema = None
    segment_schema = None
    if pa is not None:
        activity_schema = pa.schema(
            [
                pa.field("activity_id", pa.int64(), nullable=False),
                pa.field("record_kind", pa.string(), nullable=False),
                pa.field("source_activity_id", pa.int64()),
                pa.field("payload", pa.binary(), nullable=False),
            ]
        )
        segment_schema = pa.schema(
            [
                pa.field("activity_ordinal", pa.int64(), nullable=False),
                pa.field("segment_ordinal", pa.int64(), nullable=False),
                pa.field("segment_kind", pa.string(), nullable=False),
                pa.field("exchange_id", pa.int64(), nullable=False),
                pa.field("source_row", pa.int64()),
                pa.field("range_length", pa.int64()),
                pa.field("payload", pa.binary()),
            ]
        )

    def flush(columns, writer, schema) -> None:
        first_column = next(iter(columns.values()))
        if not first_column:
            return
        arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
        writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
        for values in columns.values():
            values.clear()

    def serialize(activity_writer=None, segment_writer=None) -> None:
        for activity_ordinal, (activity, segments) in enumerate(
            _iter_scenario_delta_records_v2(store, storage)
        ):
            record_kind, activity_id, source_activity_id, payload = activity
            activity_columns["activity_id"].append(activity_id)
            activity_columns["record_kind"].append(record_kind)
            activity_columns["source_activity_id"].append(source_activity_id)
            activity_columns["payload"].append(
                pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
            )
            if (
                activity_writer is not None
                and len(activity_columns["activity_id"]) >= batch_size
            ):
                flush(activity_columns, activity_writer, activity_schema)

            for segment_ordinal, segment in enumerate(segments):
                kind, exchange_id, source_row, range_length, segment_payload = segment
                segment_columns["activity_ordinal"].append(activity_ordinal)
                segment_columns["segment_ordinal"].append(segment_ordinal)
                segment_columns["segment_kind"].append(kind)
                segment_columns["exchange_id"].append(exchange_id)
                segment_columns["source_row"].append(source_row)
                segment_columns["range_length"].append(range_length)
                segment_columns["payload"].append(
                    pickle.dumps(segment_payload, protocol=pickle.HIGHEST_PROTOCOL)
                    if segment_payload is not None
                    else None
                )
                if (
                    segment_writer is not None
                    and len(segment_columns["exchange_id"]) >= batch_size
                ):
                    flush(segment_columns, segment_writer, segment_schema)

        if activity_writer is not None:
            flush(activity_columns, activity_writer, activity_schema)
            flush(segment_columns, segment_writer, segment_schema)

    if pa is None:
        serialize()
        _write_table(temporary / "scenario-activities.arrow", activity_columns)
        _write_table(temporary / "scenario-segments.arrow", segment_columns)
        return

    with (
        pa.OSFile(str(temporary / "scenario-activities.arrow"), "wb") as activity_sink,
        pa.OSFile(str(temporary / "scenario-segments.arrow"), "wb") as segment_sink,
        pa_ipc.new_stream(activity_sink, activity_schema) as activity_writer,
        pa_ipc.new_stream(segment_sink, segment_schema) as segment_writer,
    ):
        serialize(activity_writer, segment_writer)


def _write_scenario_delta_checkpoint(
    store: _InMemoryInventoryStore,
    path: Path,
    storage: _ColumnarExchangeStorage,
) -> Path:
    """Persist a scenario-delta-v2 bundle with typed Arrow structure."""

    path = path.expanduser().resolve()
    if path == Path(path.anchor):
        raise ValueError("Refusing to use a filesystem root as a checkpoint path.")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{path.name}.tmp-", dir=path.parent))
    backup: Path | None = None
    try:
        _write_scenario_delta_v2_tables(store, temporary, storage)
        (temporary / "activity-fingerprints.pkl").write_bytes(
            pickle.dumps(
                dict(store._state.activity_fingerprints),
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        )

        try:
            from . import __version__ as premise_version

            premise_version_text = ".".join(map(str, premise_version))
        except (ImportError, TypeError):  # pragma: no cover
            premise_version_text = "unknown"

        base_checkpoint = storage.checkpoint.resolve()
        manifest = {
            "schema_version": STORE_SCHEMA_VERSION,
            "premise_version": premise_version_text,
            "backend": store.backend_name,
            "scenario_identity": store.scenario_identity,
            "generation": store.generation,
            "activity_count": len(store),
            "exchange_count": len(store._state.exchanges),
            "row_counts": {
                "activities": len(store),
                "exchanges": len(store._state.exchanges),
            },
            "columnar_format": "scenario-delta-v2",
            "metadata_layout": "scenario-delta-v2",
            "base_checkpoint": os.path.relpath(base_checkpoint, path.parent),
            "base_checksum_fingerprint": _sha256(base_checkpoint / "checksums.json"),
        }
        validation_certificate = getattr(store, "_validation_certificate_payload", None)
        if validation_certificate is not None:
            manifest["validation_certificate"] = validation_certificate
        provenance = getattr(store, "_provenance_payload", None)
        if provenance is not None:
            manifest["provenance"] = provenance
        try:
            manifest_text = json.dumps(manifest, sort_keys=True, indent=2)
        except TypeError:
            manifest["scenario_identity"] = repr(store.scenario_identity)
            manifest_text = json.dumps(manifest, sort_keys=True, indent=2)
        (temporary / "manifest.json").write_text(manifest_text, encoding="utf-8")
        checksums = {
            name: _sha256(temporary / name)
            for name in (
                "manifest.json",
                "scenario-activities.arrow",
                "scenario-segments.arrow",
                "activity-fingerprints.pkl",
            )
        }
        (temporary / "checksums.json").write_text(
            json.dumps(checksums, sort_keys=True, indent=2), encoding="utf-8"
        )

        if path.exists():
            backup = path.with_name(f".{path.name}.old-{os.getpid()}")
            if backup.exists():
                if backup.is_dir():
                    shutil.rmtree(backup)
                else:
                    backup.unlink()
            os.replace(path, backup)
        os.replace(temporary, path)
        if backup is not None:
            if backup.is_dir():
                shutil.rmtree(backup)
            else:
                backup.unlink()
        return path
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        if backup is not None and backup.exists() and not path.exists():
            os.replace(backup, path)
        raise


def _write_checkpoint(store: _InMemoryInventoryStore, path: Path) -> Path:
    if getattr(store, "_scenario_cache_compatibility", False):
        storage = _scenario_delta_base_storage(store)
        if storage is not None:
            return _write_scenario_delta_checkpoint(store, path, storage)

    path = path.expanduser().resolve()
    if path == Path(path.anchor):
        raise ValueError("Refusing to use a filesystem root as a checkpoint path.")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{path.name}.tmp-", dir=path.parent))
    backup: Path | None = None
    try:
        offsets, activities, strings = _write_checkpoint_payloads(store, temporary)

        (temporary / "activity-fingerprints.pkl").write_bytes(
            pickle.dumps(
                dict(store._state.activity_fingerprints),
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        )

        _write_table(temporary / "strings.arrow", {"value": strings})
        _write_table(temporary / "activities.arrow", dict(activities))
        _write_table(temporary / "metadata_offsets.arrow", offsets)

        try:
            from . import __version__ as premise_version

            premise_version_text = ".".join(map(str, premise_version))
        except (ImportError, TypeError):  # pragma: no cover - defensive fallback
            premise_version_text = "unknown"

        manifest = {
            "schema_version": STORE_SCHEMA_VERSION,
            "premise_version": premise_version_text,
            "backend": store.backend_name,
            "scenario_identity": store.scenario_identity,
            "generation": store.generation,
            "activity_count": len(store),
            "exchange_count": len(store._state.exchanges),
            "row_counts": {
                "activities": len(store),
                "exchanges": len(store._state.exchanges),
            },
            "source_fingerprint": _sha256(temporary / "metadata.bin"),
            "inventory_fingerprints": [],
            "uncertainty_settings": {},
            "columnar_format": "arrow-ipc" if pa is not None else "pickle-fallback",
            "metadata_layout": "split-activity-exchange-v5",
        }
        validation_certificate = getattr(store, "_validation_certificate_payload", None)
        if validation_certificate is not None:
            manifest["validation_certificate"] = validation_certificate
        provenance = getattr(store, "_provenance_payload", None)
        if provenance is not None:
            manifest["provenance"] = provenance
        try:
            manifest_text = json.dumps(manifest, sort_keys=True, indent=2)
        except TypeError:
            manifest["scenario_identity"] = repr(store.scenario_identity)
            manifest_text = json.dumps(manifest, sort_keys=True, indent=2)
        (temporary / "manifest.json").write_text(manifest_text, encoding="utf-8")

        checksummed = [
            "manifest.json",
            "strings.arrow",
            "activities.arrow",
            "exchanges.arrow",
            "metadata.bin",
            "metadata_offsets.arrow",
            "activity-fingerprints.pkl",
        ]
        checksums = {name: _sha256(temporary / name) for name in checksummed}
        (temporary / "checksums.json").write_text(
            json.dumps(checksums, sort_keys=True, indent=2), encoding="utf-8"
        )

        if path.exists():
            backup = path.with_name(f".{path.name}.old-{os.getpid()}")
            if backup.exists():
                if backup.is_dir():
                    shutil.rmtree(backup)
                else:
                    backup.unlink()
            os.replace(path, backup)
        os.replace(temporary, path)
        if backup is not None:
            if backup.is_dir():
                shutil.rmtree(backup)
            else:
                backup.unlink()
        return path
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        if backup is not None and backup.exists() and not path.exists():
            os.replace(backup, path)
        raise


def _iter_scenario_delta_activity_records(
    path: Path, expected_count: int
) -> Iterator[tuple[Any, ...]]:
    """Yield scenario records without loading the full delta into memory."""

    try:
        with (path / "scenario-delta.pkl").open("rb") as stream:
            header = pickle.load(stream)
            if isinstance(header, dict):
                records = header.get("activities")
                if not isinstance(records, list) or len(records) != expected_count:
                    raise InventoryStoreCorruptionError(
                        "Invalid scenario delta activities."
                    )
                yield from records
                return
            if (
                not isinstance(header, tuple)
                or len(header) != 2
                or header[0] != "scenario-delta-stream-v1"
                or header[1] != expected_count
            ):
                raise InventoryStoreCorruptionError(
                    "Invalid scenario delta stream header."
                )
            for _ in range(expected_count):
                record = pickle.load(stream)
                if not isinstance(record, tuple):
                    raise InventoryStoreCorruptionError(
                        "Invalid scenario delta activity record."
                    )
                yield record
            try:
                pickle.load(stream)
            except EOFError:
                return
            raise InventoryStoreCorruptionError(
                "Scenario delta stream contains trailing records."
            )
    except InventoryStoreCorruptionError:
        raise
    except Exception as error:
        raise InventoryStoreCorruptionError(
            "Invalid scenario delta payload."
        ) from error


def _unpickle_scenario_delta_blob(blob: Any, label: str) -> Any:
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        raise InventoryStoreCorruptionError(
            f"Invalid scenario delta V2 {label} payload."
        )
    try:
        return pickle.loads(bytes(blob))
    except Exception as error:
        raise InventoryStoreCorruptionError(
            f"Invalid scenario delta V2 {label} payload."
        ) from error


def _iter_scenario_delta_v2_activity_records(
    path: Path, expected_count: int
) -> Iterator[tuple[Any, ...]]:
    """Stream V2 Arrow rows and reconstruct the unchanged V1 record protocol."""

    activity_rows = iter(_iter_table_rows(path / "scenario-activities.arrow"))
    segment_rows = iter(_iter_table_rows(path / "scenario-segments.arrow"))
    pending_segment = next(segment_rows, None)
    activity_count = 0
    for activity_ordinal, activity_row in enumerate(activity_rows):
        activity_count += 1
        try:
            kind = activity_row["record_kind"]
            activity_id = int(activity_row["activity_id"])
            source_activity_id = activity_row.get("source_activity_id")
            payload = _unpickle_scenario_delta_blob(
                activity_row.get("payload"), "activity"
            )
        except InventoryStoreCorruptionError:
            raise
        except Exception as error:
            raise InventoryStoreCorruptionError(
                "Invalid scenario delta V2 activity row."
            ) from error

        segments = []
        expected_segment_ordinal = 0
        while pending_segment is not None:
            try:
                segment_activity_ordinal = int(pending_segment["activity_ordinal"])
            except Exception as error:
                raise InventoryStoreCorruptionError(
                    "Invalid scenario delta V2 segment row."
                ) from error
            if segment_activity_ordinal < activity_ordinal:
                raise InventoryStoreCorruptionError(
                    "Scenario delta V2 segments are not in activity order."
                )
            if segment_activity_ordinal > activity_ordinal:
                break
            try:
                segment_ordinal = int(pending_segment["segment_ordinal"])
                if segment_ordinal != expected_segment_ordinal:
                    raise ValueError("invalid segment ordinal")
                segment_kind = pending_segment["segment_kind"]
                exchange_id = int(pending_segment["exchange_id"])
                source_row = pending_segment.get("source_row")
                range_length = pending_segment.get("range_length")
                segment_payload = pending_segment.get("payload")
                if segment_kind == "r":
                    if segment_payload is not None:
                        raise ValueError("range segment has a payload")
                    segments.append(
                        ("r", exchange_id, int(source_row), int(range_length))
                    )
                elif segment_kind == "p":
                    changes = _unpickle_scenario_delta_blob(
                        segment_payload, "sparse exchange"
                    )
                    segments.append(("p", exchange_id, int(source_row), changes))
                elif segment_kind == "v":
                    exchange_payload = _unpickle_scenario_delta_blob(
                        segment_payload, "exchange value"
                    )
                    segments.append(("v", exchange_id, exchange_payload))
                else:
                    raise ValueError("unknown segment kind")
            except InventoryStoreCorruptionError:
                raise
            except Exception as error:
                raise InventoryStoreCorruptionError(
                    "Invalid scenario delta V2 segment row."
                ) from error
            expected_segment_ordinal += 1
            pending_segment = next(segment_rows, None)

        if kind == "p":
            if source_activity_id is None:
                raise InventoryStoreCorruptionError(
                    "Sparse scenario delta V2 activity has no source activity."
                )
            try:
                updates, deletions = payload
            except Exception as error:
                raise InventoryStoreCorruptionError(
                    "Invalid sparse scenario delta V2 activity payload."
                ) from error
            yield (
                "p",
                activity_id,
                int(source_activity_id),
                updates,
                deletions,
                segments,
            )
        elif kind == "v":
            if source_activity_id is not None:
                raise InventoryStoreCorruptionError(
                    "Value scenario delta V2 activity has a source activity."
                )
            yield "v", activity_id, payload, segments
        else:
            raise InventoryStoreCorruptionError(
                f"Unknown scenario delta V2 activity kind: {kind!r}."
            )

    if activity_count != expected_count:
        raise InventoryStoreCorruptionError(
            "Scenario delta V2 activity count does not match its manifest."
        )
    if pending_segment is not None:
        raise InventoryStoreCorruptionError(
            "Scenario delta V2 has segments without an activity."
        )


def _open_scenario_delta_checkpoint(
    path: Path, manifest: Mapping[str, Any]
) -> InventoryStore:
    """Open a sparse scenario overlay on its immutable source checkpoint."""

    from .utils import _normalize_scenario_cache_activity

    base_reference = manifest.get("base_checkpoint")
    if not isinstance(base_reference, str):
        raise InventoryStoreCorruptionError(
            "Scenario delta checkpoint has no valid base reference."
        )
    base_checkpoint = (path.parent / base_reference).resolve()
    base_checksums = base_checkpoint / "checksums.json"
    if not base_checksums.is_file() or _sha256(base_checksums) != manifest.get(
        "base_checksum_fingerprint"
    ):
        raise InventoryStoreCorruptionError(
            "Scenario delta base checkpoint is missing or has changed."
        )
    base_store = InventoryStore.open(base_checkpoint)
    base_table = base_store._state.exchanges
    if not isinstance(base_table, _ColumnarExchangeTable):
        raise InventoryStoreCorruptionError(
            "Scenario delta base is not a compact columnar checkpoint."
        )
    storage = base_table._storage

    activity_count = int(manifest.get("activity_count", 0))
    if manifest.get("columnar_format") == "scenario-delta-v2":
        activity_records = _iter_scenario_delta_v2_activity_records(
            path, activity_count
        )
    else:
        activity_records = _iter_scenario_delta_activity_records(path, activity_count)

    store = object.__new__(CompactInventoryStore)
    store._state = store._new_state()
    exchange_count = int(manifest.get("exchange_count", 0))
    exchange_table = _DeltaExchangeTable(storage, exchange_count)
    store._state.exchanges = exchange_table

    for record in activity_records:
        try:
            kind = record[0]
            if kind == "p":
                (
                    _,
                    activity_id,
                    source_activity_id,
                    updates,
                    deletions,
                    segments,
                ) = record
                activity_payload = storage.base_activity_payload(
                    int(source_activity_id)
                )
                _normalize_scenario_cache_activity(activity_payload)
                for field_name in deletions:
                    activity_payload.pop(field_name, None)
                activity_payload.update(updates)
            elif kind == "v":
                _, activity_id, payload, segments = record
                activity_payload = dict(payload)
            else:
                raise ValueError(f"unknown activity record kind: {kind!r}")
            activity_id = int(activity_id)
        except Exception as error:
            raise InventoryStoreCorruptionError(
                "Invalid scenario delta activity record."
            ) from error
        activity_payload.pop("exchanges", None)
        if activity_id in store._state.activities:
            raise InventoryStoreCorruptionError(
                f"Duplicate scenario delta activity id: {activity_id}."
            )
        store._state.activities[activity_id] = activity_payload
        store._state.activity_order.append(activity_id)
        activity_exchange_ids: list[int] = []

        for segment in segments:
            try:
                kind = segment[0]
                if kind == "r":
                    _, exchange_id, source_row, length = segment
                    exchange_id = int(exchange_id)
                    source_row = int(source_row)
                    length = int(length)
                    if length <= 0 or not 0 <= source_row < storage.row_count:
                        raise ValueError("invalid source range")
                    if source_row + length > storage.row_count:
                        raise ValueError("source range exceeds base")
                    exchange_table.set_source_range(exchange_id, source_row, length)
                    activity_exchange_ids.extend(
                        range(exchange_id, exchange_id + length)
                    )
                elif kind == "p":
                    _, exchange_id, source_row, changes = segment
                    exchange_id = int(exchange_id)
                    source_row = int(source_row)
                    exchange = _ColumnarExchangeMapping(storage, source_row)
                    for field_name, deleted, value in changes:
                        exchange._set_change(
                            field_name,
                            _COLUMNAR_DELETED if deleted else value,
                        )
                    exchange_table[exchange_id] = exchange
                    activity_exchange_ids.append(exchange_id)
                elif kind == "v":
                    _, exchange_id, exchange_payload = segment
                    exchange_id = int(exchange_id)
                    exchange_table[exchange_id] = compact_exchange_payload(
                        exchange_payload
                    )
                    activity_exchange_ids.append(exchange_id)
                else:
                    raise ValueError(f"unknown segment kind: {kind!r}")
            except Exception as error:
                if isinstance(error, InventoryStoreCorruptionError):
                    raise
                raise InventoryStoreCorruptionError(
                    f"Invalid scenario delta exchange segment: {segment!r}."
                ) from error

        if activity_exchange_ids and activity_exchange_ids == list(
            range(
                activity_exchange_ids[0],
                activity_exchange_ids[0] + len(activity_exchange_ids),
            )
        ):
            store._state.activity_exchanges[activity_id] = range(
                activity_exchange_ids[0],
                activity_exchange_ids[0] + len(activity_exchange_ids),
            )
        else:
            store._state.activity_exchanges[activity_id] = activity_exchange_ids

    if len(store._state.activities) != manifest.get("activity_count"):
        raise InventoryStoreCorruptionError(
            "Scenario delta activity count does not match its manifest."
        )
    if len(exchange_table) != exchange_count:
        raise InventoryStoreCorruptionError(
            "Scenario delta exchange count does not match its manifest."
        )
    store._state.next_activity_id = max(store._state.activity_order, default=-1) + 1
    store._state.next_exchange_id = len(exchange_table._rows)
    store._state.generation = int(manifest.get("generation", 0))
    store._state.activity_fingerprints = _read_activity_fingerprints(
        path, store._state.activity_order
    )
    store._scenario_identity = manifest.get("scenario_identity")
    store._lock = threading.RLock()
    store._active_transaction = False
    store._shared_state = False
    store._scenario_cache_compatibility = False
    store._shares_source_storage = True
    store._validation_certificate_payload = manifest.get("validation_certificate")
    store._provenance_payload = manifest.get("provenance")
    return store


def _open_checkpoint(path: Path) -> InventoryStore:
    path = path.expanduser().resolve()
    manifest_path = path / "manifest.json"
    checksums_path = path / "checksums.json"
    if not manifest_path.exists() or not checksums_path.exists():
        raise InventoryStoreCorruptionError(
            f"Inventory checkpoint at {path} is incomplete."
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        checksums = json.loads(checksums_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InventoryStoreCorruptionError(
            f"Cannot read inventory checkpoint at {path}."
        ) from error
    if manifest.get("schema_version") != STORE_SCHEMA_VERSION:
        raise InventoryStoreVersionError(
            "Unsupported inventory-store schema "
            f"{manifest.get('schema_version')!r}; expected {STORE_SCHEMA_VERSION}."
        )
    for name, expected in checksums.items():
        candidate = path / name
        if not candidate.is_file() or _sha256(candidate) != expected:
            raise InventoryStoreCorruptionError(
                f"Checksum validation failed for checkpoint file {name!r}."
            )

    if manifest.get("columnar_format") in {
        "scenario-delta-v1",
        "scenario-delta-v2",
    }:
        return _open_scenario_delta_checkpoint(path, manifest)

    offsets = _read_table(path / "metadata_offsets.arrow")
    activity_offsets: dict[int, tuple[int, int]] = {}
    exchange_metadata_offsets: dict[int, tuple[int, int]] = {}
    for kind, record_id, offset, length in zip(
        offsets["kind"], offsets["id"], offsets["offset"], offsets["length"]
    ):
        target = {
            "activity": activity_offsets,
            "exchange-metadata": exchange_metadata_offsets,
        }.get(kind)
        if target is None:
            raise InventoryStoreCorruptionError(
                f"Invalid metadata record kind {kind!r} for schema "
                f"{STORE_SCHEMA_VERSION}."
            )
        target[int(record_id)] = (int(offset), int(length))

    activities = _read_table(path / "activities.arrow")
    backend = manifest.get("backend", "compact")
    if backend not in {"compact", "legacy"}:
        raise InventoryStoreVersionError(
            f"Unsupported inventory-store backend {backend!r}."
        )
    if (
        backend == "compact"
        and pa is not None
        and manifest.get("columnar_format") == "arrow-ipc"
    ):
        activity_ids = np.asarray(activities.get("activity_id", ()), dtype=np.int64)
        exchange_starts = np.asarray(
            activities.get("exchange_start", ()), dtype=np.int64
        )
        exchange_counts = np.asarray(
            activities.get("exchange_count", ()), dtype=np.int64
        )
        if len(activity_ids) != manifest.get("activity_count"):
            raise InventoryStoreCorruptionError(
                "Activity row count does not match the checkpoint manifest."
            )
        storage = _ColumnarExchangeStorage(
            path,
            exchange_count=int(manifest.get("exchange_count", 0)),
            activity_ids=activity_ids,
            exchange_starts=exchange_starts,
            exchange_counts=exchange_counts,
            activity_common_columns={
                field_name: activities[field_name]
                for field_name in _ACTIVITY_COMMON_FIELDS
            },
            activity_offsets=activity_offsets,
            exchange_metadata_offsets=exchange_metadata_offsets,
        )
        store = object.__new__(CompactInventoryStore)
        store._state = store._new_state()
        store._state.exchanges = _ColumnarExchangeTable(storage)
        for position, activity_id_value in enumerate(activity_ids):
            activity_id = int(activity_id_value)
            common = {
                field_name: activities[field_name][position]
                for field_name in _ACTIVITY_COMMON_FIELDS
                if activities[field_name][position] is not None
            }
            payload = _ColumnarActivityMapping(storage, activity_id, common)
            store._state.activities[activity_id] = payload
            store._state.activity_order.append(activity_id)
            store._state.activity_exchanges[activity_id] = (
                storage.activity_exchange_ids(position)
            )
        store._state.next_activity_id = max(store._state.activity_order, default=-1) + 1
        store._state.next_exchange_id = (
            int(storage.exchange_ids.max()) + 1 if storage.row_count else 0
        )
        store._state.generation = int(manifest.get("generation", 0))
        store._state.activity_fingerprints = _read_activity_fingerprints(
            path, store._state.activity_order
        )
        store._scenario_identity = manifest.get("scenario_identity")
        store._lock = threading.RLock()
        store._active_transaction = False
        store._shared_state = False
        store._scenario_cache_compatibility = False
        store._validation_certificate_payload = manifest.get("validation_certificate")
        store._provenance_payload = manifest.get("provenance")
        return store

    activity_payloads: dict[int, dict[str, Any]] = {}
    exchange_metadata_by_id: dict[int, dict[int, dict[str, Any]]] = {}
    with (path / "metadata.bin").open("rb") as sidecar:
        for kind, records, target in (
            ("activity", activity_offsets, activity_payloads),
            (
                "exchange-metadata",
                exchange_metadata_offsets,
                exchange_metadata_by_id,
            ),
        ):
            for record_id, (offset, length) in records.items():
                sidecar.seek(offset)
                encoded = sidecar.read(length)
                try:
                    payload = pickle.loads(encoded)
                except Exception as error:
                    raise InventoryStoreCorruptionError(
                        f"Invalid {kind} payload for activity {record_id}."
                    ) from error
                target[record_id] = payload

    database: list[dict[str, Any]] = []
    datasets_by_id = {}
    for position, activity_id in enumerate(activities.get("activity_id", [])):
        try:
            payload = activity_payloads[activity_id]
        except KeyError as error:
            raise InventoryStoreCorruptionError(
                f"Missing activity metadata for activity {activity_id}."
            ) from error
        for field_name in _ACTIVITY_COMMON_FIELDS:
            value = activities[field_name][position]
            if value is not None and field_name not in payload:
                payload[field_name] = value
        payload["exchanges"] = []
        database.append(payload)
        datasets_by_id[activity_id] = payload
        exchange_metadata_by_id[activity_id] = dict(
            exchange_metadata_by_id.get(activity_id, ())
        )

    exchange_count = 0
    for row in _iter_table_rows(path / "exchanges.arrow"):
        activity_id = row["activity_id"]
        try:
            dataset = datasets_by_id[activity_id]
        except KeyError as error:
            raise InventoryStoreCorruptionError(
                f"Exchange references missing activity {activity_id}."
            ) from error
        exchange_ordinal = int(row["exchange_ordinal"])
        if exchange_ordinal != len(dataset["exchanges"]):
            raise InventoryStoreCorruptionError(
                f"Non-contiguous exchange order for activity {activity_id}."
            )
        exchange = _exchange_from_arrow_row(row)
        exchange.update(exchange_metadata_by_id[activity_id].get(exchange_ordinal, {}))
        dataset["exchanges"].append(exchange)
        exchange_count += 1

    if exchange_count != manifest.get("exchange_count"):
        raise InventoryStoreCorruptionError(
            "Exchange row count does not match the checkpoint manifest."
        )

    store = create_inventory_store(
        database,
        backend=backend,
        scenario_identity=manifest.get("scenario_identity"),
        take_ownership=True,
    )
    store._state.generation = int(manifest.get("generation", 0))
    store._state.activity_fingerprints = _read_activity_fingerprints(
        path, store._state.activity_order
    )
    store._validation_certificate_payload = manifest.get("validation_certificate")
    store._provenance_payload = manifest.get("provenance")
    return store


__all__ = [
    "ActivityId",
    "ExchangeId",
    "ActivityKey",
    "ProviderKey",
    "FilterExpression",
    "ActivityQuery",
    "ActivityRecord",
    "ExchangeRecord",
    "InventoryStore",
    "InventoryTransaction",
    "InventoryStoreBuilder",
    "LegacyInventoryStore",
    "CompactInventoryStore",
    "ReadOnlyInventoryStore",
    "IndexedInventoryList",
    "InventoryStoreError",
    "InventoryStoreReadOnlyError",
    "InventoryStoreCorruptionError",
    "InventoryStoreVersionError",
    "STORE_SCHEMA_VERSION",
    "compact_exchange_payload",
    "create_inventory_store",
    "get_scenario_inventory",
    "replace_scenario_inventory",
    "install_wurst_query_engine",
    "get_wurst_query_diagnostics",
]
