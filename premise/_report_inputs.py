"""Private, immutable inputs shared by the report's isolated workers.

The temporary snapshots are made from already validated effective stores. They
share numeric columns through read-only memory maps, and retain the checkpoint's
lazy metadata instead of loading the full inventory in every worker. Pickles in
this module are private files created and consumed within one report session.
"""

from __future__ import annotations

import mmap
import pickle
import threading
from collections import OrderedDict
from pathlib import Path

import numpy as np

from .inventory_store import (
    _ColumnarExchangeStorage,
    _ColumnarExchangeTable,
    ReadOnlyInventoryStore,
    create_inventory_store,
)
from .inventory_normalization import (
    normalize_inventory_numeric_types,
    normalize_inventory_uncertainty,
    normalize_exact_deterministic_exchange_duplicates,
)


def _normalize_source(database):
    normalize_inventory_numeric_types(database)
    normalize_inventory_uncertainty(database)
    normalize_exact_deterministic_exchange_duplicates(database)
    return database


class DeferredReportSource(ReadOnlyInventoryStore):
    """Normalize independent source activities in workers when possible.

    Ordinary inventory APIs still resolve the same fully normalized store.
    This view owns a freshly loaded source list, never a caller's live graph.
    """

    def __init__(self, database, backend):
        self._report_database = database
        self._resolved_store = None
        self.backend_name = backend

    @property
    def _store(self):
        if self._resolved_store is None:
            self._resolved_store = create_inventory_store(
                _normalize_source(self._report_database),
                backend=self.backend_name,
                scenario_identity="source",
                take_ownership=True,
                compute_fingerprints=False,
            )
            self._report_database = None
        return self._resolved_store

    def __len__(self):
        return (
            len(self._report_database)
            if self._resolved_store is None
            else len(self._resolved_store)
        )


_ARRAY_FIELDS = (
    "activity_ids",
    "exchange_starts",
    "exchange_ends",
    "_string_columns",
    "_numeric_kinds",
    "_numeric_floats",
    "_numeric_ints",
    "_boolean_columns",
)
_METADATA_FIELDS = (
    "_activity_position_by_id",
    "_activity_common_columns",
    "activity_offsets",
    "exchange_metadata_offsets",
    "_string_values",
)


def write_report_input(store, directory: Path):
    """Snapshot effective values and return small ordered activity headers."""
    directory.mkdir()
    deferred = isinstance(store, DeferredReportSource) and store._resolved_store is None
    if not deferred:
        while hasattr(store, "_store"):
            store = store._store
    state = None if deferred else getattr(store, "_state", None)
    table = getattr(state, "exchanges", None)
    columnar = (
        isinstance(table, _ColumnarExchangeTable)
        and getattr(store, "_reporting_generation", None) == store.generation
    )
    headers = []

    def add_header(activity_id, payload, exchange_count):
        code = payload.get("code")
        headers.append(
            (
                int(activity_id),
                None if code in (None, "") else str(code),
                (
                    str(payload.get("name") or ""),
                    str(payload.get("reference product", payload.get("product")) or ""),
                    str(payload.get("location") or ""),
                    str(payload.get("unit") or ""),
                ),
                exchange_count,
            )
        )

    if columnar:
        storage = table._storage
        metadata = {name: getattr(storage, name) for name in _METADATA_FIELDS}
        metadata["sidecar"] = storage.checkpoint / "metadata.bin"
        with (directory / "columns.bin").open("wb") as stream:

            def write_array(array):
                array = np.ascontiguousarray(array)
                offset = stream.tell()
                array.tofile(stream)
                return offset, array.dtype.str, array.shape

            for name in _ARRAY_FIELDS:
                value = getattr(storage, name)
                metadata[name] = (
                    {key: write_array(array) for key, array in value.items()}
                    if isinstance(value, dict)
                    else write_array(value)
                )
        for activity_id in store.iter_activity_ids():
            position = storage._activity_position(activity_id)
            add_header(
                activity_id,
                state.activities[activity_id],
                int(
                    storage.exchange_ends[position] - storage.exchange_starts[position]
                ),
            )
        metadata["columnar"] = True
    else:
        offsets = {}
        payloads = (
            enumerate(store._report_database)
            if deferred
            else (
                (activity_id, store._report_activity_payload(activity_id))
                for activity_id in store.iter_activity_ids()
            )
        )
        with (directory / "payloads.bin").open("wb") as stream:
            for activity_id, payload in payloads:
                add_header(activity_id, payload, len(payload.get("exchanges", ())))
                start = stream.tell()
                pickle.dump(payload, stream, protocol=pickle.HIGHEST_PROTOCOL)
                offsets[int(activity_id)] = start, stream.tell() - start
        metadata = {"columnar": False, "offsets": offsets, "normalize_source": deferred}
    with (directory / "metadata.pickle").open("wb") as stream:
        pickle.dump(metadata, stream, protocol=pickle.HIGHEST_PROTOCOL)
    return headers


class ReportInput:
    """Minimal report reader; it exposes no mutation or public store API."""

    def __init__(self, directory: Path):
        from .change_report import _PreparedExchange

        self._exchange_factory = _PreparedExchange
        with (directory / "metadata.pickle").open("rb") as stream:
            metadata = pickle.load(stream)
        self._normalize_source = metadata.pop("normalize_source", False)
        self._storage = None
        self._columns_map = None
        self._columns_file = None
        if metadata.pop("columnar"):
            self._columns_file = (directory / "columns.bin").open("rb")
            self._columns_map = mmap.mmap(
                self._columns_file.fileno(), 0, access=mmap.ACCESS_READ
            )

            def read_array(specification):
                offset, dtype, shape = specification
                return np.ndarray(
                    shape, dtype=dtype, buffer=self._columns_map, offset=offset
                )

            for name in _ARRAY_FIELDS:
                value = metadata[name]
                metadata[name] = (
                    {key: read_array(spec) for key, spec in value.items()}
                    if isinstance(value, dict)
                    else read_array(value)
                )
            self._payload_file = metadata.pop("sidecar").open("rb")
            self._payload_map = mmap.mmap(
                self._payload_file.fileno(), 0, access=mmap.ACCESS_READ
            )
            storage = object.__new__(_ColumnarExchangeStorage)
            storage.__dict__.update(metadata)
            storage._sidecar = self._payload_map
            storage._metadata_lock = threading.RLock()
            storage._activity_cache = OrderedDict()
            self._storage = storage
        else:
            self._offsets = metadata["offsets"]
            self._payload_file = (directory / "payloads.bin").open("rb")
            self._payload_map = (
                mmap.mmap(self._payload_file.fileno(), 0, access=mmap.ACCESS_READ)
                if self._offsets
                else None
            )

    def _report_activity_payload(self, activity_id):
        if self._storage is not None:
            payload = self._storage.scenario_cache_base_activity_payload(activity_id)
            payload["exchanges"] = self._storage.report_exchange_payloads(
                activity_id, self._exchange_factory
            )
            return payload
        offset, length = self._offsets[activity_id]
        payload = pickle.loads(self._payload_map[offset : offset + length])
        if self._normalize_source:
            _normalize_source([payload])
        return payload

    def close(self):
        self._storage = None
        for resource in (
            self._payload_map,
            self._payload_file,
            self._columns_map,
            self._columns_file,
        ):
            if resource is not None:
                resource.close()
