"""Zero-copy sparse inventory views over Premise's compact store.

The normal export path materializes every activity and exchange as nested
Python dictionaries before a consumer can build matrix coordinates.  This
module exposes the same ordered graph as two re-iterable sparse streams while
borrowing the compact store's immutable payload mappings.  Consumers therefore
pay only for the representation they actually construct.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
import json
from typing import Any
import uuid

from ..inventory.inventory_store import CompactInventoryStore, ReadOnlyInventoryStore


@dataclass(frozen=True, slots=True)
class SparseActivity:
    """One activity row borrowed from a compact inventory store."""

    storage_id: int
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SparseExchange:
    """One non-nested exchange row and its consumer activity."""

    storage_id: int
    output_activity_id: int
    ordinal: int
    payload: Mapping[str, Any]


class InMemorySparseInventory:
    """Read-only, re-iterable export view backed directly by a compact store.

    Payload mappings are borrowed and must be treated as immutable.  No nested
    ``list[dict]`` database is created.  The view intentionally exposes storage
    IDs because they are dense, stable for the lifetime of the store, and let a
    downstream matrix builder use integer coordinates immediately.
    """

    schema_version = 1

    def __init__(
        self,
        store,
        *,
        factorization_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        underlying = (
            store._store if isinstance(store, ReadOnlyInventoryStore) else store
        )
        if not isinstance(underlying, CompactInventoryStore):
            raise TypeError(
                "In-memory sparse export requires a CompactInventoryStore; "
                f"received {type(underlying).__name__}."
            )
        self._store = underlying
        self.factorization_metadata = dict(factorization_metadata or {})
        self.activity_count = len(underlying)
        self.exchange_count = len(underlying._state.exchanges)
        self.scenario_identity = underlying.scenario_identity
        self.store_generation = underlying.generation
        self._synthetic_codes = {
            int(activity_id): uuid.uuid5(
                uuid.NAMESPACE_URL,
                "premise:sparse-export:"
                + json.dumps(
                    [self.scenario_identity, int(activity_id)],
                    default=str,
                    separators=(",", ":"),
                ),
            ).hex
            for activity_id, payload, _ in underlying._iter_validation_storage()
            if not payload.get("code")
        }

    def _activity_payload(self, activity_id: int, payload: Mapping[str, Any]):
        code = self._synthetic_codes.get(int(activity_id))
        if code is None:
            return payload
        result = dict(payload)
        result["code"] = code
        return result

    def iter_activities(self) -> Iterator[SparseActivity]:
        """Yield ordered activity rows without cloning their metadata."""

        for activity_id, payload, _ in self._store._iter_validation_storage():
            yield SparseActivity(
                int(activity_id), self._activity_payload(activity_id, payload)
            )

    def iter_exchanges(self) -> Iterator[SparseExchange]:
        """Yield ordered exchange rows without nesting or materialization."""

        for activity_id, _, exchanges in self._store._iter_validation_storage():
            for ordinal, (exchange_id, payload) in enumerate(exchanges):
                yield SparseExchange(
                    storage_id=int(exchange_id),
                    output_activity_id=int(activity_id),
                    ordinal=ordinal,
                    payload=payload,
                )

    def factorization_activities(self) -> list[dict[str, Any]]:
        """Return activity-only facades for provenance ownership mapping."""

        return [
            dict(self._activity_payload(activity_id, payload))
            for activity_id, payload, _ in self._store._iter_validation_storage()
        ]


def export_sparse_inventory(
    store,
    *,
    factorization_metadata: Mapping[str, Any] | None = None,
) -> InMemorySparseInventory:
    """Create a zero-copy sparse view of a compact inventory store."""

    return InMemorySparseInventory(
        store,
        factorization_metadata=factorization_metadata,
    )


__all__ = [
    "InMemorySparseInventory",
    "SparseActivity",
    "SparseExchange",
    "export_sparse_inventory",
]
