"""Stable, sidecar-only identities used by factorized inventory consumers.

The registry never mutates activity dictionaries. Standard premise exports are
therefore unchanged; callers must explicitly enable and retrieve the sidecar.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

FACTORIZATION_IDENTITY_SCHEMA_VERSION = 5
_NAMESPACE = uuid.UUID("f32c09b4-893b-5e8f-a6f8-c5bb6ad44dc9")


def _product(payload: Mapping[str, Any]) -> Any:
    return payload.get("reference product", payload.get("product"))


def activity_semantics(payload: Mapping[str, Any]) -> tuple[str, str, str, str]:
    """Return the stable public fields which identify an activity role."""

    return (
        str(payload.get("name") or ""),
        str(_product(payload) or ""),
        str(payload.get("location") or ""),
        str(payload.get("unit") or ""),
    )


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _plain(item)
            for key, item in value.items()
            if key not in {"database", "code", "input"}
        }
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _plain(item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def activity_fingerprint(payload: Mapping[str, Any]) -> str:
    """Fingerprint inventory content while excluding run-specific identifiers."""

    activity = {
        str(key): _plain(value)
        for key, value in payload.items()
        if key not in {"database", "code", "exchanges"}
    }
    exchanges = [_plain(exchange) for exchange in payload.get("exchanges", ())]
    exchanges.sort(
        key=lambda exchange: json.dumps(
            exchange, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    )
    encoded = json.dumps(
        [activity, exchanges],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def exchange_semantics(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return stable exchange fields used for sidecar supplier links."""

    return (
        str(payload.get("type") or ""),
        str(payload.get("name") or ""),
        str(payload.get("product") or payload.get("reference product") or ""),
        str(payload.get("location") or ""),
        str(payload.get("unit") or ""),
        tuple(str(value) for value in (payload.get("categories") or ())),
        str(payload.get("comment") or ""),
    )


def exchange_identity_key(payload: Mapping[str, Any], occurrence: int) -> str:
    """Serialize one semantic exchange occurrence for stable sidecar lookup."""

    return json.dumps(
        [exchange_semantics(payload), int(occurrence)],
        ensure_ascii=False,
        separators=(",", ":"),
    )


class FactorizationIdentityRegistry:
    """Assign deterministic identities without modifying inventory payloads."""

    schema_version = FACTORIZATION_IDENTITY_SCHEMA_VERSION

    def __init__(self) -> None:
        self._by_object: dict[int, str] = {}
        self._by_code: dict[str, str] = {}
        self._created_counts: dict[tuple[str, tuple[str, str, str, str]], int] = {}
        self._baseline_exchanges: dict[tuple[str, tuple[Any, ...], int], str] = {}
        self._baseline_registered = False
        self._active_sector: str | None = None
        self._last_sector: str | None = None

    @staticmethod
    def _identity(token: str) -> str:
        return str(uuid.uuid5(_NAMESPACE, token))

    def register_baseline(self, datasets: Iterable[Mapping[str, Any]]) -> None:
        """Register the immutable scenario starting graph exactly once."""

        if self._baseline_registered:
            return
        occurrences: dict[tuple[str, str, str, str], int] = {}
        for dataset in datasets:
            database = dataset.get("database")
            code = dataset.get("code")
            if code not in (None, ""):
                # Compact scenario activities can omit ``database`` but retain
                # the immutable source code. Avoid hashing every baseline
                # exchange when that stable storage identity is available.
                token = json.dumps(
                    ["baseline-key", str(database or ""), str(code)],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            else:
                semantics = activity_semantics(dataset)
                ordinal = occurrences.get(semantics, 0)
                occurrences[semantics] = ordinal + 1
                token = json.dumps(
                    ["baseline-role", semantics, ordinal],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            self._by_object[id(dataset)] = self._identity(token)
            if code not in (None, ""):
                self._by_code[str(code)] = self._by_object[id(dataset)]
            exchange_occurrences: dict[tuple[Any, ...], int] = {}
            exchange_consumer = (
                str(code) if code not in (None, "") else self._by_object[id(dataset)]
            )
            for exchange in dataset.get("exchanges", ()):
                semantics = exchange_semantics(exchange)
                occurrence = exchange_occurrences.get(semantics, 0)
                exchange_occurrences[semantics] = occurrence + 1
                exchange_token = json.dumps(
                    [
                        "baseline-exchange",
                        exchange_consumer,
                        semantics,
                        occurrence,
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                identity = self._identity(exchange_token)
                self._baseline_exchanges[(exchange_consumer, semantics, occurrence)] = (
                    identity
                )
        self._baseline_registered = True

    def register_created(
        self, dataset: Mapping[str, Any], *, sector: str | None = None
    ) -> str | None:
        """Register one sector-created activity at its creation boundary."""

        existing = self._by_object.get(id(dataset))
        if existing is not None:
            return existing
        owner = str(sector or self._active_sector or "")
        if not owner:
            return None
        semantics = activity_semantics(dataset)
        group = owner, semantics
        ordinal = self._created_counts.get(group, 0)
        self._created_counts[group] = ordinal + 1
        token = json.dumps(
            ["created", owner, semantics, ordinal],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        identity = self._identity(token)
        self._by_object[id(dataset)] = identity
        code = dataset.get("code")
        if code not in (None, ""):
            self._by_code[str(code)] = identity
        return identity

    @contextmanager
    def session(self, sector: str) -> Iterator[None]:
        previous = self._active_sector
        self._active_sector = self._last_sector = str(sector)
        token = _ACTIVE_REGISTRY.set(self)
        try:
            yield
        finally:
            _ACTIVE_REGISTRY.reset(token)
            self._active_sector = previous

    def payload_for(self, datasets: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        """Return identities keyed by current activity code for one scenario."""

        datasets = list(datasets)
        identities: dict[str, str] = {}
        missing: list[Mapping[str, Any]] = []
        for dataset in datasets:
            code = dataset.get("code")
            identity = self._by_object.get(id(dataset))
            if identity is None and code not in (None, ""):
                identity = self._by_code.get(str(code))
                if identity is not None:
                    self._by_object[id(dataset)] = identity
            if identity is None:
                # A few legacy transformations still replace list entries
                # instead of using the common creation boundary. Recover a
                # deterministic identity at the final sidecar boundary while
                # retaining the most recently active sector as provenance.
                identity = self.register_created(
                    dataset, sector=self._last_sector or "recovered"
                )
            if identity is None:
                missing.append(dataset)
            elif code not in (None, ""):
                identities[str(code)] = identity

        candidates: dict[tuple[str, str, str, str], list[str]] = {}
        for dataset in datasets:
            code = dataset.get("code")
            identity = identities.get(str(code))
            if identity is not None:
                candidates.setdefault(activity_semantics(dataset), []).append(identity)

        provider_identities: dict[str, dict[str, str]] = {}
        exchange_identities: dict[str, dict[str, str]] = {}
        ambiguous_provider_count = 0
        ambiguous_candidates = {
            semantics: matches
            for semantics, matches in candidates.items()
            if len(matches) > 1
        }
        for dataset in datasets:
            consumer_code = dataset.get("code")
            if consumer_code in (None, ""):
                continue
            links: dict[str, str] = {}
            exchange_links: dict[str, str] = {}
            occurrences: dict[tuple[Any, ...], int] = {}
            for exchange in dataset.get("exchanges", ()):
                semantics = exchange_semantics(exchange)
                occurrence = occurrences.get(semantics, 0)
                occurrences[semantics] = occurrence + 1
                exchange_key = exchange_identity_key(exchange, occurrence)
                consumer_identity = identities.get(str(consumer_code), "")
                identity = self._baseline_exchanges.get(
                    (str(consumer_code), semantics, occurrence)
                )
                if identity is None:
                    token = json.dumps(
                        ["exchange", consumer_identity, semantics, occurrence],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    identity = self._identity(token)
                exchange_links[exchange_key] = identity

                if exchange.get("type") != "technosphere":
                    continue
                raw_input = exchange.get("input")
                if isinstance(raw_input, (list, tuple)) and len(raw_input) == 2:
                    # Explicit storage keys are resolved directly by matrix
                    # consumers and need no semantic sidecar link.
                    continue
                matches = ambiguous_candidates.get(activity_semantics(exchange), [])
                if matches:
                    links[exchange_key] = matches[0]
                    ambiguous_provider_count += 1
            if links:
                provider_identities[str(consumer_code)] = links
            if exchange_links:
                exchange_identities[str(consumer_code)] = exchange_links
        return {
            "schema_version": self.schema_version,
            "identities": identities,
            "provider_identities": provider_identities,
            "exchange_identities": exchange_identities,
            "ambiguous_provider_count": ambiguous_provider_count,
            "missing_exchange_count": 0,
            "missing_count": len(missing),
        }

    def merge_registered_identities(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Merge identities registered after a compact-store handoff."""

        result = dict(payload)
        identities = dict(result.get("identities", {}))
        identities.update(self._by_code)
        result.update(
            {
                "schema_version": self.schema_version,
                "identities": identities,
                "missing_count": int(result.get("missing_count", 0)),
            }
        )
        return result


_ACTIVE_REGISTRY: ContextVar[FactorizationIdentityRegistry | None] = ContextVar(
    "premise_factorization_identity_registry", default=None
)


def register_factorization_activity(dataset: Mapping[str, Any]) -> str | None:
    """Register an addition when a factorization session is active."""

    registry = _ACTIVE_REGISTRY.get()
    return None if registry is None else registry.register_created(dataset)


__all__ = [
    "FACTORIZATION_IDENTITY_SCHEMA_VERSION",
    "FactorizationIdentityRegistry",
    "activity_fingerprint",
    "activity_semantics",
    "exchange_identity_key",
    "exchange_semantics",
    "register_factorization_activity",
]
