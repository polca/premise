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

FACTORIZATION_IDENTITY_SCHEMA_VERSION = 29
FACTORIZATION_SECTOR_PRIORITY = {
    sector: priority
    for priority, sector in enumerate(
        (
            "biomass",
            "electricity",
            "cement",
            "steel",
            "fuels",
            "renewable",
            "metals",
            "mining",
            "heat",
            "cdr",
            "battery",
            "cars",
            "two_wheelers",
            "trucks",
            "ships",
            "buses",
            "trains",
            "final energy",
            "emissions",
            "external",
        )
    )
}
FACTORIZATION_SECTOR_MIGRATION = tuple(
    {
        "sector": sector,
        "mode": (
            "native-sector-patch-v1"
            if sector == "emissions"
            else "immutable-singleton-atomic-diff-v1"
        ),
        "patch_contract": (
            "premise.declarative.SectorPatch"
            if sector == "emissions"
            else "db_assembler.declarative.AtomicExchangePatch"
        ),
        "immutable_reference": True,
        "selection_independent": True,
    }
    for sector in FACTORIZATION_SECTOR_PRIORITY
    if sector != "external"
)
FACTORIZATION_EXCHANGE_LINEAGE_FIELD = "_premise factorization exchange lineages"
FACTORIZATION_PROVIDER_ROUTING_FIELD = "_premise factorization provider routing"
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


def derived_exchange_role(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return the provider-specific role for a derived exchange row."""

    return exchange_semantics(payload)


class FactorizationIdentityRegistry:
    """Assign deterministic identities without modifying inventory payloads."""

    schema_version = FACTORIZATION_IDENTITY_SCHEMA_VERSION

    def __init__(self) -> None:
        self._by_object: dict[int, str] = {}
        self._by_code: dict[str, str] = {}
        self._by_storage_id: dict[int, str] = {}
        self._baseline_identities: dict[tuple[str, str, str, str], list[str]] = {}
        self._created_counts: dict[tuple[str, str, str, str], int] = {}
        self._created_identities: dict[tuple[str, str, str, str], list[str]] = {}
        self._created_identity_codes: dict[str, set[str]] = {}
        self._activity_origins: dict[str, dict[str, Any]] = {}
        self._baseline_exchanges: dict[tuple[str, tuple[Any, ...], int], str] = {}
        self._baseline_registered = False
        self._active_sector: str | None = None
        self._last_sector: str | None = None
        self._shared_operations: dict[str, dict[str, Any]] = {}

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
            self._baseline_identities.setdefault(
                activity_semantics(dataset), []
            ).append(self._by_object[id(dataset)])
            storage_id = getattr(dataset, "activity_id", None)
            if storage_id is not None:
                self._by_storage_id[int(storage_id)] = self._by_object[id(dataset)]
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
        storage_id = getattr(dataset, "activity_id", None)
        if storage_id is not None:
            existing = self._by_storage_id.get(int(storage_id))
            if existing is not None:
                self._by_object[id(dataset)] = existing
                return existing
        code = dataset.get("code")
        if code not in (None, ""):
            existing = self._by_code.get(str(code))
            if existing is not None:
                self._by_object[id(dataset)] = existing
                return existing
        owner = str(sector or self._active_sector or "")
        if not owner:
            return None
        semantics = activity_semantics(dataset)
        # The creator is provenance, not identity. Independent sectors which
        # declare the same first logical role in separate singleton runs must
        # address the same latent activity. If genuinely distinct activities
        # with identical public semantics coexist, their deterministic ordinal
        # still keeps them separate; those transformations should prefer
        # ``register_derived`` so source lineage supplies the stronger role.
        group = semantics
        candidates = self._created_identities.get(group, ())
        if candidates:
            if code not in (None, ""):
                matching = [
                    identity
                    for identity in candidates
                    if str(code) in self._created_identity_codes.get(identity, ())
                ]
                if len(matching) == 1:
                    identity = matching[0]
                    self._by_object[id(dataset)] = identity
                    self._by_code[str(code)] = identity
                    return identity
            code_free = [
                identity
                for identity in candidates
                if not self._created_identity_codes.get(identity)
            ]
            if len(candidates) == 1 and (code in (None, "") or code_free):
                identity = candidates[0]
                self._by_object[id(dataset)] = identity
                if code not in (None, ""):
                    self._by_code[str(code)] = identity
                    self._created_identity_codes.setdefault(identity, set()).add(
                        str(code)
                    )
                return identity
        ordinal = self._created_counts.get(group, 0)
        self._created_counts[group] = ordinal + 1
        token = json.dumps(
            ["created", semantics, ordinal],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        identity = self._identity(token)
        self._created_identities.setdefault(group, []).append(identity)
        self._activity_origins[identity] = {
            "kind": "created",
            "authority": "canonical-role",
            "role": list(semantics),
            "ordinal": ordinal,
        }
        self._by_object[id(dataset)] = identity
        if code not in (None, ""):
            self._by_code[str(code)] = identity
            self._created_identity_codes.setdefault(identity, set()).add(str(code))
        return identity

    def identity_for(self, dataset: Mapping[str, Any]) -> str | None:
        """Return an already registered identity for a materialized or store view."""

        identity = self._by_object.get(id(dataset))
        if identity is not None:
            return identity
        storage_id = getattr(dataset, "activity_id", None)
        if storage_id is not None:
            identity = self._by_storage_id.get(int(storage_id))
        code = dataset.get("code")
        if identity is None and code not in (None, ""):
            identity = self._by_code.get(str(code))
        if identity is not None:
            self._by_object[id(dataset)] = identity
        return identity

    def exchange_lineages_for(
        self,
        dataset: Mapping[str, Any],
        exchanges: Iterable[Mapping[str, Any]] | None = None,
    ) -> tuple[tuple[str, ...], ...]:
        """Return immutable lineages aligned with ``exchanges``.

        Transformations which reconstruct an exchange recipe after creating a
        derived activity cannot rely on the copied exchange metadata: the new
        rows may come from a different source activity. Resolve each source row
        against the baseline registry instead, without mutating that source.
        """

        rows = tuple(dataset.get("exchanges", ()) if exchanges is None else exchanges)
        source_identity = self.identity_for(dataset)
        if source_identity is None:
            return tuple(() for _ in rows)
        source_code = dataset.get("code")
        source_consumer = (
            str(source_code) if source_code not in (None, "") else source_identity
        )
        occurrences: dict[tuple[Any, ...], int] = {}
        result: list[tuple[str, ...]] = []
        for exchange in rows:
            inherited = tuple(
                sorted(
                    set(
                        map(
                            str,
                            exchange.get(FACTORIZATION_EXCHANGE_LINEAGE_FIELD, ()),
                        )
                    )
                )
            )
            semantics = exchange_semantics(exchange)
            occurrence = occurrences.get(semantics, 0)
            occurrences[semantics] = occurrence + 1
            if inherited:
                result.append(inherited)
                continue
            identity = self._baseline_exchanges.get(
                (source_consumer, semantics, occurrence)
            )
            if identity is None:
                token = json.dumps(
                    ["exchange", source_identity, semantics, occurrence],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                identity = self._identity(token)
            result.append((identity,))
        return tuple(result)

    def bind_storage_id(
        self, dataset: Mapping[str, Any], storage_id: int
    ) -> str | None:
        """Bind a compact-store row to the identity assigned before insertion."""

        identity = self.identity_for(dataset)
        if identity is not None:
            self._by_storage_id[int(storage_id)] = identity
        return identity

    def register_derived(
        self,
        dataset: Mapping[str, Any],
        *,
        source: Mapping[str, Any],
        operation: str,
        qualifier: Any,
    ) -> str | None:
        """Register a derived activity from stable source lineage.

        The lineage deliberately excludes the active sector. Two transformations
        which regionalize the same source activity for the same target therefore
        declare one latent activity instead of context-dependent duplicates.
        """

        existing = self._by_object.get(id(dataset))
        code = dataset.get("code")
        if existing is None and code not in (None, ""):
            existing = self._by_code.get(str(code))
        if existing is not None:
            existing_origin = self._activity_origins.get(existing)
            if not (
                isinstance(existing_origin, Mapping)
                and existing_origin.get("kind") == "created"
            ):
                self._by_object[id(dataset)] = existing
                return existing

        source_identity = self._by_object.get(id(source))
        source_code = source.get("code")
        if source_identity is None and source_code not in (None, ""):
            source_identity = self._by_code.get(str(source_code))
        if source_identity is None:
            source_identity = self.register_created(source)
        if source_identity is None:
            return self.register_created(dataset)

        token = json.dumps(
            ["derived", str(operation), source_identity, _plain(qualifier)],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        identity = self._identity(token)
        self._by_object[id(dataset)] = identity
        self._activity_origins[identity] = {
            "kind": "derived",
            "authority": "proxy",
            "source": source_identity,
            "source code": str(source_code or ""),
            "source role": list(activity_semantics(source)),
            "operation": str(operation),
            "qualifier": _plain(qualifier),
            "role": list(activity_semantics(dataset)),
        }
        if code not in (None, ""):
            self._by_code[str(code)] = identity
            if existing is not None:
                self._created_identity_codes.get(existing, set()).discard(str(code))
        storage_id = getattr(dataset, "activity_id", None)
        if storage_id is not None:
            self._by_storage_id[int(storage_id)] = identity

        source_code = source.get("code")
        source_consumer = (
            str(source_code) if source_code not in (None, "") else source_identity
        )
        source_occurrences: dict[tuple[Any, ...], int] = {}
        for source_exchange, derived_exchange in zip(
            source.get("exchanges", ()),
            dataset.get("exchanges", ()),
        ):
            semantics = exchange_semantics(source_exchange)
            occurrence = source_occurrences.get(semantics, 0)
            source_occurrences[semantics] = occurrence + 1
            inherited = source_exchange.get(FACTORIZATION_EXCHANGE_LINEAGE_FIELD)
            if inherited:
                lineages = tuple(sorted(map(str, inherited)))
            else:
                source_exchange_identity = self._baseline_exchanges.get(
                    (source_consumer, semantics, occurrence)
                )
                if source_exchange_identity is None:
                    exchange_token = json.dumps(
                        ["exchange", source_identity, semantics, occurrence],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    source_exchange_identity = self._identity(exchange_token)
                lineages = (source_exchange_identity,)
            derived_exchange[FACTORIZATION_EXCHANGE_LINEAGE_FIELD] = list(lineages)
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

    def register_shared_operation(
        self,
        payload: Mapping[str, Any],
        *,
        declaration_identity: Any = None,
    ) -> None:
        """Record one invariant generic reducer declaration in the sidecar."""

        if not self._active_sector:
            return
        operation = {
            "sector": self._active_sector,
            **{str(key): _plain(value) for key, value in payload.items()},
        }
        if operation.get("operator") in {
            "cached-provider-route",
            "relink-provider-route",
            "source-recipe-provider-route",
        }:
            operation["composition_priority"] = FACTORIZATION_SECTOR_PRIORITY.get(
                self._active_sector, len(FACTORIZATION_SECTOR_PRIORITY)
            )
        token_payload = (
            operation
            if declaration_identity is None
            else [self._active_sector, _plain(declaration_identity)]
        )
        token = json.dumps(
            token_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        self._shared_operations[token] = operation

    def payload_for(
        self,
        datasets: Iterable[Mapping[str, Any]],
        *,
        include_exchanges: bool = True,
    ) -> dict[str, Any]:
        """Return identities keyed by current activity code for one scenario.

        ``include_exchanges=False`` resolves only activity identities. It is
        used by compact sparse exporters after atomic edge identities have
        already been finalized during ``NewDatabase.update``.
        """

        datasets = list(datasets)
        identities: dict[str, str] = {}
        missing: list[Mapping[str, Any]] = []
        unresolved: list[Mapping[str, Any]] = []
        used_identities: set[str] = set()
        for dataset in datasets:
            code = dataset.get("code")
            identity = self._by_object.get(id(dataset))
            if identity is None and code not in (None, ""):
                identity = self._by_code.get(str(code))
                if identity is not None:
                    self._by_object[id(dataset)] = identity
            if identity is None:
                unresolved.append(dataset)
            elif code not in (None, ""):
                identities[str(code)] = identity
                used_identities.add(identity)

        baseline_positions: dict[tuple[str, str, str, str], int] = {}
        for dataset in unresolved:
            code = dataset.get("code")
            semantics = activity_semantics(dataset)
            candidates = self._baseline_identities.get(semantics, ())
            position = baseline_positions.get(semantics, 0)
            while (
                position < len(candidates) and candidates[position] in used_identities
            ):
                position += 1
            identity = None
            if position < len(candidates):
                identity = candidates[position]
                baseline_positions[semantics] = position + 1
                self._by_object[id(dataset)] = identity
                if code not in (None, ""):
                    self._by_code[str(code)] = identity
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
                used_identities.add(identity)

        # Creation ordinals are useful while several same-role objects coexist,
        # but transient objects must not shift the identity of the sole role
        # which survives into delivery. Reconcile only generic creations here;
        # lineage-derived identities deliberately keep their stronger source
        # contract. Transformations which deliver several indistinguishable
        # roles must register them as derived activities.
        generic_created = {
            identity
            for candidates in self._created_identities.values()
            for identity in candidates
        }
        surviving_created: dict[tuple[str, str, str, str], list[Mapping[str, Any]]] = {}
        for dataset in datasets:
            code = dataset.get("code")
            identity = identities.get(str(code))
            if identity in generic_created:
                surviving_created.setdefault(activity_semantics(dataset), []).append(
                    dataset
                )
        for semantics, survivors in surviving_created.items():
            if len(survivors) != 1:
                continue
            dataset = survivors[0]
            code = dataset.get("code")
            token = json.dumps(
                ["created", semantics, 0],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            identity = self._identity(token)
            self._by_object[id(dataset)] = identity
            self._activity_origins[identity] = {
                "kind": "created",
                "authority": "canonical-role",
                "role": list(semantics),
                "ordinal": 0,
            }
            if code not in (None, ""):
                identities[str(code)] = identity
                self._by_code[str(code)] = identity

        if not include_exchanges:
            return {
                "schema_version": self.schema_version,
                "sector_migration": FACTORIZATION_SECTOR_MIGRATION,
                "identities": identities,
                "provider_identities": {},
                "exchange_identities": {},
                "exchange_lineages": {},
                "ambiguous_provider_count": 0,
                "missing_exchange_count": 0,
                "missing_count": len(missing),
                "activity_origins": {
                    identity: self._activity_origins[identity]
                    for identity in sorted(set(identities.values()))
                    if identity in self._activity_origins
                },
                "shared_operations": [
                    self._shared_operations[token]
                    for token in sorted(self._shared_operations)
                ],
            }

        candidates: dict[tuple[str, str, str, str], list[str]] = {}
        for dataset in datasets:
            code = dataset.get("code")
            identity = identities.get(str(code))
            if identity is not None:
                candidates.setdefault(activity_semantics(dataset), []).append(identity)

        provider_identities: dict[str, dict[str, str]] = {}
        exchange_identities: dict[str, dict[str, str]] = {}
        exchange_lineages: dict[str, dict[str, list[str]]] = {}
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
            lineage_links: dict[str, list[str]] = {}
            occurrences: dict[tuple[Any, ...], int] = {}
            derived_role_occurrences: dict[tuple[Any, ...], int] = {}
            for exchange in dataset.get("exchanges", ()):
                semantics = exchange_semantics(exchange)
                occurrence = occurrences.get(semantics, 0)
                occurrences[semantics] = occurrence + 1
                exchange_key = exchange_identity_key(exchange, occurrence)
                consumer_identity = identities.get(str(consumer_code), "")
                inherited = exchange.pop(FACTORIZATION_EXCHANGE_LINEAGE_FIELD, None)
                inherited = tuple(sorted(set(map(str, inherited or ()))))
                identity = None
                if inherited:
                    lineage_links[exchange_key] = list(inherited)
                    consumer_semantics = activity_semantics(dataset)
                    consumer_role = (
                        ["public-role", consumer_semantics]
                        if len(candidates.get(consumer_semantics, ())) == 1
                        else ["stable-identity", consumer_identity]
                    )
                    if exchange.get("type") == "production":
                        lineage_payload = ["production-exchange", consumer_role]
                    else:
                        role = derived_exchange_role(exchange)
                        role_occurrence = derived_role_occurrences.get(role, 0)
                        derived_role_occurrences[role] = role_occurrence + 1
                        lineage_payload = [
                            "derived-exchange-role",
                            consumer_role,
                            role,
                            role_occurrence,
                        ]
                    lineage_token = json.dumps(
                        lineage_payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    identity = self._identity(lineage_token)
                if identity is None:
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
            if lineage_links:
                stable_consumer = identities.get(str(consumer_code))
                if stable_consumer is not None:
                    exchange_lineages[str(stable_consumer)] = lineage_links
        return {
            "schema_version": self.schema_version,
            "sector_migration": FACTORIZATION_SECTOR_MIGRATION,
            "identities": identities,
            "provider_identities": provider_identities,
            "exchange_identities": exchange_identities,
            "exchange_lineages": exchange_lineages,
            "ambiguous_provider_count": ambiguous_provider_count,
            "missing_exchange_count": 0,
            "missing_count": len(missing),
            "activity_origins": {
                identity: self._activity_origins[identity]
                for identity in sorted(set(identities.values()))
                if identity in self._activity_origins
            },
            "shared_operations": [
                self._shared_operations[token]
                for token in sorted(self._shared_operations)
            ],
        }

    def merge_registered_identities(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Merge identities registered after a compact-store handoff."""

        result = dict(payload)
        identities = dict(result.get("identities", {}))
        identities.update(self._by_code)
        current_identities = set(identities.values())
        activity_origins = dict(result.get("activity_origins", {}))
        activity_origins.update(
            {
                identity: origin
                for identity, origin in self._activity_origins.items()
                if identity in current_identities
            }
        )
        result.update(
            {
                "schema_version": self.schema_version,
                "sector_migration": FACTORIZATION_SECTOR_MIGRATION,
                "identities": identities,
                "activity_origins": activity_origins,
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


def register_factorization_shared_operation(
    payload: Mapping[str, Any], *, declaration_identity: Any = None
) -> None:
    """Record a reducer declaration only while factorization mode is active."""

    registry = _ACTIVE_REGISTRY.get()
    if registry is not None:
        registry.register_shared_operation(
            payload, declaration_identity=declaration_identity
        )


def factorization_session_active() -> bool:
    """Return whether mutations are running inside a factorization session."""

    return _ACTIVE_REGISTRY.get() is not None


def bind_factorization_activity_storage_id(
    dataset: Mapping[str, Any], storage_id: int
) -> str | None:
    """Bind a compact activity row while a factorization session is active."""

    registry = _ACTIVE_REGISTRY.get()
    return None if registry is None else registry.bind_storage_id(dataset, storage_id)


def current_factorization_activity_identity(
    dataset: Mapping[str, Any],
) -> str | None:
    """Resolve provenance for the current factorization session without mutation."""

    registry = _ACTIVE_REGISTRY.get()
    return None if registry is None else registry.identity_for(dataset)


def current_factorization_exchange_lineages(
    dataset: Mapping[str, Any],
    exchanges: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[tuple[str, ...], ...]:
    """Return source-exchange lineages during an active factorization session."""

    registry = _ACTIVE_REGISTRY.get()
    if registry is None:
        rows = tuple(dataset.get("exchanges", ()) if exchanges is None else exchanges)
        return tuple(() for _ in rows)
    return registry.exchange_lineages_for(dataset, exchanges)


def register_factorization_derived_activity(
    dataset: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    operation: str,
    qualifier: Any,
) -> str | None:
    """Register a lineage-derived activity when a factorization session is active."""

    registry = _ACTIVE_REGISTRY.get()
    if registry is None:
        return None
    return registry.register_derived(
        dataset,
        source=source,
        operation=operation,
        qualifier=qualifier,
    )


__all__ = [
    "FACTORIZATION_EXCHANGE_LINEAGE_FIELD",
    "FACTORIZATION_IDENTITY_SCHEMA_VERSION",
    "FACTORIZATION_PROVIDER_ROUTING_FIELD",
    "FACTORIZATION_SECTOR_MIGRATION",
    "FACTORIZATION_SECTOR_PRIORITY",
    "FactorizationIdentityRegistry",
    "activity_fingerprint",
    "activity_semantics",
    "bind_factorization_activity_storage_id",
    "current_factorization_activity_identity",
    "current_factorization_exchange_lineages",
    "derived_exchange_role",
    "exchange_identity_key",
    "exchange_semantics",
    "factorization_session_active",
    "register_factorization_activity",
    "register_factorization_derived_activity",
]
