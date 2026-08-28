"""Instance-owned structured provenance for premise transformations.

The collector in this module deliberately does not write files.  A
``NewDatabase`` instance owns the collector and activates it only while one
sector transformation is running.  Transformation classes can therefore keep
their historical ``write_log`` call sites without sharing report state through
process-global log files.
"""

from __future__ import annotations

import copy
import json
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Iterable, Iterator, Mapping

PROVENANCE_SCHEMA_VERSION = 2


def _plain(value: Any) -> Any:
    """Return a compact JSON-compatible representation."""

    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
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


def _tuple_tree(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return tuple(_tuple_tree(item) for item in value)
    return value


def _activity_identity(dataset: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "code": dataset.get("code"),
        "name": dataset.get("name"),
        "product": dataset.get("reference product", dataset.get("product")),
        "location": dataset.get("location"),
        "unit": dataset.get("unit"),
    }


def _default_explanation(
    transformation: str, status: str, dataset: Mapping[str, Any]
) -> str:
    name = dataset.get("name", "activity")
    location = dataset.get("location", "unknown location")
    return (
        f"{transformation.replace('_', ' ').title()} {status} "
        f"{name!r} in {location}."
    )


@dataclass(frozen=True, slots=True)
class ProvenanceEvent:
    """One structured declaration made by a transformation."""

    schema_version: int
    scenario_identity: tuple[Any, ...]
    sector: str
    transformation: str
    activity: Mapping[str, Any]
    reason_code: str
    explanation: str
    status: str = "modified"
    iam_variable: str | None = None
    algorithm: str | None = None
    configuration_reference: str | None = None
    proxy: str | None = None
    fallback_rank: int | None = None
    computed_target_values: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "scenario_identity", _tuple_tree(self.scenario_identity)
        )
        object.__setattr__(
            self, "activity", MappingProxyType(copy.deepcopy(dict(self.activity)))
        )
        object.__setattr__(
            self,
            "computed_target_values",
            MappingProxyType(copy.deepcopy(dict(self.computed_target_values))),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "scenario_identity": _plain(self.scenario_identity),
            "sector": self.sector,
            "transformation": self.transformation,
            "activity": _plain(self.activity),
            "reason_code": self.reason_code,
            "explanation": self.explanation,
            "status": self.status,
            "iam_variable": self.iam_variable,
            "algorithm": self.algorithm,
            "configuration_reference": self.configuration_reference,
            "proxy": self.proxy,
            "fallback_rank": self.fallback_rank,
            "computed_target_values": _plain(self.computed_target_values),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ProvenanceEvent":
        data = dict(payload)
        data["scenario_identity"] = _tuple_tree(data.get("scenario_identity", ()))
        data.setdefault("schema_version", PROVENANCE_SCHEMA_VERSION)
        data.setdefault("activity", {})
        data.setdefault("computed_target_values", {})
        return cls(**data)


@dataclass(frozen=True, slots=True)
class _ActiveSession:
    collector: "ProvenanceCollector"
    scenario_identity: tuple[Any, ...]
    sector: str
    transformation: str


_ACTIVE_SESSION: ContextVar[_ActiveSession | None] = ContextVar(
    "premise_provenance_session", default=None
)


class ProvenanceCollector:
    """Versioned, thread-safe provenance owned by one database build."""

    schema_version = PROVENANCE_SCHEMA_VERSION

    def __init__(self, build_id: str) -> None:
        self.build_id = str(build_id)
        self._events: dict[tuple[Any, ...], list[ProvenanceEvent]] = {}
        self._restored_payloads: dict[tuple[Any, ...], tuple[int, int, str | None]] = {}
        self._lock = threading.RLock()

    @contextmanager
    def session(
        self,
        scenario_identity: Iterable[Any],
        sector: str,
        transformation: str | None = None,
    ) -> Iterator[None]:
        identity = tuple(scenario_identity)
        token = _ACTIVE_SESSION.set(
            _ActiveSession(
                collector=self,
                scenario_identity=identity,
                sector=str(sector),
                transformation=str(transformation or sector),
            )
        )
        try:
            yield
        finally:
            _ACTIVE_SESSION.reset(token)

    def record(
        self,
        *,
        scenario_identity: Iterable[Any],
        sector: str,
        transformation: str,
        activity: Mapping[str, Any],
        reason_code: str,
        explanation: str,
        status: str = "modified",
        iam_variable: str | None = None,
        algorithm: str | None = None,
        configuration_reference: str | None = None,
        proxy: str | None = None,
        fallback_rank: int | None = None,
        computed_target_values: Mapping[str, Any] | None = None,
    ) -> ProvenanceEvent:
        event = ProvenanceEvent(
            schema_version=self.schema_version,
            scenario_identity=tuple(scenario_identity),
            sector=str(sector),
            transformation=str(transformation),
            activity=activity,
            reason_code=str(reason_code),
            explanation=str(explanation),
            status=str(status),
            iam_variable=None if iam_variable is None else str(iam_variable),
            algorithm=None if algorithm is None else str(algorithm),
            configuration_reference=(
                None
                if configuration_reference is None
                else str(configuration_reference)
            ),
            proxy=None if proxy is None else str(proxy),
            fallback_rank=(None if fallback_rank is None else int(fallback_rank)),
            computed_target_values=computed_target_values or {},
        )
        with self._lock:
            self._events.setdefault(event.scenario_identity, []).append(event)
        return event

    def events_for(
        self, scenario_identity: Iterable[Any]
    ) -> tuple[ProvenanceEvent, ...]:
        with self._lock:
            return tuple(self._events.get(tuple(scenario_identity), ()))

    def payload_for(self, scenario_identity: Iterable[Any]) -> dict[str, Any]:
        identity = tuple(scenario_identity)
        events = self.events_for(identity)
        payload = {
            "schema_version": self.schema_version,
            "build_id": self.build_id,
            "events": [event.to_dict() for event in events],
        }
        with self._lock:
            self._restored_payloads[identity] = (
                id(payload),
                len(events),
                self.build_id,
            )
        return payload

    def restore(
        self, scenario_identity: Iterable[Any], payload: Mapping[str, Any] | None
    ) -> None:
        if not isinstance(payload, Mapping):
            return
        if int(payload.get("schema_version", -1)) != self.schema_version:
            return
        identity = tuple(scenario_identity)
        token = (
            id(payload),
            len(payload.get("events", ())),
            payload.get("build_id"),
        )
        with self._lock:
            if self._restored_payloads.get(identity) == token:
                return
        restored = []
        for item in payload.get("events", ()):
            try:
                event = ProvenanceEvent.from_dict(item)
            except (KeyError, TypeError, ValueError):
                continue
            if event.scenario_identity == identity:
                restored.append(event)
        with self._lock:
            existing = self._events.setdefault(identity, [])
            signatures = {
                json.dumps(event.to_dict(), sort_keys=True, ensure_ascii=False)
                for event in existing
            }
            for event in restored:
                signature = json.dumps(
                    event.to_dict(), sort_keys=True, ensure_ascii=False
                )
                if signature not in signatures:
                    existing.append(event)
                    signatures.add(signature)
            self._restored_payloads[identity] = token


def record_change_event(
    transformation: Any,
    dataset: Mapping[str, Any],
    status: str = "modified",
    *,
    sector: str | None = None,
    reason_code: str | None = None,
    explanation: str | None = None,
    iam_variable: str | None = None,
    algorithm: str | None = None,
    configuration_reference: str | None = None,
    proxy: str | None = None,
    fallback_rank: int | None = None,
) -> ProvenanceEvent | None:
    """Record a historical ``write_log`` call in the active build session.

    Calls made outside ``NewDatabase.update`` are intentionally harmless.  This
    keeps transformation classes independently testable without introducing a
    process-global fallback collector.
    """

    active = _ACTIVE_SESSION.get()
    if active is None:
        return None

    parameters = _plain(dataset.get("log parameters", {}))
    if isinstance(parameters, Mapping):
        iam_variable = (
            iam_variable
            or parameters.get("IAM variable")
            or parameters.get("iam variable")
        )
        configuration_reference = (
            configuration_reference
            or parameters.get("configuration reference")
            or parameters.get("configuration source")
        )
        proxy = proxy or parameters.get("proxy") or parameters.get("selected proxy")
        if fallback_rank is None:
            fallback_rank = parameters.get("fallback rank")
    if fallback_rank not in (None, ""):
        try:
            fallback_rank = int(fallback_rank)
        except (TypeError, ValueError, OverflowError):
            fallback_rank = None
    transformation_name = active.transformation
    sector_name = sector or active.sector
    normalized_status = str(status).strip().lower().replace(" ", "_")
    code = reason_code or f"{transformation_name}.{normalized_status}"
    return active.collector.record(
        scenario_identity=active.scenario_identity,
        sector=sector_name,
        transformation=transformation_name,
        activity=_activity_identity(dataset),
        reason_code=code,
        explanation=explanation
        or _default_explanation(transformation_name, str(status), dataset),
        status=str(status),
        iam_variable=iam_variable,
        algorithm=algorithm
        or (
            "marginal mix"
            if getattr(transformation, "system_model", None) == "consequential"
            and sector_name in {"electricity", "fuels"}
            else (
                "average production-volume mix"
                if sector_name in {"electricity", "fuels"}
                else None
            )
        ),
        configuration_reference=configuration_reference,
        proxy=proxy,
        fallback_rank=fallback_rank,
        computed_target_values=parameters,
    )


__all__ = [
    "PROVENANCE_SCHEMA_VERSION",
    "ProvenanceCollector",
    "ProvenanceEvent",
    "record_change_event",
]
