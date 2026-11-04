"""Immutable sector declarations and deterministic composition primitives.

This module contains no inventory mutation code. Sector planners emit
``SectorPatch`` objects against one immutable reference; the composer reduces
those declarations in canonical sector order so caller order cannot affect the
result.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

DECLARATIVE_PATCH_SCHEMA_VERSION = 1
MatrixName = Literal["technosphere", "biosphere"]
ActivationAction = Literal["activate", "deactivate"]


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _plain(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_plain(item) for item in value), key=_canonical_json)
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _plain(item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Declarative payloads cannot contain non-finite numbers")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"Unsupported declarative payload value: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _plain(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


@dataclass(frozen=True, order=True, slots=True)
class MatrixCoordinate:
    matrix: MatrixName
    row: str
    column: str

    def __post_init__(self) -> None:
        if self.matrix not in {"technosphere", "biosphere"}:
            raise ValueError(f"Unsupported matrix: {self.matrix!r}")
        if not self.row or not self.column:
            raise ValueError("Matrix coordinates require non-empty stable identities")

    def to_dict(self) -> dict[str, str]:
        return {"matrix": self.matrix, "row": self.row, "column": self.column}


@dataclass(frozen=True, order=True, slots=True)
class ExchangeFactor:
    coordinate: MatrixCoordinate
    factor: float

    def __post_init__(self) -> None:
        value = float(self.factor)
        if not math.isfinite(value):
            raise ValueError("Exchange factors must be finite")
        object.__setattr__(self, "factor", value)

    def to_dict(self) -> dict[str, Any]:
        return {"coordinate": self.coordinate.to_dict(), "factor": self.factor}


@dataclass(frozen=True, order=True, slots=True)
class ActivityProposal:
    activity: str
    action: ActivationAction

    def __post_init__(self) -> None:
        if not self.activity:
            raise ValueError("Activity proposals require a stable identity")
        if self.action not in {"activate", "deactivate"}:
            raise ValueError(f"Unsupported activity action: {self.action!r}")

    def to_dict(self) -> dict[str, str]:
        return {"activity": self.activity, "action": self.action}


@dataclass(frozen=True, order=True, slots=True)
class ProviderProposal:
    exchange: str
    provider: str

    def __post_init__(self) -> None:
        if not self.exchange or not self.provider:
            raise ValueError(
                "Provider proposals require stable exchange and provider IDs"
            )

    def to_dict(self) -> dict[str, str]:
        return {"exchange": self.exchange, "provider": self.provider}


@dataclass(frozen=True, order=True, slots=True)
class MetadataProposal:
    activity: str
    field: str
    value_json: str

    def __post_init__(self) -> None:
        if not self.activity or not self.field:
            raise ValueError("Metadata proposals require activity and field identities")
        canonical = _canonical_json(json.loads(self.value_json))
        object.__setattr__(self, "value_json", canonical)

    @classmethod
    def from_value(cls, activity: str, field: str, value: Any) -> MetadataProposal:
        return cls(activity=activity, field=field, value_json=_canonical_json(value))

    def to_dict(self) -> dict[str, Any]:
        return {
            "activity": self.activity,
            "field": self.field,
            "value": json.loads(self.value_json),
        }


@dataclass(frozen=True, order=True, slots=True)
class SharedOperation:
    operator: str
    target: str
    payload_json: str

    def __post_init__(self) -> None:
        if not self.operator or not self.target:
            raise ValueError("Shared operations require operator and target identities")
        canonical = _canonical_json(json.loads(self.payload_json))
        object.__setattr__(self, "payload_json", canonical)

    @classmethod
    def from_payload(cls, operator: str, target: str, payload: Any) -> SharedOperation:
        return cls(
            operator=operator,
            target=target,
            payload_json=_canonical_json(payload),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator": self.operator,
            "target": self.target,
            "payload": json.loads(self.payload_json),
        }

    @property
    def payload(self) -> Any:
        return json.loads(self.payload_json)


@dataclass(frozen=True, order=True, slots=True)
class ReferenceExchange:
    identity: str
    input_identity: str
    type: str
    name: str
    amount: float
    product: str = ""
    location: str = ""
    unit: str = ""
    metadata_json: str = "{}"

    def __post_init__(self) -> None:
        if not self.identity or not self.input_identity or not self.type:
            raise ValueError("Reference exchanges require stable identities and type")
        amount = float(self.amount)
        if not math.isfinite(amount):
            raise ValueError("Reference exchange amounts must be finite")
        object.__setattr__(self, "amount", amount)
        object.__setattr__(
            self,
            "metadata_json",
            _canonical_json(json.loads(self.metadata_json)),
        )

    @classmethod
    def from_metadata(
        cls,
        *,
        identity: str,
        input_identity: str,
        type: str,
        name: str,
        amount: float,
        product: str = "",
        location: str = "",
        unit: str = "",
        metadata: Mapping[str, Any] | None = None,
    ) -> ReferenceExchange:
        return cls(
            identity=identity,
            input_identity=input_identity,
            type=type,
            name=name,
            amount=amount,
            product=product,
            location=location,
            unit=unit,
            metadata_json=_canonical_json(metadata or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "input_identity": self.input_identity,
            "type": self.type,
            "name": self.name,
            "amount": self.amount,
            "product": self.product,
            "location": self.location,
            "unit": self.unit,
            "metadata": json.loads(self.metadata_json),
        }

    @property
    def metadata(self) -> dict[str, Any]:
        return json.loads(self.metadata_json)


@dataclass(frozen=True, order=True, slots=True)
class ReferenceActivity:
    identity: str
    name: str
    product: str
    location: str
    unit: str
    exchanges: tuple[ReferenceExchange, ...] = ()
    metadata_json: str = "{}"

    def __post_init__(self) -> None:
        if not self.identity or not self.name:
            raise ValueError("Reference activities require stable identity and name")
        object.__setattr__(self, "exchanges", tuple(sorted(self.exchanges)))
        object.__setattr__(
            self,
            "metadata_json",
            _canonical_json(json.loads(self.metadata_json)),
        )
        _require_unique(
            self.exchanges, lambda item: item.identity, "reference exchange"
        )

    @classmethod
    def from_metadata(
        cls,
        *,
        identity: str,
        name: str,
        product: str,
        location: str,
        unit: str,
        exchanges: Iterable[ReferenceExchange] = (),
        metadata: Mapping[str, Any] | None = None,
    ) -> ReferenceActivity:
        return cls(
            identity=identity,
            name=name,
            product=product,
            location=location,
            unit=unit,
            exchanges=tuple(exchanges),
            metadata_json=_canonical_json(metadata or {}),
        )

    @property
    def metadata(self) -> dict[str, Any]:
        return json.loads(self.metadata_json)

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "name": self.name,
            "product": self.product,
            "location": self.location,
            "unit": self.unit,
            "exchanges": [exchange.to_dict() for exchange in self.exchanges],
            "metadata": self.metadata,
        }


@dataclass(frozen=True, slots=True)
class LatentReference:
    model: str
    pathway: str
    year: int
    activities: tuple[ReferenceActivity, ...]
    schema_version: int = DECLARATIVE_PATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DECLARATIVE_PATCH_SCHEMA_VERSION:
            raise ValueError(f"Unsupported reference schema: {self.schema_version}")
        if not self.model or not self.pathway:
            raise ValueError("Reference model and pathway must be non-empty")
        object.__setattr__(self, "year", int(self.year))
        object.__setattr__(self, "activities", tuple(sorted(self.activities)))
        _require_unique(
            self.activities, lambda item: item.identity, "reference activity"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "model": self.model,
            "pathway": self.pathway,
            "year": self.year,
            "activities": [activity.to_dict() for activity in self.activities],
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict()).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SectorPatch:
    sector: str
    model: str
    pathway: str
    year: int
    reference_digest: str
    factors: tuple[ExchangeFactor, ...] = ()
    activities: tuple[ActivityProposal, ...] = ()
    providers: tuple[ProviderProposal, ...] = ()
    metadata: tuple[MetadataProposal, ...] = ()
    shared_operations: tuple[SharedOperation, ...] = ()
    schema_version: int = DECLARATIVE_PATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DECLARATIVE_PATCH_SCHEMA_VERSION:
            raise ValueError(f"Unsupported patch schema: {self.schema_version}")
        if not all((self.sector, self.model, self.pathway, self.reference_digest)):
            raise ValueError("Patch identity fields must be non-empty")
        object.__setattr__(self, "year", int(self.year))
        for name in (
            "factors",
            "activities",
            "providers",
            "metadata",
            "shared_operations",
        ):
            object.__setattr__(self, name, tuple(sorted(getattr(self, name))))
        _require_unique(self.factors, lambda item: item.coordinate, "factor coordinate")
        _require_unique(self.activities, lambda item: item.activity, "activity")
        _require_unique(self.providers, lambda item: item.exchange, "exchange provider")
        _require_unique(
            self.metadata,
            lambda item: (item.activity, item.field),
            "metadata field",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "sector": self.sector,
            "model": self.model,
            "pathway": self.pathway,
            "year": self.year,
            "reference_digest": self.reference_digest,
            "factors": [item.to_dict() for item in self.factors],
            "activities": [item.to_dict() for item in self.activities],
            "providers": [item.to_dict() for item in self.providers],
            "metadata": [item.to_dict() for item in self.metadata],
            "shared_operations": [item.to_dict() for item in self.shared_operations],
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict()).encode()).hexdigest()


def _require_unique(values: Iterable[Any], key, label: str) -> None:
    seen = set()
    for value in values:
        identity = key(value)
        if identity in seen:
            raise ValueError(f"Duplicate {label} declaration: {identity!r}")
        seen.add(identity)


class PatchConflict(ValueError):
    """Raised when invariant sector declarations cannot be merged generically."""


@dataclass(frozen=True, order=True, slots=True)
class ComposedFactor:
    coordinate: MatrixCoordinate
    factor: float
    sectors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "coordinate": self.coordinate.to_dict(),
            "factor": self.factor,
            "sectors": list(self.sectors),
        }


@dataclass(frozen=True, order=True, slots=True)
class ComposedDeclaration:
    identity: tuple[str, ...]
    value_json: str
    sectors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": list(self.identity),
            "value": json.loads(self.value_json),
            "sectors": list(self.sectors),
        }


@dataclass(frozen=True, slots=True)
class ComposedPatch:
    model: str
    pathway: str
    year: int
    reference_digest: str
    sectors: tuple[str, ...]
    factors: tuple[ComposedFactor, ...]
    activities: tuple[ComposedDeclaration, ...]
    providers: tuple[ComposedDeclaration, ...]
    metadata: tuple[ComposedDeclaration, ...]
    shared_operations: tuple[ComposedDeclaration, ...]
    schema_version: int = DECLARATIVE_PATCH_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "model": self.model,
            "pathway": self.pathway,
            "year": self.year,
            "reference_digest": self.reference_digest,
            "sectors": list(self.sectors),
            "factors": [item.to_dict() for item in self.factors],
            "activities": [item.to_dict() for item in self.activities],
            "providers": [item.to_dict() for item in self.providers],
            "metadata": [item.to_dict() for item in self.metadata],
            "shared_operations": [item.to_dict() for item in self.shared_operations],
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict()).encode()).hexdigest()


def _same_value_declarations(
    groups: Mapping[tuple[str, ...], list[tuple[str, str]]], label: str
) -> tuple[ComposedDeclaration, ...]:
    output = []
    for identity, declarations in sorted(groups.items()):
        values = {value for _, value in declarations}
        if len(values) != 1:
            raise PatchConflict(
                f"Conflicting {label} declarations for {identity!r}: "
                f"{sorted(declarations)!r}"
            )
        output.append(
            ComposedDeclaration(
                identity=identity,
                value_json=next(iter(values)),
                sectors=tuple(sorted(sector for sector, _ in declarations)),
            )
        )
    return tuple(output)


def compose_patches(patches: Iterable[SectorPatch]) -> ComposedPatch:
    """Compose invariant sector patches independently of caller order."""

    ordered = tuple(sorted(patches, key=lambda patch: patch.sector))
    if not ordered:
        raise ValueError("At least one sector patch is required")
    _require_unique(ordered, lambda patch: patch.sector, "sector")
    scenario_keys = {
        (patch.model, patch.pathway, patch.year, patch.reference_digest)
        for patch in ordered
    }
    if len(scenario_keys) != 1:
        raise PatchConflict(
            f"Patches do not share one immutable reference: {scenario_keys}"
        )

    factor_groups: dict[MatrixCoordinate, list[tuple[str, float]]] = defaultdict(list)
    activities: dict[tuple[str, ...], list[tuple[str, str]]] = defaultdict(list)
    providers: dict[tuple[str, ...], list[tuple[str, str]]] = defaultdict(list)
    metadata: dict[tuple[str, ...], list[tuple[str, str]]] = defaultdict(list)
    shared: dict[tuple[str, ...], list[tuple[str, str]]] = defaultdict(list)
    for patch in ordered:
        for proposal in patch.factors:
            factor_groups[proposal.coordinate].append((patch.sector, proposal.factor))
        for proposal in patch.activities:
            activities[(proposal.activity,)].append(
                (patch.sector, _canonical_json(proposal.action))
            )
        for proposal in patch.providers:
            providers[(proposal.exchange,)].append(
                (patch.sector, _canonical_json(proposal.provider))
            )
        for proposal in patch.metadata:
            metadata[(proposal.activity, proposal.field)].append(
                (patch.sector, proposal.value_json)
            )
        for proposal in patch.shared_operations:
            shared[(proposal.operator, proposal.target)].append(
                (patch.sector, proposal.payload_json)
            )

    factors = []
    for coordinate, declarations in sorted(factor_groups.items()):
        ordered_values = sorted(declarations)
        product = math.prod(value for _, value in ordered_values)
        if not math.isfinite(product):
            raise PatchConflict(f"Non-finite factor product at {coordinate!r}")
        factors.append(
            ComposedFactor(
                coordinate=coordinate,
                factor=product,
                sectors=tuple(sector for sector, _ in ordered_values),
            )
        )

    model, pathway, year, reference_digest = next(iter(scenario_keys))
    return ComposedPatch(
        model=model,
        pathway=pathway,
        year=year,
        reference_digest=reference_digest,
        sectors=tuple(patch.sector for patch in ordered),
        factors=tuple(factors),
        activities=_same_value_declarations(activities, "activity"),
        providers=_same_value_declarations(providers, "provider"),
        metadata=_same_value_declarations(metadata, "metadata"),
        # Different payloads are valid inputs to one generic shared operator;
        # retain every declaration in canonical sector order for its reducer.
        shared_operations=tuple(
            ComposedDeclaration(
                identity=identity,
                value_json=_canonical_json(
                    [
                        {"sector": sector, "payload": json.loads(payload)}
                        for sector, payload in sorted(declarations)
                    ]
                ),
                sectors=tuple(sector for sector, _ in sorted(declarations)),
            )
            for identity, declarations in sorted(shared.items())
        ),
    )


__all__ = [
    "DECLARATIVE_PATCH_SCHEMA_VERSION",
    "ActivityProposal",
    "ComposedDeclaration",
    "ComposedFactor",
    "ComposedPatch",
    "ExchangeFactor",
    "LatentReference",
    "MatrixCoordinate",
    "MetadataProposal",
    "PatchConflict",
    "ProviderProposal",
    "ReferenceActivity",
    "ReferenceExchange",
    "SectorPatch",
    "SharedOperation",
    "compose_patches",
]
