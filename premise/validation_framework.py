"""Read-only methodological validation for premise inventory graphs.

The historical validators in :mod:`premise.validation` grew together with the
export preparation code and therefore contain a mixture of checks and repairs.
This module is deliberately different: it only reads an :class:`InventoryStore`
and returns immutable, serialisable results.  Any inventory normalisation must
happen before calling this module.
"""

from __future__ import annotations

import hashlib
import json
import math
import pickle
import re
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields, is_dataclass, replace
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

import numpy as np
import yaml

from .filesystem_constants import DATA_DIR
from .inventory_store import InventoryStore

VALIDATION_RULESET_VERSION = 5
ValidationSeverity = Literal["error", "warning"]
Applicability = Literal["applicable", "not_applicable"]
ValidationPhaseKind = Literal["sector", "graph", "export"]


def _plain(value: Any) -> Any:
    """Return a deterministic JSON-compatible representation."""

    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _plain(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Mapping):
        return {
            str(key): _plain(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_plain(item) for item in value), key=repr)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


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


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        _plain(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _activity_key(payload: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    return (
        payload.get("name"),
        payload.get("reference product", payload.get("product")),
        payload.get("location"),
    )


def _exchange_provider_key(exchange: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    return (
        exchange.get("name"),
        exchange.get("product", exchange.get("reference product")),
        exchange.get("location"),
    )


@dataclass(frozen=True, slots=True)
class ActivitySelector:
    """Stable selector used by validation intents and suppressions."""

    name: str | None = None
    product: str | None = None
    location: str | None = None
    code: str | None = None
    name_pattern: str | None = None

    def __post_init__(self) -> None:
        if self.name_pattern is not None:
            re.compile(self.name_pattern)

    def matches(self, payload: Mapping[str, Any]) -> bool:
        exact = all(
            expected is None or payload.get(field_name) == expected
            for field_name, expected in (
                ("name", self.name),
                ("reference product", self.product),
                ("location", self.location),
                ("code", self.code),
            )
        )
        return exact and (
            self.name_pattern is None
            or re.fullmatch(self.name_pattern, str(payload.get("name", ""))) is not None
        )

    @classmethod
    def from_key(cls, key: Iterable[Any]) -> "ActivitySelector":
        name, product, location = tuple(key)
        return cls(name=name, product=product, location=location)

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in (
                ("name", self.name),
                ("product", self.product),
                ("location", self.location),
                ("code", self.code),
                ("name_pattern", self.name_pattern),
            )
            if value is not None
        }


@dataclass(frozen=True, slots=True)
class ValidationSuppression:
    """Narrow, reviewable exception to one validation rule."""

    rule_id: str
    selector: ActivitySelector
    versions: tuple[str, ...]
    system_models: tuple[str, ...]
    explanation: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "versions", tuple(self.versions))
        object.__setattr__(self, "system_models", tuple(self.system_models))
        if not self.rule_id or not self.explanation.strip():
            raise ValueError(
                "A validation suppression needs a rule ID and explanation."
            )
        if not self.selector.to_dict():
            raise ValueError(
                "A validation suppression needs a non-empty dataset selector."
            )
        if not self.versions or not self.system_models:
            raise ValueError(
                "A validation suppression needs applicable versions and system models."
            )

    def applies(
        self,
        issue: "ValidationIssue",
        *,
        version: str | None,
        system_model: str | None,
    ) -> bool:
        payload = {
            "name": issue.activity_key[0] if issue.activity_key else None,
            "reference product": issue.activity_key[1] if issue.activity_key else None,
            "location": issue.activity_key[2] if issue.activity_key else None,
            "code": issue.activity_code,
        }
        return (
            issue.rule_id == self.rule_id
            and ("*" in self.versions or str(version) in self.versions)
            and ("*" in self.system_models or system_model in self.system_models)
            and self.selector.matches(payload)
        )


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """One immutable methodological or structural validation finding."""

    rule_id: str
    severity: ValidationSeverity
    message: str
    applicability: Applicability = "applicable"
    checked_object_count: int = 0
    activity_id: int | None = None
    activity_key: tuple[Any, Any, Any] | None = None
    activity_code: str | None = None
    exchange_id: int | None = None
    expected: Any = None
    actual: Any = None
    tolerance: float | None = None
    suppressed: bool = False
    suppression_explanation: str | None = None

    def __post_init__(self) -> None:
        if not self.rule_id:
            raise ValueError("Validation issues require a stable rule ID.")
        if self.severity not in {"error", "warning"}:
            raise ValueError(f"Invalid validation severity: {self.severity!r}.")
        if self.applicability not in {"applicable", "not_applicable"}:
            raise ValueError(
                f"Invalid validation applicability: {self.applicability!r}."
            )
        object.__setattr__(self, "activity_key", _freeze(self.activity_key))
        object.__setattr__(self, "expected", _freeze(self.expected))
        object.__setattr__(self, "actual", _freeze(self.actual))

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "message": self.message,
            "applicability": self.applicability,
            "checked_object_count": self.checked_object_count,
            "activity_id": self.activity_id,
            "activity_key": _plain(self.activity_key),
            "activity_code": self.activity_code,
            "exchange_id": self.exchange_id,
            "expected": _plain(self.expected),
            "actual": _plain(self.actual),
            "tolerance": self.tolerance,
            "suppressed": self.suppressed,
            "suppression_explanation": self.suppression_explanation,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationIssue":
        payload = dict(data)
        if payload.get("activity_key") is not None:
            payload["activity_key"] = tuple(payload["activity_key"])
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class ValidationRuleResult:
    """Complete outcome of one rule, including successful zero-issue checks."""

    rule_id: str
    severity: ValidationSeverity
    applicability: Applicability
    checked_object_count: int
    expected: Any = None
    actual: Any = None
    tolerance: float | None = None
    issues: tuple[ValidationIssue, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "expected", _freeze(self.expected))
        object.__setattr__(self, "actual", _freeze(self.actual))
        object.__setattr__(self, "issues", tuple(self.issues))

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "applicability": self.applicability,
            "checked_object_count": self.checked_object_count,
            "expected": _plain(self.expected),
            "actual": _plain(self.actual),
            "tolerance": self.tolerance,
            "issues": [issue.to_dict() for issue in self.issues],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationRuleResult":
        payload = dict(data)
        payload["issues"] = tuple(
            ValidationIssue.from_dict(issue) for issue in payload.get("issues", ())
        )
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class ValidationPhaseResult:
    """Immutable outcome of one sector, graph, or exporter validation phase."""

    phase_id: str
    kind: ValidationPhaseKind
    rule_results: tuple[ValidationRuleResult, ...] = ()
    elapsed_seconds: float = 0.0
    reused: bool = False

    def __post_init__(self) -> None:
        if not self.phase_id:
            raise ValueError("Validation phases require a stable phase ID.")
        if self.kind not in {"sector", "graph", "export"}:
            raise ValueError(f"Invalid validation phase kind: {self.kind!r}.")
        if self.elapsed_seconds < 0 or not math.isfinite(self.elapsed_seconds):
            raise ValueError(
                "Validation phase elapsed time must be finite and non-negative."
            )
        object.__setattr__(self, "rule_results", tuple(self.rule_results))

    @property
    def issues(self) -> tuple[ValidationIssue, ...]:
        return tuple(issue for result in self.rule_results for issue in result.issues)

    @property
    def errors(self) -> tuple[ValidationIssue, ...]:
        return tuple(
            issue
            for issue in self.issues
            if issue.severity == "error" and not issue.suppressed
        )

    @property
    def warnings(self) -> tuple[ValidationIssue, ...]:
        return tuple(
            issue
            for issue in self.issues
            if issue.severity == "warning" and not issue.suppressed
        )

    @property
    def valid(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase_id": self.phase_id,
            "kind": self.kind,
            "rule_results": [result.to_dict() for result in self.rule_results],
            "elapsed_seconds": self.elapsed_seconds,
            "reused": self.reused,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationPhaseResult":
        payload = dict(data)
        payload["rule_results"] = tuple(
            ValidationRuleResult.from_dict(result)
            for result in payload.get("rule_results", ())
        )
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Immutable result of a complete or targeted validation pass."""

    scenario_identity: Any
    store_generation: int
    ruleset_version: int
    certificate_key: str
    rule_results: tuple[ValidationRuleResult, ...]
    reused: bool = False
    phase_results: tuple[ValidationPhaseResult, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "scenario_identity", _freeze(self.scenario_identity))
        object.__setattr__(self, "rule_results", tuple(self.rule_results))
        phases = tuple(self.phase_results)
        if not phases:
            phases = (
                ValidationPhaseResult(
                    phase_id="graph:full",
                    kind="graph",
                    rule_results=self.rule_results,
                    reused=self.reused,
                ),
            )
        object.__setattr__(self, "phase_results", phases)

    @property
    def issues(self) -> tuple[ValidationIssue, ...]:
        return tuple(issue for phase in self.phase_results for issue in phase.issues)

    @property
    def errors(self) -> tuple[ValidationIssue, ...]:
        return tuple(
            issue
            for issue in self.issues
            if issue.severity == "error" and not issue.suppressed
        )

    @property
    def warnings(self) -> tuple[ValidationIssue, ...]:
        return tuple(
            issue
            for issue in self.issues
            if issue.severity == "warning" and not issue.suppressed
        )

    @property
    def suppressed_issues(self) -> tuple[ValidationIssue, ...]:
        return tuple(issue for issue in self.issues if issue.suppressed)

    @property
    def valid(self) -> bool:
        return not self.errors

    def raise_for_errors(self) -> None:
        if self.errors:
            raise PremiseValidationError(self)

    def with_reuse(self, reused: bool) -> "ValidationReport":
        phases = tuple(
            replace(phase, reused=True) if phase.kind == "graph" else phase
            for phase in self.phase_results
        )
        return replace(self, reused=reused, phase_results=phases)

    def get_phase(self, phase_id: str) -> ValidationPhaseResult | None:
        """Return the most recent phase with ``phase_id``, if present."""

        return next(
            (
                phase
                for phase in reversed(self.phase_results)
                if phase.phase_id == phase_id
            ),
            None,
        )

    def with_phase(self, phase: ValidationPhaseResult) -> "ValidationReport":
        """Add or replace a phase while preserving graph-rule access patterns."""

        phases = tuple(
            item for item in self.phase_results if item.phase_id != phase.phase_id
        ) + (phase,)
        rule_results = self.rule_results
        if phase.kind == "graph":
            rule_results = phase.rule_results
        return replace(self, rule_results=rule_results, phase_results=phases)

    def semantic_only(self) -> "ValidationReport":
        """Drop transient exporter checks before checkpoint persistence."""

        return replace(
            self,
            phase_results=tuple(
                phase for phase in self.phase_results if phase.kind != "export"
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_identity": _plain(self.scenario_identity),
            "store_generation": self.store_generation,
            "ruleset_version": self.ruleset_version,
            "certificate_key": self.certificate_key,
            "rule_results": [result.to_dict() for result in self.rule_results],
            "reused": self.reused,
            "phase_results": [phase.to_dict() for phase in self.phase_results],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationReport":
        payload = dict(data)
        payload["rule_results"] = tuple(
            ValidationRuleResult.from_dict(result)
            for result in payload.get("rule_results", ())
        )
        payload["phase_results"] = tuple(
            ValidationPhaseResult.from_dict(phase)
            for phase in payload.get("phase_results", ())
        )
        return cls(**payload)


class PremiseValidationError(ValueError):
    """Raised when a report contains one or more unsuppressed errors."""

    def __init__(self, report: ValidationReport, report_artifacts: Any = None):
        self.report = report
        self.report_artifacts = report_artifacts

        def describe(issue: ValidationIssue) -> str:
            details = []
            if issue.activity_key is not None:
                details.append(f"activity={tuple(issue.activity_key)!r}")
            if issue.exchange_id is not None:
                details.append(f"exchange_id={issue.exchange_id}")
            if issue.expected is not None:
                expected = repr(_plain(issue.expected))
                if len(expected) > 120:
                    expected = f"{expected[:117]}..."
                details.append(f"expected={expected}")
            if issue.actual is not None:
                actual = repr(_plain(issue.actual))
                if len(actual) > 160:
                    actual = f"{actual[:157]}..."
                details.append(f"actual={actual}")
            detail = f" ({', '.join(details)})" if details else ""
            return f"{issue.rule_id}: {issue.message}{detail}"

        error_counts: dict[str, int] = defaultdict(int)
        representative_issues: dict[str, list[ValidationIssue]] = defaultdict(list)
        for issue in report.errors:
            error_counts[issue.rule_id] += 1
            limit = 5 if issue.rule_id == "GRAPH.NEW_FORBIDDEN_CYCLE" else 1
            if len(representative_issues[issue.rule_id]) < limit:
                representative_issues[issue.rule_id].append(issue)
        preview = "; ".join(
            describe(issue)
            for _, issues in sorted(representative_issues.items())
            for issue in issues
        )
        shown = sum(len(issues) for issues in representative_issues.values())
        suffix = "" if len(report.errors) <= shown else "; ..."
        counts = ", ".join(
            f"{rule_id}={count}" for rule_id, count in sorted(error_counts.items())
        )
        message = (
            f"Inventory validation failed with {len(report.errors)} "
            f"unsuppressed error(s) [{counts}]: {preview}{suffix}"
        )
        workbook_path = getattr(report_artifacts, "workbook_path", None)
        if workbook_path is not None:
            message += f" Diagnostic report: {workbook_path}"
        super().__init__(message)

    @property
    def artifacts(self) -> Any:
        """Optional structured artifacts created for the invalid inventory."""

        return self.report_artifacts

    def attach_report_artifacts(self, report_artifacts: Any) -> None:
        """Attach a diagnostic workbook without replacing the validation error."""

        self.report_artifacts = report_artifacts
        workbook_path = getattr(report_artifacts, "workbook_path", None)
        if workbook_path is not None and str(workbook_path) not in str(self):
            self.args = (f"{self.args[0]} Diagnostic report: {workbook_path}",)


@dataclass(frozen=True, slots=True)
class ValidationIntent:
    """Targets and independent expectations declared by a transformation.

    Keys are semantic activity triples ``(name, reference product, location)``.
    Supplier vectors map a target activity key to ``((supplier_key, share), ...)``.
    ``baseline_fingerprints`` enables collateral-mutation detection without
    keeping a mutable copy of the source graph.
    """

    transformation: str
    applicability: Applicability = "applicable"
    applicability_reason: str | None = None
    targeted: bool = True
    scope_complete: bool = True
    affected_activity_ids: frozenset[int] = frozenset()
    affected_activity_keys: frozenset[tuple[Any, Any, Any]] = frozenset()
    expected_match_count: int | None = None
    expected_regions: tuple[str, ...] = ()
    expected_technologies: tuple[str, ...] = ()
    algorithm: str | None = None
    intended_suppliers: Mapping[
        tuple[Any, Any, Any],
        tuple[tuple[tuple[Any, Any, Any], float], ...],
    ] = field(default_factory=dict)
    computed_target_values: Mapping[str, Any] = field(default_factory=dict)
    baseline_fingerprints: Mapping[tuple[Any, Any, Any], str] = field(
        default_factory=dict
    )
    allowed_added_keys: frozenset[tuple[Any, Any, Any]] = frozenset()
    allowed_removed_keys: frozenset[tuple[Any, Any, Any]] = frozenset()
    baseline_cycles: frozenset[frozenset[tuple[Any, Any, Any]]] = frozenset()
    tolerance: float = 1e-9

    def __post_init__(self) -> None:
        if self.applicability not in {"applicable", "not_applicable"}:
            raise ValueError(
                f"Invalid validation intent applicability: {self.applicability!r}."
            )
        if self.applicability == "not_applicable" and not self.applicability_reason:
            raise ValueError(
                "A not-applicable validation intent requires an explanation."
            )
        object.__setattr__(
            self, "affected_activity_ids", frozenset(self.affected_activity_ids)
        )
        object.__setattr__(
            self, "affected_activity_keys", frozenset(self.affected_activity_keys)
        )
        object.__setattr__(self, "expected_regions", tuple(self.expected_regions))
        object.__setattr__(
            self, "expected_technologies", tuple(self.expected_technologies)
        )
        object.__setattr__(self, "intended_suppliers", _freeze(self.intended_suppliers))
        object.__setattr__(
            self, "computed_target_values", _freeze(self.computed_target_values)
        )
        object.__setattr__(
            self, "baseline_fingerprints", _freeze(self.baseline_fingerprints)
        )
        object.__setattr__(
            self, "allowed_added_keys", frozenset(self.allowed_added_keys)
        )
        object.__setattr__(
            self, "allowed_removed_keys", frozenset(self.allowed_removed_keys)
        )
        object.__setattr__(
            self,
            "baseline_cycles",
            frozenset(frozenset(cycle) for cycle in self.baseline_cycles),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "transformation": self.transformation,
            "applicability": self.applicability,
            "applicability_reason": self.applicability_reason,
            "targeted": self.targeted,
            "scope_complete": self.scope_complete,
            "affected_activity_ids": sorted(self.affected_activity_ids),
            "affected_activity_keys": [
                list(key) for key in sorted(self.affected_activity_keys, key=repr)
            ],
            "expected_match_count": self.expected_match_count,
            "expected_regions": list(self.expected_regions),
            "expected_technologies": list(self.expected_technologies),
            "algorithm": self.algorithm,
            "intended_suppliers": [
                {
                    "target": list(target),
                    "suppliers": [
                        {"key": list(key), "amount": amount} for key, amount in entries
                    ],
                }
                for target, entries in sorted(
                    self.intended_suppliers.items(), key=lambda item: repr(item[0])
                )
            ],
            "computed_target_values": _plain(self.computed_target_values),
            "baseline_fingerprints": [
                {"key": list(key), "fingerprint": fingerprint}
                for key, fingerprint in sorted(
                    self.baseline_fingerprints.items(), key=lambda item: repr(item[0])
                )
            ],
            "allowed_added_keys": [
                list(key) for key in sorted(self.allowed_added_keys, key=repr)
            ],
            "allowed_removed_keys": [
                list(key) for key in sorted(self.allowed_removed_keys, key=repr)
            ],
            "baseline_cycles": [
                [list(key) for key in sorted(cycle, key=repr)]
                for cycle in sorted(self.baseline_cycles, key=repr)
            ],
            "tolerance": self.tolerance,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationIntent":
        payload = dict(data)
        for field_name in (
            "affected_activity_keys",
            "allowed_added_keys",
            "allowed_removed_keys",
        ):
            payload[field_name] = frozenset(
                tuple(key) for key in payload.get(field_name, ())
            )
        payload["affected_activity_ids"] = frozenset(
            payload.get("affected_activity_ids", ())
        )
        intended = payload.get("intended_suppliers", ())
        if isinstance(intended, Mapping):
            payload["intended_suppliers"] = intended
        else:
            payload["intended_suppliers"] = {
                tuple(item["target"]): tuple(
                    (tuple(entry["key"]), float(entry["amount"]))
                    for entry in item.get("suppliers", ())
                )
                for item in intended
            }
        fingerprints = payload.get("baseline_fingerprints", ())
        if isinstance(fingerprints, Mapping):
            payload["baseline_fingerprints"] = fingerprints
        else:
            payload["baseline_fingerprints"] = {
                tuple(item["key"]): item["fingerprint"] for item in fingerprints
            }
        payload["baseline_cycles"] = frozenset(
            frozenset(tuple(key) for key in cycle)
            for cycle in payload.get("baseline_cycles", ())
        )
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class ValidationCertificate:
    """Cacheable proof that one exact store generation passed validation."""

    cache_key: str
    store_generation: int
    ruleset_version: int
    scenario_identity: Any
    source_fingerprint: str
    iam_fingerprint: str
    system_model: str
    version: str
    report: ValidationReport

    def __post_init__(self) -> None:
        object.__setattr__(self, "scenario_identity", _freeze(self.scenario_identity))

    def to_dict(self) -> dict[str, Any]:
        return {
            "cache_key": self.cache_key,
            "store_generation": self.store_generation,
            "ruleset_version": self.ruleset_version,
            "scenario_identity": _plain(self.scenario_identity),
            "source_fingerprint": self.source_fingerprint,
            "iam_fingerprint": self.iam_fingerprint,
            "system_model": self.system_model,
            "version": self.version,
            "report": self.report.semantic_only().to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationCertificate":
        payload = dict(data)
        payload["report"] = ValidationReport.from_dict(payload["report"])
        return cls(**payload)


RULES: tuple[tuple[str, ValidationSeverity], ...] = (
    ("GRAPH.REQUIRED_ACTIVITY_FIELDS", "error"),
    ("GRAPH.REQUIRED_EXCHANGE_FIELDS", "error"),
    ("GRAPH.EXCHANGE_TYPE", "error"),
    ("GRAPH.FINITE_NUMERIC", "error"),
    ("GRAPH.UNCERTAINTY", "error"),
    ("GRAPH.PRODUCTION_REFERENCE", "error"),
    ("GRAPH.REFERENCE_PRODUCTION_AMOUNT", "error"),
    ("GRAPH.PROVIDER_EXISTS", "error"),
    ("GRAPH.STALE_SUPPLIER", "error"),
    ("GRAPH.PROVIDER_AMBIGUOUS", "error"),
    ("GRAPH.PROVIDER_PRODUCT_UNIT", "error"),
    ("GRAPH.GEOGRAPHIC_FALLBACK", "error"),
    ("GRAPH.NEGATIVE_MARKET_SHARE", "error"),
    ("GRAPH.DUPLICATE_SUPPLIER", "error"),
    ("GRAPH.TRANSFORMATION_SCOPE", "error"),
    ("GRAPH.RULE_TARGET_CARDINALITY", "error"),
    ("GRAPH.NEW_FORBIDDEN_CYCLE", "error"),
    ("METHOD.SUPPLIER_VECTOR", "error"),
    ("METHOD.CONSEQUENTIAL_ALGORITHM", "error"),
    ("METHOD.EXPECTED_COVERAGE", "error"),
)


@dataclass(slots=True)
class _Accumulator:
    rule_id: str
    severity: ValidationSeverity
    applicability: Applicability = "applicable"
    checked: int = 0
    expected: Any = None
    tolerance: float | None = None
    issues: list[ValidationIssue] = field(default_factory=list)

    def issue(self, message: str, **kwargs: Any) -> None:
        self.issues.append(
            ValidationIssue(
                rule_id=self.rule_id,
                severity=self.severity,
                message=message,
                expected=kwargs.pop("expected", self.expected),
                tolerance=kwargs.pop("tolerance", self.tolerance),
                **kwargs,
            )
        )

    def result(self) -> ValidationRuleResult:
        issues = tuple(
            replace(issue, checked_object_count=self.checked) for issue in self.issues
        )
        return ValidationRuleResult(
            rule_id=self.rule_id,
            severity=self.severity,
            applicability=self.applicability,
            checked_object_count=self.checked,
            expected=self.expected,
            actual={"issue_count": len(issues)},
            tolerance=self.tolerance,
            issues=issues,
        )


def _iter_storage(store: InventoryStore):
    """Yield activity metadata and exchange storage without graph materialisation."""

    underlying = getattr(store, "_store", store)
    validation_iterator = getattr(underlying, "_iter_validation_storage", None)
    if validation_iterator is not None:
        yield from validation_iterator()
        return
    iterator = getattr(underlying, "_iter_storage_activities", None)
    exchange_getter = getattr(underlying, "_storage_exchange", None)
    if iterator is not None and exchange_getter is not None:
        for activity_id, payload, exchange_ids in iterator():
            yield activity_id, payload, tuple(
                (exchange_id, exchange_getter(exchange_id))
                for exchange_id in exchange_ids
            )
        return

    for record in store.iter_activities():
        yield record.id, record, tuple(
            (exchange_id, store.exchange(exchange_id))
            for exchange_id in record.exchange_ids
        )


def _iter_activity_metadata(store: InventoryStore):
    """Yield activity metadata without constructing exchange views."""

    underlying = getattr(store, "_store", store)
    iterator = getattr(underlying, "_iter_storage_activities", None)
    if iterator is not None:
        for activity_id, payload, _ in iterator():
            yield activity_id, payload
        return
    for record in store.iter_activities():
        yield record.id, record


def _numeric(value: Any) -> bool:
    return isinstance(value, (int, float, np.number)) and not isinstance(value, bool)


def _finite(value: Any) -> bool:
    try:
        return _numeric(value) and math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _missing_required(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value == "")


def _payload_fingerprint(
    payload: Mapping[str, Any], exchanges: Iterable[tuple[int, Mapping[str, Any]]]
) -> str:
    activity = dict(payload)
    activity["exchanges"] = [dict(exchange) for _, exchange in exchanges]
    return _stable_hash(activity)


def _scope_fingerprint(
    payload: Mapping[str, Any], exchanges: Iterable[tuple[int, Mapping[str, Any]]]
) -> str:
    """Return a fast structural hash used only within one build's scope audit."""

    activity = dict(payload)
    activity["exchanges"] = tuple(dict(exchange) for _, exchange in exchanges)
    encoded = pickle.dumps(activity, protocol=pickle.HIGHEST_PROTOCOL)
    return hashlib.blake2b(encoded, digest_size=16).hexdigest()


def _stored_activity_fingerprint(
    store: InventoryStore,
    activity_id: int,
    payload: Mapping[str, Any],
    exchanges: Iterable[tuple[int, Mapping[str, Any]]],
) -> str:
    underlying = getattr(store, "_store", store)
    getter = getattr(underlying, "_activity_fingerprint", None)
    if getter is not None:
        return getter(activity_id)
    return _scope_fingerprint(payload, exchanges)


def inventory_store_fingerprint(store: InventoryStore) -> str:
    """Hash a store deterministically without building a materialised database list."""

    digest = hashlib.sha256()
    for activity_id, payload, exchanges in _iter_storage(store):
        digest.update(str(activity_id).encode("ascii"))
        digest.update(_payload_fingerprint(payload, exchanges).encode("ascii"))
    return digest.hexdigest()


def inventory_activity_fingerprints(
    store: InventoryStore,
) -> Mapping[tuple[Any, Any, Any], str]:
    """Return immutable per-activity hashes for transformation-scope contracts."""

    grouped: dict[tuple[Any, Any, Any], list[str]] = defaultdict(list)
    for activity_id, payload, exchanges in _iter_storage(store):
        grouped[_activity_key(payload)].append(
            _stored_activity_fingerprint(store, activity_id, payload, exchanges)
        )
    return MappingProxyType(
        {
            key: values[0] if len(values) == 1 else _stable_hash(sorted(values))
            for key, values in grouped.items()
        }
    )


def _cycle_signatures(
    adjacency: Mapping[int, set[int]],
    keys_by_id: Mapping[int, tuple[Any, Any, Any]],
) -> frozenset[frozenset[tuple[Any, Any, Any]]]:
    """Return strongly-connected components without recursive graph walking."""

    reverse: dict[int, set[int]] = defaultdict(set)
    for source, targets in adjacency.items():
        for target in targets:
            reverse[target].add(source)

    visited: set[int] = set()
    order: list[int] = []
    for root in keys_by_id:
        if root in visited:
            continue
        stack = [(root, False)]
        while stack:
            node, expanded = stack.pop()
            if expanded:
                order.append(node)
                continue
            if node in visited:
                continue
            visited.add(node)
            stack.append((node, True))
            stack.extend(
                (neighbour, False)
                for neighbour in adjacency.get(node, ())
                if neighbour not in visited
            )

    cycles: set[frozenset[tuple[Any, Any, Any]]] = set()
    assigned: set[int] = set()
    for root in reversed(order):
        if root in assigned:
            continue
        component = []
        stack = [root]
        assigned.add(root)
        while stack:
            node = stack.pop()
            component.append(node)
            for neighbour in reverse.get(node, ()):
                if neighbour not in assigned:
                    assigned.add(neighbour)
                    stack.append(neighbour)
        if len(component) > 1 or root in adjacency.get(root, ()):
            cycles.add(frozenset(keys_by_id[member] for member in component))
    return frozenset(cycles)


def inventory_cycle_signatures(
    store: InventoryStore,
) -> frozenset[frozenset[tuple[Any, Any, Any]]]:
    """Return semantic cycle signatures for a baseline inventory graph."""

    return inventory_baseline_snapshot(store)[1]


def inventory_baseline_snapshot(
    store: InventoryStore,
) -> tuple[
    Mapping[tuple[Any, Any, Any], str],
    frozenset[frozenset[tuple[Any, Any, Any]]],
]:
    """Capture activity fingerprints and cycles in one baseline graph audit."""

    keys_by_id: dict[int, tuple[Any, Any, Any]] = {}
    ids_by_key: dict[tuple[Any, Any, Any], list[int]] = defaultdict(list)
    ids_by_identifier: dict[tuple[Any, Any], int] = {}
    for activity_id, payload in _iter_activity_metadata(store):
        key = _activity_key(payload)
        keys_by_id[activity_id] = key
        ids_by_key[key].append(activity_id)
        if payload.get("database") is not None and payload.get("code") is not None:
            ids_by_identifier[(payload["database"], payload["code"])] = activity_id

    adjacency: dict[int, set[int]] = defaultdict(set)
    fingerprint_rows: dict[tuple[Any, Any, Any], list[str]] = defaultdict(list)
    for activity_id, payload, exchanges in _iter_storage(store):
        exchange_rows = tuple(exchanges)
        fingerprint_rows[keys_by_id[activity_id]].append(
            _stored_activity_fingerprint(store, activity_id, payload, exchange_rows)
        )
        for _, exchange in exchange_rows:
            if exchange.get("type") != "technosphere":
                continue
            provider_id = None
            exchange_input = exchange.get("input")
            if isinstance(exchange_input, (tuple, list)) and len(exchange_input) == 2:
                provider_id = ids_by_identifier.get(tuple(exchange_input))
            if provider_id is None:
                matches = ids_by_key.get(_exchange_provider_key(exchange), ())
                if len(matches) == 1:
                    provider_id = matches[0]
            if provider_id is not None:
                adjacency[activity_id].add(provider_id)
    fingerprints = MappingProxyType(
        {
            key: values[0] if len(values) == 1 else _stable_hash(sorted(values))
            for key, values in fingerprint_rows.items()
        }
    )
    return fingerprints, _cycle_signatures(adjacency, keys_by_id)


def _load_suppressions(path: Path) -> tuple[ValidationSuppression, ...]:
    if not path.exists():
        return ()
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(data, list):
        raise ValueError("Validation suppressions must be a YAML list.")
    suppressions = []
    for row in data:
        if not isinstance(row, Mapping):
            raise ValueError("Each validation suppression must be a mapping.")
        suppressions.append(
            ValidationSuppression(
                rule_id=row.get("rule_id", ""),
                selector=ActivitySelector(**dict(row.get("dataset", {}))),
                versions=tuple(str(item) for item in row.get("versions", ())),
                system_models=tuple(row.get("system_models", ())),
                explanation=row.get("explanation", ""),
            )
        )
    return tuple(suppressions)


@lru_cache(maxsize=1)
def load_validation_suppressions() -> tuple[ValidationSuppression, ...]:
    return _load_suppressions(DATA_DIR / "utils" / "validation" / "suppressions.yaml")


class InventoryGraphValidator:
    """Single-pass, read-only validator for an inventory-store generation."""

    valid_exchange_types = frozenset({"production", "technosphere", "biosphere"})
    required_activity_fields = ("name", "reference product", "location", "unit")
    required_exchange_fields = ("name", "unit", "type", "amount")
    uncertainty_fields = {
        2: ("loc", "scale"),
        3: ("loc", "scale"),
        4: ("minimum", "maximum"),
        5: ("loc", "minimum", "maximum"),
        6: ("loc", "minimum", "maximum"),
        7: ("minimum", "maximum"),
        8: ("loc", "scale", "shape"),
        9: ("loc", "scale", "shape"),
        10: ("loc", "scale", "shape"),
        11: ("loc", "scale", "shape"),
        12: ("loc", "scale", "shape"),
    }

    def __init__(
        self,
        store: InventoryStore,
        *,
        scenario_identity: Any = None,
        source_fingerprint: str = "unknown",
        iam_fingerprint: str = "unknown",
        system_model: str = "cutoff",
        version: str = "unknown",
        intent: ValidationIntent | None = None,
        baseline_cycles: Iterable[Iterable[tuple[Any, Any, Any]]] = (),
        suppressions: Iterable[ValidationSuppression] | None = None,
    ) -> None:
        self.store = store
        self.scenario_identity = (
            scenario_identity
            if scenario_identity is not None
            else getattr(store, "scenario_identity", None)
        )
        self.source_fingerprint = str(source_fingerprint)
        self.iam_fingerprint = str(iam_fingerprint)
        self.system_model = str(system_model)
        self.version = str(version)
        self.intent = intent
        self.baseline_cycles = frozenset(
            frozenset(tuple(key) for key in cycle) for cycle in baseline_cycles
        )
        self.suppressions = tuple(
            load_validation_suppressions() if suppressions is None else suppressions
        )
        self.generation = int(getattr(store, "generation", 0))
        self.cache_key = validation_cache_key(
            store_generation=self.generation,
            scenario_identity=self.scenario_identity,
            source_fingerprint=self.source_fingerprint,
            iam_fingerprint=self.iam_fingerprint,
            system_model=self.system_model,
            version=self.version,
            intent=self.intent,
            baseline_cycles=self.baseline_cycles,
        )

    def _cached_certificate(self) -> ValidationCertificate | None:
        underlying = getattr(self.store, "_store", self.store)
        payload = getattr(underlying, "_validation_certificate_payload", None)
        if not isinstance(payload, Mapping):
            return None
        try:
            certificate = ValidationCertificate.from_dict(payload)
        except (KeyError, TypeError, ValueError):
            return None
        if (
            certificate.cache_key == self.cache_key
            and certificate.ruleset_version == VALIDATION_RULESET_VERSION
            and certificate.store_generation == self.generation
        ):
            return certificate
        return None

    def certify(self, *, raise_on_error: bool = True) -> ValidationCertificate:
        cached = self._cached_certificate()
        if cached is not None:
            report = cached.report.with_reuse(True)
            if raise_on_error:
                report.raise_for_errors()
            return replace(cached, report=report)

        report = self.validate()
        certificate = ValidationCertificate(
            cache_key=self.cache_key,
            store_generation=self.generation,
            ruleset_version=VALIDATION_RULESET_VERSION,
            scenario_identity=self.scenario_identity,
            source_fingerprint=self.source_fingerprint,
            iam_fingerprint=self.iam_fingerprint,
            system_model=self.system_model,
            version=self.version,
            report=report,
        )
        underlying = getattr(self.store, "_store", self.store)
        underlying._validation_certificate_payload = certificate.to_dict()
        if raise_on_error:
            report.raise_for_errors()
        return certificate

    def validate(self) -> ValidationReport:
        started = time.perf_counter()
        rules = {
            rule_id: _Accumulator(rule_id, severity) for rule_id, severity in RULES
        }
        keys_by_id: dict[int, tuple[Any, Any, Any]] = {}
        activity_payloads: dict[int, Mapping[str, Any]] = {}
        ids_by_key: dict[tuple[Any, Any, Any], list[int]] = defaultdict(list)
        ids_by_name_location: dict[tuple[Any, Any], list[int]] = defaultdict(list)
        ids_by_identifier: dict[tuple[Any, Any], int] = {}

        # Metadata and exchange payloads are retained as immutable store views;
        # no activity or exchange dictionaries are materialised here.
        for activity_id, payload in _iter_activity_metadata(self.store):
            key = _activity_key(payload)
            keys_by_id[activity_id] = key
            activity_payloads[activity_id] = payload
            ids_by_key[key].append(activity_id)
            ids_by_name_location[(key[0], key[2])].append(activity_id)
            if payload.get("database") is not None and payload.get("code") is not None:
                ids_by_identifier[(payload["database"], payload["code"])] = activity_id

        intent = self.intent
        if intent is not None and intent.applicability == "not_applicable":
            results = tuple(
                ValidationRuleResult(
                    rule_id=rule_id,
                    severity=severity,
                    applicability="not_applicable",
                    checked_object_count=0,
                    expected=intent.applicability_reason,
                    actual="not applicable",
                )
                for rule_id, severity in RULES
            )
            phase = ValidationPhaseResult(
                phase_id=f"sector:{intent.transformation}:graph",
                kind="sector",
                rule_results=results,
                elapsed_seconds=time.perf_counter() - started,
            )
            return ValidationReport(
                scenario_identity=self.scenario_identity,
                store_generation=self.generation,
                ruleset_version=VALIDATION_RULESET_VERSION,
                certificate_key=self.cache_key,
                rule_results=results,
                phase_results=(phase,),
            )

        target_ids = set(keys_by_id)
        resolved_target_ids: set[int] = set(keys_by_id) if intent is None else set()
        if intent is not None and (
            intent.affected_activity_ids or intent.affected_activity_keys
        ):
            resolved_target_ids = set(intent.affected_activity_ids)
            resolved_target_ids.update(
                activity_id
                for key in intent.affected_activity_keys
                for activity_id in ids_by_key.get(tuple(key), ())
            )
            if intent.targeted:
                target_ids = set(resolved_target_ids)

        cardinality = rules["GRAPH.RULE_TARGET_CARDINALITY"]
        if intent is None:
            cardinality.applicability = "not_applicable"
        else:
            declared_keys = {tuple(key) for key in intent.affected_activity_keys}
            found_key_count = sum(bool(ids_by_key.get(key)) for key in declared_keys)
            found_id_count = sum(
                activity_id in keys_by_id
                and keys_by_id[activity_id] not in declared_keys
                for activity_id in intent.affected_activity_ids
            )
            found_target_count = found_key_count + found_id_count
            cardinality.checked = found_target_count
            expected_count = intent.expected_match_count
            if expected_count is None:
                expected_count = len(intent.affected_activity_ids) + len(
                    intent.affected_activity_keys
                )
            cardinality.expected = expected_count
            if expected_count != found_target_count:
                cardinality.issue(
                    "Validation targets did not resolve to the expected number of activities.",
                    expected=expected_count,
                    actual=found_target_count,
                )

        adjacency: dict[int, set[int]] = defaultdict(set)
        fingerprint_rows: dict[tuple[Any, Any, Any], list[str]] = defaultdict(list)
        scope_targeted_keys = set(intent.affected_activity_keys) if intent else set()
        if intent is not None:
            scope_targeted_keys.update(
                keys_by_id[activity_id]
                for activity_id in intent.affected_activity_ids
                if activity_id in keys_by_id
            )
        scope_fingerprint_keys = (
            set(intent.baseline_fingerprints) - scope_targeted_keys
            if intent is not None and intent.baseline_fingerprints
            else set()
        )
        actual_vectors: dict[
            tuple[Any, Any, Any], dict[tuple[Any, Any, Any], float]
        ] = {}

        for activity_id, payload, exchanges in _iter_storage(self.store):
            key = keys_by_id[activity_id]
            activity_code = payload.get("code")
            activity_unit = payload.get("unit")
            if key in scope_fingerprint_keys:
                fingerprint_rows[key].append(
                    _stored_activity_fingerprint(
                        self.store, activity_id, payload, exchanges
                    )
                )
            if activity_id not in target_ids:
                continue

            required = rules["GRAPH.REQUIRED_ACTIVITY_FIELDS"]
            required.checked += 1
            missing = [
                field_name
                for field_name in self.required_activity_fields
                if not payload.get(field_name)
            ]
            if missing:
                required.issue(
                    "Activity is missing required fields.",
                    activity_id=activity_id,
                    activity_key=key,
                    activity_code=payload.get("code"),
                    expected=self.required_activity_fields,
                    actual=missing,
                )

            reference_productions = 0
            negative_reference_product = False
            activity_name_lower = str(key[0] or "").lower()
            activity_product_lower = str(key[1] or "").lower()
            waste_tokens = (
                "waste",
                "treatment",
                "scrap",
                "residue",
                "sewage",
                "sludge",
                "tailing",
            )
            is_share_market = (
                activity_name_lower.startswith("market for ")
                or activity_name_lower.startswith("market group for ")
            ) and not any(
                token in activity_name_lower or token in activity_product_lower
                for token in waste_tokens
            )
            market_candidates = []
            seen_supplier_rows: dict[
                tuple[Any, ...], list[tuple[int, Mapping[str, Any]]]
            ] = {}
            vector: dict[tuple[Any, Any, Any], float] = defaultdict(float)
            for exchange_id, exchange in exchanges:
                exchange_type = exchange.get("type")
                exchange_name = exchange.get("name")
                exchange_unit = exchange.get("unit")
                exchange_amount = exchange.get("amount")
                exchange_product = exchange.get(
                    "product", exchange.get("reference product")
                )
                exchange_location = exchange.get("location")
                uncertainty_type = exchange.get("uncertainty type", 0)
                required_exchange = rules["GRAPH.REQUIRED_EXCHANGE_FIELDS"]
                required_exchange.checked += 1
                common_values = {
                    "name": exchange_name,
                    "unit": exchange_unit,
                    "type": exchange_type,
                    "amount": exchange_amount,
                    "product": exchange_product,
                    "location": exchange_location,
                }
                exchange_required_fields = self.required_exchange_fields
                if exchange_type in {"production", "technosphere"}:
                    exchange_required_fields += ("product", "location")
                elif exchange_type == "biosphere":
                    exchange_required_fields += ("categories",)
                missing_exchange_fields = [
                    field_name
                    for field_name in exchange_required_fields
                    if _missing_required(
                        exchange.get(field_name)
                        if field_name == "categories"
                        else common_values[field_name]
                    )
                ]
                if missing_exchange_fields:
                    required_exchange.issue(
                        "Exchange is missing required fields.",
                        activity_id=activity_id,
                        activity_key=key,
                        activity_code=activity_code,
                        exchange_id=exchange_id,
                        expected=tuple(exchange_required_fields),
                        actual=missing_exchange_fields,
                    )
                exchange_type_rule = rules["GRAPH.EXCHANGE_TYPE"]
                exchange_type_rule.checked += 1
                if exchange_type not in self.valid_exchange_types:
                    exchange_type_rule.issue(
                        "Exchange has an invalid or missing type.",
                        activity_id=activity_id,
                        activity_key=key,
                        activity_code=activity_code,
                        exchange_id=exchange_id,
                        expected=tuple(sorted(self.valid_exchange_types)),
                        actual=exchange_type,
                    )

                finite_rule = rules["GRAPH.FINITE_NUMERIC"]
                finite_rule.checked += 1
                if not _finite(exchange_amount):
                    finite_rule.issue(
                        "Exchange amount must be a finite numeric value.",
                        activity_id=activity_id,
                        activity_key=key,
                        activity_code=activity_code,
                        exchange_id=exchange_id,
                        expected="finite numeric amount",
                        actual=exchange_amount,
                    )

                self._check_uncertainty(
                    rules["GRAPH.UNCERTAINTY"],
                    activity_id,
                    key,
                    payload,
                    exchange_id,
                    exchange,
                    uncertainty_type,
                    exchange_amount,
                    activity_code,
                )

                if exchange_type == "production":
                    production = rules["GRAPH.PRODUCTION_REFERENCE"]
                    production.checked += 1
                    exchange_key = (
                        exchange_name if exchange_name is not None else key[0],
                        exchange_product if exchange_product is not None else key[1],
                        exchange_location if exchange_location is not None else key[2],
                    )
                    if (
                        exchange_key == key
                        and (
                            exchange_unit
                            if exchange_unit is not None
                            else activity_unit
                        )
                        == activity_unit
                    ):
                        reference_productions += 1
                        if _finite(exchange_amount) and float(exchange_amount) < 0:
                            negative_reference_product = True
                        production_amount = rules["GRAPH.REFERENCE_PRODUCTION_AMOUNT"]
                        production_amount.checked += 1
                        amount = exchange_amount
                        if not _finite(amount) or float(amount) == 0.0:
                            production_amount.issue(
                                "Reference-production amount must be finite and non-zero.",
                                activity_id=activity_id,
                                activity_key=key,
                                activity_code=activity_code,
                                exchange_id=exchange_id,
                                expected="non-zero production amount",
                                actual=amount,
                            )
                    continue

                if exchange_type != "technosphere":
                    continue

                provider_key = (
                    exchange_name,
                    exchange_product,
                    exchange_location,
                )
                if _finite(exchange_amount):
                    vector[provider_key] += float(exchange_amount)
                provider_id = None
                exchange_input = exchange.get("input")
                has_input_identifier = (
                    isinstance(exchange_input, (tuple, list))
                    and len(exchange_input) == 2
                )
                if has_input_identifier:
                    provider_id = ids_by_identifier.get(tuple(exchange_input))
                matches = ids_by_key.get(provider_key, ())

                stale = rules["GRAPH.STALE_SUPPLIER"]
                stale.checked += 1
                if has_input_identifier and provider_id is None:
                    stale.issue(
                        "Technosphere exchange carries an input identifier absent from the inventory graph.",
                        activity_id=activity_id,
                        activity_key=key,
                        activity_code=activity_code,
                        exchange_id=exchange_id,
                        expected="existing (database, code) provider",
                        actual=tuple(exchange_input),
                    )

                exists = rules["GRAPH.PROVIDER_EXISTS"]
                exists.checked += 1
                if provider_id is None and not matches:
                    exists.issue(
                        "Technosphere exchange links to no activity in the inventory graph.",
                        activity_id=activity_id,
                        activity_key=key,
                        activity_code=activity_code,
                        exchange_id=exchange_id,
                        expected="one provider",
                        actual=0,
                    )

                ambiguous = rules["GRAPH.PROVIDER_AMBIGUOUS"]
                ambiguous.checked += 1
                if provider_id is None and len(matches) > 1:
                    ambiguous.issue(
                        "Technosphere exchange has multiple providers and no resolving input identifier.",
                        activity_id=activity_id,
                        activity_key=key,
                        activity_code=activity_code,
                        exchange_id=exchange_id,
                        expected=1,
                        actual=len(matches),
                    )

                agreement = rules["GRAPH.PROVIDER_PRODUCT_UNIT"]
                agreement.checked += 1
                resolved_id = (
                    provider_id
                    if provider_id is not None
                    else (matches[0] if len(matches) == 1 else None)
                )
                if resolved_id is not None:
                    provider = activity_payloads[resolved_id]
                    expected_product = provider.get(
                        "reference product", provider.get("product")
                    )
                    expected_unit = provider.get("unit")
                    if (
                        exchange_name != provider.get("name")
                        or exchange_location != provider.get("location")
                        or exchange_product != expected_product
                        or exchange_unit != expected_unit
                    ):
                        agreement.issue(
                            "Technosphere exchange product or unit disagrees with its provider.",
                            activity_id=activity_id,
                            activity_key=key,
                            activity_code=activity_code,
                            exchange_id=exchange_id,
                            expected={
                                "product": expected_product,
                                "unit": expected_unit,
                                "name": provider.get("name"),
                                "location": provider.get("location"),
                            },
                            actual={
                                "product": exchange_product,
                                "unit": exchange_unit,
                                "name": exchange_name,
                                "location": exchange_location,
                            },
                        )
                    adjacency[activity_id].add(resolved_id)
                elif not matches:
                    name_location_matches = ids_by_name_location.get(
                        (exchange_name, exchange_location), ()
                    )
                    if name_location_matches:
                        agreement.issue(
                            "Technosphere provider name/location exists but product does not match.",
                            activity_id=activity_id,
                            activity_key=key,
                            activity_code=activity_code,
                            exchange_id=exchange_id,
                            expected=sorted(
                                {
                                    activity_payloads[item].get("reference product")
                                    for item in name_location_matches
                                }
                            ),
                            actual=exchange_product,
                        )

                expected_entries = (
                    intent.intended_suppliers.get(key, ()) if intent is not None else ()
                )
                if expected_entries:
                    fallback = rules["GRAPH.GEOGRAPHIC_FALLBACK"]
                    fallback.checked += 1
                    self._check_geographic_fallback(
                        fallback,
                        activity_id,
                        key,
                        payload,
                        exchange_id,
                        exchange,
                        expected_entries,
                    )

                if (
                    is_share_market
                    and exchange_unit == activity_unit
                    and exchange_product == key[1]
                    and not any(
                        token in str(exchange_name or "").lower()
                        for token in waste_tokens
                    )
                ):
                    market_candidates.append((exchange_id, exchange_amount))

                duplicate = rules["GRAPH.DUPLICATE_SUPPLIER"]
                duplicate.checked += 1
                # Repeated links to one provider can represent distinct source
                # components and are supported. Reject only an accidental
                # byte-equivalent exchange record, while resolving the
                # provider independently of ambiguous semantic activity keys.
                supplier_signature = (
                    ("provider-id", resolved_id)
                    if resolved_id is not None
                    else (
                        (
                            "input",
                            *tuple(exchange_input),
                        )
                        if has_input_identifier
                        else ("provider-key", *provider_key)
                    )
                )
                previous_rows = seen_supplier_rows.setdefault(supplier_signature, [])
                matching_exchange_id = next(
                    (
                        previous_exchange_id
                        for previous_exchange_id, previous_exchange in previous_rows
                        if dict(previous_exchange) == dict(exchange)
                    ),
                    None,
                )
                if matching_exchange_id is not None:
                    duplicate.issue(
                        "Activity contains an exact duplicate supplier exchange.",
                        activity_id=activity_id,
                        activity_key=key,
                        activity_code=payload.get("code"),
                        exchange_id=exchange_id,
                        expected="one supplier exchange",
                        actual={
                            "first_exchange_id": matching_exchange_id,
                            "supplier": _exchange_provider_key(exchange),
                            "amount": exchange_amount,
                        },
                    )
                else:
                    previous_rows.append((exchange_id, exchange))

            if not negative_reference_product:
                market_share = rules["GRAPH.NEGATIVE_MARKET_SHARE"]
                market_share.checked += len(market_candidates)
                for exchange_id, exchange_amount in market_candidates:
                    if _finite(exchange_amount) and float(exchange_amount) < 0:
                        market_share.issue(
                            "A market supplier share is negative.",
                            activity_id=activity_id,
                            activity_key=key,
                            activity_code=activity_code,
                            exchange_id=exchange_id,
                            expected=">= 0",
                            actual=exchange_amount,
                        )

            production = rules["GRAPH.PRODUCTION_REFERENCE"]
            if reference_productions != 1:
                production.issue(
                    "Activity must have exactly one matching reference-production exchange.",
                    activity_id=activity_id,
                    activity_key=key,
                    activity_code=payload.get("code"),
                    expected=1,
                    actual=reference_productions,
                )
            actual_vectors[key] = dict(vector)

        fingerprints = {
            key: values[0] if len(values) == 1 else _stable_hash(sorted(values))
            for key, values in fingerprint_rows.items()
        }
        self._check_scope(rules["GRAPH.TRANSFORMATION_SCOPE"], keys_by_id, fingerprints)
        self._check_supplier_vectors(rules["METHOD.SUPPLIER_VECTOR"], actual_vectors)
        self._check_algorithm(rules["METHOD.CONSEQUENTIAL_ALGORITHM"])
        coverage_keys = (
            {
                activity_id: keys_by_id[activity_id]
                for activity_id in resolved_target_ids
                if activity_id in keys_by_id
            }
            if intent is not None
            and (intent.affected_activity_ids or intent.affected_activity_keys)
            else keys_by_id
        )
        self._check_coverage(rules["METHOD.EXPECTED_COVERAGE"], coverage_keys)
        if rules["GRAPH.GEOGRAPHIC_FALLBACK"].checked == 0:
            rules["GRAPH.GEOGRAPHIC_FALLBACK"].applicability = "not_applicable"

        cycles_rule = rules["GRAPH.NEW_FORBIDDEN_CYCLE"]
        cycles_rule.checked = sum(len(values) for values in adjacency.values())
        cycles = _cycle_signatures(adjacency, keys_by_id)
        baseline_cycles = (
            intent.baseline_cycles
            if intent is not None and intent.baseline_cycles
            else self.baseline_cycles
        )

        for cycle in cycles.difference(baseline_cycles):
            representative = min(cycle, key=repr, default=None)
            cycles_rule.issue(
                "Transformation introduced a technosphere cycle absent from the baseline.",
                activity_key=representative,
                expected="baseline cycle or acyclic graph",
                actual=sorted(cycle, key=repr),
            )

        results = tuple(
            self._apply_suppressions(rule.result()) for rule in rules.values()
        )
        phase = ValidationPhaseResult(
            phase_id="graph:full",
            kind="graph",
            rule_results=results,
            elapsed_seconds=time.perf_counter() - started,
        )
        return ValidationReport(
            scenario_identity=self.scenario_identity,
            store_generation=self.generation,
            ruleset_version=VALIDATION_RULESET_VERSION,
            certificate_key=self.cache_key,
            rule_results=results,
            phase_results=(phase,),
        )

    def _check_uncertainty(
        self,
        rule: _Accumulator,
        activity_id: int,
        activity_key: tuple[Any, Any, Any],
        activity: Mapping[str, Any],
        exchange_id: int,
        exchange: Mapping[str, Any],
        uncertainty_type: Any,
        amount: Any,
        activity_code: Any,
    ) -> None:
        rule.checked += 1
        raw_uncertainty_type = uncertainty_type
        try:
            uncertainty_type = int(uncertainty_type)
        except (TypeError, ValueError, OverflowError):
            uncertainty_type = -1
        common = {
            "activity_id": activity_id,
            "activity_key": activity_key,
            "activity_code": activity_code,
            "exchange_id": exchange_id,
        }
        if uncertainty_type not in range(13):
            rule.issue(
                "Exchange has an unsupported uncertainty type.",
                expected="integer from 0 to 12",
                actual=raw_uncertainty_type,
                **common,
            )
            return
        required = self.uncertainty_fields.get(uncertainty_type, ())
        missing = [field_name for field_name in required if field_name not in exchange]
        if missing:
            rule.issue(
                "Exchange uncertainty parameters are incomplete.",
                expected=required,
                actual={"missing": missing},
                **common,
            )
            return
        invalid = {
            field_name: exchange.get(field_name)
            for field_name in required
            if not _finite(exchange.get(field_name))
        }
        if invalid:
            rule.issue(
                "Exchange uncertainty parameters must be finite numbers.",
                expected="finite numeric parameters",
                actual=invalid,
                **common,
            )
            return
        if "scale" in required and float(exchange["scale"]) < 0:
            rule.issue(
                "Uncertainty scale must not be negative.",
                expected=">= 0",
                actual=exchange["scale"],
                **common,
            )
        if "minimum" in required and "maximum" in required:
            minimum = float(exchange["minimum"])
            maximum = float(exchange["maximum"])
            if minimum > maximum:
                rule.issue(
                    "Uncertainty minimum exceeds maximum.",
                    expected="minimum <= maximum",
                    actual={"minimum": minimum, "maximum": maximum},
                    **common,
                )
            if "loc" in required and not minimum <= float(exchange["loc"]) <= maximum:
                rule.issue(
                    "Uncertainty location lies outside its bounds.",
                    expected={"minimum": minimum, "maximum": maximum},
                    actual=exchange["loc"],
                    **common,
                )
        if uncertainty_type == 2:
            if _finite(amount):
                negative = bool(exchange.get("negative", False))
                if float(amount) == 0:
                    rule.issue(
                        "A lognormal exchange cannot have a zero deterministic amount.",
                        expected="non-zero amount",
                        actual=amount,
                        **common,
                    )
                elif negative != (float(amount) < 0):
                    rule.issue(
                        "Lognormal negative flag disagrees with the exchange sign.",
                        expected={"negative": float(amount) < 0},
                        actual={"negative": negative},
                        **common,
                    )

    @staticmethod
    def _is_market_share(
        activity: Mapping[str, Any],
        exchange: Mapping[str, Any],
        negative_reference_product: bool = False,
    ) -> bool:
        name = str(activity.get("name", "")).lower()
        product = str(activity.get("reference product", "")).lower()
        supplier = str(exchange.get("name", "")).lower()
        waste_tokens = (
            "waste",
            "treatment",
            "scrap",
            "residue",
            "sewage",
            "sludge",
            "tailing",
        )
        return (
            (name.startswith("market for ") or name.startswith("market group for "))
            and not negative_reference_product
            and exchange.get("unit") == activity.get("unit")
            and exchange.get("product")
            == activity.get("reference product", activity.get("product"))
            and not any(token in name or token in product for token in waste_tokens)
            and not any(token in supplier for token in waste_tokens)
        )

    @staticmethod
    def _check_geographic_fallback(
        rule: _Accumulator,
        activity_id: int,
        activity_key: tuple[Any, Any, Any],
        activity: Mapping[str, Any],
        exchange_id: int,
        exchange: Mapping[str, Any],
        expected_entries: Iterable[tuple[tuple[Any, Any, Any], float]],
    ) -> None:
        linked_location = exchange.get("location")
        expected_locations = {
            supplier_key[2]
            for supplier_key, _ in expected_entries
            if supplier_key[0] == exchange.get("name")
            and supplier_key[1] == exchange.get("product")
        }
        if expected_locations and linked_location not in expected_locations:
            rule.issue(
                "Technosphere exchange uses a lower-ranked geographic fallback while a better provider exists.",
                activity_id=activity_id,
                activity_key=activity_key,
                activity_code=activity.get("code"),
                exchange_id=exchange_id,
                expected={
                    "locations": sorted(expected_locations),
                },
                actual={"location": linked_location},
            )

    def _check_scope(
        self,
        rule: _Accumulator,
        keys_by_id: Mapping[int, tuple[Any, Any, Any]],
        fingerprints: Mapping[tuple[Any, Any, Any], str],
    ) -> None:
        intent = self.intent
        if (
            intent is None
            or not intent.scope_complete
            or not intent.baseline_fingerprints
        ):
            rule.applicability = "not_applicable"
            return
        current_keys = set(keys_by_id.values())
        baseline_keys = set(intent.baseline_fingerprints)
        targeted = set(intent.affected_activity_keys)
        targeted.update(
            keys_by_id.get(activity_id) for activity_id in intent.affected_activity_ids
        )
        targeted.discard(None)
        rule.checked = len(baseline_keys | current_keys)
        unexpected_added = current_keys - baseline_keys - set(intent.allowed_added_keys)
        unexpected_removed = (
            baseline_keys - current_keys - set(intent.allowed_removed_keys)
        )
        for key in sorted(unexpected_added, key=repr):
            rule.issue(
                "Transformation added an activity outside its declared scope.",
                activity_key=key,
                expected="declared addition",
                actual="unexpected addition",
            )
        for key in sorted(unexpected_removed, key=repr):
            rule.issue(
                "Transformation removed an activity outside its declared scope.",
                activity_key=key,
                expected="declared removal",
                actual="unexpected removal",
            )
        for key in sorted(baseline_keys & current_keys - targeted, key=repr):
            if fingerprints.get(key) != intent.baseline_fingerprints[key]:
                rule.issue(
                    "Transformation modified an activity outside its declared targets.",
                    activity_key=key,
                    expected=intent.baseline_fingerprints[key],
                    actual=fingerprints.get(key),
                )

    def _check_supplier_vectors(
        self,
        rule: _Accumulator,
        actual_vectors: Mapping[
            tuple[Any, Any, Any], Mapping[tuple[Any, Any, Any], float]
        ],
    ) -> None:
        intent = self.intent
        if intent is None or not intent.intended_suppliers:
            rule.applicability = "not_applicable"
            return
        rule.tolerance = intent.tolerance
        for target_key, expected_entries in intent.intended_suppliers.items():
            rule.checked += 1
            expected = {tuple(key): float(amount) for key, amount in expected_entries}
            actual = dict(actual_vectors.get(tuple(target_key), {}))
            all_keys = set(expected) | set(actual)
            wrong = {
                key: {
                    "expected": expected.get(key, 0.0),
                    "actual": actual.get(key, 0.0),
                }
                for key in all_keys
                if not math.isclose(
                    expected.get(key, 0.0),
                    actual.get(key, 0.0),
                    rel_tol=intent.tolerance,
                    abs_tol=intent.tolerance,
                )
            }
            if wrong:
                rule.issue(
                    "Supplier vector composition differs from the independently declared target.",
                    activity_key=tuple(target_key),
                    expected=expected,
                    actual={"vector": actual, "differences": wrong},
                    tolerance=intent.tolerance,
                )

    def _check_algorithm(self, rule: _Accumulator) -> None:
        intent = self.intent
        if intent is None or intent.algorithm is None:
            rule.applicability = "not_applicable"
            return
        rule.checked = 1
        algorithm = intent.algorithm.lower().replace("_", "-")
        if self.system_model == "consequential" and "marginal" not in algorithm:
            rule.issue(
                "Consequential electricity and fuel markets must use a marginal-mix algorithm.",
                expected="marginal mix",
                actual=intent.algorithm,
            )

    def _check_coverage(
        self,
        rule: _Accumulator,
        keys_by_id: Mapping[int, tuple[Any, Any, Any]],
    ) -> None:
        intent = self.intent
        if intent is None or not (
            intent.expected_regions or intent.expected_technologies
        ):
            rule.applicability = "not_applicable"
            return
        keys = tuple(keys_by_id.values())
        for region in intent.expected_regions:
            rule.checked += 1
            if not any(key[2] == region for key in keys):
                rule.issue(
                    "Expected region has no validation target.",
                    expected=region,
                    actual="missing",
                )
        for technology in intent.expected_technologies:
            rule.checked += 1
            if not any(technology.lower() in str(key[0]).lower() for key in keys):
                rule.issue(
                    "Expected technology has no validation target.",
                    expected=technology,
                    actual="missing",
                )

    def _apply_suppressions(self, result: ValidationRuleResult) -> ValidationRuleResult:
        issues = []
        for issue in result.issues:
            suppression = next(
                (
                    item
                    for item in self.suppressions
                    if item.applies(
                        issue,
                        version=self.version,
                        system_model=self.system_model,
                    )
                ),
                None,
            )
            if suppression is not None:
                issue = replace(
                    issue,
                    suppressed=True,
                    suppression_explanation=suppression.explanation,
                )
            issues.append(issue)
        return replace(result, issues=tuple(issues))


def validation_cache_key(
    *,
    store_generation: int,
    scenario_identity: Any,
    source_fingerprint: str,
    iam_fingerprint: str,
    system_model: str,
    version: str,
    intent: ValidationIntent | None = None,
    baseline_cycles: Iterable[Iterable[tuple[Any, Any, Any]]] = (),
) -> str:
    return _stable_hash(
        {
            "store_generation": int(store_generation),
            "scenario_identity": scenario_identity,
            "source_fingerprint": source_fingerprint,
            "iam_fingerprint": iam_fingerprint,
            "system_model": system_model,
            "version": version,
            "ruleset_version": VALIDATION_RULESET_VERSION,
            "intent": intent,
            "baseline_cycles": baseline_cycles,
        }
    )


def record_validation_phase(
    scenario: dict[str, Any],
    result: ValidationReport | ValidationPhaseResult,
) -> None:
    """Attach a semantic phase to a scenario for final report aggregation."""

    phases = result.phase_results if isinstance(result, ValidationReport) else (result,)
    serialized = list(scenario.get("_validation_phase_results", ()))
    for phase in phases:
        if phase.kind == "export":
            continue
        serialized = [
            item for item in serialized if item.get("phase_id") != phase.phase_id
        ]
        serialized.append(phase.to_dict())
    scenario["_validation_phase_results"] = serialized


__all__ = [
    "VALIDATION_RULESET_VERSION",
    "ActivitySelector",
    "InventoryGraphValidator",
    "PremiseValidationError",
    "ValidationCertificate",
    "ValidationIntent",
    "ValidationIssue",
    "ValidationPhaseResult",
    "ValidationReport",
    "ValidationRuleResult",
    "ValidationSuppression",
    "inventory_activity_fingerprints",
    "inventory_baseline_snapshot",
    "inventory_cycle_signatures",
    "inventory_store_fingerprint",
    "load_validation_suppressions",
    "record_validation_phase",
    "validation_cache_key",
]
