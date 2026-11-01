"""Public contracts for independent, read-only physical inventory audits."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from pathlib import Path
import re
from typing import Any

import numpy as np

from ..validation.validation_framework import ValidationIssue

ActivityKey = tuple[str, str, str, str]


@dataclass(frozen=True)
class PhysicalContract:
    """An independently sourced bound on an indicator per reference output.

    Patterns are full regular-expression matches. ``scope`` is ``direct`` or
    ``total``. Allocation-sensitive bounds must explicitly restrict their
    system models. Contracts never change the inventory.
    """

    name: str
    indicator: str
    source: str
    name_pattern: str = ".*"
    product_pattern: str = ".*"
    unit: str = "kilogram"
    location_pattern: str = ".*"
    scope: str = "direct"
    minimum: float | None = None
    maximum: float | None = None
    rtol: float = 1e-6
    atol: float = 1e-12
    system_models: tuple[str, ...] = ()
    versions: tuple[str, ...] = ()

    def __post_init__(self):
        if not self.name or not self.source or not self.indicator:
            raise ValueError("Contracts require a name, indicator and source")
        if self.scope not in {"direct", "total"}:
            raise ValueError("Contract scope must be direct or total")
        for pattern in (self.name_pattern, self.product_pattern, self.location_pattern):
            re.compile(pattern)
        if self.minimum is None and self.maximum is None:
            raise ValueError("A contract requires at least one bound")
        for value in (self.minimum, self.maximum, self.rtol, self.atol):
            if value is not None and not math.isfinite(value):
                raise ValueError("Contract bounds and tolerances must be finite")
        if self.rtol < 0 or self.atol < 0:
            raise ValueError("Contract tolerances must be nonnegative")
        if (
            self.minimum is not None
            and self.maximum is not None
            and self.minimum > self.maximum
        ):
            raise ValueError("Contract bounds are reversed")
        object.__setattr__(self, "system_models", tuple(self.system_models))
        object.__setattr__(self, "versions", tuple(self.versions))

    def matches(self, key, system_model, version=None):
        from .graph import unit

        return (
            re.fullmatch(self.name_pattern, key[0]) is not None
            and re.fullmatch(self.product_pattern, key[1]) is not None
            and re.fullmatch(self.location_pattern, key[2]) is not None
            and unit(self.unit) == key[3]
            and (not self.system_models or system_model in self.system_models)
            and (not self.versions or version in self.versions)
        )


@dataclass(frozen=True)
class AuditConfig:
    """Sampling and numerical settings; no scientific inventory repairs."""

    sample_size: int = 5000
    seed: int = 20261101
    system_model: str | None = None
    version: str | None = None
    # A source/native control should disable Premise-specific contracts.
    include_default_contracts: bool = True
    contracts: tuple[PhysicalContract, ...] = ()
    budgets: tuple = ()
    numerical_rtol: float = 1e-7
    numerical_atol: float = 1e-12
    carbon_fraction_tolerance: float = 0.02
    comparison_percent: float = 20.0
    contribution_limit: int = 5
    explanation_limit: int = 100
    manual_review_size: int = 100
    explicit_sample: tuple[ActivityKey, ...] = ()

    def __post_init__(self):
        for name in (
            "sample_size",
            "contribution_limit",
            "explanation_limit",
            "manual_review_size",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise ValueError("seed must be an integer")
        for name in (
            "numerical_rtol",
            "numerical_atol",
            "carbon_fraction_tolerance",
            "comparison_percent",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        if self.carbon_fraction_tolerance > 1:
            raise ValueError("Carbon fraction tolerance cannot exceed one")
        if self.system_model not in (None, "cutoff", "consequential", "EN15804"):
            raise ValueError("Unknown system model")
        object.__setattr__(self, "contracts", tuple(self.contracts))
        object.__setattr__(self, "budgets", tuple(self.budgets))
        object.__setattr__(
            self, "explicit_sample", tuple(tuple(k) for k in self.explicit_sample)
        )


@dataclass(frozen=True)
class AuditFinding:
    """A standard validation issue with scientific evidence and confidence."""

    issue: ValidationIssue
    category: str
    confidence: str
    activity: ActivityKey | None = None
    indicator: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    root_cause: str | None = None

    @property
    def rule_id(self):
        return self.issue.rule_id

    def to_dict(self):
        return dict(
            self.issue.to_dict(),
            category=self.category,
            confidence=self.confidence,
            activity=list(self.activity) if self.activity else None,
            indicator=self.indicator,
            evidence=self.evidence,
            root_cause=self.root_cause,
        )


@dataclass
class AuditReport:
    """Scores, coverage and findings; an empty finding list is not certification."""

    metadata: dict
    indicators: tuple[dict, ...]
    sample: tuple[dict, ...]
    findings: tuple[AuditFinding, ...]
    coverage: tuple[dict, ...]
    rule_coverage: tuple[dict, ...]
    total: np.ndarray
    direct: np.ndarray
    positive: np.ndarray
    negative: np.ndarray
    explanations: tuple[dict, ...] = ()
    screen: tuple[dict, ...] = ()
    review_sample: tuple[dict, ...] = ()

    @property
    def issues(self):
        return tuple(f.issue for f in self.findings)

    @property
    def has_errors(self):
        return any(f.issue.severity == "error" for f in self.findings)

    def score(self, activity, indicator, scope="total"):
        """Return a sampled activity's score, preserving its reference sign."""
        i = next(
            i
            for i, row in enumerate(self.sample)
            if tuple(row["activity"]) == tuple(activity)
        )
        j = next(j for j, row in enumerate(self.indicators) if row["key"] == indicator)
        if scope not in {"total", "direct", "positive", "negative"}:
            raise ValueError("Unknown score scope")
        return float(getattr(self, scope)[i, j])

    def to_dict(self):
        return dict(
            metadata=self.metadata,
            indicators=list(self.indicators),
            sample=list(self.sample),
            findings=[f.to_dict() for f in self.findings],
            coverage=list(self.coverage),
            rule_coverage=list(self.rule_coverage),
            explanations=list(self.explanations),
            screen=list(self.screen),
            review_sample=list(self.review_sample),
        )

    def write(self, directory: str | Path):
        """Write CSV/JSON and a standalone searchable HTML report to a new directory."""
        from .report import write_report

        return write_report(self, directory)

    @classmethod
    def read(cls, directory: str | Path):
        """Reload portable audit results without opening a Brightway project."""
        import json

        path = Path(directory)
        data = json.loads((path / "audit.json").read_text(encoding="utf-8"))
        arrays = np.load(path / "scores.npz", allow_pickle=False)
        findings = []
        for record in data["findings"]:
            issue = ValidationIssue.from_dict(
                {
                    k: record[k]
                    for k in ValidationIssue.__dataclass_fields__
                    if k in record
                }
            )
            findings.append(
                AuditFinding(
                    issue,
                    record["category"],
                    record["confidence"],
                    tuple(record["activity"]) if record["activity"] else None,
                    record["indicator"],
                    record["evidence"],
                    record["root_cause"],
                )
            )
        values = [arrays[key] for key in ("total", "direct", "positive", "negative")]
        expected = len(data["sample"]), len(data["indicators"])
        if any(v.shape != expected or not np.all(np.isfinite(v)) for v in values):
            raise ValueError("Invalid or inconsistent audit score arrays")
        for v in values:
            v.setflags(write=False)
        return cls(
            data["metadata"],
            tuple(data["indicators"]),
            tuple(data["sample"]),
            tuple(findings),
            tuple(data["coverage"]),
            tuple(data["rule_coverage"]),
            *values,
            tuple(data["explanations"]),
            tuple(data["screen"]),
            tuple(data.get("review_sample", ())),
        )
