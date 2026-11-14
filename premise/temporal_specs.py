"""Validated temporal profiles and declarative dataset selectors for TRAILS.

The registry is deliberately independent from a particular inventory backend.  Its
selector operators mirror the small, stable subset of :mod:`wurst.searching` used
throughout premise, while remaining serialisable and auditable in YAML.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase
from itertools import pairwise
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = 1
TARGETS = {"activity", "supplier", "caller", "biosphere"}
TARGET_QUANTITIES = {
    "activity": {"service_life", "biomass_growth"},
    "supplier": {"construction_timing"},
    "caller": {"fleet_age", "retirement_timing"},
    "biosphere": {"long_term_release"},
}
ASSIGNMENT_KINDS = {"fallback", "proxy", "selector", "override"}
OPERATORS = {"equals", "contains", "startswith", "in", "glob"}
QUANTITIES = {
    "service_life",
    "fleet_age",
    "construction_timing",
    "retirement_timing",
    "biomass_growth",
    "long_term_release",
}
DISTRIBUTIONS = {
    "fixed",
    "uniform",
    "triangular",
    "normal",
    "lognormal",
    "discrete",
    "derived",
}
EVIDENCE_STATUSES = {
    "empirical",
    "modelled",
    "literature",
    "assumption",
    "legacy_unreviewed",
}
REVIEW_STATUSES = {"active", "eligible", "evidence_only", "hold"}
FIELD_ALIASES = {
    "reference_product": "reference product",
    "reference product": "reference product",
}


class TemporalSpecError(ValueError):
    """Raised when a temporal registry is invalid or resolves ambiguously."""


@dataclass(frozen=True)
class TemporalResolution:
    """The winning temporal-profile assignment for one inventory record."""

    assignment_id: str
    profile_id: str
    target: str
    precedence: tuple[int, int, int]
    competing_assignment_ids: tuple[str, ...] = ()


def _selector_block_matches(
    record: Mapping[str, Any],
    include: Mapping[str, Any] | None,
    exclude: Mapping[str, Any] | None = None,
) -> bool:
    """Apply one ``all``/``any`` selector pair to an inventory record."""

    include = include or {}
    include_all = include.get("all", []) or []
    include_any = include.get("any", []) or []
    if include_all and not all(predicate_matches(record, p) for p in include_all):
        return False
    if include_any and not any(predicate_matches(record, p) for p in include_any):
        return False

    exclude = exclude or {}
    exclude_all = exclude.get("all", []) or []
    exclude_any = exclude.get("any", []) or []
    if exclude_all and all(predicate_matches(record, p) for p in exclude_all):
        return False
    return not (exclude_any and any(predicate_matches(record, p) for p in exclude_any))


def profile_to_temporal_parameters(profile: Mapping[str, Any]) -> dict[str, Any]:
    """Translate a canonical profile into TRAILS exchange parameter names.

    ``derived`` distributions intentionally remain declarative because they need
    caller or scenario context (for example, maintenance over a service life).
    """

    distribution = profile["distribution"]
    kind = distribution["kind"]
    result = {
        "temporal_distribution": None,
        "temporal_loc": None,
        "temporal_scale": None,
        "temporal_offsets": None,
        "temporal_weights": None,
        "temporal_min": None,
        "temporal_max": None,
        "lifetime": profile.get("service_life_years"),
    }
    if kind == "derived":
        result["generator"] = distribution.get("generator")
        result["generator_parameters"] = distribution.get("parameters", {})
        return result
    if kind == "fixed":
        result.update(
            temporal_distribution=6,
            temporal_offsets=[float(distribution["offset_years"])],
            temporal_weights=[1.0],
        )
    elif kind == "discrete":
        result.update(
            temporal_distribution=6,
            temporal_offsets=[float(x) for x in distribution["offsets_years"]],
            temporal_weights=[float(x) for x in distribution["weights"]],
        )
    elif kind == "uniform":
        result.update(
            temporal_distribution=4,
            temporal_min=float(distribution["minimum_years"]),
            temporal_max=float(distribution["maximum_years"]),
        )
    elif kind == "triangular":
        result.update(
            temporal_distribution=5,
            temporal_loc=float(distribution["mode_years"]),
            temporal_min=float(distribution["minimum_years"]),
            temporal_max=float(distribution["maximum_years"]),
        )
    elif kind in {"normal", "lognormal"}:
        result.update(
            temporal_distribution=3 if kind == "normal" else 2,
            temporal_loc=float(distribution["loc"]),
            temporal_scale=float(distribution["scale"]),
        )
        if "minimum_years" in distribution:
            result["temporal_min"] = float(distribution["minimum_years"])
        if "maximum_years" in distribution:
            result["temporal_max"] = float(distribution["maximum_years"])
    return result


def _load_yaml(path: Path, collection: str) -> Any:
    if not path.exists():
        raise TemporalSpecError(f"Missing temporal registry file: {path}")
    with path.open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream) or {}
    if not isinstance(document, dict):
        raise TemporalSpecError(f"{path} must contain a YAML mapping.")
    if document.get("schema_version") != SCHEMA_VERSION:
        raise TemporalSpecError(
            f"{path} must declare schema_version: {SCHEMA_VERSION}."
        )
    value = document.get(collection)
    if value is None:
        raise TemporalSpecError(f"{path} is missing the {collection!r} collection.")
    return value


def _as_list(value: Any, label: str) -> list:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TemporalSpecError(f"{label} must be a list.")
    return value


def _normalise_versions(value: Any, label: str) -> tuple[str, ...]:
    versions = _as_list(value, label)
    if any(not isinstance(item, (str, int, float)) for item in versions):
        raise TemporalSpecError(f"{label} must contain scalar version identifiers.")
    return tuple(str(item) for item in versions)


def _normalise_scalar(value: Any, *, case_sensitive: bool) -> Any:
    if isinstance(value, str) and not case_sensitive:
        return value.casefold()
    return value


def _record_value(record: Mapping[str, Any], field: str) -> Any:
    field = FIELD_ALIASES.get(field, field)
    if field == "classification":
        values = []
        for item in record.get("classifications", ()) or ():
            if isinstance(item, (list, tuple)):
                values.extend(str(value) for value in item)
            else:
                values.append(str(item))
        return tuple(values)
    return record.get(field)


def predicate_matches(record: Mapping[str, Any], predicate: Mapping[str, Any]) -> bool:
    """Return whether an inventory record satisfies one validated predicate."""

    field = predicate["field"]
    operator = predicate["operator"]
    expected = predicate["value"]
    case_sensitive = predicate.get("case_sensitive", True)
    actual = _record_value(record, field)

    if operator == "in":
        expected_values = expected if isinstance(expected, list) else [expected]
        if isinstance(actual, (list, tuple, set)):
            actual_values = actual
        else:
            actual_values = [actual]
        expected_values = {
            _normalise_scalar(value, case_sensitive=case_sensitive)
            for value in expected_values
        }
        return any(
            _normalise_scalar(value, case_sensitive=case_sensitive) in expected_values
            for value in actual_values
        )

    if isinstance(actual, (list, tuple, set)):
        return any(
            _scalar_predicate_matches(
                value,
                expected,
                operator=operator,
                case_sensitive=case_sensitive,
            )
            for value in actual
        )
    if actual is None:
        return False

    return _scalar_predicate_matches(
        actual,
        expected,
        operator=operator,
        case_sensitive=case_sensitive,
    )


def _scalar_predicate_matches(
    actual: Any,
    expected: Any,
    *,
    operator: str,
    case_sensitive: bool,
) -> bool:
    """Apply a non-collection predicate to one value."""

    actual = _normalise_scalar(actual, case_sensitive=case_sensitive)
    expected = _normalise_scalar(expected, case_sensitive=case_sensitive)
    if operator == "equals":
        return actual == expected
    if not isinstance(actual, str) or not isinstance(expected, str):
        return False
    if operator == "contains":
        return expected in actual
    if operator == "startswith":
        return actual.startswith(expected)
    if operator == "glob":
        return fnmatchcase(actual, expected)
    raise TemporalSpecError(f"Unsupported selector operator: {operator!r}")


def selector_matches(
    record: Mapping[str, Any],
    assignment: Mapping[str, Any],
    *,
    caller_record: Mapping[str, Any] | None = None,
) -> bool:
    """Apply target and optional caller predicates for one assignment."""

    match = assignment.get("exchange_match") or assignment.get("match") or {}
    if (
        not any(match.get(group) for group in ("all", "any"))
        and assignment["kind"] != "fallback"
    ):
        return False
    if not _selector_block_matches(record, match, assignment.get("exclude")):
        return False
    caller_match = assignment.get("caller_match")
    if caller_match is None:
        return True
    return caller_record is not None and _selector_block_matches(
        caller_record,
        caller_match,
        assignment.get("caller_exclude"),
    )


def _validate_predicate(predicate: Any, label: str) -> None:
    if not isinstance(predicate, dict):
        raise TemporalSpecError(f"{label} must be a mapping.")
    missing = {"field", "operator", "value"} - set(predicate)
    if missing:
        raise TemporalSpecError(f"{label} is missing fields: {sorted(missing)}")
    if not isinstance(predicate["field"], str) or not predicate["field"].strip():
        raise TemporalSpecError(f"{label}.field must be a nonempty string.")
    if predicate["operator"] not in OPERATORS:
        raise TemporalSpecError(f"{label}.operator must be one of {sorted(OPERATORS)}.")
    if predicate["operator"] == "in" and not isinstance(predicate["value"], list):
        raise TemporalSpecError(f"{label}.value must be a list for operator 'in'.")
    if "case_sensitive" in predicate and not isinstance(
        predicate["case_sensitive"], bool
    ):
        raise TemporalSpecError(f"{label}.case_sensitive must be boolean.")


def _validate_selector_block(value: Any, label: str) -> None:
    if value is None:
        return
    if not isinstance(value, dict) or set(value) - {"all", "any"}:
        raise TemporalSpecError(f"{label} may contain only 'all' and 'any' lists.")
    for group in ("all", "any"):
        for index, predicate in enumerate(
            _as_list(value.get(group), f"{label}.{group}")
        ):
            _validate_predicate(predicate, f"{label}.{group}[{index}]")


def _validate_sources(sources: Any) -> dict[str, dict]:
    if not isinstance(sources, dict):
        raise TemporalSpecError("sources must be a mapping keyed by source ID.")
    for source_id, source in sources.items():
        if not isinstance(source_id, str) or not source_id.strip():
            raise TemporalSpecError("Every source ID must be a nonempty string.")
        if not isinstance(source, dict) or not source.get("title"):
            raise TemporalSpecError(f"Source {source_id!r} requires a title.")
        if not source.get("uri") and not source.get("citation"):
            raise TemporalSpecError(
                f"Source {source_id!r} requires either uri or citation."
            )
    return sources


def _validate_profiles(profiles: Any, sources: Mapping[str, dict]) -> dict[str, dict]:
    if not isinstance(profiles, dict):
        raise TemporalSpecError("profiles must be a mapping keyed by profile ID.")
    for profile_id, profile in profiles.items():
        label = f"Profile {profile_id!r}"
        if not isinstance(profile, dict):
            raise TemporalSpecError(f"{label} must be a mapping.")
        if profile.get("quantity") not in QUANTITIES:
            raise TemporalSpecError(
                f"{label}.quantity must be one of {sorted(QUANTITIES)}."
            )
        if (
            not isinstance(profile.get("reference_event"), str)
            or not profile["reference_event"].strip()
        ):
            raise TemporalSpecError(f"{label} requires a reference_event.")
        if "service_life_years" in profile:
            lifetime = float(profile["service_life_years"])
            if not math.isfinite(lifetime) or lifetime <= 0:
                raise TemporalSpecError(
                    f"{label}.service_life_years must be finite and positive."
                )
        distribution = profile.get("distribution")
        if not isinstance(distribution, dict):
            raise TemporalSpecError(f"{label} requires a distribution mapping.")
        if distribution.get("kind") not in DISTRIBUTIONS:
            raise TemporalSpecError(
                f"{label}.distribution.kind must be one of {sorted(DISTRIBUTIONS)}."
            )
        kind = distribution["kind"]
        required_parameters = {
            "fixed": {"offset_years"},
            "uniform": {"minimum_years", "maximum_years"},
            "triangular": {"minimum_years", "mode_years", "maximum_years"},
            "normal": {"loc", "scale"},
            "lognormal": {"loc", "scale"},
            "derived": {"generator"},
        }.get(kind, set())
        missing_parameters = required_parameters - set(distribution)
        if missing_parameters:
            raise TemporalSpecError(
                f"{label} {kind} distribution is missing {sorted(missing_parameters)}."
            )
        if kind == "discrete":
            offsets = distribution.get("offsets_years")
            weights = distribution.get("weights")
            if not isinstance(offsets, list) or not isinstance(weights, list):
                raise TemporalSpecError(
                    f"{label} discrete distributions require offsets_years and weights."
                )
            if not offsets or len(offsets) != len(weights):
                raise TemporalSpecError(
                    f"{label} discrete offsets and weights must be nonempty and aligned."
                )
            numeric_offsets = [float(offset) for offset in offsets]
            numeric_weights = [float(weight) for weight in weights]
            if not all(math.isfinite(offset) for offset in numeric_offsets):
                raise TemporalSpecError(f"{label} offsets must be finite.")
            if not all(math.isfinite(weight) for weight in numeric_weights):
                raise TemporalSpecError(f"{label} weights must be finite.")
            if (
                any(weight < 0 for weight in numeric_weights)
                or sum(numeric_weights) <= 0
            ):
                raise TemporalSpecError(f"{label} weights must have positive mass.")
            if not math.isclose(sum(numeric_weights), 1.0, rel_tol=1e-9, abs_tol=1e-12):
                raise TemporalSpecError(f"{label} weights must sum to one.")
        if kind == "derived":
            generator = distribution["generator"]
            parameters = distribution.get("parameters", {})
            if not isinstance(generator, str) or not generator.strip():
                raise TemporalSpecError(f"{label} requires a named generator.")
            if not isinstance(parameters, dict):
                raise TemporalSpecError(
                    f"{label}.distribution.parameters must be a mapping."
                )
            if generator == "uniform_within_reported_age_bands":
                bands = parameters.get("bands")
                if not isinstance(bands, list) or not bands:
                    raise TemporalSpecError(f"{label} requires reported age bands.")
                band_weights = []
                for index, band in enumerate(bands):
                    if not isinstance(band, dict):
                        raise TemporalSpecError(
                            f"{label} age band {index} must be a mapping."
                        )
                    minimum = float(band["minimum_years"])
                    maximum = band.get("maximum_years")
                    weight = float(band["weight"])
                    if not math.isfinite(minimum) or minimum < 0:
                        raise TemporalSpecError(
                            f"{label} age-band minima must be finite and nonnegative."
                        )
                    if maximum is not None and (
                        not math.isfinite(float(maximum)) or float(maximum) <= minimum
                    ):
                        raise TemporalSpecError(
                            f"{label} age-band maxima must exceed their minima."
                        )
                    if not math.isfinite(weight) or weight < 0:
                        raise TemporalSpecError(
                            f"{label} age-band weights must be finite and nonnegative."
                        )
                    band_weights.append(weight)
                if not math.isclose(
                    sum(band_weights), 1.0, rel_tol=1e-9, abs_tol=1e-12
                ):
                    raise TemporalSpecError(
                        f"{label} age-band weights must sum to one."
                    )
            if generator == "right_censored_survival_curve":
                ages = parameters.get("ages_years")
                survival = parameters.get("survival_probabilities")
                if (
                    not isinstance(ages, list)
                    or not isinstance(survival, list)
                    or not ages
                    or len(ages) != len(survival)
                ):
                    raise TemporalSpecError(
                        f"{label} survival ages and probabilities must align."
                    )
                ages = [float(value) for value in ages]
                survival = [float(value) for value in survival]
                if any(not math.isfinite(value) for value in ages + survival):
                    raise TemporalSpecError(
                        f"{label} survival parameters must be finite."
                    )
                if any(age <= 0 for age in ages) or any(
                    right <= left for left, right in pairwise(ages)
                ):
                    raise TemporalSpecError(
                        f"{label} survival ages must be positive and increasing."
                    )
                if any(not 0 <= value <= 1 for value in survival) or any(
                    right > left for left, right in pairwise(survival)
                ):
                    raise TemporalSpecError(
                        f"{label} survival probabilities must be nonincreasing in [0, 1]."
                    )
                censor = float(parameters.get("right_censored_after_years", ages[-1]))
                if not math.isfinite(censor) or censor < ages[-1]:
                    raise TemporalSpecError(
                        f"{label} censoring age cannot precede the final observation."
                    )
        if kind in {"uniform", "triangular"}:
            minimum = float(distribution["minimum_years"])
            maximum = float(distribution["maximum_years"])
            if not minimum < maximum:
                raise TemporalSpecError(f"{label} requires minimum < maximum.")
            if (
                kind == "triangular"
                and not minimum <= float(distribution["mode_years"]) <= maximum
            ):
                raise TemporalSpecError(
                    f"{label} triangular mode must lie within its bounds."
                )
        assumptions = profile.get("assumptions")
        if not isinstance(assumptions, list):
            raise TemporalSpecError(f"{label}.assumptions must be a list.")
        evidence = profile.get("evidence")
        if not isinstance(evidence, dict):
            raise TemporalSpecError(f"{label} requires an evidence mapping.")
        if evidence.get("status") not in EVIDENCE_STATUSES:
            raise TemporalSpecError(
                f"{label}.evidence.status must be one of {sorted(EVIDENCE_STATUSES)}."
            )
        for source_id in _as_list(evidence.get("source_ids"), f"{label}.source_ids"):
            if source_id not in sources:
                raise TemporalSpecError(
                    f"{label} references unknown source {source_id!r}."
                )
        coverage = evidence.get("coverage", {})
        if not isinstance(coverage, dict):
            raise TemporalSpecError(f"{label}.evidence.coverage must be a mapping.")
        quality_flags = evidence.get("quality_flags", [])
        if not isinstance(quality_flags, list) or any(
            not isinstance(flag, str) or not flag.strip() for flag in quality_flags
        ):
            raise TemporalSpecError(
                f"{label}.evidence.quality_flags must contain nonempty strings."
            )
        if len(quality_flags) != len(set(quality_flags)):
            raise TemporalSpecError(
                f"{label}.evidence.quality_flags must not contain duplicates."
            )
        if evidence["status"] == "empirical":
            for field in ("population", "weighting", "reference_period", "censoring"):
                if (
                    not isinstance(evidence.get(field), str)
                    or not evidence[field].strip()
                ):
                    raise TemporalSpecError(
                        f"{label}.evidence.{field} is required for empirical profiles."
                    )
            sample = evidence.get("sample_size")
            if not isinstance(sample, dict) or not isinstance(sample.get("unit"), str):
                raise TemporalSpecError(
                    f"{label}.evidence.sample_size requires a unit and either count or not_reported."
                )
            if "count" in sample:
                count = sample["count"]
                if (
                    not isinstance(count, (int, float))
                    or not math.isfinite(count)
                    or count <= 0
                ):
                    raise TemporalSpecError(
                        f"{label}.evidence.sample_size.count must be finite and positive."
                    )
                threshold = sample.get("minimum_for_generalisation")
                if threshold is not None:
                    if (
                        not isinstance(threshold, (int, float))
                        or not math.isfinite(threshold)
                        or threshold <= 0
                    ):
                        raise TemporalSpecError(
                            f"{label}.evidence.sample_size.minimum_for_generalisation "
                            "must be finite and positive."
                        )
                    flagged = "small-sample" in quality_flags
                    if flagged != (count < threshold):
                        raise TemporalSpecError(
                            f"{label}.evidence quality flag 'small-sample' must agree "
                            "with count and minimum_for_generalisation."
                        )
            elif (
                not isinstance(sample.get("not_reported"), str)
                or not sample["not_reported"].strip()
            ):
                raise TemporalSpecError(
                    f"{label}.evidence.sample_size requires count or not_reported."
                )
        review = profile.get("review")
        if not isinstance(review, dict):
            raise TemporalSpecError(f"{label} requires a review mapping.")
        if review.get("activation_status") not in REVIEW_STATUSES:
            raise TemporalSpecError(
                f"{label}.review.activation_status must be one of {sorted(REVIEW_STATUSES)}."
            )
        if (
            not isinstance(review.get("rationale"), str)
            or not review["rationale"].strip()
        ):
            raise TemporalSpecError(f"{label}.review requires a rationale.")
    return profiles


def _validate_assignments(assignments: Any, profiles: Mapping[str, dict]) -> list[dict]:
    if not isinstance(assignments, list):
        raise TemporalSpecError("assignments must be a list.")
    seen = set()
    for index, assignment in enumerate(assignments):
        label = f"Assignment #{index + 1}"
        if not isinstance(assignment, dict):
            raise TemporalSpecError(f"{label} must be a mapping.")
        assignment_id = assignment.get("id")
        if not isinstance(assignment_id, str) or not assignment_id.strip():
            raise TemporalSpecError(f"{label} requires a nonempty ID.")
        if assignment_id in seen:
            raise TemporalSpecError(f"Duplicate assignment ID: {assignment_id!r}")
        seen.add(assignment_id)
        if assignment.get("profile") not in profiles:
            raise TemporalSpecError(
                f"Assignment {assignment_id!r} references unknown profile "
                f"{assignment.get('profile')!r}."
            )
        if profiles[assignment["profile"]]["review"]["activation_status"] != "active":
            raise TemporalSpecError(
                f"Assignment {assignment_id!r} references profile {assignment['profile']!r}, "
                "which is not approved for active assignment."
            )
        if assignment.get("target") not in TARGETS:
            raise TemporalSpecError(
                f"Assignment {assignment_id!r}.target must be one of {sorted(TARGETS)}."
            )
        target = assignment["target"]
        quantity = profiles[assignment["profile"]]["quantity"]
        if quantity not in TARGET_QUANTITIES[target]:
            raise TemporalSpecError(
                f"Assignment {assignment_id!r} cannot apply {quantity!r} to "
                f"target {target!r}; expected one of {sorted(TARGET_QUANTITIES[target])}."
            )
        if assignment.get("kind", "selector") not in ASSIGNMENT_KINDS:
            raise TemporalSpecError(
                f"Assignment {assignment_id!r}.kind must be one of "
                f"{sorted(ASSIGNMENT_KINDS)}."
            )
        assignment.setdefault("kind", "selector")
        priority = assignment.get("priority", 0)
        if not isinstance(priority, int):
            raise TemporalSpecError(
                f"Assignment {assignment_id!r}.priority must be an integer."
            )
        assignment.setdefault("priority", 0)
        _validate_selector_block(assignment.get("match"), f"{label}.match")
        _validate_selector_block(
            assignment.get("exchange_match"), f"{label}.exchange_match"
        )
        _validate_selector_block(assignment.get("exclude"), f"{label}.exclude")
        _validate_selector_block(
            assignment.get("caller_match"), f"{label}.caller_match"
        )
        _validate_selector_block(
            assignment.get("caller_exclude"), f"{label}.caller_exclude"
        )
        if assignment.get("match") and assignment.get("exchange_match"):
            raise TemporalSpecError(
                f"Assignment {assignment_id!r} cannot define both match and exchange_match."
            )
        if assignment["kind"] != "fallback" and not (
            assignment.get("match") or assignment.get("exchange_match")
        ):
            raise TemporalSpecError(
                f"Assignment {assignment_id!r} requires match or exchange_match predicates."
            )
        if assignment.get("caller_match") and target not in {"supplier", "biosphere"}:
            raise TemporalSpecError(
                f"Assignment {assignment_id!r}.caller_match requires supplier or biosphere target."
            )
        if assignment.get("caller_exclude") and not assignment.get("caller_match"):
            raise TemporalSpecError(
                f"Assignment {assignment_id!r}.caller_exclude requires caller_match."
            )
        applicability = assignment.get("applicability", {}) or {}
        if not isinstance(applicability, dict):
            raise TemporalSpecError(
                f"Assignment {assignment_id!r}.applicability must be a mapping."
            )
        _normalise_versions(
            applicability.get("ecoinvent_versions"),
            f"Assignment {assignment_id!r}.applicability.ecoinvent_versions",
        )
        bounds = assignment.get("expected_matches", {}) or {}
        if not isinstance(bounds, dict) or set(bounds) - {"minimum", "maximum"}:
            raise TemporalSpecError(
                f"Assignment {assignment_id!r}.expected_matches accepts only minimum/maximum."
            )
        minimum, maximum = bounds.get("minimum", 0), bounds.get("maximum")
        if not isinstance(minimum, int) or minimum < 0:
            raise TemporalSpecError(
                f"Assignment {assignment_id!r} minimum match count must be nonnegative."
            )
        if maximum is not None and (not isinstance(maximum, int) or maximum < minimum):
            raise TemporalSpecError(
                f"Assignment {assignment_id!r} maximum must be >= minimum."
            )
    return assignments


class TemporalRegistry:
    """Validated temporal sources, profiles, and dataset assignments."""

    def __init__(
        self,
        sources: dict[str, dict],
        profiles: dict[str, dict],
        assignments: list[dict],
    ) -> None:
        self.sources = _validate_sources(sources)
        self.profiles = _validate_profiles(profiles, self.sources)
        self.assignments = _validate_assignments(assignments, self.profiles)

    @classmethod
    def from_directory(cls, directory: str | Path) -> TemporalRegistry:
        directory = Path(directory)
        return cls(
            sources=_load_yaml(directory / "sources.yaml", "sources"),
            profiles=_load_yaml(directory / "profiles.yaml", "profiles"),
            assignments=_load_yaml(directory / "assignments.yaml", "assignments"),
        )

    @staticmethod
    def _applicable(assignment: Mapping[str, Any], context: Mapping[str, Any]) -> bool:
        applicability = assignment.get("applicability", {}) or {}
        versions = _normalise_versions(
            applicability.get("ecoinvent_versions"), "ecoinvent_versions"
        )
        if versions and str(context.get("ecoinvent_version")) not in versions:
            return False
        for field in ("years", "geographies"):
            allowed = applicability.get(field) or []
            context_field = "year" if field == "years" else "geography"
            if allowed and context.get(context_field) not in allowed:
                return False
        return True

    @staticmethod
    def _precedence(assignment: Mapping[str, Any]) -> tuple[int, int, int]:
        kind = assignment["kind"]
        if kind == "override":
            tier = 4
        elif kind == "selector" and (assignment.get("applicability", {}) or {}).get(
            "ecoinvent_versions"
        ):
            tier = 3
        elif kind == "selector":
            tier = 2
        elif kind == "proxy":
            tier = 1
        else:
            tier = 0
        predicates = sum(
            len((assignment.get(block, {}) or {}).get(group, []) or [])
            for block in (
                "match",
                "exchange_match",
                "exclude",
                "caller_match",
                "caller_exclude",
            )
            for group in ("all", "any")
        )
        return tier, int(assignment.get("priority", 0)), predicates

    def matching_assignments(
        self,
        record: Mapping[str, Any],
        *,
        target: str,
        context: Mapping[str, Any] | None = None,
        caller_record: Mapping[str, Any] | None = None,
    ) -> list[dict]:
        if target not in TARGETS:
            raise TemporalSpecError(f"Unknown temporal assignment target: {target!r}")
        context = context or {}
        return [
            assignment
            for assignment in self.assignments
            if assignment["target"] == target
            and self._applicable(assignment, context)
            and selector_matches(record, assignment, caller_record=caller_record)
        ]

    def matching_caller_assignments(
        self,
        caller_record: Mapping[str, Any],
        *,
        target: str,
        context: Mapping[str, Any] | None = None,
    ) -> list[dict]:
        """Return relationship assignments whose caller side is applicable."""

        context = context or {}
        return [
            assignment
            for assignment in self.assignments
            if assignment["target"] == target
            and assignment.get("caller_match")
            and self._applicable(assignment, context)
            and _selector_block_matches(
                caller_record,
                assignment["caller_match"],
                assignment.get("caller_exclude"),
            )
        ]

    def resolve(
        self,
        record: Mapping[str, Any],
        *,
        target: str,
        context: Mapping[str, Any] | None = None,
        caller_record: Mapping[str, Any] | None = None,
    ) -> TemporalResolution | None:
        """Resolve one record, rejecting equally authoritative conflicts."""

        matches = self.matching_assignments(
            record,
            target=target,
            context=context,
            caller_record=caller_record,
        )
        if not matches:
            return None
        winning_precedence = max(self._precedence(item) for item in matches)
        winners = [
            item for item in matches if self._precedence(item) == winning_precedence
        ]
        profile_ids = {item["profile"] for item in winners}
        if len(profile_ids) > 1:
            identity = (
                record.get("name", ""),
                record.get("reference product", record.get("reference_product", "")),
            )
            details = ", ".join(
                f"{item['id']}->{item['profile']}"
                for item in sorted(winners, key=lambda x: x["id"])
            )
            raise TemporalSpecError(
                f"Conflicting temporal selectors for {identity}: {details}"
            )
        winner = min(winners, key=lambda item: item["id"])
        return TemporalResolution(
            assignment_id=winner["id"],
            profile_id=winner["profile"],
            target=target,
            precedence=winning_precedence,
            competing_assignment_ids=tuple(
                sorted(item["id"] for item in matches if item is not winner)
            ),
        )

    def audit(
        self,
        records: Iterable[Mapping[str, Any]],
        *,
        target: str,
        context: Mapping[str, Any] | None = None,
        enforce_expected_matches: bool = True,
    ) -> dict[str, Any]:
        """Return raw/selected match counts and uncovered inventory identities."""

        context = context or {}
        raw_counts: Counter[str] = Counter()
        selected_counts: Counter[str] = Counter()
        uncovered = []
        resolutions = []
        records = list(records)
        for record in records:
            for assignment in self.matching_assignments(
                record, target=target, context=context
            ):
                raw_counts[assignment["id"]] += 1
            resolution = self.resolve(record, target=target, context=context)
            if resolution is None:
                uncovered.append(
                    {
                        "name": record.get("name", ""),
                        "reference product": record.get(
                            "reference product", record.get("reference_product", "")
                        ),
                        "unit": record.get("unit", ""),
                        "location": record.get("location", ""),
                    }
                )
            else:
                selected_counts[resolution.assignment_id] += 1
                resolutions.append(resolution)

        violations = []
        for assignment in self.assignments:
            if assignment["target"] != target or not self._applicable(
                assignment, context
            ):
                continue
            bounds = assignment.get("expected_matches", {}) or {}
            count = selected_counts[assignment["id"]]
            minimum, maximum = bounds.get("minimum", 0), bounds.get("maximum")
            if count < minimum or (maximum is not None and count > maximum):
                violations.append(
                    {
                        "assignment_id": assignment["id"],
                        "selected_matches": count,
                        "minimum": minimum,
                        "maximum": maximum,
                    }
                )
        if enforce_expected_matches and violations:
            raise TemporalSpecError(
                "Temporal selector match-count violations: " + repr(violations)
            )
        return {
            "records": len(records),
            "covered": len(resolutions),
            "uncovered": uncovered,
            "raw_match_counts": dict(raw_counts),
            "selected_match_counts": dict(selected_counts),
            "match_count_violations": violations,
        }
