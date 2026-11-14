"""Canonical legacy mappings and deterministic TRAILS CSV compilation.

The legacy CSV remains a compatibility artifact for existing consumers.  Its
authoritative inputs are the reviewed YAML profiles and exact dataset mappings
in :mod:`premise.data.trails.temporal`.
"""

from __future__ import annotations

import csv
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from .temporal_specs import SCHEMA_VERSION, TemporalSpecError

LEGACY_COLUMNS = (
    "name",
    "reference product",
    "ISIC rev.4 ecoinvent",
    "CPC",
    "EcoSpold01Categories",
    "temporal_tag",
    "tag_confidence",
    "tag_notes",
    "lifetime",
    "age distribution type",
    "loc",
    "scale",
    "offsets",
    "weights",
    "minimum",
    "maximum",
    "param_confidence",
    "param_notes",
    "timing_reference",
    "fleet_lifetime",
    "fleet_age_offsets",
    "fleet_age_weights",
    "timing_reference_notes",
)


def _load_collection(path: Path, collection: str) -> Any:
    if not path.exists():
        raise TemporalSpecError(f"Missing canonical temporal file: {path}")
    with path.open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream) or {}
    if not isinstance(document, dict):
        raise TemporalSpecError(f"{path} must contain a YAML mapping.")
    if document.get("schema_version") != SCHEMA_VERSION:
        raise TemporalSpecError(
            f"{path} must declare schema_version: {SCHEMA_VERSION}."
        )
    if collection not in document:
        raise TemporalSpecError(f"{path} is missing {collection!r}.")
    return document[collection]


def _validate_number(value: Any, label: str, *, positive: bool = False) -> None:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise TemporalSpecError(f"{label} must be numeric.") from error
    if not math.isfinite(number) or (positive and number <= 0):
        qualifier = "finite and positive" if positive else "finite"
        raise TemporalSpecError(f"{label} must be {qualifier}.")


def load_legacy_registry(directory: str | Path) -> tuple[dict, list[dict]]:
    """Load and validate canonical profiles and exact dataset mappings."""

    directory = Path(directory)
    profiles = _load_collection(directory / "legacy_profiles.yaml", "profiles")
    mappings = _load_collection(directory / "legacy_mappings.yaml", "mappings")
    if not isinstance(profiles, dict):
        raise TemporalSpecError("legacy profiles must be keyed by profile ID.")
    if not isinstance(mappings, list):
        raise TemporalSpecError("legacy mappings must be a list.")

    for profile_id, profile in profiles.items():
        label = f"Legacy profile {profile_id!r}"
        if not isinstance(profile_id, str) or not profile_id:
            raise TemporalSpecError("Legacy profile IDs must be nonempty strings.")
        if not isinstance(profile, dict):
            raise TemporalSpecError(f"{label} must be a mapping.")
        if not isinstance(profile.get("temporal_tag", ""), str):
            raise TemporalSpecError(f"{label}.temporal_tag must be a string.")
        if not isinstance(profile.get("timing_reference"), str):
            raise TemporalSpecError(f"{label} requires timing_reference.")
        if profile.get("lifetime_years") is not None:
            _validate_number(
                profile["lifetime_years"], f"{label}.lifetime_years", positive=True
            )
        distribution = profile.get("distribution")
        if distribution is not None:
            if not isinstance(distribution, dict):
                raise TemporalSpecError(f"{label}.distribution must be a mapping.")
            try:
                distribution_type = int(distribution["type"])
            except (KeyError, TypeError, ValueError) as error:
                raise TemporalSpecError(
                    f"{label}.distribution requires an integer type."
                ) from error
            if distribution_type not in {2, 3, 4, 5, 6}:
                raise TemporalSpecError(
                    f"{label}.distribution.type must be between 2 and 6."
                )
            offsets = distribution.get("offsets_years")
            weights = distribution.get("weights")
            if distribution_type == 6:
                if not isinstance(offsets, list) or not isinstance(weights, list):
                    # Named long-term profiles are expanded by the legacy loader.
                    assumptions = profile.get("parameter_assumptions", [])
                    if not any("profile:" in str(item) for item in assumptions):
                        raise TemporalSpecError(
                            f"{label} discrete data require offsets and weights."
                        )
                elif not offsets or len(offsets) != len(weights):
                    raise TemporalSpecError(
                        f"{label} discrete offsets and weights must align."
                    )
        fleet = profile.get("fleet_age")
        if fleet is not None:
            if not isinstance(fleet, dict):
                raise TemporalSpecError(f"{label}.fleet_age must be a mapping.")
            offsets = fleet.get("offsets_years")
            weights = fleet.get("weights")
            if not isinstance(offsets, list) or not isinstance(weights, list):
                raise TemporalSpecError(
                    f"{label}.fleet_age requires offsets_years and weights."
                )
            if not offsets or len(offsets) != len(weights):
                raise TemporalSpecError(
                    f"{label}.fleet_age offsets and weights must align."
                )

    mapping_ids = set()
    identities: dict[tuple[str, str], str] = {}
    for index, mapping in enumerate(mappings):
        label = f"Legacy mapping {index}"
        if not isinstance(mapping, dict):
            raise TemporalSpecError(f"{label} must be a mapping.")
        mapping_id = mapping.get("id")
        if not isinstance(mapping_id, str) or not mapping_id:
            raise TemporalSpecError(f"{label} requires a nonempty ID.")
        if mapping_id in mapping_ids:
            raise TemporalSpecError(f"Duplicate legacy mapping ID {mapping_id!r}.")
        mapping_ids.add(mapping_id)
        if mapping.get("profile") not in profiles:
            raise TemporalSpecError(
                f"Mapping {mapping_id!r} references unknown profile "
                f"{mapping.get('profile')!r}."
            )
        classification = mapping.get("classification", {})
        if not isinstance(classification, dict):
            raise TemporalSpecError(
                f"Mapping {mapping_id!r}.classification must be a mapping."
            )
        datasets = mapping.get("datasets")
        if not isinstance(datasets, list) or not datasets:
            raise TemporalSpecError(
                f"Mapping {mapping_id!r} requires a nonempty datasets list."
            )
        for dataset in datasets:
            if (
                not isinstance(dataset, list)
                or len(dataset) != 2
                or not all(isinstance(value, str) and value for value in dataset)
            ):
                raise TemporalSpecError(
                    f"Mapping {mapping_id!r} datasets must be [name, reference product]."
                )
            identity = tuple(dataset)
            if identity in identities:
                raise TemporalSpecError(
                    f"Dataset identity {identity!r} occurs in mappings "
                    f"{identities[identity]!r} and {mapping_id!r}."
                )
            identities[identity] = mapping_id
    return profiles, mappings


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return repr(value)
    if isinstance(value, float):
        return format(value, ".15g")
    return str(value)


def compile_legacy_rows(directory: str | Path) -> list[dict[str, str]]:
    """Return deterministic compatibility rows from the canonical YAML files."""

    profiles, mappings = load_legacy_registry(directory)
    rows = []
    for mapping in sorted(mappings, key=lambda item: item["id"]):
        profile = profiles[mapping["profile"]]
        classification = mapping.get("classification", {})
        distribution: Mapping[str, Any] = profile.get("distribution") or {}
        fleet: Mapping[str, Any] = profile.get("fleet_age") or {}
        assumptions = profile.get("parameter_assumptions", [])
        if not isinstance(assumptions, list):
            assumptions = [assumptions]
        for name, reference_product in sorted(mapping["datasets"]):
            row = {column: "" for column in LEGACY_COLUMNS}
            row.update(
                {
                    "name": name,
                    "reference product": reference_product,
                    "ISIC rev.4 ecoinvent": _csv_value(classification.get("isic")),
                    "CPC": _csv_value(classification.get("cpc")),
                    "EcoSpold01Categories": _csv_value(
                        classification.get("ecospold_category")
                    ),
                    "temporal_tag": profile.get("temporal_tag", ""),
                    "tag_confidence": _csv_value(mapping.get("tag_confidence")),
                    "tag_notes": f"Canonical exact mapping: {mapping['id']}",
                    "lifetime": _csv_value(profile.get("lifetime_years")),
                    "age distribution type": _csv_value(distribution.get("type")),
                    "loc": _csv_value(distribution.get("loc")),
                    "scale": _csv_value(distribution.get("scale")),
                    "offsets": _csv_value(distribution.get("offsets_years")),
                    "weights": _csv_value(distribution.get("weights")),
                    "minimum": _csv_value(distribution.get("minimum_years")),
                    "maximum": _csv_value(distribution.get("maximum_years")),
                    "param_confidence": _csv_value(profile.get("parameter_confidence")),
                    "param_notes": "; ".join(str(item) for item in assumptions),
                    "timing_reference": profile["timing_reference"],
                    "fleet_lifetime": _csv_value(fleet.get("service_life_years")),
                    "fleet_age_offsets": _csv_value(fleet.get("offsets_years")),
                    "fleet_age_weights": _csv_value(fleet.get("weights")),
                    "timing_reference_notes": profile.get(
                        "timing_reference_assumption", ""
                    ),
                }
            )
            rows.append(row)
    return rows


def write_legacy_csv(directory: str | Path, output: str | Path) -> None:
    """Compile the backward-compatible CSV deterministically."""

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=LEGACY_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(compile_legacy_rows(directory))
