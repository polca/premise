"""One-time migration of the TRAILS temporal CSV to canonical YAML.

This script is retained to make the conflict-resolution and grouping procedure
auditable.  After migration, edit the YAML registry and use
``compile_trails_temporal_csv.py``; do not regenerate YAML from the CSV.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

OPERATIONAL_FIELDS = (
    "temporal_tag",
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
CLASSIFICATION_FIELDS = (
    "ISIC rev.4 ecoinvent",
    "CPC",
    "EcoSpold01Categories",
)


def clean(value: Any) -> str:
    return str(value or "").strip()


def number(value: Any) -> float | None:
    value = clean(value)
    return float(value) if value else None


def number_list(value: Any) -> list[float] | None:
    value = clean(value)
    if not value:
        return None
    try:
        parsed = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        parsed = [item.strip() for item in re.split(r"[|;,]", value) if item.strip()]
    if not isinstance(parsed, (list, tuple)):
        raise TypeError(f"Expected a numeric list, got {value!r}.")
    return [float(item) for item in parsed]


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def fingerprint(value: Any, length: int = 10) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:length]


def profile_from_row(row: dict[str, str]) -> dict[str, Any]:
    distribution_type = number(row["age distribution type"])
    distribution = None
    if distribution_type is not None:
        distribution = {
            key: value
            for key, value in {
                "type": int(distribution_type),
                "loc": number(row["loc"]),
                "scale": number(row["scale"]),
                "offsets_years": number_list(row["offsets"]),
                "weights": number_list(row["weights"]),
                "minimum_years": number(row["minimum"]),
                "maximum_years": number(row["maximum"]),
            }.items()
            if value is not None
        }
    fleet = None
    if clean(row["fleet_age_offsets"]):
        fleet = {
            "service_life_years": number(row["fleet_lifetime"]),
            "offsets_years": number_list(row["fleet_age_offsets"]),
            "weights": number_list(row["fleet_age_weights"]),
        }
    profile = {
        "temporal_tag": clean(row["temporal_tag"]),
        "timing_reference": clean(row["timing_reference"]),
        "timing_reference_assumption": clean(row["timing_reference_notes"]),
        "lifetime_years": number(row["lifetime"]),
        "distribution": distribution,
        "fleet_age": fleet,
        "parameter_confidence": number(row["param_confidence"]),
        "parameter_assumptions": (
            [clean(row["param_notes"])] if clean(row["param_notes"]) else []
        ),
        "evidence_status": "legacy_unreviewed",
    }
    return {key: value for key, value in profile.items() if value is not None}


def profile_id(profile: dict[str, Any]) -> str:
    tag = slug(profile.get("temporal_tag") or "untagged")
    reference = slug(profile["timing_reference"])
    lifetime = profile.get("lifetime_years")
    life = f"life-{format(lifetime, '.8g')}" if lifetime is not None else "no-life"
    distribution = profile.get("distribution") or {}
    dist = f"dist-{distribution.get('type', 'none')}"
    return f"legacy-{tag}-{reference}-{life}-{dist}-{fingerprint(profile)}"


def migrate(source: Path, output_directory: Path) -> None:
    with source.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    indexed = [(index, row) for index, row in enumerate(rows, start=2)]
    occurrences: dict[tuple[str, str], list[tuple[int, dict]]] = defaultdict(list)
    for index, row in indexed:
        occurrences[(clean(row["name"]), clean(row["reference product"]))].append(
            (index, row)
        )

    effective = []
    conflicts = []
    for identity, candidates in occurrences.items():
        chosen_row_number, chosen = candidates[-1]
        chosen = chosen.copy()
        # The historical loader updated its lifetime lookup only for nonblank
        # values.  Preserve that field-level behavior while resolving the
        # identity to one canonical record.
        inherited_lifetime_row = None
        if not clean(chosen["lifetime"]):
            for row_number, candidate in reversed(candidates[:-1]):
                if clean(candidate["lifetime"]):
                    chosen["lifetime"] = candidate["lifetime"]
                    inherited_lifetime_row = row_number
                    break
        effective.append(chosen)
        if len(candidates) > 1:
            differing_fields = sorted(
                field
                for field in rows[0]
                if len({clean(row[field]) for _, row in candidates}) > 1
            )
            conflict = {
                "identity": list(identity),
                "source_rows": [row_number for row_number, _ in candidates],
                "differing_fields": differing_fields,
                "chosen_source_row": chosen_row_number,
                "resolution": "preserve_legacy_last_effective_row",
            }
            if inherited_lifetime_row is not None:
                conflict["field_resolutions"] = {
                    "lifetime": {
                        "source_row": inherited_lifetime_row,
                        "reason": "legacy loader retained the latest nonblank lifetime",
                    }
                }
            conflicts.append(conflict)

    profiles = {}
    grouped = defaultdict(list)
    for row in effective:
        profile = profile_from_row(row)
        identifier = profile_id(profile)
        profiles[identifier] = profile
        classification = tuple(clean(row[field]) for field in CLASSIFICATION_FIELDS)
        tag_confidence = number(row["tag_confidence"])
        grouped[(identifier, classification, tag_confidence)].append(
            [clean(row["name"]), clean(row["reference product"])]
        )

    mappings = []
    for (identifier, classification, tag_confidence), datasets in grouped.items():
        classification_record = {
            key: value
            for key, value in zip(("isic", "cpc", "ecospold_category"), classification)
            if value
        }
        mapping_signature = {
            "profile": identifier,
            "classification": classification_record,
            "tag_confidence": tag_confidence,
        }
        mappings.append(
            {
                "id": f"map-{fingerprint(mapping_signature, 12)}",
                "profile": identifier,
                "classification": classification_record,
                "tag_confidence": tag_confidence,
                "datasets": sorted(datasets),
            }
        )

    output_directory.mkdir(parents=True, exist_ok=True)
    documents = {
        "legacy_profiles.yaml": {
            "schema_version": 1,
            "profiles": dict(sorted(profiles.items())),
        },
        "legacy_mappings.yaml": {
            "schema_version": 1,
            "mappings": sorted(mappings, key=lambda item: item["id"]),
        },
        "legacy_conflicts.yaml": {
            "schema_version": 1,
            "conflicts": sorted(conflicts, key=lambda item: item["identity"]),
        },
    }
    for filename, document in documents.items():
        with (output_directory / filename).open("w", encoding="utf-8") as stream:
            yaml.safe_dump(
                document,
                stream,
                sort_keys=False,
                allow_unicode=True,
                width=120,
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output_directory", type=Path)
    args = parser.parse_args()
    migrate(args.source, args.output_directory)


if __name__ == "__main__":
    main()
