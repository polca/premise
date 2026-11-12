"""Explicit reference events for end-of-life exchange timing.

These helpers retain fixed service lives. They do not reconstruct stock inflows,
survival curves, utilisation, or IAM-specific cohorts.
"""

import csv
import json
import math
from collections import defaultdict

TIMING_REFERENCES = {
    "new_asset",
    "existing_fleet",
    "current_output",
    "lifecycle_event",
}


def remaining_life_profile(lifetime, age_offsets, age_weights):
    """Return annual retirement offsets and weights for a fixed-life fleet.

    ``age_offsets`` are non-positive production offsets relative to current
    service. An asset at offset -a retires after L-a years. No second vintage
    distribution or additional year is applied. Annual dates use nearest-year
    rounding, with halves rounded upward.
    """
    lifetime = float(lifetime)
    if not math.isfinite(lifetime) or lifetime <= 0:
        raise ValueError("Fleet lifetime must be finite and positive.")
    if not age_offsets or len(age_offsets) != len(age_weights):
        raise ValueError("Fleet age offsets and weights must be nonempty and aligned.")
    pulses = defaultdict(float)
    for offset, weight in zip(age_offsets, age_weights):
        offset, weight = float(offset), float(weight)
        if not math.isfinite(offset) or not -lifetime <= offset <= 0:
            raise ValueError("Fleet ages must lie between zero and the fixed lifetime.")
        if not math.isfinite(weight) or weight < 0:
            raise ValueError("Fleet age weights must be finite and nonnegative.")
        pulses[int(math.floor(lifetime + offset + 0.5))] += weight
    total = sum(pulses.values())
    if total <= 0 or not math.isfinite(total):
        raise ValueError("Fleet age weights must have positive finite total mass.")
    offsets = sorted(offset for offset, weight in pulses.items() if weight > 0)
    weights = [pulses[offset] / total for offset in offsets]
    weights[-1] += 1.0 - sum(weights)
    return offsets, weights


def load_lifecycle_timing(path):
    """Read optional caller reference events without changing inventory amounts.

    Existing-fleet rows require explicit, exchange-relevant age weights. A
    lifetime or the age of unrelated factory capital is not a fleet age profile.
    """
    records = {}
    with open(path, encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            reference = (row.get("timing_reference") or "").strip()
            if not reference:
                continue
            if reference not in TIMING_REFERENCES:
                raise ValueError(f"Unknown end-of-life timing reference: {reference!r}")
            key = (row["name"].strip(), row["reference product"].strip())
            record = {"timing_reference": reference}
            if reference == "existing_fleet":
                try:
                    ages = json.loads(row.get("fleet_age_offsets") or "[]")
                    weights = json.loads(row.get("fleet_age_weights") or "[]")
                    lifetime = float(row["lifetime"])
                    offsets, normalised = remaining_life_profile(
                        lifetime, ages, weights
                    )
                except (ValueError, TypeError, KeyError) as error:
                    raise ValueError(
                        f"Invalid fleet retirement profile for {key}: {error}"
                    ) from error
                record.update(
                    lifetime=lifetime,
                    age_offsets=ages,
                    age_weights=weights,
                    offsets=offsets,
                    weights=normalised,
                )
            if key in records and records[key] != record:
                raise ValueError(f"Conflicting end-of-life reference events for {key}")
            records[key] = record
    return records


def retirement_profile(reference, lifetime=None, fleet=None):
    """Return a retirement profile relative to an explicitly identified caller."""
    if reference in {"current_output", "lifecycle_event"}:
        return [0], [1.0]
    if reference == "new_asset":
        if (
            lifetime is None
            or not math.isfinite(float(lifetime))
            or float(lifetime) <= 0
        ):
            raise ValueError("New assets require a finite positive service life.")
        return [int(math.floor(float(lifetime) + 0.5))], [1.0]
    if reference == "existing_fleet":
        if fleet is None:
            raise ValueError(
                "Existing fleets require explicit exchange-relevant age weights."
            )
        return remaining_life_profile(
            lifetime, fleet["age_offsets"], fleet["age_weights"]
        )
    raise ValueError(f"Unresolved end-of-life reference event: {reference!r}")
