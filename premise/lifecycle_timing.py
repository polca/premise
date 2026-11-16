"""Explicit reference events for end-of-life exchange timing.

These helpers retain fixed service lives. They do not reconstruct stock inflows,
survival curves, utilisation, or IAM-specific cohorts.
"""

import csv
import json
import math
from collections import defaultdict

import yaml

TIMING_REFERENCES = {
    "new_asset",
    "existing_fleet",
    "current_output",
    "lifecycle_event",
}


def load_retirement_asset_links(path):
    """Read reviewed links from retirement suppliers to the assets they retire.

    These are asset identities, not fuzzy name matches. Geographic variants of
    the same asset are allowed, but the calling service must actually consume
    one of the declared assets.
    """
    with open(path, encoding="utf-8") as stream:
        document = yaml.safe_load(stream)
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("Retirement links require schema_version: 1.")
    links = {}
    for row in document.get("retirement_asset_links", []):
        supplier = row["retirement_supplier"]
        key = (supplier["name"], supplier["reference_product"])
        assets = row["construction_suppliers"]
        values = tuple((asset["name"], asset["reference_product"]) for asset in assets)
        if not values or key in links or any(not all(value) for value in values):
            raise ValueError(f"Invalid or duplicate retirement asset link: {key}")
        links[key] = values
    return links


def construction_age_profile(parameters):
    """Annualise bounded construction timing without changing its shape.

    Uniform and triangular profiles use the same integer support and tent
    weights as TRAILS. Explicit profiles preserve their supplied weights.
    Unsupported or unbounded distributions are rejected, not approximated by
    an unrelated fleet. This helper does not import TRAILS.
    """
    distribution = parameters.get("temporal_distribution")
    if distribution == 6:
        offsets = parameters.get("temporal_offsets")
        weights = parameters.get("temporal_weights")
        if not offsets or weights is None or len(offsets) != len(weights):
            raise ValueError("Construction offsets and weights must be aligned.")
        offsets = [float(value) for value in offsets]
        weights = [float(value) for value in weights]
    elif distribution in {1, 4, 5}:
        minimum = parameters.get("temporal_min")
        maximum = parameters.get("temporal_max")
        if distribution == 1 and minimum is None and maximum is None:
            minimum = maximum = parameters.get("temporal_loc")
        if minimum is None or maximum is None:
            raise ValueError("Construction timing requires finite explicit bounds.")
        if not all(math.isfinite(float(v)) for v in [minimum, maximum]):
            raise ValueError("Construction timing requires finite explicit bounds.")
        offsets = list(range(int(minimum), int(maximum) + 1))
        if not offsets:
            raise ValueError("Construction timing has empty annual support.")
        if distribution == 4:
            weights = [1.0] * len(offsets)
        elif distribution == 5:
            loc = float(parameters.get("temporal_loc") or 0)
            distances = [abs(value - loc) for value in offsets]
            maximum_distance = max(distances)
            weights = (
                [maximum_distance - distance for distance in distances]
                if maximum_distance
                else [1.0] * len(offsets)
            )
        else:
            loc = parameters.get("temporal_loc")
            target = round(float(loc)) if loc is not None else 0
            nearest = min(offsets, key=lambda value: abs(value - target))
            weights = [float(value == nearest) for value in offsets]
    else:
        raise ValueError("Construction timing has no supported bounded age profile.")
    if any(not math.isfinite(value) or value > 0 for value in offsets):
        raise ValueError("Fleet construction offsets must be finite and non-positive.")
    if any(not math.isfinite(value) or value < 0 for value in weights):
        raise ValueError("Construction weights must be finite and nonnegative.")
    total = sum(weights)
    if not math.isfinite(total) or total <= 0:
        raise ValueError("Construction weights must have positive finite total mass.")
    positive = [
        (offset, weight / total)
        for offset, weight in zip(offsets, weights)
        if weight > 0
    ]
    return [v[0] for v in positive], [v[1] for v in positive]


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
        pulses[math.floor(lifetime + offset + 0.5)] += weight
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
    with open(path, encoding="utf-8-sig", newline="") as stream:
        return load_lifecycle_timing_rows(csv.DictReader(stream))


def load_lifecycle_timing_rows(rows):
    """Load caller reference events from canonical in-memory registry rows."""

    records = {}
    for row in rows:
        reference = (row.get("timing_reference") or "").strip()
        if not reference:
            continue
        if reference not in TIMING_REFERENCES:
            raise ValueError(f"Unknown end-of-life timing reference: {reference!r}")
        key = (row["name"].strip(), row["reference product"].strip())
        record = {
            "timing_reference": reference,
            "assumption": (row.get("timing_reference_notes") or "").strip(),
        }
        if reference == "existing_fleet":
            try:
                ages = json.loads(row.get("fleet_age_offsets") or "[]")
                weights = json.loads(row.get("fleet_age_weights") or "[]")
                lifetime = float(row.get("fleet_lifetime") or row["lifetime"])
                offsets, normalised = remaining_life_profile(lifetime, ages, weights)
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
        return [math.floor(float(lifetime) + 0.5)], [1.0]
    if reference == "existing_fleet":
        if fleet is None:
            raise ValueError(
                "Existing fleets require explicit exchange-relevant age weights."
            )
        return remaining_life_profile(
            fleet.get("lifetime", lifetime),
            fleet["age_offsets"],
            fleet["age_weights"],
        )
    raise ValueError(f"Unresolved end-of-life reference event: {reference!r}")
