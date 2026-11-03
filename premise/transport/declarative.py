"""Declarative shared operations emitted by transport-sector planners."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from ..declarative import ReferenceActivity, SharedOperation
from .vehicle_energy_use import SOURCE_KEY

VEHICLE_EMISSIONS_OPERATOR = "vehicle-emissions-from-source"


def _activity_identity(activity: ReferenceActivity) -> tuple[str, str, str, str]:
    return activity.name, activity.product, activity.location, activity.unit


def _fuel_proportional_flow(exchange) -> bool:
    categories: Any = exchange.metadata.get("categories", ())
    if isinstance(categories, str):
        categories = tuple(categories.split("::"))
    else:
        categories = tuple(categories or ())
    if not categories or categories[0] != "air":
        return False
    if exchange.name in ("Carbon dioxide, fossil", "Carbon dioxide, non-fossil"):
        return True
    return categories in (("air",), ("air", "unspecified")) and exchange.name in (
        "Sulfur dioxide",
        "Methane, fossil",
        "Methane, non-fossil",
    )


def vehicle_emission_operations(
    activities: Iterable[ReferenceActivity],
) -> tuple[SharedOperation, ...]:
    """Compile invariant vehicle-wrapper declarations from sector metadata.

    ``normalize`` records the source identity and energy denominator while the
    vehicle sector is planned alone. The declaration contains no selected-sector
    state; the global reducer later reads the already-composed source B column.
    """

    activities = tuple(activities)
    by_identity: dict[tuple[str, str, str, str], list[ReferenceActivity]] = defaultdict(
        list
    )
    for activity in activities:
        by_identity[_activity_identity(activity)].append(activity)

    operations = []
    for wrapper in activities:
        raw_record = wrapper.metadata.get(SOURCE_KEY)
        if raw_record is None:
            continue
        record = json.loads(raw_record) if isinstance(raw_record, str) else raw_record
        source_key = tuple(record["source"])
        matches = by_identity.get(source_key, ())
        if len(matches) != 1:
            raise ValueError(
                f"Expected one declarative vehicle source {source_key!r}, "
                f"found {len(matches)}"
            )
        denominator = float(record["purchased MJ per source service"])
        if not math.isfinite(denominator) or denominator <= 0.0:
            raise ValueError(
                f"Invalid vehicle energy denominator for {wrapper.identity!r}: "
                f"{denominator!r}"
            )
        fuel_flows = sorted(
            {
                exchange.input_identity
                for activity in (wrapper, matches[0])
                for exchange in activity.exchanges
                if exchange.type == "biosphere" and _fuel_proportional_flow(exchange)
            }
        )
        operations.append(
            SharedOperation.from_payload(
                operator=VEHICLE_EMISSIONS_OPERATOR,
                target=wrapper.identity,
                payload={
                    "source": matches[0].identity,
                    "purchased_mj_per_source_service": denominator,
                    "fuel_proportional_flows": fuel_flows,
                },
            )
        )
    return tuple(sorted(operations))


__all__ = ["VEHICLE_EMISSIONS_OPERATOR", "vehicle_emission_operations"]
