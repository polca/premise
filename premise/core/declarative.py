"""Generic declarative operations emitted by core transformation planners."""

from __future__ import annotations

from collections.abc import Iterable

from ..declarative import ReferenceActivity, SharedOperation

RELINK_UNAVAILABLE_PROVIDERS = "relink-unavailable-providers"
AUTHORITATIVE_TECHNOSPHERE_COLUMN = "authoritative-technosphere-column"


def unavailable_provider_operation(
    activities: Iterable[ReferenceActivity],
) -> SharedOperation | None:
    """Declare providers removed from the semantic index by one sector.

    The global reducer resolves any demand for these providers against the
    active provider catalogue. It therefore also handles consumers activated by
    another sector without either planner inspecting the other's output.
    """

    providers = sorted(
        activity.identity
        for activity in activities
        if activity.metadata.get("emptied") is True
    )
    if not providers:
        return None
    return SharedOperation.from_payload(
        operator=RELINK_UNAVAILABLE_PROVIDERS,
        target="technosphere",
        payload={"providers": providers},
    )


def authoritative_column_operations(
    activities: Iterable[ReferenceActivity],
) -> tuple[SharedOperation, ...]:
    """Declare complete technosphere columns for emptied forwarding activities."""

    operations = []
    for activity in activities:
        if activity.metadata.get("emptied") is not True:
            continue
        operations.append(
            SharedOperation.from_payload(
                operator=AUTHORITATIVE_TECHNOSPHERE_COLUMN,
                target=activity.identity,
                payload={
                    "allowed_providers": sorted(
                        exchange.input_identity
                        for exchange in activity.exchanges
                        if exchange.type == "technosphere"
                    )
                },
            )
        )
    return tuple(sorted(operations))


__all__ = [
    "AUTHORITATIVE_TECHNOSPHERE_COLUMN",
    "RELINK_UNAVAILABLE_PROVIDERS",
    "authoritative_column_operations",
    "unavailable_provider_operation",
]
