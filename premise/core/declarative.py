"""Generic declarative operations emitted by core transformation planners."""

from __future__ import annotations

import math
from collections.abc import Iterable

from ..declarative import ReferenceActivity, SharedOperation

RELINK_UNAVAILABLE_PROVIDERS = "relink-unavailable-providers"
AUTHORITATIVE_TECHNOSPHERE_COLUMN = "authoritative-technosphere-column"
NORMALIZE_PROVIDER_VECTOR = "normalize-provider-vector"


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


def normalized_provider_vector_operation(
    *,
    target: str,
    weights: dict[str, float],
    total: float,
    replace_providers: Iterable[str] = (),
) -> SharedOperation:
    """Declare raw market weights for one global normalization pass.

    Planners calculate ``weights`` from the immutable latent provider universe.
    Multiple sectors may repeat the same provider weight or add disjoint
    providers; the reducer unions compatible declarations and normalizes once.
    """

    if not target:
        raise ValueError("Normalized provider vectors require a target")
    normalized_weights = {str(key): float(value) for key, value in weights.items()}
    total = float(total)
    if not math.isfinite(total):
        raise ValueError("Normalized provider-vector total must be finite")
    if not normalized_weights or any(
        not provider or not math.isfinite(weight) or weight < 0.0
        for provider, weight in normalized_weights.items()
    ):
        raise ValueError(
            "Normalized provider vectors require non-negative finite weights"
        )
    if sum(normalized_weights.values()) <= 0.0:
        raise ValueError("Normalized provider-vector weights must sum above zero")
    return SharedOperation.from_payload(
        operator=NORMALIZE_PROVIDER_VECTOR,
        target=target,
        payload={
            "weights": normalized_weights,
            "total": total,
            "replace_providers": sorted({str(value) for value in replace_providers}),
        },
    )


__all__ = [
    "AUTHORITATIVE_TECHNOSPHERE_COLUMN",
    "NORMALIZE_PROVIDER_VECTOR",
    "RELINK_UNAVAILABLE_PROVIDERS",
    "authoritative_column_operations",
    "normalized_provider_vector_operation",
    "unavailable_provider_operation",
]
