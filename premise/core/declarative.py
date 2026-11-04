"""Generic declarative operations emitted by core transformation planners."""

from __future__ import annotations

import math
from collections.abc import Iterable

from ..declarative import ReferenceActivity, SharedOperation

RELINK_UNAVAILABLE_PROVIDERS = "relink-unavailable-providers"
AUTHORITATIVE_TECHNOSPHERE_COLUMN = "authoritative-technosphere-column"
NORMALIZE_PROVIDER_VECTOR = "normalize-provider-vector"
CANONICALIZE_PROVIDER_KIND = "canonicalize-provider-kind"


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


def canonical_provider_kind_operation(
    *, source_name: str, target_names: Iterable[str], product: str, unit: str
) -> SharedOperation | None:
    """Declare a market/market-group canonicalization observed during relinking."""

    prefixes = ("market for ", "market group for ")
    targets = {str(name) for name in target_names}
    if len(targets) != 1 or not source_name.startswith(prefixes):
        return None
    target_name = next(iter(targets))
    if not target_name.startswith(prefixes) or target_name == source_name:
        return None

    def stem(name: str) -> str:
        return next(
            name.removeprefix(prefix)
            for prefix in prefixes
            if name.startswith(prefix)
        )

    if stem(source_name) != stem(target_name) or not product or not unit:
        return None
    family = stem(source_name)
    return SharedOperation.from_payload(
        operator=CANONICALIZE_PROVIDER_KIND,
        target="technosphere",
        payload={
            "source_names": [
                f"market for {family}",
                f"market group for {family}",
            ],
            "target_name": target_name,
            "product": product,
            "unit": unit,
        },
    )


__all__ = [
    "AUTHORITATIVE_TECHNOSPHERE_COLUMN",
    "CANONICALIZE_PROVIDER_KIND",
    "NORMALIZE_PROVIDER_VECTOR",
    "RELINK_UNAVAILABLE_PROVIDERS",
    "authoritative_column_operations",
    "canonical_provider_kind_operation",
    "normalized_provider_vector_operation",
    "unavailable_provider_operation",
]
