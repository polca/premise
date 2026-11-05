"""Generic declarative operations emitted by core transformation planners."""

from __future__ import annotations

import math
from collections.abc import Iterable

from ..declarative import ReferenceActivity, SharedOperation

RELINK_UNAVAILABLE_PROVIDERS = "relink-unavailable-providers"
AUTHORITATIVE_TECHNOSPHERE_COLUMN = "authoritative-technosphere-column"
NORMALIZE_PROVIDER_VECTOR = "normalize-provider-vector"
CANONICALIZE_PROVIDER_KIND = "canonicalize-provider-kind"
CACHED_PROVIDER_ROUTE = "cached-provider-route"
RELINK_DATASET_SCOPE = "relink-dataset-scope"
RELINK_PROVIDER_ROUTE = "relink-provider-route"


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
            name.removeprefix(prefix) for prefix in prefixes if name.startswith(prefix)
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


def cached_provider_route_operation(
    *,
    consumer_location: str,
    request: tuple[str, str, str, str],
    providers: Iterable[tuple[str, str, str, str, float]],
) -> SharedOperation:
    """Declare one deterministic entry from Premise's relinking cache."""

    request = tuple(map(str, request))
    entries = tuple(
        sorted((*map(str, provider[:4]), float(provider[4])) for provider in providers)
    )
    if not consumer_location or len(request) != 4 or not all(request):
        raise ValueError("Cached provider routes require a complete request")
    if not entries or any(
        not all(provider[:4]) or not math.isfinite(provider[4]) or provider[4] < 0.0
        for provider in entries
    ):
        raise ValueError("Cached provider routes require finite provider shares")
    if not math.isclose(sum(provider[4] for provider in entries), 1.0):
        raise ValueError("Cached provider-route shares must sum to one")
    return SharedOperation.from_payload(
        operator=CACHED_PROVIDER_ROUTE,
        target="technosphere",
        payload={
            "consumer_location": str(consumer_location),
            "request": {
                "name": request[0],
                "product": request[1],
                "location": request[2],
                "unit": request[3],
            },
            "providers": [
                {
                    "name": provider[0],
                    "product": provider[1],
                    "location": provider[2],
                    "unit": provider[3],
                    "share": provider[4],
                }
                for provider in entries
            ],
        },
    )


def relink_provider_route_operation(
    *,
    consumer_location: str,
    request: tuple[str, str, str, str],
    providers: Iterable[tuple[str, str, str, str, float]],
) -> SharedOperation:
    """Declare one route actually selected by a global relinking traversal."""

    request = tuple(map(str, request))
    entries = tuple(
        sorted((*map(str, provider[:4]), float(provider[4])) for provider in providers)
    )
    if not consumer_location or len(request) != 4 or not all(request):
        raise ValueError("Relink provider routes require a complete request")
    if not entries or any(
        not all(provider[:4]) or not math.isfinite(provider[4]) or provider[4] < 0.0
        for provider in entries
    ):
        raise ValueError("Relink provider routes require finite provider shares")
    if not math.isclose(sum(provider[4] for provider in entries), 1.0):
        raise ValueError("Relink provider-route shares must sum to one")
    return SharedOperation.from_payload(
        operator=RELINK_PROVIDER_ROUTE,
        target="technosphere",
        payload={
            "consumer_location": str(consumer_location),
            "request": {
                "name": request[0],
                "product": request[1],
                "location": request[2],
                "unit": request[3],
            },
            "providers": [
                {
                    "name": provider[0],
                    "product": provider[1],
                    "location": provider[2],
                    "unit": provider[3],
                    "share": provider[4],
                }
                for provider in entries
            ],
        },
    )


def relink_dataset_scope_operation(
    *,
    excludes_datasets: Iterable[str] = (),
    alternative_names: Iterable[str] = (),
) -> SharedOperation:
    """Declare the selection-independent traversal scope of ``relink_datasets``.

    Cached provider routes describe outcomes observed during one singleton
    traversal, but not which potential activities another sector would have
    traversed. This capability declaration lets a later latent-universe planner
    apply the same generic relinking policy without inspecting selected sectors.
    """

    exclusions = tuple(sorted({str(value) for value in excludes_datasets if value}))
    alternatives = tuple(sorted({str(value) for value in alternative_names if value}))
    return SharedOperation.from_payload(
        operator=RELINK_DATASET_SCOPE,
        target="technosphere",
        payload={
            "scope": "all-datasets",
            "exclude_name_substrings": list(exclusions),
            "alternative_names": list(alternatives),
        },
    )


__all__ = [
    "AUTHORITATIVE_TECHNOSPHERE_COLUMN",
    "CACHED_PROVIDER_ROUTE",
    "CANONICALIZE_PROVIDER_KIND",
    "NORMALIZE_PROVIDER_VECTOR",
    "RELINK_DATASET_SCOPE",
    "RELINK_PROVIDER_ROUTE",
    "RELINK_UNAVAILABLE_PROVIDERS",
    "authoritative_column_operations",
    "cached_provider_route_operation",
    "canonical_provider_kind_operation",
    "normalized_provider_vector_operation",
    "relink_dataset_scope_operation",
    "relink_provider_route_operation",
    "unavailable_provider_operation",
]
