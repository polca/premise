"""Shared preparation primitives for premise's private fast export path."""

from __future__ import annotations

import math
from typing import Any, Mapping

import numpy as np

from .inventory_store import IndexedInventoryList

FAST_EXCHANGE_REQUIRED_FIELDS = frozenset(
    {
        "input",
        "amount",
        "type",
        "name",
        "product",
        "unit",
        "location",
        "output",
    }
)
FAST_STRING_FIELDS = frozenset(
    {"name", "reference product", "product", "unit", "location"}
)
FAST_EXCHANGE_FORBIDDEN_FIELDS = {
    "biosphere": frozenset({"location", "product"}),
    "technosphere": frozenset({"categories"}),
}
_UNCHANGED = object()


class PreparedExportInventory(IndexedInventoryList):
    """Private marker for inventories whose exchange rows are writer-ready."""

    _premise_exchange_payloads_prepared = True


def is_prepared_export_inventory(inventory: Any) -> bool:
    """Return whether exchange dictionaries have completed shared preparation."""

    return bool(getattr(inventory, "_premise_exchange_payloads_prepared", False))


def mark_prepared_export_inventory(inventory: list) -> list:
    """Mark a private checkout without changing activity or exchange ordering."""

    try:
        inventory._premise_exchange_payloads_prepared = True
        return inventory
    except AttributeError:
        return PreparedExportInventory(inventory)


def keep_fast_export_value(value: Any) -> bool:
    """Apply the established Brightway fast-payload missing-value policy."""

    if value is None:
        return False
    if isinstance(value, str) and value in {"", "None", "nan"}:
        return False
    if isinstance(value, (list, tuple, dict, set)):
        return True
    try:
        return not math.isnan(value)
    except (TypeError, ValueError):
        return True


def normalize_no_uncertainty_exchange(exchange: dict[str, Any]) -> dict[str, Any]:
    """Normalize deterministic uncertainty exactly as the legacy fast writers."""

    uncertainty_type = exchange.get(
        "uncertainty type", exchange.get("uncertainty_type", 0)
    )
    try:
        uncertainty_type = int(uncertainty_type)
    except (TypeError, ValueError, OverflowError):
        return exchange
    if uncertainty_type not in {0, 1} or "amount" not in exchange:
        return exchange
    exchange["loc"] = exchange["amount"]
    for field in ("scale", "shape", "minimum", "maximum"):
        exchange.pop(field, None)
    return exchange


def _writer_scalar(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        if value.size != 1:
            return value
        return value.reshape(-1)[0].item()
    if isinstance(value, np.generic):
        return value.item()
    return value


def prepare_fast_exchange_payload(
    exchange: Mapping[str, Any],
    *,
    input_override: Any = _UNCHANGED,
) -> dict[str, Any]:
    """Project one exchange into the canonical fast-writer dictionary.

    Compact and columnar mappings can expose their fields in one specialized
    call. Ordinary mappings retain the established item-iteration fallback.
    """

    fast_payload = getattr(exchange, "_premise_fast_export_payload", None)
    source = fast_payload() if fast_payload is not None else exchange
    exchange_type = source.get("type")
    forbidden_fields = FAST_EXCHANGE_FORBIDDEN_FIELDS.get(exchange_type, frozenset())
    compact_exchange = {
        field: _writer_scalar(value)
        for field, value in source.items()
        if field not in forbidden_fields and keep_fast_export_value(value)
    }
    for field in FAST_EXCHANGE_REQUIRED_FIELDS:
        if (
            field not in forbidden_fields
            and field not in compact_exchange
            and field in source
        ):
            value = source[field]
            compact_exchange[field] = (
                ""
                if field in FAST_STRING_FIELDS and value is None
                else _writer_scalar(value)
            )

    if input_override is not _UNCHANGED:
        if input_override is None:
            compact_exchange.pop("input", None)
        else:
            compact_exchange["input"] = input_override

    return normalize_no_uncertainty_exchange(compact_exchange)


__all__ = [
    "FAST_EXCHANGE_FORBIDDEN_FIELDS",
    "FAST_EXCHANGE_REQUIRED_FIELDS",
    "FAST_STRING_FIELDS",
    "PreparedExportInventory",
    "is_prepared_export_inventory",
    "keep_fast_export_value",
    "mark_prepared_export_inventory",
    "normalize_no_uncertainty_exchange",
    "prepare_fast_exchange_payload",
]
