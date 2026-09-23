"""Reclassify emitted fuel carbon without changing the inventory's CO2 balance."""

from collections import defaultdict
import math

from ..utils import rescale_exchange

_TRANSFER_KEY = "premise fuel CO2 reclassification"


def _categories(exchange):
    """Treat missing and explicit unspecified subcompartments identically."""
    categories = tuple(exchange.get("categories") or ("air",))
    return categories if len(categories) > 1 else (*categories, "unspecified")


def _non_fossil_target(categories, existing, biosphere_flows, dataset):
    """Prefer the exact air compartment, falling back to unspecified air.

    In ecoinvent 3.10 the high-altitude non-fossil CO2 flow was migrated to
    unspecified air. Always return categories matching the selected flow code.
    An existing exchange can supply the target even without a lookup entry.
    """
    for candidate in (categories, ("air", "unspecified")):
        code = biosphere_flows.get(
            ("Carbon dioxide, non-fossil", *candidate, "kilogram")
        )
        if existing.get(candidate) or code is not None:
            return candidate, code
    raise ValueError(
        f"Cannot reclassify fuel CO2 in {dataset.get('name', '<unnamed dataset>')!r}: "
        f"no non-fossil CO2 flow for {categories!r} or ('air', 'unspecified')."
    )


def _set_amount(exchange, amount):
    """Keep distribution parameters consistent, including zero endpoints."""
    previous = exchange["amount"]
    if amount == previous:
        return
    if amount > 0 and previous > 0 and exchange.get("uncertainty type", 0) != 0:
        rescale_exchange(exchange, amount / previous, remove_uncertainty=False)
        if exchange.get("uncertainty type", 0) == 0:
            exchange["loc"] = amount
    else:
        exchange["uncertainty type"] = 0
        exchange["loc"] = amount
        for key in ("scale", "shape", "minimum", "maximum", "negative"):
            exchange.pop(key, None)
    exchange["amount"] = amount


def reclassify_fuel_co2(dataset, fuel_co2, non_fossil_share, biosphere_flows, fuel_key):
    """Transfer fossil air emissions to non-fossil air emissions.

    Fuel carbon limits the affected pool in multi-fuel inventories. When the
    recorded emissions are lower (e.g. after CCS), split only those residual
    emissions. Capture inputs and negative biosphere exchanges are unchanged.
    Track transfers per fuel on source exchanges so repeated updates replace
    earlier transfers and preserve pre-existing non-fossil emissions.
    Missing non-fossil air subcompartments fall back to unspecified air, with
    transfers combined before changing shared targets. Invalid amounts or
    missing targets raise ValueError before any exchanges are changed.
    """
    if not math.isfinite(fuel_co2) or fuel_co2 < 0:
        raise ValueError("Fuel CO2 must be finite and nonnegative")
    if not math.isfinite(non_fossil_share) or not 0 <= non_fossil_share <= 1:
        raise ValueError("Non-fossil fuel share must be between zero and one")
    fossil = []
    non_fossil = defaultdict(list)
    for exc in dataset["exchanges"]:
        if (
            exc.get("type") == "biosphere"
            and exc.get("name")
            in {"Carbon dioxide, fossil", "Carbon dioxide, non-fossil"}
            and exc.get("unit") == "kilogram"
            and _categories(exc)[0] == "air"
        ):
            if not math.isfinite(exc["amount"]):
                raise ValueError("Fuel CO2 emission amounts must be finite")
            if exc["amount"] < 0:
                continue
            if exc["name"] == "Carbon dioxide, non-fossil":
                non_fossil[_categories(exc)].append(exc)
                continue
            old = exc.get(_TRANSFER_KEY, {}).get(fuel_key, 0.0)
            fossil.append((exc, old, exc["amount"] + old))
    total = sum(base for _, _, base in fossil)
    if total <= 0:
        return
    transfer = min(total, fuel_co2) * non_fossil_share
    changes = defaultdict(float)
    updates = []
    for exc, old, base in fossil:
        new = transfer * base / total
        categories = _categories(exc)
        changes[categories] += new - old
        updates.append((exc, base - new, new))

    # Several source compartments can resolve to the same non-fossil target.
    # Aggregate their changes before reading or changing any target amounts.
    target_changes = defaultdict(float)
    target_codes = {}
    for categories, delta in changes.items():
        if delta == 0:
            continue
        target, code = _non_fossil_target(
            categories, non_fossil, biosphere_flows, dataset
        )
        target_changes[target] += delta
        target_codes[target] = code

    # Resolve all targets and validate withdrawals before changing the inventory.
    targets = []
    for categories, delta in target_changes.items():
        if delta == 0:
            continue
        existing = non_fossil[categories]
        current = sum(e["amount"] for e in existing)
        if current + delta < -1e-12:
            raise ValueError("Previously reclassified non-fossil CO2 is missing")
        created = not existing
        if created:
            existing = [
                dict(
                    name="Carbon dioxide, non-fossil",
                    categories=categories,
                    amount=0.0,
                    unit="kilogram",
                    type="biosphere",
                    input=("biosphere3", target_codes[categories]),
                    **{"uncertainty type": 0},
                )
            ]
        targets.append((existing, current, max(0.0, current + delta), created))
    for existing, current, updated, created in targets:
        for i, exc in enumerate(existing):
            weight = exc["amount"] / current if current else float(i == 0)
            _set_amount(exc, updated * weight)
            if created:
                dataset["exchanges"].append(exc)
    for exc, updated, transferred in updates:
        _set_amount(exc, updated)
        if transferred or fuel_key in exc.get(_TRANSFER_KEY, {}):
            exc.setdefault(_TRANSFER_KEY, {})[fuel_key] = transferred
