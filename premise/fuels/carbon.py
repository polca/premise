"""Reclassify emitted fuel carbon without changing the inventory's CO2 balance."""

from collections import defaultdict
import math

from ..utils import rescale_exchange

_TRANSFER_KEY = "premise fuel CO2 reclassification"


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
    """
    if not math.isfinite(fuel_co2) or fuel_co2 < 0:
        raise ValueError("Fuel CO2 must be finite and nonnegative")
    if not math.isfinite(non_fossil_share) or not 0 <= non_fossil_share <= 1:
        raise ValueError("Non-fossil fuel share must be between zero and one")
    fossil = []
    for exc in dataset["exchanges"]:
        if (
            exc.get("type") == "biosphere"
            and exc.get("name") == "Carbon dioxide, fossil"
            and exc.get("unit") == "kilogram"
            and tuple(exc.get("categories", ("air",)))[0] == "air"
            and exc["amount"] >= 0
        ):
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
        categories = tuple(exc.get("categories", ("air",)))
        changes[categories] += new - old
        updates.append((exc, base - new, new))

    # Resolve flow identities before changing the inventory.
    targets = []
    for categories, delta in changes.items():
        if delta == 0:
            continue
        existing = [
            e
            for e in dataset["exchanges"]
            if e.get("type") == "biosphere"
            and e.get("name") == "Carbon dioxide, non-fossil"
            and e.get("unit") == "kilogram"
            and tuple(e.get("categories", ("air",))) == categories
            and e["amount"] >= 0
        ]
        current = sum(e["amount"] for e in existing)
        if current + delta < -1e-12:
            raise ValueError("Previously reclassified non-fossil CO2 is missing")
        created = not existing
        if created:
            code = biosphere_flows[
                (
                    "Carbon dioxide, non-fossil",
                    "air",
                    categories[1] if len(categories) > 1 else "unspecified",
                    "kilogram",
                )
            ]
            existing = [
                dict(
                    name="Carbon dioxide, non-fossil",
                    categories=categories,
                    amount=0.0,
                    unit="kilogram",
                    type="biosphere",
                    input=("biosphere3", code),
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
