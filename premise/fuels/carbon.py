"""Reclassify emitted fuel carbon without changing the inventory's CO2 balance."""

from collections import defaultdict
from copy import deepcopy
import math

from ..utils import FUEL_CO2_TRANSFER_KEY, rescale_exchange
from .properties import PROPERTIES_KEY, activity_key

_TRANSFER_KEY = FUEL_CO2_TRANSFER_KEY
_VEHICLE_FUEL_KEY = "vehicle operating fuel"


def _categories(exchange):
    """Treat missing and explicit unspecified subcompartments identically."""
    categories = tuple(exchange.get("categories") or ("air",))
    return categories if len(categories) > 1 else (*categories, "unspecified")


def _non_fossil_target(
    categories, existing, biosphere_flows, dataset, name="Carbon dioxide, non-fossil"
):
    """Prefer the exact air compartment, falling back to unspecified air.

    In ecoinvent 3.10 the high-altitude non-fossil CO2 flow was migrated to
    unspecified air. Always return categories matching the selected flow code.
    An existing exchange can supply the target even without a lookup entry.
    """
    for candidate in (categories, ("air", "unspecified")):
        code = biosphere_flows.get((name, *candidate, "kilogram"))
        if existing.get(candidate) or code is not None:
            return candidate, code
    raise ValueError(
        f"Cannot reclassify fuel CO2 in {dataset.get('name', '<unnamed dataset>')!r}: "
        f"no {name} flow for {categories!r} or ('air', 'unspecified')."
    )


def _set_amount(exchange, amount):
    """Keep distribution parameters consistent, including zero endpoints."""
    previous = exchange["amount"]
    if amount == previous:
        return
    if amount > 0 and previous > 0 and exchange.get("uncertainty type", 0) != 0:
        rescale_exchange(
            exchange,
            amount / previous,
            remove_uncertainty=False,
            scale_fuel_co2_transfers=False,
        )
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


def is_vehicle_fuel_consumer(dataset, vehicle_type=None):
    """Select direct car/truck services, excluding manufacture and fleet markets."""
    prefixes = {
        "car": "transport, passenger car,",
        "truck": "transport, freight, lorry,",
    }
    return any(
        dataset.get("name", "").startswith(prefix)
        for kind, prefix in prefixes.items()
        if vehicle_type is None or kind == vehicle_type
    )


def vehicle_fuel_carbon(dataset, providers):
    """Read current fuel purchases and supplier oxidation properties.

    Returns None for services without direct supported fuel (including
    forwarding activities), or an explicit unresolved result if fuel properties
    are unavailable. No geographic guess replaces the actual supplier link.
    """
    fuels = []
    unresolved = []
    fossil = non_fossil = 0.0
    for exchange in dataset["exchanges"]:
        if exchange.get("type") != "technosphere":
            continue
        name = exchange.get("name", "")
        if not name.startswith(("market for ", "market group for ")):
            continue
        stem = name.removeprefix("market for ").removeprefix("market group for ")
        if not stem.startswith(
            ("petrol", "gasoline", "diesel", "natural gas", "liquefied petroleum gas")
        ):
            continue
        amount = float(exchange["amount"])
        if not math.isfinite(amount) or amount < 0:
            raise ValueError("Vehicle fuel quantities must be finite and nonnegative")
        key = activity_key(exchange)
        provider = providers.get(key)
        properties = provider.get(PROPERTIES_KEY) if provider is not None else None
        source = "linked fuel supplier properties"
        if (
            properties is None
            and provider is not None
            and stem == "liquefied petroleum gas"
            and key[1] == "liquefied petroleum gas"
            and exchange["unit"] == "kilogram"
        ):
            # Explicit user-approved exception where the IAM has no LPG route.
            properties = {
                "reference unit": "kilogram",
                "fossil CO2 kg per unit": 45.5 * 0.0631,
                "non-fossil CO2 kg per unit": 0.0,
                "unresolved CO2 kg per unit": 0.0,
            }
            source = "documented fossil LPG fallback; no scenario blend"
        if properties is None:
            unresolved.append(f"Missing fuel properties for {key}")
            continue
        if properties["reference unit"] != exchange["unit"]:
            raise ValueError(f"Vehicle fuel property unit mismatch: {key}")
        factors = [
            float(properties[k])
            for k in (
                "fossil CO2 kg per unit",
                "non-fossil CO2 kg per unit",
                "unresolved CO2 kg per unit",
            )
        ]
        if any(not math.isfinite(value) or value < 0 for value in factors):
            raise ValueError(f"Invalid vehicle fuel carbon properties: {key}")
        if factors[2] > 0:
            unresolved.append(f"Unresolved carbon origin for {key}")
            continue
        fossil += amount * factors[0]
        non_fossil += amount * factors[1]
        fuels.append(
            {
                "name": name,
                "product": key[1],
                "location": key[2],
                "unit": key[3],
                "amount": amount,
                "fossil CO2 kg per unit": factors[0],
                "non-fossil CO2 kg per unit": factors[1],
                "source": source,
            }
        )
    if unresolved:
        return {"status": "unresolved", "reasons": sorted(set(unresolved))}
    if not fuels:
        return None
    production = [e for e in dataset["exchanges"] if e.get("type") == "production"]
    if (
        len(production) != 1
        or not math.isfinite(production[0]["amount"])
        or production[0]["amount"] <= 0
    ):
        raise ValueError(
            "Vehicle fuel accounting requires positive reference production"
        )
    return {
        "status": "resolved",
        "location": dataset.get("location"),
        "reference unit": dataset.get("unit"),
        "production amount": production[0]["amount"],
        "fossil CO2 kg": fossil,
        "non-fossil CO2 kg": non_fossil,
        "convention": "Full oxidation of final fuel purchases; separate CO/CH4/NMVOC and negative capture credits unchanged",
        "suppliers": fuels,
    }


def _positive_air_co2(exchange):
    return (
        exchange.get("type") == "biosphere"
        and exchange.get("name")
        in ("Carbon dioxide, fossil", "Carbon dioxide, non-fossil")
        and exchange.get("unit") == "kilogram"
        and _categories(exchange)[0] == "air"
        and exchange["amount"] >= 0
    )


def replace_vehicle_fuel_co2(dataset, balance, biosphere_flows):
    """Replace the vehicle exhaust pool, then reuse the fuel reclassifier.

    Work on private CO2 exchanges so failures cannot leave a partially updated
    vehicle. Undo tracked classification first to retain source compartments.
    This vehicle-only full-oxidation convention must not be applied to CCS or
    other industrial consumers, whose residual emission pools are preserved.
    """
    selected = [e for e in dataset["exchanges"] if _positive_air_co2(e)]
    for exchange in dataset["exchanges"]:
        if (
            exchange.get("type") == "biosphere"
            and exchange.get("name")
            in ("Carbon dioxide, fossil", "Carbon dioxide, non-fossil")
            and not math.isfinite(exchange["amount"])
        ):
            raise ValueError("Vehicle CO2 amounts must be finite")
    target_fossil, target_bio = balance["fossil CO2 kg"], balance["non-fossil CO2 kg"]
    working = {
        "name": dataset["name"],
        "exchanges": [deepcopy(dict(e)) for e in selected],
    }
    keys = {key for e in working["exchanges"] for key in e.get(_TRANSFER_KEY, {})}
    for key in sorted(keys):
        reclassify_fuel_co2(working, 0, 0, biosphere_flows, key)
    weights = defaultdict(float)
    existing_fossil = defaultdict(list)
    for exchange in working["exchanges"]:
        weights[_categories(exchange)] += exchange["amount"]
        exchange.pop(_TRANSFER_KEY, None)
        if exchange["name"] == "Carbon dioxide, fossil":
            existing_fossil[_categories(exchange)].append(exchange)
        else:
            _set_amount(exchange, 0.0)
    total = sum(weights.values())
    if total == 0:
        weights = {("air", "unspecified"): 1.0}
        total = 1.0
    # A bio-only source compartment can require a fossil counterpart; aggregate
    # all fallback weights before setting exchanges that share the same target.
    target_weights, target_codes = defaultdict(float), {}
    for categories, weight in weights.items():
        if weight == 0:
            continue
        target, code = _non_fossil_target(
            categories,
            existing_fossil,
            biosphere_flows,
            dataset,
            "Carbon dioxide, fossil",
        )
        target_weights[target] += weight / total
        target_codes[target] = code
    gross = target_fossil + target_bio
    for categories in set(existing_fossil) | set(target_weights):
        exchanges = existing_fossil[categories]
        current = sum(e["amount"] for e in exchanges)
        if not exchanges:
            exchanges = [
                {
                    "name": "Carbon dioxide, fossil",
                    "unit": "kilogram",
                    "categories": categories,
                    "type": "biosphere",
                    "amount": 0.0,
                    "input": ("biosphere3", target_codes[categories]),
                    "uncertainty type": 0,
                }
            ]
            working["exchanges"].extend(exchanges)
        for i, exchange in enumerate(exchanges):
            share = exchange["amount"] / current if current else float(i == 0)
            _set_amount(exchange, gross * target_weights.get(categories, 0.0) * share)
    reclassify_fuel_co2(
        working,
        gross,
        target_bio / gross if gross else 0.0,
        biosphere_flows,
        _VEHICLE_FUEL_KEY,
    )
    # Keep original order and unrelated exchanges; append only newly needed CO2.
    replacements = iter(working["exchanges"][: len(selected)])
    dataset["exchanges"] = [
        next(replacements) if _positive_air_co2(e) else e for e in dataset["exchanges"]
    ]
    dataset["exchanges"].extend(working["exchanges"][len(selected) :])


def update_vehicle_fuel_co2(database, version, vehicle_type=None):
    """Refresh road-vehicle CO2 from the currently linked suppliers and amounts."""
    from ..inventory_imports import get_biosphere_code

    providers = {activity_key(ds): ds for ds in database}
    flows = get_biosphere_code(version)
    for dataset in database:
        if not is_vehicle_fuel_consumer(dataset, vehicle_type):
            continue
        balance = vehicle_fuel_carbon(dataset, providers)
        if balance is not None and balance["status"] == "resolved":
            replace_vehicle_fuel_co2(dataset, balance, flows)
