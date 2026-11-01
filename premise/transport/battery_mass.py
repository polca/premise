"""Activate scenario capacity purchases from explicitly marked static inventories.

Imported inventories retain an NMC622 complete-pack mass proxy and its retirement.
The declared baseline conversion restores the original lifetime capacity purchase;
it is not a density estimate for the native pack or for the scenario year.
"""

import hashlib
import json
import math
from collections.abc import Mapping

import numpy as np
from wurst import searching as ws

from ..inventory.inventory_copy import clone_inventory_dataset
from ..core.utils import rescale_exchange

SPEC_KEY = "battery mass to capacity"
STATE_KEY = "premise battery mass to capacity"
MASS_PROVIDER = {
    "name": "market for battery, Li-ion, NMC622, rechargeable",
    "reference product": "battery, Li-ion, NMC622, rechargeable",
    "location": "GLO",
    "unit": "kilogram",
}
# The 3.10 -> 3.11 migration renames the same complete-pack supplier.
LEGACY_MASS_PROVIDER = {
    **MASS_PROVIDER,
    "name": "market for battery, Li-ion, NMC622",
    "reference product": "battery, Li-ion, NMC622",
}
RETIREMENT_PROVIDER = {
    "name": "market for used Li-ion battery",
    "reference product": "used Li-ion battery",
    "location": "GLO",
    "unit": "kilogram",
}
CAPACITY_PRODUCT = "electricity storage capacity"
HIGH_POWER_CAPACITY = {
    (
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
}
CAPACITY_PRODUCTS = {CAPACITY_PRODUCT} | {p for _, p in HIGH_POWER_CAPACITY}


def _identity(record):
    if not isinstance(record, Mapping):
        raise ValueError("Invalid battery supplier identity")
    product = record.get("product", record.get("reference product"))
    if "product" in record and "reference product" in record:
        if record["product"] != record["reference product"]:
            raise ValueError("Battery supplier product fields disagree")
    return record.get("name"), product, record.get("location"), record.get("unit")


def _number(value, label, *, positive=True):
    if (
        isinstance(value, (bool, np.bool_))
        or not isinstance(value, (int, float, np.floating))
        or not math.isfinite(value)
        or (value <= 0 if positive else value >= 0)
    ):
        raise ValueError(f"Battery conversion requires finite {label}")
    return float(value)


def _matching_amount(value, expected, label, *, positive=True):
    observed = _number(value, label, positive=positive)
    if isinstance(value, np.float32):
        # Legacy source caches deliberately downcast exchange amounts while
        # keeping the JSON baseline at full precision. Accept exactly that
        # known representation, not an arbitrary wider matching tolerance.
        return observed == float(np.float32(expected))
    return math.isclose(observed, expected, rel_tol=1e-12)


def _specification(dataset):
    raw = dataset[SPEC_KEY]
    try:
        spec = json.loads(raw) if isinstance(raw, str) else dict(raw)
    except (TypeError, ValueError) as error:
        raise ValueError("Invalid battery mass-to-capacity metadata") from error
    if not isinstance(spec, Mapping) or type(spec.get("version")) is not int:
        raise ValueError("Invalid battery mass-to-capacity metadata version")
    if spec["version"] != 1:
        raise ValueError("Unsupported battery mass-to-capacity metadata version")
    if spec.get("vehicle family") not in {
        "car",
        "truck",
        "bus",
        "train",
        "two-wheeler",
        "ship",
    }:
        raise ValueError("Battery conversion requires an explicit vehicle family")
    mass = _number(spec.get("baseline mass kg"), "positive baseline mass")
    capacity = _number(spec.get("baseline capacity kWh"), "positive baseline capacity")
    retirement = _number(
        spec.get("retirement amount kg"), "negative retirement", positive=False
    )
    if not math.isclose(retirement, -mass, rel_tol=1e-12):
        raise ValueError("Battery retirement does not match its baseline mass")
    supplier = spec.get("capacity supplier")
    if not isinstance(supplier, Mapping):
        raise ValueError("Battery conversion requires a capacity supplier")
    key = _identity(supplier)
    if (
        not isinstance(key[0], str)
        or not (
            (
                key[0].startswith("market for battery capacity")
                and key[1] == CAPACITY_PRODUCT
            )
            or key[:2] in HIGH_POWER_CAPACITY
        )
        or not isinstance(key[2], str)
        or not key[2]
        or key[3] != "kilowatt hour"
    ):
        raise ValueError("Invalid battery capacity supplier identity")
    if spec.get("capacity supplier includes end of life") is not True:
        raise ValueError("Battery capacity retirement ownership must be explicit")
    if _identity(spec.get("mass supplier", MASS_PROVIDER)) != _identity(MASS_PROVIDER):
        raise ValueError("Only the declared NMC622 pack mass proxy is supported")
    if _identity(spec.get("retirement supplier", RETIREMENT_PROVIDER)) != _identity(
        RETIREMENT_PROVIDER
    ):
        raise ValueError("Unsupported battery retirement supplier")
    return spec, mass, capacity, retirement, key


def _matches(exchange, identity):
    return exchange.get("type") == "technosphere" and _identity(exchange) == identity


def preserve_cached_battery_amount(dataset, exchange):
    """Retain source-cache precision until the explicit conversion validates it."""
    return (
        SPEC_KEY in dataset
        and STATE_KEY not in dataset
        and isinstance(exchange.get("amount"), np.float32)
        and any(
            _matches(exchange, _identity(provider))
            for provider in (MASS_PROVIDER, LEGACY_MASS_PROVIDER, RETIREMENT_PROVIDER)
        )
    )


def _plan_conversion(dataset, parsed):
    spec, mass, capacity, retirement, supplier = parsed
    fingerprint = hashlib.sha256(
        json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    # Compact inventories may create a fresh exchange view on each iteration.
    # Capture them once so replacing/removing the selected records is reliable.
    exchanges = list(dataset["exchanges"])
    purchases = [
        e
        for e in exchanges
        if any(
            _matches(e, _identity(provider))
            for provider in (MASS_PROVIDER, LEGACY_MASS_PROVIDER)
        )
    ]
    waste = [e for e in exchanges if _matches(e, _identity(RETIREMENT_PROVIDER))]
    capacities = [
        e
        for e in exchanges
        if e.get("type") == "technosphere" and _identity(e)[1] in CAPACITY_PRODUCTS
    ]
    state = dataset.get(STATE_KEY)
    if state is not None:
        if (
            not isinstance(state, Mapping)
            or state.get("version") != 1
            or state.get("specification sha256") != fingerprint
            or purchases
            or waste
            or len(capacities) != 1
            or _identity(capacities[0]) != supplier
        ):
            raise ValueError(
                "Converted battery inventory disagrees with its provenance"
            )
        _number(capacities[0]["amount"], "positive runtime capacity")
        return None
    if len(purchases) != 1 or len(waste) != 1 or capacities:
        raise ValueError(
            "Battery conversion requires one mass purchase, one retirement and no capacity purchase"
        )
    purchase, discarded = purchases[0], waste[0]
    if not _matching_amount(purchase.get("amount"), mass, "positive pack purchase"):
        raise ValueError("Battery mass purchase disagrees with its conversion baseline")
    if not _matching_amount(
        discarded.get("amount"),
        retirement,
        "negative retirement",
        positive=False,
    ):
        raise ValueError("Battery retirement disagrees with its conversion baseline")
    if purchase.get("formula") or discarded.get("formula"):
        raise ValueError(
            "Executable battery formulas require a separate capacity conversion"
        )
    if discarded.get("uncertainty type") not in (None, 0, 1):
        raise ValueError(
            "Independent battery retirement uncertainty cannot be absorbed into capacity supply"
        )
    replacement = clone_inventory_dataset(dict(purchase))
    rescale_exchange(
        replacement, capacity / float(purchase["amount"]), remove_uncertainty=False
    )
    # The literal capacity is the original full-lifetime purchase, not an
    # estimate obtained by multiplying rounded coefficients.
    replacement["amount"] = capacity
    replacement.update(
        name=supplier[0], product=supplier[1], location=supplier[2], unit=supplier[3]
    )
    if "reference product" in replacement:
        replacement["reference product"] = supplier[1]
    for field in ("input", "output", "database", "code"):
        replacement.pop(field, None)
    original_comment = replacement.get("comment", "")
    replacement["comment"] = (
        f"Runtime battery conversion: {mass:.12g} kg static NMC622 proxy -> "
        f"{capacity:.12g} kWh baseline lifetime capacity. The capacity supplier "
        "includes manufacture and retirement; the matching foreground retirement "
        "is removed once. Scenario density is applied in capacity suppliers, "
        "not in this baseline conversion. " + original_comment
    )
    result = [
        replacement if e is purchase else e for e in exchanges if e is not discarded
    ]
    state = {
        "version": 1,
        "specification sha256": fingerprint,
        "baseline mass kg": mass,
        "baseline capacity kWh": capacity,
        "original mass exchange": clone_inventory_dataset(dict(purchase)),
        "original retirement exchange": clone_inventory_dataset(dict(discarded)),
    }
    return result, state


def convert_battery_mass_inputs(database, vehicle_type=None):
    """Convert only opt-in additional inventories, atomically and once per family.

    Unmarked native activities and legacy capacity inventories are untouched.
    All selected contracts and target suppliers are checked before any write.
    """
    selected = []
    # Vehicle-production references are in units. Use the compact inventory's
    # indexed query rather than decoding metadata for every background activity.
    for dataset in ws.get_many(database, ws.equals("unit", "unit")):
        if SPEC_KEY not in dataset:
            continue
        parsed = _specification(dataset)
        if vehicle_type is None or parsed[0]["vehicle family"] == vehicle_type:
            selected.append((dataset, parsed))
    if not selected:
        return 0
    suppliers = {}
    for dataset in ws.get_many(database, ws.equals("unit", "kilowatt hour")):
        key = _identity(dataset)
        if key[1] in CAPACITY_PRODUCTS:
            suppliers[key] = suppliers.get(key, 0) + 1
    plans = []
    for dataset, parsed in selected:
        plan = _plan_conversion(dataset, parsed)
        if plan is None:
            continue
        if suppliers.get(parsed[-1], 0) != 1:
            raise ValueError(
                f"Battery capacity supplier missing or ambiguous: {parsed[-1]}"
            )
        plans.append((dataset, plan))
    for dataset, (exchanges, state) in plans:
        dataset["exchanges"] = exchanges
        dataset[STATE_KEY] = state
    return len(plans)
