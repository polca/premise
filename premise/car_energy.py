"""Passenger-car fuel accounting and a provisional IAM consumption floor.

No database-wide index or scan: callers reuse their existing vehicle traversal.
Fuel properties and name resolution are cached. Generic fuel markets do not
establish a fossil/biogenic split; preserve existing combustion-flow shares.
"""

import math
from functools import lru_cache

import yaml

from .filesystem_constants import DATA_DIR, VARIABLES_DIR


@lru_cache(maxsize=1)
def floor_config():
    with (DATA_DIR / "transport" / "car_energy_floor.yaml").open() as handle:
        return yaml.safe_load(handle)


@lru_cache(maxsize=1)
def fuel_properties():
    with (VARIABLES_DIR / "fuels.yaml").open() as handle:
        return yaml.safe_load(handle)


@lru_cache(maxsize=2048)
def car_class(name):
    if not name.startswith("transport, passenger car, "):
        return None
    parts = name.split(", ")
    if len(parts) < 4 or parts[2] not in ("diesel", "gasoline", "compressed gas"):
        return None
    return parts[2], parts[3].lower()


@lru_cache(maxsize=4096)
def fuel_spec(name, unit):
    if not name.startswith(("market for ", "market group for ")):
        return None
    stem = name.removeprefix("market for ").removeprefix("market group for ")
    family = next(
        (
            f
            for prefix, f in (
                ("diesel", "diesel"),
                ("petrol", "gasoline"),
                ("gasoline", "gasoline"),
                ("natural gas", "natural gas"),
            )
            if stem == prefix or stem.startswith(prefix + ",")
        ),
        None,
    )
    if family is None:
        return None
    props = fuel_properties()[family]
    if family == "natural gas" and unit == "kilogram":
        # Existing transport convention, MJ/kg; packaged gas LHV is MJ/m3.
        lhv = 47.5
    elif (family == "natural gas" and unit == "cubic meter") or (
        family != "natural gas" and unit == "kilogram"
    ):
        lhv = float(props["lhv"]["value"])
    else:
        raise ValueError(f"Unsupported car fuel unit: {name!r}, {unit!r}")
    factor = float(props["co2"])
    if not math.isfinite(lhv) or lhv <= 0 or not math.isfinite(factor) or factor <= 0:
        raise ValueError(f"Invalid fuel properties: {family}")
    return lhv, factor


def fuel_balance(dataset):
    """Return MJ/km, expected total kg CO2/km, and selected fuel exchanges.

    The CO2 check uses packaged complete-combustion factors and deliberately
    makes no claim about the fossil/biogenic split of generic market blends.
    """
    if dataset.get("unit") != "kilometer":
        raise ValueError("Car fuel accounting requires vehicle-kilometre activities")
    production = 0.0
    production_count = 0
    energy = co2 = 0.0
    selected = []
    for exc in dataset["exchanges"]:
        kind = exc.get("type")
        if kind == "production":
            production_count += 1
            if exc.get("unit", "kilometer") != "kilometer":
                raise ValueError("Car reference production must be in kilometres")
            production += float(exc["amount"])
        elif kind == "technosphere":
            spec = fuel_spec(exc.get("name", ""), exc.get("unit", ""))
            if spec is None:
                continue
            amount = float(exc["amount"])
            if not math.isfinite(amount) or amount < 0:
                raise ValueError("Car fuel amounts must be finite and nonnegative")
            energy += amount * spec[0]
            co2 += amount * spec[0] * spec[1]
            selected.append(exc)
    if production_count != 1 or not math.isfinite(production) or production <= 0:
        raise ValueError("Car activity requires positive reference production")
    if not selected or energy <= 0:
        raise ValueError("Combustion car has no positive supported fuel input")
    return energy / production, co2 / production, selected, production


def minimum_energy(dataset, config=None):
    config = floor_config() if config is None else config
    if not config["enabled"] or car_class(dataset["name"]) is None:
        return None
    powertrain, size = car_class(dataset["name"])
    overrides = config.get("overrides", {}).get(powertrain, {})
    value = float(
        overrides.get(
            size, overrides.get("default", config["minimum_mj_per_vehicle_km"])
        )
    )
    if not math.isfinite(value) or value <= 0:
        raise ValueError("Car energy floor must be positive and finite")
    return value


def apply_floor(dataset, config=None):
    """Clamp fuel use, preserving non-exhaust burdens and uncertainty fields."""
    minimum = minimum_energy(dataset, config)
    if minimum is None:
        return None
    energy, _, fuels, _ = fuel_balance(dataset)
    if energy >= minimum or math.isclose(energy, minimum, rel_tol=1e-12):
        return None
    from .utils import rescale_exchange

    ratio = minimum / energy
    changed = []
    for exc in fuels:
        rescale_exchange(exc, ratio, remove_uncertainty=False)
        changed.append(exc["name"])
    # Only exhaust air flows, not tyre/brake wear, road dust, noise or resources.
    exhaust = (
        "Carbon dioxide",
        "Carbon monoxide",
        "Methane",
        "Nitrogen oxides",
        "Dinitrogen monoxide",
        "Sulfur dioxide",
        "Ammonia",
        "NMVOC",
    )
    for exc in dataset["exchanges"]:
        if (
            exc.get("type") == "biosphere"
            and exc.get("categories", [None])[0] == "air"
            and exc.get("name", "").startswith(exhaust)
        ):
            rescale_exchange(exc, ratio, remove_uncertainty=False)
            changed.append(exc["name"])
    event = {
        "projected energy MJ/km": energy,
        "minimum energy MJ/km": minimum,
        "final energy MJ/km": minimum,
        "floor correction factor": ratio,
        "affected exchanges": changed,
        "carbon split": "retained; generic fuel-market composition not inferred",
    }
    dataset.setdefault("log parameters", {})["car energy floor"] = event
    dataset["comment"] = dataset.get("comment", "") + (
        f" Provisional car energy floor applied: {energy:.8g} -> {minimum:.8g} MJ/vehicle-km."
    )
    return event
