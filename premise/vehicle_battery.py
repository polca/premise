"""Constant-range battery accounting, without a vehicle model at runtime.

The operating-electricity ratio before payload normalization includes the IAM
improvements. Scale usable capacity by that ratio without a mass/energy feedback.
BEV trucks retain payload utilization and normalize service inputs to their load.
Imported metadata supplies the 2020 design and precomputed wear relationships.
"""

import json
import math
import uuid
from collections.abc import Mapping
from functools import lru_cache

import numpy as np
from wurst import searching as ws

from .battery import load_cell_energy_density
from .inventory_copy import clone_inventory_dataset
from .utils import rescale_exchange

SPEC_KEY = "vehicle battery sizing"
STATE_KEY = "premise vehicle battery sizing"
CAPACITY_PRODUCT = "electricity storage capacity"


@lru_cache(maxsize=256)
def _read_specification(value):
    return json.loads(value)


def specification(dataset):
    value = dataset.get(SPEC_KEY)
    if value is None:
        return None
    spec = _read_specification(value) if isinstance(value, str) else value
    if not isinstance(spec, Mapping) or spec.get("version") != 1:
        raise ValueError("Unsupported vehicle battery sizing metadata")
    if spec.get("powertrain") not in {"BEV", "PHEV-p", "PHEV-d"}:
        raise ValueError("Range sizing requires a BEV or plug-in hybrid energy pack")
    if spec.get("baseline year") != 2020:
        raise ValueError("Battery sizing requires the 2020 reference design")
    grid = spec["grid kWh per service"]
    if not math.isfinite(grid) or grid <= 0:
        raise ValueError("Battery sizing requires positive reference electricity")
    return spec


@lru_cache(maxsize=1)
def _densities():
    return load_cell_energy_density()


@lru_cache(maxsize=128)
def cell_density(chemistry, year):
    targets = _densities()[
        f"market for battery capacity, Li-ion, {chemistry.replace('-', '')}"
    ]
    years = sorted(targets)
    return float(np.interp(year, years, [targets[y]["mean"] for y in years]))


def battery_state(spec, energy_factor, density, year):
    """Return a constant-range design, retaining BEV truck payload utilization."""
    positive = (
        energy_factor,
        density,
        spec["capacity kWh"],
        spec["pack mass kg"],
        spec["cell density kWh/kg"],
        spec["curb mass kg"],
        spec["driving mass kg"],
        spec["electric range km"],
    )
    if any(not math.isfinite(x) or x <= 0 for x in positive):
        raise ValueError("Battery sizing requires finite positive design values")
    replacements = spec["replacements"]
    if not math.isfinite(replacements) or replacements < 0:
        raise ValueError("Battery replacements must be finite and nonnegative")
    capacity = spec["capacity kWh"] * energy_factor
    mass = spec["pack mass kg"] * energy_factor * spec["cell density kWh/kg"] / density
    delta = mass - spec["pack mass kg"]
    curb = spec["curb mass kg"] + delta
    driving = spec["driving mass kg"] + delta
    payload = {}
    gross = spec.get("gross mass kg")
    if gross is not None:
        cargo = spec["cargo mass kg"]
        available = gross - spec["driving mass kg"] + cargo
        passengers = spec["driving mass kg"] - spec["curb mass kg"] - cargo
        if (
            any(not math.isfinite(x) or x <= 0 for x in (gross, cargo, available))
            or cargo > available + 1e-4
            or passengers < -1e-4
        ):
            raise ValueError("Invalid baseline truck payload or gross mass")
        future_available = available - delta
        if future_available <= 0:
            raise ValueError(
                "Resized battery leaves no payload within truck gross mass"
            )
        if spec.get("family") == "truck" and spec["powertrain"] == "BEV":
            utilization = cargo / available
            future_cargo = cargo - utilization * delta
            driving += future_cargo - cargo
            payload["payload normalization factor"] = cargo / future_cargo
        else:
            future_cargo = cargo
            utilization = cargo / future_available
        payload.update(
            {
                "cargo mass kg": future_cargo,
                "available payload kg": future_available,
                "capacity utilization": utilization,
            }
        )
    if min(curb, driving) <= 0 or curb < mass:
        raise ValueError("Resized battery is inconsistent with vehicle mass")
    if gross is not None and driving > gross + 1e-4:
        raise ValueError("Resized battery exceeds the approved truck gross mass")
    result = {
        "year": year,
        "energy factor": energy_factor,
        "capacity kWh": capacity,
        "pack mass kg": mass,
        "cell mass kg": mass * spec["cell mass share"],
        "balance of pack mass kg": mass * (1 - spec["cell mass share"]),
        "chemistry": spec["chemistry"],
        "cell density kWh/kg": density,
        "curb mass kg": curb,
        "driving mass kg": driving,
        "electric range km": spec["electric range km"],
        "replacements": replacements,
        "lifetime capacity kWh": capacity * (1 + replacements),
        "method": "constant-range accounting; IAM energy already includes mass effects",
    }
    result.update(payload)
    return result


def _product(exchange):
    return exchange.get("product", exchange.get("reference product", ""))


def electricity_per_service(dataset):
    production = sum(
        e["amount"] for e in dataset["exchanges"] if e["type"] == "production"
    )
    if not math.isfinite(production) or production <= 0:
        raise ValueError("Vehicle service requires positive production")
    electricity = 0.0
    for exc in dataset["exchanges"]:
        if (
            exc["type"] == "technosphere"
            and _product(exc) == "electricity, low voltage"
            and exc["unit"] == "kilowatt hour"
        ):
            if not math.isfinite(exc["amount"]) or exc["amount"] < 0:
                raise ValueError("Vehicle electricity must be finite and nonnegative")
            electricity += exc["amount"]
    return electricity / production


def mass_factors(spec, state):
    """Evaluate a few precomputed mass powers; no driving-cycle calculation."""
    delta = state["driving mass kg"] - spec["driving mass kg"]
    factors = {
        "curb": state["curb mass kg"] / spec["curb mass kg"],
        "driving": state["driving mass kg"] / spec["driving mass kg"],
    }
    for kind, terms in spec["wear terms"].items():
        # Each term holds its baseline share, mode mass and mass exponent.
        factors[kind] = sum(
            share * (1 + delta / mass) ** exponent for share, mass, exponent in terms
        )
    return factors


def _driver(exchange):
    if exchange.get("type") != "technosphere":
        return None
    product = _product(exchange)
    if product == "road" and exchange.get("unit") == "meter-year":
        return "driving"
    if product in {"passenger car maintenance", "assembly operation, for lorry"}:
        return "curb"
    for wear in ("brake", "road", "tyre"):
        if product in {
            f"{wear} wear emissions, passenger car",
            f"{wear} wear emissions, lorry",
        }:
            return wear
    return None


def _resize_mass_exchanges(dataset, spec, state):
    factors = mass_factors(spec, state)
    previous = dataset.get(STATE_KEY)
    old = mass_factors(spec, previous) if previous else dict.fromkeys(factors, 1.0)
    for exc in dataset["exchanges"]:
        driver = _driver(exc)
        if driver and factors[driver] != old[driver]:
            rescale_exchange(
                exc, factors[driver] / old[driver], remove_uncertainty=False
            )


def _record(dataset, state):
    dataset[STATE_KEY] = dict(state)
    payload_note = ""
    if "payload normalization factor" in state:
        payload_note = (
            f" BEV truck payload utilization remains {state['capacity utilization']:.6g}; "
            f"cargo is {state['cargo mass kg']:.6g} kg with "
            f"{state['available payload kg']:.6g} kg available payload. "
            "Service inputs are normalized to this cargo per tonne-kilometre. "
            "IAM electricity per vehicle-km is retained without further load feedback."
        )
    note = (
        f" Scenario battery design ({state['year']}, {dataset['location']}): "
        f"{state['capacity kWh']:.6g} kWh nominal, {state['pack mass kg']:.6g} kg pack; "
        f"{state['electric range km']:.6g} km electric range retained. "
        "IAM energy is the operating target before payload normalization. "
        "Other hardware, charging efficiency, DoD and distance usage retain the 2020 design."
        f"{payload_note} "
        "Original model specifications below describe the 2020 baseline."
    )
    old_note = dataset.get("premise battery sizing comment", "")
    comment = dataset.get("comment", "")
    if old_note:
        comment = comment.replace(old_note, "", 1)
    dataset["comment"] = note + comment
    dataset["premise battery sizing comment"] = note


def resize_regional_batteries(transport):
    """Update marked regional services and their own production suppliers once.

    Production location identifies the regional design. Its existing component
    supplier geographies are retained; it is not a manufacturing-location claim.
    Legacy inventories without a sizing contract are left on their own policy.
    """
    if transport.vehicle_type not in {"car", "truck"} or transport.year < 2020:
        return
    services = {}
    for datasets in transport.vehicle_map.values():
        for ds in datasets:
            if ds.get("regionalized") and not ds.get("emptied") and SPEC_KEY in ds:
                services[(ds["name"], ds["location"])] = ds
    if not services:
        return
    # Reuse the inventory's indexed unit query; never scan the full database per vehicle.
    providers = {
        (d["name"], d["reference product"], d["location"]): d
        for d in ws.get_many(transport.database, ws.equals("unit", "unit"))
    }
    additions = []
    for ds in services.values():
        spec = specification(ds)
        previous = ds.get(STATE_KEY)
        factor = electricity_per_service(ds) / spec["grid kWh per service"]
        if spec.get("family") == "truck" and spec["powertrain"] == "BEV":
            if ds["unit"] != "ton kilometer":
                raise ValueError(
                    "BEV truck payload normalization requires tonne-kilometres"
                )
            # Undo any previous per-tkm normalization before sizing from per-vkm energy.
            if previous:
                factor /= previous.get("payload normalization factor", 1.0)
        if previous and math.isclose(factor, previous["energy factor"], rel_tol=1e-12):
            factor = previous["energy factor"]
        if math.isclose(factor, 1, rel_tol=1e-12):
            factor = 1.0
        density = cell_density(spec["chemistry"], transport.year)
        # Preserve the exact float32 baseline used by the original model.
        if transport.year == 2020:
            density = spec["cell density kWh/kg"]
        state = battery_state(spec, factor, density, transport.year)
        if ds.get(STATE_KEY) == state:
            continue
        vehicles = [
            e
            for e in ds["exchanges"]
            if e["type"] == "technosphere"
            and e["unit"] == "unit"
            and (
                e["name"].lower().startswith("passenger car,")
                or e["name"].startswith("lorry production,")
            )
        ]
        if len(vehicles) != 1:
            raise ValueError(
                "Battery sizing requires exactly one vehicle-production supplier"
            )
        exchange = vehicles[0]
        source_key = exchange["name"], _product(exchange), exchange["location"]
        source = providers[source_key]
        _resize_mass_exchanges(ds, spec, state)
        normalization = state.get("payload normalization factor", 1.0)
        old_normalization = (
            previous.get("payload normalization factor", 1.0) if previous else 1.0
        )
        if normalization != old_normalization:
            for exc in ds["exchanges"]:
                if exc["type"] != "production":
                    rescale_exchange(
                        exc, normalization / old_normalization, remove_uncertainty=False
                    )
        _record(ds, state)
        if factor == 1 and density == spec["cell density kWh/kg"]:
            continue
        target_key = source_key[:2] + (ds["location"],)
        if target_key in providers:
            target = providers[target_key]
            if target.get(STATE_KEY) != state:
                raise ValueError(f"Conflicting regional battery designs: {target_key}")
        else:
            target = clone_inventory_dataset(source)
            target["location"] = ds["location"]
            target["code"] = uuid.uuid4().hex
            target["regionalized"] = True
            target.pop("input", None)
            for exc in target["exchanges"]:
                if exc["type"] == "production":
                    exc["location"] = target["location"]
                    exc.pop("input", None)
            purchases = [
                e
                for e in target["exchanges"]
                if e["type"] == "technosphere"
                and _product(e) == CAPACITY_PRODUCT
                and e["unit"] == "kilowatt hour"
            ]
            if len(purchases) != 1:
                raise ValueError(
                    "Range-sized vehicles require one battery capacity market"
                )
            expected = spec["capacity kWh"] * (1 + spec["replacements"])
            if not math.isclose(purchases[0]["amount"], expected, rel_tol=2e-6):
                raise ValueError(
                    "Production battery purchase disagrees with baseline metadata"
                )
            rescale_exchange(purchases[0], factor, remove_uncertainty=False)
            purchases[0]["comment"] = (
                f"{state['capacity kWh']:.12g} kWh nominal * "
                f"(1 + {state['replacements']:.12g} lifetime replacements), "
                f"{transport.year} regional constant-range design. "
                "Capacity market includes manufacture and end of life. "
                "Physical pack chemistry/density and the MIX supply recipe are distinct."
            )
            _resize_mass_exchanges(target, spec, state)
            _record(target, state)
            target[
                "comment"
            ] += " Production location denotes regional design; original component supplier geographies are retained."
            providers[target_key] = target
            additions.append(target)
        exchange["location"] = target["location"]
        exchange.pop("input", None)
    if additions:
        transport.database.extend(additions)
        transport.add_to_index(additions)
