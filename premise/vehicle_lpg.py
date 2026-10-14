"""Describe and validate the explicit LPG vehicle fallback after final linking."""

import json
import math
from functools import lru_cache

import yaml

from .filesystem_constants import VARIABLES_DIR
from .fuels.carbon import vehicle_fuel_carbon
from .fuels.properties import PROPERTIES_KEY, activity_key
from .vehicle_energy import vehicle_energy_exchange

SPEC_KEY = "vehicle lpg policy"
STATE_KEY = "premise vehicle LPG supply"
COMMENT_KEY = "premise vehicle LPG comment"
LPG = "liquefied petroleum gas"


@lru_cache(maxsize=32)
def _decode(value):
    return json.loads(value)


@lru_cache(maxsize=2)
def _mapping(family):
    filename = (
        "transport_passenger_cars.yaml"
        if family == "car"
        else "transport_road_freight.yaml"
    )
    with (VARIABLES_DIR / filename).open() as stream:
        return yaml.safe_load(stream)


def services_and_providers(database, vehicle_type):
    prefix = (
        "transport, passenger car,"
        if vehicle_type == "car"
        else "transport, freight, lorry,"
    )
    services = [
        d for d in database if SPEC_KEY in d and d.get("name", "").startswith(prefix)
    ]
    wanted = {
        activity_key(e)
        for ds in services
        for e in ds["exchanges"]
        if vehicle_energy_exchange(e) and e.get("unit") != "kilowatt hour"
    }
    providers = (
        {activity_key(d): d for d in database if activity_key(d) in wanted}
        if wanted
        else {}
    )
    return services, providers


def describe_lpg_supply(dataset, providers, context):
    """Return fresh coverage and balances; never infer fuel identity from a proxy model."""
    if dataset.get("emptied"):
        return None
    spec = dataset[SPEC_KEY]
    spec = _decode(spec) if isinstance(spec, str) else spec
    if (
        spec.get("version") != 1
        or spec.get("fuel") != LPG
        or spec.get("baseline year") != 2020
    ):
        raise ValueError("Unsupported LPG vehicle policy")
    if dataset["unit"] != spec["reference unit"]:
        raise ValueError("LPG service unit disagrees with baseline")
    for field in (
        "fuel kg per service",
        "energy MJ per service",
        "lhv MJ per kg",
        "fossil CO2 kg per kg",
    ):
        if not math.isfinite(spec[field]) or spec[field] <= 0:
            raise ValueError(f"Invalid LPG baseline {field}")
    if not math.isclose(
        spec["fuel kg per service"] * spec["lhv MJ per kg"],
        spec["energy MJ per service"],
        rel_tol=1e-12,
    ):
        raise ValueError("Inconsistent LPG baseline energy")
    fuel = [
        e
        for e in dataset["exchanges"]
        if vehicle_energy_exchange(e) and e["unit"] != "kilowatt hour"
    ]
    if not fuel and any(
        e.get("type") == "technosphere"
        and e.get("product", e.get("reference product")) == dataset["reference product"]
        and e["unit"] == dataset["unit"]
        for e in dataset["exchanges"]
    ):
        return None
    if not fuel:
        raise ValueError("LPG vehicle has no direct fuel purchase")
    for exc in fuel:
        if (
            exc.get("product", exc.get("reference product")) != LPG
            or exc["unit"] != "kilogram"
            or exc["name"] not in {f"market for {LPG}", f"market group for {LPG}"}
        ):
            raise ValueError(
                "LPG vehicle must purchase LPG in kilograms, not its proxy's fuel"
            )
        if activity_key(exc) not in providers:
            raise ValueError(f"Missing final LPG supplier: {activity_key(exc)}")
    balance = vehicle_fuel_carbon(dataset, providers)
    if balance is None or balance["status"] != "resolved":
        raise ValueError(f"Unresolved LPG carbon properties: {balance}")
    production = balance["production amount"]
    energy, sources = 0.0, []
    for exc, carbon in zip(fuel, balance["suppliers"]):
        properties = providers[activity_key(exc)].get(PROPERTIES_KEY)
        fallback = properties is None
        lhv = (
            spec["lhv MJ per kg"] if fallback else properties.get("energy MJ per unit")
        )
        if lhv is None or not math.isfinite(lhv) or lhv <= 0:
            raise ValueError("LPG supplier lacks a valid delivered-energy basis")
        if fallback and not math.isclose(
            carbon["fossil CO2 kg per unit"],
            spec["fossil CO2 kg per kg"],
            rel_tol=1e-12,
        ):
            raise ValueError(
                "LPG fallback carbon disagrees with its approved reference"
            )
        energy += exc["amount"] * lhv / production
        sources.append(
            {
                **carbon,
                "energy MJ per unit": lhv,
                "supply mode": (
                    "fossil fallback" if fallback else "linked supplier properties"
                ),
            }
        )
    family = (
        "car" if dataset["name"].startswith("transport, passenger car,") else "truck"
    )
    mapping = _mapping(family)[spec["IAM technology"]]
    mapped = context.model in mapping.get(
        "iam_aliases", {}
    ) and context.model in mapping.get("energy_use_aliases", {})
    log = dataset.get("log parameters", {})
    factor = log.get("efficiency change", 1)
    if not math.isfinite(factor) or factor <= 0:
        raise ValueError("Invalid LPG efficiency adjustment")
    if not mapped and (
        not math.isclose(factor, 1, rel_tol=1e-12)
        or log.get("IAM efficiency source", {}).get("kind", "unavailable")
        != "unavailable"
    ):
        raise ValueError("Unmapped LPG vehicle claims an IAM efficiency adjustment")
    if not math.isclose(
        energy, spec["energy MJ per service"] * factor, rel_tol=1e-6, abs_tol=1e-12
    ):
        raise ValueError(
            "LPG fuel energy changed without the recorded LPG efficiency trend"
        )
    for exc in dataset["exchanges"]:
        if (
            exc.get("type") == "biosphere"
            and exc.get("name") in {"Methane, fossil", "Methane, non-fossil"}
            and tuple(exc.get("categories", ())) in {("air",), ("air", "unspecified")}
            and abs(exc["amount"]) > 1e-15
        ):
            raise ValueError(
                "LPG proxy retains excluded CNG pump-to-tank methane leakage"
            )
    for name, field in (
        ("Carbon dioxide, fossil", "fossil CO2 kg"),
        ("Carbon dioxide, non-fossil", "non-fossil CO2 kg"),
    ):
        amounts = [
            e["amount"]
            for e in dataset["exchanges"]
            if e.get("type") == "biosphere"
            and e["name"] == name
            and e.get("unit") == "kilogram"
            and tuple(e.get("categories", ()))[:1] == ("air",)
        ]
        if any(not math.isfinite(a) for a in amounts) or not math.isclose(
            sum(a for a in amounts if a >= 0),
            balance[field],
            rel_tol=1e-8,
            abs_tol=1e-12,
        ):
            raise ValueError(f"LPG {name} is inconsistent with the final supplier")
    modes = {s["supply mode"] for s in sources}
    return {
        "status": "resolved",
        "model": context.model,
        "scenario": context.scenario,
        "year": context.year,
        "vehicle location": dataset["location"],
        "reference unit": dataset["unit"],
        "baseline year": 2020,
        "IAM technology": spec["IAM technology"],
        "LPG efficiency mapping": "declared" if mapped else "unavailable",
        "applied energy factor": factor,
        "supply mode": next(iter(modes)) if len(modes) == 1 else "mixed",
        "energy MJ per service": energy,
        "fossil CO2 kg per service": balance["fossil CO2 kg"] / production,
        "non-fossil CO2 kg per service": balance["non-fossil CO2 kg"] / production,
        "suppliers": sources,
        "proxy boundary": spec["proxy boundary"],
        "CNG pump-to-tank methane leakage": spec["CNG pump-to-tank methane leakage"],
    }


def record_lpg_supply(transport):
    """Add current final-link coverage to marked services, including unmapped templates."""
    services, providers = services_and_providers(
        transport.database, transport.vehicle_type
    )
    for dataset in services:
        old_note = dataset.pop(COMMENT_KEY, "")
        comment = (
            dataset.get("comment", "").replace(old_note, "", 1)
            if old_note
            else dataset.get("comment", "")
        )
        try:
            state = describe_lpg_supply(dataset, providers, transport)
        except (KeyError, TypeError, ValueError) as error:
            state = {"status": "invalid", "reason": str(error)}
        if state is None:
            dataset.pop(STATE_KEY, None)
            dataset["comment"] = comment
            continue
        dataset[STATE_KEY] = state
        if state["status"] == "resolved":
            suppliers = "; ".join(
                f"{s['name']} ({s['location']}, {s['unit']})"
                for s in state["suppliers"]
            )
            coverage = (
                "No scenario LPG blend is represented by these fossil suppliers. "
                if state["supply mode"] == "fossil fallback"
                else "Carbon origin follows the linked supplier properties. "
            )
            efficiency = (
                "No LPG efficiency mapping for this IAM; 2020 operating energy retained. "
                if state["LPG efficiency mapping"] == "unavailable"
                else f"Explicit LPG efficiency mapping; applied energy multiplier {state['applied energy factor']:.8g}. "
            )
            note = f" LPG supply ({transport.model}, {transport.scenario}, {transport.year}): {suppliers}. {coverage}{efficiency}"
        else:
            note = f" LPG supply validation failed: {state['reason']}. "
        dataset[COMMENT_KEY] = note
        dataset["comment"] = note + comment
