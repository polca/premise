"""Read nominal heat-conversion factors through explicitly reviewed energy routes."""

from __future__ import annotations

import copy
import math
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass

Identity = tuple[str, str, str, str]


def _identity(record: Mapping) -> Identity:
    return (
        record["name"],
        record.get("reference product", record.get("product")),
        record["location"],
        record["unit"],
    )


@dataclass(frozen=True)
class HeatEnergyBasis:
    """Nominal delivered MJ per input MJ, with the inventory evidence used."""

    delivered_heat_factor: float
    input_MJ_per_delivered_MJ: float
    witnesses: tuple[dict, ...]


def resolve_heat_energy_basis(
    database: list[dict],
    supplier: Identity,
    *,
    routes: Mapping[Identity, tuple[Identity, ...]],
    carrier_MJ_per_unit: Mapping[Identity, float],
) -> HeatEnergyBasis:
    """Evaluate a reviewed delivered-heat to fuel/electricity boundary, read-only.

    ``routes`` declares the energy-bearing suppliers to follow at each service.
    Every route edge must exist as a linked technosphere input. Other inputs
    remain outside this *conversion* boundary and are listed in the witnesses;
    their complete LCI is not removed or changed. No name/substring, geography,
    unit, LHV or allocation inference takes place here.

    Carrier values are reviewed MJ per supplier output unit (for example 3.6
    for kWh electricity or 1 for an MJ fuel). Nested services are divided by
    their actual production quantities. This computes nominal factors; it does
    not propagate upstream uncertainty or establish a proxy's suitability.
    """
    if supplier[3] != "megajoule" or supplier not in routes:
        raise ValueError("Delivered heat requires a reviewed MJ service route.")
    if set(routes) & set(carrier_MJ_per_unit):
        raise ValueError("An energy carrier cannot also be an intermediate service.")
    for value in carrier_MJ_per_unit.values():
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Reviewed carrier energy must be finite and positive.")

    identities = defaultdict(list)
    linked = defaultdict(list)
    for dataset in database:
        identities[_identity(dataset)].append(dataset)
        if dataset.get("database") is not None and dataset.get("code") is not None:
            linked[(dataset["database"], dataset["code"])].append(dataset)
    witnesses = []

    def unique(key):
        candidates = identities.get(key, [])
        if len(candidates) != 1:
            raise ValueError(f"Missing or ambiguous reviewed energy supplier: {key!r}.")
        return candidates[0]

    def production(dataset):
        outputs = [e for e in dataset["exchanges"] if e["type"] == "production"]
        if len(outputs) != 1 or _identity(outputs[0]) != _identity(dataset):
            raise ValueError("Energy service needs exactly one matching production.")
        amount = outputs[0]["amount"]
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Energy reference production must be finite and positive.")
        return amount

    def visit(key, trail):
        if key in trail:
            raise ValueError("Cycle in reviewed heat-energy routes.")
        dataset = unique(key)
        reference_amount = production(dataset)
        if key in carrier_MJ_per_unit:
            witnesses.append(
                {
                    "supplier": key,
                    "production_amount": reference_amount,
                    "reviewed_carrier_MJ_per_unit": carrier_MJ_per_unit[key],
                }
            )
            return carrier_MJ_per_unit[key]
        targets = routes.get(key)
        if not targets or len(set(targets)) != len(targets):
            raise ValueError("Missing or duplicate reviewed energy route targets.")
        selected = defaultdict(list)
        excluded = []
        for exchange in dataset["exchanges"]:
            if exchange["type"] != "technosphere":
                continue
            target = _identity(exchange)
            if target not in targets:
                excluded.append(target)
                continue
            expected = unique(target)
            if exchange.get("input") is not None:
                actual = linked.get(tuple(exchange["input"]), [])
                if len(actual) != 1 or actual[0] is not expected:
                    raise ValueError(
                        "Energy input link disagrees with reviewed identity."
                    )
            amount = exchange["amount"]
            if not math.isfinite(amount) or amount < 0:
                raise ValueError(
                    "Negative/nonfinite energy route needs a separate review."
                )
            selected[target].append(exchange)
        if set(selected) != set(targets):
            raise ValueError(
                "A reviewed energy route is absent from the actual service."
            )
        contributions = []
        for target in targets:
            energy_per_unit = visit(target, (*trail, key))
            for exchange in selected[target]:
                contributions.append(exchange["amount"] * energy_per_unit)
        energy = math.fsum(contributions) / reference_amount
        if not math.isfinite(energy) or energy <= 0:
            raise ValueError("Reviewed service has no finite positive energy draw.")
        witnesses.append(
            {
                "supplier": key,
                "production_amount": reference_amount,
                "energy_routes": [
                    {"supplier": target, "exchanges": copy.deepcopy(selected[target])}
                    for target in targets
                ],
                "other_technosphere_inputs_outside_conversion_boundary": excluded,
                "input_MJ_per_output_unit": energy,
            }
        )
        return energy

    energy = visit(supplier, ())
    factor = 1.0 / energy
    if not math.isfinite(factor):
        raise ValueError("Delivered-heat factor is not finite.")
    return HeatEnergyBasis(factor, energy, tuple(witnesses))
