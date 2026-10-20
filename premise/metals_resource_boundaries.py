"""Dataset-specific primary resource boundaries, separate from product matching.

These rules implement a declared resource-attribution convention. They never
alter supplier quantities or infer an ore grade from a desired LCIA result.
"""

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class ResourceBoundaryPlan:
    assignments: list = field(default_factory=list)
    reserved_ids: set = field(default_factory=set)
    diagnostics: list = field(default_factory=list)


def ground_resources(dataset, element):
    return [
        exchange
        for exchange in dataset.get("exchanges", [])
        if exchange.get("type") == "biosphere"
        and exchange.get("unit") == "kilogram"
        and tuple(exchange.get("categories", ())) == ("natural resource", "in ground")
        and exchange.get("name") == element
    ]


def material_inputs(dataset):
    return [
        exchange
        for exchange in dataset.get("exchanges", [])
        if exchange.get("type") == "technosphere"
        and exchange.get("unit") == "kilogram"
        and exchange.get("amount", 0) > 0
    ]


def provider(exchange, providers):
    matches = providers.get(
        (exchange.get("name"), exchange.get("product"), exchange.get("location")), []
    )
    return (
        matches[0]
        if len(matches) == 1 and matches[0].get("unit") == "kilogram"
        else None
    )


def unit_output(dataset):
    return (
        dataset.get("unit") == "kilogram"
        and dataset.get("production amount", 1) == 1
        and all(
            e.get("amount") == 1
            for e in dataset.get("exchanges", [])
            if e.get("type") == "production"
        )
    )


def build_resource_boundary_plan(database, available_flows):
    """Plan exact boundaries and reserve failed candidates from generic fallback."""
    plan = ResourceBoundaryPlan()
    providers = defaultdict(list)
    for dataset in database:
        providers[
            (
                dataset.get("name"),
                dataset.get("reference product"),
                dataset.get("location"),
            )
        ].append(dataset)

    for dataset in database:
        element = {"copper, cathode": "Copper", "silver": "Silver"}.get(
            dataset.get("reference product")
        )
        if (
            dataset.get("name") != "primary zinc production from concentrate"
            or not element
        ):
            continue
        plan.reserved_ids.add(id(dataset))
        feeds = material_inputs(dataset)
        primary = [e for e in feeds if e.get("product") == "zinc concentrate"]
        auxiliary = [
            e
            for e in feeds
            if element.lower() in (e.get("product") or e.get("name", "")).lower()
        ]
        valid = (
            unit_output(dataset)
            and element in available_flows
            and len(ground_resources(dataset, element)) <= 1
            and sum(e["amount"] for e in primary) >= 1
            and sum(e["amount"] for e in auxiliary) < 1
            and all(provider(e, providers) is not None for e in [*primary, *auxiliary])
        )
        if valid:
            plan.assignments.append(
                (dataset, element, 1.0, "primary_coproduct_reagent_guard")
            )
        else:
            plan.diagnostics.append((dataset, "primary_coproduct_boundary_unresolved"))
    return plan
