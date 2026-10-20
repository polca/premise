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


LITHIUM_CARBONATE_FRACTION = 2 * 6.94 / (2 * 6.94 + 12.011 + 3 * 15.999)
LITHIUM_CARBONATE_FEEDS = {
    "lithium carbonate production, from concentrated brine": "lithium brine, 6.7 % Li",
    "lithium carbonate production, from spodumene": "spodumene",
}


def primary_lithium_feed(exchange, product, providers, visited=None):
    """Resolve every primary mineral-market branch; cycles are not absence."""
    dataset = provider(exchange, providers)
    visited = set() if visited is None else visited
    if dataset is None or id(dataset) in visited:
        return False
    if dataset.get("reference product") != product:
        return False
    if dataset.get("name", "").startswith(("market for ", "market group for ")):
        branches = [e for e in material_inputs(dataset) if e.get("product") == product]
        return bool(branches) and all(
            primary_lithium_feed(e, product, providers, visited | {id(dataset)})
            for e in branches
        )
    name = dataset.get("name", "").lower()
    if any(term in name for term in ("recycl", "treatment", "scrap", "waste", "used")):
        return False
    if name != "lithium brine inspissation" and not any(
        term in name for term in ("mine", "mining", "production", "extraction")
    ):
        return False
    # Brine is already a declared downstream-attribution carrier in metals.py.
    # A different mineral with its own Li extraction must not be counted twice.
    return product == "lithium brine, 6.7 % Li" or not any(
        e.get("amount", 0) != 0 for e in ground_resources(dataset, "Lithium")
    )


def plan_lithium_boundary(dataset, providers, available_flows, plan):
    product = LITHIUM_CARBONATE_FEEDS.get(dataset.get("name"))
    if product is None or dataset.get("reference product") != "lithium carbonate":
        return
    plan.reserved_ids.add(id(dataset))
    feeds = material_inputs(dataset)
    primary = [e for e in feeds if e.get("product") == product]
    auxiliaries = [
        e
        for e in feeds
        if e not in primary
        and "lithium" in (e.get("product") or e.get("name", "")).lower()
    ]
    valid = (
        unit_output(dataset)
        and "Lithium" in available_flows
        and len(ground_resources(dataset, "Lithium")) <= 1
        and sum(e["amount"] for e in primary) >= LITHIUM_CARBONATE_FRACTION
        and all(primary_lithium_feed(e, product, providers) for e in primary)
        and sum(e["amount"] for e in auxiliaries) < LITHIUM_CARBONATE_FRACTION
        and all(provider(e, providers) is not None for e in auxiliaries)
    )
    if valid:
        plan.assignments.append(
            (
                dataset,
                "Lithium",
                LITHIUM_CARBONATE_FRACTION,
                "primary_lithium_carbonate_attribution",
            )
        )
    else:
        plan.diagnostics.append((dataset, "primary_lithium_boundary_unresolved"))


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
        plan_lithium_boundary(dataset, providers, available_flows, plan)
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
