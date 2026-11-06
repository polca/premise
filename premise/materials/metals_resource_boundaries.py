"""Dataset-specific primary resource boundaries, separate from product matching.

These rules implement a declared resource-attribution convention. They never
alter supplier quantities or infer an ore grade from a desired LCIA result.
"""

from collections import defaultdict
from dataclasses import dataclass, field

from .metals_rules import load_resource_policy

_BOUNDARIES = load_resource_policy()["boundaries"]


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


LITHIUM_CARBONATE_FRACTION = _BOUNDARIES["LITHIUM_CARBONATE_FRACTION"]
LITHIUM_CARBONATE_FEEDS = _BOUNDARIES["LITHIUM_CARBONATE_FEEDS"]


def primary_lithium_feed(exchange, product, providers, visited=None):
    """Resolve every primary mineral-market branch; cycles are not absence."""
    dataset = provider(exchange, providers)
    visited = set() if visited is None else visited
    if dataset is None or id(dataset) in visited:
        return False
    if dataset.get("reference product") != product:
        return False
    if dataset.get("name", "").startswith(("market for ", "market group for ")):
        if any(not e.get("product") for e in material_inputs(dataset)):
            return False
        branches = [e for e in material_inputs(dataset) if e.get("product") == product]
        return bool(branches) and all(
            primary_lithium_feed(e, product, providers, visited | {id(dataset)})
            for e in branches
        )
    name = dataset.get("name", "").lower()
    if any(term in name for term in ("recycl", "treatment", "scrap", "waste", "used")):
        return False
    if name != _BOUNDARIES["lithium_primary_operation"] and not any(
        term in name for term in ("mine", "mining", "production", "extraction")
    ):
        return False
    # Brine is already a declared downstream-attribution carrier in metals.py.
    # A different mineral with its own Li extraction must not be counted twice.
    return product == _BOUNDARIES["lithium_carrier"] or not any(
        e.get("amount", 0) != 0 for e in ground_resources(dataset, "Lithium")
    )


def plan_lithium_boundary(dataset, providers, available_flows, plan):
    product = LITHIUM_CARBONATE_FEEDS.get(dataset.get("name"))
    if (
        product is None
        or dataset.get("reference product") != _BOUNDARIES["lithium_product"]
    ):
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


SODIUM_ARSENIDE_FRACTION = _BOUNDARIES["SODIUM_ARSENIDE_FRACTION"]


def plan_arsenic_boundary(dataset, providers, available_flows, plan):
    if (dataset.get("name"), dataset.get("reference product")) != tuple(
        _BOUNDARIES["arsenic_identity"]
    ):
        return
    plan.reserved_ids.add(id(dataset))
    feeds = material_inputs(dataset)
    zinc = [
        e for e in feeds if e.get("product") == _BOUNDARIES["arsenic_primary_product"]
    ]
    valid = (
        unit_output(dataset)
        and "Arsenic" in available_flows
        and len(ground_resources(dataset, "Arsenic")) <= 1
        and sum(e["amount"] for e in zinc) >= SODIUM_ARSENIDE_FRACTION
        and all(provider(e, providers) is not None for e in zinc)
        and not any(
            "arsen" in (e.get("product") or e.get("name", "")).lower() for e in feeds
        )
    )
    if valid:
        plan.assignments.append(
            (
                dataset,
                "Arsenic",
                SODIUM_ARSENIDE_FRACTION,
                "primary_sodium_arsenide_attribution",
            )
        )
    else:
        plan.diagnostics.append((dataset, "primary_arsenic_boundary_unresolved"))


SILICON_PRIMARY_NAMES = set(_BOUNDARIES["SILICON_PRIMARY_NAMES"])


def plan_silicon_boundary(dataset, providers, available_flows, plan):
    if (
        dataset.get("name") not in SILICON_PRIMARY_NAMES
        or dataset.get("reference product") != _BOUNDARIES["silicon_product"]
    ):
        return
    plan.reserved_ids.add(id(dataset))
    feeds = material_inputs(dataset)
    silica = [
        e for e in feeds if e.get("product") == _BOUNDARIES["silicon_primary_product"]
    ]
    silicon = [
        e for e in feeds if "silicon" in (e.get("product") or e.get("name", "")).lower()
    ]
    valid = (
        unit_output(dataset)
        and "Silicon" in available_flows
        and len(ground_resources(dataset, "Silicon")) <= 1
        and sum(e["amount"] for e in silica) >= 1
        and all(provider(e, providers) is not None for e in [*silica, *silicon])
        and sum(e["amount"] for e in silicon) < 1
    )
    if valid:
        plan.assignments.append(
            (dataset, "Silicon", 1.0, "primary_metallurgical_silicon_attribution")
        )
    else:
        plan.diagnostics.append((dataset, "primary_silicon_boundary_unresolved"))


RARE_EARTH_ATOMIC_MASSES = _BOUNDARIES["RARE_EARTH_ATOMIC_MASSES"]
RARE_EARTH_CARBONATE = _BOUNDARIES["RARE_EARTH_CARBONATE"]
RARE_EARTH_CARRIER_NAME = _BOUNDARIES["RARE_EARTH_CARRIER_NAME"]
RARE_EARTH_OXIDE_NAME = _BOUNDARIES["RARE_EARTH_OXIDE_NAME"]


def carbonate_branch_resolves(exchange, providers, carrier_ids, visited=None):
    dataset = provider(exchange, providers)
    visited = set() if visited is None else visited
    if dataset is None or id(dataset) in visited:
        return False
    if id(dataset) in carrier_ids:
        return True
    if (
        not dataset.get("name", "").startswith(("market for ", "market group for "))
        or dataset.get("reference product") != RARE_EARTH_CARBONATE
    ):
        return False
    if any(not e.get("product") for e in material_inputs(dataset)):
        return False
    branches = [
        e for e in material_inputs(dataset) if e.get("product") == RARE_EARTH_CARBONATE
    ]
    return bool(branches) and all(
        carbonate_branch_resolves(e, providers, carrier_ids, visited | {id(dataset)})
        for e in branches
    )


def plan_rare_earth_carbonate_branch(database, providers, available_flows, plan):
    """Validate the complete shared branch before transferring any REE resource."""
    carriers = [
        d
        for d in database
        if d.get("name") == RARE_EARTH_CARRIER_NAME
        and d.get("reference product") == RARE_EARTH_CARBONATE
    ]
    oxides = [d for d in database if d.get("name") == RARE_EARTH_OXIDE_NAME]
    if not carriers and not oxides:
        return
    candidates = [*carriers, *oxides]
    branch_ids = {id(d) for d in candidates}
    plan.reserved_ids.update(branch_ids)
    oxide_elements = {
        element.lower() + " oxide": element for element in RARE_EARTH_ATOMIC_MASSES
    }
    covered = {oxide_elements.get(d.get("reference product")) for d in oxides}
    required = {
        element
        for d in carriers
        for element in RARE_EARTH_ATOMIC_MASSES
        if ground_resources(d, element)
    }
    carrier_ids = {id(d) for d in carriers}
    valid = bool(carriers and oxides) and all(unit_output(d) for d in candidates)
    valid &= None not in covered and required <= covered and covered <= available_flows
    for d in carriers:
        valid &= all(len(ground_resources(d, element)) <= 1 for element in required)
    for d in oxides:
        element = oxide_elements.get(d.get("reference product"))
        primary = [
            e for e in material_inputs(d) if e.get("product") == RARE_EARTH_CARBONATE
        ]
        valid &= bool(primary) and all(
            carbonate_branch_resolves(e, providers, carrier_ids) for e in primary
        )
        valid &= len(ground_resources(d, element)) <= 1
    # Do not clear a shared feedstock that also supplies an unhandled route.
    for d in database:
        if id(d) in branch_ids or d.get("name", "").startswith(
            ("market for ", "market group for ")
        ):
            continue
        if any(e.get("product") == RARE_EARTH_CARBONATE for e in material_inputs(d)):
            valid = False
    if not valid:
        plan.diagnostics.extend(
            (d, "rare_earth_carbonate_boundary_unresolved") for d in candidates
        )
        return
    for d in carriers:
        for element in required:
            plan.assignments.append(
                (d, element, 0.0, "rare_earth_carbonate_carrier_transfer")
            )
    for d in oxides:
        element = oxide_elements[d["reference product"]]
        atoms, oxygen = _BOUNDARIES["rare_earth_oxide_atoms"].get(
            element, _BOUNDARIES["rare_earth_default_oxide_atoms"]
        )
        metal_mass = atoms * RARE_EARTH_ATOMIC_MASSES[element]
        fraction = metal_mass / (
            metal_mass + oxygen * _BOUNDARIES["oxygen_atomic_mass"]
        )
        plan.assignments.append(
            (d, element, fraction, "separated_rare_earth_oxide_attribution")
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
        plan_lithium_boundary(dataset, providers, available_flows, plan)
        plan_arsenic_boundary(dataset, providers, available_flows, plan)
        plan_silicon_boundary(dataset, providers, available_flows, plan)
        element = _BOUNDARIES["primary_coproduct_elements"].get(
            dataset.get("reference product")
        )
        if (
            dataset.get("name") != _BOUNDARIES["primary_coproduct_source"]
            or not element
        ):
            continue
        plan.reserved_ids.add(id(dataset))
        feeds = material_inputs(dataset)
        primary = [
            e
            for e in feeds
            if e.get("product") == _BOUNDARIES["primary_coproduct_feed"]
        ]
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
    plan_rare_earth_carbonate_branch(database, providers, available_flows, plan)
    return plan
