"""Read-only applicability evidence for preserved metal resource inventories."""

import math
import re
from collections import defaultdict, deque

from .metals import (
    METAL_RESOURCE_FLOW_NAMES,
    canonical_resource_flow_label,
    get_in_ground_resource_exchanges,
    get_reference_product_resource_flow_name,
    is_downstream_attributed_resource_carrier_dataset,
    is_metal_resource_flow,
    resource_flow_matches_reference_product,
)

OUTSIDE_SCOPE = "non_metal_resource_scope"
UPSTREAM_SUPPLY = "upstream_resource_supply"


def identity(dataset):
    return tuple(
        dataset.get(field)
        for field in ("name", "reference product", "location", "unit")
    )


def resource_diagnostic_is_informational(diagnostic):
    """Only verified non-applicability decisions leave the warning stream."""
    evidence = diagnostic.get("evidence", {})
    if diagnostic.get("applicability") != "not_applicable":
        return False
    if diagnostic.get("reason") == OUTSIDE_SCOPE:
        return (
            evidence.get("metal resource count") == 0
            and evidence.get("product has metal target") is False
        )
    return (
        diagnostic.get("reason") == UPSTREAM_SUPPLY
        and evidence.get("all material branches accounted for") is True
    )


def secondary_boundary(dataset, system_model):
    """Recognize explicit cut-off/recycling supplies, not arbitrary zero flows."""
    if system_model != "cutoff":
        return False
    name = dataset.get("name", "")
    if "Recycled Content cut-off" in name:
        return not any(
            e.get("type") == "technosphere" and e.get("amount", 0) > 0
            for e in dataset.get("exchanges", ())
        )
    return (
        name.lower().startswith("treatment of ")
        and "recycl" in name.lower()
        and re.search(r"\bscrap\b", dataset.get("reference product", "").lower())
        is not None
    )


def upstream_resource_coverage(
    dataset, flow_name, provider_index, system_model, max_depth=12
):
    """Check every target-bearing material branch to an attribution boundary.

    This is boundary coverage, not a weighted resource mass balance. An existing
    extraction flow or explicit cut-off recycling boundary stops a branch.
    Unassigned carriers, unknown leaves, isolated cycles and broken links remain
    unresolved. Purpose clauses such as ``for aluminium electrolysis`` do not
    turn carbon electrodes into aluminium feedstock.
    """
    pending = deque([(dataset, 0)])
    visited, extraction, secondary = set(), set(), set()
    reverse = defaultdict(set)
    failures, ignored = [], []
    target_label = canonical_resource_flow_label(flow_name)
    while pending:
        current, depth = pending.popleft()
        key = identity(current)
        if key in visited:
            continue
        visited.add(key)
        if depth > max_depth:
            failures.append({"reason": "depth limit", "activity key": key})
            continue
        if is_downstream_attributed_resource_carrier_dataset(current):
            failures.append(
                {
                    "reason": "unassigned downstream-attribution carrier",
                    "activity key": key,
                }
            )
            continue
        resources = [
            e
            for e in get_in_ground_resource_exchanges(current)
            if canonical_resource_flow_label(e.get("name", "")) == target_label
        ]
        if any(not math.isfinite(e["amount"]) or e["amount"] < 0 for e in resources):
            failures.append({"reason": "invalid target resource", "activity key": key})
            continue
        if any(e["amount"] > 0 for e in resources):
            extraction.add(key)
            continue
        if secondary_boundary(current, system_model):
            secondary.add(key)
            continue
        for exchange in current.get("exchanges", ()):
            if exchange.get("type") != "technosphere":
                continue
            label = exchange.get("product") or exchange.get("name", "")
            material = re.split(r", (?:for|from) ", label, maxsplit=1)[0]
            if not resource_flow_matches_reference_product(flow_name, material):
                if resource_flow_matches_reference_product(flow_name, label):
                    ignored.append(
                        {
                            "product": label,
                            "reason": "target appears only in purpose/origin clause",
                        }
                    )
                continue
            amount = float(exchange["amount"])
            if not math.isfinite(amount):
                failures.append(
                    {
                        "reason": "invalid material quantity",
                        "activity key": key,
                        "product": label,
                    }
                )
                continue
            if amount == 0:
                continue
            if exchange.get("unit") in {"kilowatt hour", "megajoule", "ton kilometer"}:
                continue
            if exchange.get("unit") == "unit" and re.search(
                r"\b(factory|facility|infrastructure|equipment|plant|furnace)\b",
                material,
            ):
                ignored.append(
                    {"product": label, "reason": "capital equipment, not material feed"}
                )
                continue
            provider_key = tuple(
                exchange.get(field) for field in ("name", "product", "location")
            )
            providers = provider_index.get(provider_key, [])
            if amount < 0:
                production = (
                    [
                        e
                        for e in providers[0].get("exchanges", ())
                        if e.get("type") == "production"
                    ]
                    if len(providers) == 1
                    else []
                )
                if (
                    exchange.get("unit") == "kilogram"
                    and len(providers) == 1
                    and providers[0].get("unit") == "kilogram"
                    and len(production) == 1
                    and production[0].get("product") == exchange.get("product")
                    and production[0].get("unit") == "kilogram"
                    and math.isfinite(production[0]["amount"])
                    and production[0]["amount"] < 0
                ):
                    ignored.append(
                        {
                            "product": label,
                            "reason": "outgoing waste treatment verified by negative provider reference production",
                        }
                    )
                    continue
                failures.append(
                    {
                        "reason": "unverified negative material quantity",
                        "activity key": key,
                        "product": label,
                    }
                )
                continue
            if (
                exchange.get("unit") != "kilogram"
                or len(providers) != 1
                or providers[0].get("unit") != "kilogram"
            ):
                failures.append(
                    {
                        "reason": "unresolved provider or material unit",
                        "provider key": provider_key,
                        "unit": exchange.get("unit"),
                        "provider count": len(providers),
                    }
                )
                continue
            provider = providers[0]
            reverse[identity(provider)].add(key)
            pending.append((provider, depth + 1))
    proven = extraction | secondary
    pending = deque(proven)
    while pending:
        for consumer in reverse[pending.popleft()] - proven:
            proven.add(consumer)
            pending.append(consumer)
    return {
        "target resource": flow_name,
        "all material branches accounted for": not failures and visited == proven,
        "checked activities": len(visited),
        "extraction boundaries": sorted(extraction),
        "cutoff secondary boundaries": sorted(secondary),
        "unresolved activities": sorted(visited - proven),
        "unresolved links": failures,
        "excluded exchanges": ignored,
        "qualification": "Attribution-boundary coverage only; not a weighted resource mass balance or assay validation",
    }


def classify_preserved_resource(dataset, provider_index, system_model):
    """Return a scope decision without modifying any exchange."""
    resources = get_in_ground_resource_exchanges(dataset)
    metals = [e for e in resources if is_metal_resource_flow(e.get("name", ""))]
    product = dataset.get("reference product", "")
    target = any(
        resource_flow_matches_reference_product(flow, product)
        for flow in METAL_RESOURCE_FLOW_NAMES
    )
    if resources and not metals and not target:
        return {
            "reason": OUTSIDE_SCOPE,
            "applicability": "not_applicable",
            "explanation": "Only non-metal in-ground flows and no metal target product; preserve this mineral inventory outside the metal correction.",
            "evidence": {
                "metal resource count": 0,
                "product has metal target": False,
                "preserved resource names": [e["name"] for e in resources],
            },
        }
    if not resources:
        flow = get_reference_product_resource_flow_name(product)
        if flow:
            evidence = upstream_resource_coverage(
                dataset, flow, provider_index, system_model
            )
            covered = evidence["all material branches accounted for"]
            return {
                "reason": (
                    UPSTREAM_SUPPLY
                    if covered
                    else "preserved_upstream_supply_unverified"
                ),
                "applicability": "not_applicable" if covered else "applicable",
                "explanation": (
                    "Extraction/recycling boundaries already account for every target-bearing material branch; no additional direct extraction applies."
                    if covered
                    else "Upstream boundary coverage is incomplete; preserve the inventory and review the listed activities/links."
                ),
                "evidence": evidence,
            }
    return {
        "reason": "preserved_target_content_unverified",
        "applicability": "applicable",
        "explanation": "Target product content or co-mined attribution is unverified; preserve resource amounts pending source-specific assay/allocation review.",
        "evidence": {
            "resource names": [e["name"] for e in resources],
            "product": product,
        },
    }
