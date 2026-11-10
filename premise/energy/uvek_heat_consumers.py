"""Plan exact native heat-consumer substitutions without changing source records."""

from __future__ import annotations

import copy
import json
import math
from collections import Counter, defaultdict

import numpy as np

from ..core.filesystem_constants import DATA_DIR
from ..inventory.profiles import identity

REVIEW_PATH = DATA_DIR / "heat" / "uvek2026_consumer_review.json"
REVIEW_REFERENCE = "premise/data/heat/uvek2026_consumer_review.json"
AUDIT_FIELD = "premise heat consumer"
RETAINED_ROUTES = {
    "retain_network_service",
    "retain_component_service",
    "retain_mass_steam",
    "retain_zero_allocated_heat",
}
ACTIVE_ROUTES = {"buildings_end_use", "industrial_end_use", "district_delivery"}


def _key(record):
    return record.get("database"), record.get("code")


def _same(left, right):
    """Accept the normal tuple/list change introduced by the JSON cache."""

    def scalar(value):
        if isinstance(value, np.generic):
            return value.item()
        raise TypeError(f"Unsupported heat-audit value: {type(value).__name__}.")

    return json.dumps(left, sort_keys=True, default=scalar) == json.dumps(
        right, sort_keys=True, default=scalar
    )


def _protected(dataset):
    if dataset.get("clic provenance") or dataset.get("brightpath_conversion"):
        return "protected_foreground_or_mapping_helper"
    if dataset.get("premise heat"):
        return "protected_scenario_heat_inventory"
    if dataset["unit"] in {"megajoule", "kilowatt hour"}:
        return "protected_energy_reference_inventory"
    return None


def _reference(dataset, expected_amount, *, allow_empty=False):
    outputs = [e for e in dataset["exchanges"] if e["type"] == "production"]
    if (
        len(outputs) != 1
        or identity(outputs[0]) != identity(dataset)
        or isinstance(outputs[0]["amount"], bool)
        or outputs[0]["amount"] != expected_amount
        or (len(dataset["exchanges"]) <= 1 and not allow_empty)
    ):
        raise ValueError("Heat-consumer source needs a nonempty matching +1 reference.")


def _targets(heat_plan, scenario):
    targets = {}
    groups = {
        **heat_plan["markets"],
        "district_delivery": heat_plan["deliveries"],
    }
    for route, records in groups.items():
        by_region = {}
        for record in records:
            marker = record.get("premise heat", {})
            expected_kind = (
                "uvek_native_district_delivery"
                if route == "district_delivery"
                else "uvek_iam_market"
            )
            if (
                marker.get("scenario") != scenario
                or marker.get("kind") != expected_kind
                or (route != "district_delivery" and marker.get("layer") != route)
                or record["unit"] != "megajoule"
                or None in _key(record)
                or record["location"] in by_region
            ):
                raise ValueError(
                    "Heat-consumer target is not this scenario's unique market."
                )
            _reference(record, 1)
            by_region[record["location"]] = record
        targets[route] = by_region
    return targets


def plan_native_heat_consumers(
    database,
    heat_plan,
    region_for_location,
    *,
    model,
    pathway,
    year,
    family="uvek",
    version="2026",
    system_model="cutoff",
    biosphere_version="3.10",
):
    """Return replacement consumer copies and literal audits, with read-only inputs.

    Exact reviewed useful-heat MJ inputs change supplier only. Network service,
    ambient components and kg steam are retained explicitly. Energy-reference
    producers and foreground/helpers remain intact, preventing the newly built
    market from rewriting its own direct supply inventories. This does not
    prohibit legitimate upstream infrastructure cycles in the complete LCI.

    ``region_for_location`` supplies Premise's usual geographic lookup. Its
    KeyError/ValueError or a region without that layer uses an explicitly audited
    World proxy. Other lookup errors propagate; they are not missing geography.
    No records are appended, no exchange is mutated and no database is written.
    """
    if (family, str(version), system_model, biosphere_version) != (
        "uvek",
        "2026",
        "cutoff",
        "3.10",
    ):
        raise ValueError("Native heat consumers require UVEK2026cutoff/biosphere3.10.")
    scenario = {"model": model, "pathway": pathway, "year": year}
    review = json.loads(REVIEW_PATH.read_text())
    if review.get("schema_version") != 1:
        raise ValueError("Unsupported native heat-consumer review schema.")
    targets = _targets(heat_plan, scenario)
    by_identity, by_key = defaultdict(list), defaultdict(list)
    for record in database:
        by_identity[identity(record)].append(record)
        by_key[_key(record)].append(record)
    bindings = {}
    for row in review["sources"]:
        expected = identity(row["identity"])
        if expected in bindings or row["route"] not in ACTIVE_ROUTES | RETAINED_ROUTES:
            raise ValueError("Duplicate or unsupported native heat-consumer binding.")
        if row["reference_amount"] != 1:
            raise ValueError("Native heat-consumer binding needs a +1 reference.")
        candidates = by_identity.get(expected, [])
        if len(candidates) != 1:
            raise ValueError("Missing or ambiguous exact native heat-consumer source.")
        source = candidates[0]
        if (
            source.get("clic provenance")
            or source.get("brightpath_conversion")
            or source.get("premise heat")
        ):
            raise ValueError("An owned/helper record cannot impersonate native heat.")
        if None in _key(source) or len(by_key[_key(source)]) != 1:
            raise ValueError("Native heat-consumer source has an ambiguous input key.")
        empty = row.get("empty_inventory_expected", False)
        if empty and (
            row["route"] != "retain_zero_allocated_heat"
            or len(source["exchanges"]) != 1
        ):
            raise ValueError("Zero-allocated heat handover disagrees with its review.")
        _reference(source, 1, allow_empty=empty)
        if row["route"] in ACTIVE_ROUTES and source["unit"] != "megajoule":
            raise ValueError("Useful-heat substitutions require reviewed MJ suppliers.")
        bindings[expected] = row, _key(source)
    source_keys = {key: expected for expected, (_, key) in bindings.items()}
    owned_keys = {_key(a) for records in targets.values() for a in records.values()}
    replacements, retained = [], []
    substitution_count = already_applied = geographic_proxies = 0
    for dataset in database:
        audit = dataset.get(AUDIT_FIELD)
        if audit is not None:
            if (
                audit.get("scenario") != scenario
                or audit.get("review") != REVIEW_REFERENCE
            ):
                raise ValueError(
                    "Heat-consumer audit belongs to another scenario/review."
                )
            previous = {row["index"]: row for row in audit["substitutions"]}
            if len(previous) != len(audit["substitutions"]):
                raise ValueError("Duplicate heat-consumer exchange audit index.")
            if any(
                not isinstance(i, int)
                or isinstance(i, bool)
                or i < 0
                or i >= len(dataset["exchanges"])
                for i in previous
            ):
                raise ValueError("Invalid heat-consumer exchange audit index.")
        else:
            previous = {}
        copied, new_audits = None, []
        reason = _protected(dataset)
        for index, current in enumerate(dataset["exchanges"]):
            prior = previous.get(index)
            exchange = prior["source_exchange"] if prior else current
            if exchange["type"] != "technosphere":
                if prior:
                    raise ValueError(
                        "Heat-consumer audit does not describe a technosphere exchange."
                    )
                continue
            expected = identity(exchange)
            link = tuple(exchange.get("input", ()))
            if link in source_keys and expected != source_keys[link]:
                raise ValueError("Heat-consumer source link and identity disagree.")
            if expected not in bindings:
                if prior or (
                    not reason and tuple(current.get("input", ())) in owned_keys
                ):
                    raise ValueError(
                        "Scenario heat input has no reviewed original-exchange audit."
                    )
                continue
            row, source_key = bindings[expected]
            if link != source_key:
                raise ValueError(
                    "Heat-consumer source exchange is not linked to its exact supplier."
                )
            if isinstance(exchange["amount"], bool) or not math.isfinite(
                exchange["amount"]
            ):
                raise ValueError(
                    "Heat-consumer input quantity must be finite and signed."
                )
            route = row["route"]
            location = dataset["location"]
            details = {
                "consumer": identity(dataset),
                "consumer_key": _key(dataset),
                "index": index,
                "source": expected,
                "route": route,
            }
            if reason or route in RETAINED_ROUTES:
                if prior:
                    raise ValueError(
                        "A protected heat consumer was previously rewritten."
                    )
                retained.append({**details, "reason": reason or route})
                continue
            selected_route = route
            if (
                not targets.get(route)
                and route == "industrial_end_use"
                and targets.get("secondary_supply")
            ):
                selected_route = "secondary_supply"
            available = targets.get(selected_route, {})
            if not available:
                if prior:
                    raise ValueError("Previously routed heat now lacks its IAM layer.")
                retained.append({**details, "reason": "IAM_layer_not_available"})
                continue
            try:
                region = region_for_location(location)
            except (KeyError, ValueError):
                region = None
            fallback = region not in available
            selected_region = "World" if fallback else region
            if selected_region not in available:
                raise ValueError(
                    "Heat-consumer geography lacks both regional and World supply."
                )
            target = available[selected_region]
            replacement = copy.deepcopy(exchange)
            replacement.update(
                name=target["name"],
                product=target["reference product"],
                location=target["location"],
                input=_key(target),
            )
            if "reference product" in replacement:
                replacement["reference product"] = target["reference product"]
            entry = {
                "index": index,
                "source_exchange": copy.deepcopy(exchange),
                "target": identity(target),
                "target_key": _key(target),
                "requested_IAM_region": region,
                "geographic_proxy": fallback,
                "source_route": route,
                "selected_route": selected_route,
                "supply_only_proxy": selected_route != route,
            }
            if prior:
                if not _same(current, replacement) or not _same(prior, entry):
                    raise ValueError(
                        "Existing heat-consumer substitution differs from this plan."
                    )
                already_applied += 1
                continue
            if copied is None:
                if None in _key(dataset) or len(by_key[_key(dataset)]) != 1:
                    raise ValueError(
                        "Heat-consumer replacement needs a unique dataset key."
                    )
                copied = copy.deepcopy(dataset)
            copied["exchanges"][index] = replacement
            new_audits.append(entry)
            substitution_count += 1
            geographic_proxies += fallback
        if copied is not None:
            if previous:
                raise ValueError(
                    "Partially applied heat-consumer plan cannot be extended silently."
                )
            copied[AUDIT_FIELD] = {
                "scenario": scenario,
                "review": REVIEW_REFERENCE,
                "limitations": copy.deepcopy(review["limitations"]),
                "substitutions": new_audits,
            }
            replacements.append({"source_key": _key(dataset), "activity": copied})
    return {
        "replacements": replacements,
        "retained": retained,
        "diagnostics": {
            "scenario": scenario,
            "modified_consumers": len(replacements),
            "substituted_exchanges": substitution_count,
            "already_applied": already_applied,
            "geographic_proxies": geographic_proxies,
            "retained_counts": dict(Counter(row["reason"] for row in retained)),
        },
    }
