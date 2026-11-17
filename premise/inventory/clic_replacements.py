"""Reviewed commercial alternatives, selected from the user's licensed background.

Rules contain identities and documented conversions, never copied restricted
inventories. A replacement clones the native process so premise can still
transform its material inputs. Replacing a parent cuts its original dependency
cone; shared suppliers remain when another retained activity needs them.
"""

from __future__ import annotations

import copy
import math
from collections import defaultdict, deque
from pathlib import Path

import yaml

from .clic_archive import (
    ClicInventoryError,
    ResolvedClicInventory,
    activity_identity,
    content_hash,
)
from .clic_licenses import assess_commercial_licenses

REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "data/clic/commercial_replacements.yaml"
)


def replacement_registry():
    with REGISTRY_PATH.open(encoding="utf-8") as stream:
        data = yaml.safe_load(stream)
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported commercial CLIC replacement registry.")
    seen = set()
    for rule in data.get("rules", ()):
        if rule["id"] in seen:
            raise ClicInventoryError(
                f"Duplicate commercial replacement rule: {rule['id']}."
            )
        seen.add(rule["id"])
        if (
            rule["review_status"] != "reviewed"
            or not rule["evidence"]
            or not rule["limitations"]
        ):
            raise ClicInventoryError(
                f"Commercial replacement {rule['id']} needs review and documentation."
            )
        factor = rule.get("amount", 1)
        if (
            isinstance(factor, bool)
            or not isinstance(factor, (int, float))
            or not math.isfinite(factor)
            or factor <= 0
        ):
            raise ClicInventoryError(
                f"Invalid commercial replacement amount in {rule['id']}."
            )
        if (
            not rule["conversion"]
            or not rule["match"]["names"]
            or not rule["suppliers"]
        ):
            raise ClicInventoryError(
                f"Incomplete commercial replacement rule: {rule['id']}."
            )
    return data


def _internal_suppliers(activity):
    for edge in activity["exchanges"]:
        reference = edge.get("input")
        if (
            edge["type"] == "technosphere"
            and isinstance(reference, (list, tuple))
            and len(reference) == 2
            and reference[0] == "__clic__"
        ):
            yield reference[1]


def _select_supplier(rule, row, index):
    for supplier in rule["suppliers"]:
        key = (
            supplier["name"].casefold(),
            supplier["product"].casefold(),
            supplier["unit"],
        )
        locations = index.get(key, {})
        for location in supplier["locations"]:
            location = row["location"] if location == "same" else location
            candidates = locations.get(location, ())
            if len(candidates) > 1:
                raise ClicInventoryError(
                    f"Ambiguous native supplier for commercial replacement {rule['id']}: {key}, {location}."
                )
            if candidates:
                return candidates[0]
    return None


def _clone_native(native, row, rule):
    from ..core.utils import rescale_exchange

    outputs = [e for e in native["exchanges"] if e["type"] == "production"]
    if len(outputs) != 1 or outputs[0]["amount"] <= 0:
        raise ClicInventoryError(
            f"Replacement {rule['id']} needs one positive native reference output."
        )
    excluded = {name.casefold() for name in rule.get("exclude_inputs", ())}
    excluded_mass = 0
    if rule.get("renormalize_excluded_mass"):
        if native["unit"] != "kilogram":
            raise ClicInventoryError(
                f"Mass renormalization requires a kg output: {rule['id']}."
            )
        excluded_mass = sum(
            edge["amount"]
            for edge in native["exchanges"]
            if edge["type"] == "technosphere"
            and edge["name"].casefold() in excluded
            and edge["unit"] == "kilogram"
        )
        if not 0 < excluded_mass < outputs[0]["amount"]:
            raise ClicInventoryError(
                f"Native electronics mass cannot be established for {rule['id']}."
            )
    factor = rule.get("amount", 1) / (outputs[0]["amount"] - excluded_mass)
    # Start from permitted data, never the restricted process's exchanges or
    # behavior-bearing metadata. Native suppliers keep their manufacturing geography.
    activity = {
        "name": row["name"],
        "reference product": row["reference_product"],
        "location": row["location"],
        "unit": row["unit"],
        "database": "__clic__",
        "code": row["activity_code"],
        "type": "process",
        "comment": f"Commercial alternative {rule['id']}. {rule['conversion']} {rule['limitations']}",
        "classifications": copy.deepcopy(native.get("classifications", [])),
        "exchanges": [],
    }
    for original in native["exchanges"]:
        if original["type"] == "production":
            continue
        if (
            original["type"] == "technosphere"
            and original["name"].casefold() in excluded
        ):
            continue
        edge = copy.deepcopy(original)
        edge.pop("input", None)
        edge.pop("output", None)
        edge.pop("formula", None)
        if factor != 1:
            rescale_exchange(edge, factor, remove_uncertainty=False)
        activity["exchanges"].append(edge)
    activity["exchanges"].insert(
        0,
        {
            "name": row["name"],
            "product": row["reference_product"],
            "location": row["location"],
            "unit": row["unit"],
            "type": "production",
            "amount": 1,
            "input": ("__clic__", row["activity_code"]),
        },
    )
    activity.update(copy.deepcopy(rule.get("metadata", {})))
    return activity, {
        "native_reference_output": outputs[0]["amount"],
        "excluded_mass_kg": excluded_mass,
        "scaling_factor": factor,
    }


def replace_commercial_inventories(
    resolved, archive, database, *, version, system_model, registry=None
):
    """Return an owned graph and portable record of substitutions and pruning.

    Identity matching is explicit and deterministic. Missing candidates leave the
    original graph in place, so the subsequent license gate reports the missing
    reviewed alternative rather than silently removing a technology.
    """
    registry = replacement_registry() if registry is None else registry
    registry_fingerprint = content_hash(registry)
    rules = defaultdict(list)
    for rule in registry["rules"]:
        if version not in rule.get(
            "target_versions", registry["target_versions"]
        ) or system_model not in rule.get("system_models", registry["system_models"]):
            continue
        for name in rule["match"]["names"]:
            rules[(name.casefold(), rule["match"]["unit"])].append(rule)
    index = defaultdict(lambda: defaultdict(list))
    for native in database:
        name, product, location, unit = activity_identity(native)
        index[(name.casefold(), product.casefold(), unit)][location].append(native)
    activities = {a["code"]: a for a in resolved.activities}
    rows = {row["activity_code"]: row for row in resolved.versions}
    parents = defaultdict(set)
    for code, activity in activities.items():
        for supplier in _internal_suppliers(activity):
            parents[supplier].add(code)
    issues = assess_commercial_licenses(resolved, archive)["issues"]
    blocked_slugs = {row["slug"] for row in issues}
    affected = {code for code, row in rows.items() if row["slug"] in blocked_slugs}
    queue = deque(affected)
    while queue:
        for code in parents[queue.popleft()]:
            if code not in affected:
                affected.add(code)
                queue.append(code)
    selected, substitutions = {}, []
    pending = [code for code, row in rows.items() if row["slug"] in resolved.roots]
    while pending:
        code = pending.pop()
        if code in selected:
            continue
        row, native = rows[code], None
        candidates = (
            rules.get((row["name"].casefold(), row["unit"]), ())
            if code in affected
            else ()
        )
        candidates = [
            rule
            for rule in candidates
            if not rule["match"].get("products")
            or row["reference_product"] in rule["match"]["products"]
        ]
        if len(candidates) > 1:
            raise ClicInventoryError(
                f"Overlapping commercial replacement rules for {row['slug']}."
            )
        if candidates:
            rule = candidates[0]
            native = _select_supplier(rule, row, index)
        if native is None:
            selected[code] = copy.deepcopy(activities[code])
            pending.extend(_internal_suppliers(activities[code]))
            continue
        selected[code], accounting = _clone_native(native, row, rule)
        substitutions.append(
            {
                "slug": row["slug"],
                "version_id": row["version_id"],
                "original_identity": list(activity_identity(row)),
                "original_license": row["license"],
                "rule": rule["id"],
                "supplier": list(activity_identity(native)),
                "replacement_inventory_sha256": content_hash(selected[code]),
                "amount": rule.get("amount", 1),
                "accounting": accounting,
                "conversion": rule["conversion"],
                "limitations": rule["limitations"],
                "evidence": rule["evidence"],
                "review_status": rule["review_status"],
                "confidence": rule.get("confidence", "medium"),
                "geography_proxy": native["location"] != row["location"],
                "inventory_license": "ecoinvent",
            }
        )
    substitutions.sort(key=lambda row: row["slug"])
    report = {
        "registry_fingerprint": registry_fingerprint,
        "target": {"version": version, "system_model": system_model},
        "substitutions": substitutions,
        "pruned_slugs": sorted(
            row["slug"] for code, row in rows.items() if code not in selected
        ),
    }
    report["fingerprint"] = content_hash(report)
    replaced = {row["slug"]: row for row in substitutions}
    kept_rows = []
    for row in resolved.versions:
        if row["activity_code"] not in selected:
            continue
        row = copy.deepcopy(row)
        if row["slug"] in replaced:
            row["replacement"] = replaced[row["slug"]]
            row["background"] = {
                "technosphere": {
                    "family": "ecoinvent",
                    "version": version,
                    "system_model": system_model,
                },
                "biosphere": {"family": "ecoinvent", "version": version},
            }
        selected[row["activity_code"]]["clic provenance"] = copy.deepcopy(row)
        kept_rows.append(row)
    result = ResolvedClicInventory(
        [selected[row["activity_code"]] for row in kept_rows],
        kept_rows,
        resolved.roots,
        copy.deepcopy(resolved.archive),
        content_hash(
            {
                "native_resolution": resolved.fingerprint,
                "commercial_replacements": report["fingerprint"],
            }
        ),
        copy.deepcopy(resolved.diagnostics),
    )
    return result, report
