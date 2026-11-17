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
from .clic_licenses import COMMERCIAL_LICENSES, assess_commercial_licenses

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
        recipe = rule.get("recipe")
        if recipe is not None:
            if rule.get("inventory_license") not in COMMERCIAL_LICENSES or not rule.get(
                "citation"
            ):
                raise ClicInventoryError(
                    f"Independent recipe {rule['id']} needs a permitted license and citation."
                )
            if not recipe.get("inputs"):
                raise ClicInventoryError(
                    f"Independent recipe {rule['id']} has no reviewed inputs."
                )
            for flow in [*recipe["inputs"], *recipe.get("emissions", ())]:
                quantity = flow.get("amount")
                if (
                    isinstance(quantity, bool)
                    or not isinstance(quantity, (int, float))
                    or not math.isfinite(quantity)
                    or quantity <= 0
                ):
                    raise ClicInventoryError(
                        f"Invalid independent recipe quantity in {rule['id']}."
                    )
        if (
            not rule["conversion"]
            or not rule["match"]["names"]
            or not (rule.get("suppliers") or recipe)
        ):
            raise ClicInventoryError(
                f"Incomplete commercial replacement rule: {rule['id']}."
            )
    for rule in data.get("background_proxies", ()):
        if (
            rule["id"] in seen
            or rule.get("review_status") != "reviewed"
            or not all(
                rule.get(key)
                for key in (
                    "identities",
                    "suppliers",
                    "conversion",
                    "limitations",
                    "evidence",
                )
            )
        ):
            raise ClicInventoryError(
                f"Background proxy {rule.get('id')} needs a unique ID, review and documentation."
            )
        seen.add(rule["id"])
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
                return candidates[0], supplier
    return None


def _new_activity(row, rule, classifications=()):
    """A unit reference, without copying the disputed source's scientific data."""
    activity = {
        "name": row["name"],
        "reference product": row["reference_product"],
        "location": row["location"],
        "unit": row["unit"],
        "database": "__clic__",
        "code": row["activity_code"],
        "type": "process",
        "comment": f"Commercial alternative {rule['id']}. {rule['conversion']} {rule['limitations']}",
        "classifications": copy.deepcopy(list(classifications)),
        "exchanges": [
            {
                "name": row["name"],
                "product": row["reference_product"],
                "location": row["location"],
                "unit": row["unit"],
                "type": "production",
                "amount": 1,
                "input": ("__clic__", row["activity_code"]),
            }
        ],
    }
    activity.update(copy.deepcopy(rule.get("metadata", {})))
    return activity


def _clone_native(native, row, rule):
    import numpy as np

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
        excluded_mass = float(
            sum(
                edge["amount"]
                for edge in native["exchanges"]
                if edge["type"] == "technosphere"
                and edge["name"].casefold() in excluded
                and edge["unit"] == "kilogram"
            )
        )
        if not 0 < excluded_mass < outputs[0]["amount"]:
            raise ClicInventoryError(
                f"Native electronics mass cannot be established for {rule['id']}."
            )
    reference_output = float(outputs[0]["amount"])
    factor = rule.get("amount", 1) / (reference_output - excluded_mass)
    activity = _new_activity(row, rule, native.get("classifications", ()))
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
        # Compact source stores and rescaling can yield NumPy scalar values.
        # Convert their representation without rounding quantities or allowing
        # arbitrary nonportable objects into the replacement fingerprint.
        for key, value in edge.items():
            if isinstance(value, np.generic):
                edge[key] = value.item()
        activity["exchanges"].append(edge)
    return activity, {
        "native_reference_output": reference_output,
        "excluded_mass_kg": excluded_mass,
        "scaling_factor": factor,
    }


def _independent_recipe(row, rule, index):
    """Build the reviewed foreground using available licensed background links."""
    activity = _new_activity(row, rule)
    inputs = []
    for specification in rule["recipe"]["inputs"]:
        match = _select_supplier(
            {"id": rule["id"], "suppliers": specification["suppliers"]}, row, index
        )
        if match is None:
            return None
        native, _ = match
        name, product, location, unit = activity_identity(native)
        activity["exchanges"].append(
            {
                "name": name,
                "product": product,
                "location": location,
                "unit": unit,
                "type": "technosphere",
                "amount": specification["amount"],
            }
        )
        inputs.append(
            {
                "supplier": list(activity_identity(native)),
                "amount": specification["amount"],
            }
        )
    for emission in rule["recipe"].get("emissions", ()):
        activity["exchanges"].append({**copy.deepcopy(emission), "type": "biosphere"})
    return activity, {"inputs": inputs}


def _background_aliases(registry, index, version):
    """Reviewed external link contracts, only for identities absent in the target."""
    result = []
    for rule in registry.get("background_proxies", ()):
        if version not in rule["target_versions"]:
            continue
        for source in rule["identities"]:
            name, product, location, unit = source
            if index.get((name.casefold(), product.casefold(), unit), {}).get(location):
                continue
            match = _select_supplier(rule, {"location": location}, index)
            if match is None:
                continue
            native, specification = match
            if native["unit"] != unit:
                raise ClicInventoryError(
                    f"Background proxy {rule['id']} changes its unit without a reviewed conversion."
                )
            result.append(
                {
                    "source": source,
                    "supplier": list(activity_identity(native)),
                    "rule": rule["id"],
                    "conversion": rule["conversion"],
                    "limitations": rule["limitations"],
                    "confidence": rule["confidence"],
                    "evidence": rule["evidence"],
                    "review_status": rule["review_status"],
                }
            )
    return result


def apply_background_aliases(records, aliases):
    """Resolve reviewed temporary aliases before strict target validation."""
    by_identity = {tuple(row["source"]): row for row in aliases}
    changes = []
    for activity in records:
        for position, edge in enumerate(activity["exchanges"]):
            reference = edge.get("input")
            if edge["type"] != "technosphere" or (
                isinstance(reference, (list, tuple)) and reference[0] == "__clic__"
            ):
                continue
            match = by_identity.get(activity_identity(edge))
            if match is None:
                continue
            name, product, location, unit = match["supplier"]
            edge.update(name=name, product=product, location=location, unit=unit)
            edge["reference product"] = product
            edge.pop("input", None)
            changes.append(
                {
                    "code": "clic.commercial_background_proxy",
                    "severity": "info",
                    "activity_code": activity["code"],
                    "exchange_index": position,
                    **match,
                }
            )
    return changes


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
        supplier, independent = None, None
        if candidates:
            rule = candidates[0]
            if "recipe" in rule:
                independent = _independent_recipe(row, rule, index)
            else:
                match = _select_supplier(rule, row, index)
                if match is not None:
                    native, supplier = match
        if native is None and independent is None:
            selected[code] = copy.deepcopy(activities[code])
            pending.extend(_internal_suppliers(activities[code]))
            continue
        # A version-specific fallback can have greater technology loss than
        # the preferred supplier. Keep its explanation in the activity/report.
        supplier = supplier or {}
        rule = {
            **rule,
            "limitations": " ".join(
                filter(None, [rule["limitations"], supplier.get("notes")])
            ),
            "confidence": supplier.get("confidence", rule.get("confidence", "medium")),
            "evidence": [*rule["evidence"], *supplier.get("evidence", ())],
        }
        selected[code], accounting = (
            independent if independent is not None else _clone_native(native, row, rule)
        )
        substitutions.append(
            {
                "slug": row["slug"],
                "version_id": row["version_id"],
                "original_identity": list(activity_identity(row)),
                "original_license": row["license"],
                "rule": rule["id"],
                "supplier": (
                    list(activity_identity(native))
                    if native is not None
                    else [
                        rule["id"],
                        row["reference_product"],
                        row["location"],
                        row["unit"],
                    ]
                ),
                "replacement_inventory_sha256": content_hash(selected[code]),
                "amount": rule.get("amount", 1),
                "accounting": accounting,
                "conversion": rule["conversion"],
                "limitations": rule["limitations"],
                "evidence": rule["evidence"],
                "review_status": rule["review_status"],
                "confidence": rule.get("confidence", "medium"),
                "geography_proxy": (
                    native["location"] != row["location"]
                    if native is not None
                    else any(
                        i["supplier"][2] != row["location"]
                        for i in accounting["inputs"]
                    )
                ),
                "inventory_license": (
                    "ecoinvent" if native is not None else rule["inventory_license"]
                ),
                "citation": rule.get("citation", ""),
                "doi": rule.get("doi", ""),
            }
        )
    substitutions.sort(key=lambda row: row["slug"])
    report = {
        "registry_fingerprint": registry_fingerprint,
        "target": {"version": version, "system_model": system_model},
        "substitutions": substitutions,
        "background_aliases": _background_aliases(registry, index, version),
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
