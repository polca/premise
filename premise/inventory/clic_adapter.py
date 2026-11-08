"""Match harmonized CLIC foregrounds to a premise source graph.

Portable CLIC codes identify foreground links. External suppliers are matched
against the actual user database, including units; source-project identifiers
from an uploaded inventory are never treated as destination links.
"""

from __future__ import annotations

import copy
import json
import math
from collections import defaultdict

from .clic_archive import ClicInventoryError, activity_identity
from .clic_target import resolve_inventory_target


def _folded(identity):
    # Unit symbols and geographic codes are case-sensitive.
    return tuple(
        value.casefold() if index in (0, 1) and isinstance(value, str) else value
        for index, value in enumerate(identity)
    )


def _normalize_compression_contract(activity, diagnostics):
    """Adapt the published conditional-delivery metadata without changing its LCI."""
    from ..transport.vehicle_hydrogen import COMPRESSION_KEY, COMPRESSION_COMMENT

    value = activity.get(COMPRESSION_KEY)
    if value is None:
        return
    specification = (
        json.loads(value) if isinstance(value, str) else copy.deepcopy(value)
    )
    if "electricity product" in specification and "hydrogen product" in specification:
        return
    if not specification.get(
        "delivery pressure is an explicit scenario not native-label evidence"
    ):
        raise ClicInventoryError(
            f"Unrecognized hydrogen compression metadata in {activity['name']!r}."
        )
    electricity = [
        edge
        for edge in activity["exchanges"]
        if edge["type"] == "technosphere"
        and edge.get("unit") == "kilowatt hour"
        and str(edge.get("comment", "")).startswith(
            "Explicit 30-bar delivery scenario, conditional on fuel not already dispensed."
        )
    ]
    hydrogen = [
        edge
        for edge in activity["exchanges"]
        if edge["type"] == "technosphere"
        and edge.get("unit") == "kilogram"
        and str(edge.get("product", "")).startswith("hydrogen,")
    ]
    products = [
        {edge["product"] for edge in edges} for edges in (electricity, hydrogen)
    ]
    if any(len(values) != 1 for values in products):
        raise ClicInventoryError(
            f"Ambiguous documented compression purchases in {activity['name']!r}."
        )
    expected = (
        sum(edge["amount"] for edge in hydrogen)
        * specification["electricity kWh per kg H2"]
    )
    if any(edge["amount"] < 0 for edge in electricity + hydrogen) or not math.isclose(
        sum(edge["amount"] for edge in electricity),
        expected,
        rel_tol=1e-8,
        abs_tol=1e-12,
    ):
        raise ClicInventoryError(
            f"Published hydrogen compression quantities do not balance in {activity['name']!r}."
        )
    specification.update(
        {
            "electricity product": next(iter(products[0])),
            "hydrogen product": next(iter(products[1])),
        }
    )
    activity[COMPRESSION_KEY] = specification
    for edge in electricity:
        edge["comment"] = f"{COMPRESSION_COMMENT}. {edge['comment']}"
    diagnostics.append(
        {
            "code": "clic.compression_metadata_normalized",
            "activity_code": activity["code"],
            "message": "Bound the published conditional compression contract to its unique purchases; quantities and pressure caveats preserved.",
        }
    )


def prepare_clic_for_premise(
    prepared,
    database,
    *,
    version,
    family="ecoinvent",
    biosphere_name="biosphere3",
    keep_uncertainty=True,
    system_model="cutoff",
    biosphere_codes=None,
    classifications=None,
    namespace_identities=(),
    catalog_provider=None,
):
    """Return owned, fully linked foregrounds without changing the source graph."""
    target = resolve_inventory_target(version, family=family, system_model=system_model)
    if biosphere_codes is None:
        if target.technosphere.family == "ecoinvent":
            from .inventory_imports import get_biosphere_code

            biosphere_codes = get_biosphere_code(target.biosphere.version)
        else:
            from brightpath.background import catalog_provider_from_environment

            provider = catalog_provider or catalog_provider_from_environment()
            catalog = provider.load_biosphere(target.biosphere)
            biosphere_codes = {
                (name, tuple(categories), unit): code
                for (name, categories, unit), code in catalog.references.items()
            }
    if classifications is None:
        if target.technosphere.family == "ecoinvent":
            from .inventory_imports import get_classifications

            classifications = get_classifications()
        else:
            # Retain published classifications without assigning ecoinvent
            # classifications to a native UVEK supplier by label similarity.
            classifications = {}
    from .inventory_imports import get_classification_entry

    records = copy.deepcopy(prepared.activities)
    original_identities = {
        activity["code"]: activity_identity(activity) for activity in records
    }
    helpers = defaultdict(list)
    for activity in records:
        if activity.get("clic provenance", {}).get("role") == "generated helper":
            helpers[activity_identity(activity)].append(activity)
    exact, folded = defaultdict(list), defaultdict(list)
    for native in database:
        key = activity_identity(native)
        exact[key].append(native)
        folded[_folded(key)].append(native)
    foreground_identities = defaultdict(list)
    for activity in records:
        foreground_identities[activity_identity(activity)].append(activity)
    diagnostics = []
    if system_model == "EN15804":
        from ..iam.system_models import adapt_en15804_scrap_exchanges

        count = adapt_en15804_scrap_exchanges(records, database)
        if count:
            diagnostics.append(
                {"code": "clic.en15804_native_scrap_adapted", "exchange_count": count}
            )
    namespace_identities = set(namespace_identities)
    for key, members in foreground_identities.items():
        if (
            len(members) > 1
            or key in exact
            or _folded(key) in folded
            or key in namespace_identities
            or any(
                a.get("clic provenance", {}).get("role") == "generated helper"
                for a in members
            )
        ):
            for activity in members:
                provenance = activity.get("clic provenance", {})
                label = provenance.get("slug", activity["code"])
                original_name = activity["name"]
                activity["name"] = f"{original_name} [CLIC {label}]"
                diagnostics.append(
                    {
                        "code": "clic.namespaced_identity",
                        "activity_code": activity["code"],
                        "original_name": original_name,
                        "name": activity["name"],
                    }
                )
    by_code = {a["code"]: a for a in records}
    if len(by_code) != len(records):
        raise ClicInventoryError("CLIC foreground activity codes are not unique.")
    database_name = "premise-CLIC"
    for activity in records:
        activity["database"] = database_name
    for activity in records:
        original = original_identities[activity["code"]]
        # CLIC preserves nullable optional fields. Premise interprets an absent
        # production volume through its existing fallback, rather than float(None).
        if activity.get("production volume") is None:
            activity.pop("production volume", None)
        entry = get_classification_entry(classifications, original[0], original[1])
        if entry:
            activity.setdefault(
                "classifications", [(k, v) for k, v in entry.items() if v]
            )
        for index, edge in enumerate(activity["exchanges"]):
            kind = edge["type"]
            if edge.get("production volume") is None:
                edge.pop("production volume", None)
            if kind == "production":
                provider = activity
            elif kind == "technosphere":
                reference = edge.get("input")
                if (
                    isinstance(reference, (list, tuple))
                    and len(reference) == 2
                    and reference[0] == "__clic__"
                ):
                    if reference[1] not in by_code:
                        raise ClicInventoryError(
                            f"Missing internal CLIC supplier {reference[1]!r}."
                        )
                    provider = by_code[reference[1]]
                else:
                    key = activity_identity(edge)
                    candidates = helpers.get(key) or exact.get(key, ())
                    if not candidates:
                        candidates = folded.get(_folded(key), ())
                        if len(candidates) == 1:
                            diagnostics.append(
                                {
                                    "code": "clic.source_spelling_matched",
                                    "activity_code": activity["code"],
                                    "exchange_index": index,
                                    "requested": list(key),
                                    "matched": list(activity_identity(candidates[0])),
                                }
                            )
                    if len(candidates) != 1:
                        raise ClicInventoryError(
                            f"Expected one source supplier for {key!r} in {activity['name']!r}; "
                            f"found {len(candidates)}. No exchange was dropped or replaced."
                        )
                    provider = candidates[0]
            elif kind == "biosphere":
                categories = edge.get("categories", ())
                if isinstance(categories, str):
                    categories = categories.split("::")
                categories = tuple(categories)
                if not categories:
                    raise ClicInventoryError(
                        f"Missing biosphere compartment in {activity['name']!r}."
                    )
                legacy_key = (
                    edge["name"],
                    categories[0],
                    categories[1] if len(categories) > 1 else "unspecified",
                    edge["unit"],
                )
                exact_key = (edge["name"], categories, edge["unit"])
                key = exact_key if exact_key in biosphere_codes else legacy_key
                if key not in biosphere_codes:
                    raise ClicInventoryError(
                        f"Biosphere flow is absent from target {target.biosphere.label()}: {key!r}."
                    )
                edge["categories"] = categories
                edge["input"] = (biosphere_name, biosphere_codes[key])
                edge.pop("database", None)
                continue
            else:
                raise ClicInventoryError(
                    f"Unsupported CLIC exchange type {kind!r} in {activity['name']!r}."
                )
            edge.update(
                name=provider["name"],
                location=provider["location"],
                unit=provider["unit"],
                product=provider["reference product"],
            )
            if (
                provider.get("database") is not None
                and provider.get("code") is not None
            ):
                edge["input"] = (provider["database"], provider["code"])
            else:
                # Legacy source caches retain qualified identities rather than
                # source-project codes. The unique match above still certifies
                # the provider; export assigns its destination identifier.
                edge.pop("input", None)
            edge["reference product"] = provider["reference product"]
            edge.pop("categories", None)
            edge.pop("database", None)
        if not keep_uncertainty:
            for edge in activity["exchanges"]:
                edge["uncertainty type"] = 0
                edge["loc"] = edge["amount"]
                for name in ("scale", "shape", "minimum", "maximum", "negative"):
                    edge.pop(name, None)
        _normalize_compression_contract(activity, diagnostics)
    return records, diagnostics
