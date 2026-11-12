"""Translate reviewed quantity lookups before the shared workbook preparation."""

import copy
import warnings
from pathlib import Path

import yaml

from .clic_archive import ClicInventoryError, activity_identity, content_hash
from .inventory_imports import BaseInventoryImport

REVIEW_PATH = (
    Path(__file__).resolve().parents[1]
    / "data/utils/import/native_replacement_quantities.yaml"
)
NATIVE_INPUT = "_premise_prepared_native_input"
NATIVE_BIOSPHERE = "_premise_prepared_native_biosphere"


def _selector(edge):
    result = {key: edge[key] for key in ("type", "name", "unit")}
    if edge["type"] == "technosphere":
        result.update(
            product=edge.get("product", edge.get("reference product")),
            location=edge.get("location"),
        )
    else:
        categories = edge.get("categories", ())
        result["categories"] = (
            categories.split("::") if isinstance(categories, str) else list(categories)
        )
    return result


def prepare_replacement_quantities(records, database, background, target):
    """Fill owned placeholders with explicitly reviewed native donor inputs.

    Reuse BaseInventoryImport's grouping and filling rules. Native purchases
    become boundary anchors during migration; native elementary flows are held
    separately until source-profile migration finishes. Never migrate an
    unknown source quantity as though it were a measured physical zero.
    """
    review = yaml.safe_load(REVIEW_PATH.read_text())
    if review.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported native replacement review schema.")
    pending = []
    diagnostics = []
    for record in records:
        placeholders = [
            edge
            for edge in record["exchanges"]
            if "replacement name" in edge
            and edge["type"] in {"technosphere", "biosphere"}
        ]
        if not placeholders:
            continue
        provenance = record["inventory provenance"]
        recipes = [
            recipe
            for recipe in review["recipes"]
            if recipe["target"] == target
            and recipe["file"] == provenance["file"]
            and recipe["source_version"] == background["technosphere"]["version"]
            and background["technosphere"]["family"] == "ecoinvent"
            and background["technosphere"]["system_model"] == "cutoff"
            and background["biosphere"]
            == {"family": "ecoinvent", "version": recipe["source_version"]}
            and recipe["activity"] == list(activity_identity(record))
        ]
        if len(recipes) != 1:
            raise ClicInventoryError(
                f"Unfilled replacement quantities in {record['name']!r}: "
                "no unique reviewed native donor recipe. The zero-valued "
                "licensed placeholders cannot be imported as physical zeros. "
                f"Review file: {REVIEW_PATH}"
            )
        recipe = recipes[0]
        donors = [
            activity
            for activity in database
            if list(activity_identity(activity)) == recipe["donor"]
        ]
        if len(donors) != 1:
            raise ClicInventoryError(
                f"Missing or ambiguous native donor: {recipe['donor']!r}."
            )
        donor = donors[0]
        production = [e for e in donor["exchanges"] if e["type"] == "production"]
        if (
            len(production) != 1
            or production[0]["amount"] != 1
            or recipe["donor_reference_amount"] != 1
        ):
            raise ClicInventoryError(
                "Native replacement donor must supply one positive reference unit."
            )
        slots = recipe["slots"]
        if len(placeholders) != len(slots):
            raise ClicInventoryError("Reviewed replacement placeholder scope changed.")
        queries, omissions, preserved, seen = [], [], [], set()
        for edge in placeholders:
            matches = [
                i for i, slot in enumerate(slots) if slot["source"] == _selector(edge)
            ]
            parent = [
                edge.get("replacement " + key)
                for key in ("name", "product", "location")
            ]
            if (
                len(matches) != 1
                or matches[0] in seen
                or parent
                != slots[matches[0]].get(
                    "replacement_source", recipe["replacement_source"]
                )
                or edge.get("formula") != slots[matches[0]].get("source_formula")
            ):
                raise ClicInventoryError(
                    "Reviewed replacement placeholder identity or quantity changed. "
                    f"Dataset: {record['name']!r}; exchange: {_selector(edge)!r}; "
                    f"replacement source: {parent!r}."
                )
            seen.add(matches[0])
            slot = slots[matches[0]]
            if slot.get("preserve_explicit_quantity"):
                if (
                    edge["type"] != "technosphere"
                    or edge["amount"] <= 0
                    or not slot.get("limitation")
                ):
                    raise ClicInventoryError(
                        "Reviewed explicit replacement input must retain a positive source quantity and limitation."
                    )
                retained = copy.deepcopy(edge)
                for field in (
                    "replacement name",
                    "replacement product",
                    "replacement location",
                ):
                    retained.pop(field, None)
                preserved.append(retained)
                continue
            if edge["amount"] != 0 or edge.get("uncertainty type", 0) > 1:
                raise ClicInventoryError(
                    "Reviewed unknown replacement quantity changed or has unsupported uncertainty."
                )
            if not slot["native"]:
                if not slot.get("limitation"):
                    raise ClicInventoryError(
                        "A native absence requires an explicit omission limitation."
                    )
                omissions.append(copy.deepcopy(slot))
            for selector in slot["native"]:
                native = [e for e in donor["exchanges"] if _selector(e) == selector]
                if not native:
                    raise ClicInventoryError(
                        f"Native replacement input is missing: {selector!r}."
                    )
                query = copy.deepcopy(edge)
                query.update(selector)
                if query["type"] == "technosphere":
                    query["reference product"] = query["product"]
                else:
                    query["categories"] = tuple(query["categories"])
                (
                    query["replacement name"],
                    query["replacement product"],
                    query["replacement location"],
                ) = recipe["donor"][:3]
                if native[0].get("input"):
                    query["input"] = native[0]["input"]
                queries.append(query)
        filler = object.__new__(BaseInventoryImport)
        filler.database = [donor]
        filled = {"exchanges": queries}
        filler.fill_dataset_data_gaps(filled)
        if any("replacement name" in e for e in filled["exchanges"]):
            raise ClicInventoryError(
                "Native replacement lookup remained unresolved; no placeholder was accepted."
            )
        for edge in filled["exchanges"]:
            if edge["type"] != "technosphere":
                continue
            suppliers = [
                a for a in database if activity_identity(a) == activity_identity(edge)
            ]
            if (
                len(suppliers) != 1
                or not suppliers[0].get("code")
                or not suppliers[0].get("database")
            ):
                raise ClicInventoryError(
                    f"Native replacement supplier is not uniquely linked: {_selector(edge)!r}."
                )
            edge["input"] = (suppliers[0]["database"], suppliers[0]["code"])
            edge[NATIVE_INPUT] = True
        pending.append(
            (
                record,
                placeholders,
                filled["exchanges"],
                preserved,
                {
                    "recipe": recipe["id"],
                    "review_sha256": content_hash(recipe),
                    "donor": recipe["donor"],
                    "limitations": recipe["limitations"],
                    "omissions": omissions,
                    "preserved explicit exchanges": copy.deepcopy(preserved),
                    "filled exchanges": [copy.deepcopy(e) for e in filled["exchanges"]],
                },
            )
        )
    # Apply only after the whole owned batch passes its identity and link checks.
    for record, placeholders, filled, preserved, audit in pending:
        removed = {id(e) for e in placeholders}
        record["exchanges"] = [e for e in record["exchanges"] if id(e) not in removed]
        record["exchanges"].extend(e for e in filled if e["type"] == "technosphere")
        record["exchanges"].extend(preserved)
        record[NATIVE_BIOSPHERE] = [e for e in filled if e["type"] == "biosphere"]
        record["inventory provenance"]["native replacement quantities"] = audit
        diagnostics.append(
            {
                "code": "clic.native_replacement_quantities_filled",
                "severity": "warning",
                "activity": list(activity_identity(record)),
                **copy.deepcopy(audit),
            }
        )
        warnings.warn(
            f"Filled replacement quantities in {record['name']!r} from native donor "
            f"{audit['donor']!r}; {len(audit['omissions'])} source slots are absent "
            "from that donor and omitted. Screening differences and unknown score "
            "effects are recorded in inventory provenance.",
            UserWarning,
            stacklevel=2,
        )
    return diagnostics
