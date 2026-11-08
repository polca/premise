"""Select bundled inventory profiles before ordinary migration and linking.

Scientific recipes and version-specific supplier choices live in package data.
The importer validates the complete affected scope before changing any records.
"""

from copy import deepcopy
import json
import math
from pathlib import Path

from packaging.version import Version

from ..core.filesystem_constants import DATA_DIR

PROFILE_PATH = DATA_DIR / "utils/import/inventory_profiles.json"
_METADATA = {
    "comment",
    "source",
    "database",
    "code",
    "input",
    "output",
    "worksheet name",
}


def identity(record):
    products = [record[k] for k in ("reference product", "product") if record.get(k)]
    if len(set(products)) > 1:
        raise ValueError("Inventory profile product fields disagree")
    return (
        record.get("name"),
        products[0] if products else None,
        record.get("location"),
        record.get("unit"),
    )


def scientific_record(record):
    result = {
        k: record.get(k)
        for k in ("name", "reference product", "location", "unit", "production amount")
    }
    if "product" in record:
        result["product"] = record["product"]
    result["exchanges"] = [
        {k: v for k, v in e.items() if k not in _METADATA} for e in record["exchanges"]
    ]
    # Normalize JSON containers and reject non-finite or unserializable science.
    return json.loads(json.dumps(result, allow_nan=False))


def _same_science(actual, expected):
    # JSON scalar types are significant: True must not qualify as 1 kg.
    # Explicit records are reviewable without an opaque recipe digest.
    return json.dumps(actual, sort_keys=True, allow_nan=False) == json.dumps(
        expected, sort_keys=True, allow_nan=False
    )


def exchange_vector(record):
    rows = []
    for edge in record.get("exchanges", ()):
        amount = edge.get("amount")
        if (
            type(amount) not in (int, float)
            or not math.isfinite(amount)
            or edge.get("formula")
        ):
            raise ValueError("Inventory profile quantity must be finite and explicit")
        categories = edge.get("categories") or ()
        if isinstance(categories, str):
            categories = categories.split("::")
        rows.append([edge.get("type"), *identity(edge), list(categories), amount])
    return sorted(rows, key=repr)


def profiles(version, system_model, phase=None, source=None):
    """Return the data profiles applicable to this source and target."""
    if version is None:
        return []
    version = Version(str(version))
    result = []
    for profile in json.loads(PROFILE_PATH.read_text())["profiles"]:
        if phase is not None and profile["phase"] != phase:
            continue
        if source is not None and profile.get("source") != Path(source).name:
            continue
        if "versions" in profile and str(version) not in profile["versions"]:
            continue
        if "equivalent_version" in profile and version != Version(
            profile["equivalent_version"]
        ):
            continue
        if "minimum_version" in profile and version < Version(
            profile["minimum_version"]
        ):
            continue
        if "release" in profile and list(version.release[:2]) != profile["release"]:
            continue
        if "system_models" in profile and system_model not in profile["system_models"]:
            continue
        result.append(profile)
    return result


def _one(records, key):
    matches = [
        r for r in records if r.get("name") == key[0] and identity(r) == tuple(key)
    ]
    if len(matches) != 1:
        raise ValueError(f"Inventory profile requires one unique activity: {key[0]}")
    return matches[0]


def _validate(record, requirement):
    if "science" in requirement and not _same_science(
        scientific_record(record), requirement["science"]
    ):
        raise ValueError("Inventory profile scientific recipe changed")
    if "vector" in requirement and exchange_vector(record) != sorted(
        requirement["vector"], key=repr
    ):
        raise ValueError("Inventory profile exchange quantities or suppliers changed")
    if requirement.get("deterministic") and any(
        e.get("uncertainty type", 0) > 1 for e in record["exchanges"]
    ):
        raise ValueError("Inventory profile reference must be deterministic")
    if requirement.get("unit_output"):
        output = [
            e for e in record.get("exchanges", ()) if e.get("type") == "production"
        ]
        if (
            len(output) != 1
            or identity(output[0]) != identity(record)
            or output[0].get("amount") != 1
        ):
            raise ValueError("Inventory profile reference output changed")


def _check_private_consumers(records, selected, others, profile):
    names = set(profile.get("private_helpers", ()))
    products = set(profile.get("private_products", ()))
    links = {
        (r.get("database"), r["code"])
        for r in selected
        if r.get("name") in names and "code" in r
    }
    selected_ids = {id(r) for r in selected}
    for source in (records, *(imp.data for imp in others)):
        for record in source:
            if id(record) in selected_ids:
                continue
            for edge in record.get("exchanges", ()):
                link = edge.get("input")
                if edge.get("type") == "technosphere" and (
                    edge.get("name") in names
                    or any(
                        edge.get(k) in products
                        for k in ("product", "reference product")
                    )
                    or (isinstance(link, (list, tuple)) and tuple(link) in links)
                ):
                    raise ValueError(
                        "Inventory profile private helper has another consumer"
                    )


def _select(records, profile, database, others):
    if "exchange_aliases" in profile:
        changes = []
        for record in records:
            for edge in record.get("exchanges", ()):
                for alias in profile["exchange_aliases"]:
                    if (
                        edge.get("type") == "technosphere"
                        and edge.get("unit") == alias["source"][3]
                        and (
                            edge.get("name"),
                            edge.get("reference product", edge.get("product")),
                            edge.get("location"),
                        )
                        == tuple(alias["source"][:3])
                    ):
                        output = deepcopy(edge)
                        target = alias["target"]
                        output.update(
                            name=target[0], product=target[1], location=target[2]
                        )
                        output["reference product"] = target[1]
                        output.pop("input", None)
                        output["comment"] = (
                            alias["comment"] + " " + edge.get("comment", "")
                        )
                        changes.append((edge, output))
        # Stage first, then preserve the importer's existing exchange identities.
        for edge, output in changes:
            edge.clear()
            edge.update(output)
        return records
    if any(
        record.get("name") in profile.get("external_only", ()) for record in records
    ):
        raise ValueError(
            "Inventory profile references must come from their companion source"
        )
    for requirement in profile.get("require_suppliers", ()):
        _validate(_one(database, requirement["identity"]), requirement)
    replacement = profile.get("replace_inputs")
    if replacement:
        record = _one(records, replacement["identity"])
        actual = scientific_record(record)
        if _same_science(actual, replacement["selected"]):
            return records
        if not _same_science(actual, replacement["legacy"]):
            raise ValueError("Inventory profile input boundary changed")
        revised = deepcopy(record)
        revised["exchanges"] = [
            revised["exchanges"][0],
            deepcopy(replacement["exchange"]),
            *[
                e
                for e in revised["exchanges"][1:]
                if e.get("name") not in replacement["remove_names"]
            ],
        ]
        revised["comment"] = replacement["comment"] + record.get("comment", "")
        if not _same_science(scientific_record(revised), replacement["selected"]):
            raise ValueError("Inventory profile replacement boundary is inconsistent")
        return [revised if r is record else r for r in records]

    removals = profile.get("remove", ())
    names = {r["identity"][0] for r in removals}
    selected = [r for r in records if r.get("name") in names]
    if selected:
        if len(selected) != len(removals):
            raise ValueError(
                "Inventory profile source is partially selected or duplicated"
            )
        for requirement in removals:
            _validate(_one(selected, requirement["identity"]), requirement)
    _check_private_consumers(records, selected, others, profile)

    relinks = profile.get("relink", ())
    revised, states = {}, []
    if relinks:
        supplier_names = {
            key
            for rule in relinks
            for key in (rule["supplier"][0], rule["replacement"][0])
        }
        supplier_products = {
            key
            for rule in relinks
            for key in (rule["supplier"][1], rule["replacement"][1])
        }
        purchases = [
            (r, i, e)
            for r in records
            for i, e in enumerate(r.get("exchanges", ()))
            if e.get("type") == "technosphere"
            and (
                e.get("name") in supplier_names
                or any(
                    e.get(k) in supplier_products
                    for k in ("product", "reference product")
                )
            )
        ]
        if len(purchases) != len(relinks):
            raise ValueError("Inventory profile consumer scope changed")
        for rule in relinks:
            matches = [
                (r, i, e)
                for r, i, e in purchases
                if identity(r) == tuple(rule["buyer"])
            ]
            if len(matches) != 1:
                raise ValueError("Inventory profile consumer identity changed")
            record, position, edge = matches[0]
            quantity = edge.get("amount")
            if (
                type(quantity) not in (int, float)
                or not math.isfinite(quantity)
                or edge.get("formula")
            ):
                raise ValueError("Inventory profile purchase quantity changed")
            if ("quantity" in rule and quantity != rule["quantity"]) or (
                rule.get("positive") and quantity <= 0
            ):
                raise ValueError("Inventory profile purchase quantity changed")
            actual, target = identity(edge), tuple(rule["replacement"])
            if actual not in (tuple(rule["supplier"]), target):
                raise ValueError("Inventory profile purchase supplier changed")
            for required in rule.get("required_exchanges", ()):
                found = [
                    e
                    for e in record["exchanges"]
                    if e.get("name") == required["identity"][0]
                ]
                if (
                    len(found) != 1
                    or identity(found[0]) != tuple(required["identity"])
                    or any(found[0].get(k) != required[k] for k in ("type", "amount"))
                ):
                    raise ValueError("Inventory profile required exchange changed")
            states.append(actual == target)
            copy = revised.setdefault(id(record), deepcopy(record))
            output = copy["exchanges"][position]
            output.update(name=target[0], location=target[2], unit=target[3])
            for field in ("product", "reference product"):
                if field in output or profile.get("add_product"):
                    output[field] = target[1]
            for field in profile.get("strip_links", ()):
                output.pop(field, None)
        if all(states) and not selected:
            return records
        if any(states) or not selected:
            raise ValueError("Inventory profile source is partially selected")
    selected_ids = {id(r) for r in selected}
    return [revised.get(id(r), r) for r in records if id(r) not in selected_ids]


def apply_inventory_profiles(
    importer,
    source,
    version,
    system_model,
    database,
    *,
    phase="raw",
    other_importers=(),
):
    """Select a bundled source atomically, preserving unrelated data and state."""
    original = importer.data
    result = original
    for profile in profiles(version, system_model, phase, source):
        result = _select(result, profile, database, other_importers)
    if result is not original:
        importer.data = result


def companion_projection_required(profile, retained_identities=None):
    """Only project companions when an imported record would be omitted.

    The workbook pipeline imports the complete source and validates every role.
    CLIC has already replaced public records before retained records are prepared.
    Companions that affect none of those records need no workbook extraction.
    """
    return retained_identities is None or any(
        tuple(key) in retained_identities.get(operation["source"], ())
        for operation in profile.get("omit", ())
        for key in operation["identities"]
    )


def select_companion_profiles(
    filepaths,
    importers,
    version,
    system_model,
    *,
    active_sources=None,
    retained_identities=None,
):
    """Validate companion roles together before omitting superseded definitions."""
    selected_profiles = profiles(version, system_model, "projection")
    if not selected_profiles:
        return
    sources = {}
    for (path, _), importer in zip(filepaths, importers, strict=True):
        sources.setdefault(Path(path).name, []).append(importer)
    for profile in selected_profiles:
        if not companion_projection_required(profile, retained_identities):
            continue
        if active_sources is not None and not any(
            requirement["source"] in active_sources
            for requirement in profile["requires"]
        ):
            continue
        selected = {}
        for requirement in profile["requires"]:
            matches = sources.get(requirement["source"], ())
            if len(matches) != 1:
                raise ValueError(
                    "Inventory profile requires one source per companion role"
                )
            importer = matches[0]
            selected[requirement["source"]] = importer
            if requirement.get("complete") and len(importer.data) != len(
                requirement["identities"]
            ):
                raise ValueError("Inventory profile companion activity count changed")
            for key in requirement["identities"]:
                _validate(_one(importer.data, key), {"unit_output": True})
        revised = []
        for operation in profile["omit"]:
            importer = selected[operation["source"]]
            omitted = {tuple(k) for k in operation["identities"]}
            revised.append(
                (importer, [r for r in importer.data if identity(r) not in omitted])
            )
        for importer, records in revised:
            importer.data = records


def source_selection_rules(version, system_model):
    """Return applicable companion-source roles without reading inventory bodies."""
    if version is None:
        return []
    result = []
    for rule in json.loads(PROFILE_PATH.read_text())["source_selection"]:
        if "versions" in rule and str(version) not in rule["versions"]:
            continue
        if "minimum_version" in rule and Version(str(version)) < Version(
            rule["minimum_version"]
        ):
            continue
        if "system_models" in rule and system_model not in rule["system_models"]:
            continue
        result.append(rule)
    return result


def select_source_files(filepaths, version, system_model, catalog):
    """Resolve declared source roles in their original preparation order."""
    rules = source_selection_rules(version, system_model)
    selected = []
    for filepath in filepaths:
        for rule in rules:
            if catalog[rule["source"]] != filepath[0]:
                continue
            selected.extend(
                (catalog[name], source_version)
                for name, source_version in rule.get("before", ())
            )
            if "replacement" in rule:
                selected.extend(
                    (catalog[name], source_version)
                    for name, source_version in rule["replacement"]
                )
                break
            if rule.get("omit"):
                break
        else:
            selected.append(filepath)
    return selected
