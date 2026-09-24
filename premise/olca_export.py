"""Export complete prepared scenarios through the optional Brightpath codec."""

from __future__ import annotations

import csv
import json
import math
import re
import sys
import tempfile
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path

import numpy as np

from .export_payload import normalize_activity_parameters
from .filesystem_constants import DATA_DIR


def load_method_mapping(method_package, version):
    """Check optional runtime support and load the exact local biosphere mapping."""
    if sys.version_info < (3, 12):
        raise RuntimeError(
            "JSON-LD export requires Python 3.12+ and Brightpath. "
            "Use format='simapro' for the legacy CSV exporter on older Python."
        )
    source_tables = {"3.8": "flows_biosphere_38.csv", "3.12": "flows_biosphere_312.csv"}
    if str(version) not in source_tables:
        raise ValueError(
            "The JSON-LD method mapping currently supports ecoinvent 3.8 and 3.12. "
            "Use format='simapro' for other versions."
        )
    if method_package is None:
        raise ValueError(
            "JSON-LD export requires method_package: the matching local ecoinvent "
            "openLCA LCIA method-package directory or ZIP."
        )
    try:
        from brightpath.formats.openlca_methods import OpenLCAMethodMapping
    except ImportError as error:
        raise ImportError(
            "Install Brightpath with local openLCA method-mapping support "
            "in this Python 3.12+ environment (see the openLCA export guide)."
        ) from error
    return OpenLCAMethodMapping(
        method_package,
        DATA_DIR / "utils" / "export" / source_tables[str(version)],
        biosphere_version=str(version),
        conflict_policy="preserve" if str(version) == "3.8" else "error",
    )


def _identity(record):
    return (
        record.get("name"),
        record.get("reference product") or record.get("product"),
        record.get("location"),
        record.get("unit"),
    )


def _plain(value):
    """Detach inventory-store mappings and convert NumPy metadata for JSON."""
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    if isinstance(value, np.ndarray):
        return _plain(value.tolist())
    if isinstance(value, np.generic):
        return _plain(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def scenario_export_identity(scenario, version, system_model):
    """Stable identity shared by archive names, processes, and product flows."""
    payload = [
        "premise-openlca-v1",
        str(version),
        system_model,
        scenario["model"],
        scenario["pathway"],
        int(scenario["year"]),
        [item.get("scenario") for item in scenario.get("external scenarios", [])],
    ]
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(payload, ensure_ascii=False))
    label = "_".join(str(scenario[field]) for field in ("model", "pathway", "year"))
    label = re.sub(r"[^\w.-]+", "_", label)
    return namespace, f"{label}_{namespace.hex[:8]}"


def _classification(dataset, scheme):
    for entry in dataset.get("classifications", ()) or ():
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            continue
        if entry[0] != scheme or not isinstance(entry[1], str):
            continue
        code, separator, description = entry[1].partition(":")
        if separator and code.strip() and description.strip():
            # Slashes in descriptions are text, not extra folder levels.
            description = re.sub(r"[/\\]+", " - ", description)
            return code.strip(), " ".join(description.split())
    return None


@lru_cache(maxsize=1)
def _isic_titles():
    """Load the bundled UNSD Rev.4 numeric hierarchy, retaining leading zeroes."""
    path = DATA_DIR / "utils" / "export" / "isic_rev4.csv"
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    titles = {row["Code"]: row["Description"] for row in rows}
    if len(titles) != len(rows):
        raise ValueError("Duplicate codes in the bundled ISIC Rev.4 hierarchy.")
    for code, title in titles.items():
        if not code.isdigit() or len(code) not in (2, 3, 4) or not title:
            raise ValueError(f"Invalid ISIC Rev.4 entry: {code!r}.")
        if len(code) > 2 and code[:-1] not in titles:
            raise ValueError(f"Missing ISIC Rev.4 parent for {code!r}.")
    return titles


def _isic_category_path(code, description, titles):
    parents = [
        code[:length]
        for length in (2, 3, 4)
        if len(code) >= length and code[:length] in titles
    ]
    parts = [f"{parent} - {titles[parent]}" for parent in parents]
    if code not in titles:
        # Keep ecoinvent extensions below their deepest recognized ancestor.
        # Unknown codes never acquire invented official titles or parents.
        if not parts:
            parts.append("Unclassified ISIC")
        parts.append(f"{code} - {description}")
    return "/".join(" ".join(re.sub(r"[/\\]+", " - ", part).split()) for part in parts)


def build_process_categories(datasets):
    """Group processes by the official ISIC division/group/class hierarchy.

    For missing ISIC, use the same product/unit only if all classified suppliers
    agree. Otherwise retain the source CPC classification or mark Unclassified.
    The most frequent description per code makes punctuation variants converge;
    ties are resolved lexically so inventory ordering cannot affect the result.
    """
    descriptions = defaultdict(Counter)
    product_codes = defaultdict(set)
    for dataset in datasets:
        classification = _classification(dataset, "ISIC rev.4 ecoinvent")
        if classification:
            code, description = classification
            descriptions[code][description] += 1
            product_codes[(dataset.get("reference product"), dataset.get("unit"))].add(
                code
            )
    titles = _isic_titles()
    labels = {
        code: _isic_category_path(
            code, min(counts, key=lambda text: (-counts[text], text)), titles
        )
        for code, counts in descriptions.items()
    }
    result = {}
    for dataset in datasets:
        classification = _classification(dataset, "ISIC rev.4 ecoinvent")
        candidates = product_codes[
            (dataset.get("reference product"), dataset.get("unit"))
        ]
        if classification:
            category = labels[classification[0]]
        elif len(candidates) == 1:
            category = labels[next(iter(candidates))]
        else:
            cpc = _classification(dataset, "CPC")
            category = f"CPC {cpc[0]} - {cpc[1]}" if cpc else "Unclassified"
        result[_identity(dataset)] = category
    return result


def build_document(scenario, version, system_model):
    """Copy a prepared inventory, enforce provider closure, and namespace IDs."""
    from brightpath.core import (
        BackgroundContext,
        BiosphereProfile,
        FormatProfile,
        InventoryContext,
        TechnosphereProfile,
    )
    from brightpath.models import InventoryDocument

    namespace, label = scenario_export_identity(scenario, version, system_model)
    data = [_plain(dataset) for dataset in scenario["database"]]
    categories = build_process_categories(data)
    suppliers = {}
    for dataset in data:
        identity = _identity(dataset)
        if not all(isinstance(value, str) and value for value in identity):
            raise ValueError(f"Incomplete openLCA activity identity: {identity!r}.")
        if identity in suppliers:
            raise ValueError(f"Ambiguous openLCA supplier identity: {identity!r}.")
        suppliers[identity] = dataset
        production = [e for e in dataset["exchanges"] if e.get("type") == "production"]
        if len(production) != 1:
            raise ValueError(f"{identity!r} must have exactly one production exchange.")
        if production[0].get("unit") != dataset["unit"]:
            raise ValueError(
                f"Production unit differs from activity unit: {identity!r}."
            )
        product = production[0].get("reference product") or production[0].get("product")
        if product != dataset["reference product"]:
            raise ValueError(
                f"Production product differs from activity product: {identity!r}."
            )
        if not production[0].get("amount"):
            raise ValueError(f"Zero or missing production amount: {identity!r}.")
        key = json.dumps(identity, ensure_ascii=False)
        dataset["openlca process"] = {
            "@id": str(uuid.uuid5(namespace, "process:" + key))
        }
        dataset["openlca category"] = categories[identity]
        production[0]["openlca flow"] = {
            "@id": str(uuid.uuid5(namespace, "product:" + key))
        }
        normalize_activity_parameters(dataset)

    for dataset in data:
        for exchange in dataset["exchanges"]:
            amount = exchange.get("amount")
            if not isinstance(amount, (int, float)) or not math.isfinite(amount):
                raise ValueError(f"Invalid exchange amount in {dataset['name']!r}.")
            if exchange.get("type") == "biosphere":
                categories = exchange.get("categories") or []
                # Brightway omits the unspecified subcompartment on some flows;
                # Premise's versioned UUID table spells it out explicitly.
                if len(categories) == 1:
                    exchange["categories"] = [categories[0], "unspecified"]
            if exchange.get("type") == "technosphere":
                identity = _identity(exchange)
                if identity not in suppliers:
                    raise ValueError(
                        f"Unresolved openLCA supplier {identity!r} in {dataset['name']!r}. "
                        "Export the complete scenario, including every supplier."
                    )
    # Category segments start with zero-padded official codes. Sorting the
    # segments also visits a parent before its descendants during import.
    data.sort(
        key=lambda dataset: (
            tuple(dataset["openlca category"].split("/")),
            tuple(str(value).casefold() for value in _identity(dataset)),
        )
    )
    context = InventoryContext(
        format=FormatProfile("openlca_jsonld"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", str(version), system_model),
            biosphere=BiosphereProfile("ecoinvent", str(version)),
        ),
    )
    return InventoryDocument(data=data, context=context, database_name=label)


def export_scenario(scenario, filepath, version, system_model, method_mapping):
    """Write a scenario ZIP and coverage sidecar after successful serialization."""
    from brightpath.formats.openlca_jsonld import write_openlca_jsonld

    document = build_document(scenario, version, system_model)
    _, label = scenario_export_identity(scenario, version, system_model)
    directory = Path(filepath)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"openlca_export_{label}.zip"
    with tempfile.TemporaryDirectory(prefix=".openlca-", dir=directory) as temporary:
        staged = Path(temporary) / destination.name
        write_openlca_jsonld(document, staged, method_mapping=method_mapping)
        coverage = staged.with_suffix(".biosphere-coverage.json")
        # A failed render never publishes a partial ZIP or changes an old export.
        coverage.replace(destination.with_suffix(".biosphere-coverage.json"))
        staged.replace(destination)
    return destination
