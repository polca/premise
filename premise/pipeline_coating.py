"""Select two approved pipeline coating references for ecoinvent 3.12 cutoff.

Only the default hydrogen-distribution source is passed to this adapter. Its
pipeline parents remain authored against 3.7. Older targets keep the qualified
legacy reference: inverse aggregation cannot preserve the modern recipe.
"""

import hashlib
import json
import math
from copy import deepcopy

from .system_models import ATTRIBUTIONAL_SYSTEM_MODELS

LEGACY = (
    "aluminium-zinc coating alloy production, for galvanizing",
    "aluminium-zinc coating alloy, for galvanizing",
    "GLO",
    "kilogram",
)
REFERENCES = (
    (
        "aluminium-zinc-silicon coating production, hot-dip, on distribution pipe",
        "aluminium-zinc-silicon coating, on distribution pipe",
        "RER",
        "kilogram",
    ),
    (
        "aluminium-zinc-silicon coating production, hot-dip, on transmission pipe",
        "aluminium-zinc-silicon coating, on transmission pipe",
        "RER",
        "kilogram",
    ),
)
BUYERS = (
    (
        "hydrogen distribution pipeline construction",
        "pipeline, for hydrogen distribution",
        "RER",
        "kilometer",
        75.25328256144047,
    ),
    (
        "hydrogen transmission pipeline construction",
        "pipeline, for hydrogen transmission",
        "RER",
        "kilometer",
        876.7114153394484,
    ),
)
# Executable exchange vectors only: comments, generated links and database
# codes are deliberately outside these guards. Approved data/history pins are
# retained in the companion's CLIC-reference metadata and application receipts.
LEGACY_RECIPE_SHA256 = (
    "87b3c1a76fb1c78e3a7c0efa04253676b105fef843a79176bcb537f5fdef2bc1"
)
REFERENCE_RECIPE_SHA256 = (
    "21899cf17d5a2d129b6dca70702c30e7ed5ff56c9746ee19dea3b3c8f0f53279",
    "9302f8bb66a007b15948ad9655c7e146b64919cff3ed3cd85234e396a2781de2",
)


def _identity(record):
    products = [
        record[field]
        for field in ("reference product", "product")
        if isinstance(record.get(field), str) and record[field]
    ]
    if len(set(products)) > 1:
        raise ValueError("Pipeline coating product fields disagree")
    return (
        record.get("name"),
        products[0] if products else None,
        record.get("location"),
        record.get("unit"),
    )


def _recipe_digest(record):
    rows = []
    for edge in record.get("exchanges", ()):
        amount = edge.get("amount")
        if (
            isinstance(amount, bool)
            or not isinstance(amount, (int, float))
            or not math.isfinite(amount)
            or edge.get("formula")
        ):
            raise ValueError("Pipeline coating recipe quantity changed")
        categories = edge.get("categories") or ()
        if isinstance(categories, str):
            categories = categories.split("::")
        rows.append(
            [
                edge.get("type"),
                *_identity(edge),
                list(categories),
                float(amount),
            ]
        )
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True))
    return hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()


def use_native_pipeline_coating(importer, version, system_model, database):
    """Atomically rebind exactly two purchases; preserve all doses/uncertainty."""
    if str(version) != "3.12" or system_model not in ATTRIBUTIONAL_SYSTEM_MODELS:
        return 0
    data = importer.data
    if any(record.get("name") in {key[0] for key in REFERENCES} for record in data):
        raise ValueError("Pipeline coating references require the separate companion")
    for identity, digest in zip(REFERENCES, REFERENCE_RECIPE_SHA256, strict=True):
        loaded = [
            record
            for record in database
            if record.get("name") == identity[0] and _identity(record) == identity
        ]
        if len(loaded) != 1 or _recipe_digest(loaded[0]) != digest:
            raise ValueError("Pipeline coating requires one exact loaded reference")
    producers = [record for record in data if record.get("name") == LEGACY[0]]
    purchases = [
        (record, i, edge)
        for record in data
        for i, edge in enumerate(record.get("exchanges", ()))
        if edge.get("type") == "technosphere"
        and (
            edge.get("name") in {LEGACY[0], *(key[0] for key in REFERENCES)}
            or edge.get("reference product")
            in {LEGACY[1], *(key[1] for key in REFERENCES)}
            or edge.get("product") in {LEGACY[1], *(key[1] for key in REFERENCES)}
        )
    ]
    if len(purchases) != 2:
        raise ValueError("Pipeline coating requires exactly two reviewed purchases")
    revised = {}
    selected = []
    for buyer, target in zip(BUYERS, REFERENCES, strict=True):
        matches = [(r, i, e) for r, i, e in purchases if _identity(r) == buyer[:4]]
        if len(matches) != 1:
            raise ValueError("Pipeline coating buyer identity changed")
        record, position, edge = matches[0]
        if (
            type(edge.get("amount")) not in (int, float)
            or edge["amount"] != buyer[4]
            or edge.get("formula")
        ):
            raise ValueError("Pipeline coating purchase quantity changed")
        actual = _identity(edge)
        if actual not in (LEGACY, target):
            raise ValueError("Pipeline coating purchase supplier changed")
        selected.append(actual == target)
        copy = deepcopy(record)
        replacement = copy["exchanges"][position]
        replacement.update(name=target[0], location=target[2], unit=target[3])
        for field in ("product", "reference product"):
            if field in replacement:
                replacement[field] = target[1]
        for field in ("input", "output", "database", "code"):
            replacement.pop(field, None)
        revised[id(record)] = copy
    if all(selected) and not producers:
        return 0
    if any(selected) or len(producers) != 1:
        raise ValueError("Pipeline coating source is partially selected or duplicated")
    producer = producers[0]
    if (
        _identity(producer) != LEGACY
        or _recipe_digest(producer) != LEGACY_RECIPE_SHA256
    ):
        raise ValueError("Pipeline coating legacy producer recipe changed")
    importer.data = [
        revised.get(id(record), record) for record in data if record is not producer
    ]
    return 2
