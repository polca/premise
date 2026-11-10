"""Reviewed inventory resources declared in source-family mapping data."""

from __future__ import annotations

import copy
import json
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")


def _identity(record):
    return tuple(
        record.get(k, record.get("product") if k == "reference product" else None)
        for k in FIELDS
    )


def prepare_inventory_resource(database, path, *, target=None):
    """Link a packaged, already-reviewed inventory declared by mapping data."""
    definition = json.loads(Path(path).read_text())
    if target is not None and definition["target"] != {
        axis: target[axis] for axis in ("technosphere", "biosphere")
    }:
        raise ValueError(
            "Prepared inventory resource has a different background context."
        )
    records = copy.deepcopy(definition["activities"])
    existing = {record["code"]: record for record in database}
    present = [record["code"] in existing for record in records]
    if any(present):
        if not all(present) or any(
            _identity(existing[r["code"]]) != _identity(r) for r in records
        ):
            raise ValueError("Incomplete or conflicting prepared inventory resource.")
        return []
    providers = {}
    for record in database:
        providers.setdefault(_identity(record), []).append(record)
    internal = {r["code"]: r for r in records}
    for record in records:
        for exchange in record["exchanges"]:
            if exchange["type"] in ("production", "technosphere"):
                exchange["product"] = exchange.pop(
                    "reference product",
                    (
                        record["reference product"]
                        if exchange["type"] == "production"
                        else exchange.get("product")
                    ),
                )
            if exchange.get("input"):
                exchange["input"] = tuple(exchange["input"])
            if "categories" in exchange:
                exchange["categories"] = tuple(exchange["categories"])
            if exchange["type"] != "technosphere":
                continue
            reference = exchange.get("input")
            if reference and reference[1] in internal:
                supplier = internal[reference[1]]
            else:
                matches = providers.get(_identity(exchange), [])
                if len(matches) != 1:
                    raise ValueError(
                        f"Prepared inventory supplier is absent or ambiguous: {_identity(exchange)!r}."
                    )
                supplier = matches[0]
            exchange["input"] = (supplier["database"], supplier["code"])
    database.extend(records)
    return records
