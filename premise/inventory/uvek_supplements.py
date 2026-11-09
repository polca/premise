"""Pinned, reviewed native-context supplements needed by the UVEK pilot."""

from __future__ import annotations

import copy
import json
from pathlib import Path

GEOTHERMAL_PATH = (
    Path(__file__).resolve().parents[1]
    / "data/clic/uvek_2026_geothermal_screening.json"
)
FIELDS = ("name", "reference product", "location", "unit")


def _identity(record):
    return tuple(
        record.get(k, record.get("product") if k == "reference product" else None)
        for k in FIELDS
    )


def prepare_geothermal_supplement(database):
    """Resolve every supplier before appending; never edit native records.

    Only the electricity pilot calls this loader when its IAM requires geothermal.
    The template is already in the exact UVEK 2026 / biosphere 3.10 context.
    """
    definition = json.loads(GEOTHERMAL_PATH.read_text())
    records = copy.deepcopy(definition["activities"])
    existing = {record["code"]: record for record in database}
    present = [record["code"] in existing for record in records]
    if any(present):
        if not all(present) or any(
            _identity(existing[r["code"]]) != _identity(r) for r in records
        ):
            raise ValueError("Incomplete or conflicting UVEK geothermal supplement.")
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
                        f"UVEK geothermal supplier is absent or ambiguous: {_identity(exchange)!r}."
                    )
                supplier = matches[0]
            exchange["input"] = (supplier["database"], supplier["code"])
    database.extend(records)
    return records
