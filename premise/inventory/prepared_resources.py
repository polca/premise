"""Reviewed inventory resources declared in source-family mapping data."""

from __future__ import annotations

import copy
import json
import math
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")


def inventory_resource_paths(sectors, scenarios, family):
    """Select declared resources, honoring an explicit active-role requirement."""
    from ..core.filesystem_constants import VARIABLES_DIR
    from .activity_maps import get_mapping
    from .clic_requirements import applicable_roles, registry

    selected = set()
    scenarios = list(scenarios) or [{}]
    for sector in sectors:
        for filename in registry()["sector_mappings"].get(sector, ()):
            definitions = get_mapping(VARIABLES_DIR / filename, f"{family}_aliases")
            for role, entry in definitions.items():
                resource = entry.get("inventory_resource")
                if not resource:
                    continue
                active_only = entry.get("inventory_requires_active_role", False)
                if not isinstance(active_only, bool):
                    raise ValueError("inventory_requires_active_role must be boolean.")
                if not active_only or any(
                    scenario.get("iam data") is not None
                    and role
                    in applicable_roles(
                        sector,
                        get_mapping(
                            VARIABLES_DIR / filename,
                            f"{family}_aliases",
                            model=scenario.get("model"),
                        ),
                        scenario,
                    )
                    for scenario in scenarios
                ):
                    selected.add(resource)
    return selected


def inventory_resource_roots(resources):
    """Read CLIC dependencies before the ordinary archive preparation step."""
    from ..core.filesystem_constants import DATA_DIR

    roots = set()
    for resource in resources:
        definition = json.loads((DATA_DIR / resource).read_text())
        declared = definition.get("clic_roots", [])
        if not isinstance(declared, list) or any(
            not isinstance(slug, str) or not slug for slug in declared
        ):
            raise ValueError("Prepared inventory CLIC roots must be nonempty strings.")
        roots.update(declared)
    return roots


def _identity(record):
    return tuple(
        record.get(k, record.get("product") if k == "reference product" else None)
        for k in FIELDS
    )


def _check_supplier_quantities(database, checks):
    """Refuse a derived inventory when its declared supplier basis changed."""
    for check in checks:
        matches = [r for r in database if _identity(r) == _identity(check["identity"])]
        if len(matches) != 1:
            raise ValueError(
                "Prepared inventory supplier check is absent or ambiguous."
            )
        for requirement in check["exchanges"]:
            selector = requirement["selector"]
            edges = [
                edge
                for edge in matches[0]["exchanges"]
                if all(
                    (
                        tuple(edge.get(key, ())) == tuple(value)
                        if key == "categories"
                        else edge.get(
                            key,
                            edge.get("product") if key == "reference product" else None,
                        )
                        == value
                    )
                    for key, value in selector.items()
                )
            ]
            if (
                len(edges) != 1
                or any(
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    for value in (edges[0]["amount"], requirement["amount"])
                )
                or not math.isclose(
                    edges[0]["amount"], requirement["amount"], rel_tol=1e-12, abs_tol=0
                )
            ):
                raise ValueError(
                    "Prepared inventory supplier quantity changed; repeat its review."
                )


def prepare_inventory_resource(database, path, *, target=None, biosphere_name=None):
    """Link a packaged, already-reviewed inventory declared by mapping data."""
    definition = json.loads(Path(path).read_text())
    if target is not None and definition["target"] != {
        axis: target[axis] for axis in ("technosphere", "biosphere")
    }:
        raise ValueError(
            "Prepared inventory resource has a different background context."
        )
    _check_supplier_quantities(database, definition.get("supplier_checks", ()))
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
            if exchange["type"] == "biosphere" and biosphere_name is not None:
                exchange["input"] = (biosphere_name, exchange["input"][1])
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
