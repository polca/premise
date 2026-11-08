"""Explicit CLIC slugs consumed by premise's sector and IAM role mappings."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from .clic_archive import ClicInventoryError

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "data/clic/requirements.json"

IAM_ARRAYS = {
    "electricity": ("electricity_mix",),
    "fuels": (
        "petrol_blend",
        "diesel_blend",
        "natural_gas_blend",
        "hydrogen_blend",
        "kerosene_blend",
        "lpg_blend",
    ),
    "heat": (
        "buildings_heat_end_use",
        "industrial_heat_end_use",
        "secondary_heat_supply",
    ),
    "cdr": ("cdr_technology_mix",),
    "biomass": ("biomass_mix",),
    "steel": ("steel_technology_mix",),
    "cement": ("cement_technology_mix",),
    "cars": ("passenger_car_fleet",),
    "two_wheelers": ("two_wheelers_fleet",),
    "buses": ("bus_fleet",),
    "trucks": ("road_freight_fleet",),
    "trains": ("rail_freight_fleet",),
    "ships": ("sea_freight_fleet",),
    "final energy": ("final_energy_use",),
}


@lru_cache(maxsize=1)
def registry():
    data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported premise CLIC requirements registry.")
    return data


def role_slugs(mapping_filename, role):
    data = registry()
    filename = Path(mapping_filename).name
    slugs = set(data["mappings"].get(filename, {}).get(role, ()))
    graph = data.get("hybrid_sources", {})
    slugs.update(
        graph["nodes"][index]["clic_slug"]
        for index in graph.get("roles", {}).get(filename, {}).get(role, ())
        if graph["nodes"][index]["clic_slug"]
    )
    return tuple(sorted(slugs))


def applicable_roles(sector, definitions, scenario):
    """Restrict role roots to technologies declared by the scenario's IAM arrays."""
    iam = scenario.get("iam data")
    if iam is None:
        return tuple(definitions)
    arrays = [getattr(iam, attribute, None) for attribute in IAM_ARRAYS.get(sector, ())]
    declared = [
        array for array in arrays if array is not None and "variables" in array.coords
    ]
    if not declared:
        return tuple(definitions)
    production = getattr(iam, "production_volumes", None)
    if production is not None and "variables" in production.coords:
        declared.append(production)
    import numpy as np

    applicable = set()
    for array in declared:
        year = scenario.get("year")
        if year is not None and "year" in array.dims:
            years = array.coords["year"].values
            selected_year = min(max(year, min(years)), max(years))
            array = (
                array.sel(year=selected_year)
                if selected_year in years
                else array.interp(year=selected_year)
            )
        for variable in array.coords["variables"].values:
            values = np.asarray(array.sel(variables=variable).values)
            if np.any(np.isfinite(values) & (values != 0)):
                applicable.add(str(variable))
    return tuple(role for role in definitions if role in applicable)


def active_role_bindings(source_files):
    """Honor premise's reviewed target-specific source replacement profiles."""
    data = registry().get("hybrid_sources", {})
    active = {Path(path).name for path, _ in source_files}
    nodes = data.get("nodes", ())
    return {
        filename: {
            role: tuple(
                sorted(
                    {
                        nodes[index]["clic_slug"]
                        for index in indices
                        if nodes[index]["file"] in active and nodes[index]["clic_slug"]
                    }
                )
            )
            for role, indices in roles.items()
        }
        for filename, roles in data.get("roles", {}).items()
    }


def bind_activity_roles(activities, source_files):
    """Assign target-specific roles and reject unreviewed reference-unit changes."""
    data = registry()["hybrid_sources"]
    active = {Path(path).name for path, _ in source_files}
    expected = {}
    for filename, roles in data["roles"].items():
        for role, indices in roles.items():
            for index in indices:
                node = data["nodes"][index]
                if node["file"] in active and node["clic_slug"]:
                    expected.setdefault(node["clic_slug"], {}).setdefault(
                        f"{filename}:{role}", set()
                    ).add(node["identity"][3])
    for activity in activities:
        provenance = activity["clic provenance"]
        roles = expected.get(provenance.get("slug"), {})
        for role, units in roles.items():
            if activity["unit"] not in units:
                raise ClicInventoryError(
                    f"CLIC reference unit changed for premise role {role!r}: "
                    f"expected {sorted(units)!r}, got {activity['unit']!r}; "
                    "a reviewed role conversion is required."
                )
        provenance["role_bindings"] = sorted(roles)
    return active_role_bindings(source_files)


def required_roots(sectors, scenarios=(), *, source_files=None):
    """Union required sector roles across scenarios without reading inventories.

    Role presence in IAM production/market arrays determines applicability.
    When a caller has no IAM arrays, the full declared sector scope is used.
    Explicit support roots cover procedural transformations outside IAM maps.
    """
    from ..core.filesystem_constants import VARIABLES_DIR
    from .activity_maps import get_mapping

    data = registry()
    bindings = active_role_bindings(source_files) if source_files is not None else None
    active_files = (
        None if source_files is None else {Path(path).name for path, _ in source_files}
    )
    eligible = (
        None
        if source_files is None
        else {
            node["clic_slug"]
            for node in data["hybrid_sources"]["nodes"]
            if node["file"] in active_files and node["clic_slug"]
        }
    )
    roots = set()
    scenarios = list(scenarios) or [{}]
    for sector in sectors:
        roots.update(
            slug
            for slug in data.get("support", {}).get(sector, ())
            if eligible is None or slug in eligible
        )
        for filename in data["sector_mappings"].get(sector, ()):
            for scenario in scenarios:
                model = scenario.get("model")
                definitions = get_mapping(
                    VARIABLES_DIR / filename, "ecoinvent_aliases", model=model
                )
                for role in applicable_roles(sector, definitions, scenario):
                    roots.update(
                        role_slugs(filename, role)
                        if bindings is None
                        else bindings.get(filename, {}).get(role, ())
                    )
    return tuple(sorted(roots))
