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


def family_role_overrides(family):
    """Return explicit reviewed CLIC choices scoped to the background family."""
    if family == "ecoinvent":
        return {}
    if family != "uvek":
        raise ValueError(f"Unsupported CLIC role family {family!r}.")
    from ..core.filesystem_constants import VARIABLES_DIR
    from .activity_maps import get_mapping

    files = {name for names in registry()["sector_mappings"].values() for name in names}
    return {
        f"{filename}:{role}": definition
        for filename in sorted(files)
        for role, definition in get_mapping(
            VARIABLES_DIR / filename, "uvek_aliases"
        ).items()
        if definition.get("clic_family_override")
    }


def bind_activity_roles(activities, source_files, *, family="ecoinvent"):
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
    overrides = family_role_overrides(family)
    for roles in expected.values():
        for role in overrides:
            roles.pop(role, None)
    for role, definition in overrides.items():
        for slug in definition["clic_slugs"]:
            expected.setdefault(slug, {})[role] = {definition["clic_unit"]}
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
            if role in overrides:
                references = [
                    e
                    for e in activity.get("exchanges", ())
                    if e.get("type") == "production"
                ]
                if (
                    len(references) != 1
                    or references[0].get("amount")
                    != overrides[role]["clic_reference_amount"]
                    or references[0].get("unit", activity["unit"])
                    != overrides[role]["clic_unit"]
                ):
                    raise ClicInventoryError(
                        f"CLIC production reference changed for reviewed {family} role {role!r}."
                    )
        provenance["role_bindings"] = sorted(roles)
        provenance.pop("role_binding_limitations", None)
        limitations = {
            role: list(overrides[role]["clic_limitations"])
            for role in roles
            if role in overrides
        }
        if limitations:
            provenance["role_binding_limitations"] = limitations
    bindings = active_role_bindings(source_files)
    for role, definition in overrides.items():
        filename, name = role.split(":", 1)
        bindings.setdefault(filename, {})[name] = tuple(definition["clic_slugs"])
    return bindings


def required_roots(sectors, scenarios=(), *, source_files=None, family="ecoinvent"):
    """Union required sector roles across scenarios without reading inventories.

    Role presence in IAM production/market arrays determines applicability.
    When a caller has no IAM arrays, the full declared sector scope is used.
    Explicit support roots cover procedural transformations outside IAM maps.
    """
    from ..core.filesystem_constants import VARIABLES_DIR
    from .activity_maps import get_mapping

    if family not in {"ecoinvent", "uvek"}:
        raise ValueError(f"Unsupported CLIC role family {family!r}.")
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
                    VARIABLES_DIR / filename, f"{family}_aliases", model=model
                )
                for role in applicable_roles(sector, definitions, scenario):
                    if definitions[role].get("clic_family_override"):
                        roots.update(definitions[role]["clic_slugs"])
                        continue
                    roots.update(
                        role_slugs(filename, role)
                        if bindings is None
                        else bindings.get(filename, {}).get(role, ())
                    )
    return tuple(sorted(roots))
