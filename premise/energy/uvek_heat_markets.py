"""Plan the three IAM heat layers on reviewed native and pinned suppliers."""

from __future__ import annotations

import copy
import hashlib
import json
import math

from ..inventory.activity_maps import HEAT_TECHS, get_mapping
from ..inventory.profiles import identity
from .heat_data import load_heat_mapping
from .uvek_heat_distribution import build_native_district_delivery
from .uvek_heat_suppliers import plan_native_heat_suppliers
from .uvek_heat_supplements import plan_heat_supplements

LAYERS = {
    "secondary_supply": (
        "secondary_heat_supply",
        "market for heat, secondary, district or industrial",
        "heat, district or industrial",
    ),
    "buildings_end_use": (
        "buildings_heat_end_use",
        "market for heat, for buildings",
        "heat, central or small-scale",
    ),
    "industrial_end_use": (
        "industrial_heat_end_use",
        "market for heat, district or industrial",
        "heat, district or industrial",
    ),
}


def _selected_values(array, year, layer, metadata):
    if array is None:
        return {}, None
    if not {"region", "variables"}.issubset(array.dims):
        raise ValueError(f"UVEK heat {layer} needs region and variable dimensions.")
    if len(set(map(str, array.region.values))) != len(array.region):
        raise ValueError("Duplicate IAM heat region.")
    if len(set(map(str, array.variables.values))) != len(array.variables):
        raise ValueError("Duplicate IAM heat technology.")
    selected_year = year
    if "year" in array.dims:
        years = array.year.values
        selected_year = min(max(year, min(years)), max(years))
        array = (
            array.sel(year=selected_year)
            if selected_year in years
            else array.interp(year=selected_year)
        )
    if set(array.dims) != {"region", "variables"}:
        raise ValueError("UVEK heat has unsupported remaining IAM dimensions.")
    units = array.attrs.get("unit", {})
    values = {}
    world_roles = set()
    for region in array.region.values:
        by_role = {}
        for variable in array.variables.values:
            role = str(variable)
            value = float(array.sel(region=region, variables=variable))
            if not math.isfinite(value) or value < 0:
                raise ValueError(
                    f"Nonfinite/negative IAM heat volume: {layer}/{region}/{role}."
                )
            if value == 0:
                continue
            if role not in metadata or metadata[role]["layer"] != layer:
                raise ValueError(
                    f"Positive heat role has no matching reviewed layer: {role!r}."
                )
            unit = units.get(role) if isinstance(units, dict) else units
            if unit is not None and unit != "EJ/yr":
                raise ValueError(
                    f"IAM heat needs canonical EJ/yr volumes, found {unit!r}."
                )
            by_role[role] = value
        if str(region) == "World":
            world_roles.update(by_role)
            continue
        if by_role:
            values[str(region)] = by_role
    regional_roles = {role for by_role in values.values() for role in by_role}
    if world_roles - regional_roles:
        raise ValueError(
            "Positive World heat role has no regional volume basis: "
            f"{sorted(world_roles - regional_roles)!r}."
        )
    return values, float(selected_year)


def _market(layer, region, values, suppliers, scenario, metadata):
    factors = {role: suppliers[role]["delivered_heat_factor"] for role in values}
    delivered = {role: volume * factors[role] for role, volume in values.items()}
    total = math.fsum(delivered.values())
    if not math.isfinite(total) or total <= 0:
        raise ValueError("IAM heat market has no finite positive delivered supply.")
    shares = {role: volume / total for role, volume in delivered.items()}
    token = json.dumps([scenario, layer, region], sort_keys=True, separators=(",", ":"))
    code = "uvek-heat-market-" + hashlib.sha256(token.encode()).hexdigest()[:32]
    _, name, product = LAYERS[layer]
    result = {
        "name": name,
        "reference product": product,
        "location": region,
        "unit": "megajoule",
        "production amount": 1,
        "type": "process",
        "database": "premise-UVEK-heat",
        "code": code,
        "comment": (
            "Regional IAM delivered-heat mix on complete reviewed UVEK/CLIC suppliers. "
            "Final energy is converted to delivered heat before computing shares; "
            "supplier inventories are not rescaled. Geographic, technology, vintage, "
            "allocation, uncertainty and native biosphere limitations remain."
        ),
        "premise heat": {
            "kind": "uvek_iam_market",
            "layer": layer,
            "scenario": copy.deepcopy(scenario),
            "raw_IAM_volumes_EJ_per_year": copy.deepcopy(values),
            "delivered_volumes_EJ_per_year": delivered,
            "conversion_factors": factors,
            "IAM_shares": shares,
            "supplier_limitations": {
                role: suppliers[role]["limitations"] for role in values
            },
            "supplier_reviews": {role: suppliers[role]["review"] for role in values},
            "IAM_role_assumptions": {
                role: metadata[role]["assumption"]
                for role in values
                if metadata[role].get("assumption")
            },
            "nominal_conversion_only": True,
        },
        "exchanges": [
            {
                "name": name,
                "product": product,
                "location": region,
                "unit": "megajoule",
                "type": "production",
                "amount": 1,
                "input": ("premise-UVEK-heat", code),
            }
        ],
    }
    for role, share in shares.items():
        provider = suppliers[role]["supplier"]
        result["exchanges"].append(
            {
                "name": provider["name"],
                "product": provider["reference product"],
                "location": provider["location"],
                "unit": "megajoule",
                "type": "technosphere",
                "amount": share,
                "uncertainty type": 0,
                "loc": share,
                "input": (provider["database"], provider["code"]),
                "comment": f"IAM role: {role}; deterministic scenario share, not forecast uncertainty.",
            }
        )
    return result


def _world_market(layer, regional, scenario):
    """Use delivered regional volumes for the established global fallback."""
    if not regional:
        return None
    volumes = {
        a["location"]: math.fsum(
            a["premise heat"]["delivered_volumes_EJ_per_year"].values()
        )
        for a in regional
    }
    total = math.fsum(volumes.values())
    result = copy.deepcopy(regional[0])
    token = json.dumps(
        [scenario, layer, "World"], sort_keys=True, separators=(",", ":")
    )
    code = "uvek-heat-market-" + hashlib.sha256(token.encode()).hexdigest()[:32]
    result.update(location="World", code=code)
    result["comment"] = (
        "Global fallback weighted by actual regional delivered-heat volumes. "
        "The IAM World aggregate is not counted as another producing region. "
        "Regional supplier inventories and qualified limitations remain."
    )
    raw, delivered, assumptions, limitations, reviews = {}, {}, {}, {}, {}
    for a in regional:
        marker = a["premise heat"]
        for field, out in (
            ("raw_IAM_volumes_EJ_per_year", raw),
            ("delivered_volumes_EJ_per_year", delivered),
        ):
            for role, value in marker[field].items():
                out.setdefault(role, []).append(value)
        assumptions.update(marker["IAM_role_assumptions"])
        limitations.update(marker["supplier_limitations"])
        reviews.update(marker["supplier_reviews"])
    raw = {role: math.fsum(v) for role, v in raw.items()}
    delivered = {role: math.fsum(v) for role, v in delivered.items()}
    result["premise heat"].update(
        raw_IAM_volumes_EJ_per_year=raw,
        delivered_volumes_EJ_per_year=delivered,
        conversion_factors={role: delivered[role] / raw[role] for role in raw},
        IAM_shares={role: value / total for role, value in delivered.items()},
        IAM_role_assumptions=assumptions,
        supplier_limitations=limitations,
        supplier_reviews=reviews,
        regional_delivered_volume_shares={
            region: value / total for region, value in volumes.items()
        },
        global_basis="regional delivered volumes; source World aggregate not double counted",
    )
    result["exchanges"] = [
        {
            "name": result["name"],
            "product": result["reference product"],
            "location": "World",
            "unit": "megajoule",
            "type": "production",
            "amount": 1,
            "input": (result["database"], code),
        }
    ]
    for a in regional:
        share = volumes[a["location"]] / total
        result["exchanges"].append(
            {
                "name": a["name"],
                "product": a["reference product"],
                "location": a["location"],
                "unit": "megajoule",
                "type": "technosphere",
                "amount": share,
                "uncertainty type": 0,
                "loc": share,
                "input": (a["database"], a["code"]),
            }
        )
    return result


def plan_native_heat_markets(
    database,
    iam_data,
    *,
    model,
    pathway,
    year,
    family="uvek",
    version="2026",
    system_model="cutoff",
    biosphere_version="3.10",
):
    """Return owned markets/delivery and supplier plans without mutating inputs.

    Purchased final heat uses the regional native-delivery copy, whose actual
    heat and network quantities stay intact. No second loss or efficiency is
    applied. Missing purchased supply is refused, never normalized away.
    """
    if (family, str(version), system_model, biosphere_version) != (
        "uvek",
        "2026",
        "cutoff",
        "3.10",
    ):
        raise ValueError("Native heat markets require UVEK2026cutoff/biosphere3.10.")
    metadata = load_heat_mapping(HEAT_TECHS, model)
    aliases = get_mapping(HEAT_TECHS, "uvek_aliases", model=model)
    selected, selected_years = {}, {}
    for layer, (attribute, _, _) in LAYERS.items():
        selected[layer], selected_years[layer] = _selected_values(
            getattr(iam_data, attribute, None), year, layer, metadata
        )
    roles = {
        role
        for regions in selected.values()
        for values in regions.values()
        for role in values
    }
    purchased = {
        role
        for role in roles
        if metadata[role].get("supplier_type") == "secondary_market"
    }
    residual = {
        role for role in roles if metadata[role].get("supplier_type") == "frozen_legacy"
    }
    if residual:
        raise ValueError(
            f"No reviewed native frozen/residual heat composition for {sorted(residual)!r}."
        )
    physical = roles - purchased
    missing = physical - set(aliases)
    if missing:
        raise ValueError(f"Missing reviewed UVEK heat aliases: {sorted(missing)!r}.")
    supplementary = {role for role in physical if aliases[role].get("supplement_kind")}
    suppliers = plan_native_heat_suppliers(
        database, sorted(physical - supplementary), model=model
    )
    extra = plan_heat_supplements(
        database,
        sorted(supplementary),
        model=model,
        family=family,
        version=version,
        system_model=system_model,
        biosphere_version=biosphere_version,
    )
    suppliers.update(extra["suppliers"])
    scenario = {"model": model, "pathway": pathway, "year": year}
    markets = {layer: [] for layer in LAYERS}
    for region, values in selected["secondary_supply"].items():
        if set(values) & purchased:
            raise ValueError("Purchased heat cannot supply its own secondary mix.")
        markets["secondary_supply"].append(
            _market("secondary_supply", region, values, suppliers, scenario, metadata)
        )
    world = _world_market("secondary_supply", markets["secondary_supply"], scenario)
    if world is not None:
        markets["secondary_supply"].append(world)
    deliveries = (
        build_native_district_delivery(
            database,
            markets["secondary_supply"],
            model=model,
            pathway=pathway,
            year=year,
            version=version,
            system_model=system_model,
            biosphere_version=biosphere_version,
        )
        if markets["secondary_supply"]
        else []
    )
    by_region = {a["location"]: a for a in deliveries}
    for layer in ("buildings_end_use", "industrial_end_use"):
        for region, values in selected[layer].items():
            regional = dict(suppliers)
            for role in set(values) & purchased:
                if region not in by_region:
                    raise ValueError(
                        f"Positive purchased heat has no regional secondary/delivery supply: {region!r}/{role!r}."
                    )
                delivery = by_region[region]
                regional[role] = {
                    "supplier": delivery,
                    "delivered_heat_factor": 1.0,
                    "limitations": delivery["premise heat"]["limitations"],
                    "review": delivery["premise heat"]["review"],
                }
            markets[layer].append(
                _market(layer, region, values, regional, scenario, metadata)
            )
    for layer in ("buildings_end_use", "industrial_end_use"):
        world = _world_market(layer, markets[layer], scenario)
        if world is not None:
            markets[layer].append(world)
    planned = [
        *extra["activities"],
        *markets["secondary_supply"],
        *deliveries,
        *markets["buildings_end_use"],
        *markets["industrial_end_use"],
    ]
    existing_keys = {(a.get("database"), a.get("code")): a for a in database}
    existing_identities = {}
    for a in database:
        existing_identities.setdefault(identity(a), []).append(a)
    additions = []
    for a in planned:
        existing = existing_identities.get(identity(a), [])
        collision = existing_keys.get((a["database"], a["code"]))
        if existing:
            if len(existing) != 1 or json.dumps(
                existing[0], sort_keys=True, default=str
            ) != json.dumps(a, sort_keys=True, default=str):
                raise ValueError(
                    "Existing heat market/delivery identity differs from this scenario plan."
                )
        elif collision is not None:
            raise ValueError(
                "Heat market/delivery code collides with another activity."
            )
        else:
            additions.append(a)
    return {
        "activities": additions,
        "markets": markets,
        "deliveries": deliveries,
        "suppliers": suppliers,
        "diagnostics": {
            "scenario": scenario,
            "selected_IAM_years": selected_years,
            "positive_roles": sorted(roles),
            "IAM_heat_diagnostics": copy.deepcopy(
                getattr(iam_data, "heat_diagnostics", {})
            ),
            "World_basis": "regional delivered-volume fallback; IAM World row excluded from regional totals",
            "purchased_heat_roles": sorted(purchased),
            "market_counts": {k: len(v) for k, v in markets.items()},
            "native_delivery_copies": len(deliveries),
            "new_owned_activities": len(additions),
            "public_heat_gate": "closed; consumer relinking/certification/export pending",
        },
    }
