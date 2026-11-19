"""Reviewed inventory recipes using owned background data and published CLIC inputs.

Only recipe parameters, identities and public input bindings are packaged.
Licensed background exchanges are reconstructed in memory and remain in private
source/scenario caches. Historical complete recipes retain their original graph.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from functools import lru_cache
from pathlib import Path
from collections.abc import Mapping

import numpy as np

from .clic_archive import ClicInventoryError, activity_identity, content_hash

DERIVATION = "background-and-clic-preparation-2026"
CONFIG_PATH = Path(__file__).resolve().parents[1] / "data/clic/preparation_recipes.json"
FILES = frozenset(
    {
        "lci-PGM.xlsx",
        "lci-rhenium.xlsx",
        "lci-afforestation.xlsx",
        "lci-combined-heat-power-plant-CCS.xlsx",
        "lci-electric-boiler-industrial.xlsx",
        "lci-electric-car-mechanical-powertrain.xlsx",
        "lci-fuel_cell.xlsx",
        "lci-germanium.xlsx",
        "lci-hydrogen-oil.xlsx",
        "lci-nuclear-heat.xlsx",
        "lci-two_wheelers.xlsx",
    }
)
FIELDS = ("name", "reference product", "location", "unit")


@lru_cache(maxsize=1)
def configuration():
    """Load public recipe parameters without reading inventory workbooks."""
    value = json.loads(CONFIG_PATH.read_text())
    if value.get("schema_version") != 1:
        raise ClicInventoryError("Unsupported inventory preparation recipe schema.")
    return value


def use_preparation_recipes(recipe, *, family="ecoinvent", system_model="cutoff"):
    """Select the reviewed cut-off path while preserving complete old lockfiles."""
    return (
        family == "ecoinvent"
        and system_model == "cutoff"
        and not (
            recipe.resolution_fingerprint is not None
            and DERIVATION not in (recipe.derived_inventories or {})
        )
        and not FILES.intersection(recipe.workbooks or {})
    )


def recipe_for_node(node):
    """Identify only the reviewed records being moved out of Excel."""
    file, (name, product, location, unit) = node["file"], node["identity"]
    cfg = configuration()
    if file == "lci-PGM.xlsx" and f"{location}:{product}" in cfg["pgm_target_shares"]:
        return {
            "kind": "pgm",
            "target_share": cfg["pgm_target_shares"][f"{location}:{product}"],
        }
    if (
        file == "lci-rhenium.xlsx"
        and product == "rhenium"
        and location in cfg["rhenium_target_shares"]
    ):
        return {
            "kind": "rhenium",
            "target_share": cfg["rhenium_target_shares"][location],
        }
    if file == "lci-afforestation.xlsx":
        return {
            "kind": "afforestation",
            "species": "eucalyptus" if name.endswith("eucalyptus") else "willow",
        }
    if file == "lci-combined-heat-power-plant-CCS.xlsx":
        fuel = next(
            (
                f
                for f in ("hard coal", "wood", "natural gas", "oil")
                if f"{f}-fired" in name
            ),
            None,
        )
        if fuel:
            return {"kind": "chp_ccs", "fuel": fuel}
    if (
        file == "lci-electric-boiler-industrial.xlsx"
        and name == "heat production, electric boiler, industrial"
    ):
        return {"kind": "boiler"}
    if file == "lci-electric-car-mechanical-powertrain.xlsx":
        if name in {
            "mechanical powertrain production, electric passenger car",
            "market for mechanical powertrain, electric passenger car",
        }:
            return {"kind": "alias", "candidate": "electric_mechanical"}
        if (
            name
            == "internal combustion engine equipment production, without working fluids"
        ):
            return {"kind": "alias", "candidate": "combustion_equipment"}
        if name in {
            "market for used mechanical powertrain, electric passenger car",
            "treatment of used mechanical powertrain, electric passenger car",
            "treatment of used combustion equipment",
        }:
            return {"kind": "standalone_treatment"}
        return None
    if (
        file == "lci-fuel_cell.xlsx"
        and name == "fuel cell system assembly production, rail transport, 860 kwe"
    ):
        return {"kind": "rail_fuel_cell", "candidate": "rail_fuel_cell"}
    if file == "lci-germanium.xlsx" and product == "germanium, concentrate":
        return {"kind": "alias", "candidate": "germanium"}
    if file == "lci-hydrogen-oil.xlsx":
        return {"kind": "refinery_ccs"}
    if file == "lci-nuclear-heat.xlsx":
        return {"kind": "nuclear_heat"}
    if file == "lci-two_wheelers.xlsx":
        if name == "mechanical powertrain production, motorcycle, electric":
            return {"kind": "motorcycle_gearbox"}
        if name == "fuel tank forming, motorcycle":
            return {"kind": "tank_forming"}
    return None


def reviewed_slug(graph, name, product, location="RER", unit="kilogram"):
    """Resolve a declared CLIC input through exact reviewed ledger identities."""
    wanted = (name.casefold(), product.casefold(), location, unit)
    matches = {
        node["clic_slug"]
        for node in graph["nodes"]
        if node.get("clic_slug")
        and (
            node["identity"][0].casefold(),
            node["identity"][1].casefold(),
            *node["identity"][2:],
        )
        == wanted
    }
    if len(matches) != 1:
        raise ClicInventoryError(
            f"Expected one reviewed CLIC preparation input for {wanted!r}; found {len(matches)}."
        )
    return next(iter(matches))


def capture_name(fuel):
    """Preserve the declared capture-chain proxies, including the oil proxy."""
    return {
        "hard coal": "carbon dioxide, captured from hard coal-fired power plant, oxy, pipeline 200km, storage 1000m",
        "wood": "carbon dioxide, captured at wood burning power plant 20 MW post, pipeline 200km, storage 1000m",
        "natural gas": "carbon dioxide, captured from natural gas, post, 200km pipeline, storage 1000m",
        "oil": "carbon dioxide, captured from hard coal-fired power plant, post, pipeline 200km, storage 1000m",
        "refinery": "carbon dioxide, captured at hydrogen production plant, pre, pipeline 200km, storage 1000m",
    }[fuel]


def required_recipe_roots(rule, graph):
    """Declare all public prerequisites before CLIC resolution and freshness checks."""
    cfg = configuration()
    if "candidate" in rule:
        return (cfg["clic_candidates"][rule["candidate"]],)
    if rule["kind"] in {"chp_ccs", "refinery_ccs"}:
        name = capture_name(rule.get("fuel", "refinery"))
        return (reviewed_slug(graph, name, name),)
    if rule["kind"] == "nuclear_heat":
        return (
            reviewed_slug(
                graph,
                "electricity production, nuclear, Evolutionary Power Reactor (EPR)",
                "electricity, high voltage",
                "CH",
                "kilowatt hour",
            ),
        )
    return ()


def _native_scalars(value):
    """Make private source coefficients portable without changing their values."""
    if isinstance(value, Mapping):
        return {key: _native_scalars(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_native_scalars(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def _background_hash(activity):
    """Fingerprint loaded records consistently across NumPy/Python scalars."""
    return content_hash(_native_scalars(activity))


def _background(database, names, products, location, unit):
    names = (names,) if isinstance(names, str) else tuple(names)
    products = (products,) if isinstance(products, str) else tuple(products)
    matches = [
        a
        for a in database
        if not a.get("clic provenance")
        and a.get("name") in names
        and a.get("reference product") in products
        and a.get("location") == location
        and a.get("unit") == unit
    ]
    if len(matches) != 1:
        raise ClicInventoryError(
            f"Inventory preparation requires one loaded background supplier: names={names!r}, products={products!r}, location={location!r}, unit={unit!r}; found {len(matches)}."
        )
    return matches[0]


def _purchase(supplier, amount):
    edge = {k: supplier[k] for k in FIELDS}
    edge.update(
        type="technosphere", amount=float(amount), product=supplier["reference product"]
    )
    if supplier.get("database") and supplier.get("code"):
        edge["input"] = (supplier["database"], supplier["code"])
    return edge


def _reference(activity):
    references = [e for e in activity["exchanges"] if e["type"] == "production"]
    if (
        len(references) != 1
        or not math.isfinite(float(references[0]["amount"]))
        or references[0]["amount"] <= 0
    ):
        raise ClicInventoryError(
            f"Invalid production reference for preparation donor {activity_identity(activity)!r}."
        )
    return references[0]


def _allocation(activity):
    reference = _reference(activity)
    value = reference.get("source allocation factor")
    if value is None:
        value = (
            (reference.get("properties") or {}).get("allocation factor") or {}
        ).get("amount")
    if value is None or not 0 < float(value) <= 1:
        raise ClicInventoryError(
            f"Loaded donor {activity_identity(activity)!r} lacks its production allocation factor. Re-extract the source database with the current Premise cache schema; no historical factor is substituted."
        )
    return float(value)


def _scaled(edge, factor):
    from ..core.utils import rescale_exchange

    owned = copy.deepcopy(edge)
    # Lognormal distributions retain log(abs(amount)); their sign is separate.
    rescale_exchange(owned, abs(factor), remove_uncertainty=False)
    if factor < 0:
        owned["amount"] = -owned["amount"]
        if owned.get("uncertainty type") == 2:
            owned["negative"] = not edge.get("negative", edge["amount"] < 0)
        elif "loc" in owned:
            owned["loc"] = -owned["loc"]
        lower, upper = owned.get("minimum"), owned.get("maximum")
        if lower is not None and upper is not None:
            owned["minimum"], owned["maximum"] = -upper, -lower
        elif lower is not None:
            owned.pop("minimum", None)
            owned["maximum"] = -lower
        elif upper is not None:
            owned.pop("maximum", None)
            owned["minimum"] = -upper
    if owned.get("uncertainty type", 0) == 0:
        owned["loc"] = owned["amount"]
    owned.pop("output", None)
    return owned


def _co2_edges(activity, name):
    return [
        e
        for e in activity["exchanges"]
        if e["type"] == "biosphere"
        and e.get("name") == name
        and e.get("unit") == "kilogram"
        and e.get("amount", 0) > 0
        and (
            not e.get("categories")
            or (
                e["categories"].split("::")
                if isinstance(e["categories"], str)
                else e["categories"]
            )[0]
            == "air"
        )
    ]


def prepare_recipes(
    records,
    database,
    graph,
    selected_nodes,
    *,
    target,
    pins=None,
    keep_uncertainty=True,
):
    """Reconstruct selected records without reading their retired Excel sources.

    :param records: Fully harmonized and linked CLIC foregrounds.
    :param database: User's loaded, unmodified background inventory.
    :param graph: Reviewed identity and dependency ledger.
    :param selected_nodes: Required preparation recipe node IDs.
    :param target: Destination background context dictionary.
    :param pins: Optional fingerprint mapping for this derivation.
    :return: Owned foregrounds and a portable preparation manifest row.
    """
    indices = sorted(selected_nodes)
    cfg = configuration()
    records = copy.deepcopy(records)
    by_slug = {
        a.get("clic provenance", {}).get("slug"): a
        for a in records
        if a.get("clic provenance", {}).get("slug")
    }
    generated, used_background, used_clic, decisions = [], {}, {}, []

    def donor(names, products, location, unit):
        activity = _background(database, names, products, location, unit)
        used_background[activity_identity(activity)] = activity
        return activity

    def public(slug, unit):
        activity = by_slug.get(slug)
        if (
            activity is None
            or activity["unit"] != unit
            or _reference(activity)["amount"] != 1
        ):
            raise ClicInventoryError(
                f"Missing or changed CLIC preparation input {slug!r} ({unit})."
            )
        used_clic[slug] = activity["clic provenance"]
        return activity

    for index in indices:
        node = graph["nodes"][index]
        rule = recipe_for_node(node)
        if rule is None:
            raise ClicInventoryError(
                f"No reviewed preparation recipe for {node['identity']!r}."
            )
        kind = rule["kind"]
        name, product, location, unit = node["identity"]
        roles = sorted(
            f"{filename}:{role}"
            for filename, definitions in graph["roles"].items()
            for role, members in definitions.items()
            if index in members
        )
        if kind == "standalone_treatment":
            raise ClicInventoryError(
                f"Standalone legacy treatment {node['identity']!r} cannot be represented by a combined CLIC lifecycle. Select a published treatment inventory or replay the historical recipe; no empty treatment is created."
            )
        if kind == "alias":
            activity = public(cfg["clic_candidates"][rule["candidate"]], unit)
            provenance = activity["clic provenance"]
            provenance.setdefault("legacy_identities", []).append(
                list(node["identity"])
            )
            provenance.setdefault("prepared_aliases", []).append(list(node["identity"]))
            provenance["derived_role_bindings"] = sorted(
                set(provenance.get("derived_role_bindings", ())) | set(roles)
            )
            decisions.append(
                {
                    "identity": node["identity"],
                    "kind": kind,
                    "slug": provenance["slug"],
                    "boundary": "Published CLIC lifecycle/refined-product boundary replaces the legacy identity; downstream units remain explicit.",
                }
            )
            continue
        activity = dict(zip(FIELDS, node["identity"]))
        code = content_hash([DERIVATION, node["identity"]])
        activity.update(
            database="premise-derived-inventories",
            code=code,
            type="process",
            exchanges=[],
        )
        activity["exchanges"].append(
            {
                **dict(zip(FIELDS, node["identity"])),
                "type": "production",
                "amount": 1.0,
                "input": (activity["database"], code),
            }
        )
        edges, detail = [], {"identity": node["identity"], "kind": kind}
        if kind in {"pgm", "rhenium"}:
            if kind == "pgm":
                base = donor(
                    (
                        "platinum group metal, extraction and refinery operations",
                        "platinum group metal mine operation, ore with high palladium content",
                    ),
                    "platinum",
                    location,
                    "kilogram",
                )
            else:
                base = donor(
                    "copper mine operation and beneficiation, sulfide ore",
                    ("copper concentrate, sulfide ore", "copper concentrate"),
                    location,
                    "kilogram",
                )
            allocation = _allocation(base)
            factor = (
                rule["target_share"] / allocation / float(_reference(base)["amount"])
            )
            edges = [
                _scaled(e, factor)
                for e in base["exchanges"]
                if e["type"] != "production"
            ]
            detail.update(
                source_allocation=allocation,
                target_allocation=rule["target_share"],
                factor=factor,
                limitations=[
                    "Retains the historical additional-product allocation model; no missing co-product production yield is inferred and existing background coproduct allocations are not rewritten."
                ],
            )
        elif kind == "afforestation":
            if rule["species"] == "eucalyptus":
                base = donor(
                    "hardwood forestry, eucalyptus ssp., planted forest management",
                    (
                        "cleft timber, green, measured as dry mass",
                        "cleft timber, measured as dry mass",
                    ),
                    "RoW",
                    "kilogram",
                )
            else:
                base = donor(
                    "willow production, short rotation coppice",
                    (
                        "wood chips and particles, willow, dry, measured as dry mass",
                        "wood chips and particles, willow",
                    ),
                    "RoW",
                    "kilogram",
                )
            uptake = sum(
                float(e["amount"])
                for e in base["exchanges"]
                if e["type"] == "biosphere"
                and e.get("name") == "Carbon dioxide, in air"
                and e.get("unit") == "kilogram"
            )
            release = sum(
                float(e["amount"])
                for e in _co2_edges(base, "Carbon dioxide, non-fossil")
            )
            net = (uptake - release) / float(_reference(base)["amount"])
            if net <= 0:
                raise ClicInventoryError(
                    f"No positive net CO2 uptake in loaded afforestation donor {activity_identity(base)!r}."
                )
            edges = [_purchase(base, 1 / net)]
            detail.update(
                net_uptake_per_kg=net,
                limitations=[
                    "Retains the existing biomass-storage boundary; no additional permanence, land-use-change or harvest/end-of-life model is invented."
                ],
            )
        elif kind == "boiler":
            base = donor(
                "market for electricity, medium voltage",
                "electricity, medium voltage",
                "CH",
                "kilowatt hour",
            )
            edges = [_purchase(base, 1 / (cfg["electric_boiler_efficiency"] * 3.6))]
            detail.update(
                efficiency=cfg["electric_boiler_efficiency"],
                source=cfg["sources"]["electric_boiler"],
                limitations=[
                    "Operation-only engineering proxy; boiler infrastructure is omitted as in the existing inventory."
                ],
            )
        elif kind in {"chp_ccs", "refinery_ccs"}:
            fuel = rule.get("fuel", "refinery")
            if fuel == "refinery":
                base = donor(
                    "hydrogen production, gaseous, petroleum refinery operation",
                    ("hydrogen, gaseous, low pressure", "hydrogen, gaseous"),
                    "Europe without Switzerland",
                    "kilogram",
                )
                multiplier = 1.0
            else:
                base_name = {
                    "hard coal": "heat and power co-generation, hard coal",
                    "wood": "heat and power co-generation, wood chips, 6667 kW",
                    "natural gas": "heat and power co-generation, natural gas, conventional power plant, 100MW electrical",
                    "oil": "heat and power co-generation, oil",
                }[fuel]
                base = donor(base_name, product, "CH" if fuel == "wood" else "DE", unit)
                multiplier = cfg["chp_input_multipliers"][fuel]
            carbon = (
                "Carbon dioxide, non-fossil"
                if fuel == "wood"
                else "Carbon dioxide, fossil"
            )
            emissions = _co2_edges(base, carbon)
            captured = (
                sum(float(e["amount"]) for e in emissions)
                / float(_reference(base)["amount"])
                * multiplier
                * cfg["capture_fraction"]
            )
            if captured <= 0:
                raise ClicInventoryError(
                    f"No positive direct {carbon} emission in loaded capture donor {activity_identity(base)!r}."
                )
            capture = capture_name(fuel)
            supplied = public(reviewed_slug(graph, capture, capture), "kilogram")
            edges = [_purchase(base, multiplier), _purchase(supplied, captured)]
            edges.extend(
                _scaled(
                    e,
                    -multiplier
                    * cfg["capture_fraction"]
                    / float(_reference(base)["amount"]),
                )
                for e in emissions
            )
            detail.update(
                capture_fraction=cfg["capture_fraction"],
                input_multiplier=multiplier,
                captured_kg=captured,
                limitations=[
                    "Capture follows direct donor CO2 only; existing CHP allocation and the declared capture-chain technology/geographic proxies are retained."
                ],
            )
            if kind == "refinery_ccs":
                detail["limitations"].append(
                    "Retains the historical 30-bar output label with a low-pressure refinery input; an independent compression inventory has not been established."
                )
        elif kind == "nuclear_heat":
            roots = required_recipe_roots(rule, graph)
            base = public(roots[0], "kilowatt hour")
            p = cfg["nuclear_heat"]
            heat = 3.6 / p["electric_efficiency"] * p["total_utilization"] - 3.6
            carnot = 1 - (p["ambient_temperature_c"] + 273.15) / (
                p["heat_temperature_c"] + 273.15
            )
            share = heat * carnot / (3.6 + heat * carnot)
            edges = [_purchase(base, share / heat)]
            detail.update(exergy_parameters=p, heat_allocation_share=share)
        elif kind == "rail_fuel_cell":
            base = public(cfg["clic_candidates"]["rail_fuel_cell"], "unit")
            p = cfg["rail_fuel_cell"]
            amount = (
                p["capacity_kwe"]
                * p["owner_reference_hours"]
                / p["clic_reference_hours"]
            )
            edges = [_purchase(base, amount)]
            detail.update(
                reference=p,
                clic_assemblies=amount,
                limitations=[
                    "Adopts the published generic CLIC stack/BoP replacement model. The owner's 30,000-hour reference and lifetime set count are retained; the CLIC 20,000-hour service basis is converted once."
                ],
            )
        elif kind == "motorcycle_gearbox":
            p = cfg["motorcycle_gearbox"]
            total = p["retained_steel_kg"] + p["retained_aluminium_kg"]
            for material, retained, scrap, working in (
                (
                    "steel, low-alloyed",
                    p["retained_steel_kg"],
                    "iron scrap, unsorted",
                    "steel",
                ),
                (
                    "aluminium, cast alloy",
                    p["retained_aluminium_kg"],
                    "aluminium scrap, new",
                    "aluminium",
                ),
            ):
                base = donor(f"market for {material}", material, "GLO", "kilogram")
                work = donor(
                    f"metal working, average for {working} product manufacturing",
                    f"metal working, average for {working} product manufacturing",
                    "RER",
                    "kilogram",
                )
                outlet = donor(
                    f"{scrap}, Recycled Content cut-off", scrap, "GLO", "kilogram"
                )
                gross = retained * p["metal_gross_factor"] / total
                edges.extend(
                    [
                        _purchase(base, gross),
                        _purchase(work, gross),
                        _purchase(outlet, -(gross - retained / total)),
                    ]
                )
            base = donor(
                "market group for electricity, medium voltage",
                "electricity, medium voltage",
                "GLO",
                "kilowatt hour",
            )
            edges.append(_purchase(base, p["assembly_kwh_per_kg"]))
            detail.update(
                parameters=p,
                source=cfg["sources"]["motorcycle"],
                limitations=[
                    "Gearbox-only historical engineering proxy; excludes motor, controller, charger, cooling and a second vehicle end-of-life inventory."
                ],
            )
        elif kind == "tank_forming":
            base = donor("injection moulding", "injection moulding", "RER", "kilogram")
            edges = [_purchase(base, 1)]
        else:
            raise ClicInventoryError(f"Unimplemented preparation kind {kind!r}.")
        activity["exchanges"].extend(edges)
        detail = _native_scalars(detail)
        activity["comment"] = json.dumps(detail, sort_keys=True)
        activity["clic provenance"] = {
            "role": "derived inventory",
            "activity_code": code,
            "derivation": DERIVATION,
            "legacy_identities": [node["identity"]],
            "derived_role_bindings": roles,
        }
        generated.append(activity)
        decisions.append(detail)
    fingerprint = content_hash(
        {
            "algorithm": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "parameters": hashlib.sha256(CONFIG_PATH.read_bytes()).hexdigest(),
            "target": target,
            "keep_uncertainty": keep_uncertainty,
            "outputs": [graph["nodes"][i]["identity"] for i in indices],
            "background": [
                (identity, _background_hash(activity))
                for identity, activity in sorted(used_background.items())
            ],
            "clic": [
                (slug, row["version_id"], row["sha256"])
                for slug, row in sorted(used_clic.items())
            ],
        }
    )
    if pins is not None and dict(pins) != {DERIVATION: fingerprint}:
        raise ClicInventoryError(
            "Inventory preparation differs from the locked reconstruction recipe."
        )
    for a in records:
        if a.get("clic provenance", {}).get("prepared_aliases"):
            a["clic provenance"]["preparation_derivation"] = DERIVATION
            a["clic provenance"]["preparation_fingerprint"] = fingerprint
    for a in generated:
        a["clic provenance"]["derivation_fingerprint"] = fingerprint
        if not keep_uncertainty:
            from ..core.utils import rescale_exchange

            for edge in a["exchanges"]:
                rescale_exchange(edge, 1, remove_uncertainty=True)
    summary = {
        "id": DERIVATION,
        "fingerprint": fingerprint,
        "activity_count": len(generated),
        "alias_count": len(indices) - len(generated),
        "inputs": [
            {k: row[k] for k in ("slug", "version_id", "sha256")}
            for _, row in sorted(used_clic.items())
        ],
        "background_inputs": [
            {"identity": list(identity), "sha256": _background_hash(a)}
            for identity, a in sorted(used_background.items())
        ],
        "decisions": decisions,
    }
    return [*records, *generated], summary
