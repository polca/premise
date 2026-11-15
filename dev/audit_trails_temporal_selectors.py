"""Audit active TRAILS temporal selectors against installed ecoinvent databases.

This developer check requires local Brightway projects containing licensed
ecoinvent databases; it is therefore not part of the normal test suite. It
compares active crop selectors with an independent candidate set and audits
relationship-aware construction-timing selectors against actual caller-to-
infrastructure exchanges, including premise's bus and lorry workbooks.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import bw2data
from openpyxl import load_workbook

from premise.core.filesystem_constants import DATA_DIR
from premise.temporal_legacy import compile_legacy_rows
from premise.temporal_specs import TemporalRegistry

DEFAULT_VERSIONS = ("3.8", "3.9.1", "3.10", "3.11", "3.12")
CEREALS = ("barley", "maize", "oat", "rye", "spelt", "triticale", "wheat")
CROP_ISIC_PREFIXES = ("0111:", "0119:")
CEREAL_PATTERN = re.compile(r"\b(?:" + "|".join(CEREALS) + r")\b", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--versions",
        nargs="+",
        default=DEFAULT_VERSIONS,
        help="Ecoinvent versions to audit (default: %(default)s).",
    )
    parser.add_argument(
        "--project-template",
        default="ecoinvent-{version}-cutoff",
        help="Brightway project name template.",
    )
    parser.add_argument(
        "--database-template",
        default="ecoinvent-{version}-cutoff",
        help="Brightway database name template.",
    )
    parser.add_argument(
        "--json",
        type=Path,
        help="Optionally write the complete audit report as JSON.",
    )
    parser.add_argument(
        "--additional-inventories",
        nargs="*",
        type=Path,
        default=[
            DATA_DIR / "additional_inventories" / "lci-buses.xlsx",
            DATA_DIR / "additional_inventories" / "lci-trucks.xlsx",
            DATA_DIR / "additional_inventories" / "lci-pass_cars.xlsx",
            DATA_DIR / "additional_inventories" / "lci-battery-capacity.xlsx",
        ],
        help="Bw2io-style inventory workbooks whose relationships are audited.",
    )
    return parser.parse_args()


def activity_record(activity: Any) -> dict[str, Any]:
    record = dict(activity)
    record["classifications"] = record.get("classifications") or []
    return record


def exchange_record(
    exchange: Any, suppliers: dict[tuple[str, str], dict[str, Any]]
) -> dict[str, Any]:
    """Return an exchange record enriched with its supplier identity."""

    record = dict(exchange)
    supplier = suppliers[exchange["input"]]
    product = (
        record.get("product")
        or record.get("reference product")
        or supplier.get("reference product", "")
    )
    record.update(
        name=record.get("name") or supplier.get("name", ""),
        reference_product=product,
        product=product,
        location=supplier.get("location", ""),
        unit=record.get("unit") or supplier.get("unit", ""),
    )
    record["reference product"] = product
    return record


def identity(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        record.get("name", ""),
        record.get("reference product", record.get("reference_product", "")),
        record.get("location", ""),
    )


def display_identity(record: dict[str, Any]) -> dict[str, str]:
    name, product, location = identity(record)
    return {"name": name, "reference_product": product, "location": location}


def relationship_identity(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> tuple[str, str, str, str, str, str]:
    """Return a stable identity for a caller-to-supplier relationship."""

    return (*identity(caller), *identity(supplier))


def display_relationship(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> dict[str, dict[str, str]]:
    return {
        "caller": display_identity(caller),
        "supplier": display_identity(supplier),
    }


def is_cement_operation(record: dict[str, Any]) -> bool:
    name = record.get("name", "").casefold()
    return name.startswith(("cement production", "clinker production"))


def is_cement_factory_exchange(record: dict[str, Any]) -> bool:
    return (
        record.get("name", "").casefold()
        in {
            "cement factory",
            "integrated cement plant",
        }
        and record.get("unit", "").casefold() == "unit"
    )


def is_potential_steel_operation(record: dict[str, Any]) -> bool:
    name = record.get("name", "").casefold()
    return name.startswith(
        (
            "pig iron production",
            "sponge iron production",
            "cast iron production",
            "steel production, converter",
            "steel production, electric",
        )
    )


def steel_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify reviewed iron/steel-operation to whole-furnace relationships."""

    caller_name = caller.get("name", "").casefold()
    supplier_name = supplier.get("name", "").casefold()
    if supplier.get("unit", "").casefold() != "unit":
        return None
    if (
        caller_name.startswith("sponge iron production")
        and supplier_name == "blast furnace"
    ):
        return "dri"
    if (
        caller_name.startswith("pig iron production")
        and supplier_name == "blast furnace"
    ):
        return "blast_furnace"
    if caller_name.startswith("steel production, converter") and supplier_name == (
        "blast oxygen furnace converter"
    ):
        return "bof"
    if caller_name.startswith(
        ("cast iron production", "steel production, electric")
    ) and supplier_name in {
        "electric arc furnace",
        "electric arc furnace converter",
    }:
        return "eaf"
    return None


def is_potential_pipeline_transport(record: dict[str, Any]) -> bool:
    """Return whether a caller can contain a reviewed pipeline relationship."""

    name = record.get("name", "").casefold()
    return name.startswith("transport, pipeline") and (
        "natural gas" in name or "petroleum" in name
    )


def pipeline_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify reviewed transport-to-whole-transmission-pipeline links."""

    if not is_potential_pipeline_transport(caller):
        return None
    caller_name = caller.get("name", "").casefold()
    supplier_name = supplier.get("name", "").casefold()
    if supplier.get("unit", "").casefold() != "kilometer":
        return None
    if "natural gas" in caller_name and supplier_name in {
        "pipeline, natural gas, long distance, high capacity, offshore",
        "pipeline, natural gas, long distance, high capacity, onshore",
        "pipeline, natural gas, long distance, low capacity, onshore",
    }:
        return "natural_gas"
    if "petroleum" in caller_name and supplier_name in {
        "pipeline, petroleum",
        "pipeline, petroleum, offshore",
    }:
        return "oil_ngl"
    return None


def is_potential_freight_train(record: dict[str, Any]) -> bool:
    """Return whether a caller can contain a reviewed goods-wagon link."""

    name = record.get("name", "").casefold()
    product = record.get(
        "reference product", record.get("reference_product", "")
    ).casefold()
    return name.startswith("transport, freight train") and product == (
        "transport, freight train"
    )


def freight_wagon_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify reviewed freight-service to whole-goods-wagon links."""

    if not is_potential_freight_train(caller):
        return None
    if (
        supplier.get("name", "").casefold() == "goods wagon"
        and supplier.get("unit", "").casefold() == "unit"
    ):
        return "goods_wagon"
    return None


def is_potential_aircraft_transport(record: dict[str, Any]) -> bool:
    """Return whether a caller can contain a reviewed whole-aircraft link."""

    return (
        record.get("name", "")
        .casefold()
        .startswith(
            (
                "transport, passenger aircraft,",
                "transport, passenger, aircraft,",
                "transport, freight, aircraft, belly-freight,",
            )
        )
    )


def aircraft_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify passenger-service to whole passenger-aircraft links."""

    if not is_potential_aircraft_transport(caller):
        return None
    if supplier.get("unit", "").casefold() != "unit":
        return None
    supplier_name = supplier.get("name", "").casefold()
    if supplier_name.startswith("aircraft, passenger,"):
        return "passenger"
    if supplier_name.startswith("aircraft, belly freight,"):
        return "belly_freight"
    return None


def is_potential_inland_freight(record: dict[str, Any]) -> bool:
    """Return whether a caller can contain a reviewed whole-barge link."""

    return (
        record.get("name", "")
        .casefold()
        .startswith("transport, freight, inland waterways, barge")
    )


def inland_vessel_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify inland-freight service to whole dry-cargo or tanker barges."""

    if not is_potential_inland_freight(caller):
        return None
    if supplier.get("unit", "").casefold() != "unit":
        return None
    supplier_name = supplier.get("name", "").casefold()
    if supplier_name == "barge":
        return "dry_cargo"
    if supplier_name == "barge tanker":
        return "tanker"
    return None


def is_potential_csp_generation(record: dict[str, Any]) -> bool:
    """Return whether a caller can contain a reviewed whole-CSP-plant link."""

    return (
        record.get("name", "")
        .casefold()
        .startswith("electricity production, solar thermal parabolic trough,")
    )


def is_potential_new_marine_service(caller):
    name = caller.get("name", "").casefold()
    return (
        name.startswith("electricity production, wind,") and "offshore" in name
    ) or name.startswith("transport, freight, sea, tanker for liquefied natural gas,")


def new_marine_relationship_category(caller, supplier):
    name = caller.get("name", "").casefold()
    product = supplier.get("name", "").casefold()
    if supplier.get("unit", "").casefold() != "unit":
        return None
    if (
        name.startswith("electricity production, wind,")
        and "offshore" in name
        and product == "wind power plant, 2mw, offshore, fixed parts"
    ):
        return "offshore_fixed_structure"
    if (
        name.startswith("transport, freight, sea, tanker for liquefied natural gas,")
        and product == "tanker, for liquefied natural gas"
    ):
        return "lng_carrier"
    return None


def is_potential_bioenergy_service(caller):
    name = caller.get("name", "").casefold()
    return name.startswith("heat and power co-generation, wood chips") or (
        name.startswith("treatment of") and "municipal incineration" in name
    )


def bioenergy_relationship_category(caller, supplier):
    """Independent candidate set for the new dedicated biomass/refuse mappings."""
    name = caller.get("name", "").casefold()
    supplier_name = supplier.get("name", "").casefold()
    if supplier.get("unit", "").casefold() != "unit" or "future" in name:
        return None
    if name.startswith("heat and power co-generation, wood chips"):
        if "state-of-the-art" in name or caller.get("location", "").upper().startswith(
            "US-"
        ):
            return None  # Existing US EIA mapping has precedence.
        if supplier_name.startswith(
            (
                "furnace, wood chips, with silo,",
                "heat and power co-generation unit, organic rankine cycle,",
            )
        ):
            return "wood_biomass"
    if (
        name.startswith("treatment of")
        and "municipal incineration" in name
        and "without energy recovery" not in name
        and supplier_name == "municipal waste incineration facility"
    ):
        return "waste_incineration"
    return None


def is_potential_passenger_rail(caller):
    name = caller.get("name", "").casefold()
    return name.startswith(
        ("transport, passenger train", "transport, passenger, train,")
    )


def passenger_rail_relationship_category(caller, supplier):
    """Identify conventional mainline transport to whole train relationships."""
    name = caller.get("name", "").casefold()
    if not is_potential_passenger_rail(caller) or any(
        token in name for token in ("high-speed", "urban")
    ):
        return None
    if supplier.get("unit", "").casefold() == "unit" and supplier.get(
        "name", ""
    ).casefold() in {"train, passenger, regional", "train, passenger, long-distance"}:
        return "conventional_passenger_train"
    return None


def platform_relationship_category(caller, supplier):
    """Classify offshore extraction links, excluding onshore and construction."""
    if caller.get("name", "").casefold() not in {
        "petroleum and gas production, off-shore",
        "petroleum and gas production, offshore",
    }:
        return None
    if supplier.get("unit", "").casefold() == "unit" and supplier.get(
        "name", ""
    ).casefold() in {"offshore platform, petroleum", "offshore platform, natural gas"}:
        return "production_platform"
    return None


def csp_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify CSP electricity service to complete plant relationships."""

    if not is_potential_csp_generation(caller):
        return None
    if supplier.get("unit", "").casefold() != "unit":
        return None
    if supplier.get("name", "").casefold() == (
        "concentrated solar power plant, solar thermal parabolic trough, 50 mw"
    ):
        return "csp_plant"
    return None


def is_potential_coal_mine_operation(record: dict[str, Any]) -> bool:
    """Return whether a caller can contain reviewed coal-mine infrastructure."""

    return (
        record.get("name", "")
        .casefold()
        .startswith(("hard coal mine operation", "lignite mine operation"))
    )


def coal_mine_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify coal operations' whole-mine-infrastructure links."""

    if not is_potential_coal_mine_operation(caller):
        return None
    if supplier.get("unit", "").casefold() != "unit":
        return None
    supplier_name = supplier.get("name", "").casefold()
    if supplier_name in {
        "mine infrastructure, open cast, hard coal",
        "mine infrastructure, open cast, lignite",
    }:
        return "surface"
    if supplier_name == "mine infrastructure, underground, hard coal":
        return "underground"
    return None


def is_potential_heating_operation(record: dict[str, Any]) -> bool:
    """Return whether a caller can contain a reviewed heating-appliance link."""

    name = record.get("name", "").casefold()
    product = record.get(
        "reference product", record.get("reference_product", "")
    ).casefold()
    if name.startswith(
        (
            "heat production, air-water heat pump",
            "heat production, borehole heat exchanger, brine-water heat pump",
        )
    ):
        return True
    return name.startswith("heat production,") and (
        "heat, central or small-scale" in product
    )


def heating_relationship_category(
    caller: dict[str, Any], supplier: dict[str, Any]
) -> str | None:
    """Classify reviewed heat-service to complete heating-appliance links."""

    if not is_potential_heating_operation(caller):
        return None
    caller_name = caller.get("name", "").casefold()
    caller_product = caller.get(
        "reference product", caller.get("reference_product", "")
    ).casefold()
    supplier_name = supplier.get("name", "").casefold()
    if supplier.get("unit", "").casefold() != "unit":
        return None
    if "future" in caller_name or "state-of-the-art" in caller_name:
        return None
    if caller_name.startswith("heat production, air-water heat pump"):
        return (
            "aerial_heat_pump"
            if supplier_name == "heat pump, brine-water, 10kw"
            else None
        )
    if caller_name.startswith(
        "heat production, borehole heat exchanger, brine-water heat pump"
    ):
        return (
            "ground_heat_pump"
            if supplier_name == "heat pump, brine-water, 10kw"
            else None
        )
    if "heat, central or small-scale" not in caller_product:
        return None
    if (
        caller_name.startswith("heat production, natural gas, at boiler")
        and "condensing" not in caller_name
        and supplier_name == "gas boiler"
    ):
        return "natural_gas_boiler"
    if (
        caller_name.startswith("heat production, light fuel oil, at boiler")
        and "condensing" not in caller_name
        and supplier_name in {"oil boiler, 10kw", "oil boiler, 100kw"}
    ):
        return "fuel_oil_boiler"
    if supplier_name in {
        "furnace, logs, 6kw",
        "furnace, pellets, 9kw",
    }:
        return "biomass_stove"
    if supplier_name in {
        "furnace, logs, 30kw",
        "furnace, logs, average storage area, 100kw",
        "furnace, wood chips, with silo, 50kw",
        "furnace, pellets, 25kw",
        "furnace, pellets, with silo, 300kw",
    }:
        return "biomass_furnace"
    return None


def direct_biogenic_uptake(activity: Any) -> list[float]:
    """Return positive ``Carbon dioxide, in air`` natural-resource inputs."""
    return [
        float(exchange.get("amount") or 0)
        for exchange in activity.biosphere()
        if exchange.get("name") == "Carbon dioxide, in air"
        and float(exchange.get("amount") or 0) > 0
    ]


def is_cereal_crop_production(record: dict[str, Any]) -> bool:
    name = record.get("name", "")
    if "production" not in name.casefold() or not CEREAL_PATTERN.search(name):
        return False
    return any(
        system == "ISIC rev.4 ecoinvent" and value.startswith(CROP_ISIC_PREFIXES)
        for system, value in record.get("classifications", [])
    )


def legacy_biomass_identities() -> set[tuple[str, str]]:
    rows = compile_legacy_rows(DATA_DIR / "trails" / "temporal")
    return {
        (row["name"], row["reference product"])
        for row in rows
        if row["temporal_tag"] == "biomass_growth"
    }


def workbook_relationships(
    paths: list[Path],
) -> list[tuple[str, dict[str, Any], list[dict[str, Any]]]]:
    """Read caller-to-technosphere relationships from bw2io Excel workbooks."""

    relationships = []
    for path in paths:
        sheet = load_workbook(path, read_only=True, data_only=True).active
        rows = list(sheet.iter_rows(values_only=True))
        starts = [index for index, row in enumerate(rows) if row[0] == "Activity"]
        for position, start in enumerate(starts):
            end = starts[position + 1] if position + 1 < len(starts) else len(rows)
            block = rows[start:end]
            caller = {"name": block[0][1]}
            for row in block:
                if row[0] in {"reference product", "location", "unit"}:
                    caller[row[0]] = row[1]
            header_index = next(
                (
                    index
                    for index, row in enumerate(block)
                    if row[0] == "name" and "type" in row
                ),
                None,
            )
            if header_index is None:
                continue
            columns = {
                value: index
                for index, value in enumerate(block[header_index])
                if value is not None
            }
            exchanges = []
            for row in block[header_index + 1 :]:
                if not row[0] or row[columns["type"]] != "technosphere":
                    continue
                exchanges.append(
                    {
                        "name": row[columns["name"]],
                        "reference product": row[columns["reference product"]],
                        "location": row[columns["location"]],
                        "unit": row[columns["unit"]],
                    }
                )
            relationships.append((path.name, caller, exchanges))
    return relationships


def is_independent_candidate(
    record: dict[str, Any],
    uptake: list[float],
    legacy_biomass: set[tuple[str, str]],
) -> bool:
    if not uptake:
        return False
    name, product, _ = identity(record)
    return (
        (name, product) in legacy_biomass and bool(CEREAL_PATTERN.search(name))
    ) or is_cereal_crop_production(record)


def audit_version(
    version: str,
    project: str,
    database_name: str,
    registry: TemporalRegistry,
    legacy_biomass: set[tuple[str, str]],
    additional_relationships: list[tuple[str, dict[str, Any], list[dict[str, Any]]]],
) -> dict[str, Any]:
    bw2data.projects.set_current(project)
    database = bw2data.Database(database_name)
    activities = list(database)
    records_by_key = {
        activity.key: activity_record(activity) for activity in activities
    }
    general_supplier_assignments = [
        a
        for a in registry.assignments
        if a["target"] == "supplier" and not a.get("caller_match")
    ]
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    candidates: dict[tuple[str, str, str], dict[str, str]] = {}
    counts: Counter[str] = Counter()
    relationship_counts: Counter[str] = Counter()
    cement_candidates: dict[
        tuple[str, str, str, str, str, str], dict[str, dict[str, str]]
    ] = {}
    cement_selected: dict[
        tuple[str, str, str, str, str, str], dict[str, dict[str, str]]
    ] = {}
    steel_candidates: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    steel_selected: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    pipeline_candidates: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    pipeline_selected: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    freight_wagon_candidates: dict[
        tuple[str, str, str, str, str, str], dict[str, Any]
    ] = {}
    freight_wagon_selected: dict[
        tuple[str, str, str, str, str, str], dict[str, Any]
    ] = {}
    aircraft_candidates: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    aircraft_selected: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    inland_vessel_candidates: dict[
        tuple[str, str, str, str, str, str], dict[str, Any]
    ] = {}
    inland_vessel_selected: dict[
        tuple[str, str, str, str, str, str], dict[str, Any]
    ] = {}
    csp_candidates: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    csp_selected: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    platform_candidates = {}
    platform_selected = {}
    passenger_rail_candidates = {}
    passenger_rail_selected = {}
    bioenergy_candidates = {}
    bioenergy_selected = {}
    new_marine_candidates = {}
    new_marine_selected = {}
    coal_mine_candidates: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    coal_mine_selected: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    heating_candidates: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    heating_selected: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    additional_inventory_misses = []

    for activity in activities:
        record = records_by_key[activity.key]
        uptake = direct_biogenic_uptake(activity)
        resolution = registry.resolve(
            record,
            target="activity",
            context={"ecoinvent_version": version},
        )
        key = identity(record)
        if resolution is not None:
            counts[resolution.assignment_id] += 1
            if registry.profiles[resolution.profile_id]["quantity"] == "biomass_growth":
                selected[key] = {
                    **display_identity(record),
                    "assignment": resolution.assignment_id,
                    "uptake_amounts": uptake,
                }
        if is_independent_candidate(record, uptake, legacy_biomass):
            candidates[key] = display_identity(record)
        caller_assignments = registry.matching_caller_assignments(
            record,
            target="supplier",
            context={"ecoinvent_version": version},
        )
        cement_caller = is_cement_operation(record)
        steel_caller = is_potential_steel_operation(record)
        pipeline_caller = is_potential_pipeline_transport(record)
        freight_train_caller = is_potential_freight_train(record)
        aircraft_caller = is_potential_aircraft_transport(record)
        inland_freight_caller = is_potential_inland_freight(record)
        csp_caller = is_potential_csp_generation(record)
        passenger_rail_caller = is_potential_passenger_rail(record)
        bioenergy_caller = is_potential_bioenergy_service(record)
        new_marine_caller = is_potential_new_marine_service(record)
        platform_caller = record.get("name", "").casefold() in {
            "petroleum and gas production, off-shore",
            "petroleum and gas production, offshore",
        }
        coal_mine_caller = is_potential_coal_mine_operation(record)
        heating_caller = is_potential_heating_operation(record)
        if (
            not caller_assignments
            and not cement_caller
            and not steel_caller
            and not pipeline_caller
            and not freight_train_caller
            and not aircraft_caller
            and not inland_freight_caller
            and not csp_caller
            and not passenger_rail_caller
            and not bioenergy_caller
            and not new_marine_caller
            and not platform_caller
            and not coal_mine_caller
            and not heating_caller
        ):
            continue
        supplier_registry = copy.copy(registry)
        supplier_registry.assignments = (
            caller_assignments + general_supplier_assignments
        )
        for exchange in activity.technosphere():
            supplier_record = exchange_record(exchange, records_by_key)
            relationship_key = relationship_identity(record, supplier_record)
            if cement_caller and is_cement_factory_exchange(supplier_record):
                cement_candidates[relationship_key] = display_relationship(
                    record, supplier_record
                )
            steel_category = steel_relationship_category(record, supplier_record)
            if steel_category is not None:
                steel_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "furnace_category": steel_category,
                }
            pipeline_category = pipeline_relationship_category(record, supplier_record)
            if pipeline_category is not None:
                pipeline_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "pipeline_category": pipeline_category,
                }
            freight_wagon_category = freight_wagon_relationship_category(
                record, supplier_record
            )
            if freight_wagon_category is not None:
                freight_wagon_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "rail_asset_category": freight_wagon_category,
                }
            aircraft_category = aircraft_relationship_category(record, supplier_record)
            if aircraft_category is not None:
                aircraft_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "aircraft_category": aircraft_category,
                }
            inland_vessel_category = inland_vessel_relationship_category(
                record, supplier_record
            )
            if inland_vessel_category is not None:
                inland_vessel_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "inland_vessel_category": inland_vessel_category,
                }
            csp_category = csp_relationship_category(record, supplier_record)
            if csp_category is not None:
                csp_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "power_plant_category": csp_category,
                }
            if platform_relationship_category(record, supplier_record):
                platform_candidates[relationship_key] = display_relationship(
                    record, supplier_record
                )
            if passenger_rail_relationship_category(record, supplier_record):
                passenger_rail_candidates[relationship_key] = display_relationship(
                    record, supplier_record
                )
            bioenergy_category = bioenergy_relationship_category(
                record, supplier_record
            )
            if bioenergy_category:
                bioenergy_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "asset_category": bioenergy_category,
                }
            new_marine_category = new_marine_relationship_category(
                record, supplier_record
            )
            if new_marine_category:
                new_marine_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "asset_category": new_marine_category,
                }
            coal_mine_category = coal_mine_relationship_category(
                record, supplier_record
            )
            if coal_mine_category is not None:
                coal_mine_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "mine_category": coal_mine_category,
                }
            heating_category = heating_relationship_category(record, supplier_record)
            if heating_category is not None:
                heating_candidates[relationship_key] = {
                    **display_relationship(record, supplier_record),
                    "heating_category": heating_category,
                }
            relationship = supplier_registry.resolve(
                supplier_record,
                target="supplier",
                context={"ecoinvent_version": version},
                caller_record=record,
            )
            if relationship is not None:
                counts[relationship.assignment_id] += 1
                relationship_counts[relationship.assignment_id] += 1
                if "gcct" in relationship.assignment_id:
                    cement_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if "gem-gist" in relationship.assignment_id:
                    steel_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if any(
                    tracker in relationship.assignment_id
                    for tracker in ("gem-ggit", "gem-goit")
                ):
                    pipeline_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if "freight-wagon" in relationship.assignment_id:
                    freight_wagon_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if (
                    "eurostat-commercial-passenger-aircraft"
                    in relationship.assignment_id
                ):
                    aircraft_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if "ccnr-" in relationship.assignment_id:
                    inland_vessel_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if "gem-gspt-csp" in relationship.assignment_id:
                    csp_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if "nod-offshore-platform" in relationship.assignment_id:
                    platform_selected[relationship_key] = display_relationship(
                        record, supplier_record
                    )
                if "orr-passenger-rail" in relationship.assignment_id:
                    passenger_rail_selected[relationship_key] = display_relationship(
                        record, supplier_record
                    )
                if "gem-gbpt" in relationship.assignment_id:
                    bioenergy_selected[relationship_key] = display_relationship(
                        record, supplier_record
                    )
                if (
                    "gem-gwpt" in relationship.assignment_id
                    or "igu-lng-carrier" in relationship.assignment_id
                ):
                    new_marine_selected[relationship_key] = display_relationship(
                        record, supplier_record
                    )
                if "gem-gcmt" in relationship.assignment_id:
                    coal_mine_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }
                if "fraunhofer" in relationship.assignment_id:
                    heating_selected[relationship_key] = {
                        **display_relationship(record, supplier_record),
                        "assignment": relationship.assignment_id,
                    }

    for workbook, caller, exchanges in additional_relationships:
        caller_assignments = registry.matching_caller_assignments(
            caller,
            target="supplier",
            context={"ecoinvent_version": version},
        )
        if not caller_assignments:
            continue
        matches = []
        for exchange in exchanges:
            relationship = registry.resolve(
                exchange,
                target="supplier",
                context={"ecoinvent_version": version},
                caller_record=caller,
            )
            if relationship is None:
                continue
            matches.append(relationship.assignment_id)
            counts[relationship.assignment_id] += 1
            relationship_counts[relationship.assignment_id] += 1
        is_vehicle_service = (
            caller["name"]
            .casefold()
            .startswith(
                (
                    "transport, passenger car,",
                    "transport, passenger bus,",
                    "transport, freight, lorry,",
                )
            )
        )
        vehicle_matches = [
            assignment_id
            for assignment_id in matches
            if "eurostat-bus-" in assignment_id
            or "eurostat-lorry-" in assignment_id
            or "eurostat-light-commercial-" in assignment_id
            or "-dft-" in assignment_id
        ]
        if is_vehicle_service and len(vehicle_matches) != 1:
            additional_inventory_misses.append(
                {
                    "workbook": workbook,
                    "caller": display_identity(caller),
                    "relationship_matches": matches,
                }
            )

    missing = [candidates[key] for key in sorted(candidates.keys() - selected.keys())]
    extra = [selected[key] for key in sorted(selected.keys() - candidates.keys())]
    without_uptake = [item for item in selected.values() if not item["uptake_amounts"]]
    cement_misses = [
        cement_candidates[key]
        for key in sorted(cement_candidates.keys() - cement_selected.keys())
    ]
    cement_extras = [
        cement_selected[key]
        for key in sorted(cement_selected.keys() - cement_candidates.keys())
    ]
    steel_misses = [
        steel_candidates[key]
        for key in sorted(steel_candidates.keys() - steel_selected.keys())
    ]
    steel_extras = [
        steel_selected[key]
        for key in sorted(steel_selected.keys() - steel_candidates.keys())
    ]
    pipeline_misses = [
        pipeline_candidates[key]
        for key in sorted(pipeline_candidates.keys() - pipeline_selected.keys())
    ]
    pipeline_extras = [
        pipeline_selected[key]
        for key in sorted(pipeline_selected.keys() - pipeline_candidates.keys())
    ]
    freight_wagon_misses = [
        freight_wagon_candidates[key]
        for key in sorted(
            freight_wagon_candidates.keys() - freight_wagon_selected.keys()
        )
    ]
    freight_wagon_extras = [
        freight_wagon_selected[key]
        for key in sorted(
            freight_wagon_selected.keys() - freight_wagon_candidates.keys()
        )
    ]
    aircraft_misses = [
        aircraft_candidates[key]
        for key in sorted(aircraft_candidates.keys() - aircraft_selected.keys())
    ]
    aircraft_extras = [
        aircraft_selected[key]
        for key in sorted(aircraft_selected.keys() - aircraft_candidates.keys())
    ]
    inland_vessel_misses = [
        inland_vessel_candidates[key]
        for key in sorted(
            inland_vessel_candidates.keys() - inland_vessel_selected.keys()
        )
    ]
    inland_vessel_extras = [
        inland_vessel_selected[key]
        for key in sorted(
            inland_vessel_selected.keys() - inland_vessel_candidates.keys()
        )
    ]
    csp_misses = [
        csp_candidates[key]
        for key in sorted(csp_candidates.keys() - csp_selected.keys())
    ]
    csp_extras = [
        csp_selected[key] for key in sorted(csp_selected.keys() - csp_candidates.keys())
    ]
    platform_misses = [
        platform_candidates[k]
        for k in sorted(platform_candidates.keys() - platform_selected.keys())
    ]
    platform_extras = [
        platform_selected[k]
        for k in sorted(platform_selected.keys() - platform_candidates.keys())
    ]
    passenger_rail_misses = [
        passenger_rail_candidates[k]
        for k in sorted(
            passenger_rail_candidates.keys() - passenger_rail_selected.keys()
        )
    ]
    passenger_rail_extras = [
        passenger_rail_selected[k]
        for k in sorted(
            passenger_rail_selected.keys() - passenger_rail_candidates.keys()
        )
    ]
    coal_mine_misses = [
        coal_mine_candidates[key]
        for key in sorted(coal_mine_candidates.keys() - coal_mine_selected.keys())
    ]
    coal_mine_extras = [
        coal_mine_selected[key]
        for key in sorted(coal_mine_selected.keys() - coal_mine_candidates.keys())
    ]
    heating_misses = [
        heating_candidates[key]
        for key in sorted(heating_candidates.keys() - heating_selected.keys())
    ]
    heating_extras = [
        heating_selected[key]
        for key in sorted(heating_selected.keys() - heating_candidates.keys())
    ]
    expected_failures = []
    new_marine_misses = [
        new_marine_candidates[k]
        for k in sorted(new_marine_candidates.keys() - new_marine_selected.keys())
    ]
    new_marine_extras = [
        new_marine_selected[k]
        for k in sorted(new_marine_selected.keys() - new_marine_candidates.keys())
    ]
    bioenergy_misses = [
        bioenergy_candidates[k]
        for k in sorted(bioenergy_candidates.keys() - bioenergy_selected.keys())
    ]
    bioenergy_extras = [
        bioenergy_selected[k]
        for k in sorted(bioenergy_selected.keys() - bioenergy_candidates.keys())
    ]
    for assignment in registry.assignments:
        applicability = assignment.get("applicability", {})
        if version not in applicability.get("ecoinvent_versions", []):
            continue
        minimum = assignment.get("expected_matches", {}).get("minimum")
        actual = counts[assignment["id"]]
        if minimum is not None and actual < minimum:
            expected_failures.append(
                {"assignment": assignment["id"], "minimum": minimum, "actual": actual}
            )

    return {
        "version": version,
        "project": project,
        "database": database_name,
        "database_activities": len(database),
        "selector_matches": len(selected),
        "all_matches_by_assignment": dict(sorted(counts.items())),
        "relationship_matches_by_assignment": dict(sorted(relationship_counts.items())),
        "independent_candidates": len(candidates),
        "matches_by_assignment": dict(sorted(counts.items())),
        "candidate_misses": missing,
        "selected_outside_candidate_set": extra,
        "selected_without_uptake": without_uptake,
        "cement_relationship_candidates": len(cement_candidates),
        "cement_relationship_misses": cement_misses,
        "cement_relationship_extras": cement_extras,
        "steel_relationship_candidates": len(steel_candidates),
        "steel_relationship_misses": steel_misses,
        "steel_relationship_extras": steel_extras,
        "pipeline_relationship_candidates": len(pipeline_candidates),
        "pipeline_relationship_misses": pipeline_misses,
        "pipeline_relationship_extras": pipeline_extras,
        "freight_wagon_relationship_candidates": len(freight_wagon_candidates),
        "freight_wagon_relationship_misses": freight_wagon_misses,
        "freight_wagon_relationship_extras": freight_wagon_extras,
        "aircraft_relationship_candidates": len(aircraft_candidates),
        "aircraft_relationship_misses": aircraft_misses,
        "aircraft_relationship_extras": aircraft_extras,
        "inland_vessel_relationship_candidates": len(inland_vessel_candidates),
        "inland_vessel_relationship_misses": inland_vessel_misses,
        "inland_vessel_relationship_extras": inland_vessel_extras,
        "csp_relationship_candidates": len(csp_candidates),
        "csp_relationship_misses": csp_misses,
        "csp_relationship_extras": csp_extras,
        "platform_relationship_candidates": len(platform_candidates),
        "platform_relationship_misses": platform_misses,
        "platform_relationship_extras": platform_extras,
        "passenger_rail_relationship_candidates": len(passenger_rail_candidates),
        "passenger_rail_relationship_misses": passenger_rail_misses,
        "passenger_rail_relationship_extras": passenger_rail_extras,
        "bioenergy_relationship_candidates": len(bioenergy_candidates),
        "bioenergy_candidates_by_category": dict(
            Counter(item["asset_category"] for item in bioenergy_candidates.values())
        ),
        "bioenergy_relationship_misses": bioenergy_misses,
        "bioenergy_relationship_extras": bioenergy_extras,
        "new_marine_candidates_by_category": dict(
            Counter(v["asset_category"] for v in new_marine_candidates.values())
        ),
        "new_marine_relationship_misses": new_marine_misses,
        "new_marine_relationship_extras": new_marine_extras,
        "coal_mine_relationship_candidates": len(coal_mine_candidates),
        "coal_mine_relationship_misses": coal_mine_misses,
        "coal_mine_relationship_extras": coal_mine_extras,
        "heating_relationship_candidates": len(heating_candidates),
        "heating_relationship_misses": heating_misses,
        "heating_relationship_extras": heating_extras,
        "additional_inventory_misses": additional_inventory_misses,
        "expected_match_failures": expected_failures,
    }


def print_report(reports: list[dict[str, Any]]) -> None:
    print("version  activities  selected  candidates  misses  extras  no-uptake")
    for report in reports:
        print(
            f"{report['version']:<8}"
            f"{report['database_activities']:>11}"
            f"{report['selector_matches']:>10}"
            f"{report['independent_candidates']:>12}"
            f"{len(report['candidate_misses']):>8}"
            f"{len(report['selected_outside_candidate_set']):>8}"
            f"{len(report['selected_without_uptake']):>11}"
        )
        electricity_counts = {
            assignment: count
            for assignment, count in report["all_matches_by_assignment"].items()
            if "service-life" in assignment
        }
        if electricity_counts:
            print(f"  service_life_matches: {electricity_counts}")
        relationship_counts = report["relationship_matches_by_assignment"]
        if relationship_counts:
            print(f"  construction_timing_matches: {relationship_counts}")
        print(
            "  cement_relationships: "
            f"{report['cement_relationship_candidates']} candidates, "
            f"{len(report['cement_relationship_misses'])} misses, "
            f"{len(report['cement_relationship_extras'])} extras"
        )
        print(
            "  steel_relationships: "
            f"{report['steel_relationship_candidates']} candidates, "
            f"{len(report['steel_relationship_misses'])} misses, "
            f"{len(report['steel_relationship_extras'])} extras"
        )
        print(
            "  pipeline_relationships: "
            f"{report['pipeline_relationship_candidates']} candidates, "
            f"{len(report['pipeline_relationship_misses'])} misses, "
            f"{len(report['pipeline_relationship_extras'])} extras"
        )
        print(
            "  freight_wagon_relationships: "
            f"{report['freight_wagon_relationship_candidates']} candidates, "
            f"{len(report['freight_wagon_relationship_misses'])} misses, "
            f"{len(report['freight_wagon_relationship_extras'])} extras"
        )
        print(
            "  aircraft_relationships: "
            f"{report['aircraft_relationship_candidates']} candidates, "
            f"{len(report['aircraft_relationship_misses'])} misses, "
            f"{len(report['aircraft_relationship_extras'])} extras"
        )
        print(
            "  inland_vessel_relationships: "
            f"{report['inland_vessel_relationship_candidates']} candidates, "
            f"{len(report['inland_vessel_relationship_misses'])} misses, "
            f"{len(report['inland_vessel_relationship_extras'])} extras"
        )
        print(
            "  csp_relationships: "
            f"{report['csp_relationship_candidates']} candidates, "
            f"{len(report['csp_relationship_misses'])} misses, "
            f"{len(report['csp_relationship_extras'])} extras"
        )
        print(
            "  coal_mine_relationships: "
            f"{report['coal_mine_relationship_candidates']} candidates, "
            f"{len(report['coal_mine_relationship_misses'])} misses, "
            f"{len(report['coal_mine_relationship_extras'])} extras"
        )
        print(
            "  heating_relationships: "
            f"{report['heating_relationship_candidates']} candidates, "
            f"{len(report['heating_relationship_misses'])} misses, "
            f"{len(report['heating_relationship_extras'])} extras"
        )
        print(
            "  platform_relationships: "
            f"{report['platform_relationship_candidates']} candidates, "
            f"{len(report['platform_relationship_misses'])} misses, "
            f"{len(report['platform_relationship_extras'])} extras"
        )
        print(
            "  passenger_rail_relationships: "
            f"{report['passenger_rail_relationship_candidates']} candidates, "
            f"{len(report['passenger_rail_relationship_misses'])} misses, "
            f"{len(report['passenger_rail_relationship_extras'])} extras"
        )
        print(
            "  bioenergy_relationships: "
            f"{report['bioenergy_candidates_by_category']}, "
            f"{len(report['bioenergy_relationship_misses'])} misses, "
            f"{len(report['bioenergy_relationship_extras'])} extras"
        )
        print(
            "  new_marine_relationships: "
            f"{report['new_marine_candidates_by_category']}, "
            f"{len(report['new_marine_relationship_misses'])} misses, "
            f"{len(report['new_marine_relationship_extras'])} extras"
        )
        for field in (
            "candidate_misses",
            "selected_outside_candidate_set",
            "selected_without_uptake",
            "cement_relationship_misses",
            "cement_relationship_extras",
            "steel_relationship_misses",
            "steel_relationship_extras",
            "pipeline_relationship_misses",
            "pipeline_relationship_extras",
            "freight_wagon_relationship_misses",
            "freight_wagon_relationship_extras",
            "aircraft_relationship_misses",
            "aircraft_relationship_extras",
            "inland_vessel_relationship_misses",
            "inland_vessel_relationship_extras",
            "csp_relationship_misses",
            "csp_relationship_extras",
            "platform_relationship_misses",
            "platform_relationship_extras",
            "passenger_rail_relationship_misses",
            "passenger_rail_relationship_extras",
            "bioenergy_relationship_misses",
            "bioenergy_relationship_extras",
            "new_marine_relationship_misses",
            "new_marine_relationship_extras",
            "coal_mine_relationship_misses",
            "coal_mine_relationship_extras",
            "heating_relationship_misses",
            "heating_relationship_extras",
            "additional_inventory_misses",
            "expected_match_failures",
        ):
            if report[field]:
                print(f"  {field}: {report[field]}")


def main() -> int:
    args = parse_args()
    registry = TemporalRegistry.from_directory(DATA_DIR / "trails" / "temporal")
    legacy_biomass = legacy_biomass_identities()
    additional_relationships = workbook_relationships(args.additional_inventories)
    available_projects = {project.name for project in bw2data.projects}
    reports = []
    missing_projects = []

    for version in args.versions:
        project = args.project_template.format(version=version)
        if project not in available_projects:
            missing_projects.append(project)
            continue
        print(f"Auditing ecoinvent {version}...", flush=True)
        reports.append(
            audit_version(
                version,
                project,
                args.database_template.format(version=version),
                registry,
                legacy_biomass,
                additional_relationships,
            )
        )

    print_report(reports)
    if missing_projects:
        print(f"Missing Brightway projects: {', '.join(missing_projects)}")
    if args.json:
        args.json.write_text(json.dumps(reports, indent=2) + "\n", encoding="utf-8")

    failures = missing_projects or any(
        report["candidate_misses"]
        or report["selected_outside_candidate_set"]
        or report["selected_without_uptake"]
        or report["cement_relationship_misses"]
        or report["cement_relationship_extras"]
        or report["steel_relationship_misses"]
        or report["steel_relationship_extras"]
        or report["pipeline_relationship_misses"]
        or report["pipeline_relationship_extras"]
        or report["freight_wagon_relationship_misses"]
        or report["freight_wagon_relationship_extras"]
        or report["aircraft_relationship_misses"]
        or report["aircraft_relationship_extras"]
        or report["inland_vessel_relationship_misses"]
        or report["inland_vessel_relationship_extras"]
        or report["csp_relationship_misses"]
        or report["csp_relationship_extras"]
        or report["platform_relationship_misses"]
        or report["platform_relationship_extras"]
        or report["passenger_rail_relationship_misses"]
        or report["passenger_rail_relationship_extras"]
        or report["bioenergy_relationship_misses"]
        or report["bioenergy_relationship_extras"]
        or report["new_marine_relationship_misses"]
        or report["new_marine_relationship_extras"]
        or report["coal_mine_relationship_misses"]
        or report["coal_mine_relationship_extras"]
        or report["heating_relationship_misses"]
        or report["heating_relationship_extras"]
        or report["additional_inventory_misses"]
        or report["expected_match_failures"]
        for report in reports
    )
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
