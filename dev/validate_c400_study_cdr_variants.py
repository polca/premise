#!/usr/bin/env python3
"""Validate the written C400 forced-CDR technology databases."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path

import bw2data as bd

PROJECT = "ecoinvent-3.12-cutoff"
PREFIX = "ei_cutoff_3.12_image_SSP2_C400"
REFERENCE_DATABASE = f"{PREFIX}_2060_all_cdr_market"
EXPECTED_ACTIVITY_COUNT = 50_858
EXPECTED_ALLOCATED_DATASETS = 14_079
EXPECTED_REGIONAL_MARKETS = 26

VARIANTS = [
    (
        "direct_air_capture_solvent_gas_heat_with_storage",
        "carbon dioxide, captured and stored, with a solvent-based direct air "
        "capture system, 1MtCO2",
    ),
    (
        "direct_air_capture_sorbent_heat_pump_with_storage",
        "carbon dioxide, captured and stored, with a sorbent-based direct air "
        "capture system, 100ktCO2, with heat pump heat, and grid electricity",
    ),
    (
        "enhanced_rock_weathering",
        "carbon dioxide, captured and stored, by olivine spreading on coastline",
    ),
    (
        "cement_production_non_fossil_co2_with_ccs",
        "carbon dioxide, captured and stored, at cement production plant, from "
        "non-fossil carbon dioxide, using monoethanolamine",
    ),
    (
        "afforestation_eucalyptus_plantation",
        "carbon dioxide, captured and stored, by re/afforestation, eucalyptus",
    ),
    (
        "afforestation_poplar_plantation",
        "carbon dioxide, captured and stored, by re/afforestation, willow",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument("--prefix", default=PREFIX)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/c400_study_cdr_variants_validation.json"),
    )
    return parser.parse_args()


def identity(activity) -> tuple[str | None, ...]:
    return (
        activity.get("name"),
        activity.get("reference product"),
        activity.get("location"),
        activity.get("unit"),
    )


def market_activities(database: bd.Database):
    return [
        activity
        for activity in database
        if activity.get("name") == "market for carbon dioxide removal"
        and activity.get("reference product") == "carbon dioxide, captured and stored"
        and activity.get("unit") == "kilogram"
    ]


def validate_variant(
    database_name: str,
    supplier_name: str,
    reference_identities: Counter,
) -> dict[str, object]:
    database = bd.Database(database_name)
    if len(database) != EXPECTED_ACTIVITY_COUNT:
        raise RuntimeError(
            f"Unexpected activity count for {database_name}: {len(database)}"
        )

    metadata = dict(bd.databases[database_name])
    expected_metadata = {
        "iam_model": "image",
        "pathway": "SSP2_C400",
        "representative_time": "2060-01-01T00:00:00",
        "ecoinvent_version": "3.12",
        "system_model": "cutoff",
    }
    for field, expected in expected_metadata.items():
        if str(metadata.get(field)) != expected:
            raise RuntimeError(
                f"Unexpected {field} for {database_name}: {metadata.get(field)!r}"
            )

    identities = Counter(identity(activity) for activity in database)
    if identities != reference_identities:
        raise RuntimeError(
            f"Activity identities differ from {REFERENCE_DATABASE}: {database_name}"
        )

    allocated = sum(
        "amount of CDR input" in (activity.get("log parameters") or {})
        for activity in database
    )
    if allocated != EXPECTED_ALLOCATED_DATASETS:
        raise RuntimeError(
            f"Unexpected allocated dataset count for {database_name}: {allocated}"
        )

    markets = market_activities(database)
    regional = [market for market in markets if market.get("location") != "World"]
    world = [market for market in markets if market.get("location") == "World"]
    if len(regional) != EXPECTED_REGIONAL_MARKETS or len(world) != 1:
        raise RuntimeError(
            f"Unexpected CDR market counts for {database_name}: "
            f"regional={len(regional)}, world={len(world)}"
        )

    invalid_regions = []
    supplier_counts = []
    for market in regional:
        exchanges = list(market.technosphere())
        supplier_counts.append(len(exchanges))
        if (
            not exchanges
            or any(exchange.get("name") != supplier_name for exchange in exchanges)
            or any(
                exchange.get("location") != market.get("location")
                for exchange in exchanges
            )
            or not math.isclose(
                sum(float(exchange.get("amount", 0.0)) for exchange in exchanges),
                1.0,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        ):
            invalid_regions.append(market.get("location"))
    if invalid_regions:
        raise RuntimeError(
            f"Invalid forced markets for {database_name}: {sorted(invalid_regions)}"
        )

    world_inputs = list(world[0].technosphere())
    if (
        len(world_inputs) != EXPECTED_REGIONAL_MARKETS
        or any(
            exchange.get("name") != "market for carbon dioxide removal"
            for exchange in world_inputs
        )
        or not math.isclose(
            sum(float(exchange.get("amount", 0.0)) for exchange in world_inputs),
            1.0,
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
    ):
        raise RuntimeError(f"Invalid World CDR market for {database_name}")

    return {
        "database": database_name,
        "activity_count": len(database),
        "allocated_dataset_count": allocated,
        "regional_market_count": len(regional),
        "world_market_count": len(world),
        "supplier_name": supplier_name,
        "supplier_exchanges_per_regional_market": sorted(set(supplier_counts)),
        "representative_time": str(metadata["representative_time"]),
    }


def main() -> None:
    args = parse_args()
    bd.projects.set_current(args.project)
    reference_name = f"{args.prefix}_2060_all_cdr_market"
    names = [f"{args.prefix}_2060_cdr_{slug}" for slug, _ in VARIANTS]
    missing = [name for name in [reference_name, *names] if name not in bd.databases]
    if missing:
        raise RuntimeError(f"Missing databases: {missing}")

    reference = bd.Database(reference_name)
    reference_identities = Counter(identity(activity) for activity in reference)
    rows = [
        validate_variant(name, supplier_name, reference_identities)
        for name, (_, supplier_name) in zip(names, VARIANTS, strict=True)
    ]
    result = {
        "project": args.project,
        "reference_database": reference_name,
        "all_activity_identity_counts_match_reference": True,
        "variants": rows,
    }
    rendered = json.dumps(result, indent=2)
    print(rendered)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
