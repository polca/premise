#!/usr/bin/env python3
"""Build the six forced-CDR technology variants for the C400 study.

The script performs the all-sector 2060 transformation with CDR allocation once,
then materializes an isolated copy for each sensitivity database.  Every
regional CDR market is forced to one technology; the World market retains its
scenario-weighted links to those regional markets.  The checksum-verified plain
C400 source is never modified, and existing Brightway databases are never
overwritten.
"""

from __future__ import annotations

import argparse
import copy
import gc
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "dev"))

import bw2data as bd

import build_c400_cdr_technology_databases as forced_cdr
import build_c400_study_ladder as ladder
import build_c400_study_pair as c400_input
from premise.inventory_store import InventoryStore, create_inventory_store

DEFAULT_OUTPUT_PREFIX = ladder.DEFAULT_OUTPUT_PREFIX
EXPECTED_REGIONAL_MARKETS = 26


@dataclass(frozen=True)
class Variant:
    slug: str
    technology: str


def variant_plan() -> list[Variant]:
    return [
        Variant(
            "direct_air_capture_solvent_gas_heat_with_storage",
            "direct air capture (solvent, high-temp, gas heat) with storage",
        ),
        Variant(
            "direct_air_capture_sorbent_heat_pump_with_storage",
            "direct air capture (sorbent, low-temp, heat pump) with storage",
        ),
        Variant("enhanced_rock_weathering", "enhanced rock weathering"),
        Variant(
            "cement_production_non_fossil_co2_with_ccs",
            "cement production, non-fossil CO2, with CCS",
        ),
        Variant(
            "afforestation_eucalyptus_plantation",
            "afforestation, eucalyptus plantation",
        ),
        Variant(
            "afforestation_poplar_plantation",
            "afforestation, poplar plantation",
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-version", default="3.12")
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument("--biosphere", default="ecoinvent-3.12-biosphere")
    parser.add_argument("--model", default="image")
    parser.add_argument("--pathway", default="SSP2_C400")
    parser.add_argument("--year", type=int, default=2060)
    parser.add_argument("--plain-iam", type=Path, default=c400_input.DEFAULT_PLAIN_IAM)
    parser.add_argument(
        "--encrypted-iam-dir",
        type=Path,
        default=c400_input.DEFAULT_ENCRYPTED_IAM_DIR,
    )
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="Optional variant slugs to build in canonical order.",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Transform and validate every requested variant without writing.",
    )
    parser.add_argument(
        "--refresh-encrypted",
        action="store_true",
        help="Recreate the encrypted IAM copy after checksum verification.",
    )
    return parser.parse_args()


def selected_variants(args: argparse.Namespace) -> list[Variant]:
    variants = variant_plan()
    if args.only is None:
        return variants

    known = {variant.slug for variant in variants}
    unknown = sorted(set(args.only) - known)
    if unknown:
        raise ValueError(f"Unknown variant slugs: {unknown}")
    requested = set(args.only)
    return [variant for variant in variants if variant.slug in requested]


def input_args(args: argparse.Namespace) -> SimpleNamespace:
    return SimpleNamespace(
        model=args.model,
        pathway=args.pathway,
        year=args.year,
        source_version=args.source_version,
        system_model=args.system_model,
        plain_iam=args.plain_iam,
        encrypted_iam_dir=args.encrypted_iam_dir,
        refresh_encrypted=args.refresh_encrypted,
    )


def output_name(args: argparse.Namespace, variant: Variant) -> str:
    return f"{args.output_prefix}_2060_cdr_{variant.slug}"


def preflight(args: argparse.Namespace, variants: list[Variant], key: bytes) -> None:
    if args.year != 2060:
        raise ValueError("The study sensitivity databases must use year 2060.")

    c400_input.prepare_encrypted_iam(input_args(args), key)
    c400_input.validate_c400_input(input_args(args), key)

    bd.projects.set_current(args.project)
    missing = [
        name for name in (args.source_db, args.biosphere) if name not in bd.databases
    ]
    if missing:
        raise RuntimeError(f"Missing Brightway databases: {missing}")

    if args.no_write:
        return
    existing = [
        output_name(args, variant)
        for variant in variants
        if output_name(args, variant) in bd.databases
    ]
    if existing:
        raise RuntimeError(
            "Refusing to overwrite existing Brightway databases: " + ", ".join(existing)
        )


def scenario_inventory_store(scenario: dict) -> InventoryStore:
    store = scenario.get("_inventory_store")
    if store is not None:
        return store
    checkpoint = scenario.get("_inventory_checkpoint")
    if checkpoint is not None:
        return InventoryStore.open(checkpoint)
    raise RuntimeError("The transformed scenario has no reusable inventory store.")


def isolated_scenario(base_scenario: dict, database: list[dict]) -> dict:
    scenario = copy.copy(base_scenario)
    for field in (
        "_inventory_store",
        "_inventory_checkpoint",
        "_inventory_working_copy",
        "_inventory_export_handoff",
        "database filepath",
    ):
        scenario.pop(field, None)
    scenario["database"] = database
    return scenario


def target_supplier_identities(scenario: dict, technology: str) -> set[tuple]:
    mapping = scenario.get("mapping", {}).get("cdr", {})
    suppliers = mapping.get(technology, [])
    return {
        (
            supplier.get("name"),
            supplier.get("reference product"),
            supplier.get("location"),
            supplier.get("unit"),
        )
        for supplier in suppliers
    }


def validate_forced_variant(scenario: dict, technology: str) -> None:
    target_identities = target_supplier_identities(scenario, technology)
    if not target_identities:
        raise RuntimeError(f"No mapped suppliers found for {technology!r}.")

    markets = forced_cdr.cdr_markets(scenario["database"])
    if len(markets) != EXPECTED_REGIONAL_MARKETS:
        raise RuntimeError(
            f"Expected {EXPECTED_REGIONAL_MARKETS} regional CDR markets; "
            f"found {len(markets)}."
        )

    invalid = []
    for market in markets:
        exchanges = [
            exchange
            for exchange in market["exchanges"]
            if exchange.get("type") == "technosphere"
        ]
        amount = sum(float(exchange.get("amount", 0.0)) for exchange in exchanges)
        identities = {
            (
                exchange.get("name"),
                exchange.get("product"),
                exchange.get("location"),
                exchange.get("unit"),
            )
            for exchange in exchanges
        }
        if (
            not exchanges
            or not identities.issubset(target_identities)
            or not math.isclose(amount, 1.0, rel_tol=1e-12, abs_tol=1e-12)
        ):
            invalid.append(market.get("location"))
    if invalid:
        raise RuntimeError(
            f"Invalid forced CDR markets for {technology!r}: {sorted(invalid)}"
        )

    world = [
        dataset
        for dataset in scenario["database"]
        if dataset.get("name") == forced_cdr.CDR_MARKET
        and dataset.get("reference product") == forced_cdr.CDR_PRODUCT
        and dataset.get("unit") == "kilogram"
        and dataset.get("location") == "World"
    ]
    if len(world) != 1:
        raise RuntimeError(f"Expected one World CDR market; found {len(world)}.")
    world_inputs = [
        exchange
        for exchange in world[0]["exchanges"]
        if exchange.get("type") == "technosphere"
    ]
    if (
        len(world_inputs) != EXPECTED_REGIONAL_MARKETS
        or any(
            exchange.get("name") != forced_cdr.CDR_MARKET for exchange in world_inputs
        )
        or not math.isclose(
            sum(float(exchange.get("amount", 0.0)) for exchange in world_inputs),
            1.0,
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
    ):
        raise RuntimeError("The World CDR market is not a valid regional aggregation.")


def transform_base(args: argparse.Namespace, key: bytes):
    stage = ladder.Stage("2060_all_cdr_market", 2060, None, cdr_allocation=True)
    ndb = ladder.new_database(args, stage, key)
    ndb.update()
    return ndb, ndb.scenarios[0]


def build_variant(
    args: argparse.Namespace,
    ndb,
    base_scenario: dict,
    store: InventoryStore,
    variant: Variant,
) -> None:
    name = output_name(args, variant)
    print()
    print(f"Preparing {name}")
    print(f"  forced technology: {variant.technology}")

    database = store.materialize(restore_metadata=True)
    scenario = isolated_scenario(base_scenario, database)
    updated, missing_regions = forced_cdr.force_cdr_market_to_technology(
        scenario, variant.technology
    )
    if updated != EXPECTED_REGIONAL_MARKETS or missing_regions:
        raise RuntimeError(
            f"Could not force every regional market for {variant.technology!r}: "
            f"updated={updated}, missing={missing_regions}"
        )
    validate_forced_variant(scenario, variant.technology)
    print(f"Validated {updated} forced regional markets and the World aggregation.")

    if args.no_write:
        print(f"No-write variant completed: {name}")
    else:
        scenario["_inventory_store"] = create_inventory_store(
            database,
            backend=store.backend_name,
            scenario_identity=(args.model, args.pathway, args.year, variant.slug),
            take_ownership=True,
        )
        scenario.pop("database", None)
        ndb.scenarios = [scenario]
        ndb.write_db_to_brightway(name)
        if name not in bd.databases:
            raise RuntimeError(f"Brightway database was not written: {name}")
        print(f"Wrote {name} with {len(bd.Database(name))} activities.")

    del scenario, database
    gc.collect()


def main() -> None:
    args = parse_args()
    variants = selected_variants(args)
    key = c400_input.get_key()
    preflight(args, variants, key)

    print()
    print(f"Brightway project: {args.project}")
    print(f"Source database: {args.source_db}")
    print(f"Scenario: {args.model} / {args.pathway} / {args.year}")
    print(f"Technology variants: {len(variants)}")

    ndb, base_scenario = transform_base(args, key)
    store = scenario_inventory_store(base_scenario)
    for variant in variants:
        build_variant(args, ndb, base_scenario, store, variant)

    del ndb, base_scenario, store
    gc.collect()


if __name__ == "__main__":
    main()
