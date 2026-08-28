#!/usr/bin/env python3
"""Build the eight-database IMAGE SSP2_C400 incremental study ladder.

The script preserves the augmented C400 source CSV, verifies its checksum,
uses an encrypted local copy, and refuses to overwrite Brightway databases.
It builds only the core cumulative sector ladder; forced single-CDR-technology
variants are outside this experiment.
"""

from __future__ import annotations

import argparse
import gc
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "dev"))

import bw2data as bd

import build_c400_study_pair as c400_input
from premise import NewDatabase

DEFAULT_OUTPUT_PREFIX = "ei_cutoff_3.12_image_SSP2_C400"
TRANSPORT_SUPPORT_SECTORS = [
    "cars",
    "two_wheelers",
    "trucks",
    "ships",
    "buses",
    "trains",
    "fuels",
    "battery",
]


@dataclass(frozen=True)
class Stage:
    slug: str
    year: int
    sectors: list[str] | None
    cdr_allocation: bool = False


def stage_plan() -> list[Stage]:
    biomass_electricity = ["biomass", "electricity"]
    heat = [*biomass_electricity, "heat"]
    steel = [*heat, "steel"]
    cement = [*steel, "cement"]
    transport = [*cement, *TRANSPORT_SUPPORT_SECTORS]
    return [
        Stage("2025_baseline", 2025, None),
        Stage("2060_biomass_electricity", 2060, biomass_electricity),
        Stage("2060_biomass_electricity_heat", 2060, heat),
        Stage("2060_biomass_electricity_heat_steel", 2060, steel),
        Stage("2060_biomass_electricity_heat_steel_cement", 2060, cement),
        Stage(
            "2060_biomass_electricity_heat_steel_cement_transport",
            2060,
            transport,
        ),
        Stage("2060_all", 2060, None),
        Stage("2060_all_cdr_market", 2060, None, cdr_allocation=True),
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
    parser.add_argument("--plain-iam", type=Path, default=c400_input.DEFAULT_PLAIN_IAM)
    parser.add_argument(
        "--encrypted-iam-dir",
        type=Path,
        default=c400_input.DEFAULT_ENCRYPTED_IAM_DIR,
    )
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--only-stages",
        nargs="*",
        default=None,
        help="Optional stage slugs to build, in canonical ladder order.",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Run transformations but do not write Brightway databases.",
    )
    parser.add_argument(
        "--refresh-encrypted",
        action="store_true",
        help="Recreate the encrypted IAM copy after checksum verification.",
    )
    return parser.parse_args()


def selected_stages(args: argparse.Namespace) -> list[Stage]:
    stages = stage_plan()
    if args.only_stages is None:
        return stages

    known = {stage.slug for stage in stages}
    unknown = sorted(set(args.only_stages) - known)
    if unknown:
        raise ValueError(f"Unknown stage slugs: {unknown}")
    requested = set(args.only_stages)
    return [stage for stage in stages if stage.slug in requested]


def input_args(args: argparse.Namespace, year: int) -> SimpleNamespace:
    return SimpleNamespace(
        model=args.model,
        pathway=args.pathway,
        year=year,
        source_version=args.source_version,
        system_model=args.system_model,
        plain_iam=args.plain_iam,
        encrypted_iam_dir=args.encrypted_iam_dir,
        refresh_encrypted=args.refresh_encrypted,
    )


def validate_inputs(args: argparse.Namespace, key: bytes) -> None:
    c400_input.prepare_encrypted_iam(input_args(args, 2060), key)
    for year in (2025, 2060):
        print()
        print(f"Validating C400 IAM input for {year}")
        c400_input.validate_c400_input(input_args(args, year), key)


def output_name(args: argparse.Namespace, stage: Stage) -> str:
    return f"{args.output_prefix}_{stage.slug}"


def preflight_brightway(args: argparse.Namespace, stages: list[Stage]) -> None:
    bd.projects.set_current(args.project)
    missing = [
        name for name in (args.source_db, args.biosphere) if name not in bd.databases
    ]
    if missing:
        raise RuntimeError(f"Missing Brightway databases: {missing}")

    if args.no_write:
        return

    existing = [
        output_name(args, stage)
        for stage in stages
        if output_name(args, stage) in bd.databases
    ]
    if existing:
        raise RuntimeError(
            "Refusing to overwrite existing Brightway databases: " + ", ".join(existing)
        )


def new_database(args: argparse.Namespace, stage: Stage, key: bytes) -> NewDatabase:
    return NewDatabase(
        scenarios=[
            {
                "model": args.model,
                "pathway": args.pathway,
                "year": stage.year,
                "filepath": args.encrypted_iam_dir,
            }
        ],
        source_db=args.source_db,
        source_version=args.source_version,
        source_type="brightway",
        system_model=args.system_model,
        biosphere_name=args.biosphere,
        key=key,
        cdr_allocation=stage.cdr_allocation,
        keep_imports_uncertainty=True,
        keep_source_db_uncertainty=False,
        generate_reports=False,
    )


def build_stage(args: argparse.Namespace, stage: Stage, key: bytes) -> None:
    name = output_name(args, stage)
    print()
    print(f"Building {name}")
    print(f"  year: {stage.year}")
    print(f"  sectors: {stage.sectors if stage.sectors is not None else 'all'}")
    print(f"  cdr_allocation: {stage.cdr_allocation}")

    ndb = new_database(args, stage, key)
    ndb.update(stage.sectors)

    if args.no_write:
        print(f"No-write transformation completed: {name}")
    else:
        ndb.write_db_to_brightway(name)
        if name not in bd.databases:
            raise RuntimeError(f"Brightway database was not written: {name}")
        print(f"Wrote {name} with {len(bd.Database(name))} activities.")

    del ndb
    gc.collect()


def main() -> None:
    args = parse_args()
    key = c400_input.get_key()
    stages = selected_stages(args)
    validate_inputs(args, key)
    preflight_brightway(args, stages)

    print()
    print(f"Brightway project: {args.project}")
    print(f"Source database: {args.source_db}")
    print(f"Scenario: {args.model} / {args.pathway}")
    print(f"Stages: {len(stages)}")
    for stage in stages:
        build_stage(args, stage, key)


if __name__ == "__main__":
    main()
