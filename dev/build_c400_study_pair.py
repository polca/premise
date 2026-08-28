#!/usr/bin/env python3
"""Build the paired IMAGE SSP2_C400 databases for the CDR-burden study.

The augmented plain IAM CSV is preserved as the provenance source.  Before a
premise build, this script verifies its checksum and creates/reuses an encrypted
Fernet copy in a separate directory.  It never re-extracts the source workbook
and never overwrites an existing Brightway database.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import bw2data as bd
from cryptography.fernet import Fernet, InvalidToken

from premise import NewDatabase
from premise.carbon_dioxide_removal import CarbonDioxideRemoval
from premise.data_collection import IAMDataCollection

DEFAULT_PLAIN_IAM = REPO_ROOT / "tmp" / "iam_c400" / "image_SSP2_C400.csv"
DEFAULT_ENCRYPTED_IAM_DIR = REPO_ROOT / "tmp" / "cdr_allocation_c400" / "iam_encrypted"
EXPECTED_PLAIN_SHA256 = (
    "c8d33475c65fb05bdabadec30be204a9281494e89dbe77557d1afea6e8d5fe94"
)
DEFAULT_OUTPUT_PREFIX = "ei_cutoff_3.12_image_SSP2_C400_2050"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-version", default="3.12")
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument("--biosphere", default="ecoinvent-3.12-biosphere")
    parser.add_argument("--model", default="image")
    parser.add_argument("--pathway", default="SSP2_C400")
    parser.add_argument("--year", type=int, default=2050)
    parser.add_argument("--plain-iam", type=Path, default=DEFAULT_PLAIN_IAM)
    parser.add_argument(
        "--encrypted-iam-dir", type=Path, default=DEFAULT_ENCRYPTED_IAM_DIR
    )
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--only",
        choices=("baseline", "cdr-market", "both"),
        default="both",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Run the premise transformations but do not write Brightway databases.",
    )
    parser.add_argument(
        "--refresh-encrypted",
        action="store_true",
        help="Recreate the encrypted IAM copy after checksum verification.",
    )
    return parser.parse_args()


def get_key() -> bytes:
    value = os.environ.get("PREMISE_KEY")
    if not value:
        raise RuntimeError("Set PREMISE_KEY before running this script.")
    return value.encode("ascii")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def prepare_encrypted_iam(args: argparse.Namespace, key: bytes) -> Path:
    if not args.plain_iam.is_file():
        raise FileNotFoundError(f"Missing augmented IAM CSV: {args.plain_iam}")

    plain_data = args.plain_iam.read_bytes()
    checksum = sha256(plain_data)
    if checksum != EXPECTED_PLAIN_SHA256:
        raise RuntimeError(
            "Unexpected C400 IAM checksum: "
            f"{checksum}; expected {EXPECTED_PLAIN_SHA256}."
        )

    args.encrypted_iam_dir.mkdir(parents=True, exist_ok=True)
    encrypted_path = args.encrypted_iam_dir / args.plain_iam.name
    fernet = Fernet(key)

    if encrypted_path.exists() and not args.refresh_encrypted:
        try:
            decrypted = fernet.decrypt(encrypted_path.read_bytes())
        except InvalidToken as error:
            raise RuntimeError(
                f"Existing encrypted IAM file cannot be decrypted: {encrypted_path}"
            ) from error
        if decrypted != plain_data:
            raise RuntimeError(
                f"Existing encrypted IAM file differs from {args.plain_iam}; "
                "use --refresh-encrypted to recreate it."
            )
        action = "Reused"
    else:
        encrypted_path.write_bytes(fernet.encrypt(plain_data))
        action = "Created"

    print(f"{action} encrypted IAM input: {encrypted_path}")
    print(f"Verified plain IAM SHA-256: {checksum}")
    return encrypted_path


def validate_c400_input(args: argparse.Namespace, key: bytes) -> None:
    iam = IAMDataCollection(
        model=args.model,
        pathway=args.pathway,
        year=args.year,
        filepath_iam_files=args.encrypted_iam_dir,
        key=key,
        system_model=args.system_model,
    )
    cdr = CarbonDioxideRemoval(
        database=[],
        iam_data=iam,
        model=args.model,
        pathway=args.pathway,
        year=args.year,
        version=args.source_version,
        system_model=args.system_model,
    )
    shares = cdr.calculate_cdr_allocation_coverage_shares()
    for region in ("WEU", "World"):
        if region not in shares:
            raise RuntimeError(
                f"C400 IAM data do not contain required region {region}."
            )
        print(f"{region} CDR allocation coverage: {shares[region]}")


def requested_builds(args: argparse.Namespace) -> list[tuple[str, bool]]:
    builds = []
    if args.only in {"baseline", "both"}:
        builds.append(("baseline", False))
    if args.only in {"cdr-market", "both"}:
        builds.append(("cdr_market", True))
    return builds


def preflight_brightway(args: argparse.Namespace) -> None:
    bd.projects.set_current(args.project)
    missing = [
        name for name in (args.source_db, args.biosphere) if name not in bd.databases
    ]
    if missing:
        raise RuntimeError(f"Missing Brightway databases: {missing}")

    if args.no_write:
        return

    existing = [
        f"{args.output_prefix}_{suffix}"
        for suffix, _ in requested_builds(args)
        if f"{args.output_prefix}_{suffix}" in bd.databases
    ]
    if existing:
        raise RuntimeError(
            "Refusing to overwrite existing Brightway databases: " + ", ".join(existing)
        )


def new_database(
    args: argparse.Namespace, key: bytes, *, cdr_allocation: bool
) -> NewDatabase:
    return NewDatabase(
        scenarios=[
            {
                "model": args.model,
                "pathway": args.pathway,
                "year": args.year,
                "filepath": args.encrypted_iam_dir,
            }
        ],
        source_db=args.source_db,
        source_version=args.source_version,
        source_type="brightway",
        system_model=args.system_model,
        biosphere_name=args.biosphere,
        key=key,
        cdr_allocation=cdr_allocation,
        keep_imports_uncertainty=True,
        keep_source_db_uncertainty=False,
        generate_reports=False,
    )


def build_one(
    args: argparse.Namespace, key: bytes, suffix: str, cdr_allocation: bool
) -> None:
    output_name = f"{args.output_prefix}_{suffix}"
    print()
    print(f"Building {output_name}")
    print(f"  year: {args.year}")
    print(f"  cdr_allocation: {cdr_allocation}")

    ndb = new_database(args, key, cdr_allocation=cdr_allocation)
    ndb.update()

    if args.no_write:
        print(f"No-write transformation completed: {output_name}")
    else:
        ndb.write_db_to_brightway(output_name)
        if output_name not in bd.databases:
            raise RuntimeError(f"Brightway database was not written: {output_name}")
        print(f"Wrote {output_name} with {len(bd.Database(output_name))} activities.")

    del ndb
    gc.collect()


def main() -> None:
    args = parse_args()
    key = get_key()
    prepare_encrypted_iam(args, key)
    validate_c400_input(args, key)
    preflight_brightway(args)

    print(f"Brightway project: {args.project}")
    print(f"Source database: {args.source_db}")
    print(f"Scenario: {args.model} / {args.pathway} / {args.year}")
    for suffix, cdr_allocation in requested_builds(args):
        build_one(args, key, suffix, cdr_allocation)


if __name__ == "__main__":
    main()
