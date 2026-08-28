"""Build C400 2050 databases with normal and forced CDR technology markets."""

from __future__ import annotations

import argparse
import contextlib
import copy
import io
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import bw2data as bd
import pandas as pd
import yaml

import premise.data_collection as data_collection_module
from premise import NewDatabase
from premise.data_collection import IAMDataCollection
from premise.filesystem_constants import VARIABLES_DIR
from premise.utils import load_database

DEFAULT_SCENARIO_WORKBOOK = Path(
    "/Users/romain/Library/CloudStorage/OneDrive-PaulScherrerInstitut/"
    "UPTAKE/CDR curves/Consolidated_UPTAKE_5_3_R1_20260703_1523.xlsx"
)
DEFAULT_IAM_DIR = REPO_ROOT / "tmp" / "iam_c400"
DEFAULT_MISSING_ROW_SOURCE = (
    REPO_ROOT
    / "results"
    / "uptake_scenarios_20260630_1553"
    / "premise_plain_iam"
    / "image_SSP2_C400.csv"
)
CDR_MARKET = "market for carbon dioxide removal"
CDR_PRODUCT = "carbon dioxide, captured and stored"
CDR_MAPPING = VARIABLES_DIR / "carbon_dioxide_removal.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-version", default="3.12")
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument("--biosphere", default="biosphere-3.12")
    parser.add_argument("--model", default="image")
    parser.add_argument("--source-model-label", default="IMAGE 3.5")
    parser.add_argument("--source-scenario-label", default="SSP2_C400")
    parser.add_argument("--pathway", default="SSP2_C400")
    parser.add_argument("--year", type=int, default=2050)
    parser.add_argument(
        "--scenario-workbook", type=Path, default=DEFAULT_SCENARIO_WORKBOOK
    )
    parser.add_argument("--iam-dir", type=Path, default=DEFAULT_IAM_DIR)
    parser.add_argument(
        "--fill-missing-from",
        type=Path,
        default=DEFAULT_MISSING_ROW_SOURCE,
        help=(
            "Optional plain IAM CSV used only to append rows missing from the "
            "requested scenario workbook."
        ),
    )
    parser.add_argument("--no-fill-missing", action="store_true")
    parser.add_argument(
        "--output-prefix",
        default="ei_cutoff_3.12_image_SSP2_C400_2050",
    )
    parser.add_argument(
        "--key",
        default=None,
        help="Optional Fernet key for encrypted IAM files. Leave unset for extracted C400 CSVs.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--warn-only", action="store_true")
    parser.add_argument(
        "--technologies",
        nargs="*",
        default=None,
        help="Optional subset of CDR technology labels from carbon_dioxide_removal.yaml.",
    )
    return parser.parse_args()


def get_key(args: argparse.Namespace) -> bytes | None:
    if args.key:
        return args.key.encode()
    return None


def append_missing_rows_from_companion(
    filtered: pd.DataFrame, args: argparse.Namespace
) -> pd.DataFrame:
    if args.no_fill_missing or args.fill_missing_from is None:
        return filtered
    if not args.fill_missing_from.exists():
        print(f"Missing-row companion not found: {args.fill_missing_from}")
        return filtered

    companion = pd.read_csv(args.fill_missing_from)
    companion = companion[
        (companion["Model"] == args.source_model_label)
        & (companion["Scenario"] == args.source_scenario_label)
    ].copy()
    if companion.empty:
        print(
            "Missing-row companion has no rows for "
            f"Model={args.source_model_label!r}, "
            f"Scenario={args.source_scenario_label!r}."
        )
        return filtered

    key_columns = ["Model", "Scenario", "Region", "Variable"]
    existing_keys = set(map(tuple, filtered[key_columns].to_numpy()))
    missing = companion[
        ~companion[key_columns].apply(tuple, axis=1).isin(existing_keys)
    ].copy()
    if missing.empty:
        print(f"No missing rows appended from {args.fill_missing_from}.")
        return filtered

    missing = missing.reindex(columns=filtered.columns)
    missing_variables = sorted(missing["Variable"].dropna().unique())
    print(f"Appended {len(missing)} missing IAM rows from {args.fill_missing_from}.")
    for variable in missing_variables:
        print(f"  filled: {variable}")
    return pd.concat([filtered, missing], ignore_index=True)


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return slug[:80]


def extract_c400_iam_csv(args: argparse.Namespace) -> Path:
    args.iam_dir.mkdir(parents=True, exist_ok=True)
    output = args.iam_dir / f"{args.model}_{args.pathway}.csv"

    data = pd.read_excel(args.scenario_workbook, sheet_name="data")
    filtered = data[
        (data["Model"] == args.source_model_label)
        & (data["Scenario"] == args.source_scenario_label)
    ].copy()

    if filtered.empty:
        raise RuntimeError(
            "No rows found for "
            f"Model={args.source_model_label!r}, "
            f"Scenario={args.source_scenario_label!r}."
        )

    if str(args.year) not in filtered.columns:
        raise RuntimeError(f"Year {args.year} not found in {args.scenario_workbook}.")

    filtered = append_missing_rows_from_companion(filtered, args)
    filtered.to_csv(output, index=False)
    print(f"Extracted {len(filtered)} C400 IAM rows to {output}.")
    return output


def collect_missing_variables(
    args: argparse.Namespace, key: bytes | None
) -> list[tuple[str, list[str]]]:
    calls: list[tuple[str, list[str]]] = []
    original = data_collection_module.print_missing_variables

    def capture_missing(missing_vars, file_name=None):
        missing = sorted(
            str(variable) for variable in missing_vars if variable is not None
        )
        if missing:
            calls.append((str(file_name), missing))

    data_collection_module.print_missing_variables = capture_missing
    try:
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            IAMDataCollection(
                model=args.model,
                pathway=args.pathway,
                year=args.year,
                filepath_iam_files=args.iam_dir,
                key=key,
                system_model=args.system_model,
            )
    finally:
        data_collection_module.print_missing_variables = original

    return calls


def print_missing_variable_warning(calls: list[tuple[str, list[str]]]) -> None:
    unique = sorted({variable for _, variables in calls for variable in variables})
    print()
    print("Missing IAM variable warnings")
    print(f"  warning calls: {len(calls)}")
    print(f"  unique variables: {len(unique)}")
    if not unique:
        print("  none")
        return

    for variable in unique:
        print(f"  - {variable}")


def load_cdr_technology_labels() -> list[str]:
    with open(CDR_MAPPING, encoding="utf-8") as stream:
        return list(yaml.safe_load(stream))


def resolve_biosphere(preferred: str) -> str:
    if preferred in bd.databases:
        return preferred

    matches = sorted(name for name in bd.databases if "biosphere" in name.lower())
    if not matches:
        raise RuntimeError(f"Missing biosphere database {preferred!r}.")

    print(f"Using biosphere database {matches[0]!r} instead of {preferred!r}.")
    return matches[0]


def check_outputs_available(outputs: list[str], overwrite: bool) -> None:
    for output in outputs:
        if output in bd.databases and overwrite:
            del bd.databases[output]
            print(f"Deleted existing output database {output!r}.")
        if output in bd.databases:
            raise RuntimeError(
                f"Output database {output!r} already exists. "
                "Use --overwrite or choose another name."
            )


def new_database(
    args: argparse.Namespace, key: bytes | None, cdr_allocation: bool
) -> NewDatabase:
    return NewDatabase(
        scenarios=[
            {
                "model": args.model,
                "pathway": args.pathway,
                "year": args.year,
                "filepath": args.iam_dir,
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


def write_scenario(
    ndb: NewDatabase,
    scenario: dict,
    output_db: str,
) -> None:
    ndb.scenarios = [scenario]
    ndb.write_db_to_brightway(output_db)
    print(f"Wrote {output_db!r} with {len(bd.Database(output_db))} datasets.")


def cdr_markets(database: list[dict]) -> list[dict]:
    return [
        dataset
        for dataset in database
        if dataset.get("name") == CDR_MARKET
        and dataset.get("reference product") == CDR_PRODUCT
        and dataset.get("unit") == "kilogram"
        and dataset.get("location") != "World"
    ]


def dataset_identity(dataset: dict) -> tuple[str, str, str]:
    return (
        dataset.get("name"),
        dataset.get("reference product"),
        dataset.get("location"),
    )


def unique_datasets(datasets: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for dataset in datasets:
        identity = dataset_identity(dataset)
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(dataset)
    return unique


def suppliers_for_market(
    cdr_mapping: dict[str, list[dict]],
    technology: str,
    market: dict,
) -> list[dict]:
    region = market["location"]
    activities = cdr_mapping.get(technology, [])
    suppliers = [dataset for dataset in activities if dataset.get("location") == region]
    return unique_datasets(suppliers)


def force_cdr_market_to_technology(
    scenario: dict, technology: str
) -> tuple[int, list[str]]:
    database = scenario["database"]
    cdr_mapping = scenario.get("mapping", {}).get("cdr", {})
    markets = cdr_markets(database)
    missing_regions = []
    updated = 0

    for market in markets:
        suppliers = suppliers_for_market(cdr_mapping, technology, market)
        if not suppliers:
            missing_regions.append(market["location"])
            continue

        supplier_share = 1.0 / len(suppliers)
        preserved = [
            exchange
            for exchange in market["exchanges"]
            if exchange.get("type") != "technosphere"
        ]
        replacement_exchanges = [
            {
                "name": supplier["name"],
                "product": supplier["reference product"],
                "location": supplier["location"],
                "amount": supplier_share,
                "unit": supplier["unit"],
                "uncertainty type": 0,
                "type": "technosphere",
            }
            for supplier in suppliers
        ]
        market["exchanges"] = preserved + replacement_exchanges
        market["comment"] = (
            market.get("comment", "")
            + f" Forced by dev/build_c400_cdr_technology_databases.py to use "
            f"100% {technology} for CDR compensation."
        )
        updated += 1

    return updated, sorted(missing_regions)


def available_technologies(scenario: dict, requested: list[str] | None) -> list[str]:
    labels = requested or load_cdr_technology_labels()
    cdr_mapping = scenario.get("mapping", {}).get("cdr", {})
    markets = cdr_markets(scenario["database"])

    available = []
    for technology in labels:
        if technology not in cdr_mapping:
            print(f"Skip {technology!r}: not present in scenario CDR mapping.")
            continue
        regions_with_supplier = [
            market["location"]
            for market in markets
            if suppliers_for_market(cdr_mapping, technology, market)
        ]
        if not regions_with_supplier:
            print(f"Skip {technology!r}: no regionalized suppliers in CDR markets.")
            continue
        available.append(technology)

    return available


def main() -> None:
    args = parse_args()
    key = get_key(args)

    extract_c400_iam_csv(args)
    missing_calls = collect_missing_variables(args, key)
    print_missing_variable_warning(missing_calls)

    if args.warn_only:
        return

    bd.projects.set_current(args.project)
    if args.source_db not in bd.databases:
        raise RuntimeError(f"Missing source database {args.source_db!r}.")
    args.biosphere = resolve_biosphere(args.biosphere)

    print()
    print("Building baseline database")
    baseline_name = f"{args.output_prefix}_baseline"
    cdr_market_name = f"{args.output_prefix}_cdr_market"

    baseline_ndb = new_database(args, key, cdr_allocation=False)
    baseline_ndb.update()
    check_outputs_available([baseline_name], args.overwrite)
    baseline_ndb.write_db_to_brightway(baseline_name)
    print(f"Wrote {baseline_name!r} with {len(bd.Database(baseline_name))} datasets.")

    print()
    print("Building CDR allocation database with normal CDR market")
    cdr_ndb = new_database(args, key, cdr_allocation=True)
    cdr_ndb.update()
    base_cdr_scenario = load_database(
        scenario=cdr_ndb.scenarios[0],
        original_database=[],
        load_metadata=True,
        warning=False,
    )

    technologies = available_technologies(base_cdr_scenario, args.technologies)
    technology_outputs = [
        f"{args.output_prefix}_cdr_{slugify(technology)}" for technology in technologies
    ]
    check_outputs_available([cdr_market_name, *technology_outputs], args.overwrite)

    write_scenario(cdr_ndb, copy.deepcopy(base_cdr_scenario), cdr_market_name)

    for technology, output_db in zip(technologies, technology_outputs):
        print()
        print(f"Building forced CDR technology database: {technology}")
        scenario = copy.deepcopy(base_cdr_scenario)
        updated, missing_regions = force_cdr_market_to_technology(scenario, technology)
        if updated == 0:
            print(f"Skip {technology!r}: no CDR markets could be rewritten.")
            continue
        if missing_regions:
            print(
                f"  Warning: {technology!r} has no supplier in "
                f"{len(missing_regions)} CDR market regions: {', '.join(missing_regions)}"
            )
        write_scenario(cdr_ndb, scenario, output_db)


if __name__ == "__main__":
    main()
