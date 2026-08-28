#!/usr/bin/env python3
"""Score all common non-market activities in the C400 database ladder.

The output table is wide: one row per common activity per database, with one
column per LCIA indicator.
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import os
import time
from pathlib import Path
from typing import Iterable

import bw2calc as bc
import bw2data as bd
import numpy as np

DEFAULT_PROJECT = "ecoinvent-3.12-cutoff"
DEFAULT_OUTPUT_DIR = Path("results/c400_common_non_market_lca")
DEFAULT_COMBINED_OUTPUT = Path("results/c400_common_non_market_lca_scores.csv")
DEFAULT_ACTIVITY_OUTPUT = Path("results/c400_common_non_market_lca_activities.csv")
DEFAULT_METHOD_OUTPUT = Path("results/c400_common_non_market_lca_methods.csv")

DATABASES = [
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2025_baseline",
        "scenario": "2025 baseline",
        "slug": "2025_baseline",
        "order": 1,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_biomass_electricity",
        "scenario": "2060 biomass+electricity",
        "slug": "2060_biomass_electricity",
        "order": 2,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_biomass_electricity_heat",
        "scenario": "2060 + heat",
        "slug": "2060_biomass_electricity_heat",
        "order": 3,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_biomass_electricity_heat_steel",
        "scenario": "2060 + steel",
        "slug": "2060_biomass_electricity_heat_steel",
        "order": 4,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_biomass_electricity_heat_steel_cement",
        "scenario": "2060 + cement",
        "slug": "2060_biomass_electricity_heat_steel_cement",
        "order": 5,
    },
    {
        "database": (
            "ei_cutoff_3.12_image_SSP2_C400_2060_"
            "biomass_electricity_heat_steel_cement_transport"
        ),
        "scenario": "2060 + transport/fuels/battery",
        "slug": "2060_biomass_electricity_heat_steel_cement_transport",
        "order": 6,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_all",
        "scenario": "2060 all updates",
        "slug": "2060_all",
        "order": 7,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_all_cdr_market",
        "scenario": "2060 all + CDR market",
        "slug": "2060_all_cdr_market",
        "order": 8,
    },
    {
        "database": (
            "ei_cutoff_3.12_image_SSP2_C400_2060_"
            "cdr_direct_air_capture_solvent_gas_heat_with_storage"
        ),
        "scenario": "CDR DAC solvent",
        "slug": "2060_cdr_dac_solvent",
        "order": 9,
    },
    {
        "database": (
            "ei_cutoff_3.12_image_SSP2_C400_2060_"
            "cdr_direct_air_capture_sorbent_heat_pump_with_storage"
        ),
        "scenario": "CDR DAC sorbent HP",
        "slug": "2060_cdr_dac_sorbent_hp",
        "order": 10,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_cdr_enhanced_rock_weathering",
        "scenario": "CDR enhanced weathering",
        "slug": "2060_cdr_enhanced_weathering",
        "order": 11,
    },
    {
        "database": (
            "ei_cutoff_3.12_image_SSP2_C400_2060_"
            "cdr_cement_production_non_fossil_co2_with_ccs"
        ),
        "scenario": "CDR cement non-fossil CO2 CCS",
        "slug": "2060_cdr_cement_non_fossil_co2_ccs",
        "order": 12,
    },
    {
        "database": (
            "ei_cutoff_3.12_image_SSP2_C400_2060_"
            "cdr_afforestation_eucalyptus_plantation"
        ),
        "scenario": "CDR afforestation eucalyptus",
        "slug": "2060_cdr_afforestation_eucalyptus",
        "order": 13,
    },
    {
        "database": "ei_cutoff_3.12_image_SSP2_C400_2060_cdr_afforestation_poplar_plantation",
        "scenario": "CDR afforestation poplar",
        "slug": "2060_cdr_afforestation_poplar",
        "order": 14,
    },
]

METHODS = [
    {
        "column": "gwp100_incl_bio_co2",
        "indicator": "GWP100 incl. bio CO2",
        "method": (
            "ecoinvent-3.12",
            "IPCC 2021 (incl. biogenic CO2)",
            "climate change: total (incl. biogenic CO2)",
            "global warming potential (GWP100)",
        ),
    },
    {
        "column": "water_extraction",
        "indicator": "Water extraction",
        "method": (
            "ecoinvent-3.12",
            "Inventory results and indicators",
            "resources",
            "total freshwater extraction",
        ),
    },
    {
        "column": "land_occupation",
        "indicator": "Land occupation",
        "method": (
            "ecoinvent-3.12",
            "Inventory results and indicators",
            "resources",
            "land occupation",
        ),
    },
    {
        "column": "metals_minerals_depletion",
        "indicator": "Metals/minerals depletion",
        "method": (
            "ecoinvent-3.12",
            "EF v3.1",
            "material resources: metals/minerals",
            "abiotic depletion potential (ADP): elements (ultimate reserves)",
        ),
    },
    {
        "column": "primary_energy_demand",
        "indicator": "Primary energy demand",
        "method": (
            "ecoinvent-3.12",
            "Cumulative Energy Demand (CED)",
            "total",
            "energy content (HHV)",
        ),
    },
]

IDENTITY_FIELDS = ["name", "reference_product", "location", "unit"]
NON_MARKET_PREFIXES = ("market for ", "market group for ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--combined-output", type=Path, default=DEFAULT_COMBINED_OUTPUT)
    parser.add_argument("--activity-output", type=Path, default=DEFAULT_ACTIVITY_OUTPUT)
    parser.add_argument("--method-output", type=Path, default=DEFAULT_METHOD_OUTPUT)
    parser.add_argument("--chunk-size", type=int, default=64)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    return parser.parse_args()


def is_non_market(activity) -> bool:
    name = (activity.get("name") or "").lower()
    return not name.startswith(NON_MARKET_PREFIXES)


def identity(activity) -> tuple[str, str, str, str]:
    return (
        activity.get("name") or "",
        activity.get("reference product") or "",
        activity.get("location") or "",
        activity.get("unit") or "",
    )


def assert_inputs() -> None:
    missing_databases = [
        row["database"] for row in DATABASES if row["database"] not in bd.databases
    ]
    if missing_databases:
        raise ValueError(
            "Missing Brightway databases:\n" + "\n".join(missing_databases)
        )

    missing_methods = [
        row["method"] for row in METHODS if row["method"] not in bd.methods
    ]
    if missing_methods:
        raise ValueError(
            "Missing LCIA methods:\n"
            + "\n".join(repr(method) for method in missing_methods)
        )


def activity_map(database_name: str) -> dict[tuple[str, str, str, str], object]:
    mapping = {}
    duplicates = set()
    for activity in bd.Database(database_name):
        if not is_non_market(activity):
            continue
        key = identity(activity)
        if key in mapping:
            duplicates.add(key)
        mapping[key] = activity

    if duplicates:
        raise ValueError(
            f"{database_name!r} has duplicate non-market identities: {len(duplicates)}"
        )
    return mapping


def common_identities(limit: int | None = None) -> list[tuple[str, str, str, str]]:
    common = None
    counts = []
    for row in DATABASES:
        mapping = activity_map(str(row["database"]))
        keys = set(mapping)
        common = keys if common is None else common & keys
        counts.append(
            {
                "database_order": row["order"],
                "database": row["database"],
                "non_market_unique_activities": len(keys),
            }
        )

    identities = sorted(common or set())
    if limit is not None:
        identities = identities[:limit]
    print(
        f"Common non-market activity identities: {len(identities)}"
        + (f" (limited to first {limit})" if limit is not None else ""),
        flush=True,
    )
    for row in counts:
        print(
            f"  {row['database_order']:02d}: {row['non_market_unique_activities']} in "
            f"{row['database']}",
            flush=True,
        )
    return identities


def write_activity_metadata(
    identities: Iterable[tuple[str, str, str, str]], output_path: Path
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["activity_index", *IDENTITY_FIELDS])
        writer.writeheader()
        for index, key in enumerate(identities, start=1):
            writer.writerow(
                {
                    "activity_index": index,
                    "name": key[0],
                    "reference_product": key[1],
                    "location": key[2],
                    "unit": key[3],
                }
            )


def write_method_metadata(output_path: Path) -> list[dict[str, object]]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for method_index, row in enumerate(METHODS, start=1):
        unit = bd.Method(row["method"]).metadata.get("unit") or ""
        rows.append(
            {
                "method_index": method_index,
                "column": row["column"],
                "indicator": row["indicator"],
                "method": " | ".join(row["method"]),
                "unit": unit,
            }
        )
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["method_index", "column", "indicator", "method", "unit"],
        )
        writer.writeheader()
        writer.writerows(rows)
    return rows


def output_path_for_database(output_dir: Path, db_row: dict[str, object]) -> Path:
    return output_dir / f"{int(db_row['order']):02d}_{db_row['slug']}_scores.csv"


def done_path_for_database(output_dir: Path, db_row: dict[str, object]) -> Path:
    return output_dir / f"{int(db_row['order']):02d}_{db_row['slug']}.done.json"


def precalculate_lcia_rows(lca: bc.LCA) -> np.ndarray:
    rows = []
    for method in [row["method"] for row in METHODS]:
        lca.switch_method(method)
        characterized_biosphere = lca.characterization_matrix @ lca.biosphere_matrix
        rows.append(np.asarray(characterized_biosphere.sum(axis=0)).ravel())
    return np.vstack(rows)


def score_database(
    db_row: dict[str, object],
    identities: list[tuple[str, str, str, str]],
    output_dir: Path,
    chunk_size: int,
    resume: bool,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_path_for_database(output_dir, db_row)
    done_path = done_path_for_database(output_dir, db_row)
    tmp_path = output_path.with_suffix(".csv.tmp")

    if resume and output_path.exists() and done_path.exists():
        print(f"Skipping completed {db_row['database']}", flush=True)
        return output_path

    mapping = activity_map(str(db_row["database"]))
    activities = [mapping[key] for key in identities]
    first_activity = activities[0]

    print(
        f"Scoring {db_row['order']:02d} {db_row['database']} "
        f"({len(activities)} activities)",
        flush=True,
    )
    started = time.time()
    lca = bc.LCA({first_activity.id: 1.0}, METHODS[0]["method"])
    lca.lci(factorize=True)
    lcia_rows = precalculate_lcia_rows(lca)

    n_products = len(lca.dicts.product)
    demand = np.zeros(n_products)
    supply_chunk = np.zeros((n_products, chunk_size))
    product_indices = [lca.dicts.product[activity.id] for activity in activities]

    fieldnames = [
        "activity_index",
        "database_order",
        "scenario",
        "database",
        "activity_id",
        "activity_key",
        "activity_code",
        *IDENTITY_FIELDS,
        *[row["column"] for row in METHODS],
    ]

    with tmp_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for start in range(0, len(activities), chunk_size):
            stop = min(start + chunk_size, len(activities))
            current_size = stop - start
            if supply_chunk.shape[1] != current_size:
                supply_block = np.zeros((n_products, current_size))
            else:
                supply_block = supply_chunk

            for column, product_index in enumerate(product_indices[start:stop]):
                demand[product_index] = 1.0
                supply_block[:, column] = lca.solver(demand)
                demand[product_index] = 0.0

            scores = lcia_rows @ supply_block[:, :current_size]
            rows = []
            for offset, activity in enumerate(activities[start:stop]):
                key = identities[start + offset]
                row = {
                    "activity_index": start + offset + 1,
                    "database_order": db_row["order"],
                    "scenario": db_row["scenario"],
                    "database": db_row["database"],
                    "activity_id": activity.id,
                    "activity_key": repr(activity.key),
                    "activity_code": activity.get("code") or "",
                    "name": key[0],
                    "reference_product": key[1],
                    "location": key[2],
                    "unit": key[3],
                }
                for method_offset, method_row in enumerate(METHODS):
                    row[method_row["column"]] = float(scores[method_offset, offset])
                rows.append(row)
            writer.writerows(rows)

            if stop == len(activities) or stop % 512 == 0:
                elapsed = time.time() - started
                rate = stop / elapsed if elapsed else 0.0
                remaining = (len(activities) - stop) / rate if rate else 0.0
                print(
                    f"  {db_row['order']:02d}: {stop}/{len(activities)} "
                    f"({rate:.1f} activities/s, {remaining / 60:.1f} min remaining)",
                    flush=True,
                )

    os.replace(tmp_path, output_path)
    elapsed = time.time() - started
    done_path.write_text(
        json.dumps(
            {
                "database": db_row["database"],
                "scenario": db_row["scenario"],
                "database_order": db_row["order"],
                "activities": len(activities),
                "seconds": elapsed,
                "output": str(output_path),
            },
            indent=2,
        )
        + "\n"
    )
    print(
        f"Finished {db_row['order']:02d} in {elapsed / 60:.1f} min: {output_path}",
        flush=True,
    )
    return output_path


def combine_outputs(paths: list[Path], combined_output: Path) -> None:
    combined_output.parent.mkdir(parents=True, exist_ok=True)
    with combined_output.open("w", newline="") as out_handle:
        writer = None
        for path in paths:
            with path.open("r", newline="") as in_handle:
                reader = csv.DictReader(in_handle)
                if writer is None:
                    writer = csv.DictWriter(out_handle, fieldnames=reader.fieldnames)
                    writer.writeheader()
                for row in reader:
                    writer.writerow(row)
    print(f"Wrote combined scores: {combined_output}", flush=True)


def score_database_worker(
    project: str,
    db_row: dict[str, object],
    identities: list[tuple[str, str, str, str]],
    output_dir: Path,
    chunk_size: int,
    resume: bool,
) -> tuple[int, Path]:
    bd.projects.set_current(project)
    path = score_database(
        db_row=db_row,
        identities=identities,
        output_dir=output_dir,
        chunk_size=chunk_size,
        resume=resume,
    )
    return int(db_row["order"]), path


def main() -> None:
    args = parse_args()
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")
    if args.workers <= 0:
        raise ValueError("--workers must be positive")

    bd.projects.set_current(args.project)
    print(f"Brightway project: {bd.projects.current}", flush=True)
    assert_inputs()

    identities = common_identities(limit=args.limit)
    write_activity_metadata(identities, args.activity_output)
    write_method_metadata(args.method_output)

    output_by_order = {}
    if args.workers == 1:
        for db_row in DATABASES:
            output_by_order[int(db_row["order"])] = score_database(
                db_row=db_row,
                identities=identities,
                output_dir=args.output_dir,
                chunk_size=args.chunk_size,
                resume=args.resume,
            )
    else:
        print(f"Using {args.workers} database workers", flush=True)
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    score_database_worker,
                    args.project,
                    db_row,
                    identities,
                    args.output_dir,
                    args.chunk_size,
                    args.resume,
                )
                for db_row in DATABASES
            ]
            for future in as_completed(futures):
                order, path = future.result()
                output_by_order[order] = path

    output_paths = [output_by_order[int(row["order"])] for row in DATABASES]

    combine_outputs(output_paths, args.combined_output)
    print(f"Wrote activity metadata: {args.activity_output}", flush=True)
    print(f"Wrote method metadata: {args.method_output}", flush=True)


if __name__ == "__main__":
    main()
