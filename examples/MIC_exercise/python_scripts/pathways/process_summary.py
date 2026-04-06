#!/usr/bin/env python3
"""
process_summary.py - Process single file or merge results
Usage:
  python process_summary.py file <filename>
  python process_summary.py merge
"""

import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq
import sys
from datetime import datetime

MESSAGE_WORLD_ONLY_VARS = [
    "FE - final energy - Transport - Freight - Int. Shipping - Biofuel",
    "FE - final energy - Transport - Freight - Int. Shipping - Liquid fossil",
    "FE - final energy - Transport - Freight - Int. Shipping - H2",
    "FE - final energy - Transport - Freight - Int. Shipping - LNG",
]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()


def get_regions_for_file(gzip_file):
    """Determine regions based on filename"""
    results_dir = Path("pathways_results")
    gzip_files = sorted(list(results_dir.glob("*.gzip")))

    if "remind" in gzip_file.name.lower():
        remind_files = [f for f in gzip_files if "remind" in f.name.lower()]
        df = pd.read_parquet(remind_files[0], columns=["region"])
    elif "image" in gzip_file.name.lower():
        image_files = [f for f in gzip_files if "image" in f.name.lower()]
        df = pd.read_parquet(image_files[0], columns=["region"])
    elif "message" in gzip_file.name.lower():
        message_files = [f for f in gzip_files if "message" in f.name.lower()]
        df = pd.read_parquet(message_files[0], columns=["region"])
    else:
        return []

    regions = sorted(set(df["region"].unique()) - {"World"})
    del df
    return regions


def extract_variable_category(variable):
    """
    Extract meaningful variable categories:
    - For 'FE - final energy - Buildings - ...': return 'FE - Buildings'
    - For 'SE - cdr - ...': return 'SE - cdr'
    - For 'SE - secondary energy - ...': return 'SE - secondary energy'

    Logic: Skip generic second-level terms like 'final energy', otherwise keep first 2 parts
    """
    parts = variable.split(" - ")

    if len(parts) < 2:
        return parts[0]

    # For FE, skip 'final energy' and take the next meaningful part (Buildings, Transport, etc.)
    if parts[0] == "FE" and len(parts) >= 3 and parts[1].lower() == "final energy":
        # return f"{parts[0]} - {parts[2]}"
        return " - ".join([parts[0]] + parts[2 : min(6, len(parts))])

    # For SE and other cases, take first 2 parts
    # return f"{parts[0]} - {parts[1]}"
    return " - ".join(parts[: min(5, len(parts))])


def process_single_file(filename):
    """Process a single file"""
    gzip_file = Path("pathways_results") / filename
    log(f"Processing {filename}")

    regions = get_regions_for_file(gzip_file)
    log(f"Using {len(regions)} regions")

    results = {}
    parquet_file = pq.ParquetFile(gzip_file)
    total_rows = parquet_file.metadata.num_rows
    log(f"Total rows: {total_rows:,}")

    chunk_num = 0
    rows_processed = 0

    for batch in parquet_file.iter_batches(
        batch_size=5_000_000,
        columns=[
            "model",
            "scenario",
            "year",
            "region",
            "variable",
            "impact_category",
            "value",
        ],
    ):
        chunk_num += 1
        df = batch.to_pandas()
        rows_processed += len(df)

        is_message = "message" in gzip_file.name.lower()
        if is_message:
            # Include World for Int. Shipping variables (no regional resolution in MESSAGE)
            df = df[
                (
                    (df["region"].isin(regions))
                    | (
                        (df["region"] == "World")
                        & (df["variable"].isin(MESSAGE_WORLD_ONLY_VARS))
                    )
                )
                & (df["value"] != 0)
            ]
        else:
            df = df[(df["region"].isin(regions)) & (df["value"] != 0)]

        if len(df) == 0:
            del df
            continue

        df["variable_category"] = df["variable"].apply(extract_variable_category)
        agg = df.groupby(
            ["model", "scenario", "year", "variable_category", "impact_category"]
        )["value"].sum()

        for idx, val in agg.items():
            results[idx] = results.get(idx, 0) + val

        del df, agg

        if chunk_num % 5 == 0:
            pct = (rows_processed / total_rows) * 100
            log(
                f"  Chunk {chunk_num}: {rows_processed:,}/{total_rows:,} ({pct:.0f}%) | Keys: {len(results):,}"
            )

    log(f"Creating DataFrame from {len(results):,} keys...")
    final_summary = pd.DataFrame(
        [
            {
                "model": k[0],
                "scenario": k[1],
                "year": k[2],
                "variable_category": k[3],
                "impact_category": k[4],
                "value": v,
            }
            for k, v in results.items()
        ]
    )

    # Output filename based on input
    output_file = Path("temp_results") / f"{Path(filename).stem}_summary.parquet"
    output_file.parent.mkdir(exist_ok=True)
    final_summary.to_parquet(output_file, index=False)
    log(f"✓ Complete: {len(final_summary):,} rows written to {output_file.name}")


def merge_results():
    """Merge all individual file results"""
    log("=" * 60)
    log("MERGING ALL RESULTS")
    log("=" * 60)

    temp_dir = Path("temp_results")

    if not temp_dir.exists():
        log("ERROR: temp_results directory not found!")
        sys.exit(1)

    parquet_files = sorted(temp_dir.glob("*_summary.parquet"))
    log(f"Found {len(parquet_files)} result files")

    if len(parquet_files) == 0:
        log("ERROR: No result files found!")
        sys.exit(1)

    # Read all and concatenate
    log("Reading all result files...")
    all_dfs = []
    for f in parquet_files:
        df = pd.read_parquet(f)
        log(f"  {f.name}: {len(df):,} rows")
        all_dfs.append(df)

    log("Concatenating...")
    combined = pd.concat(all_dfs, ignore_index=True)
    log(f"Combined: {len(combined):,} rows")

    log("Final aggregation...")
    final = (
        combined.groupby(
            ["model", "scenario", "year", "variable_category", "impact_category"],
            as_index=False,
        )["value"]
        .sum()
        .sort_values(["model", "scenario", "year"])
        .reset_index(drop=True)
    )

    log(f"Final summary: {len(final):,} rows")
    log("Writing output...")
    final.to_parquet("MIC_analysis_summary.parquet", index=False)

    # Cleanup temp files
    log("Cleaning up temp files...")
    import shutil

    shutil.rmtree(temp_dir)

    log("=" * 60)
    log("✓ COMPLETE!")
    log(f"✓ Output: MIC_analysis_summary.parquet ({len(final):,} rows)")
    log("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python process_summary.py [file <filename>|merge]")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "file":
        if len(sys.argv) != 3:
            print("Usage: python process_summary.py file <filename>")
            sys.exit(1)
        process_single_file(sys.argv[2])
    elif mode == "merge":
        merge_results()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
