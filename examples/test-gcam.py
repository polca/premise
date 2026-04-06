from premise import *
import os
import bw2data
import bw2io as bi
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def first_existing_path(candidates):
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"None of the IAM files were found: {candidates}")


def ensure_biosphere_database(name="biosphere3"):
    if name not in bw2data.databases:
        print(f"Creating missing biosphere database: {name}")
        bi.create_default_biosphere3()


def strip_invalid_exchanges(datasets):
    removed = 0

    for dataset in datasets:
        valid_exchanges = []
        for exchange in dataset.get("exchanges", []):
            if "amount" not in exchange or "type" not in exchange:
                removed += 1
                continue

            if exchange["type"] in {"biosphere", "technosphere", "production"} and "input" not in exchange:
                removed += 1
                continue

            valid_exchanges.append(exchange)

        dataset["exchanges"] = valid_exchanges

    return removed

bw2data.projects.set_current('premise')
print(bw2data.databases)

print("="*60)
print("STEP 1: Import and setup ecoinvent 3.11_cutoff database")
print("="*60)

source_db = "ecoinvent-3.11-cutoff"
biosphere_db = "ecoinvent-3.11-biosphere"
# ensure_biosphere_database()

if source_db in bw2data.databases and len(bw2data.Database(source_db)) > 0:
    print(f"Found existing Brightway database: {source_db} ({len(bw2data.Database(source_db))} datasets)")
else:
    username = os.getenv("ECOINVENT_USERNAME")
    password = os.getenv("ECOINVENT_PASSWORD")

    if not username or not password:
        raise ValueError(
            "Missing ecoinvent credentials. Set ECOINVENT_USERNAME and ECOINVENT_PASSWORD environment variables."
        )

    ei = bi.import_ecoinvent_release(
        version="3.11",
        system_model="cutoff",
        username=username,
        password=password,
        use_mp=False,
    )
    ei.apply_strategies()  # fix issues when ecoinvent and Brightway exchange data

print("="*60)
print("STEP 2: Ensure biosphere database")
print("="*60)

ensure_biosphere_database(biosphere_db)

print("="*60)
print("STEP 3: Set up and process GCAM scenario")
print("="*60)

iam_file = first_existing_path(
    [
        ROOT / "gcam" / "output" / "SSP2" / "gcam_SSP2.xlsx",
    ]
)
print(f"Using IAM file: {iam_file}")

ndb = NewDatabase(
    scenarios=[
        {
            "model": "gcam",
            "pathway": "SSP2",
            "year": 2050,
            "filepath": str(iam_file.parent),
        }
    ],
    source_db=source_db,
    source_version="3.11",
    biosphere_name=biosphere_db,
    keep_source_db_uncertainty=False,
    keep_imports_uncertainty=False,
)

print("NewDatabase created successfully")

print("Updating all sectors with GCAM data...")
ndb.update()

print("="*60)
print("STEP 4: Validate and export to Brightway")
print("="*60)

try:
    print("Writing database to Brightway...")
    ndb.write_db_to_brightway()
    print("SUCCESS: GCAM scenario written to Brightway!")
except Exception as e:
    print(f"ERROR during write_db_to_brightway(): {type(e).__name__}: {e}")
    traceback.print_exc()

print("="*60)
print("Premise test completed.")
print("="*60)
