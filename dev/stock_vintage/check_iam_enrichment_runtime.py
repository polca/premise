"""Compare real enriched, filtered and encrypted IAM profiles at runtime."""

import argparse
import json
from pathlib import Path

from premise.data_collection import IAMDataCollection
from premise.iam_stock_vintage import verified_report


def load(directory, key=None):
    collection = IAMDataCollection.__new__(IAMDataCollection)
    collection.model = "remind"
    collection.pathway = "SSP2-NPi2025"
    collection.year = 2030
    collection.data = collection._IAMDataCollection__get_iam_data(
        key=key, filedir=directory, variables=[]
    )
    return collection


def check(directory, test_key):
    name = "REMIND_generic_SSP2-NPi2025_stock_vintages_report.json"
    report = json.loads((directory / name).read_text())
    metadata = report["assumptions"]
    baseline = load(directory)
    years = range(metadata["reference_year"], metadata["end_year"] + 1)
    records = {}
    for asset in metadata["assets"]:
        for year in years:
            key = (asset["asset"], asset["region"], year)
            records[key] = baseline.stock_vintage_weights(*key)
    checks = []
    for kind, encryption_key in (("plain", None), ("encrypted", test_key.read_bytes())):
        target = directory / "runtime" / kind
        cold = load(target, encryption_key)
        verified_report(directory / "runtime/plain" / name, cold.iam_source_path)
        for key, expected in records.items():
            if cold.stock_vintage_weights(*key) != expected:
                raise AssertionError(f"{kind} changed runtime cohort weights: {key}")
        warm = load(target, encryption_key)
        # Verify cached reads preserve both values and source/report identity.
        for asset in metadata["assets"]:
            key = (asset["asset"], asset["region"], 2030)
            if warm.stock_vintage_weights(*key) != records[key]:
                raise AssertionError("Cached read changed a stock-vintage profile")
        verified_report(directory / "runtime/plain" / name, warm.iam_source_path)
        checks.append(
            {
                "variant": kind,
                "annual_profiles": len(records),
                "cached_profiles": len(metadata["assets"]),
                "passed": True,
            }
        )
    result = {
        "scenario": "SSP2-NPi2025",
        "all_passed": True,
        "checks": checks,
        "key_note": "Random local test key; never a production IAM key.",
    }
    (directory / "runtime-validation.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(json.dumps(result))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--test-key", type=Path, required=True)
    args = parser.parse_args()
    check(args.directory, args.test_key)
