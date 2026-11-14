import csv
from pathlib import Path

import yaml

from premise.lifecycle_timing import (
    load_lifecycle_timing,
    load_lifecycle_timing_rows,
)
from premise.temporal_legacy import (
    LEGACY_COLUMNS,
    compile_legacy_rows,
    load_legacy_registry,
    write_legacy_csv,
)
from premise.trails import TrailsDataPackage

REGISTRY = Path("premise/data/trails/temporal")
CSV = Path("premise/data/trails/temporal_distributions.csv")


def test_packaged_legacy_registry_is_valid_and_unambiguous():
    profiles, mappings = load_legacy_registry(REGISTRY)

    assert profiles
    assert mappings
    identities = [
        tuple(dataset) for mapping in mappings for dataset in mapping["datasets"]
    ]
    assert len(identities) == len(set(identities)) == 9293


def test_committed_csv_is_deterministically_generated(tmp_path):
    generated = tmp_path / "temporal_distributions.csv"
    write_legacy_csv(REGISTRY, generated)

    assert generated.read_bytes() == CSV.read_bytes()


def test_compiled_rows_keep_the_compatibility_contract():
    rows = compile_legacy_rows(REGISTRY)

    assert tuple(rows[0]) == LEGACY_COLUMNS
    assert all(row["name"] and row["reference product"] for row in rows)
    assert all(row["timing_reference"] for row in rows)


def test_conflict_ledger_records_explicit_resolutions():
    with (REGISTRY / "legacy_conflicts.yaml").open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream)

    conflicts = document["conflicts"]
    assert len(conflicts) == 378
    assert all(item["source_rows"] for item in conflicts)
    assert all(item["chosen_source_row"] in item["source_rows"] for item in conflicts)
    assert all(item["resolution"] for item in conflicts)
    assert any("field_resolutions" in item for item in conflicts)


def test_generated_csv_has_no_duplicate_dataset_identities():
    with CSV.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))

    identities = [(row["name"], row["reference product"]) for row in rows]
    assert len(identities) == len(set(identities))


def test_direct_yaml_runtime_matches_the_compatibility_csv(monkeypatch):
    rows = compile_legacy_rows(REGISTRY)
    trails = TrailsDataPackage.__new__(TrailsDataPackage)

    assert trails._load_temporal_specs(rows) == trails._load_temporal_specs_from_csv(
        CSV
    )
    assert load_lifecycle_timing_rows(rows) == load_lifecycle_timing(CSV)
    assert TrailsDataPackage._load_temporal_classifications_from_rows(
        rows
    ) == TrailsDataPackage._load_temporal_classifications(CSV)

    def fail_on_csv_access(*_):
        raise AssertionError("CSV runtime access")

    monkeypatch.setattr(
        TrailsDataPackage, "_load_temporal_specs_from_csv", fail_on_csv_access
    )
    direct = TrailsDataPackage.__new__(TrailsDataPackage)
    direct._load_canonical_temporal_data()
    assert direct.dataset_lifetimes == trails._load_temporal_specs(rows)[-1]
    assert direct.lifecycle_timing == load_lifecycle_timing_rows(rows)
