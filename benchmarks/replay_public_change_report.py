"""Time the public report path, including loading saved checkpoint inputs.

Use a trusted-local fixture captured by profile_sps_change_report.py or
profile_change_report.py. No scenario rebuild or Brightway database write occurs.
Baseline module paths must be preserved copies from the same checkout before
editing. Generated metrics work with compare_change_reports.py.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import gc
import importlib.util
import json
import os
from pathlib import Path
import pickle
import resource
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def main():
    # The existing market workbook sums a set of supplier keys. Fix its order
    # across fresh processes so last-bit float roundoff cannot spoil an exact
    # workbook comparison. Honor an explicitly selected seed.
    if "PYTHONHASHSEED" not in os.environ:
        os.execve(
            sys.executable,
            [sys.executable, *sys.argv],
            {**os.environ, "PYTHONHASHSEED": "0"},
        )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-report", type=Path)
    parser.add_argument("--baseline-database", type=Path)
    args = parser.parse_args()
    if bool(args.baseline_report) != bool(args.baseline_database):
        parser.error("Supply both baseline module paths, or neither")
    import premise.change_report as report_module
    import premise.new_database as database_module
    from premise.validation_framework import ValidationReport

    if args.baseline_report:
        report_module = load_module(args.baseline_report, "premise._before_sps_report")
        database_module = load_module(
            args.baseline_database, "premise._before_sps_database"
        )
        database_module.generate_structured_change_report = (
            report_module.generate_structured_change_report
        )

    with (args.fixture / "metadata.pickle").open("rb") as stream:
        metadata = pickle.load(stream)
    database = object.__new__(database_module.NewDatabase)
    database.scenarios = []
    database._validation_enabled = True
    database._validation_reports = {}
    database._source_inventory_store = None
    database._compact_source_checkpoint = args.fixture / "source.inventory-store"
    database._change_report_cache = None
    database.generate_reports = False
    source_manifest = json.loads(
        (args.fixture / "source.inventory-store" / "manifest.json").read_text()
    )
    database.inventory_backend = source_manifest["backend"]
    database.build_id = metadata["build_id"]
    database.source = metadata["source_database"]
    for field in ("source_type", "version", "system_model"):
        setattr(database, field, metadata[field])
    database._validation_source_fingerprint = lambda: metadata["source_fingerprint"]
    for i, entry in enumerate(metadata["scenarios"]):
        model, pathway, year, external = entry["identity"]
        definition = dict(entry.get("definition") or {})
        definition.update(
            {
                "model": model,
                "pathway": pathway,
                "year": year,
                "external scenarios": [{"scenario": name} for name in external],
                "applied functions": ["replay"],
                "_inventory_checkpoint": args.fixture / f"final-{i}.inventory-store",
                "_validation_report": entry["validation_report"],
                "_provenance": entry["provenance_payload"],
            }
        )
        database.scenarios.append(definition)
        validation = ValidationReport.from_dict(entry["validation_report"])
        database._validation_reports[database._scenario_identity(definition)] = (
            validation
        )

    args.output.mkdir(parents=True, exist_ok=False)
    phases = []

    def timed(name, function):
        def call(*a, **kw):
            print(f"START {name}", flush=True)
            start = time.perf_counter()
            try:
                return function(*a, **kw)
            finally:
                seconds = time.perf_counter() - start
                phases.append({"phase": name, "seconds": seconds})
                (args.output / "phases.json").write_text(json.dumps(phases, indent=2))
                print(f"END {name}: {seconds:.2f} s", flush=True)

        return call

    gc.collect()
    with ExitStack() as stack:
        for name in ("_report_scenarios", "_report_source_store"):
            stack.enter_context(
                patch.object(database, name, timed(name, getattr(database, name)))
            )
        for name in ("_generate_details", "_write_workbook"):
            stack.enter_context(
                patch.object(
                    report_module, name, timed(name, getattr(report_module, name))
                )
            )
        start = time.perf_counter()
        artifacts = database.generate_change_report(filepath=args.output)
        seconds = time.perf_counter() - start
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        metrics = dict(
            seconds=seconds,
            hash_seed=os.environ["PYTHONHASHSEED"],
            phases=list(phases),
            peak_rss_bytes=rss if sys.platform == "darwin" else rss * 1024,
            details=str(artifacts.details_path),
            workbook=str(artifacts.workbook_path),
        )
        (args.output / "metrics.json").write_text(json.dumps(metrics, indent=2))
        start = time.perf_counter()
        cached = database.generate_change_report(filepath=args.output)
        metrics["cached_seconds"] = time.perf_counter() - start
        metrics["cache_reused"] = cached.details_path == artifacts.details_path
        (args.output / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2), flush=True)


if __name__ == "__main__":
    main()
