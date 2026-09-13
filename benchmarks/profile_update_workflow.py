"""Time complete all-sector updates and capture their outputs for comparison.

Run with PREMISE_KEY or IAM_FILES_KEY, a fixed PYTHONHASHSEED, and a fresh local
output directory. --profile profiles update only. --export measures the normal
SimaPro/report path too. Optional snapshot capture runs after all timed work.
"""

from __future__ import annotations

import argparse
import cProfile
from copy import deepcopy
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import pickle
import platform
import pstats
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.profile_simapro_workflow import Recorder, instrument


def capture(ndb, output):
    from benchmarks.compare_build_outputs import (
        canonicalize,
        canonical_exchange,
        canonical_json,
        IGNORED_DATASET_FIELDS,
        write_semantic_snapshot,
    )
    from premise.inventory_store import InventoryStore

    directory = output / "snapshots"
    directory.mkdir()
    manifests = []
    for index, scenario in enumerate(ndb.scenarios):
        checkpoint = Path(scenario["_inventory_checkpoint"])
        saved = directory / f"scenario-{index}.inventory-store"
        shutil.copytree(checkpoint, saved)
        store = InventoryStore.open_for_reporting(saved)
        ordered = hashlib.sha256()

        def activities():
            for activity_id in store.iter_activity_ids():
                dataset = store._report_activity_payload(activity_id)
                payload = {
                    key: value
                    for key, value in dataset.items()
                    if key not in IGNORED_DATASET_FIELDS and key != "exchanges"
                }
                payload["exchanges"] = [
                    canonical_exchange(exchange)
                    for exchange in dataset.get("exchanges", ())
                ]
                ordered.update(canonical_json(canonicalize(payload)).encode())
                ordered.update(b"\n")
                yield dataset

        summary = write_semantic_snapshot(
            activities(), directory / f"scenario-{index}", write_details=False
        )
        identity = ndb._scenario_identity(scenario)
        validation = ndb._validation_reports.get(identity)
        metadata = {
            "identity": identity,
            "applied_functions": scenario.get("applied functions"),
            "validation": validation.to_dict() if validation else None,
            "provenance": scenario.get("_provenance"),
            "heat_diagnostics": scenario.get("heat diagnostics"),
            "validation_intents": scenario.get("_validation_intents"),
            "build_id": ndb.build_id,
        }
        with (directory / f"scenario-{index}-metadata.pickle").open("wb") as stream:
            pickle.dump(metadata, stream, protocol=pickle.HIGHEST_PROTOCOL)
        manifests.append(
            {
                "identity": identity,
                "activities": summary["dataset_count"],
                "exchanges": summary["exchange_count"],
                "semantic_sha256": summary["semantic_sha256"],
                "ordered_sha256": ordered.hexdigest(),
            }
        )
        print("CAPTURE", json.dumps(manifests[-1]), flush=True)
    (directory / "manifest.json").write_text(json.dumps(manifests, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--scenario", action="append", metavar="MODEL:PATHWAY:YEAR")
    parser.add_argument(
        "--inventory-backend", choices=("legacy", "compact"), default="legacy"
    )
    args = parser.parse_args()
    specs = args.scenario or [
        "image:SSP2-M:2030",
        "image:SSP2-M:2040",
        "image:SSP2-M:2050",
        "image:SSP2-L:2030",
    ]
    scenarios = []
    for spec in specs:
        model, pathway, year = spec.split(":")
        scenarios.append({"model": model, "pathway": pathway, "year": int(year)})
    key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
    if not key:
        raise RuntimeError("Set PREMISE_KEY or IAM_FILES_KEY")
    import bw2data as bd
    from premise import NewDatabase

    bd.projects.set_current("ecoinvent-3.12-cutoff")
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    configuration = {
        "scenarios": scenarios,
        "inventory_backend": args.inventory_backend,
        "source": "ecoinvent-3.12-cutoff",
        "biosphere": "ecoinvent-3.12-biosphere",
        "keep_imports_uncertainty": True,
        "keep_source_db_uncertainty": False,
        "persist": True,
        "generate_reports": True,
        "profile_update": args.profile,
        "export": args.export,
        "capture": args.capture,
        "python": sys.version,
        "platform": platform.platform(),
        "python_hash_seed": os.environ.get("PYTHONHASHSEED"),
        "revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "working_tree": subprocess.check_output(
            ["git", "status", "--short"], text=True
        ),
    }
    (output / "configuration.json").write_text(json.dumps(configuration, indent=2))
    recorder = Recorder(output, "none")
    profiler = cProfile.Profile() if args.profile else None
    status = "failed"
    try:
        with ExitStack() as patches:
            instrument(recorder, patches)
            with recorder.span("NewDatabase", "constructor"):
                ndb = NewDatabase(
                    scenarios=deepcopy(scenarios),
                    source_db="ecoinvent-3.12-cutoff",
                    source_version="3.12",
                    biosphere_name="ecoinvent-3.12-biosphere",
                    key=key,
                    inventory_backend=args.inventory_backend,
                    keep_imports_uncertainty=True,
                    keep_source_db_uncertainty=False,
                    generate_reports=True,
                    quiet=True,
                )
            with recorder.span("ndb.update()", "update_overhead"):
                if profiler:
                    profiler.enable()
                try:
                    ndb.update()
                finally:
                    if profiler:
                        profiler.disable()
            if args.export:
                with recorder.span("ndb.write_db_to_simapro()", "export_overhead"):
                    ndb.write_db_to_simapro(filepath=str(output / "simapro"))
            if any(event["status"] != "passed" for event in recorder.events):
                raise RuntimeError("A workflow stage failed; inspect events.jsonl")
            status = "passed"
    finally:
        if profiler:
            profiler.dump_stats(str(output / "update.pstats"))
            with (output / "hotspots.txt").open("w") as stream:
                stats = pstats.Stats(profiler, stream=stream)
                stats.sort_stats("cumulative").print_stats(100)
                stats.sort_stats("tottime").print_stats(60)
        recorder.save(configuration, status)
    if args.capture:
        started = time.perf_counter()
        capture(ndb, output)
        (output / "capture-seconds.txt").write_text(str(time.perf_counter() - started))


if __name__ == "__main__":
    if os.environ.get("PYTHONHASHSEED") != "0":
        os.environ["PYTHONHASHSEED"] = "0"
        os.execv(sys.executable, [sys.executable, *sys.argv])
    main()
