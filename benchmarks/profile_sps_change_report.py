"""Reproduce the four-year SPS report without writing Brightway databases.

Run with the premise Python environment. Reads PREMISE_KEY or IAM_FILES_KEY
from the environment or repository .env. Results contain proprietary inventory
data and belong in the ignored export directory. --profile adds cProfile
overhead; omit it to measure normal wall time. --capture saves trusted-local
inputs for replay with benchmarks/profile_change_report.py.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import ExitStack
import cProfile
from datetime import datetime
from functools import wraps
import json
import os
from pathlib import Path
import pickle
import pstats
import resource
import shutil
import subprocess
import sys
import threading
import time
import traceback
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def capture_checkpoint(store, destination, original=None):
    """Preserve the configured checkpoint backend, not its reporting facade."""
    if original is not None:
        original = Path(original)
        manifest = json.loads((original / "manifest.json").read_text())
        if manifest.get("columnar_format") not in {
            "scenario-delta-v1",
            "scenario-delta-v2",
        }:
            return shutil.copytree(original, destination)
    # Expand sparse overlays to keep the fixture independent of cache paths.
    return store.checkpoint(destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datapackage",
        type=Path,
        default=Path(
            "/Users/romain/Github/sweet_sure-2050-switzerland/datapackage.json"
        ),
    )
    parser.add_argument(
        "--years", type=int, nargs="+", default=[2020, 2030, 2040, 2050]
    )
    parser.add_argument("--scenario", default="SPS1_bas0")
    parser.add_argument("--model", default="remind")
    parser.add_argument("--pathway", default="SSP2-PkBudg1000")
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-version", default="3.12")
    parser.add_argument("--biosphere", default="biosphere")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT
        / "export"
        / ("sps-report-profile-" + datetime.now().strftime("%Y%m%d-%H%M%S")),
    )
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--capture", action="store_true")
    parser.add_argument(
        "--capture-only",
        action="store_true",
        help="Save report inputs after update, without generating reports",
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    import bw2data as bd
    from datapackage import Package
    from dotenv import load_dotenv
    import premise
    import premise.change_report as report

    load_dotenv(ROOT / ".env")
    key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
    if not key:
        parser.error("Set PREMISE_KEY or IAM_FILES_KEY in the environment or .env")
    metrics = {
        "premise_file": premise.__file__,
        "python": sys.executable,
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "years": args.years,
        "profiled": args.profile,
        "keep_imports_uncertainty": True,
        "keep_source_db_uncertainty": True,
        "inventory_backend": "compact",
        "stages": [],
    }

    def save():
        (args.output / "metrics.json").write_text(json.dumps(metrics, indent=2))

    def timed(label, function, *a, **kw):
        print(f"START {label}", flush=True)
        start = time.perf_counter()
        try:
            return function(*a, **kw)
        finally:
            elapsed = time.perf_counter() - start
            metrics["stages"].append({"stage": label, "seconds": elapsed})
            save()
            print(f"END {label}: {elapsed:.2f} s", flush=True)

    print(json.dumps(metrics, indent=2), flush=True)
    bd.projects.set_current(args.project)
    package = Package(str(args.datapackage))
    ndb = timed(
        "NewDatabase",
        premise.NewDatabase,
        scenarios=[
            {
                "model": args.model,
                "pathway": args.pathway,
                "year": year,
                "external scenarios": [{"scenario": args.scenario, "data": package}],
            }
            for year in args.years
        ],
        source_db=args.source_db,
        source_version=args.source_version,
        biosphere_name=args.biosphere,
        key=key,
        keep_imports_uncertainty=True,
        keep_source_db_uncertainty=True,
        generate_reports=False,
    )
    timed("update", ndb.update)

    if args.capture or args.capture_only:
        fixture = args.output / "fixture"
        fixture.mkdir()
        timed(
            "checkpoint source",
            capture_checkpoint,
            ndb._report_source_store(),
            fixture / "source.inventory-store",
            original=getattr(ndb, "_compact_source_checkpoint", None),
        )
        scenarios = []
        for i, scenario in enumerate(ndb._report_scenarios()):
            timed(
                f"checkpoint scenario {i}",
                capture_checkpoint,
                scenario.store,
                fixture / f"final-{i}.inventory-store",
                original=(scenario.definition or {}).get("_inventory_checkpoint"),
            )
            scenarios.append(
                dict(
                    identity=scenario.identity,
                    validation_report=scenario.validation_report.to_dict(),
                    provenance_payload=scenario.provenance_payload,
                    definition={
                        "_validation_intents": (scenario.definition or {}).get(
                            "_validation_intents", {}
                        )
                    },
                )
            )
        metadata = dict(
            scenarios=scenarios,
            build_id=ndb.build_id,
            source_fingerprint=ndb._validation_source_fingerprint(),
            source_database=ndb.source,
            source_type=ndb.source_type,
            version=ndb.version,
            system_model=ndb.system_model,
            premise_version=".".join(map(str, premise.__version__)),
        )
        with (fixture / "metadata.pickle").open("wb") as f:
            pickle.dump(metadata, f)
        # Restore the same cold scenario-loading path as update() leaves behind.
        for definition in ndb.scenarios:
            definition.pop("_inventory_store", None)
        if args.capture_only:
            print(f"Captured: {fixture}", flush=True)
            return

    # Measure the public entry point, including certification and store loading.
    # Wrappers are local to this process and restored after each report run.
    def run_report(label, profile=False):
        totals = defaultdict(lambda: {"calls": 0, "seconds": 0.0})
        index_count = 0

        def wrapper(name, function, verbose=False):
            @wraps(function)
            def measured(*a, **kw):
                nonlocal index_count
                stage = name
                if name == "_activity_index":
                    stage += (
                        " source" if index_count == 0 else f" scenario {index_count}"
                    )
                    index_count += 1
                if verbose:
                    print(f"  START {stage}", flush=True)
                start = time.perf_counter()
                try:
                    result = function(*a, **kw)
                    if name == "_activity_index":
                        totals[stage]["activities"] = len(result[0])
                        totals[stage]["exchanges"] = result[1]
                        totals[stage]["hash_skip_safe_activities"] = sum(
                            locator.hash_skip_safe for locator in result[0]
                        )
                    return result
                finally:
                    elapsed = time.perf_counter() - start
                    totals[stage]["calls"] += 1
                    totals[stage]["seconds"] += elapsed
                    if verbose:
                        metrics[label] = dict(totals)
                        save()
                        print(f"  END {stage}: {elapsed:.2f} s", flush=True)

            return measured

        profiler = cProfile.Profile() if profile else None
        with ExitStack() as stack:
            for name in (
                "_report_scenarios",
                "_report_source_store",
                "_validation_source_fingerprint",
            ):
                stack.enter_context(
                    patch.object(ndb, name, wrapper(name, getattr(ndb, name), True))
                )
            for name in (
                "_activity_index",
                "_generate_details",
                "_write_workbook",
                "_activity_records",
                "_collect_event_summaries",
            ):
                stack.enter_context(
                    patch.object(
                        report,
                        name,
                        wrapper(
                            name, getattr(report, name), name != "_activity_records"
                        ),
                    )
                )
            stack.enter_context(
                patch.object(
                    report._ParquetSink,
                    "write",
                    wrapper("parquet_write", report._ParquetSink.write),
                )
            )
            # Sample Python frames while holding the GIL. Native faulthandler
            # sampling hung in traceback cleanup on this Python 3.11 runtime.
            with (args.output / f"{label}-stacks.txt").open("w") as stacks:
                stopped = threading.Event()
                main_thread = threading.get_ident()

                def sample():
                    while not stopped.wait(60):
                        frame = sys._current_frames().get(main_thread)
                        if frame is not None:
                            traceback.print_stack(frame, file=stacks)
                            stacks.flush()
                        del frame

                sampler = threading.Thread(target=sample, daemon=True)
                sampler.start()
                if profiler:
                    profiler.enable()
                try:
                    artifacts = timed(
                        label,
                        ndb.generate_change_report,
                        filepath=args.output,
                        name=label,
                    )
                finally:
                    if profiler:
                        profiler.disable()
                        profiler.dump_stats(str(args.output / f"{label}.pstats"))
                        with (args.output / f"{label}-profile.txt").open("w") as f:
                            stats = pstats.Stats(profiler, stream=f)
                            stats.sort_stats("cumulative").print_stats(60)
                            stats.sort_stats("tottime").print_stats(40)
                    stopped.set()
                    sampler.join()
                    metrics[label] = dict(totals)
                    save()
        return artifacts

    first = run_report("cold-report", args.profile)
    import pyarrow.parquet as pq

    metrics["audit_rows"] = pq.ParquetFile(first.details_path).metadata.num_rows
    metrics["audit_bytes"] = first.details_path.stat().st_size
    second = run_report("cached-report")
    metrics["cache_reused"] = first.details_path == second.details_path
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    metrics["peak_rss_bytes"] = rss if sys.platform == "darwin" else rss * 1024
    save()

    print(f"Results: {args.output}", flush=True)


if __name__ == "__main__":
    main()
