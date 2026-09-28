"""Profile four full NewDatabase updates and SimaPro exports, including reports.

Set PREMISE_KEY (or IAM_FILES_KEY); an optional repository .env is supported.
Run with --profile none for ordinary wall timings, or focused (default) for
cProfile inside validation and reporting only. Profiling adds overhead.
Outputs include proprietary inventories: keep the output directory local.
"""

from __future__ import annotations

import argparse
import cProfile
from copy import deepcopy
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
import json
import os
from pathlib import Path
import platform
import pstats
import resource
import subprocess
import sys
import time
from unittest.mock import patch
from contextlib import ExitStack

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks._process_tree_memory import ProcessTreeMemory


class Recorder:
    """Nested inclusive/exclusive wall times; never force GC or rerun validation."""

    def __init__(self, output, mode):
        self.output = output
        self.mode = mode
        self.profiler = cProfile.Profile()
        self.profile_depth = 0
        self.stack = []
        self.events = []
        self.started = time.perf_counter()
        self.memory = ProcessTreeMemory().start()

    @contextmanager
    def span(self, name, category, scenario=None):
        record = {
            "name": name,
            "category": category,
            "scenario": scenario,
            "parent": self.stack[-1]["name"] if self.stack else None,
            "depth": len(self.stack),
            "child_seconds": 0.0,
            "start_seconds": time.perf_counter() - self.started,
            "status": "running",
        }
        profiling = self.mode == "all" or (
            self.mode == "focused"
            and category in {"validation", "change_report", "scenario_report"}
        )
        if profiling:
            if not self.profile_depth:
                self.profiler.enable()
            self.profile_depth += 1
        self.stack.append(record)
        start = time.perf_counter()
        print(f"PROFILE START {name} {scenario or ''}", flush=True)
        try:
            yield record
            record["status"] = "passed"
        except BaseException as error:
            record["status"] = "failed"
            record["error_type"] = type(error).__name__
            raise
        finally:
            elapsed = time.perf_counter() - start
            if profiling:
                self.profile_depth -= 1
                if not self.profile_depth:
                    self.profiler.disable()
            self.stack.pop()
            record["wall_seconds"] = elapsed
            record["exclusive_seconds"] = elapsed - record.pop("child_seconds")
            if self.stack:
                self.stack[-1]["child_seconds"] += elapsed
            self.events.append(record)
            with (self.output / "events.jsonl").open("a") as stream:
                stream.write(json.dumps(record) + "\n")
            print(f"PROFILE END {name}: {elapsed:.3f}s", flush=True)

    def wrap(self, owner, name, category, patches, *, report_path=False):
        original = getattr(owner, name)
        label = f"{owner.__name__}.{name}"

        @wraps(original)
        def wrapped(*args, **kwargs):
            scenario = next(
                (a for a in args if isinstance(a, dict) and "pathway" in a), None
            )
            scenario = scenario or kwargs.get("scenario")
            identity = None
            if isinstance(scenario, dict):
                identity = {
                    key: scenario.get(key) for key in ("model", "pathway", "year")
                }
            if report_path:
                kwargs.setdefault("filepath", self.output / "reports")
            with self.span(label, category, identity) as record:
                result = original(*args, **kwargs)
                if hasattr(result, "reused"):
                    record["certificate_reused"] = result.reused
                if name == "export_db_to_simapro":
                    record["unmatched_flow_categories"] = len(
                        args[0].unmatched_category_flows
                    )
                return result

        patches.enter_context(patch.object(owner, name, wrapped))

    def save(self, metadata, status):
        totals = defaultdict(float)
        for event in self.events:
            totals[event["category"]] += event["exclusive_seconds"]
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform != "darwin":
            peak *= 1024
        result = {
            "status": status,
            "configuration": metadata,
            "wall_seconds": time.perf_counter() - self.started,
            "peak_rss_bytes": peak,
            "peak_process_tree_rss_bytes": self.memory.stop(),
            "exclusive_category_seconds": dict(totals),
            "events": self.events,
            "artifacts": [
                str(p.relative_to(self.output))
                for p in self.output.rglob("*")
                if p.is_file() and p.suffix in {".csv", ".xlsx", ".parquet"}
            ],
        }
        (self.output / "metrics.json").write_text(json.dumps(result, indent=2))
        if self.mode != "none" and self.profiler.getstats():
            self.profiler.dump_stats(str(self.output / "workflow.pstats"))
            with (self.output / "hotspots.txt").open("w") as stream:
                stats = pstats.Stats(self.profiler, stream=stream)
                stats.sort_stats("cumulative").print_stats(80)
                stats.sort_stats("tottime").print_stats(80)
        lines = [
            "# Four-scenario SimaPro profile",
            "",
            f"Status: {status}",
            "",
            f"Profiler: {self.mode}. cProfile adds overhead; use `--profile none` for wall-time comparisons.",
            "",
            "| Category (exclusive, no double counting) | Seconds |",
            "|---|---:|",
        ]
        lines += [
            f"| {key} | {value:.2f} |"
            for key, value in sorted(totals.items(), key=lambda item: -item[1])
        ]
        lines += [
            "",
            "Inclusive call timings and scenario identities: `metrics.json` / `events.jsonl`.",
            "Detailed cumulative and self-time hotspots: `hotspots.txt` (when profiling).",
            "Automatic report failures are recorded even if the exporter catches them.",
            "",
        ]
        (self.output / "summary.md").write_text("\n".join(lines))


def instrument(recorder, patches):
    if recorder.mode != "none":
        worker_profiles = recorder.output / "workers"
        worker_profiles.mkdir(exist_ok=True)
        patches.enter_context(
            patch.dict(os.environ, {"PREMISE_REPORT_PROFILE_DIR": str(worker_profiles)})
        )
    import premise.new_database as ndb_module
    import premise.validation as validation
    import premise.change_report as change_report
    import premise.export as export_module
    from premise import NewDatabase
    from premise.export import Export
    from premise.validation_framework import InventoryGraphValidator

    for name in vars(ndb_module):
        if name.startswith("_update_") and callable(getattr(ndb_module, name)):
            recorder.wrap(ndb_module, name, "sector_update", patches)
    recorder.wrap(ndb_module, "validate_sector_contract", "validation", patches)
    for name in (
        "_certify_scenario_store",
        "_ensure_semantic_certification",
        "_record_export_validation_phase",
    ):
        recorder.wrap(NewDatabase, name, "validation", patches)
    recorder.wrap(InventoryGraphValidator, "validate", "validation", patches)
    # Only entry points, not per-exchange checks; include inherited validators once.
    for cls in vars(validation).values():
        if isinstance(cls, type) and cls.__module__ == validation.__name__:
            for name, method in list(vars(cls).items()):
                if name.startswith("run") and callable(method):
                    recorder.wrap(cls, name, "validation", patches)
    for name in ("_load_scenario_database_for_update", "_store_updated_scenario"):
        recorder.wrap(NewDatabase, name, "inventory_storage", patches)
    recorder.wrap(ndb_module, "_prepare_database", "export_preparation", patches)
    recorder.wrap(
        validation.DatasetNormalizer,
        "normalize_database",
        "export_preparation",
        patches,
    )
    recorder.wrap(
        export_module, "check_geographical_linking", "export_preparation", patches
    )
    recorder.wrap(Export, "export_db_to_simapro", "csv_export", patches)
    recorder.wrap(
        NewDatabase,
        "generate_change_report",
        "change_report",
        patches,
        report_path=True,
    )
    recorder.wrap(
        NewDatabase,
        "generate_scenario_report",
        "scenario_report",
        patches,
        report_path=True,
    )
    for name in ("_report_source_store", "_report_scenarios"):
        recorder.wrap(NewDatabase, name, "change_report", patches)
    for name in ("_activity_index", "_generate_details", "_write_workbook"):
        recorder.wrap(change_report, name, "change_report", patches)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-version", default="3.12")
    parser.add_argument("--biosphere", default="ecoinvent-3.12-biosphere")
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument(
        "--inventory-backend", choices=["legacy", "compact"], default="legacy"
    )
    parser.add_argument(
        "--scenario",
        action="append",
        metavar="MODEL:PATHWAY:YEAR",
        help="Repeat exactly four times; defaults to IMAGE SSP2-M for 2030/2040/2050 and SSP2-L for 2030.",
    )
    parser.add_argument(
        "--profile", choices=["none", "focused", "all"], default="focused"
    )
    parser.add_argument(
        "--use-cached-database", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--use-cached-inventories", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="New local directory; must not exist.",
    )
    args = parser.parse_args()
    specs = args.scenario or [
        "image:SSP2-M:2030",
        "image:SSP2-M:2040",
        "image:SSP2-M:2050",
        "image:SSP2-L:2030",
    ]
    try:
        args.scenarios = [
            dict(zip(("model", "pathway", "year"), (model, pathway, int(year))))
            for model, pathway, year in (spec.split(":") for spec in specs)
        ]
    except ValueError:
        parser.error("Each scenario must be MODEL:PATHWAY:YEAR with an integer year.")
    if len(specs) != 4 or len(set(specs)) != 4:
        parser.error("Provide exactly four distinct scenarios.")
    return args


def main():
    args = parse_args()
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
    if not key:
        raise RuntimeError(
            "Set PREMISE_KEY or IAM_FILES_KEY to use encrypted IAM inputs."
        )
    import bw2data as bd
    import premise

    if args.project not in bd.projects:
        raise RuntimeError(f"Brightway project does not exist: {args.project}")
    bd.projects.set_current(args.project)
    for name in (args.source_db, args.biosphere):
        if name not in bd.databases:
            raise RuntimeError(f"Database {name!r} is absent from {args.project!r}")
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        **vars(args),
        "output_dir": str(output),
        "python": sys.version,
        "platform": platform.platform(),
        "premise_version": str(premise.__version__),
        "git_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "git_status": subprocess.check_output(
            ["git", "status", "--short"], cwd=ROOT, text=True
        ),
        "keep_imports_uncertainty": True,
        "keep_source_db_uncertainty": False,
        "generate_reports": True,
        "python_hash_seed": os.environ.get("PYTHONHASHSEED"),
    }
    (output / "configuration.json").write_text(json.dumps(metadata, indent=2))
    recorder = Recorder(output, args.profile)
    status = "failed"
    try:
        with ExitStack() as patches:
            instrument(recorder, patches)
            with recorder.span("NewDatabase", "constructor"):
                ndb = premise.NewDatabase(
                    scenarios=deepcopy(args.scenarios),
                    source_db=args.source_db,
                    source_version=args.source_version,
                    source_type="brightway",
                    system_model=args.system_model,
                    biosphere_name=args.biosphere,
                    key=key,
                    inventory_backend=args.inventory_backend,
                    use_cached_database=args.use_cached_database,
                    use_cached_inventories=args.use_cached_inventories,
                    generate_reports=True,
                    keep_imports_uncertainty=True,
                    keep_source_db_uncertainty=False,
                )
            with recorder.span("ndb.update()", "update_overhead"):
                ndb.update()
            with recorder.span("ndb.write_db_to_simapro()", "export_overhead"):
                ndb.write_db_to_simapro(filepath=str(output / "simapro"))
            if any(event["status"] == "failed" for event in recorder.events):
                raise RuntimeError(
                    "A timed operation failed; inspect events.jsonl and the run log."
                )
            csvs = list((output / "simapro").glob("*.csv"))
            if len(csvs) != 4 or any(p.stat().st_size == 0 for p in csvs):
                raise RuntimeError(
                    f"Expected four nonempty SimaPro CSV files; found {len(csvs)}."
                )
            if ndb._last_change_report_artifacts is None:
                raise RuntimeError("Automatic change report did not produce artifacts.")
            status = "passed"
    finally:
        recorder.save(metadata, status)
        print(f"Profile results: {output}", flush=True)


if __name__ == "__main__":
    main()
