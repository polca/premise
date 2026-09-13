"""Capture a no-write build once, then benchmark identical report inputs.

Use --capture with PREMISE_KEY/IAM_FILES_KEY set to create local input checkpoints.
Subsequent runs use --fixture and optionally --baseline-module or --profile.
Fixtures contain proprietary inventories and trusted-local pickle metadata; never
publish them or load fixtures from an untrusted source. Each run uses a fresh process.
"""

from __future__ import annotations

import argparse
import cProfile
import gc
import importlib.util
import json
import os
from pathlib import Path
import pickle
import pstats
import resource
import sys
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks._process_tree_memory import ProcessTreeMemory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline-module", type=Path)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--read-only-checkpoints", action="store_true")
    parser.add_argument("--scenarios-file", type=Path)
    parser.add_argument("--biosphere", default="ecoinvent-3.12-biosphere")
    parser.add_argument(
        "--inventory-backend", choices=("legacy", "compact"), default="compact"
    )
    args = parser.parse_args()
    import premise
    from premise.inventory_store import InventoryStore, ReadOnlyInventoryStore
    from premise.validation_framework import ValidationReport

    if args.capture:
        import bw2data as bd
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
        key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
        if not key:
            raise RuntimeError("Set PREMISE_KEY or IAM_FILES_KEY")
        if args.fixture.exists():
            raise FileExistsError(args.fixture)
        bd.projects.set_current("ecoinvent-3.12-cutoff")
        ndb = premise.NewDatabase(
            scenarios=(
                json.loads(args.scenarios_file.read_text())
                if args.scenarios_file
                else [{"model": "image", "pathway": "SSP2-M", "year": 2050}]
            ),
            source_db="ecoinvent-3.12-cutoff",
            source_version="3.12",
            biosphere_name=args.biosphere,
            key=key.encode(),
            inventory_backend=args.inventory_backend,
            generate_reports=False,
            quiet=True,
        )
        ndb.update()
        args.fixture.mkdir(parents=True)
        ndb._report_source_store().checkpoint(args.fixture / "source.inventory-store")
        scenarios = []
        for i, scenario in enumerate(ndb._report_scenarios()):
            scenario.store.checkpoint(args.fixture / f"final-{i}.inventory-store")
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
        with (args.fixture / "metadata.pickle").open("wb") as f:
            pickle.dump(metadata, f)
        print("Captured", args.fixture, flush=True)
        return

    if args.output is None:
        parser.error("--output is required for report runs")
    import premise.change_report as report

    if args.baseline_module:
        spec = importlib.util.spec_from_file_location(
            "premise._baseline_report", args.baseline_module
        )
        report = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = report
        spec.loader.exec_module(report)
    with (args.fixture / "metadata.pickle").open("rb") as f:
        metadata = pickle.load(f)
    load_started = time.perf_counter()
    memory = ProcessTreeMemory().start()
    open_store = (
        InventoryStore.open_for_reporting
        if args.read_only_checkpoints
        else InventoryStore.open
    )
    scenario_data = metadata.pop("scenarios")
    checkpoints = [
        args.fixture / f"final-{i}.inventory-store" for i in range(len(scenario_data))
    ]
    if args.read_only_checkpoints:
        with ThreadPoolExecutor(max_workers=4) as readers:
            stores = list(readers.map(open_store, checkpoints))
    else:
        stores = list(map(open_store, checkpoints))
    scenarios = []
    for store, data in zip(stores, scenario_data):
        data["validation_report"] = ValidationReport.from_dict(
            data["validation_report"]
        )
        scenarios.append(
            report.ReportScenario(
                store=ReadOnlyInventoryStore(store),
                **data,
            )
        )
    source = ReadOnlyInventoryStore(open_store(args.fixture / "source.inventory-store"))
    input_seconds = time.perf_counter() - load_started
    args.output.mkdir(parents=True, exist_ok=True)
    gc.collect()
    profiler = cProfile.Profile() if args.profile else None
    started = time.perf_counter()
    if profiler:
        worker_profiles = (args.output / "workers").resolve()
        worker_profiles.mkdir(exist_ok=True)
        os.environ["PREMISE_REPORT_PROFILE_DIR"] = str(worker_profiles)
        profiler.enable()
    result = report.generate_structured_change_report(
        source_store=source,
        scenarios=scenarios,
        filepath=args.output,
        **metadata,
    )
    seconds = time.perf_counter() - started
    tree_rss = memory.stop()
    if profiler:
        profiler.disable()
        profiler.dump_stats(str(args.output / "report.pstats"))
        with (args.output / "profile.txt").open("w") as f:
            stats = pstats.Stats(profiler, stream=f)
            stats.sort_stats("cumulative").print_stats(60)
            stats.sort_stats("tottime").print_stats(30)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    metrics = dict(
        seconds=seconds,
        input_seconds=input_seconds,
        total_seconds=input_seconds + seconds,
        peak_rss_bytes=rss if sys.platform == "darwin" else rss * 1024,
        peak_process_tree_rss_bytes=tree_rss,
        profiled=args.profile,
        python_hash_seed=os.environ.get("PYTHONHASHSEED"),
        workbook=str(result.artifacts.workbook_path),
        details=str(result.artifacts.details_path),
    )
    started = time.perf_counter()
    report.generate_structured_change_report(
        source_store=source,
        scenarios=scenarios,
        filepath=args.output,
        cache_entry=result.cache_entry,
        **metadata,
    )
    metrics["cached_seconds"] = time.perf_counter() - started
    (args.output / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics), flush=True)


if __name__ == "__main__":
    if "PYTHONHASHSEED" not in os.environ:
        os.environ["PYTHONHASHSEED"] = "0"
        os.execv(sys.executable, [sys.executable, *sys.argv])
    main()
