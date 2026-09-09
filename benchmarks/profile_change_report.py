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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline-module", type=Path)
    parser.add_argument("--profile", action="store_true")
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
            scenarios=[{"model": "image", "pathway": "SSP2-M", "year": 2050}],
            source_db="ecoinvent-3.12-cutoff",
            source_version="3.12",
            biosphere_name="biosphere",
            key=key.encode(),
            inventory_backend="compact",
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
    scenarios = []
    for i, data in enumerate(metadata.pop("scenarios")):
        data["validation_report"] = ValidationReport.from_dict(
            data["validation_report"]
        )
        scenarios.append(
            report.ReportScenario(
                store=ReadOnlyInventoryStore(
                    InventoryStore.open(args.fixture / f"final-{i}.inventory-store")
                ),
                **data,
            )
        )
    source = ReadOnlyInventoryStore(
        InventoryStore.open(args.fixture / "source.inventory-store")
    )
    args.output.mkdir(parents=True, exist_ok=True)
    gc.collect()
    profiler = cProfile.Profile() if args.profile else None
    started = time.perf_counter()
    if profiler:
        profiler.enable()
    result = report.generate_structured_change_report(
        source_store=source,
        scenarios=scenarios,
        filepath=args.output,
        **metadata,
    )
    seconds = time.perf_counter() - started
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
        peak_rss_bytes=rss if sys.platform == "darwin" else rss * 1024,
        profiled=args.profile,
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
    main()
