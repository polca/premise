"""Profile a no-write ``premise.NewDatabase`` build.

The benchmark records wall-clock time and resident memory for construction,
each requested sector update, and the scenario-cache dump.  It intentionally
does not write a Brightway database, so repeated runs do not mutate the source
project.

Example
-------
PREMISE_KEY=... python benchmarks/profile_new_database.py \
    --inventory-backend compact \
    --output /tmp/premise-profile.json \
    --pstats /tmp/premise-profile.pstats
"""

from __future__ import annotations

import argparse
import cProfile
import gc
import json
import os
import platform
import resource
import sys
import threading
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import bw2data as bd  # noqa: E402

import premise  # noqa: E402
import premise.new_database as new_database_module  # noqa: E402
from premise import NewDatabase  # noqa: E402
from premise.inventory_store import (  # noqa: E402
    CompactInventoryStore,
    get_scenario_inventory,
    get_wurst_query_diagnostics,
)

try:  # psutil is optional; max RSS remains available through ``resource``.
    import psutil
except ImportError:  # pragma: no cover - depends on the profiling environment
    psutil = None


UPDATE_FUNCTION_LABELS = {
    "_update_biomass": "biomass",
    "_update_electricity": "electricity",
    "_update_cement": "cement",
    "_update_steel": "steel",
    "_update_fuels": "fuels",
    "_update_wind_turbines": "renewable",
    "_update_metals": "metals",
    "_update_mining": "mining",
    "_update_heat": "heat",
    "_update_cdr": "cdr",
    "_update_battery": "battery",
    "_update_final_energy": "final energy",
    "_update_external_scenarios": "external",
    "_update_emissions": "emissions",
}


def _max_rss_bytes() -> int:
    """Return process peak RSS in bytes on macOS and Linux."""

    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() == "Darwin":
        return int(value)
    return int(value * 1024)


class MemorySampler:
    """Sample current process RSS from a lightweight background thread."""

    def __init__(self, interval: float = 0.05) -> None:
        self.interval = interval
        self.peak_rss = 0
        self._process = psutil.Process() if psutil is not None else None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def current_rss(self) -> int | None:
        if self._process is None:
            return None
        return int(self._process.memory_info().rss)

    def _sample(self) -> None:
        while not self._stop_event.wait(self.interval):
            rss = self.current_rss()
            if rss is not None:
                self.peak_rss = max(self.peak_rss, rss)

    def start(self) -> None:
        initial_rss = self.current_rss()
        if initial_rss is not None:
            self.peak_rss = initial_rss
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        final_rss = self.current_rss()
        if final_rss is not None:
            self.peak_rss = max(self.peak_rss, final_rss)


class Recorder:
    """Collect phase durations and RSS observations."""

    def __init__(self, sampler: MemorySampler) -> None:
        self.sampler = sampler
        self.phases: list[dict[str, Any]] = []

    @contextmanager
    def phase(self, name: str) -> Iterator[dict[str, Any]]:
        gc.collect()
        record: dict[str, Any] = {
            "name": name,
            "rss_start_bytes": self.sampler.current_rss(),
            "peak_rss_start_bytes": _max_rss_bytes(),
        }
        started = time.perf_counter()
        try:
            yield record
        finally:
            record.update(
                {
                    "wall_seconds": time.perf_counter() - started,
                    "rss_end_bytes": self.sampler.current_rss(),
                    "peak_rss_end_bytes": _max_rss_bytes(),
                }
            )
            self.phases.append(record)
            print(
                f"profile_phase={name!r} "
                f"wall_seconds={record['wall_seconds']:.3f} "
                f"rss_bytes={record['rss_end_bytes']}",
                flush=True,
            )


def _trace_sector_functions(
    recorder: Recorder,
    *,
    profiler: cProfile.Profile | None = None,
    profile_sector: str | None = None,
) -> None:
    """Wrap imported sector entry points without changing their behavior."""

    def activity_count(scenario: dict[str, Any]) -> int:
        store = scenario.get("_inventory_store")
        if store is not None:
            return len(store)
        working_copy = scenario.get("_inventory_working_copy")
        if working_copy is not None:
            return len(working_copy)
        return len(get_scenario_inventory(scenario))

    for function_name, sector_label in UPDATE_FUNCTION_LABELS.items():
        original = getattr(new_database_module, function_name)

        def wrapper(
            scenario: dict[str, Any],
            *args: Any,
            _original=original,
            _label=sector_label,
            **kwargs: Any,
        ) -> dict[str, Any]:
            should_profile = profiler is not None and _label == profile_sector
            if should_profile:
                profiler.enable()
            with recorder.phase(f"sector:{_label}") as record:
                try:
                    updated = _original(scenario, *args, **kwargs)
                    record["activities"] = activity_count(updated)
                    return updated
                finally:
                    if should_profile:
                        profiler.disable()

        setattr(new_database_module, function_name, wrapper)

    original_vehicles = new_database_module._update_vehicles

    def vehicles_wrapper(
        scenario: dict[str, Any], vehicle_type: str, *args: Any, **kwargs: Any
    ) -> dict[str, Any]:
        should_profile = profiler is not None and vehicle_type == profile_sector
        if should_profile:
            profiler.enable()
        with recorder.phase(f"sector:{vehicle_type}") as record:
            try:
                updated = original_vehicles(scenario, vehicle_type, *args, **kwargs)
                record["activities"] = activity_count(updated)
                return updated
            finally:
                if should_profile:
                    profiler.disable()

    new_database_module._update_vehicles = vehicles_wrapper


def _trace_inventory_store_boundaries(recorder: Recorder) -> None:
    """Record the remaining list/store conversion boundaries separately."""

    original_create = new_database_module.create_inventory_store

    def create_wrapper(*args: Any, **kwargs: Any):
        with recorder.phase("inventory:create-store"):
            return original_create(*args, **kwargs)

    new_database_module.create_inventory_store = create_wrapper

    original_checkpoint = CompactInventoryStore.checkpoint

    def checkpoint_wrapper(self, *args: Any, **kwargs: Any):
        with recorder.phase("inventory:checkpoint"):
            return original_checkpoint(self, *args, **kwargs)

    CompactInventoryStore.checkpoint = checkpoint_wrapper

    original_load = NewDatabase._load_scenario_database_for_update

    def load_wrapper(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with recorder.phase("inventory:materialize-working-copy"):
            return original_load(self, *args, **kwargs)

    NewDatabase._load_scenario_database_for_update = load_wrapper

    original_store = NewDatabase._store_updated_scenario

    def store_wrapper(self, *args: Any, **kwargs: Any):
        with recorder.phase("inventory:store-and-checkpoint"):
            return original_store(self, *args, **kwargs)

    NewDatabase._store_updated_scenario = store_wrapper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-version", default="3.12")
    parser.add_argument("--biosphere", default="ecoinvent-3.12-biosphere")
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument("--model", default="image")
    parser.add_argument("--pathway", default="SSP2-M")
    parser.add_argument("--year", type=int, default=2050)
    parser.add_argument(
        "--scenario",
        action="append",
        nargs=3,
        metavar=("MODEL", "PATHWAY", "YEAR"),
        help="Repeat to benchmark multiple scenarios sharing one source build.",
    )
    parser.add_argument(
        "--inventory-backend", choices=("compact", "legacy"), default="compact"
    )
    parser.add_argument(
        "--sectors",
        nargs="+",
        help="Sector labels accepted by NewDatabase.update; default is all sectors.",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pstats", type=Path)
    parser.add_argument(
        "--write-database-prefix",
        help=(
            "Also time the fast Brightway write using one deterministic database "
            "name per scenario. Existing names are overwritten."
        ),
    )
    parser.add_argument(
        "--profile-sector",
        choices=tuple(UPDATE_FUNCTION_LABELS.values())
        + ("car", "two-wheeler", "truck", "ship", "bus", "train"),
        help="Limit cProfile collection to one sector (requires --pstats).",
    )
    parser.add_argument("--sample-interval", type=float, default=0.05)
    parser.add_argument(
        "--keep-imports-uncertainty",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--use-cached-database", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--use-cached-inventories", action=argparse.BooleanOptionalAction, default=True
    )
    return parser.parse_args()


def _get_key() -> bytes | None:
    key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
    return key.encode() if key else None


def _fixed_python_hash_seed() -> str:
    value = os.environ.get("PYTHONHASHSEED")
    try:
        seed = int(value) if value is not None else None
    except ValueError as error:
        raise RuntimeError(
            "Set PYTHONHASHSEED to an integer from 0 through 4294967295."
        ) from error
    if seed is None or not 0 <= seed <= 4_294_967_295:
        raise RuntimeError(
            "Set PYTHONHASHSEED to an integer from 0 through 4294967295."
        )
    return value


def main() -> None:
    args = parse_args()
    hash_seed = _fixed_python_hash_seed()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.pstats is not None:
        args.pstats.parent.mkdir(parents=True, exist_ok=True)
    if args.profile_sector and args.pstats is None:
        raise ValueError("--profile-sector requires --pstats.")

    bd.projects.set_current(args.project)
    if args.source_db not in bd.databases:
        raise RuntimeError(
            f"Missing source database {args.source_db!r} in project {args.project!r}."
        )

    sampler = MemorySampler(interval=args.sample_interval)
    recorder = Recorder(sampler)
    profiler = cProfile.Profile() if args.pstats is not None else None
    sampler.start()
    get_wurst_query_diagnostics(reset=True)
    started = time.perf_counter()
    scenario_specs = args.scenario or [(args.model, args.pathway, args.year)]
    scenario_configuration = [
        {"model": model, "pathway": pathway, "year": int(year)}
        for model, pathway, year in scenario_specs
    ]
    scenarios = [scenario.copy() for scenario in scenario_configuration]

    if profiler is not None and args.profile_sector is None:
        profiler.enable()

    try:
        _trace_inventory_store_boundaries(recorder)
        with recorder.phase("new-database-constructor"):
            ndb = NewDatabase(
                scenarios=scenarios,
                source_db=args.source_db,
                source_version=args.source_version,
                source_type="brightway",
                system_model=args.system_model,
                biosphere_name=args.biosphere,
                key=_get_key(),
                use_cached_database=args.use_cached_database,
                use_cached_inventories=args.use_cached_inventories,
                keep_imports_uncertainty=args.keep_imports_uncertainty,
                keep_source_db_uncertainty=False,
                generate_reports=False,
                quiet=True,
                inventory_backend=args.inventory_backend,
            )

        _trace_sector_functions(
            recorder,
            profiler=profiler,
            profile_sector=args.profile_sector,
        )
        with recorder.phase("update-total"):
            ndb.update(args.sectors)
        with recorder.phase("validation:cache-reuse"):
            report = ndb.get_validation_report()
        semantic_phases = [
            phase for phase in report.phase_results if phase.kind != "export"
        ]
        validation_metrics = {
            "ruleset_version": report.ruleset_version,
            "sector_seconds": sum(
                phase.elapsed_seconds
                for phase in semantic_phases
                if phase.kind == "sector"
            ),
            "full_graph_seconds": sum(
                phase.elapsed_seconds
                for phase in semantic_phases
                if phase.kind == "graph"
            ),
            "phase_count": len(semantic_phases),
            "warning_count": len(report.warnings),
            "warning_rule_counts": dict(
                sorted(Counter(issue.rule_id for issue in report.warnings).items())
            ),
            "error_count": len(report.errors),
            "suppressed_rule_counts": dict(
                sorted(
                    Counter(issue.rule_id for issue in report.suppressed_issues).items()
                )
            ),
            "certificate_reused": report.reused,
        }
        written_database_names = None
        if args.write_database_prefix:
            written_database_names = [
                f"{args.write_database_prefix}-{position + 1}"
                for position in range(len(scenarios))
            ]
            with recorder.phase("brightway-write-total"):
                ndb.write_db_to_brightway(written_database_names)
    finally:
        if profiler is not None:
            profiler.disable()
            profiler.dump_stats(args.pstats)
        sampler.stop()

    result = {
        "benchmark": "NewDatabase no-write update",
        "configuration": {
            "project": args.project,
            "source_db": args.source_db,
            "source_version": args.source_version,
            "biosphere": args.biosphere,
            "system_model": args.system_model,
            "model": args.model,
            "pathway": args.pathway,
            "year": args.year,
            "scenarios": scenario_configuration,
            "inventory_backend": args.inventory_backend,
            "sectors": args.sectors or "all",
            "keep_imports_uncertainty": args.keep_imports_uncertainty,
            "use_cached_database": args.use_cached_database,
            "use_cached_inventories": args.use_cached_inventories,
            "written_database_names": written_database_names,
        },
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "premise": ".".join(map(str, premise.__version__)),
            "bw2data": str(bd.__version__),
            "python_hash_seed": hash_seed,
        },
        "wall_seconds": time.perf_counter() - started,
        "sampled_peak_rss_bytes": sampler.peak_rss or None,
        "resource_peak_rss_bytes": _max_rss_bytes(),
        "phases": recorder.phases,
        "query_diagnostics": get_wurst_query_diagnostics(),
        "validation": validation_metrics,
    }
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"profile_output={args.output}", flush=True)


if __name__ == "__main__":
    main()
