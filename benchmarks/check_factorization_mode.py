#!/usr/bin/env python3
"""Check factorization output equivalence and warm-cache runtime overhead.

This benchmark requires ``PREMISE_KEY`` and writes no inventory data. It runs
the standard and factorization paths in alternating order, compares a complete
canonical graph digest after every run, and fails above the configured median
runtime ratio.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from _process_tree_memory import ProcessTreeMemory

from premise import NewDatabase
from premise.core.utils import load_database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--biosphere", default="ecoinvent-3.12-biosphere")
    parser.add_argument("--model", default="remind")
    parser.add_argument("--pathway", default="SSP2-NPi")
    parser.add_argument("--year", type=int, default=2050)
    parser.add_argument("--sector", action="append", dest="sectors")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--max-runtime-ratio", type=float, default=1.1)
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Pre-refactor JSON report for the exact same scenario and sectors",
    )
    parser.add_argument("--max-standard-baseline-ratio", type=float, default=1.05)
    parser.add_argument("--max-declarative-baseline-ratio", type=float, default=1.1)
    parser.add_argument(
        "--absolute-max-declarative-baseline-ratio", type=float, default=1.2
    )
    parser.add_argument("--max-peak-rss-baseline-ratio", type=float, default=1.2)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def plain(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): plain(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((plain(item) for item in value), key=canonical_json)
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return plain(item())
        except (TypeError, ValueError):
            pass
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def semantics(dataset: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(dataset.get("name") or ""),
        str(dataset.get("reference product") or dataset.get("product") or ""),
        str(dataset.get("location") or ""),
        str(dataset.get("unit") or ""),
        str(dataset.get("type") or ""),
    )


def graph_digest(database: list[dict[str, Any]]) -> str:
    """Hash all graph data while normalizing only run-specific storage keys."""

    occurrences: dict[tuple[str, str, str, str, str], int] = defaultdict(int)
    code_to_identity: dict[str, tuple[Any, ...]] = {}
    identities: list[tuple[Any, ...]] = []
    for dataset in database:
        role = semantics(dataset)
        ordinal = occurrences[role]
        occurrences[role] += 1
        identity = (*role, ordinal)
        identities.append(identity)
        code = dataset.get("code")
        if code not in (None, ""):
            code_to_identity[str(code)] = identity

    normalized = []
    for identity, dataset in zip(identities, database, strict=True):
        activity = {
            key: value
            for key, value in dataset.items()
            if key not in {"database", "code", "input", "exchanges"}
        }
        exchanges = []
        for exchange in dataset.get("exchanges", ()):
            item = {
                key: value
                for key, value in exchange.items()
                if key not in {"database", "code", "output", "input"}
            }
            raw_input = exchange.get("input")
            if isinstance(raw_input, (list, tuple)) and len(raw_input) == 2:
                provider = code_to_identity.get(str(raw_input[1]))
                item["input"] = provider if provider is not None else raw_input
            exchanges.append(plain(item))
        normalized.append(
            {
                "identity": identity,
                "activity": plain(activity),
                "exchanges": exchanges,
            }
        )
    return hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()


def build(args: argparse.Namespace, *, factorization_mode: bool) -> dict[str, Any]:
    import bw2data as bd

    bd.projects.set_current(args.project)
    key = os.environ.get("PREMISE_KEY")
    if not key:
        raise RuntimeError("Set PREMISE_KEY; unencrypted IAM fallback is not allowed.")
    scenario = {
        "model": args.model,
        "pathway": args.pathway,
        "year": args.year,
    }
    ndb = NewDatabase(
        scenarios=[scenario],
        source_db=args.source_db,
        source_version="3.12",
        source_type="brightway",
        system_model="cutoff",
        biosphere_name=args.biosphere,
        key=key.encode(),
        keep_imports_uncertainty=False,
        keep_source_db_uncertainty=False,
        generate_reports=False,
        quiet=True,
        inventory_backend="compact",
        factorization_mode=factorization_mode,
    )
    memory = ProcessTreeMemory().start()
    started = time.perf_counter()
    try:
        ndb.update(args.sectors, persist=False)
    finally:
        elapsed = time.perf_counter() - started
        peak_rss = memory.stop()
    materialized = load_database(
        scenario=ndb.scenarios[0],
        original_database=ndb._load_original_database(),
        delete=False,
        load_metadata=True,
        warning=False,
    )["database"]
    result = {
        "factorization_mode": factorization_mode,
        "elapsed_seconds": elapsed,
        "peak_rss_bytes": peak_rss,
        "dataset_count": len(materialized),
        "exchange_count": sum(len(ds.get("exchanges", ())) for ds in materialized),
        "graph_sha256": graph_digest(materialized),
    }
    if factorization_mode:
        sidecar = ndb.factorization_metadata()
        result["sidecar_schema_version"] = sidecar.get("schema_version")
        result["sidecar_identity_count"] = len(sidecar.get("identities", {}))
        result["sidecar_missing_count"] = sidecar.get("missing_count")
        if result["sidecar_identity_count"] == 0:
            raise AssertionError(
                "Factorization sidecar contains no activity identities"
            )
        if result["sidecar_missing_count"] != 0:
            raise AssertionError("Factorization sidecar reports missing activities")
    return result


def compare_baseline(
    *,
    args: argparse.Namespace,
    baseline: dict[str, Any],
    standard_median: float,
    factorized_median: float,
    first_factorized_peak_rss: int | None,
) -> dict[str, Any]:
    """Evaluate current measurements against a scenario-matched baseline."""

    expected_scenario = {
        "project": args.project,
        "source_database": args.source_db,
        "model": args.model,
        "pathway": args.pathway,
        "year": args.year,
        "sectors": args.sectors,
        "inventory_backend": "compact",
    }
    observed_scenario = baseline.get("scenario", {})
    mismatches = {
        field: {
            "expected": expected,
            "observed": observed_scenario.get(field),
        }
        for field, expected in expected_scenario.items()
        if observed_scenario.get(field) != expected
    }
    if mismatches:
        raise ValueError(
            "Baseline scenario does not match requested benchmark: "
            + canonical_json(mismatches)
        )

    measurement = baseline.get("measurement", {})
    baseline_standard = float(measurement["standard_median_seconds"])
    baseline_peak_rss = measurement.get("first_standard_peak_rss_bytes")
    standard_ratio = standard_median / baseline_standard
    declarative_ratio = factorized_median / baseline_standard
    peak_rss_ratio = (
        first_factorized_peak_rss / int(baseline_peak_rss)
        if first_factorized_peak_rss and baseline_peak_rss
        else None
    )
    return {
        "baseline_commit": baseline.get("baseline_commit"),
        "baseline_standard_median_seconds": baseline_standard,
        "current_standard_to_baseline_ratio": standard_ratio,
        "current_standard_accepted": (
            standard_ratio <= args.max_standard_baseline_ratio
        ),
        "declarative_to_baseline_standard_ratio": declarative_ratio,
        "declarative_accepted": (
            declarative_ratio <= args.max_declarative_baseline_ratio
        ),
        "declarative_absolute_failure": (
            declarative_ratio > args.absolute_max_declarative_baseline_ratio
        ),
        "first_factorized_peak_rss_bytes": first_factorized_peak_rss,
        "baseline_first_standard_peak_rss_bytes": baseline_peak_rss,
        "peak_rss_to_baseline_ratio": peak_rss_ratio,
        "peak_rss_accepted": (
            peak_rss_ratio is not None
            and peak_rss_ratio <= args.max_peak_rss_baseline_ratio
        ),
        "limits": {
            "current_standard_ratio": args.max_standard_baseline_ratio,
            "declarative_ratio": args.max_declarative_baseline_ratio,
            "declarative_absolute_ratio": (
                args.absolute_max_declarative_baseline_ratio
            ),
            "peak_rss_ratio": args.max_peak_rss_baseline_ratio,
        },
    }


def main() -> int:
    args = parse_args()
    if args.repeats < 3:
        raise ValueError("Use at least three measured repeats")
    if not args.sectors:
        args.sectors = ["electricity", "emissions"]

    # Warm both paths before alternating measured runs.
    build(args, factorization_mode=False)
    build(args, factorization_mode=True)
    runs = []
    for repeat in range(args.repeats):
        order = (False, True) if repeat % 2 == 0 else (True, False)
        runs.extend(build(args, factorization_mode=mode) for mode in order)

    digests = {run["graph_sha256"] for run in runs}
    standard = [run["elapsed_seconds"] for run in runs if not run["factorization_mode"]]
    factorized = [run["elapsed_seconds"] for run in runs if run["factorization_mode"]]
    ratio = statistics.median(factorized) / statistics.median(standard)
    first_factorized_peak_rss = next(
        (
            run["peak_rss_bytes"]
            for run in runs
            if run["factorization_mode"] and run.get("peak_rss_bytes")
        ),
        None,
    )
    result = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "processor": platform.processor(),
        },
        "scenario": {
            "project": args.project,
            "source_database": args.source_db,
            "model": args.model,
            "pathway": args.pathway,
            "year": args.year,
            "sectors": args.sectors,
            "inventory_backend": "compact",
        },
        "runs": runs,
        "graph_identical": len(digests) == 1,
        "standard_median_seconds": statistics.median(standard),
        "factorized_median_seconds": statistics.median(factorized),
        "runtime_ratio": ratio,
        "max_runtime_ratio": args.max_runtime_ratio,
        "runtime_accepted": ratio <= args.max_runtime_ratio,
    }
    if args.baseline:
        result["baseline_comparison"] = compare_baseline(
            args=args,
            baseline=json.loads(args.baseline.read_text(encoding="utf-8")),
            standard_median=result["standard_median_seconds"],
            factorized_median=result["factorized_median_seconds"],
            first_factorized_peak_rss=first_factorized_peak_rss,
        )
    payload = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    baseline_accepted = True
    if comparison := result.get("baseline_comparison"):
        baseline_accepted = all(
            comparison[field]
            for field in (
                "current_standard_accepted",
                "declarative_accepted",
                "peak_rss_accepted",
            )
        )
    return (
        0
        if result["graph_identical"]
        and result["runtime_accepted"]
        and baseline_accepted
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
