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
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

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
    parser.add_argument("--max-runtime-ratio", type=float, default=1.2)
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
    started = time.perf_counter()
    ndb.update(args.sectors, persist=False)
    elapsed = time.perf_counter() - started
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
            raise AssertionError("Factorization sidecar contains no activity identities")
        if result["sidecar_missing_count"] != 0:
            raise AssertionError("Factorization sidecar reports missing activities")
    return result


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
    result = {
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
    payload = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if result["graph_identical"] and result["runtime_accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
