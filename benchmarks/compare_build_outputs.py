#!/usr/bin/env python3
"""Certify semantic output equivalence between two premise builds.

The ``build`` command runs a complete scenario, restores all cache metadata,
writes a canonical snapshot, optionally writes the database to Brightway, and
records LCIA scores. The ``compare`` command requires exact canonical hashes
and near-machine-precision LCIA equality. Builds require a fixed
``PYTHONHASHSEED`` so equivalent supplier sets cannot differ merely because two
Python processes iterate an unordered collection differently.

Random storage identifiers are intentionally excluded from canonical data:
dataset ``code``/``database`` and exchange ``input``/``output`` fields. Supplier
identity remains represented by exchange name, product, location, unit, and
categories. All other dataset and exchange fields are compared exactly.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import platform
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import bw2calc as bc  # noqa: E402
import bw2data as bd  # noqa: E402

import premise  # noqa: E402
from premise import NewDatabase  # noqa: E402

IGNORED_DATASET_FIELDS = frozenset({"code", "database", "input"})
IGNORED_EXCHANGE_FIELDS = frozenset({"input", "output"})

DEFAULT_ACTIVITIES = {
    "electricity_low_voltage_ch": {
        "name": "market for electricity, low voltage",
        "reference product": "electricity, low voltage",
        "location": "CH",
        "unit": "kilowatt hour",
    },
    "heat_natural_gas_europe": {
        "name": "market for heat, district or industrial, natural gas",
        "reference product": "heat, district or industrial, natural gas",
        "location": "Europe without Switzerland",
        "unit": "megajoule",
    },
    "diesel_europe": {
        "name": "market for diesel",
        "reference product": "diesel",
        "location": "Europe without Switzerland",
        "unit": "kilogram",
    },
    "cement_portland_europe": {
        "name": "market for cement, Portland",
        "reference product": "cement, Portland",
        "location": "Europe without Switzerland",
        "unit": "kilogram",
    },
    "steel_low_alloyed_glo": {
        "name": "market for steel, low-alloyed",
        "reference product": "steel, low-alloyed",
        "location": "GLO",
        "unit": "kilogram",
    },
}

DEFAULT_METHODS = (
    (
        "ecoinvent-3.12",
        "IPCC 2013",
        "climate change",
        "global warming potential (GWP100)",
    ),
    (
        "ecoinvent-3.12",
        "EF v3.1",
        "particulate matter formation",
        "impact on human health",
    ),
    (
        "ecoinvent-3.12",
        "EF v3.1",
        "energy resources: non-renewable",
        "abiotic depletion potential (ADP): fossil fuels",
    ),
    (
        "ecoinvent-3.12",
        "EF v3.1",
        "material resources: metals/minerals",
        "abiotic depletion potential (ADP): elements (ultimate reserves)",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build")
    build.add_argument("--output-dir", type=Path, required=True)
    build.add_argument("--label", required=True)
    build.add_argument("--revision", required=True)
    build.add_argument("--project", default="ecoinvent-3.12-cutoff")
    build.add_argument("--source-db", default="ecoinvent-3.12-cutoff")
    build.add_argument("--source-version", default="3.12")
    build.add_argument("--biosphere", default="ecoinvent-3.12-biosphere")
    build.add_argument("--system-model", default="cutoff")
    build.add_argument("--model", default="image")
    build.add_argument("--pathway", default="SSP2-M")
    build.add_argument("--year", type=int, default=2050)
    build.add_argument(
        "--inventory-backend", choices=("compact", "legacy"), default="compact"
    )
    build.add_argument("--database-name")
    build.add_argument("--sample-details", action="store_true")

    compare = subparsers.add_parser("compare")
    compare.add_argument("--left", type=Path, required=True)
    compare.add_argument("--right", type=Path, required=True)
    compare.add_argument("--report", type=Path, required=True)
    compare.add_argument("--relative-tolerance", type=float, default=1e-12)
    compare.add_argument("--absolute-tolerance", type=float, default=1e-12)

    return parser.parse_args()


def canonicalize(value: Any) -> Any:
    """Convert common scientific Python values to deterministic JSON values."""

    if isinstance(value, dict):
        return {
            str(key): canonicalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [canonicalize(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [canonicalize(item) for item in value]
        return sorted(items, key=canonical_json)
    if isinstance(value, Path):
        return str(value)

    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return canonicalize(item_method())
        except (TypeError, ValueError):
            pass

    tolist_method = getattr(value, "tolist", None)
    if callable(tolist_method):
        try:
            return canonicalize(tolist_method())
        except (TypeError, ValueError):
            pass

    if isinstance(value, float):
        if math.isnan(value):
            return {"__float__": "nan"}
        if math.isinf(value):
            return {"__float__": "inf" if value > 0 else "-inf"}
        if value == 0:
            return 0.0
        return value
    if value is None or isinstance(value, (str, int, bool)):
        return value

    raise TypeError(f"Unsupported value in canonical database: {type(value)!r}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def semantic_identity(dataset: dict[str, Any]) -> list[Any]:
    return canonicalize(
        [
            dataset.get("name"),
            dataset.get("reference product"),
            dataset.get("location"),
            dataset.get("unit"),
            dataset.get("type"),
        ]
    )


def canonical_exchange(exchange: dict[str, Any]) -> dict[str, Any]:
    return canonicalize(
        {
            key: value
            for key, value in exchange.items()
            if key not in IGNORED_EXCHANGE_FIELDS
        }
    )


def canonical_dataset(dataset: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        key: value
        for key, value in dataset.items()
        if key not in IGNORED_DATASET_FIELDS and key != "exchanges"
    }
    normalized["exchanges"] = sorted(
        (canonical_exchange(exchange) for exchange in dataset.get("exchanges", [])),
        key=canonical_json,
    )
    return canonicalize(normalized)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def write_semantic_snapshot(
    database: Iterable[dict[str, Any]], prefix: Path, write_details: bool
) -> dict[str, Any]:
    """Write a compact per-dataset hash manifest and optional canonical JSONL."""

    prefix.parent.mkdir(parents=True, exist_ok=True)
    groups: dict[str, Counter[str]] = defaultdict(Counter)
    exchange_types: Counter[str] = Counter()
    dataset_count = 0
    exchange_count = 0

    detail_file = None
    if write_details:
        detail_file = gzip.open(
            prefix.with_suffix(".jsonl.gz"), "wt", encoding="utf-8", compresslevel=1
        )

    try:
        for dataset in database:
            dataset_count += 1
            identity = semantic_identity(dataset)
            normalized = canonical_dataset(dataset)
            digest = sha256_json(normalized)
            groups[canonical_json(identity)][digest] += 1

            for exchange in dataset.get("exchanges", []):
                exchange_count += 1
                exchange_types[str(exchange.get("type"))] += 1

            if detail_file is not None:
                detail_file.write(
                    canonical_json(
                        {
                            "identity": identity,
                            "digest": digest,
                            "dataset": normalized,
                        }
                    )
                    + "\n"
                )
    finally:
        if detail_file is not None:
            detail_file.close()

    records = [
        {
            "identity": json.loads(identity),
            "digests": sorted(counter.items()),
        }
        for identity, counter in sorted(groups.items())
    ]
    summary = {
        "schema": 1,
        "semantic_sha256": sha256_json(records),
        "dataset_count": dataset_count,
        "exchange_count": exchange_count,
        "exchange_types": dict(sorted(exchange_types.items())),
        "ignored_dataset_fields": sorted(IGNORED_DATASET_FIELDS),
        "ignored_exchange_fields": sorted(IGNORED_EXCHANGE_FIELDS),
        "groups": records,
    }
    prefix.with_suffix(".summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def extract_brightway_database(database_name: str) -> list[dict[str, Any]]:
    """Extract activity and exchange payloads from a written Brightway database."""

    try:
        from bw2data.backends.peewee import ActivityDataset, ExchangeDataset
    except ImportError:  # pragma: no cover - Brightway 2 fallback
        try:
            from bw2data.backends.schema import ActivityDataset, ExchangeDataset
        except ImportError:
            from bw2data.backends import ActivityDataset, ExchangeDataset

    datasets_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    activity_query = ActivityDataset.select().where(
        ActivityDataset.database == database_name
    )
    for activity in activity_query:
        dataset = dict(getattr(activity, "data", {}) or {})
        dataset.setdefault("database", activity.database)
        dataset.setdefault("code", activity.code)
        dataset.setdefault("location", getattr(activity, "location", None))
        dataset.setdefault("name", getattr(activity, "name", None))
        dataset.setdefault("reference product", getattr(activity, "product", None))
        dataset.setdefault("type", getattr(activity, "type", None))
        dataset["exchanges"] = []
        datasets_by_key[(activity.database, activity.code)] = dataset

    exchange_query = ExchangeDataset.select().where(
        ExchangeDataset.output_database == database_name
    )
    for exchange in exchange_query:
        payload = dict(getattr(exchange, "data", {}) or {})
        payload.setdefault("input", (exchange.input_database, exchange.input_code))
        payload.setdefault("output", (exchange.output_database, exchange.output_code))
        payload.setdefault("type", getattr(exchange, "type", None))
        datasets_by_key[(exchange.output_database, exchange.output_code)][
            "exchanges"
        ].append(payload)

    return list(datasets_by_key.values())


def stable_database_metadata(database_name: str) -> dict[str, Any]:
    metadata = dict(bd.databases[database_name])
    for key in ("name", "created", "modified", "processed"):
        metadata.pop(key, None)
    return canonicalize(metadata)


def find_activity(database: Any, specification: dict[str, str]) -> Any:
    matches = [
        activity
        for activity in database
        if all(activity.get(field) == value for field, value in specification.items())
    ]
    if len(matches) != 1:
        raise AssertionError(
            f"Expected exactly one activity matching {specification!r} in "
            f"{database.name!r}; found {len(matches)}."
        )
    return matches[0]


def calculate_lcia_scores(database_name: str) -> dict[str, Any]:
    database = bd.Database(database_name)
    activities = {
        label: find_activity(database, specification)
        for label, specification in DEFAULT_ACTIVITIES.items()
    }
    missing_methods = [method for method in DEFAULT_METHODS if method not in bd.methods]
    if missing_methods:
        raise AssertionError(f"Missing LCIA methods: {missing_methods!r}")

    results: dict[str, dict[str, float]] = {}
    for method in DEFAULT_METHODS:
        first_activity = next(iter(activities.values()))
        lca = bc.LCA({first_activity.id: 1}, method)
        lca.lci()
        lca.lcia()

        method_scores = {}
        for label, activity in activities.items():
            lca.redo_lcia({activity.id: 1})
            method_scores[label] = float(lca.score)
        results[canonical_json(method)] = method_scores

    return {
        "database": database_name,
        "activities": DEFAULT_ACTIVITIES,
        "scores": results,
    }


def decryption_key() -> bytes:
    key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
    if not key:
        raise RuntimeError("Set PREMISE_KEY or IAM_FILES_KEY for encrypted IAM data.")
    return key.encode()


def fixed_python_hash_seed() -> str:
    """Return a valid deterministic hash seed or fail before building."""

    value = os.environ.get("PYTHONHASHSEED")
    try:
        seed = int(value) if value is not None else None
    except ValueError as exc:
        raise RuntimeError(
            "Set PYTHONHASHSEED to an integer from 0 through 4294967295 before "
            "starting the equivalence build."
        ) from exc
    if seed is None or not 0 <= seed <= 4294967295:
        raise RuntimeError(
            "Set PYTHONHASHSEED to an integer from 0 through 4294967295 before "
            "starting the equivalence build."
        )
    return str(value)


def build_snapshot(args: argparse.Namespace) -> None:
    hash_seed = fixed_python_hash_seed()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    bd.projects.set_current(args.project)
    missing = [
        name for name in (args.source_db, args.biosphere) if name not in bd.databases
    ]
    if missing:
        raise RuntimeError(
            f"Missing Brightway databases in project {args.project!r}: {missing!r}"
        )
    if args.database_name and args.database_name in bd.databases:
        raise RuntimeError(
            f"Refusing to overwrite existing database {args.database_name!r}."
        )

    ndb = NewDatabase(
        scenarios=[
            {
                "model": args.model,
                "pathway": args.pathway,
                "year": args.year,
            }
        ],
        source_db=args.source_db,
        source_version=args.source_version,
        source_type="brightway",
        system_model=args.system_model,
        biosphere_name=args.biosphere,
        key=decryption_key(),
        use_cached_database=True,
        use_cached_inventories=True,
        keep_imports_uncertainty=False,
        keep_source_db_uncertainty=False,
        generate_reports=False,
        quiet=True,
        inventory_backend=args.inventory_backend,
    )
    ndb.update()
    scenario_database = ndb.materialize_inventory(restore_metadata=True)

    scenario_summary = write_semantic_snapshot(
        scenario_database,
        args.output_dir / "scenario",
        write_details=args.sample_details,
    )

    written_summary = None
    metadata = None
    scores = None
    if args.database_name:
        ndb.write_db_to_brightway(args.database_name)
        written_database = extract_brightway_database(args.database_name)
        written_summary = write_semantic_snapshot(
            written_database,
            args.output_dir / "brightway",
            write_details=args.sample_details,
        )
        metadata = stable_database_metadata(args.database_name)
        scores = calculate_lcia_scores(args.database_name)
        (args.output_dir / "lcia.json").write_text(
            json.dumps(scores, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    run = {
        "label": args.label,
        "revision": args.revision,
        "configuration": {
            "project": args.project,
            "source_db": args.source_db,
            "source_version": args.source_version,
            "biosphere": args.biosphere,
            "system_model": args.system_model,
            "model": args.model,
            "pathway": args.pathway,
            "year": args.year,
            "inventory_backend": args.inventory_backend,
        },
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "premise": ".".join(map(str, premise.__version__)),
            "bw2data": str(bd.__version__),
            "python_hash_seed": hash_seed,
        },
        "database_name": args.database_name,
        "database_metadata": metadata,
        "scenario": {
            key: scenario_summary[key]
            for key in (
                "semantic_sha256",
                "dataset_count",
                "exchange_count",
                "exchange_types",
            )
        },
        "brightway": (
            {
                key: written_summary[key]
                for key in (
                    "semantic_sha256",
                    "dataset_count",
                    "exchange_count",
                    "exchange_types",
                )
            }
            if written_summary
            else None
        ),
    }
    (args.output_dir / "run.json").write_text(
        json.dumps(run, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(run, ensure_ascii=False, indent=2))


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def compare_snapshot_summaries(
    left_path: Path, right_path: Path, label: str
) -> dict[str, Any]:
    left = load_json(left_path)
    right = load_json(right_path)
    result = {
        "label": label,
        "equivalent": left["semantic_sha256"] == right["semantic_sha256"],
        "left_sha256": left["semantic_sha256"],
        "right_sha256": right["semantic_sha256"],
        "left_dataset_count": left["dataset_count"],
        "right_dataset_count": right["dataset_count"],
        "left_exchange_count": left["exchange_count"],
        "right_exchange_count": right["exchange_count"],
        "group_differences": [],
    }
    if result["equivalent"]:
        return result

    left_groups = {canonical_json(row["identity"]): row for row in left["groups"]}
    right_groups = {canonical_json(row["identity"]): row for row in right["groups"]}
    for identity in sorted(set(left_groups) | set(right_groups)):
        left_group = left_groups.get(identity)
        right_group = right_groups.get(identity)
        if left_group != right_group:
            result["group_differences"].append(
                {
                    "identity": json.loads(identity),
                    "left": left_group,
                    "right": right_group,
                }
            )
            if len(result["group_differences"]) >= 50:
                break
    return result


def compare_lcia(
    left_path: Path,
    right_path: Path,
    relative_tolerance: float,
    absolute_tolerance: float,
) -> dict[str, Any]:
    left = load_json(left_path)
    right = load_json(right_path)
    differences = []

    for method in sorted(set(left["scores"]) | set(right["scores"])):
        left_scores = left["scores"].get(method, {})
        right_scores = right["scores"].get(method, {})
        for activity in sorted(set(left_scores) | set(right_scores)):
            left_score = left_scores.get(activity)
            right_score = right_scores.get(activity)
            if (
                left_score is None
                or right_score is None
                or not math.isclose(
                    left_score,
                    right_score,
                    rel_tol=relative_tolerance,
                    abs_tol=absolute_tolerance,
                )
            ):
                differences.append(
                    {
                        "method": json.loads(method),
                        "activity": activity,
                        "left": left_score,
                        "right": right_score,
                    }
                )

    return {"equivalent": not differences, "differences": differences}


def compare_builds(args: argparse.Namespace) -> None:
    left_run = load_json(args.left / "run.json")
    right_run = load_json(args.right / "run.json")
    configuration_equivalent = left_run["configuration"] == right_run["configuration"]
    left_hash_seed = left_run.get("environment", {}).get("python_hash_seed")
    right_hash_seed = right_run.get("environment", {}).get("python_hash_seed")
    hash_seed_equivalent = left_hash_seed == right_hash_seed
    scenario = compare_snapshot_summaries(
        args.left / "scenario.summary.json",
        args.right / "scenario.summary.json",
        "fully restored scenario",
    )

    brightway = None
    lcia = None
    metadata_equivalent = None
    if (args.left / "brightway.summary.json").exists() and (
        args.right / "brightway.summary.json"
    ).exists():
        brightway = compare_snapshot_summaries(
            args.left / "brightway.summary.json",
            args.right / "brightway.summary.json",
            "written Brightway database",
        )
        metadata_equivalent = (
            left_run["database_metadata"] == right_run["database_metadata"]
        )
        lcia = compare_lcia(
            args.left / "lcia.json",
            args.right / "lcia.json",
            args.relative_tolerance,
            args.absolute_tolerance,
        )

    equivalent = (
        configuration_equivalent
        and hash_seed_equivalent
        and scenario["equivalent"]
        and (brightway is None or brightway["equivalent"])
        and (metadata_equivalent is None or metadata_equivalent)
        and (lcia is None or lcia["equivalent"])
    )
    report = {
        "equivalent": equivalent,
        "left": {"label": left_run["label"], "revision": left_run["revision"]},
        "right": {
            "label": right_run["label"],
            "revision": right_run["revision"],
        },
        "configuration_equivalent": configuration_equivalent,
        "python_hash_seed": {
            "equivalent": hash_seed_equivalent,
            "left": left_hash_seed,
            "right": right_hash_seed,
        },
        "scenario": scenario,
        "brightway": brightway,
        "database_metadata_equivalent": metadata_equivalent,
        "lcia": lcia,
        "lcia_tolerance": {
            "relative": args.relative_tolerance,
            "absolute": args.absolute_tolerance,
        },
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not equivalent:
        raise SystemExit(1)


def main() -> None:
    args = parse_args()
    if args.command == "build":
        build_snapshot(args)
    elif args.command == "compare":
        compare_builds(args)
    else:  # pragma: no cover
        raise ValueError(args.command)


if __name__ == "__main__":
    main()
