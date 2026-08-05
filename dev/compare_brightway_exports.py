#!/usr/bin/env python3
"""Generate and compare Premise Brightway exports across BW2 and BW2.5.

Usage:

1. Prepare one shared payload, write it through both writers, and compare:
   python dev/compare_brightway_exports.py run

2. Prepare a fast-export-ready payload inside one environment:
   python dev/compare_brightway_exports.py prepare --output prepared_payload.pkl

3. Write a prepared payload in one environment and dump the resulting database:
   python dev/compare_brightway_exports.py write --input prepared_payload.pkl --output bw2.json

4. Dump an already-written database without rewriting it:
   python dev/compare_brightway_exports.py dump --output bw2.json

5. Compare two previously generated dumps:
   python dev/compare_brightway_exports.py compare --left bw2.json --right bw25.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_PROJECT = "ecoinvent-3.12-cutoff"
DEFAULT_SOURCE_DB = "ecoinvent-3.12-cutoff"
DEFAULT_BIOSPHERE = "ecoinvent-3.12-biosphere"
DEFAULT_DB_NAME = "premise-bw-compare"
DEFAULT_WRITE_PROJECT = "premise-bw-export-compare"
DEFAULT_SCENARIOS = [
    {
        "model": "image",
        "pathway": "SSP2-M",
        "year": 2050,
    }
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--bw2-env", default="premise-bw2")
    run_parser.add_argument("--bw25-env", default="premise-bw25")
    run_parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("export") / "bw-compare",
    )
    run_parser.add_argument("--project", default=DEFAULT_PROJECT)
    run_parser.add_argument("--source-db", default=DEFAULT_SOURCE_DB)
    run_parser.add_argument("--biosphere-name", default=DEFAULT_BIOSPHERE)
    run_parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    run_parser.add_argument("--iam-files-key")

    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--project", default=DEFAULT_PROJECT)
    prepare_parser.add_argument("--source-db", default=DEFAULT_SOURCE_DB)
    prepare_parser.add_argument("--biosphere-name", default=DEFAULT_BIOSPHERE)
    prepare_parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    prepare_parser.add_argument("--iam-files-key")

    write_parser = subparsers.add_parser("write")
    write_parser.add_argument("--input", type=Path, required=True)
    write_parser.add_argument("--output", type=Path, required=True)
    write_parser.add_argument("--project", default=DEFAULT_WRITE_PROJECT)
    write_parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    write_parser.add_argument(
        "--bootstrap-project-from-default-dir",
        action="store_true",
    )

    dump_parser = subparsers.add_parser("dump")
    dump_parser.add_argument("--output", type=Path, required=True)
    dump_parser.add_argument("--project", default=DEFAULT_WRITE_PROJECT)
    dump_parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    dump_parser.add_argument(
        "--bootstrap-project-from-default-dir",
        action="store_true",
    )

    compare_parser = subparsers.add_parser("compare")
    compare_parser.add_argument("--left", type=Path, required=True)
    compare_parser.add_argument("--right", type=Path, required=True)

    return parser.parse_args()


def _ensure_brightway_dir_exists() -> None:
    brightway_dir = os.environ.get("BRIGHTWAY2_DIR")
    if brightway_dir:
        Path(brightway_dir).mkdir(parents=True, exist_ok=True)


def _load_scenarios() -> list[dict[str, Any]]:
    return json.loads(json.dumps(DEFAULT_SCENARIOS))


def _brightway_major_version(version: Any) -> int:
    if isinstance(version, tuple):
        return int(version[0])
    return int(str(version).split(".")[0])


def _load_repo_module(module_filename: str, module_name: str) -> Any:
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "premise" / module_filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _bootstrap_project_from_default_dir(*, bw2data: Any, project_name: str) -> None:
    from bw2data.project import safe_filename
    from platformdirs import PlatformDirs

    source_base_dir = Path(PlatformDirs("Brightway3", "pylca").user_data_dir)
    source_projects_db = source_base_dir / "projects.db"

    if not source_projects_db.exists():
        raise RuntimeError(
            f"Can't bootstrap Brightway project {project_name!r}: "
            f"{source_projects_db} does not exist."
        )

    with sqlite3.connect(source_projects_db) as connection:
        row = connection.execute(
            """
            SELECT data, full_hash, is_sourced, revision
            FROM projectdataset
            WHERE name = ?
            """,
            (project_name,),
        ).fetchone()

    if row is None:
        raise RuntimeError(
            f"Can't bootstrap Brightway project {project_name!r} from {source_base_dir}."
        )

    data_blob, full_hash, is_sourced, revision = row
    current_base_dir = Path(bw2data.projects._base_data_dir)
    current_projects_db = current_base_dir / "projects.db"
    project_dir_name = safe_filename(project_name, full=bool(full_hash))
    source_project_dir = source_base_dir / project_dir_name
    target_project_dir = current_base_dir / project_dir_name

    if not source_project_dir.exists():
        raise RuntimeError(
            f"Can't bootstrap Brightway project {project_name!r}: "
            f"{source_project_dir} does not exist."
        )

    if target_project_dir.exists():
        shutil.rmtree(target_project_dir)
    shutil.copytree(source_project_dir, target_project_dir)

    with sqlite3.connect(current_projects_db) as connection:
        connection.execute(
            "DELETE FROM projectdataset WHERE name = ?",
            (project_name,),
        )
        connection.execute(
            """
            INSERT INTO projectdataset (data, name, full_hash, is_sourced, revision)
            VALUES (?, ?, ?, ?, ?)
            """,
            (data_blob, project_name, full_hash, is_sourced, revision),
        )
        connection.commit()


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, tuple):
        return [_canonicalize(item) for item in value]
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    if isinstance(value, set):
        return sorted(_canonicalize(item) for item in value)
    if isinstance(value, Path):
        return str(value)

    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return _canonicalize(item_method())
        except Exception:
            pass

    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value

    return value


def _exchange_sort_key(exchange: dict[str, Any]) -> tuple[str, str]:
    canonical = _canonicalize(exchange)
    return (
        str(canonical.get("type")),
        json.dumps(canonical, sort_keys=True, separators=(",", ":")),
    )


def _normalize_dataset(dataset: dict[str, Any]) -> dict[str, Any]:
    normalized = _canonicalize(dataset)
    exchanges = normalized.get("exchanges", [])
    normalized["exchanges"] = sorted(exchanges, key=_exchange_sort_key)
    return normalized


def _extract_full_database(db_name: str) -> list[dict[str, Any]]:
    try:
        from bw2data.backends.peewee import ActivityDataset, ExchangeDataset
    except ImportError:
        try:
            from bw2data.backends.schema import ActivityDataset, ExchangeDataset
        except ImportError:
            from bw2data.backends import ActivityDataset, ExchangeDataset

    datasets_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    activity_query = ActivityDataset.select().where(ActivityDataset.database == db_name)
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
        ExchangeDataset.output_database == db_name
    )
    for exchange in exchange_query:
        payload = dict(getattr(exchange, "data", {}) or {})
        payload.setdefault(
            "input",
            (exchange.input_database, exchange.input_code),
        )
        payload.setdefault(
            "output",
            (exchange.output_database, exchange.output_code),
        )
        payload.setdefault("type", getattr(exchange, "type", None))

        output_key = (exchange.output_database, exchange.output_code)
        datasets_by_key[output_key]["exchanges"].append(payload)

    return list(datasets_by_key.values())


def _dataset_group_key(dataset: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        str(dataset.get("database", "")),
        str(dataset.get("name", "")),
        str(dataset.get("reference product", "")),
        json.dumps(_canonicalize(dataset.get("location", "")), sort_keys=True),
        str(dataset.get("unit", "")),
        str(dataset.get("type", "")),
    )


def _group_datasets(datasets: list[dict[str, Any]]) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for dataset in datasets:
        key = _dataset_group_key(dataset)
        grouped.setdefault(key, []).append(_normalize_dataset(dataset))

    for key in grouped:
        grouped[key].sort(
            key=lambda dataset: (
                str(dataset.get("code", "")),
                json.dumps(dataset, sort_keys=True, separators=(",", ":")),
            )
        )
    return grouped


def _first_difference(left: Any, right: Any, path: str = "root") -> str | None:
    if isinstance(left, dict) and isinstance(right, dict):
        left_keys = set(left)
        right_keys = set(right)
        if left_keys != right_keys:
            missing = sorted(right_keys - left_keys)
            extra = sorted(left_keys - right_keys)
            return f"{path}: key mismatch; missing={missing}, extra={extra}"
        for key in sorted(left):
            difference = _first_difference(left[key], right[key], f"{path}.{key}")
            if difference is not None:
                return difference
        return None

    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return f"{path}: list length mismatch; left={len(left)} right={len(right)}"
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            difference = _first_difference(left_item, right_item, f"{path}[{index}]")
            if difference is not None:
                return difference
        return None

    if left != right:
        return f"{path}: left={left!r} right={right!r}"

    return None


def compare_payloads(left_payload: dict[str, Any], right_payload: dict[str, Any]) -> None:
    left_grouped = _group_datasets(left_payload["datasets"])
    right_grouped = _group_datasets(right_payload["datasets"])

    if set(left_grouped) != set(right_grouped):
        missing = sorted(set(right_grouped) - set(left_grouped))
        extra = sorted(set(left_grouped) - set(right_grouped))
        raise AssertionError(
            "Dataset identity mismatch.\n"
            f"Missing from left: {missing[:5]}\n"
            f"Extra on left: {extra[:5]}"
        )

    for key in sorted(left_grouped):
        left_datasets = left_grouped[key]
        right_datasets = right_grouped[key]
        if len(left_datasets) != len(right_datasets):
            raise AssertionError(
                f"Dataset multiplicity mismatch for {key}: "
                f"left={len(left_datasets)} right={len(right_datasets)}"
            )

        for index, (left_dataset, right_dataset) in enumerate(
            zip(left_datasets, right_datasets)
        ):
            difference = _first_difference(
                left_dataset,
                right_dataset,
                path=f"dataset[{key}][{index}]",
            )
            if difference is not None:
                raise AssertionError(difference)


def _ensure_source_database(
    *,
    bw2data: Any,
    bw2io: Any,
    source_db: str,
    source_version: str,
    system_model: str,
    biosphere_name: str,
) -> None:
    if source_db in bw2data.databases:
        return

    username = os.environ.get("EI_USERNAME")
    password = os.environ.get("EI_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            f"Source database {source_db!r} is missing in project "
            f"{bw2data.projects.current!r}, and EI_USERNAME/EI_PASSWORD are not set."
        )

    bw2io.import_ecoinvent_release(
        version=source_version,
        system_model=system_model,
        username=username,
        password=password,
        biosphere_name=biosphere_name,
    )


def prepare_payload(args: argparse.Namespace) -> None:
    import bw2data
    import bw2io
    from premise import NewDatabase, clear_inventory_cache
    from premise.export import prepare_db_for_fast_export
    from premise.utils import load_database

    project_name = args.project
    source_db = args.source_db
    biosphere_name = args.biosphere_name
    db_name = args.db_name
    scenarios = _load_scenarios()

    key = args.iam_files_key or os.environ.get("IAM_FILES_KEY")

    bw2data.projects.set_current(project_name)
    clear_inventory_cache()
    _ensure_source_database(
        bw2data=bw2data,
        bw2io=bw2io,
        source_db=source_db,
        source_version="3.12",
        system_model="cutoff",
        biosphere_name=biosphere_name,
    )

    if biosphere_name not in bw2data.databases:
        biosphere_candidates = sorted(
            database_name
            for database_name in bw2data.databases
            if "biosphere" in database_name
        )
        raise RuntimeError(
            f"Biosphere database {biosphere_name!r} not found in project "
            f"{project_name!r}. Available biosphere databases: {biosphere_candidates}"
        )

    ndb = NewDatabase(
        scenarios=scenarios,
        source_db=source_db,
        source_version="3.12",
        key=key.encode() if key else None,
        system_model="cutoff",
        biosphere_name=biosphere_name,
        quiet=True,
        generate_reports=False,
    )
    ndb.update()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    scenario = load_database(
        scenario=ndb.scenarios[0],
        original_database=[],
        load_metadata=True,
        warning=False,
    )
    prepared_database = prepare_db_for_fast_export(
        scenario=scenario,
        name=db_name,
        biosphere_name=biosphere_name,
        version="3.12",
    )
    payload = {
        "project": project_name,
        "source_db": source_db,
        "biosphere_name": biosphere_name,
        "db_name": db_name,
        "bw2data_version": str(getattr(bw2data, "__version__", "unknown")),
        "dataset_count": len(prepared_database),
        "datasets": prepared_database,
    }
    import pickle

    with open(args.output, "wb") as file:
        pickle.dump(payload, file, protocol=4)
    print(f"Wrote prepared payload to {args.output}")


def write_dump(args: argparse.Namespace) -> None:
    import copy
    import pickle

    import bw2data

    with open(args.input, "rb") as file:
        prepared_payload = pickle.load(file)

    if args.bootstrap_project_from_default_dir:
        _bootstrap_project_from_default_dir(
            bw2data=bw2data,
            project_name=args.project,
        )

    bw2data.projects.set_current(args.project)
    bw2data.preferences["allow incomplete imports"] = True
    bw2data.preferences.flush()

    if args.db_name in bw2data.databases:
        del bw2data.databases[args.db_name]

    bw2data_major = _brightway_major_version(getattr(bw2data, "__version__", "0"))
    if bw2data_major >= 4:
        write_brightway_database = _load_repo_module(
            "brightway25.py", "premise_brightway25_writer"
        ).write_brightway_database
    else:
        write_brightway_database = _load_repo_module(
            "brightway2.py", "premise_brightway2_writer"
        ).write_brightway_database

    write_brightway_database(
        data=copy.deepcopy(prepared_payload["datasets"]),
        name=args.db_name,
        fast=True,
        check_internal=True,
    )

    datasets = _extract_full_database(args.db_name)
    normalized_payload = {
        "project": args.project,
        "db_name": args.db_name,
        "bw2data_version": str(getattr(bw2data, "__version__", "unknown")),
        "dataset_count": len(datasets),
        "datasets": [_normalize_dataset(dataset) for dataset in datasets],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            normalized_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    print(f"Wrote normalized dump to {args.output}")


def dump_existing_database(args: argparse.Namespace) -> None:
    import bw2data

    if args.bootstrap_project_from_default_dir:
        _bootstrap_project_from_default_dir(
            bw2data=bw2data,
            project_name=args.project,
        )

    bw2data.projects.set_current(args.project)
    datasets = _extract_full_database(args.db_name)
    normalized_payload = {
        "project": args.project,
        "db_name": args.db_name,
        "bw2data_version": str(getattr(bw2data, "__version__", "unknown")),
        "dataset_count": len(datasets),
        "datasets": [_normalize_dataset(dataset) for dataset in datasets],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            normalized_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    print(f"Wrote normalized dump to {args.output}")


def compare_files(args: argparse.Namespace) -> None:
    left_payload = json.loads(args.left.read_text())
    right_payload = json.loads(args.right.read_text())
    compare_payloads(left_payload, right_payload)
    print(
        "Databases are identical at dataset and exchange level: "
        f"{left_payload['dataset_count']} datasets compared."
    )


def _run_subprocess(command: list[str], env: dict[str, str]) -> None:
    print("Running:", " ".join(command), flush=True)
    completed = subprocess.run(command, env=env)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def run_orchestrated(args: argparse.Namespace) -> None:
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    script_path = Path(__file__).resolve()
    prepared_payload = output_dir / "prepared_payload.pkl"
    bw2_output = output_dir / "bw2.json"
    bw25_output = output_dir / "bw25.json"
    env = os.environ.copy()

    bw25_env = env.copy()
    bw25_env["BRIGHTWAY2_DIR"] = str(output_dir / "bw25-brightway")
    _run_subprocess(
        [
            "conda",
            "run",
            "-n",
            args.bw25_env,
            "python",
            str(script_path),
            "prepare",
            "--output",
            str(prepared_payload),
            "--project",
            args.project,
            "--source-db",
            args.source_db,
            "--biosphere-name",
            args.biosphere_name,
            "--db-name",
            args.db_name,
            *(
                ["--iam-files-key", args.iam_files_key]
                if args.iam_files_key
                else []
            ),
        ],
        env=env,
    )

    bw2_env = env.copy()
    bw2_env["BRIGHTWAY2_DIR"] = str(output_dir / "bw2-brightway")
    _run_subprocess(
        [
            "conda",
            "run",
            "-n",
            args.bw2_env,
            "python",
            str(script_path),
            "write",
            "--input",
            str(prepared_payload),
            "--output",
            str(bw2_output),
            "--project",
            DEFAULT_WRITE_PROJECT,
            "--db-name",
            args.db_name,
        ],
        env=bw2_env,
    )

    _run_subprocess(
        [
            "conda",
            "run",
            "-n",
            args.bw25_env,
            "python",
            str(script_path),
            "write",
            "--input",
            str(prepared_payload),
            "--output",
            str(bw25_output),
            "--project",
            args.project,
            "--db-name",
            args.db_name,
            "--bootstrap-project-from-default-dir",
        ],
        env=bw25_env,
    )

    compare_payloads(
        json.loads(bw2_output.read_text()),
        json.loads(bw25_output.read_text()),
    )
    print(f"Comparison successful. Dumps stored in {output_dir}")


def main() -> None:
    _ensure_brightway_dir_exists()
    args = parse_args()
    if args.command == "run":
        run_orchestrated(args)
    elif args.command == "prepare":
        prepare_payload(args)
    elif args.command == "write":
        write_dump(args)
    elif args.command == "dump":
        dump_existing_database(args)
    elif args.command == "compare":
        compare_files(args)
    else:
        raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
