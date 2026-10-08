"""Rebuild and record an integration-test case without replacing its references.

Requires an existing ecoinvent source project and PREMISE_KEY or IAM_FILES_KEY.
Use a fresh database prefix: source and existing test databases are never replaced.
The JSON report contains scores and validation summaries, not inventory data.
"""

from __future__ import annotations

import argparse
import copy
import importlib.metadata
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

import bw2calc as bc
import bw2data as bd

from lcia_regression import (
    _find_activity,
    _load_reference_scores,
    get_lcia_regression_method,
)
import lcia_regression
import premise
from premise import NewDatabase

SCENARIOS = {
    "test1": {"model": "remind", "pathway": "SSP3-rollBack", "year": 2050},
    "test2": {"model": "image", "pathway": "SSP2-VLHO", "year": 2050},
    "test3": {"model": "tiam-ucl", "pathway": "SSP2-RCP19", "year": 2050},
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True)
    parser.add_argument("--database-prefix", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--score-existing", action="store_true", help="Only score the named outputs."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Also run the integration-test assertion against the reference file.",
    )
    args = parser.parse_args()
    references = _load_reference_scores()
    case = references["cases"][args.case]
    version, source_model = args.case.removeprefix("ecoinvent-").split("-", 1)
    system_model = "cutoff" if source_model == "EN15804" else source_model
    scenario_labels = list(case["scores"])
    names = [f"{args.database_prefix}-{label}" for label in scenario_labels]
    if args.case not in bd.projects:
        raise RuntimeError(f"Source project {args.case!r} must already exist.")
    bd.projects.set_current(args.case)
    if args.case not in bd.databases:
        raise RuntimeError(f"Source database {args.case!r} must already exist.")
    biosphere = f"ecoinvent-{version}-biosphere"
    if biosphere not in bd.databases:
        candidates = [
            name
            for name in bd.databases[args.case].get("depends", [])
            if "biosphere" in name and name in bd.databases
        ]
        if len(candidates) != 1:
            raise RuntimeError(f"Cannot identify the source biosphere: {candidates!r}.")
        biosphere = candidates[0]
    method = get_lcia_regression_method(args.case)
    start = time.perf_counter()
    validation = {}
    if not args.score_existing:
        if any(name in bd.databases for name in names):
            raise RuntimeError("Choose a fresh prefix; output databases already exist.")
        key = os.environ.get("PREMISE_KEY") or os.environ.get("IAM_FILES_KEY")
        if not key:
            raise RuntimeError(
                "Set PREMISE_KEY or IAM_FILES_KEY for encrypted IAM data."
            )
        if os.environ.get("PYTHONHASHSEED") != "0":
            raise RuntimeError("Run with PYTHONHASHSEED=0 to match certification.")
        ndb = NewDatabase(
            scenarios=[copy.deepcopy(SCENARIOS[label]) for label in scenario_labels],
            source_db=args.case,
            source_version=version,
            key=key.encode(),
            system_model=system_model,
            biosphere_name=biosphere,
            inventory_backend="compact",
            generate_reports=False,
            quiet=True,
        )
        ndb.update()
        for position, label in enumerate(scenario_labels):
            report = ndb.get_validation_report(position)
            report.raise_for_errors()
            validation[label] = {
                "ruleset_version": report.ruleset_version,
                "errors": len(report.errors),
                "warnings": len(report.warnings),
            }
        ndb.write_db_to_brightway(names)
        del ndb

    rows = []
    for label, name in zip(scenario_labels, names):
        if name not in bd.databases:
            raise RuntimeError(f"Output database {name!r} is missing.")
        database = bd.Database(name)
        lca = None
        for activity_key, activity in references["activities"].items():
            node = _find_activity(database, activity)
            if lca is None:
                lca = bc.LCA({node.id: 1}, method)
                lca.lci()
                lca.lcia()
            else:
                lca.redo_lcia({node.id: 1})
            score = float(lca.score)
            if not math.isfinite(score):
                raise RuntimeError(f"Non-finite score for {label}/{activity_key}.")
            expected = case["scores"][label][activity_key]
            tolerance = references["tolerance"]
            matches = abs(score - expected) <= max(
                tolerance["absolute"], tolerance["relative"] * abs(expected)
            )
            rows.append(
                {
                    "scenario": label,
                    "activity": activity_key,
                    "score": score,
                    "expected": expected,
                    "relative_change": (score / expected - 1) if expected else None,
                    "matches_reference": matches,
                }
            )
            print(f"{args.case}/{label}/{activity_key}: {score:.15g}", flush=True)
        del lca
    result = {
        "case": args.case,
        "git_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "environment": {
            "premise_source_version": ".".join(map(str, premise.__version__)),
            "premise_source_path": premise.__file__,
            "installed_distributions": {
                name: importlib.metadata.version(name)
                for name in ("premise", "bw2data", "bw2calc", "bw2io")
            },
        },
        "biosphere": biosphere,
        "system_model": system_model,
        "functional_unit_amount": 1,
        "method": list(method),
        "method_unit": bd.Method(method).metadata.get("unit"),
        "scenarios": {label: SCENARIOS[label] for label in scenario_labels},
        "databases": dict(zip(scenario_labels, names)),
        "validation": validation,
        "tolerance": references["tolerance"],
        "scores": rows,
        "wall_seconds": time.perf_counter() - start,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        f"Recorded {len(rows)} scores; "
        f"{sum(not row['matches_reference'] for row in rows)} differ from references. "
        f"Report: {args.output}",
        flush=True,
    )
    if args.check:
        # The fixture uses test1/test2/test3; point those expectations at the
        # isolated audit outputs without renaming or replacing any database.
        mapped = copy.deepcopy(references)
        mapped["cases"][args.case]["scores"] = {
            name: case["scores"][label] for label, name in zip(scenario_labels, names)
        }
        original_loader = lcia_regression._load_reference_scores
        try:
            lcia_regression._load_reference_scores = lambda: mapped
            lcia_regression.assert_lcia_regression_scores(args.case, names)
        finally:
            lcia_regression._load_reference_scores = original_loader
        print("Integration-test LCIA assertion passed.", flush=True)


if __name__ == "__main__":
    main()
