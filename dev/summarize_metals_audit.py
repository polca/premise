#!/usr/bin/env python3
"""Summarize private before/after snapshots without redistributing inventories."""

import argparse
import csv
import hashlib
import json
import math
import pickle
from collections import Counter, defaultdict
from pathlib import Path

import yaml


def identity(dataset):
    return tuple(
        dataset.get(k) for k in ("name", "reference product", "location", "unit")
    )


def inventory(path):
    with path.open("rb") as stream:
        return {identity(ds): ds for ds in pickle.load(stream)}


def csv_file(path, rows):
    if rows:
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def summarize_regressions(output):
    references_path = (
        Path(__file__).resolve().parents[1] / "tests/data/lcia_regression_scores.yaml"
    )
    references = yaml.safe_load(references_path.read_text())
    tolerance = references["tolerance"]
    rows = []
    for system in ("cutoff", "consequential"):
        before_path = output / f"regression-before-{system}.json"
        after_path = output / f"regression-{system}.json"
        if not before_path.exists() or not after_path.exists():
            continue
        before = json.loads(before_path.read_text())
        after = json.loads(after_path.read_text())
        index = {(r["scenario"], r["activity"]): r["score"] for r in before["scores"]}
        for row in after["scores"]:
            baseline = index[row["scenario"], row["activity"]]
            expected = references["cases"][after["case"]]["scores"][row["scenario"]][
                row["activity"]
            ]
            rows.append(
                {
                    "case": after["case"],
                    "scenario": row["scenario"],
                    "activity": row["activity"],
                    "previous reference": row["expected"],
                    "baseline": baseline,
                    "corrected": row["score"],
                    "change from baseline": (
                        row["score"] / baseline - 1 if baseline else None
                    ),
                    "current reference": expected,
                    "matches current reference": math.isfinite(row["score"])
                    and abs(row["score"] - expected)
                    <= max(
                        tolerance["absolute"], tolerance["relative"] * abs(expected)
                    ),
                }
            )
    if rows:
        csv_file(output / "regression_comparison.csv", rows)
        (output / "reference_check.json").write_text(
            json.dumps(
                {
                    "reference_sha256": hashlib.sha256(
                        references_path.read_bytes()
                    ).hexdigest(),
                    "tolerance": tolerance,
                    "scores_checked": len(rows),
                    "mismatches": sum(not r["matches current reference"] for r in rows),
                    "note": "Recompares recorded scores; does not repeat the LCA solves.",
                },
                indent=2,
            )
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    baseline = [
        json.loads(path.read_text())
        for path in sorted(args.snapshots.glob("before-*/metadata.json"))
    ]
    (args.output / "baseline_builds.json").write_text(json.dumps(baseline, indent=2))
    builds = []
    for path in sorted(args.snapshots.glob("final-*/metadata.json")):
        metadata = json.loads(path.read_text())
        folder = path.parent
        decisions = json.loads((folder / "material_decisions.json").read_text())
        diagnostics = json.loads((folder / "market_diagnostics.json").read_text())
        resources = json.loads((folder / "resource_diagnostics.json").read_text())
        validation = json.loads((folder / "validation.json").read_text())
        issues = [
            issue
            for phase in validation
            for rule in phase.get("rule_results", [])
            for issue in rule.get("issues", [])
        ]
        builds.append(
            {
                **metadata,
                "material_update_metrics": json.loads(
                    (folder / "material_update_metrics.json").read_text()
                ),
                "material_decisions": dict(
                    Counter(d["reason code"] for d in decisions)
                ),
                "market_diagnostics": dict(Counter(d["reason"] for d in diagnostics)),
                "resource_diagnostics": dict(Counter(d["reason"] for d in resources)),
                "validation_errors": [
                    issue for issue in issues if issue.get("severity") == "error"
                ],
            }
        )
    (args.output / "builds.json").write_text(json.dumps(builds, indent=2))
    summaries, market_rows, sample_rows, coverage_rows = [], [], [], []
    for system in ("cutoff", "consequential"):
        before = inventory(args.snapshots / f"before-{system}" / "inventory.pickle")
        after_folder = args.snapshots / f"final-{system}"
        after = inventory(after_folder / "inventory.pickle")
        common = before.keys() & after.keys()
        decisions = json.loads((after_folder / "material_decisions.json").read_text())
        decision_index = {
            (identity(d["activity"]), d.get("provider product")): d for d in decisions
        }
        resource_changes = Counter()
        for key in sorted(common):

            def resources(ds):
                return {
                    (e["name"], tuple(e.get("categories", [])), e["unit"]): e["amount"]
                    for e in ds["exchanges"]
                    if e["type"] == "biosphere"
                    and tuple(e.get("categories", []))
                    == ("natural resource", "in ground")
                }

            old, new = resources(before[key]), resources(after[key])
            for flow in old.keys() | new.keys():
                a, b = old.get(flow, 0), new.get(flow, 0)
                if a != b:
                    resource_changes["increased" if b > a else "decreased"] += 1
            if key[0] not in {
                "electric motor production, vehicle (electric powertrain)",
                "permanent magnet production, for electric motor",
                "internal combustion engine production, passenger car",
            }:
                continue

            def inputs(ds):
                result = defaultdict(float)
                for e in ds["exchanges"]:
                    if e["type"] == "technosphere" and e.get("unit") == "kilogram":
                        result[e["product"]] += e["amount"]
                return result

            old, new = inputs(before[key]), inputs(after[key])
            for product in sorted(old.keys() | new.keys()):
                if old[product] == new[product]:
                    continue
                decision = decision_index.get((key, product), {})
                sample_rows.append(
                    {
                        "system_model": system,
                        "activity": key[0],
                        "product": key[1],
                        "location": key[2],
                        "input": product,
                        "unit": "kg per " + key[3],
                        "before": old[product],
                        "after": new[product],
                        "reason": decision.get("reason code", "supplier relinking"),
                        "rule": decision.get("material rule id"),
                    }
                )
        shares = json.loads((after_folder / "applied_market_shares.json").read_text())
        missing_transport = defaultdict(float)
        for diagnostic in json.loads(
            (after_folder / "market_diagnostics.json").read_text()
        ):
            if diagnostic["reason"] == "transport_missing":
                missing_transport[diagnostic["metal"]] += diagnostic["share"]
        for metal, applied in sorted(shares.items()):
            coverage_rows.append(
                {
                    "system_model": system,
                    "metal": metal,
                    "primary share": applied["primary share"],
                    "primary share with transport": max(
                        0, applied["primary share"] - missing_transport[metal]
                    ),
                    "primary share without transport": missing_transport[metal],
                }
            )
            key = (f"market for {metal}", metal, "World", "kilogram")

            def transport(ds):
                result = defaultdict(float)
                for exc in ds.get("exchanges", []):
                    if exc["type"] == "technosphere" and (
                        "transport, freight" in exc["name"]
                        or "aircraft, belly freight" in exc["name"]
                    ):
                        result[(exc["name"], exc["unit"])] += exc["amount"]
                return result

            old = transport(before.get(key, {}))
            new = transport(after[key])
            for supplier, unit in sorted(old.keys() | new.keys()):
                market_rows.append(
                    {
                        "system_model": system,
                        "metal": metal,
                        "freight_provider": supplier,
                        "unit": unit,
                        "before": old[(supplier, unit)],
                        "after": new[(supplier, unit)],
                        "primary_share": applied["primary share"],
                        "secondary_share": applied["secondary share"],
                    }
                )
        summaries.append(
            {
                "system_model": system,
                "common_activities": len(common),
                "added_activities": len(after.keys() - before.keys()),
                "removed_activities": len(before.keys() - after.keys()),
                "resource_exchange_changes": dict(resource_changes),
            }
        )
    csv_file(args.output / "selected_material_changes.csv", sample_rows)
    csv_file(args.output / "market_transport.csv", market_rows)
    csv_file(args.output / "transport_coverage.csv", coverage_rows)
    (args.output / "inventory_summary.json").write_text(json.dumps(summaries, indent=2))
    compact = inventory(args.snapshots / "final-cutoff" / "inventory.pickle")
    legacy = inventory(args.snapshots / "final-legacy-cutoff" / "inventory.pickle")

    def canonical(ds):
        return sorted(json.dumps(e, sort_keys=True) for e in ds["exchanges"])

    differing = [
        key
        for key in compact.keys() & legacy.keys()
        if canonical(compact[key]) != canonical(legacy[key])
    ]
    backend = {
        "same_activities": compact.keys() == legacy.keys(),
        "different_exchange_lists": len(differing),
        "examples": differing[:5],
    }
    (args.output / "backend_comparison.json").write_text(json.dumps(backend, indent=2))
    summarize_regressions(args.output)
    print(json.dumps({"builds": len(builds), "backend": backend}))


if __name__ == "__main__":
    main()
