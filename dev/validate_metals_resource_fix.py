#!/usr/bin/env python3
"""Compare fresh resource-fix builds with the audited pre-fix databases.

Run validate_metals_correctness.py with labels resource-fix-cutoff and
resource-fix-consequential first. Only aggregate results enter the repository.
"""

import argparse
import csv
import hashlib
import json
import math
import pickle
import runpy
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def key(ds):
    return ds["name"], ds["reference product"], ds["location"]


def is_resource(exc):
    return (
        exc["type"] == "biosphere"
        and exc.get("unit") == "kilogram"
        and tuple(exc.get("categories", ())) == ("natural resource", "in ground")
    )


def inventories(private, system, output):
    stages = []
    for label in ("final", "resource-fix"):
        with (private / f"{label}-{system}" / "inventory.pickle").open("rb") as stream:
            rows = pickle.load(stream)
        index = {key(ds): ds for ds in rows}
        if len(index) != len(rows):
            raise ValueError("Duplicate dataset identities")
        stages.append(index)
    before, after = stages
    assert before.keys() == after.keys(), "Dataset coverage changed"
    changes = []
    for identity, ds in before.items():
        new = after[identity]

        def other_exchanges(d):
            return Counter(
                json.dumps(e, sort_keys=True)
                for e in d["exchanges"]
                if not is_resource(e)
            )

        assert ds["unit"] == new["unit"]
        assert other_exchanges(ds) == other_exchanges(
            new
        ), f"Non-resource exchanges changed: {identity}"

        def resources(d):
            result = Counter()
            for e in d["exchanges"]:
                if is_resource(e):
                    result[e["name"]] += e["amount"]
            return result

        old_resources, new_resources = resources(ds), resources(new)
        for name in old_resources.keys() | new_resources.keys():
            old, new_amount = old_resources[name], new_resources[name]
            if old != new_amount:
                changes.append(
                    {
                        "name": identity[0],
                        "product": identity[1],
                        "location": identity[2],
                        "resource": name,
                        "before": old,
                        "after": new_amount,
                    }
                )
    with (output / "resource_changes.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(changes[0]))
        writer.writeheader()
        writer.writerows(
            sorted(changes, key=lambda r: (r["resource"], r["name"], r["location"]))
        )
    return {
        "datasets": len(before),
        "non_resource_exchanges_unchanged": True,
        "resource_changes": len(changes),
        "changes_by_resource": dict(Counter(r["resource"] for r in changes)),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=("cutoff", "consequential"), required=True)
    parser.add_argument(
        "--private", type=Path, default=Path("/private/tmp/premise-metals-audit")
    )
    parser.add_argument(
        "--reuse-scores",
        action="store_true",
        help="Recheck existing outputs without repeating LCAs",
    )
    parser.add_argument(
        "--verified-label",
        help="Optional no-write rebuild prefix; require complete exchange equivalence with the scored export",
    )
    args = parser.parse_args()
    import bw2data as bd

    original = json.loads(
        (
            ROOT / "results/metals-correctness" / args.system / "metadata.json"
        ).read_text()
    )
    built = json.loads(
        (args.private / f"resource-fix-{args.system}" / "metadata.json").read_text()
    )
    bd.projects.set_current(original["project"])
    after = built["written_database"]
    before = original["after"]
    assert after in bd.databases and before in bd.databases
    old_fingerprint = hashlib.sha256(
        Path(bd.Database(before).filepath_processed()).read_bytes()
    ).hexdigest()
    assert old_fingerprint == original["database_fingerprints"][before]
    fingerprints = {
        before: old_fingerprint,
        after: hashlib.sha256(
            Path(bd.Database(after).filepath_processed()).read_bytes()
        ).hexdigest(),
    }
    config = {
        **original,
        "before": before,
        "after": after,
        "database_fingerprints": fingerprints,
    }
    audit = args.private / f"resource-fix-score-config-{args.system}"
    audit.mkdir(exist_ok=True)
    (audit / "metadata.json").write_text(json.dumps(config, indent=2))
    output = ROOT / "results/metals-correctness/resource-fix" / args.system
    output.mkdir(parents=True, exist_ok=True)
    checks = inventories(args.private, args.system, output)
    print("Inventory checks", checks, flush=True)
    if not args.reuse_scores:
        sys.argv = [
            str(ROOT / "dev/score_metals_markets.py"),
            "--audit",
            str(audit),
            "--output",
            str(output),
            "--skip-saved-score-check",
        ]
        runpy.run_path(sys.argv[0], run_name="__main__")
    metadata = json.loads((output / "metadata.json").read_text())
    assert metadata["database_fingerprints"] == fingerprints
    with (
        ROOT / "results/metals-correctness/markets" / args.system / "all_indicators.csv"
    ).open() as stream:
        old_scores = {
            (r["market product"], r["indicator"]): r for r in csv.DictReader(stream)
        }
    with (output / "all_indicators.csv").open() as stream:
        scores = list(csv.DictReader(stream))
    checked = 0
    for row in scores:
        old = old_scores[row["market product"], row["indicator"]]
        if not row["before"]:
            assert not old["after"]
            continue
        assert math.isclose(
            float(row["before"]), float(old["after"]), rel_tol=1e-9, abs_tol=1e-12
        )
        assert math.isfinite(float(row["after"]))
        checked += 1
    checks["prior_scores_reproduced"] = checked
    checks["scenario"] = built["scenario"]
    checks["source_fingerprint"] = built["source_fingerprint"]
    checks["code_sha256"] = built["code_sha256"]
    checks["configuration_sha256"] = built["configuration_sha256"]
    if args.verified_label:
        verified_path = args.private / f"{args.verified_label}-{args.system}"
        verified = json.loads((verified_path / "metadata.json").read_text())
        with (verified_path / "inventory.pickle").open("rb") as stream:
            verified_inventory = {key(d): d for d in pickle.load(stream)}
        with (args.private / f"resource-fix-{args.system}" / "inventory.pickle").open(
            "rb"
        ) as stream:
            exported_inventory = {key(d): d for d in pickle.load(stream)}
        assert verified_inventory.keys() == exported_inventory.keys()
        for identity, ds in verified_inventory.items():
            old = exported_inventory[identity]
            assert ds["unit"] == old["unit"]
            assert Counter(
                json.dumps(e, sort_keys=True) for e in ds["exchanges"]
            ) == Counter(
                json.dumps(e, sort_keys=True) for e in old["exchanges"]
            ), f"Verified build differs from scored database: {identity}"
        assert (
            verified["code_sha256"]["metals.py"]
            == hashlib.sha256((ROOT / "premise/metals.py").read_bytes()).hexdigest()
        )
        checks["verified_export_equivalent"] = True
        checks["verified_code_sha256"] = verified["code_sha256"]
    (output / "validation.json").write_text(json.dumps(checks, indent=2))
    with (output / "own_resource.csv").open() as stream:
        targets = [
            r
            for r in csv.DictReader(stream)
            if r["market product"] in ("cobalt", "zinc")
        ]
    assert len(targets) == 2
    for row in targets:
        assert (
            0.98 < float(row["after"]) < 1.1
        ), f"Primary resource attribution still incomplete: {row}"
    print(json.dumps(targets, indent=2), flush=True)


if __name__ == "__main__":
    main()
