#!/usr/bin/env python3
"""Score source/before/after audit databases with RELICS and climate change.

Only aggregated results and identities are exported. RELICS CFs are registered
in the isolated audit project against the explicitly selected biosphere.
"""

import argparse
import csv
import hashlib
import importlib.metadata
import json
from pathlib import Path


def write_csv(path, rows):
    if not rows:
        return
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--biosphere", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--reuse-scores",
        type=Path,
        help="Reuse unchanged source/before scores, verified by processed-database hashes.",
    )
    args = parser.parse_args()
    import bw2data as bd
    import bw2calc as bc
    from relics.utils import RELICS_MAPPING, load_mappings

    bd.projects.set_current(args.project)
    for name in (args.biosphere, args.source, args.before, args.after):
        if name not in bd.databases:
            raise ValueError(f"Missing database {name!r}")
    args.output.mkdir(parents=True, exist_ok=True)
    flows = list(bd.Database(args.biosphere))
    methods, coverage = [], []
    for mapping in load_mappings(RELICS_MAPPING):
        cfs = {}
        for rule in mapping["environmental flow"]:
            matches = [
                f
                for f in flows
                if f["name"].lower().startswith(rule["name"].lower())
                and tuple(f["categories"]) == tuple(rule["categories"].split("::"))
            ]
            coverage.append(
                {
                    "indicator": mapping["name"],
                    "flow pattern": rule["name"],
                    "matched flows": len(matches),
                    "factor": rule["amount"],
                }
            )
            for flow in matches:
                if flow.key in cfs and cfs[flow.key] != rule["amount"]:
                    raise ValueError(f"Conflicting RELICS factors for {flow.key}")
                cfs[flow.key] = rule["amount"]
        key = ("RELICS", "metals extraction", mapping["name"])
        method = bd.Method(key)
        if key not in bd.methods:
            method.register(
                unit="kilogram",
                description=f"RELICS extraction of {mapping['name']} from the ground",
            )
        method.write(list(cfs.items()))
        if cfs:
            methods.append(key)
    climate = (
        "ecoinvent-3.12",
        "IPCC 2013",
        "climate change",
        "global warming potential (GWP100)",
    )
    if climate not in bd.methods:
        raise ValueError(f"Missing climate method {climate!r}")
    methods.append(climate)
    specifications = {
        "motor_powertrain": (
            "electric motor production, vehicle (electric powertrain)",
            "electric motor, electric passenger car",
            ["GLO"],
        ),
        "motor_scooter": (
            "electric motor production, for electric scooter",
            "electric motor, for electric scooter",
            ["GLO"],
        ),
        "magnet": (
            "permanent magnet production, for electric motor",
            "permanent magnet, for electric motor",
            ["GLO"],
        ),
        "car_diesel": (
            "passenger car production, diesel",
            "passenger car, diesel",
            ["GLO"],
        ),
        "car_petrol": (
            "passenger car production, petrol/natural gas",
            "passenger car, petrol/natural gas",
            ["GLO"],
        ),
        "copper": ("market for copper, cathode", "copper, cathode", ["World", "GLO"]),
        "gold": ("market for gold", "gold", ["World", "GLO"]),
        "electricity_ch": (
            "market for electricity, low voltage",
            "electricity, low voltage",
            ["CH"],
        ),
    }
    scores, identities = [], []
    fingerprints = {}
    cached_scores, cached_identities, cached_metadata = [], [], {}
    if args.reuse_scores:
        cached_metadata = json.loads((args.reuse_scores / "metadata.json").read_text())
        if (
            cached_metadata.get("relics_mapping_sha256")
            != hashlib.sha256(Path(RELICS_MAPPING).read_bytes()).hexdigest()
        ):
            raise ValueError("Cached RELICS mapping differs.")
        with (args.reuse_scores / "scores.csv").open() as stream:
            cached_scores = list(csv.DictReader(stream))
        with (args.reuse_scores / "activities.csv").open() as stream:
            cached_identities = list(csv.DictReader(stream))
    reused = []
    for stage, name in (
        ("source", args.source),
        ("before", args.before),
        ("after", args.after),
    ):
        fingerprints[name] = hashlib.sha256(
            Path(bd.Database(name).filepath_processed()).read_bytes()
        ).hexdigest()
        if (
            stage in {"source", "before"}
            and cached_metadata.get(stage) == name
            and cached_metadata.get("database_fingerprints", {}).get(name)
            == fingerprints[name]
        ):
            rows = [r for r in cached_scores if r["stage"] == stage]
            if len(rows) != len(specifications) * len(methods):
                raise ValueError("Incomplete cached scores.")
            scores.extend({**r, "score": float(r["score"])} for r in rows)
            identities.extend(r for r in cached_identities if r["stage"] == stage)
            reused.append(stage)
            continue
        nodes = list(bd.Database(name))
        activities = {}
        for label, (activity_name, product, locations) in specifications.items():
            matches = []
            for location in locations:
                matches = [
                    a
                    for a in nodes
                    if a["name"] == activity_name
                    and a.get("reference product") == product
                    and a.get("location") == location
                ]
                if matches:
                    break
            if len(matches) != 1:
                raise ValueError(f"Expected unique {label} in {name}: {len(matches)}")
            activity = matches[0]
            activities[label] = activity
            identities.append(
                {
                    "stage": stage,
                    "label": label,
                    "database": name,
                    "code": activity["code"],
                    "name": activity["name"],
                    "product": activity["reference product"],
                    "location": activity["location"],
                    "unit": activity["unit"],
                    "functional unit": 1,
                }
            )
        lca = bc.LCA({next(iter(activities.values())).id: 1}, methods[0])
        lca.lci(factorize=True)
        lca.lcia()
        matrices = {}
        for method in methods:
            lca.switch_method(method)
            matrices[method] = lca.characterization_matrix
        for label, activity in activities.items():
            lca.redo_lcia({activity.id: 1})
            for method in methods:
                lca.characterization_matrix = matrices[method]
                lca.lcia()
                scores.append(
                    {
                        "stage": stage,
                        "activity": label,
                        "method": json.dumps(method),
                        "unit": bd.methods[method].get("unit"),
                        "score": float(lca.score),
                    }
                )
        print(
            f"Scored {stage}: {len(activities)} activities, {len(methods)} indicators",
            flush=True,
        )
    index = {(r["stage"], r["activity"], r["method"]): r["score"] for r in scores}
    differences = []
    for row in scores:
        if row["stage"] != "after":
            continue
        before = index[("before", row["activity"], row["method"])]
        source = index[("source", row["activity"], row["method"])]
        differences.append(
            {
                "activity": row["activity"],
                "method": row["method"],
                "unit": row["unit"],
                "source": source,
                "before": before,
                "after": row["score"],
                "change": row["score"] - before,
                "relative change": (
                    (row["score"] - before) / abs(before) if before else None
                ),
            }
        )
    write_csv(args.output / "scores.csv", scores)
    write_csv(args.output / "differences.csv", differences)
    write_csv(args.output / "activities.csv", identities)
    write_csv(args.output / "relics_coverage.csv", coverage)
    metadata = vars(args) | {
        "relics_version": importlib.metadata.version("relics"),
        "relics_mapping_sha256": hashlib.sha256(
            Path(RELICS_MAPPING).read_bytes()
        ).hexdigest(),
        "bw2calc_version": importlib.metadata.version("bw2calc"),
        "characterized_indicators": len(methods),
        "unmatched_patterns": sum(not row["matched flows"] for row in coverage),
        "database_fingerprints": fingerprints,
        "reused_stages": reused,
    }
    (args.output / "metadata.json").write_text(
        json.dumps(metadata, default=str, indent=2)
    )


if __name__ == "__main__":
    main()
