#!/usr/bin/env python3
"""Build an isolated metals audit; raw licensed snapshots stay outside the repo.

Use the same arguments and source project for before/after builds, changing only
--repo and --label. PREMISE_KEY is required. Install RELICS separately for LCIA.
"""

import argparse
import hashlib
import json
import os
import pickle
import sys
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--biosphere", required=True)
    parser.add_argument("--version", default="3.12")
    parser.add_argument("--year", type=int, default=2050)
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument("--backend", default="compact", choices=("compact", "legacy"))
    parser.add_argument("--uncertainty", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.repo.resolve()))
    import bw2data as bd
    from premise import NewDatabase

    bd.projects.set_current(args.project)
    for name in (args.source, args.biosphere):
        if name not in bd.databases:
            raise ValueError(f"Missing database {name!r}")
    audit_project = (
        f"metals-correctness-{args.version}-{args.system_model}"
        if args.write
        else args.project
    )
    if args.write and audit_project not in bd.projects:
        bd.projects.copy_project(audit_project)
    bd.projects.set_current(audit_project)
    output = args.output / args.label
    output.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    ndb = NewDatabase(
        scenarios=[{"model": "image", "pathway": "SSP2-M", "year": args.year}],
        source_db=args.source,
        source_version=args.version,
        source_type="brightway",
        biosphere_name=args.biosphere,
        system_model=args.system_model,
        key=os.environ["PREMISE_KEY"].encode(),
        inventory_backend=args.backend,
        keep_source_db_uncertainty=args.uncertainty,
        keep_imports_uncertainty=args.uncertainty,
        use_cached_database=True,
        use_cached_inventories=True,
        generate_reports=False,
        quiet=True,
    )
    update_started = time.perf_counter()
    ndb.update(None if args.full else ["metals"])
    update_seconds = time.perf_counter() - update_started
    database = ndb.materialize_inventory(restore_metadata=True)
    with (output / "inventory.pickle").open("wb") as stream:
        pickle.dump(
            [
                {
                    **{
                        key: ds.get(key)
                        for key in (
                            "name",
                            "reference product",
                            "location",
                            "unit",
                            "code",
                        )
                    },
                    "exchanges": [
                        {
                            key: exc.get(key)
                            for key in (
                                "name",
                                "product",
                                "location",
                                "unit",
                                "type",
                                "amount",
                                "categories",
                                "uncertainty type",
                                "loc",
                                "scale",
                                "minimum",
                                "maximum",
                            )
                            if key in exc
                        }
                        for exc in ds.get("exchanges", [])
                    ],
                }
                for ds in database
            ],
            stream,
        )
    mapping = ndb.scenarios[0].get("mapping", {}).get("metals", {})
    (output / "validation.json").write_text(
        json.dumps(
            ndb.scenarios[0].get("_validation_phase_results", []), indent=2, default=str
        )
    )
    for name in (
        "material decisions",
        "material update metrics",
        "market diagnostics",
        "resource diagnostics",
        "applied market shares",
        "methodology",
    ):
        (output / f"{name.replace(' ', '_')}.json").write_text(
            json.dumps(mapping.get(name, []), indent=2, default=str)
        )
    metadata = vars(args) | {
        "audit_project": audit_project,
        "scenario": {"model": "image", "pathway": "SSP2-M", "year": args.year},
        "seconds": time.perf_counter() - started,
        "update_seconds": update_seconds,
        "source_fingerprint": ndb._validation_source_fingerprint(),
        "code_sha256": {
            name: hashlib.sha256(
                (args.repo / "premise" / name).read_bytes()
            ).hexdigest()
            for name in (
                "metals.py",
                "metals_rules.py",
                "data_collection.py",
                "validation.py",
            )
        },
        "datasets": len(database),
        "configuration_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (args.repo / "premise/data/metals").glob("*")
            if p.suffix in ("yaml", ".yaml", ".csv", ".xlsx", ".yml")
        },
    }
    if args.write:
        name = f"metals-audit-{args.label}"
        if name in bd.databases:
            raise ValueError(f"Refusing to overwrite {name}")
        ndb.write_db_to_brightway(name)
        metadata["written_database"] = name
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str))
    print(
        json.dumps(
            {
                "label": args.label,
                "datasets": len(database),
                "seconds": metadata["seconds"],
            }
        )
    )


if __name__ == "__main__":
    main()
