"""Re-export an existing local 3.8 scenario through Premise (licensed data stay local).

Run from an isolated directory with variables.yaml redirecting USER_DATA_BASE_DIR.
This restores an existing scenario; it does not rebuild or decrypt IAM data.
"""

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import bw2data as bd

from premise import NewDatabase
from premise.clean_datasets import extract_brightway_databases_for_premise
from premise.geomap import Geomap
from premise.inventory_store import create_inventory_store
from premise.new_database import _normalize_inventory_before_certification


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="ecoinvent-3.8-cutoff")
    parser.add_argument("--database", default="test1")
    parser.add_argument("--method-package", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if args.project not in bd.projects:
        raise ValueError(f"Missing project: {args.project}")
    bd.projects.set_current(args.project)
    metadata = bd.databases[args.database]
    if metadata.get("ecoinvent_version") != "3.8":
        raise ValueError(
            "The selected database must explicitly identify ecoinvent 3.8."
        )
    model, pathway = metadata["iam_model"], metadata["pathway"]
    year = int(metadata["representative_time"][:4])
    data = extract_brightway_databases_for_premise(args.database)
    before_count = sum(len(ds["exchanges"]) for ds in data)
    _normalize_inventory_before_certification(data)
    after_count = sum(len(ds["exchanges"]) for ds in data)
    print(
        "Standard Premise normalization consolidated",
        before_count - after_count,
        "duplicate deterministic exchanges; summed amounts are retained.",
        flush=True,
    )
    ndb = NewDatabase.__new__(NewDatabase)
    ndb.version = "3.8"
    ndb.system_model = metadata["system_model"]
    # This is a re-export, not a new IAM transformation. The restored graph is
    # the baseline; its existing cycles are not newly introduced by export.
    ndb.source = args.database
    ndb.biosphere_name = "ecoinvent-3.8-biosphere"
    ndb._validation_enabled = True
    ndb.generate_reports = False
    scenario = {
        "model": model,
        "pathway": pathway,
        "year": year,
        "database": data,
        "iam data": SimpleNamespace(regions=Geomap(model).iam_regions),
    }
    scenario["_inventory_store"] = create_inventory_store(
        data,
        backend="compact",
        scenario_identity=(model, pathway, year, ()),
        take_ownership=False,
    )
    ndb._source_inventory_store = scenario["_inventory_store"]
    ndb.scenarios = [scenario]
    args.output.mkdir(parents=True, exist_ok=True)
    report = ndb._ensure_semantic_certification(scenario)
    (args.output / "semantic-validation.json").write_text(
        json.dumps(report.to_dict(), indent=2)
    )
    print("Certified", args.database, len(data), flush=True)
    if args.prepare_only:
        return
    paths = ndb.write_db_to_olca(args.output, method_package=args.method_package)
    print("Exported", paths, flush=True)


if __name__ == "__main__":
    main()
