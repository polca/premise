"""Run in a configured Brightway environment with licensed source data."""

import os
from pathlib import Path

import bw2data as bd
from premise import NewDatabase


def main():
    project = os.environ.get("PREMISE_BW_PROJECT", "ecoinvent-3.12-cutoff")
    source = os.environ.get("PREMISE_SOURCE_DB", "ecoinvent-3.12-cutoff")
    biosphere = os.environ.get("PREMISE_BIOSPHERE", "ecoinvent-3.12-biosphere")
    output = os.environ.get("PREMISE_OUTPUT_DB", "docs-example-remind-SSP2-NPi-2025")
    bd.projects.set_current(project)
    if source not in bd.databases or biosphere not in bd.databases:
        raise ValueError("Import the source and matching biosphere before running.")
    if output in bd.databases:
        raise ValueError(f"Output already exists: {output}; choose a new name.")
    scenario = {"model": "remind", "pathway": "SSP2-NPi", "year": 2025}
    if os.environ.get("PREMISE_IAM_DIR"):
        scenario["filepath"] = Path(os.environ["PREMISE_IAM_DIR"])
    ndb = NewDatabase(
        scenarios=[scenario],
        source_db=source,
        source_version="3.12",
        system_model="cutoff",
        inventory_backend="compact",
        biosphere_name=biosphere,
        key=os.environ["PREMISE_KEY"],
    )
    ndb.update()
    report = ndb.get_validation_report(scenario=0)
    report.raise_for_errors()
    store = ndb.get_inventory_store(scenario=0)
    print(f"Scenario inventory: {len(store)} activities")
    ndb.write_db_to_brightway(name=output)
    print(f"Created {output} in {project}")


if __name__ == "__main__":
    main()
