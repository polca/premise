"""Build reviewable slug mappings from existing sector definitions and a CLIC snapshot.

Only names and reference products are compared without case. Units and
locations retain their exact meaning. This is a maintainer tool: runtime
selection uses the resulting explicit slug registry, never fuzzy matching.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from premise.inventory.clic_archive import ClicArchive

SECTOR_FILES = {
    "electricity": "electricity.yaml",
    "fuels": "fuels.yaml",
    "heat": "heat.yaml",
    "cdr": "carbon_dioxide_removal.yaml",
    "biomass": "biomass.yaml",
    "steel": "steel.yaml",
    "cement": "cement.yaml",
    "final energy": "final_energy.yaml",
    "cars": "transport_passenger_cars.yaml",
    "two_wheelers": "transport_two_wheelers.yaml",
    "buses": "transport_bus.yaml",
    "trucks": "transport_road_freight.yaml",
    "trains": "transport_rail_freight.yaml",
    "ships": "transport_sea_freight.yaml",
}


def matches(record, rule):
    def criteria(value):
        if isinstance(value, (str, list)):
            return {"name": value}
        return value or {}

    def contains(field, needles):
        actual = record.get(field)
        if not isinstance(actual, str):
            return False
        needles = needles if isinstance(needles, list) else [needles]
        if field in {"name", "reference product", "product"}:
            actual = actual.casefold()
            needles = [v.casefold() for v in needles]
        return any(value in actual for value in needles)

    return all(
        contains(field, needles)
        for field, needles in criteria(rule.get("fltr")).items()
    ) and not any(
        contains(field, needles)
        for field, needles in criteria(rule.get("mask")).items()
    )


def build(archive, mapping_dir):
    records = [
        {
            **v,
            "reference product": v["reference_product"],
            "slug": archive.datasets[did]["slug"],
        }
        for did, v in archive.latest.items()
    ]
    mappings = {}
    for filename in dict.fromkeys(SECTOR_FILES.values()):
        definitions = yaml.safe_load((mapping_dir / filename).read_text())
        mappings[filename] = {
            role: sorted(
                record["slug"]
                for record in records
                if matches(record, definition["ecoinvent_aliases"])
            )
            for role, definition in definitions.items()
            if "ecoinvent_aliases" in definition
        }
    # Battery scenario markets are constructed from IAM shares. These are their
    # physical capacity suppliers, not the scenario-specific market scaffolds.
    batteries = sorted(
        r["slug"]
        for r in records
        if r["name"].startswith("market for battery capacity")
    )
    return {
        "schema_version": 1,
        "review_source": {
            "archive_fingerprint": archive.manifest["fingerprint"],
            "created_at": archive.manifest["created_at"],
            "method": "Existing sector name/product filters; units and locations exact; explicit published slugs.",
        },
        "sector_mappings": {
            sector: [filename] for sector, filename in SECTOR_FILES.items()
        },
        "mappings": mappings,
        "support": {
            "cement": [
                "carbon-dioxide-captured-at-cement-production-plant-using-direct-separation",
                "carbon-dioxide-captured-at-cement-production-plant-using-oxyfuel",
                "carbon-dioxide-captured-at-cement-production-plant-using-monoethanolamine",
            ],
            "fuels": ["hydrogen-supply-distributed-by-pipeline"],
            "battery": batteries,
        },
    }


def format_registry(value, depth=0):
    """Keep one generated activity per line, with role/sector sections readable."""
    indent = "  " * depth
    if isinstance(value, dict) and depth < 3:
        lines = [
            "  " * (depth + 1)
            + json.dumps(key)
            + ": "
            + format_registry(item, depth + 1)
            for key, item in value.items()
        ]
        return "{\n" + ",\n".join(lines) + "\n" + indent + "}"
    if isinstance(value, list) and value and isinstance(value[0], dict) and depth < 3:
        return (
            "[\n"
            + ",\n".join(
                "  " * (depth + 1) + format_registry(item, depth + 1) for item in value
            )
            + "\n"
            + indent
            + "]"
        )
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument(
        "--mapping-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "premise/iam_variables_mapping",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    registry = build(ClicArchive.read(args.archive), args.mapping_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(format_registry(registry) + "\n")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "roots_by_mapping": {
                    filename: len(
                        {slug for values in roles.values() for slug in values}
                    )
                    for filename, roles in registry["mappings"].items()
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
