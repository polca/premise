#!/usr/bin/env python3
"""Compare dataset-wise direct metal inputs in three Brightway databases."""

from __future__ import annotations

import argparse
import json
import platform
import sys
from pathlib import Path

import bw2data as bd

import premise
from premise.metal_input_comparison import (
    compare_direct_metal_inputs,
    write_comparison_bundle,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--source", required=True, help="Unupdated database name")
    parser.add_argument("--before", required=True, help="Updated pre-fix database name")
    parser.add_argument("--after", required=True, help="Updated fixed database name")
    parser.add_argument(
        "--decisions",
        required=True,
        type=Path,
        help="JSON list from the fixed scenario's metals material decisions",
    )
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def database_inventory(name: str) -> list[dict]:
    if name not in bd.databases:
        raise ValueError(f"Brightway database {name!r} does not exist.")
    inventory = []
    for node in bd.Database(name):
        dataset = {
            "name": node.get("name"),
            "reference product": node.get("reference product"),
            "location": node.get("location"),
            "unit": node.get("unit"),
            "code": node.get("code"),
            "exchanges": [],
        }
        for exchange in node.exchanges():
            data = dict(exchange)
            if exchange.get("type") == "technosphere":
                supplier = exchange.input
                data.setdefault("name", supplier.get("name"))
                data.setdefault("product", supplier.get("reference product"))
                data.setdefault("location", supplier.get("location"))
                data.setdefault("unit", supplier.get("unit"))
            dataset["exchanges"].append(data)
        inventory.append(dataset)
    return inventory


def main() -> int:
    args = parse_args()
    bd.projects.set_current(args.project)
    decisions = json.loads(args.decisions.read_text(encoding="utf-8"))
    if not isinstance(decisions, list):
        raise ValueError("--decisions must contain a JSON list.")

    rows = compare_direct_metal_inputs(
        database_inventory(args.source),
        database_inventory(args.before),
        database_inventory(args.after),
        decisions=decisions,
    )
    metadata = {
        "project": args.project,
        "source database": args.source,
        "before database": args.before,
        "after database": args.after,
        "premise version": ".".join(map(str, premise.__version__)),
        "python": sys.version,
        "platform": platform.platform(),
    }
    complete, unexpected, metadata_path = write_comparison_bundle(
        rows, args.output, metadata=metadata, decisions=decisions
    )
    unexpected_count = sum(not row["valid"] for row in rows)
    print(f"Complete comparison: {complete}")
    print(f"Unexpected differences: {unexpected} ({unexpected_count})")
    print(f"Metadata: {metadata_path}")
    return 1 if unexpected_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
