#!/usr/bin/env python3
"""Validate the eight-database IMAGE SSP2_C400 study ladder."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path

import bw2calc as bc
import bw2data as bd

PROJECT = "ecoinvent-3.12-cutoff"
PREFIX = "ei_cutoff_3.12_image_SSP2_C400"
STAGES = [
    ("2025_baseline", 2025),
    ("2060_biomass_electricity", 2060),
    ("2060_biomass_electricity_heat", 2060),
    ("2060_biomass_electricity_heat_steel", 2060),
    ("2060_biomass_electricity_heat_steel_cement", 2060),
    ("2060_biomass_electricity_heat_steel_cement_transport", 2060),
    ("2060_all", 2060),
    ("2060_all_cdr_market", 2060),
]

PRODUCT = {
    "name": "transport, passenger car, battery electric, Medium",
    "reference product": "transport, passenger car",
    "location": "WEU",
    "unit": "kilometer",
}

METHODS = {
    "GWP100 incl. biogenic CO2": (
        "ecoinvent-3.12",
        "IPCC 2021 (incl. biogenic CO2)",
        "climate change: total (incl. biogenic CO2)",
        "global warming potential (GWP100)",
    ),
    "Freshwater extraction": (
        "ecoinvent-3.12",
        "Inventory results and indicators",
        "resources",
        "total freshwater extraction",
    ),
    "Cumulative energy demand": (
        "ecoinvent-3.12",
        "Cumulative Energy Demand (CED)",
        "total",
        "energy content (HHV)",
    ),
    "Metals/minerals depletion": (
        "ecoinvent-3.12",
        "EF v3.1",
        "material resources: metals/minerals",
        "abiotic depletion potential (ADP): elements (ultimate reserves)",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument("--prefix", default=PREFIX)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def activity_identity(activity) -> tuple[str | None, ...]:
    return (
        activity.get("name"),
        activity.get("reference product"),
        activity.get("location"),
        activity.get("unit"),
    )


def find_product(database: bd.Database):
    matches = [
        activity
        for activity in database
        if all(activity.get(field) == value for field, value in PRODUCT.items())
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one product match in {database.name}; found {len(matches)}"
        )
    return matches[0]


def score_product(activity) -> dict[str, dict[str, float | str]]:
    first_method = next(iter(METHODS.values()))
    lca = bc.LCA({activity: 1.0}, first_method)
    lca.lci()
    scores = {}
    for label, method in METHODS.items():
        lca.switch_method(method)
        lca.lcia()
        scores[label] = {
            "score": float(lca.score),
            "unit": bd.Method(method).metadata.get("unit") or "",
        }
    return scores


def main() -> None:
    args = parse_args()
    bd.projects.set_current(args.project)

    databases = {slug: f"{args.prefix}_{slug}" for slug, _ in STAGES}
    missing_databases = [
        name for name in databases.values() if name not in bd.databases
    ]
    if missing_databases:
        raise RuntimeError(f"Missing databases: {missing_databases}")

    missing_methods = [
        method for method in METHODS.values() if method not in bd.methods
    ]
    if missing_methods:
        raise RuntimeError(f"Missing methods: {missing_methods}")

    summaries = []
    for slug, expected_year in STAGES:
        name = databases[slug]
        metadata = dict(bd.databases[name])
        representative_time = str(metadata.get("representative_time", ""))
        if not representative_time.startswith(str(expected_year)):
            raise RuntimeError(
                f"Unexpected representative time for {name}: {representative_time}"
            )
        for field, expected in {
            "iam_model": "image",
            "pathway": "SSP2_C400",
            "ecoinvent_version": "3.12",
            "system_model": "cutoff",
        }.items():
            if metadata.get(field) != expected:
                raise RuntimeError(
                    f"Unexpected {field} for {name}: {metadata.get(field)!r}"
                )
        summaries.append(
            {
                "stage": slug,
                "database": name,
                "activity_count": len(bd.Database(name)),
                "representative_time": representative_time,
            }
        )

    base = bd.Database(databases["2060_all"])
    compensated = bd.Database(databases["2060_all_cdr_market"])
    base_identities = Counter(activity_identity(activity) for activity in base)
    compensated_identities = Counter(
        activity_identity(activity) for activity in compensated
    )
    if base_identities != compensated_identities:
        raise RuntimeError("The final database pair has different activity identities")

    base_allocated_count = sum(
        "amount of CDR input" in (activity.get("log parameters") or {})
        for activity in base
    )
    allocated = [
        activity
        for activity in compensated
        if "amount of CDR input" in (activity.get("log parameters") or {})
    ]
    if base_allocated_count:
        raise RuntimeError("Uncompensated database contains CDR allocation parameters")
    if not allocated:
        raise RuntimeError("Compensated database contains no CDR allocations")

    invalid_allocations = []
    for activity in allocated:
        amount = float(activity["log parameters"]["amount of CDR input"])
        exchanges = [
            exchange
            for exchange in activity.technosphere()
            if exchange.get("name") == "market for carbon dioxide removal"
        ]
        if len(exchanges) != 1 or not math.isclose(
            float(exchanges[0]["amount"]) if exchanges else math.nan,
            amount,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            invalid_allocations.append(repr(activity.key))
    if invalid_allocations:
        raise RuntimeError(
            f"Invalid CDR exchanges for {len(invalid_allocations)} activities"
        )

    cdr_markets = [
        activity
        for activity in compensated
        if activity.get("name") == "market for carbon dioxide removal"
        and activity.get("reference product") == "carbon dioxide, captured and stored"
        and activity.get("unit") == "kilogram"
    ]

    base_activity = find_product(base)
    compensated_activity = find_product(compensated)
    base_scores = score_product(base_activity)
    compensated_scores = score_product(compensated_activity)
    score_comparison = {}
    for label in METHODS:
        base_score = base_scores[label]["score"]
        compensated_score = compensated_scores[label]["score"]
        score_comparison[label] = {
            "unit": base_scores[label]["unit"],
            "uncompensated": base_score,
            "cdr_compensated": compensated_score,
            "absolute_change": compensated_score - base_score,
            "relative_change_percent": (
                100 * (compensated_score - base_score) / abs(base_score)
                if base_score
                else None
            ),
        }

    result = {
        "project": args.project,
        "databases": summaries,
        "final_pair_identity_counts_match": True,
        "allocated_dataset_count": len(allocated),
        "invalid_allocation_count": 0,
        "cdr_market_count": len(cdr_markets),
        "cdr_market_locations": sorted(
            activity.get("location") for activity in cdr_markets
        ),
        "lcia_smoke_product": PRODUCT,
        "lcia_smoke_scores": score_comparison,
    }
    rendered = json.dumps(result, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
