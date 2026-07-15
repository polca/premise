"""Validate deriving metal secondary supply from existing ecoinvent markets.

This is a read-only Brightway inspection unless ``--ensure-import`` is passed.
It compares the secondary technosphere inputs detected from old ecoinvent
markets with the current YAML fallback values.
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from premise.metals import (
    OLD_METAL_MARKET_LOCATIONS,
    extract_reference_products_from_filter,
    is_secondary_metal_supply_exchange,
    load_mining_shares_mapping,
    load_primary_secondary_split,
)

DEFAULT_VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12")


def activity_text(activity: Any) -> tuple[str, str, str]:
    return (
        activity.get("name"),
        activity.get("reference product"),
        activity.get("location"),
    )


def exchange_as_dict(exchange: Any) -> dict:
    provider = exchange.input
    return {
        "type": exchange.get("type"),
        "amount": float(exchange.get("amount")),
        "unit": provider.get("unit"),
        "name": provider.get("name"),
        "product": provider.get("reference product"),
        "location": provider.get("location"),
    }


def reference_products_from_group(group) -> list[str]:
    products = []
    for value in group["Reference product"].dropna().unique():
        for product in extract_reference_products_from_filter(value):
            if product and product not in products:
                products.append(product)
    return products


def find_old_market(by_name_product: dict, reference_products: list[str]) -> Any | None:
    markets = []
    seen = set()
    for reference_product in reference_products:
        market_name = f"market for {reference_product}"
        for activity in by_name_product.get((market_name, reference_product), []):
            if activity.get("location") not in OLD_METAL_MARKET_LOCATIONS:
                continue
            if activity.id in seen:
                continue
            markets.append(activity)
            seen.add(activity.id)

    if not markets:
        return None

    markets.sort(
        key=lambda activity: OLD_METAL_MARKET_LOCATIONS.index(activity.get("location"))
    )
    return markets[0]


def ensure_database(
    version: str, project: str, database: str, ensure_import: bool
) -> str:
    import bw2data as bd

    existing_projects = {project.name for project in bd.projects}
    if project not in existing_projects and not ensure_import:
        return "missing project"

    bd.projects.set_current(project)
    if database in bd.databases:
        return "available"

    if not ensure_import:
        return "missing database"

    import bw2io

    username = os.environ.get("EI_USERNAME") or os.environ.get("ECOINVENT_USERNAME")
    password = os.environ.get("EI_PASSWORD") or os.environ.get("ECOINVENT_PASSWORD")
    if not username or not password:
        return "missing credentials"

    try:
        bw2io.import_ecoinvent_release(
            version=version,
            system_model="cutoff",
            username=username,
            password=password,
        )
    except Exception as exc:
        return f"import failed: {type(exc).__name__}: {exc}"

    return "imported" if database in bd.databases else "import did not create database"


def validate_version(version: str, project: str, database: str) -> list[dict]:
    import bw2data as bd

    bd.projects.set_current(project)
    db = list(bd.Database(database))
    by_name_product = {}
    for activity in db:
        by_name_product.setdefault(
            (activity.get("name"), activity.get("reference product")), []
        ).append(activity)

    yaml_split = load_primary_secondary_split()
    mining_shares = load_mining_shares_mapping(version)
    mining_shares = mining_shares.loc[mining_shares["Work done"] == "Yes"]
    mining_shares = mining_shares.loc[~mining_shares["Country"].isnull()]

    rows = []
    for metal, group in mining_shares.groupby("Metal"):
        reference_products = reference_products_from_group(group)
        old_market = find_old_market(by_name_product, reference_products)
        candidates = []
        total_kg = 0.0

        if old_market is not None:
            for exchange in old_market.technosphere():
                exchange_dict = exchange_as_dict(exchange)
                if exchange_dict["unit"] == "kilogram":
                    total_kg += exchange_dict["amount"]
                if is_secondary_metal_supply_exchange(
                    exchange_dict, old_market.get("reference product")
                ):
                    candidates.append(exchange_dict)

        derived_secondary = sum(candidate["amount"] for candidate in candidates)
        yaml_secondary = (
            yaml_split.get(metal, {})
            .get("shares", {})
            .get("secondary", {})
            .get(2020, 0.0)
        )
        yaml_secondary = float(yaml_secondary or 0.0)

        rows.append(
            {
                "version": version,
                "metal": metal,
                "reference_products": "|".join(reference_products),
                "old_market": (
                    "" if old_market is None else " | ".join(activity_text(old_market))
                ),
                "yaml_secondary": yaml_secondary,
                "derived_secondary": derived_secondary,
                "delta": derived_secondary - yaml_secondary,
                "candidate_count": len(candidates),
                "total_kg_inputs": total_kg,
                "candidate_exchanges": " || ".join(
                    f"{candidate['amount']:.12g}: {candidate['name']} | "
                    f"{candidate['product']} | {candidate['location']}"
                    for candidate in candidates
                ),
            }
        )

    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--versions", nargs="+", default=list(DEFAULT_VERSIONS))
    parser.add_argument("--ensure-import", action="store_true")
    parser.add_argument(
        "--output",
        default="results/metals_secondary_from_old_markets.csv",
        help="CSV output path.",
    )
    parser.add_argument(
        "--dotenv",
        default=".env",
        help="Dotenv file with EI_USERNAME/EI_PASSWORD for --ensure-import.",
    )
    args = parser.parse_args()

    dotenv_path = Path(args.dotenv)
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_rows = []
    statuses = []
    for version in args.versions:
        project = f"ecoinvent-{version}-cutoff"
        database = f"ecoinvent-{version}-cutoff"
        status = ensure_database(version, project, database, args.ensure_import)
        statuses.append((version, project, database, status))
        if status in {"available", "imported"}:
            all_rows.extend(validate_version(version, project, database))

    fieldnames = [
        "version",
        "metal",
        "reference_products",
        "old_market",
        "yaml_secondary",
        "derived_secondary",
        "delta",
        "candidate_count",
        "total_kg_inputs",
        "candidate_exchanges",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print("version\tproject\tdatabase\tstatus")
    for version, project, database, status in statuses:
        print(f"{version}\t{project}\t{database}\t{status}")

    print("\nsummary for available/imported databases")
    print(
        "version\tmetals\tderived_nonzero\tyaml_nonzero\tfalse_positive\tfalse_negative"
    )
    for version in args.versions:
        rows = [row for row in all_rows if row["version"] == version]
        if not rows:
            continue
        false_positive = [
            row["metal"]
            for row in rows
            if row["derived_secondary"] > 1e-12 and row["yaml_secondary"] == 0
        ]
        false_negative = [
            row["metal"]
            for row in rows
            if row["yaml_secondary"] > 1e-12 and row["derived_secondary"] <= 1e-12
        ]
        print(
            f"{version}\t{len(rows)}\t"
            f"{sum(row['derived_secondary'] > 1e-12 for row in rows)}\t"
            f"{sum(row['yaml_secondary'] > 1e-12 for row in rows)}\t"
            f"{','.join(false_positive)}\t{','.join(false_negative)}"
        )

    print(f"\nwrote {output_path}")


if __name__ == "__main__":
    main()
