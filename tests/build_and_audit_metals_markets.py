"""Build and audit premise metal markets against source ecoinvent markets.

The script writes one metals-updated Brightway database per ecoinvent cutoff
version. It then checks that each created World metal market has kilogram inputs
that sum to one and that copied secondary-metal providers keep the same share
as in the source ecoinvent market.
"""

from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from premise.metals import (
    OLD_METAL_MARKET_LOCATIONS,
    extract_reference_products_from_filter,
    is_secondary_metal_supply_exchange,
    load_mining_shares_mapping,
)

DEFAULT_VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12")
DEFAULT_MODEL = "remind"
DEFAULT_PATHWAY = "SSP2-NPi"
DEFAULT_YEAR = 2025


def activity_text(activity: Any | None) -> str:
    if activity is None:
        return ""
    return " | ".join(
        str(activity.get(field, ""))
        for field in ("name", "reference product", "location")
    )


def activity_id(activity: Any) -> Any:
    return getattr(activity, "id", getattr(activity, "key", id(activity)))


def exchange_as_dict(exchange: Any) -> dict[str, Any]:
    provider = exchange.input
    return {
        "type": exchange.get("type"),
        "amount": float(exchange.get("amount")),
        "unit": exchange.get("unit") or provider.get("unit"),
        "name": provider.get("name"),
        "product": provider.get("reference product"),
        "location": provider.get("location"),
    }


def exchange_key(exchange: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        exchange.get("name") or "",
        exchange.get("product") or "",
        exchange.get("location") or "",
        exchange.get("unit") or "",
    )


def reference_products_from_group(group) -> list[str]:
    products = []
    for value in group["Reference product"].dropna().unique():
        for product in extract_reference_products_from_filter(value):
            if product and product not in products:
                products.append(product)
    return products


def index_by_name_product(database_name: str) -> dict[tuple[str, str], list[Any]]:
    import bw2data as bd

    index: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for activity in bd.Database(database_name):
        index[(activity.get("name"), activity.get("reference product"))].append(
            activity
        )
    return index


def index_by_market_triple(database_name: str) -> dict[tuple[str, str, str], list[Any]]:
    import bw2data as bd

    index: dict[tuple[str, str, str], list[Any]] = defaultdict(list)
    for activity in bd.Database(database_name):
        index[
            (
                activity.get("name"),
                activity.get("reference product"),
                activity.get("location"),
            )
        ].append(activity)
    return index


def find_old_market(
    by_name_product: dict[tuple[str, str], list[Any]],
    reference_products: list[str],
) -> Any | None:
    markets = []
    seen = set()
    for reference_product in reference_products:
        market_name = f"market for {reference_product}"
        for activity in by_name_product.get((market_name, reference_product), []):
            if activity.get("location") not in OLD_METAL_MARKET_LOCATIONS:
                continue
            if activity_id(activity) in seen:
                continue
            markets.append(activity)
            seen.add(activity_id(activity))

    if not markets:
        return None

    markets.sort(
        key=lambda activity: OLD_METAL_MARKET_LOCATIONS.index(activity.get("location"))
    )
    return markets[0]


def expected_secondary_by_metal(
    version: str,
    source_database: str,
) -> dict[str, dict[str, Any]]:
    by_name_product = index_by_name_product(source_database)
    mining_shares = load_mining_shares_mapping(version)
    mining_shares = mining_shares.loc[mining_shares["Work done"] == "Yes"]
    mining_shares = mining_shares.loc[~mining_shares["Country"].isnull()]

    expected = {}
    for metal, group in mining_shares.groupby("Metal"):
        reference_products = reference_products_from_group(group)
        old_market = find_old_market(by_name_product, reference_products)
        candidates = []

        if old_market is not None:
            reference_product = old_market.get("reference product")
            for exchange in old_market.technosphere():
                exchange_dict = exchange_as_dict(exchange)
                if is_secondary_metal_supply_exchange(exchange_dict, reference_product):
                    candidates.append(exchange_dict)

        expected_by_key: dict[tuple[str, str, str, str], float] = defaultdict(float)
        for candidate in candidates:
            expected_by_key[exchange_key(candidate)] += candidate["amount"]

        expected[metal] = {
            "reference_products": reference_products,
            "old_market": old_market,
            "old_reference_product": (
                None if old_market is None else old_market.get("reference product")
            ),
            "expected_by_key": dict(expected_by_key),
            "expected_secondary": sum(candidate["amount"] for candidate in candidates),
            "expected_candidate_count": len(candidates),
            "candidate_exchanges": " || ".join(
                f"{candidate['amount']:.12g}: {candidate['name']} | "
                f"{candidate['product']} | {candidate['location']}"
                for candidate in candidates
            ),
        }

    return expected


def find_biosphere_database(version: str) -> str:
    import bw2data as bd

    candidates = [
        f"ecoinvent-{version}-biosphere",
        f"ecoinvent {version} biosphere",
        "biosphere3",
    ]
    for candidate in candidates:
        if candidate in bd.databases:
            return candidate

    for database in bd.databases:
        if "biosphere" in database.lower():
            return database

    raise ValueError("No biosphere database found in current Brightway project.")


def load_key(dotenv_path: Path | None) -> str:
    if dotenv_path is not None and dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)

    for variable in ("PREMISE_KEY", "IAM_FILES_KEY"):
        value = os.environ.get(variable)
        if value:
            return value

    raise ValueError(
        "Missing premise IAM decryption key. Set PREMISE_KEY or IAM_FILES_KEY."
    )


def assert_source_database(project: str, source_database: str) -> None:
    import bw2data as bd

    projects = {project.name for project in bd.projects}
    if project not in projects:
        raise ValueError(f"Missing Brightway project: {project}")

    bd.projects.set_current(project)
    if source_database not in bd.databases:
        raise ValueError(
            f"Missing source database {source_database!r} in project {project!r}."
        )


def build_metals_database(
    *,
    version: str,
    source_database: str,
    output_database: str,
    model: str,
    pathway: str,
    year: int,
    key: str,
    biosphere_database: str,
) -> bool:
    from premise import NewDatabase

    ndb = NewDatabase(
        scenarios=[{"model": model, "pathway": pathway, "year": year}],
        source_db=source_database,
        source_version=version,
        source_type="brightway",
        key=key,
        system_model="cutoff",
        biosphere_name=biosphere_database,
        generate_reports=False,
        quiet=True,
        use_cached_database=True,
        use_cached_inventories=True,
        keep_imports_uncertainty=True,
        keep_source_db_uncertainty=False,
    )
    ndb.update(["metals"])
    ndb.write_db_to_brightway(output_database)

    return False


def audit_written_database(
    *,
    version: str,
    project: str,
    source_database: str,
    output_database: str,
    expected: dict[str, dict[str, Any]],
    route_yaml_used: bool,
) -> list[dict[str, Any]]:
    built_index = index_by_market_triple(output_database)
    rows = []

    for metal, expected_data in expected.items():
        key = (f"market for {metal}", metal, "World")
        matches = built_index.get(key, [])
        status = "ok"

        kg_input_sum: float | None = 0.0
        actual_by_expected = 0.0
        actual_by_keywords = 0.0
        actual_candidate_count = 0

        if not matches:
            status = "not created"
            kg_input_sum = None
        elif len(matches) != 1:
            status = f"expected one market, found {len(matches)}"
        else:
            market = matches[0]
            expected_by_key = expected_data["expected_by_key"]
            for exchange in market.technosphere():
                exchange_dict = exchange_as_dict(exchange)
                if exchange_dict["unit"] == "kilogram":
                    kg_input_sum += exchange_dict["amount"]

                candidate_key = exchange_key(exchange_dict)
                if candidate_key in expected_by_key:
                    actual_by_expected += exchange_dict["amount"]
                    actual_candidate_count += 1

                if is_secondary_metal_supply_exchange(
                    exchange_dict, expected_data["old_reference_product"]
                ):
                    actual_by_keywords += exchange_dict["amount"]

        expected_secondary = expected_data["expected_secondary"]
        rows.append(
            {
                "version": version,
                "project": project,
                "source_database": source_database,
                "built_database": output_database,
                "metal": metal,
                "old_market": activity_text(expected_data["old_market"]),
                "reference_products": "|".join(expected_data["reference_products"]),
                "expected_secondary": expected_secondary,
                "actual_secondary_expected_providers": actual_by_expected,
                "actual_secondary_keywords": actual_by_keywords,
                "secondary_delta": actual_by_expected - expected_secondary,
                "secondary_keyword_delta": actual_by_keywords - expected_secondary,
                "kg_input_sum": kg_input_sum,
                "balance_error": None if kg_input_sum is None else kg_input_sum - 1.0,
                "expected_candidate_count": expected_data["expected_candidate_count"],
                "actual_candidate_count": actual_candidate_count,
                "route_yaml_used": route_yaml_used,
                "status": status,
                "candidate_exchanges": expected_data["candidate_exchanges"],
            }
        )

    return rows


def write_rows(output_path: Path, rows: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "version",
        "project",
        "source_database",
        "built_database",
        "metal",
        "old_market",
        "reference_products",
        "expected_secondary",
        "actual_secondary_expected_providers",
        "actual_secondary_keywords",
        "secondary_delta",
        "secondary_keyword_delta",
        "kg_input_sum",
        "balance_error",
        "expected_candidate_count",
        "actual_candidate_count",
        "route_yaml_used",
        "status",
        "candidate_exchanges",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def summarize(
    rows: list[dict[str, Any]], balance_tolerance: float, share_tolerance: float
):
    print(
        "version\tproject\tsource_db\tbuilt_db\tmarkets\tcreated\tnot_created\t"
        "route_yaml_used\t"
        "max_abs_balance_error\tmax_abs_secondary_delta\tmax_abs_keyword_delta\tstatus"
    )
    versions = []
    for row in rows:
        if row["version"] not in versions:
            versions.append(row["version"])

    failures = []
    for version in versions:
        version_rows = [row for row in rows if row["version"] == version]
        created_rows = [row for row in version_rows if row["status"] != "not created"]
        max_balance = (
            max(abs(row["balance_error"]) for row in created_rows)
            if created_rows
            else 0.0
        )
        max_secondary = (
            max(abs(row["secondary_delta"]) for row in created_rows)
            if created_rows
            else 0.0
        )
        max_keyword = (
            max(abs(row["secondary_keyword_delta"]) for row in created_rows)
            if created_rows
            else 0.0
        )
        route_yaml_used = any(row["route_yaml_used"] for row in version_rows)
        not_created = [row for row in version_rows if row["status"] == "not created"]
        bad_rows = []
        for row in version_rows:
            if row["route_yaml_used"]:
                bad_rows.append(row)
            elif row["status"] == "not created":
                if abs(row["expected_secondary"]) > share_tolerance:
                    bad_rows.append(row)
            elif row["status"] != "ok":
                bad_rows.append(row)
            elif (
                abs(row["balance_error"]) > balance_tolerance
                or abs(row["secondary_delta"]) > share_tolerance
                or abs(row["secondary_keyword_delta"]) > share_tolerance
            ):
                bad_rows.append(row)
        status = "pass" if not bad_rows else f"fail:{len(bad_rows)}"
        if bad_rows:
            failures.extend(bad_rows)

        first = version_rows[0]
        print(
            f"{version}\t{first['project']}\t{first['source_database']}\t"
            f"{first['built_database']}\t{len(version_rows)}\t"
            f"{len(created_rows)}\t{len(not_created)}\t{route_yaml_used}\t"
            f"{max_balance:.3g}\t{max_secondary:.3g}\t{max_keyword:.3g}\t{status}"
        )

    return failures


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--versions", nargs="+", default=list(DEFAULT_VERSIONS))
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--pathway", default=DEFAULT_PATHWAY)
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    parser.add_argument("--dotenv", type=Path, default=Path(".env"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/metals_premise_build_audit.csv"),
    )
    parser.add_argument(
        "--run-id",
        default=datetime.now().strftime("%Y%m%dT%H%M%S"),
        help="Suffix used in the written Brightway database names.",
    )
    parser.add_argument("--balance-tolerance", type=float, default=1e-8)
    parser.add_argument("--share-tolerance", type=float, default=1e-7)
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Audit already written databases named from --run-id without rebuilding.",
    )
    args = parser.parse_args()

    key = None if args.skip_build else load_key(args.dotenv)

    all_rows = []
    for version in args.versions:
        project = f"ecoinvent-{version}-cutoff"
        source_database = f"ecoinvent-{version}-cutoff"
        output_database = (
            f"premise_metals_ei{version.replace('.', '')}_cutoff_{args.run_id}"
        )

        action = "Auditing" if args.skip_build else "Building"
        print(
            f"\n{action} {output_database} from {source_database} "
            f"in project {project}."
        )

        assert_source_database(project, source_database)

        import bw2data as bd

        bd.projects.set_current(project)
        biosphere_database = find_biosphere_database(version)
        expected = expected_secondary_by_metal(version, source_database)
        if args.skip_build:
            if output_database not in bd.databases:
                raise ValueError(
                    f"Missing built database {output_database!r} in project {project!r}."
                )
            route_yaml_used = False
        else:
            route_yaml_used = build_metals_database(
                version=version,
                source_database=source_database,
                output_database=output_database,
                model=args.model,
                pathway=args.pathway,
                year=args.year,
                key=key,
                biosphere_database=biosphere_database,
            )
        all_rows.extend(
            audit_written_database(
                version=version,
                project=project,
                source_database=source_database,
                output_database=output_database,
                expected=expected,
                route_yaml_used=route_yaml_used,
            )
        )

    write_rows(args.output, all_rows)
    print(f"\nWrote audit rows to {args.output}")
    failures = summarize(all_rows, args.balance_tolerance, args.share_tolerance)
    if failures:
        preview = ", ".join(
            f"{row['version']}:{row['metal']}:{row['status']}" for row in failures[:10]
        )
        raise SystemExit(f"Audit failed for {len(failures)} rows: {preview}")


if __name__ == "__main__":
    main()
