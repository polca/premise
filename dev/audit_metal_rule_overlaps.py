#!/usr/bin/env python3
"""Screen configured metal rules for direct and upstream product overlaps."""

from __future__ import annotations

import argparse
import csv
import json
from collections import deque
from pathlib import Path
from typing import Any

import bw2data as bd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument(
        "--decisions",
        required=True,
        type=Path,
        help="A decision list, or a validation snapshot containing 'decisions'",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--max-paths", type=int, default=100)
    parser.add_argument(
        "--activity-name",
        action="append",
        help="Optional exact target activity name; repeat to audit several names",
    )
    return parser.parse_args()


def semantic_key(node: Any) -> tuple[str, str, str, str]:
    return (
        str(node.get("name") or ""),
        str(node.get("reference product") or ""),
        str(node.get("location") or ""),
        str(node.get("unit") or ""),
    )


def load_decisions(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("decisions")
    if not isinstance(payload, list):
        raise ValueError("Decision input must be a list or contain a 'decisions' list.")
    return payload


def matching_paths(
    target: Any,
    provider_name: str,
    provider_product: str,
    *,
    max_depth: int,
    max_paths: int,
) -> list[dict]:
    """Return bounded, cycle-safe technosphere paths to an exact provider."""

    queue = deque([(target, 1.0, [semantic_key(target)], frozenset({target.id}), 0)])
    found = []
    while queue and len(found) < max_paths:
        node, path_amount, path, visited, depth = queue.popleft()
        if depth >= max_depth:
            continue
        for exchange in node.technosphere():
            supplier = exchange.input
            amount = path_amount * float(exchange.get("amount", 0.0))
            supplier_key = semantic_key(supplier)
            new_path = [*path, supplier_key]
            if (
                supplier.get("name") == provider_name
                and supplier.get("reference product") == provider_product
            ):
                found.append(
                    {
                        "depth": depth + 1,
                        "amount": amount,
                        "provider location": supplier.get("location"),
                        "path": " -> ".join(item[0] for item in new_path),
                    }
                )
                if len(found) >= max_paths:
                    break
                continue
            if supplier.id in visited:
                continue
            queue.append(
                (
                    supplier,
                    amount,
                    new_path,
                    visited | {supplier.id},
                    depth + 1,
                )
            )
    return found


def main() -> int:
    args = parse_args()
    if args.max_depth < 1 or args.max_paths < 1:
        raise ValueError("--max-depth and --max-paths must be positive.")
    bd.projects.set_current(args.project)
    if args.database not in bd.databases:
        raise ValueError(f"Brightway database {args.database!r} does not exist.")

    database = bd.Database(args.database)
    activity_index = {semantic_key(node): node for node in database}
    decisions = load_decisions(args.decisions)
    if args.activity_name:
        selected_names = set(args.activity_name)
        decisions = [
            decision
            for decision in decisions
            if decision.get("activity", {}).get("name") in selected_names
        ]

    summaries = []
    path_rows = []
    cache = {}
    for decision in decisions:
        activity = decision.get("activity", {})
        key = (
            str(activity.get("name") or ""),
            str(activity.get("reference product") or ""),
            str(activity.get("location") or ""),
            str(activity.get("unit") or ""),
        )
        target = activity_index.get(key)
        if target is None:
            summaries.append(
                {
                    "activity": key[0],
                    "reference product": key[1],
                    "location": key[2],
                    "rule id": decision.get("material rule id"),
                    "element": decision.get("element"),
                    "provider name": decision.get("provider name"),
                    "provider product": decision.get("provider product"),
                    "target amount": decision.get("target direct amount"),
                    "direct amount": None,
                    "upstream path amount": None,
                    "path count": 0,
                    "classification": "target_not_found",
                    "screening note": "Target activity is absent from the audited database.",
                }
            )
            continue

        provider_name = str(decision.get("provider name") or "")
        provider_product = str(decision.get("provider product") or "")
        cache_key = (target.id, provider_name, provider_product, args.max_depth)
        if cache_key not in cache:
            cache[cache_key] = matching_paths(
                target,
                provider_name,
                provider_product,
                max_depth=args.max_depth,
                max_paths=args.max_paths,
            )
        paths = cache[cache_key]
        direct = sum(path["amount"] for path in paths if path["depth"] == 1)
        upstream = sum(path["amount"] for path in paths if path["depth"] > 1)
        classification = (
            "direct_match" if direct else "upstream_match" if upstream else "no_match"
        )
        summaries.append(
            {
                "activity": key[0],
                "reference product": key[1],
                "location": key[2],
                "rule id": decision.get("material rule id"),
                "element": decision.get("element"),
                "provider name": provider_name,
                "provider product": provider_product,
                "target amount": decision.get("target direct amount"),
                "direct amount": direct,
                "upstream path amount": upstream,
                "path count": len(paths),
                "classification": classification,
                "screening note": (
                    "Bounded exact-product path screen; it is not an installed-material balance."
                ),
            }
        )
        for path in paths:
            path_rows.append(
                {
                    "activity": key[0],
                    "location": key[2],
                    "rule id": decision.get("material rule id"),
                    "element": decision.get("element"),
                    "provider product": provider_product,
                    **path,
                }
            )

    args.output.mkdir(parents=True, exist_ok=True)
    summary_path = args.output / "metal_overlap_summary.csv"
    paths_path = args.output / "metal_overlap_paths.csv"
    summary_headers = list(summaries[0]) if summaries else []
    path_headers = (
        list(path_rows[0])
        if path_rows
        else [
            "activity",
            "location",
            "rule id",
            "element",
            "provider product",
            "depth",
            "amount",
            "provider location",
            "path",
        ]
    )
    with summary_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=summary_headers)
        if summary_headers:
            writer.writeheader()
            writer.writerows(summaries)
    with paths_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=path_headers)
        writer.writeheader()
        writer.writerows(path_rows)
    print(f"Summary: {summary_path}")
    print(f"Paths: {paths_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
