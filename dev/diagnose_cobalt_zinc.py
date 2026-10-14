#!/usr/bin/env python3
"""Trace missing extraction attribution; keep licensed snapshots outside the repo.

Capture a fresh metals-only build before its allocation correction, then compare
with the existing audited Brightway databases. Counterfactual scores only change
the characterization-weighted biosphere vector in memory, never a database.
"""

import argparse
import csv
import hashlib
import json
import os
import pickle
import sys
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
ROUTES = {
    "cobalt": "cobalt metal production, from copper mining, via electrolysis, economic allocation",
    "zinc": "primary zinc production from concentrate",
}


def identity(dataset):
    return (dataset["name"], dataset["reference product"], dataset["location"])


def snapshot(database):
    return [
        {
            **{k: ds.get(k) for k in ("name", "reference product", "location", "unit")},
            "exchanges": [
                {
                    k: exc[k]
                    for k in (
                        "name",
                        "product",
                        "location",
                        "unit",
                        "type",
                        "amount",
                        "categories",
                    )
                    if k in exc
                }
                for exc in ds.get("exchanges", [])
            ],
        }
        for ds in database
    ]


def capture(args, bd):
    from premise import NewDatabase
    from premise.metals import Metals

    original = Metals.post_allocation_correction

    def wrapped(self):
        with (args.private / f"pre-{args.system}.pickle").open("wb") as stream:
            pickle.dump(snapshot(self.database), stream)
        original(self)

    Metals.post_allocation_correction = wrapped
    try:
        ndb = NewDatabase(
            scenarios=[{"model": "image", "pathway": "SSP2-M", "year": 2050}],
            source_db=f"ecoinvent-3.12-{args.system}",
            source_version="3.12",
            source_type="brightway",
            biosphere_name="ecoinvent-3.12-biosphere",
            system_model=args.system,
            key=os.environ["PREMISE_KEY"].encode(),
            inventory_backend="compact",
            keep_source_db_uncertainty=False,
            keep_imports_uncertainty=False,
            use_cached_database=True,
            use_cached_inventories=True,
            generate_reports=False,
            quiet=True,
        )
        ndb.update(["metals"])
    finally:
        Metals.post_allocation_correction = original


def inspect_graph(database, product):
    from premise.metals import (
        Metals,
        exchange_may_carry_target_resource,
        is_downstream_attributed_resource_carrier_dataset,
    )
    import premise.metals as module

    metals = object.__new__(Metals)
    metals.database = database
    index = metals.build_exchange_provider_index()
    root = next(d for d in database if identity(d) == (ROUTES[product], product, "CN"))
    flow = product.title()
    original_result = metals.target_resource_is_supplied_upstream(root, flow, index, {})
    edges, terminal, links, paths = [], [], [], {id(root): [identity(root)]}
    queue = deque([root])
    while queue:
        node = queue.popleft()
        for exc in node["exchanges"]:
            if (
                exc["type"] != "technosphere"
                or exc["amount"] <= 0
                or not exchange_may_carry_target_resource(exc, flow)
            ):
                continue
            key = (exc["name"], exc.get("product"), exc.get("location"))
            providers = index.get(key, [])
            if len(providers) != 1:
                links.append(
                    {
                        "path": paths[id(node)],
                        "supplier": key,
                        "matches": len(providers),
                    }
                )
                continue
            provider = providers[0]
            carrier = is_downstream_attributed_resource_carrier_dataset(provider)
            edges.append(
                {
                    "from": identity(node),
                    "to": key,
                    "unit": exc["unit"],
                    "amount": exc["amount"],
                    "excluded_carrier": carrier,
                }
            )
            if carrier:
                continue
            if metals.dataset_has_target_resource_exchange(provider, flow):
                terminal.append(
                    {"path": paths[id(node)] + [key], "direct_resource": True}
                )
                continue
            if id(provider) not in paths:
                paths[id(provider)] = paths[id(node)] + [key]
                queue.append(provider)

    # Diagnostic ablation only: exclude energy inputs or the small auxiliary
    # zinc/zinc-oxide feeds on primary smelters. Do not edit inventories.
    original_predicate = module.exchange_may_carry_target_resource
    if product == "cobalt":

        def predicate(exc, name):
            return exc.get("unit") == "kilogram" and original_predicate(exc, name)

        excluded = "non-kilogram inputs (the cobalt-industry electricity chain)"
    else:
        auxiliary_ids = {
            id(e)
            for d in database
            if d["name"] == ROUTES[product] and d["reference product"] == product
            for e in d["exchanges"]
            if e.get("product") in ("zinc", "zinc oxide")
            and e["type"] == "technosphere"
            and e["amount"] > 0
        }

        def predicate(exc, name):
            return id(exc) not in auxiliary_ids and original_predicate(exc, name)

        excluded = "positive zinc and zinc-oxide feeds on primary zinc producers"
    module.exchange_may_carry_target_resource = predicate
    try:
        ablated_result = metals.target_resource_is_supplied_upstream(
            root, flow, index, {}
        )
    finally:
        module.exchange_may_carry_target_resource = original_predicate

    routes = [
        d
        for d in database
        if d["name"] == ROUTES[product] and d["reference product"] == product
    ]
    return {
        "root": identity(root),
        "stage": "immediately before post-allocation correction",
        "upstream_result": original_result,
        "ablation_exclusion": excluded,
        "upstream_result_with_exclusion": ablated_result,
        "route_count": len(routes),
        "nodes": len(paths),
        "edges": edges,
        "unresolved_links": links,
        "resource_terminals": terminal,
        "representative_paths": list(paths.values()),
    }


def scores(args, config, bd):
    import numpy as np
    import bw2calc as bc
    from premise.metals import (
        get_in_ground_resource_exchanges,
        canonical_resource_flow_label,
    )

    snapshots = {}
    for label in ("before", "final"):
        with (args.snapshots / f"{label}-{args.system}" / "inventory.pickle").open(
            "rb"
        ) as stream:
            snapshots[label] = {identity(d): d for d in pickle.load(stream)}
    activities = {identity(a): a for a in bd.Database(config["after"])}
    market = activities[("market for cobalt", "cobalt", "World")]
    lca = bc.LCA({market.id: 1}, ("RELICS", "metals extraction", "Cobalt"))
    lca.lci(factorize=True)
    summary, changes, contributions = [], [], []
    for product, route in ROUTES.items():
        flow = product.title()
        method = ("RELICS", "metals extraction", flow)
        if method not in bd.methods:
            raise ValueError(f"Missing {method}")
        cfs = dict(bd.Method(method).load())
        simple_flows = [
            f
            for f in bd.Database(config["biosphere"])
            if f["name"] == flow
            and tuple(f["categories"]) == ("natural resource", "in ground")
        ]
        assert len(simple_flows) == 1 and cfs[simple_flows[0].id] == 1
        market = activities[(f"market for {product}", product, "World")]
        assert market["unit"] == "kilogram"
        lca.redo_lci({market.id: 1})
        lca.switch_method(method)
        lca.lcia()
        current = float(lca.score)
        amounts = np.asarray(lca.characterized_inventory.sum(axis=0)).ravel()
        total_added, changed = 0.0, 0
        for key, after in snapshots["final"].items():
            if key[:2] != (route, product):
                continue
            before = snapshots["before"].get(key)
            if before is None:
                raise ValueError(f"Missing before identity: {key}")

            def extraction(d):
                return sum(
                    e["amount"]
                    for e in get_in_ground_resource_exchanges(d)
                    if canonical_resource_flow_label(e["name"]) == product
                )

            old, new = extraction(before), extraction(after)
            a = activities[key]
            supply = float(lca.supply_array[lca.dicts.activity[a.id]])
            extra = (old - new) * supply
            total_added += extra
            changed += old != new
            changes.append(
                {
                    "product": product,
                    "name": key[0],
                    "location": key[2],
                    "before_direct_resource": old,
                    "after_direct_resource": new,
                    "supply_per_kg_market": supply,
                    "restoration_score_increment": extra,
                }
            )
        for a in activities.values():
            amount = float(amounts[lca.dicts.activity[a.id]])
            if abs(amount) > 1e-10:
                contributions.append(
                    {
                        "market": product,
                        "name": a["name"],
                        "product": a["reference product"],
                        "location": a["location"],
                        "score_contribution": amount,
                    }
                )
        with (
            ROOT
            / "results/metals-correctness/markets"
            / args.system
            / "own_resource.csv"
        ).open() as stream:
            saved = next(
                r
                for r in csv.DictReader(stream)
                if r["market product"] == product and r["indicator"] == flow
            )
        assert np.isclose(current, float(saved["after"]), rtol=1e-9, atol=1e-12)
        summary.append(
            {
                "product": product,
                "method": method,
                "market_key": market.key,
                "before": float(saved["before"]),
                "after": current,
                "restoring_previous_additions_only": current + total_added,
                "restoration_increment": total_added,
                "changed_routes": changed,
                "contribution_sum": float(amounts.sum()),
            }
        )
    for name, rows in (("route_changes", changes), ("contributions", contributions)):
        rows = sorted(
            rows,
            key=lambda r: -abs(
                r.get("score_contribution", r.get("restoration_score_increment", 0))
            ),
        )
        with (args.output / f"{name}.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=("cutoff", "consequential"), required=True)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument(
        "--repo",
        type=Path,
        default=ROOT,
        help="Implementation checkout to diagnose (use e54ac3e2 to reproduce the archived regression)",
    )
    parser.add_argument(
        "--private", type=Path, default=Path("/private/tmp/metals-resource-diagnosis")
    )
    parser.add_argument(
        "--snapshots", type=Path, default=Path("/private/tmp/premise-metals-audit")
    )
    args = parser.parse_args()
    sys.path.insert(0, str(args.repo.resolve()))
    args.output = (
        ROOT / "results/metals-correctness/cobalt-zinc-diagnosis" / args.system
    )
    args.output.mkdir(parents=True, exist_ok=True)
    args.private.mkdir(parents=True, exist_ok=True)
    config = json.loads(
        (
            ROOT / "results/metals-correctness" / args.system / "metadata.json"
        ).read_text()
    )
    import bw2data as bd

    bd.projects.set_current(config["project"])
    for name in (
        config["before"],
        config["after"],
        config["source"],
        config["biosphere"],
    ):
        if name not in bd.databases:
            raise ValueError(f"Missing database: {name}")
    for stage in ("before", "after"):
        name = config[stage]
        assert (
            hashlib.sha256(
                Path(bd.Database(name).filepath_processed()).read_bytes()
            ).hexdigest()
            == config["database_fingerprints"][name]
        )
    if args.capture:
        capture(args, bd)
    with (args.private / f"pre-{args.system}.pickle").open("rb") as stream:
        database = pickle.load(stream)
    graphs = {p: inspect_graph(database, p) for p in ROUTES}
    (args.output / "graph_traces.json").write_text(json.dumps(graphs, indent=2))
    print(
        "Graph traces complete",
        {
            p: (g["upstream_result"], g["upstream_result_with_exclusion"])
            for p, g in graphs.items()
        },
        flush=True,
    )
    result = {
        "project": config["project"],
        "database": config["after"],
        "scenario": {"model": "image", "pathway": "SSP2-M", "year": 2050},
        "functional_unit": "1 kg of market product",
        "unit": "kg extracted element",
        "database_fingerprints": config["database_fingerprints"],
        "metals_py_sha256": hashlib.sha256(
            (args.repo / "premise/metals.py").read_bytes()
        ).hexdigest(),
        "scores": scores(args, config, bd),
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result["scores"], indent=2))


if __name__ == "__main__":
    main()
