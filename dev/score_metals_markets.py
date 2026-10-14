#!/usr/bin/env python3
"""Compare all configured metal markets in existing before/after audit databases."""

import argparse
import csv
import hashlib
import importlib.metadata
import json
import math
import time
from pathlib import Path


def write_csv(path, rows):
    if rows:
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(
                stream, fieldnames=list(rows[0]), lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(rows)


def target_indicators(product, available):
    """Explicit mineral/mixed-product choices; never invent a missing indicator."""
    aliases = {
        "bauxite": ["Aluminium"],
        "bentonite": [],
        "boric acid, anhydrous, powder": ["Boron"],
        "boric oxide": ["Boron"],
        "calcium borates": ["Boron"],
        "sodium borates": ["Boron"],
        "chromite ore concentrate": ["Chromium"],
        "copper-cobalt ore": ["Copper", "Cobalt"],
        "ferroniobium, 66% Nb": ["Niobium"],
        "ilmenite, 54% titanium dioxide": ["Titanium"],
        "lanthanum-cerium oxide": ["Lanthanum", "Cerium"],
        "lutetium oxide": [],
        "molybdenite": ["Molybdenum"],
        "phosphate rock, beneficiated": ["Phosphorus"],
        "phosphoric acid, fertiliser grade, without water, in 70% solution state": [
            "Phosphorus"
        ],
        "platinum group metal concentrate": [
            "Platinum",
            "Palladium",
            "Rhodium",
            "Ruthenium",
            "Iridium",
        ],
        "potash salt": ["Potassium"],
        "pyrochlore concentrate": ["Niobium"],
        "rare earth carbonate concentrate": [
            "Lanthanum",
            "Cerium",
            "Praseodymium",
            "Neodymium",
            "Samarium",
            "Europium",
            "Gadolinium",
            "Terbium",
            "Dysprosium",
            "Holmium",
            "Erbium",
            "Thulium",
            "Ytterbium",
            "Scandium",
            "Yttrium",
        ],
        "rutile, 95% titanium dioxide": ["Titanium"],
        "sand": [],
        "silica sand": [],
        "stibnite concentrate": ["Antimony"],
        "uranium ore, as U": [],
        "vanadium bearing magnetite": ["Vanadium", "Iron"],
        "zircon": ["Zirconium"],
    }
    if product in aliases:
        result = aliases[product]
    else:
        result = [
            name
            for name in available
            if product.lower().split(",")[0].split()[0] == name.lower()
        ]
        if len(result) != 1:
            raise ValueError(
                f"No explicit indicator selection for {product!r}: {result}"
            )
    if not set(result) <= set(available):
        raise ValueError(f"Selected unavailable indicator for {product!r}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--audit",
        type=Path,
        required=True,
        help="Existing cutoff/consequential audit folder with metadata.json",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--skip-saved-score-check",
        action="store_true",
        help="For a new database pair without the earlier selected-product scores; direct LCIA and fingerprint checks still run.",
    )
    args = parser.parse_args()
    import numpy as np
    import pandas as pd
    import bw2data as bd
    import bw2calc as bc
    from relics.utils import RELICS_MAPPING, load_mappings

    config = json.loads((args.audit / "metadata.json").read_text())
    bd.projects.set_current(config["project"])
    for name in (config["before"], config["after"], config["biosphere"]):
        if name not in bd.databases:
            raise ValueError(f"Missing database {name!r}")
    mapping_hash = hashlib.sha256(Path(RELICS_MAPPING).read_bytes()).hexdigest()
    if config["relics_mapping_sha256"] != mapping_hash:
        raise ValueError("RELICS mapping differs from the previous audit")
    flows = list(bd.Database(config["biosphere"]))
    methods = []
    for mapping in load_mappings(RELICS_MAPPING):
        key = ("RELICS", "metals extraction", mapping["name"])
        expected = {}
        for rule in mapping["environmental flow"]:
            for flow in flows:
                if flow["name"].lower().startswith(rule["name"].lower()) and tuple(
                    flow["categories"]
                ) == tuple(rule["categories"].split("::")):
                    if flow.id in expected and expected[flow.id] != rule["amount"]:
                        raise ValueError(f"Conflicting factors in {key!r}")
                    expected[flow.id] = rule["amount"]
        if key not in bd.methods or dict(bd.Method(key).load()) != expected:
            raise ValueError(
                f"Registered method differs from the audited mapping: {key!r}"
            )
        if expected:
            methods.append(key)

    root = Path(__file__).resolve().parents[1]
    workbook = root / "premise/data/metals/mining_shares_mapping.xlsx"
    data = pd.read_excel(workbook, sheet_name="ei312")
    products = sorted(
        data.loc[
            (data["Work done"] == "Yes") & data["Country"].notna(), "Metal"
        ].unique()
    )
    targets = {p: target_indicators(p, [m[-1] for m in methods]) for p in products}
    args.output.mkdir(parents=True, exist_ok=True)
    scores, activities, fingerprints, checks = [], [], {}, []
    started = time.perf_counter()
    for stage in ("before", "after"):
        name = config[stage]
        fingerprint = hashlib.sha256(
            Path(bd.Database(name).filepath_processed()).read_bytes()
        ).hexdigest()
        if fingerprint != config["database_fingerprints"][name]:
            raise ValueError(f"Database changed since the original audit: {name}")
        fingerprints[name] = fingerprint
        index = {}
        for activity in bd.Database(name):
            index.setdefault(
                (
                    activity["name"],
                    activity.get("reference product"),
                    activity.get("location"),
                ),
                [],
            ).append(activity)
        selected = {}
        for product in products:
            matches, location = [], None
            for location in ("World", "GLO", "RoW"):
                matches = index.get((f"market for {product}", product, location), [])
                if matches:
                    break
            if len(matches) > 1:
                raise ValueError(f"Ambiguous market for {product!r} in {name}")
            activity = matches[0] if matches else None
            if activity is not None and activity["unit"] != "kilogram":
                raise ValueError(f"Expected kilogram market for {product!r}")
            activities.append(
                {
                    "stage": stage,
                    "market product": product,
                    "database": name,
                    "name": activity["name"] if activity is not None else "",
                    "location": location if activity is not None else "",
                    "code": activity["code"] if activity is not None else "",
                    "functional unit": 1,
                    "unit": "kilogram",
                    "status": (
                        "available"
                        if activity is not None
                        else "no matching global market"
                    ),
                }
            )
            if activity is not None:
                selected[product] = activity
        print(
            f"{stage}: found {len(selected)}/{len(products)} markets; factorizing",
            flush=True,
        )
        lca = bc.LCA({next(iter(selected.values())).id: 1}, methods[0])
        lca.lci(factorize=True)
        lca.lcia()
        factors = []
        for method in methods:
            lca.switch_method(method)
            factors.append(lca.characterization_matrix.diagonal())
        factors = np.asarray(factors)
        for position, (product, activity) in enumerate(selected.items()):
            lca.redo_lci({activity.id: 1})
            # Equivalent to summing each characterized inventory, without
            # rebuilding it separately for every indicator.
            inventory = np.asarray(lca.inventory.sum(axis=1)).ravel()
            values = factors @ inventory
            if not np.isfinite(values).all():
                raise ValueError(f"Non-finite result for {product}")
            for method, score in zip(methods, values):
                scores.append(
                    {
                        "stage": stage,
                        "market product": product,
                        "indicator": method[-1],
                        "unit": "kg resource/kg market product",
                        "score": float(score),
                    }
                )
            if product in ("copper, cathode", "gold"):
                indicator = "Copper" if product.startswith("copper") else "Gold"
                key = ("RELICS", "metals extraction", indicator)
                lca.switch_method(key)
                lca.lcia()
                bulk = float(values[methods.index(key)])
                if not math.isclose(bulk, lca.score, rel_tol=1e-11, abs_tol=1e-12):
                    raise ValueError("Bulk LCIA differs from Brightway LCIA")
                old = None
                if not args.skip_saved_score_check:
                    with (args.audit / "scores.csv").open() as stream:
                        old = next(
                            float(r["score"])
                            for r in csv.DictReader(stream)
                            if r["stage"] == stage
                            and r["activity"]
                            == ("copper" if product.startswith("copper") else "gold")
                            and json.loads(r["method"]) == list(key)
                        )
                    if not math.isclose(old, bulk, rel_tol=1e-8, abs_tol=1e-12):
                        raise ValueError(
                            f"Score differs from original audit: {product}: {old} vs {bulk}"
                        )
                checks.append(
                    {"stage": stage, "product": product, "original": old, "score": bulk}
                )
            if (position + 1) % 20 == 0:
                print(
                    f"{stage}: scored {position + 1}/{len(selected)} markets",
                    flush=True,
                )
        write_csv(
            args.output / f"{stage}_scores.csv",
            [r for r in scores if r["stage"] == stage],
        )
        del lca

    index = {
        (r["stage"], r["market product"], r["indicator"]): r["score"] for r in scores
    }
    activity_index = {(r["stage"], r["market product"]): r for r in activities}
    differences, own = [], []
    for product in products:
        for method in methods:
            before = index.get(("before", product, method[-1]))
            after = index.get(("after", product, method[-1]))
            change = (
                after - before if before is not None and after is not None else None
            )
            row = {
                "market product": product,
                "indicator": method[-1],
                "unit": "kg resource/kg market product",
                "before": before,
                "after": after,
                "change": change,
                "percent change": (
                    100 * change / abs(before)
                    if change is not None and before
                    else None
                ),
                "before location": activity_index["before", product]["location"],
                "after location": activity_index["after", product]["location"],
                "status": (
                    "comparable"
                    if change is not None
                    else "market unavailable in one or both builds"
                ),
            }
            differences.append(row)
            if method[-1] in targets[product]:
                own.append(row)
        if not targets[product]:
            own.append(
                {
                    "market product": product,
                    "indicator": "no dedicated RELICS indicator",
                    "unit": "",
                    "before": None,
                    "after": None,
                    "change": None,
                    "percent change": None,
                    "before location": activity_index["before", product]["location"],
                    "after location": activity_index["after", product]["location"],
                    "status": "no dedicated RELICS indicator",
                }
            )
    write_csv(args.output / "activities.csv", activities)
    write_csv(args.output / "all_indicators.csv", differences)
    write_csv(args.output / "own_resource.csv", own)
    (args.output / "metadata.json").write_text(
        json.dumps(
            {
                "project": config["project"],
                "before": config["before"],
                "after": config["after"],
                "scenario": {"model": "image", "pathway": "SSP2-M", "year": 2050},
                "scope": "metals-only update",
                "functional unit": "1 kg of each market product",
                "database_fingerprints": fingerprints,
                "methods": methods,
                "relics_mapping_sha256": mapping_hash,
                "relics_version": importlib.metadata.version("relics"),
                "market_mapping_sha256": hashlib.sha256(
                    workbook.read_bytes()
                ).hexdigest(),
                "configured_markets": len(products),
                "indicator_count": len(methods),
                "own_indicator_selection": targets,
                "cross_checks": checks,
                "seconds": time.perf_counter() - started,
            },
            indent=2,
        )
    )
    print(
        f"Completed {len(products)} markets and {len(methods)} indicators", flush=True
    )


if __name__ == "__main__":
    main()
