"""Validate an in-memory premise PV build and its GWP regression benchmarks.

No Brightway database is written. The optional --inventory-pickle accepts a
trusted, locally materialised premise inventory, including its full background.
Without it, build IMAGE SSP2-M 2050 using the installed source and PREMISE_KEY.
"""

import argparse
import csv
import hashlib
import json
import os
import pickle
import sys
from collections import Counter
from pathlib import Path

import bw2data as bd
import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.linalg import splu

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from premise.photovoltaic import identity

METHOD = (
    "ecoinvent-3.12",
    "IPCC 2021",
    "climate change: total (excl. biogenic CO2)",
    "global warming potential (GWP100)",
)


def validate_graph(data):
    counts = Counter(identity(d) for d in data)
    duplicate = [key for key, count in counts.items() if count > 1]
    missing = [
        (d["name"], identity(e))
        for d in data
        for e in d["exchanges"]
        if e["type"] == "technosphere" and identity(e) not in counts
    ]
    if duplicate or missing:
        raise ValueError(
            f"Duplicate activities: {duplicate[:5]}; missing suppliers: {missing[:5]}"
        )
    return {
        "activities": len(data),
        "duplicates": len(duplicate),
        "missing_suppliers": len(missing),
    }


def characterize(data):
    """Solve the adjoint LCA system once for scores of all functional units."""
    index = {identity(d): i for i, d in enumerate(data)}
    factors = {
        (bd.get_node(id=key)["code"] if isinstance(key, int) else key[1]): amount
        for key, amount in bd.Method(METHOD).load()
    }
    biosphere = {
        (node["name"], tuple(node.get("categories", ())), node["unit"]): node["code"]
        for node in bd.Database("ecoinvent-3.12-biosphere")
    }
    rows, cols, values = [], [], []
    direct = np.zeros(len(data))
    for col, dataset in enumerate(data):
        for exchange in dataset["exchanges"]:
            if exchange["type"] == "biosphere":
                code = biosphere[
                    (
                        exchange["name"],
                        tuple(exchange.get("categories", ())),
                        exchange["unit"],
                    )
                ]
                direct[col] += exchange["amount"] * factors.get(code, 0)
            elif exchange["type"] in ("production", "technosphere"):
                rows.append(
                    col
                    if exchange["type"] == "production"
                    else index[identity(exchange)]
                )
                cols.append(col)
                values.append(
                    exchange["amount"] * (1 if exchange["type"] == "production" else -1)
                )
            else:
                raise ValueError(f"Unexpected exchange type: {exchange['type']}")
    matrix = coo_matrix((values, (rows, cols)), shape=(len(data), len(data))).tocsc()
    solver = splu(matrix)
    scores = solver.solve(direct, trans="T")
    assert np.isfinite(scores).all()
    residual = np.max(np.abs(matrix.T @ scores - direct))
    scale = max(
        1,
        np.max(np.abs(direct)),
        float(abs(matrix.T).sum(axis=1).max()) * np.max(np.abs(scores)),
    )
    assert residual / scale < 1e-10
    return {identity(d): float(scores[i]) for i, d in enumerate(data)}, residual / scale


def reference_regression(data, scores):
    with (ROOT / "premise/data/solar/pv_2026_gwp100_reference.csv").open() as stream:
        reference = list(csv.DictReader(stream))
    lookup = {
        (d["name"], d.get("pv reference location", d["location"])): d for d in data
    }
    rows = []
    for row in reference:
        dataset = lookup.get((row["name"], row["location"]))
        if dataset is None and row["location"] == "REF":
            # Descriptive metadata is sidecar-backed on the normal cache path.
            dataset = lookup[(row["name"], "GLO")]
        score = scores[identity(dataset)]
        expected = float(row["kg_co2eq_per_kwh"])
        rows.append(
            {
                **row,
                "kg_co2eq_per_kwh": score,
                "reference_kg_co2eq_per_kwh": expected,
                "relative_difference": abs(score - expected) / abs(expected),
            }
        )
    return rows


def validate_pv_coverage(data):
    parameters = json.loads(
        (ROOT / "premise/data/solar/pv_2026_parameters.json").read_text()
    )
    expected = set(parameters["locations"])
    for segment in ("residential", "commercial", "production mix"):
        actual = {
            d["location"]
            for d in data
            if d["name"] == f"electricity production, photovoltaic, {segment}"
        }
        assert actual == expected, (segment, actual ^ expected)
    with (ROOT / "premise/data/solar/pv_2026_gwp100_reference.csv").open() as stream:
        reference_names = {
            row["name"] for row in csv.DictReader(stream) if row["location"] == "REF"
        }
    references = [d for d in data if d["name"] in reference_names]
    assert len(references) == 12 and all(d["location"] == "GLO" for d in references)
    plants = [
        d
        for d in data
        if d["name"].startswith("photovoltaic installation construction")
    ]
    assert plants and all(
        "pv capacity kwp" in d and "pv technology" in d for d in plants
    )
    return {
        "country_locations": len(expected),
        "reference_systems": len(references),
        "installations_with_operational_metadata": len(plants),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="ecoinvent-3.12-cutoff")
    parser.add_argument("--inventory-pickle", type=Path)
    parser.add_argument("--use-cached-inventories", action="store_true")
    parser.add_argument(
        "--skip-scenario-lcia",
        action="store_true",
        help="Validate the scenario graph and market links without repeating future LCIA.",
    )
    parser.add_argument(
        "--save-inventories",
        type=Path,
        help="Optional directory for locally materialised validation inventories.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "results/iea_pv_2026/integration_validation.json",
    )
    args = parser.parse_args()
    bd.projects.set_current(args.project)
    build = None
    if args.inventory_pickle:
        with args.inventory_pickle.open("rb") as stream:
            data = pickle.load(stream)
    else:
        from premise import NewDatabase

        build = NewDatabase(
            scenarios=[{"model": "image", "pathway": "SSP2-M", "year": 2050}],
            source_db="ecoinvent-3.12-cutoff",
            source_version="3.12",
            system_model="cutoff",
            biosphere_name="ecoinvent-3.12-biosphere",
            key=os.environ["PREMISE_KEY"],
            use_cached_inventories=args.use_cached_inventories,
            keep_imports_uncertainty=True,
            generate_reports=False,
        )
        data = build.materialize_inventory()
    report = {
        "background_project": args.project,
        "method": METHOD,
        "brightway_database_written": False,
        "baseline": validate_graph(data),
        "pv_coverage": validate_pv_coverage(data),
        "workbook_sha256": {
            name: hashlib.sha256(
                (ROOT / "premise/data/additional_inventories" / name).read_bytes()
            ).hexdigest()
            for name in (
                "lci-PV-2026.xlsx",
                "lci-PV-2026-electricity.xlsx",
                "lci-PV-CIGS.xlsx",
            )
        },
    }
    if args.save_inventories:
        args.save_inventories.mkdir(parents=True, exist_ok=True)
        with (args.save_inventories / "baseline.pickle").open("wb") as stream:
            pickle.dump(data, stream)
    print("Complete baseline graph validated:", report["baseline"], flush=True)
    scores, error = characterize(data)
    rows = reference_regression(data, scores)
    report.update(
        reference_results=rows,
        maximum_relative_difference=max(r["relative_difference"] for r in rows),
        solver_backward_error=error,
    )
    report["references_pass"] = report["maximum_relative_difference"] < 1e-6
    print(
        "45 reference scores; maximum relative difference:",
        report["maximum_relative_difference"],
        flush=True,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    assert report["references_pass"], "The 45 reference scores changed."
    if build:
        del data
        build.update(["electricity"])
        data = build.materialize_inventory()
        report["scenario"] = validate_graph(data)
        report["scenario"].update(model="image", pathway="SSP2-M", year=2050)
        if args.save_inventories:
            with (args.save_inventories / "scenario.pickle").open("wb") as stream:
                pickle.dump(data, stream)
        pv_names = {
            "electricity production, photovoltaic, residential",
            "electricity production, photovoltaic, commercial",
        }
        market_links = [
            {
                "market": d["name"],
                "region": d["location"],
                "supplier": e["name"],
                "supplier_location": e["location"],
                "product": e.get("product"),
                "amount": e["amount"],
            }
            for d in data
            if d["name"].startswith("market group for electricity")
            for e in d["exchanges"]
            if e["type"] == "technosphere" and e["name"] in pv_names and e["amount"] > 0
        ]
        assert {e["supplier"] for e in market_links} == pv_names
        assert all(e["supplier_location"] != "GLO" for e in market_links)
        report["scenario"]["pv_market_links"] = market_links
        print(
            "2050 graph validated; direct country PV market links:",
            len(market_links),
            flush=True,
        )
        if not args.skip_scenario_lcia:
            scenario_scores, error = characterize(data)
            report["scenario"]["solver_backward_error"] = error
            report["scenario"]["pv_suppliers"] = [
                {
                    "name": d["name"],
                    "location": d["location"],
                    "kg_co2eq_per_kwh": scenario_scores[identity(d)],
                }
                for d in data
                if d["name"] in pv_names
            ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: v
                for k, v in report.items()
                if k not in ("reference_results", "scenario")
            },
            indent=2,
        )
    )
    assert report["references_pass"], "The 45 reference scores changed."


if __name__ == "__main__":
    main()
