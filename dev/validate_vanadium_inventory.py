"""Compare issue-286 inventories in an ecoinvent 3.12 no-write scenario build.

Run with PREMISE_KEY and the modern Brightway environment. Only local caches,
logs and aggregate reports are written; no Brightway database is changed.
"""

import argparse
from collections import defaultdict
import gc
import json
import os
from pathlib import Path
import pickle
import sys
import time

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.linalg import splu

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import bw2data as bd
from premise import NewDatabase
import premise.new_database as new_database

PROJECT = "ecoinvent-3.12-cutoff"
SCENARIO = {"model": "remind", "pathway": "SSP1-PkBudg1000", "year": 2050}


def identity(ds):
    return (
        ds["name"].lower(),
        ds.get("product", ds.get("reference product", "")).lower(),
        ds.get("location"),
        ds.get("unit"),
    )


def scores(database, method):
    """Solve A x = demand, retaining every production amount and waste sign."""
    print("SCORING", len(database), "activities", flush=True)
    lookup = defaultdict(list)
    for i, ds in enumerate(database):
        lookup[identity(ds)].append(i)
    method_cfs = {
        (
            bd.get_node(id=k)
            if isinstance(k, int)
            else bd.get_node(database=k[0], code=k[1])
        )["code"]: v
        for k, v in bd.Method(method).load()
    }
    biosphere = {a["code"]: dict(a) for a in bd.Database("biosphere")}
    biosphere_codes = {
        (b["name"], tuple(b.get("categories", ())), b["unit"]): code
        for code, b in biosphere.items()
    }
    elementary = np.zeros((4, len(database)))
    rows, cols, amounts = [], [], []
    missing = []
    for j, ds in enumerate(database):
        for exc in ds["exchanges"]:
            amount = float(exc["amount"])
            if amount == 0:
                continue
            if exc["type"] in ("production", "technosphere"):
                indices = [j] if exc["type"] == "production" else lookup[identity(exc)]
                if len(indices) != 1:
                    missing.append((identity(ds), identity(exc), len(indices)))
                    continue
                rows.append(indices[0])
                cols.append(j)
                amounts.append(amount if exc["type"] == "production" else -amount)
            elif exc["type"] == "biosphere":
                code = exc.get("input", ("", ""))[1] or biosphere_codes.get(
                    (exc["name"], tuple(exc.get("categories", ())), exc["unit"]), ""
                )
                elementary[3, j] += amount * method_cfs.get(code, 0)
                bio = biosphere.get(code, exc)
                categories = bio.get("categories", ())
                if tuple(categories[:2]) == ("natural resource", "in ground"):
                    for idx, metal in enumerate(("Titanium", "Iron", "Vanadium")):
                        if bio.get("name") == metal:
                            elementary[idx, j] += amount
    if missing:
        (ROOT / "results" / "vanadium-unresolved-links.json").write_text(
            json.dumps(missing, indent=2)
        )
        raise ValueError(
            f"{len(missing)} unresolved or ambiguous suppliers; see local report"
        )
    matrix = coo_matrix(
        (amounts, (rows, cols)), shape=(len(database), len(database))
    ).tocsc()
    print("FACTORING", matrix.shape, matrix.nnz, flush=True)
    solver = splu(matrix, permc_spec="MMD_AT_PLUS_A")
    print("FACTORED", flush=True)
    targets = []
    for i, ds in enumerate(database):
        if (
            ds["location"] in ("ZA", "CN")
            and ds["name"]
            in (
                "vanadium bearing magnetite production",
                "vanadium pentoxide production",
            )
        ) or (
            ds["name"]
            == "vanadium-redox flow battery system assembly, 8.3 megawatt hour"
        ):
            targets.append((i, ds))
    if not targets:
        raise ValueError("No target activities")
    result = []
    for i, ds in targets:
        demand = np.zeros(len(database))
        demand[i] = 1
        supply = solver.solve(demand)
        if np.max(np.abs(matrix @ supply - demand)) > 1e-7:
            raise ValueError("LCI solve residual exceeds tolerance")
        values = elementary @ supply
        assert (
            values[3] > 0
        ), "No positive climate score; verify biosphere characterization"
        result.append(
            {
                "name": ds["name"],
                "reference product": ds["reference product"],
                "location": ds["location"],
                "unit": ds["unit"],
                "functional_unit": 1,
                **dict(
                    zip(
                        ("titanium_kg", "iron_kg", "vanadium_kg", "climate_kg_CO2_eq"),
                        map(float, values),
                    )
                ),
            }
        )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--baseline-workbook", type=Path)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    assert PROJECT in bd.projects
    bd.projects.set_current(PROJECT)
    assert PROJECT in bd.databases and "biosphere" in bd.databases
    methods = [
        m
        for m in bd.methods
        if m[0] == "IPCC 2021"
        and "GWP100" in " ".join(m)
        and "climate change: total (excl. biogenic CO2)" in m
    ]
    assert len(methods) == 1, methods
    method = methods[0]
    label = "baseline" if args.baseline else "corrected"
    if args.baseline:
        if args.baseline_workbook is None:
            parser.error("--baseline requires --baseline-workbook from v.2.5.3")
        new_database.FILEPATH_VANADIUM = args.baseline_workbook.resolve()
    report = {
        "project": PROJECT,
        "source_database": PROJECT,
        "source_version": "3.12",
        "system_model": "cutoff",
        "scenario": SCENARIO,
        "method": method,
        "variant": label,
    }
    started = time.monotonic()
    result_path = ROOT / f"results/vanadium-{label}-validation.json"
    if args.resume:
        report = json.loads(result_path.read_text())
        with (ROOT / f"dev/vanadium-{label}-source.pickle").open("rb") as stream:
            report["source"] = scores(pickle.load(stream), method)
        with (ROOT / f"dev/vanadium-{label}-scenario.pickle").open("rb") as stream:
            scenario_database = pickle.load(stream)
    else:
        ndb = NewDatabase(
            scenarios=[dict(SCENARIO)],
            source_db=PROJECT,
            source_version="3.12",
            key=os.environ["PREMISE_KEY"],
            system_model="cutoff",
            biosphere_name="biosphere",
            generate_reports=False,
            cleanup_expired_caches=False,
            keep_imports_uncertainty=True,
        )
        source = ndb.materialize_inventory()
        with (ROOT / f"dev/vanadium-{label}-source.pickle").open("wb") as stream:
            pickle.dump(source, stream)
        report["source"] = scores(source, method)
        result_path.write_text(json.dumps(report, indent=2))
        del source
        gc.collect()
        ndb.update()
        scenario_database = ndb.materialize_inventory()
        with (ROOT / f"dev/vanadium-{label}-scenario.pickle").open("wb") as stream:
            pickle.dump(scenario_database, stream)
    report["scenario_results"] = scores(scenario_database, method)
    report["elapsed_seconds"] = time.monotonic() - started
    result_path.write_text(json.dumps(report, indent=2) + "\n")
    print("VALIDATION COMPLETE", result_path, flush=True)


if __name__ == "__main__":
    main()
