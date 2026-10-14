#!/usr/bin/env python3
"""Audit conservative metals defaults against the previous correction and source.

Requires existing isolated Brightway audit databases and private build snapshots.
Only aggregate LCIA results, diagnostics and contribution summaries are exported.
"""

import argparse
import hashlib
import json
import pickle
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from score_metals_markets import write_csv


def identity(dataset):
    return (dataset["name"], dataset["reference product"], dataset["location"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=["cutoff", "consequential"], required=True)
    parser.add_argument(
        "--private", type=Path, default=Path("/private/tmp/premise-metals-audit")
    )
    parser.add_argument(
        "--pre-correction",
        type=Path,
        default=Path("/private/tmp/metals-resource-diagnosis"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "results/metals-correctness/conservative-defaults",
    )
    args = parser.parse_args()
    import bw2data as bd
    import bw2calc as bc
    import numpy as np
    from premise.metals import get_in_ground_resource_exchanges

    output = args.output / args.system
    output.mkdir(parents=True, exist_ok=True)
    build = args.private / f"conservative-{args.system}"
    built = json.loads((build / "metadata.json").read_text())
    original = json.loads(
        (
            ROOT
            / f"results/metals-correctness/resource-fix/{args.system}/metadata.json"
        ).read_text()
    )
    bd.projects.set_current(built["audit_project"])
    before, after = original["after"], built["written_database"]
    config = {
        "project": built["audit_project"],
        "before": before,
        "after": after,
        "biosphere": built["biosphere"],
        "relics_mapping_sha256": original["relics_mapping_sha256"],
        "database_fingerprints": {
            name: hashlib.sha256(
                Path(bd.Database(name).filepath_processed()).read_bytes()
            ).hexdigest()
            for name in [before, after]
        },
    }
    assert (
        config["database_fingerprints"][before]
        == original["database_fingerprints"][before]
    )
    config_dir = args.private / f"conservative-score-config-{args.system}"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "metadata.json").write_text(json.dumps(config, indent=2))
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "dev/score_metals_markets.py"),
            "--audit",
            str(config_dir),
            "--output",
            str(output),
            "--skip-saved-score-check",
        ],
        check=True,
    )

    # Verify every source in-ground flow survives consequential transformation,
    # using the captured pre-correction inventory rather than rounded BW values.
    current = {
        identity(d): d for d in pickle.loads((build / "inventory.pickle").read_bytes())
    }
    preservation = {}
    if args.system == "consequential":
        previous = {
            identity(d): d
            for d in pickle.loads(
                (args.pre_correction / "pre-consequential.pickle").read_bytes()
            )
        }
        assert current.keys() <= previous.keys()

        def resources(ds):
            return sorted(
                (e["name"], tuple(e["categories"]), e["unit"], e["amount"])
                for e in get_in_ground_resource_exchanges(ds)
            )

        mismatches = [
            key
            for key, ds in current.items()
            if resources(ds) != resources(previous[key])
        ]
        assert not mismatches, f"Changed source resource flows: {mismatches[:5]}"
        preservation = {
            "resource_datasets_checked": len(current),
            "resource_mismatches": len(mismatches),
        }

    rows, contributions = [], []
    for stage, name in [
        ("source", built["source"]),
        ("previous", before),
        ("conservative", after),
    ]:
        activities = list(bd.Database(name))
        for product, indicator in [("copper, cathode", "Copper"), ("sulfur", "Sulfur")]:
            for location in ["World", "GLO", "RoW"]:
                matches = [
                    a
                    for a in activities
                    if identity(a) == (f"market for {product}", product, location)
                ]
                if matches:
                    break
            assert len(matches) == 1 and matches[0]["unit"] == "kilogram"
            activity = matches[0]
            method = ("RELICS", "metals extraction", indicator)
            lca = bc.LCA({activity.id: 1}, method)
            lca.lci()
            lca.lcia()
            amounts = np.asarray(lca.characterized_inventory.sum(axis=0)).ravel()
            assert np.isclose(amounts.sum(), lca.score, rtol=1e-10, atol=1e-12)
            rows.append(
                {
                    "stage": stage,
                    "database": name,
                    "activity": activity["name"],
                    "product": product,
                    "location": location,
                    "code": activity["code"],
                    "functional_unit": 1,
                    "unit": "kilogram",
                    "method": json.dumps(method),
                    "score": float(lca.score),
                }
            )
            top = np.argsort(abs(amounts))[-10:][::-1]
            for index in top:
                a = bd.get_node(id=lca.dicts.activity.reversed[index])
                contributions.append(
                    {
                        "stage": stage,
                        "market": product,
                        "activity": a["name"],
                        "product": a["reference product"],
                        "location": a["location"],
                        "contribution": float(amounts[index]),
                    }
                )
            contributions.append(
                {
                    "stage": stage,
                    "market": product,
                    "activity": "Remaining contributions",
                    "product": "",
                    "location": "",
                    "contribution": float(amounts.sum() - amounts[top].sum()),
                }
            )

    for filename in [
        "methodology.json",
        "material_decisions.json",
        "market_diagnostics.json",
        "resource_diagnostics.json",
        "validation.json",
    ]:
        (output / filename).write_text((build / filename).read_text())
    (output / "build_metadata.json").write_text(json.dumps(built, indent=2))
    (output / "preservation.json").write_text(json.dumps(preservation, indent=2))
    write_csv(output / "copper_sulfur_source_comparison.csv", rows)
    write_csv(output / "copper_sulfur_contributions.csv", contributions)
    print(
        json.dumps(
            {"system": args.system, "preservation": preservation, "focus_scores": rows}
        )
    )


if __name__ == "__main__":
    main()
