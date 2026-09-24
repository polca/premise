"""Calculate a Brightway reference using the same local openLCA method factors.

No Brightway methods or inventories are modified. Conflicting package flows are
excluded exactly as in the JSON-LD export; full UUID-only scores are also reported
so the impact of excluded definitions is visible.
"""

import argparse
import json
import zipfile
from pathlib import Path

import bw2calc as bc
import bw2data as bd
import numpy as np

from premise.olca_export import load_method_mapping


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--method-package", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    bd.projects.set_current("ecoinvent-3.8-cutoff")
    target = json.loads((args.output / "comparison-target.json").read_text())
    candidates = [
        a
        for a in bd.Database("test1")
        if a["name"] == target["name"]
        and a["location"] == target["location"]
        and a["reference product"] == "electricity, low voltage"
    ]
    assert len(candidates) == 1
    activity = candidates[0]
    lca = bc.LCA({activity: 1})
    lca.lci()
    amounts = np.asarray(lca.inventory.sum(axis=1)).ravel()
    codes = {a.id: a["code"] for a in bd.Database("ecoinvent-3.8-biosphere")}
    inventory = {
        codes[node]: float(amounts[index])
        for node, index in lca.dicts.biosphere.items()
    }
    mapping = load_method_mapping(args.method_package, "3.8")
    values = []
    with zipfile.ZipFile(args.method_package) as package:
        method = json.loads(package.read(f"lcia_methods/{target['method_uuid']}.json"))
        for ref in method["impactCategories"]:
            category = json.loads(package.read(f"lcia_categories/{ref['@id']}.json"))
            score, uuid_score = 0.0, 0.0
            seen = set()
            for cf in category["impactFactors"]:
                code = cf["flow"]["@id"]
                assert (
                    code not in seen
                ), "Regional or duplicate factors need explicit handling"
                seen.add(code)
                assert not cf.get("location"), "Regional factors need explicit handling"
                assert cf["unit"]["name"] == "kg", cf
                amount = inventory.get(code, 0.0) * cf["value"]
                uuid_score += amount
                if code in mapping._references:
                    score += amount
            values.append(
                {
                    "uuid": ref["@id"],
                    "name": category["name"],
                    "value": score,
                    "uuid_only_score_including_conflicts": uuid_score,
                    "difference_from_excluded_definitions": uuid_score - score,
                }
            )
    report = {
        "project": bd.projects.current,
        "database": "test1",
        "activity": list(activity.key),
        "functional_unit": target["functional_unit"],
        "method": method["name"],
        "version": "3.8",
        "system_model": "cutoff",
        "values": values,
    }
    (args.output / "brightway-reference.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
