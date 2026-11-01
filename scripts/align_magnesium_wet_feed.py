"""Apply the approved water-retention convention to the active magnesium route."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/magnesium-wet-feed-adoption-20261027.json"
PLAN_SHA = "cb3f36e62c0c25afdceb5c0b437e57bbf685aad7e8abfd58ff901842b8c65f76"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED MAGNESIUM WET-FEED WATER ACCOUNTING, 2026-10-27. Keep synthetic "
    "magnesium chloride in solution for hydrated-lime precipitation rather "
    "than isolating and re-dissolving a dry intermediate. The MgCl2 reference "
    "amount denotes solute equivalent; retained acid-carrier and reaction "
    "water are accounted for in the downstream brine water requirement. "
    "Per kg magnesium hydroxide, count 3.0709019328959926 kg acid-carrier "
    "water and 0.3251626038638667 kg reaction water toward the existing "
    "104.07159280039637 kg solution-water requirement. Added tap water "
    "becomes 100.67552826363651 kg, or 0.9516544877931421 kg per kg of the "
    "unchanged 105.79 kg brine purchase. Remove only the corresponding "
    "reaction-water emission from the MgCl2 helper. This is a qualified "
    "wet-feed assumption, not measured plant operation or complete water "
    "accounting. Reagents, utilities, treatment, precursor amounts, "
    "uncertainty and filtrate throughput remain unchanged. The disconnected "
    "legacy recipe and separately reviewed land-occupation flow are outside "
    "this correction. Original technosphere and biosphere declarations both "
    "remain 3.9. This qualification supersedes earlier dry-intermediate "
    "water descriptions below only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved magnesium wet-feed reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Changed water accounting requires formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        data = root.find(NS + "sheetData")
        removed = data.find(f"{NS}row[@r='{plan['removed_row']}']")
        if removed is None:
            raise ValueError("Original reaction-water row changed")
        data.remove(removed)
        cell = root.find(f".//{NS}c[@r='{plan['water_quantity_cell']}']")
        if cell is None or cell.find(NS + "f") is not None:
            raise ValueError("Water quantity cell changed or became a formula")
        value = cell.find(NS + "v")
        if value is None or value.text != str(plan["before_tap_water_kg_per_kg_brine"]):
            raise ValueError("Original brine-water quantity changed")
        value.text = str(plan["after_tap_water_kg_per_kg_brine"])
        for record in plan["records"]:
            note = root.find(f".//{NS}c[@r='{record['comment_cell']}']/{NS}is/{NS}t")
            if note is None or note.text != record["before_snapshot"]["comment"]:
                raise ValueError("Original magnesium helper metadata changed")
            note.text = NOTE + note.text
        result = BytesIO()
        with ZipFile(result, "w") as target:
            target.comment = source.comment
            for info in source.infolist():
                target.writestr(
                    info,
                    (
                        etree.tostring(root)
                        if info.filename == SHEET
                        else source.read(info.filename)
                    ),
                )
    return result.getvalue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    plan = reference()
    path = ROOT / "premise/data/additional_inventories" / plan["workbook"]
    original = path.read_bytes()
    updated = revised_workbook(original, plan)
    args.artifacts.mkdir(parents=True, exist_ok=False)
    before, staged = args.artifacts / "before.xlsx", args.artifacts / "staged.xlsx"
    before.write_bytes(original)
    staged.write_bytes(updated)
    from bw2io import ExcelImporter

    old, new = ExcelImporter(str(before)).data, ExcelImporter(str(staged)).data
    changes = {r["name"]: r for r in plan["records"]}
    expected = []
    for activity in old:
        change = changes.get(activity["name"])
        if change is None:
            expected.append(activity)
            continue
        if activity != change["before_snapshot"]:
            raise ValueError("Original helper scientific reference changed")
        revised = deepcopy(change["after_snapshot"])
        revised["comment"] = NOTE + revised["comment"]
        expected.append(revised)
    if new != expected:
        raise ValueError("Unapproved magnesium or other source data changed")
    if path.read_bytes() != original:
        raise ValueError("Concurrent source change")
    if args.apply:
        temp = path.with_suffix(".xlsx.tmp")
        temp.write_bytes(updated)
        temp.replace(path)
    receipt = {
        "applied": args.apply,
        "before_sha256": sha256(original).hexdigest(),
        "after_sha256": sha256(updated).hexdigest(),
        "original_activities": len(old),
        "final_activities": len(new),
        "edited_activities": list(changes),
        "other_29_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
