"""Remove the approved inherited extraction-land charge from precipitation."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/magnesium-land-adoption-20261027.json"
PLAN_SHA = "068519ddac5d7ccf7e530019e57bf7622209c5b1efd6b2ba98bf213461351275"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED MAGNESIUM PRECIPITATION LAND BOUNDARY, 2026-10-27. Remove only "
    "the inherited 1.63 square meter-year mineral-extraction occupation per "
    "kg magnesium hydroxide from this active synthetic precipitation stage. "
    "Upstream supplier inventories retain their own land burdens. Own-stage "
    "chemical-plant land remains unquantified, not zero; no substitute plant "
    "area or land transformation is invented. Preserve the disconnected "
    "legacy recipe and its land exchange, the NMMT precursor dose of "
    "0.0275 kg, all reagents, wet-feed water accounting, utilities, waste "
    "treatment, other exchanges and uncertainty. Original technosphere and "
    "biosphere declarations both remain 3.9. This qualification supersedes "
    "earlier foreground extraction-land descriptions below only within this "
    "active precipitation reference.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved magnesium land-boundary reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Land removal requires dependent formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        data = root.find(NS + "sheetData")
        row = data.find(f"{NS}row[@r='{plan['removed_row']}']")
        if row is None:
            raise ValueError("Original precipitation land row changed")
        data.remove(row)
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original precipitation metadata changed")
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
    (precipitation,) = [d for d in old if d["name"] == plan["activity"]]
    if precipitation != plan["before_snapshot"]:
        raise ValueError("Original precipitation scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is precipitation else d for d in old]:
        raise ValueError("Unapproved precipitation or other source data changed")
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
        "edited_activity": plan["activity"],
        "removed_exchange_row": 616,
        "other_30_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
