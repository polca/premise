"""Remove the approved extra foreground yield allowance from aluminium plate."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/aluminium-endplate-adoption-20261027.json"
PLAN_SHA = "5f4b890603586d87e296ff624fd12cca208d2b53a102d1f1cca081b754182319"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED ALUMINIUM END-PLATE LOSS OWNERSHIP, 2026-10-27. Per kg nominal "
    "finished plate, purchase 1 kg generic wrought alloy and 1 kg aluminium "
    "rolling service, with the existing 1 kg average working service unchanged. "
    "Remove only the unsupported extra foreground 95% retention allowance and "
    "its 0.05263157894736836 kg incineration purchase. Native embedded "
    "processing stock, scrap, losses, treatment and uncertainty remain at "
    "version-specific values; no fixed native-stock cancellation is applied. "
    "The reviewed reference has a 0.00298 kg/kg stock-minus-scrap diagnostic "
    "residual, left unallocated rather than added as a new waste, emission or "
    "recovery credit. Generic alloy and processing remain engineering proxies. "
    "The source specifies 10 mm plate while native rolling describes 0.2-6 mm "
    "sheet; geometry and processing equivalence are not certified. Alloy-product "
    "kilograms are nominal stock, not an elemental aluminium assay, and service "
    "kilograms are process purchases rather than retained material. Preserve "
    "plate output, installed parent quantities, other services and consumers. "
    "Original technosphere and biosphere declarations both remain 3.9. This "
    "qualification supersedes older extra-loss/incineration descriptions below "
    "only within this plate reference.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved aluminium endplate reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Plate correction requires dependent formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        for change in plan["changes"]:
            cell = root.find(f".//{NS}c[@r='{change['cell']}']")
            if cell is None or cell.find(NS + "f") is not None:
                raise ValueError("Plate quantity changed or became a formula")
            value = cell.find(NS + "v")
            if value is None or value.text != str(change["before"]):
                raise ValueError("Original plate quantity changed")
            value.text = str(change["after"])
        data = root.find(NS + "sheetData")
        row = data.find(f"{NS}row[@r='{plan['removed_row']}']")
        if row is None:
            raise ValueError("Original plate land row changed")
        data.remove(row)
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original plate metadata changed")
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
    (plate,) = [d for d in old if d["name"] == plan["activity"]]
    if plate != plan["before_snapshot"]:
        raise ValueError("Original plate scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is plate else d for d in old]:
        raise ValueError("Unapproved plate or other source data changed")
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
        "removed_exchange_row": 982,
        "other_52_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
