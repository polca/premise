"""Align the reviewed display-film geometry and coupled glass allowance."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/display-coating-adoption-20261027.json"
PLAN_SHA = "2b1a9a94c4878af0224b5e6b46bd95fcc636ff07b925b91f240646fb782b13e2"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED DISPLAY-FILM GEOMETRY, 2026-10-27. The 7-inch charging display "
    "uses a qualified 0.013832 m2 active area and 200 nm indium tin oxide "
    "film: 2.7663999999999995e-9 m3 of native sputtering service per display. "
    "At 7140 kg/m3 the retained film is 1.9752095999999996e-5 kg; subtract "
    "it from the glass allowance once, leaving 0.26197404852393796 kg glass. "
    "Area and film thickness remain cross-revision engineering proxies, "
    "not measured fitted coating geometry. Native sputtering target losses "
    "are purchased once; add no separate indium supply or foreground target-"
    "loss multiplier. Preserve the documented 1.65 kg hardware reference, "
    "other component purchases, assembly, uncertainty and downstream doses. "
    "Original technosphere and biosphere source declarations both remain "
    "3.12. This qualification supersedes earlier coating-geometry and glass "
    "bookkeeping descriptions below only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved display-coating reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        if list(root.iter(NS + "f")):
            raise ValueError("New source formulas require dependency/cache review")
        for change in plan["changes"]:
            cell = root.find(f".//{NS}c[@r='{change['cell']}']")
            if cell is None or cell.find(NS + "f") is not None:
                raise ValueError("Display quantity cell changed or became a formula")
            value = cell.find(NS + "v")
            if value is None or value.text != str(change["before"]):
                raise ValueError("Original display quantity changed")
            value.text = str(change["after"])
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original display metadata changed")
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
    (display,) = [d for d in old if d["name"] == plan["activity"]]
    if display != plan["before_snapshot"]:
        raise ValueError("Original display scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is display else d for d in old]:
        raise ValueError("Unapproved display or other source data changed")
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
        "changes": plan["changes"],
        "comment_cell": plan["comment_cell"],
        "other_130_activities_unchanged": True,
        "source_background_version": "3.12",
        "source_biosphere_version": "3.12",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
