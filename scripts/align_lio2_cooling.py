"""Reuse the approved detailed RER cooling reference in standalone Li-O2 manufacture."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/lio2-cooling-adoption-20261027.json"
PLAN_SHA = "6c30b7a1f4e633de71b350e8375607514a2bca18451cb204ebd91ba7b793f1c3"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED Li-O2 STANDALONE COOLING SUPPLIER, 2026-10-27. Reuse the existing "
    "RER cooling system production, for Li-O2 battery reference with radiator, "
    "manifolds, clamps, pipe fitting, thermal pad, glycol and freight. Preserve "
    "0.08707503369776846 kg cooling per kg battery (29.07 kg per 333.85 kg "
    "reference system), including its formula and cached value. This supersedes "
    "the former GLO 7 kg aluminium plus 22.07 kg pure-glycol choice only for "
    "this standalone manufacturing input. The RER composition is an inherited "
    "engineering proxy, not verified battery-specific thermal design or "
    "performance. Its nominal material sum of 0.99996 kg retains source rounding"
    " without invented closure. Preserve the approved final-assembly "
    "electricity, all other exchanges, output, every helper and every other "
    "consumer. Retain the GLO helper for existing other uses. Control alignment "
    "remains separate; this supplier change does not establish complete recipe "
    "equivalence or authorize future learning. Original technosphere and "
    "biosphere declarations both remain 3.9. Historical source text follows "
    "unchanged."
    "\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved Li-O2 cooling reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        for change in plan["changes"]:
            cell = root.find(f".//{NS}c[@r='{change['cell']}']")
            if cell is None or cell.get("t") != "inlineStr":
                raise ValueError("Original cooling supplier storage changed")
            text = cell.find(f"{NS}is/{NS}t")
            if text is None or text.text != change["before"]:
                raise ValueError("Original cooling supplier identity changed")
            text.text = change["after"]
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original Li-O2 metadata changed")
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
    (battery,) = [d for d in old if d["name"] == plan["activity"]]
    if battery != plan["before_snapshot"]:
        raise ValueError("Original Li-O2 scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is battery else d for d in old]:
        raise ValueError("Unapproved Li-O2 or other source data changed")
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
        "modified_cells": ["A283", "E283", "G283", "B269"],
        "other_33_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
