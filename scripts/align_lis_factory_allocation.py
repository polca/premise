"""Correct the approved Li-S factory allocation to the nominal-capacity basis."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/lis-factory-allocation-adoption-20261027.json"
PLAN_SHA = "f3b45492cf93aad59c900c83cb32aa3a389203c6334c734892bab23bc5d9d9f4"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED Li-S FACTORY ALLOCATION BASIS, 2026-10-27. Allocate 5e-7 "
    "operating-factory units per nominal kWh, multiplied by the retained 0.15 "
    "kWh/kg cell reference, giving 7.5e-8 factory units per kg cell. The factory"
    " coefficient is per nominal kWh, not per cell; the prior per-cell "
    "conversion and any conflicting basis descriptions in the activity history "
    "are superseded for this factory exchange only. This is a qualified static "
    "allocation convention, not measured industrial productivity. Preserve the "
    "47.319 g ingredient-sum cell mass basis, 150 Wh/kg specific-energy "
    "reference, cell assembly, formation electricity, factory construction and "
    "utility recipe, output and all consumers. This correction does not "
    "authorize a new cell design, future learning or automatic resizing. "
    "Original technosphere and biosphere declarations both remain 3.9. "
    "Historical exchange text follows unchanged."
    "\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved Li-S factory reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Factory correction requires dependent formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        cell = root.find(f".//{NS}c[@r='{plan['quantity_cell']}']")
        if cell is None or cell.get("s") != plan["quantity_style"]:
            raise ValueError("Factory quantity style changed")
        formula, value = cell.find(NS + "f"), cell.find(NS + "v")
        if formula is None or formula.text != plan["before_formula"]:
            raise ValueError("Factory allocation formula changed")
        if value is None or value.text != str(plan["before_cached_value"]):
            raise ValueError("Factory allocation cached value changed")
        formula.text, value.text = plan["after_formula"], str(
            plan["after_cached_value"]
        )
        comment = root.find(f".//{NS}c[@r='{plan['comment_cell']}']")
        if comment is None or comment.get("t") != "s":
            raise ValueError("Factory exchange comment storage changed")
        index = comment.find(NS + "v")
        if index is None or index.text != str(
            plan["before_comment_shared_string_index"]
        ):
            raise ValueError("Factory exchange comment reference changed")
        strings = etree.fromstring(source.read("xl/sharedStrings.xml"))
        old_comment = "".join(strings[int(index.text)].itertext())
        if old_comment != plan["before_exchange_comment"]:
            raise ValueError("Factory exchange comment text changed")
        comment.remove(index)
        comment.set("t", "inlineStr")
        etree.SubElement(etree.SubElement(comment, NS + "is"), NS + "t").text = (
            NOTE + old_comment
        )
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
    (cell,) = [d for d in old if d["name"] == plan["activity"]]
    if cell != plan["before_snapshot"]:
        raise ValueError("Original Li-S cell scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    (factory,) = [
        e for e in revised["exchanges"] if e["name"] == plan["comment_exchange"]
    ]
    factory[plan["comment_field"]] = NOTE + factory[plan["comment_field"]]
    if new != [revised if d is cell else d for d in old]:
        raise ValueError("Unapproved Li-S cell or other source data changed")
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
        "modified_cells": ["B472", "G472"],
        "other_84_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
