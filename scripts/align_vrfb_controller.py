"""Align the approved VRFB controller component masses and moulding input."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/vrfb-controller-adoption-20261027.json"
PLAN_SHA = "d1b4bf4940ab6e06bab4dda4f30438ed3c238302060ef4505fe11ed1f4d7561c"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED VRFB CONTROLLER MASS/MOULDING REFERENCE, 2026-10-27. Per kg "
    "controller, retained PP housing 0.15210236949152542 kg, LCD "
    "0.5750483805084746 kg and electronics 0.27284925 kg total 1 kg. "
    "Apply the reviewed 0.994 moulding-retention reference once: external "
    "PP feed and injection-moulding service each become 0.15302049244620264 kg. "
    "Native auxiliaries, embedded stock, recorded waste and uncertainty are "
    "retained; no fixed stock cancellation or new foreground polymer-waste "
    "flow is added. Unallocated native polymer-residual fate remains unknown. "
    "The inherited 15-inch LCD is a mass proxy for a 7-inch device; dimensional "
    "equivalence and complete component manufacture are unverified. Preserve "
    "all suppliers, source output, freight and consumer amounts. The existing "
    "13 kg parent allocation of 91% monitoring and 9% BMS remains an assumption, "
    "without a new parent architecture. Original technosphere and biosphere "
    "declarations both remain 3.9. Keep existing profile-specific supplier "
    "mapping and regional PP shares for later targets; do not invent a "
    "new global supplier. This qualification supersedes older component-mass "
    "and plastic-feed explanations below only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved VRFB controller reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        if plan["formula_reference_audit"]["dependent_formula_cells"]:
            raise ValueError("New source formulas require dependency/cache review")
        for change in plan["changes"]:
            cell = root.find(f".//{NS}c[@r='{change['cell']}']")
            if cell is None or cell.find(NS + "f") is not None:
                raise ValueError("Controller quantity cell changed or became a formula")
            value = cell.find(NS + "v")
            if value is None or value.text != str(change["before"]):
                raise ValueError("Original controller quantity changed")
            value.text = str(change["after"])
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original controller metadata changed")
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
    (controller,) = [d for d in old if d["name"] == plan["activity"]]
    if controller != plan["before_snapshot"]:
        raise ValueError("Original controller scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is controller else d for d in old]:
        raise ValueError("Unapproved controller or other source data changed")
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
        "other_29_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
