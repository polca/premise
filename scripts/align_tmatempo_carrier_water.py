"""Complete the approved TMATEMPO peroxide-carrier water input."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/tmatempo-carrier-water-adoption-20261027.json"
PLAN_SHA = "b058e60bcaf4304c1c37fba6d8a6824df4d10de880b790252ee7f1ec0457d77b"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED TMATEMPO PEROXIDE-CARRIER WATER, 2026-10-27. Per kg product, "
    "preserve the 0.407459375 kg active peroxide supplied as the documented "
    "30% reaction feed. Its full carrier water is 0.9507385416666666 kg. "
    "Together with the unchanged 1.0027 kg initial-water allowance, total "
    "deionised water becomes 1.9534385416666666 kg, replacing the previous "
    "1.5459791666666667 kg partial-carrier total. The additional "
    "0.407459375 kg water retains an unassigned outlet. Evaporation, recovery, "
    "discharge and complete reaction/workup mass closure remain unresolved. "
    "Preserve 1 kg output, active peroxide, the existing neat-caustic and "
    "initial-water basis, pH adjustment, all other chemicals, utilities, "
    "waste treatment, uncertainty and consumer quantities. No new caustic "
    "stock assumption, outlet or treatment is assigned by this correction. "
    "Original technosphere and biosphere declarations both remain 3.9. This "
    "qualification supersedes older partial-carrier-water explanations below "
    "only within this bounded input correction.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved TMATEMPO carrier-water reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Water correction requires formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        cell = root.find(f".//{NS}c[@r='{plan['water_quantity_cell']}']")
        if cell is None or cell.find(NS + "f") is not None:
            raise ValueError("Original water quantity cell changed")
        value = cell.find(NS + "v")
        if value is None or value.text != str(plan["before_water_kg_per_kg_product"]):
            raise ValueError("Original water quantity changed")
        value.text = str(plan["after_water_kg_per_kg_product"])
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']")
        if note is None:
            raise ValueError("Original process comment changed")
        if note.get("t") == "s":
            strings = etree.fromstring(source.read("xl/sharedStrings.xml"))
            original = "".join(strings[int(note.find(NS + "v").text)].itertext())
        elif note.get("t") == "inlineStr":
            original = "".join(note.find(NS + "is").itertext())
        else:
            raise ValueError("Unexpected source comment representation")
        if original != plan["before_snapshot"]["comment"]:
            raise ValueError("Original process provenance changed")
        # A private inline comment leaves the shared-string table untouched.
        for child in list(note):
            note.remove(child)
        note.set("t", "inlineStr")
        text = etree.SubElement(etree.SubElement(note, NS + "is"), NS + "t")
        text.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        text.text = NOTE + original
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
    (activity,) = [d for d in old if d["name"] == plan["activity"]]
    if activity != plan["before_snapshot"]:
        raise ValueError("Original process scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is activity else d for d in old]:
        raise ValueError("Unapproved process or other source data changed")
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
        "water_quantity_cell": "B156",
        "comment_cell": "B144",
        "other_52_activities_unchanged": True,
        "unassigned_water_outlet_kg_per_kg_product": 0.407459375,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
