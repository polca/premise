"""Add the approved explicit carrier water to the dry zinc-chloride reference."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/zinc-chloride-water-adoption-20261027.json"
PLAN_SHA = "248645abeaa4ef40a7b9867d68d4cd06b0be41bbf55805b5982f4cfd0c1dc53f"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED DRY ZINC-CHLORIDE CARRIER WATER, 2026-10-27. For the retained "
    "1.363 kg nominal ZnCl2 batch, purchase 1.7013733333333334 kg deionised "
    "water from Europe without Switzerland to prepare the selected 30 wt% acid "
    "stock: 0.72916 kg pure-equivalent HCl * (1/0.30 - 1). Under this adopted "
    "accounting convention the pure-equivalent acid input does not replace the "
    "separately purchased carrier water. The earlier assertion below that this "
    "carrier must not be purchased separately is superseded. Preserve all nine "
    "existing exchanges, including 0.0017013733333333333 m3 water to air as "
    "liquid-volume-equivalent evaporation accounting, 0.02 kg hydrogen, "
    "8.189967723809524 MJ heat, 0.1363 kWh electricity, reagents, freight and "
    "the 1.363 kg product reference. Dry phase, 30% stock, evaporation and "
    "heat/electricity allowances are qualified engineering assumptions, not "
    "measured industrial assay, losses or energy demand. Preserve the existing "
    "nominal batch rounding without invented closure or duplicate wastewater. "
    "Other recipes and all consumers stay unchanged. Original technosphere and "
    "biosphere declarations both remain 3.9. Historical source text follows "
    "unchanged."
    "\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved zinc chloride water reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Water addition requires dependent formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        root.find(NS + "dimension").set("ref", plan["after_dimension"])
        data = root.find(NS + "sheetData")
        number = plan["addition_row"]
        if data.find(f"{NS}row[@r='{number}']") is not None:
            raise ValueError("Water-addition row is no longer empty")
        row = etree.Element(NS + "row", r=str(number))
        exchange = plan["addition_exchange"]
        for column, field in (
            ("A", "name"),
            ("B", "amount"),
            ("C", "location"),
            ("D", "unit"),
            ("F", "type"),
            ("G", "database"),
            ("H", "reference product"),
        ):
            cell = etree.SubElement(row, NS + "c", r=f"{column}{number}")
            if field == "amount":
                etree.SubElement(cell, NS + "v").text = str(exchange[field])
            else:
                cell.set("t", "inlineStr")
                etree.SubElement(etree.SubElement(cell, NS + "is"), NS + "t").text = (
                    exchange[field]
                )
        data.append(row)
        data[:] = sorted(data, key=lambda element: int(element.get("r")))
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original zinc chloride metadata changed")
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
    (salt,) = [d for d in old if d["name"] == plan["activity"]]
    if salt != plan["before_snapshot"]:
        raise ValueError("Original zinc chloride scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is salt else d for d in old]:
        raise ValueError("Unapproved zinc chloride or other source data changed")
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
        "added_exchange_row": 1390,
        "other_52_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
