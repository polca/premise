"""Add the approved nominal argon outlet to the grade 430 steel reference."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/ferritic-argon-adoption-20261027.json"
PLAN_SHA = "6bd32ada870b952986be57ee39737026041d8f58757c142eb50166c8ced93a9b"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED NOMINAL ARGON OUTLET, 2026-10-27. Per kg finished grade 430 "
    "ferritic steel, retain the 0.02138737625378074 kg purchased liquid argon "
    "and add an equal positive Argon emission to generic air using the "
    "existing ecoinvent 3.12 biosphere identity. This assumes complete "
    "venting; actual factory emissions and gas retention remain unmeasured. "
    "Preserve the existing CO2 and nitrogen outlets, all material and energy "
    "purchases, other exchanges, uncertainty and downstream steel quantities. "
    "This does not establish complete emissions coverage or an LCIA "
    "improvement and adds no heat-recovery credit. Original technosphere and "
    "biosphere source declarations both remain 3.12. This qualification "
    "supersedes earlier gas-outlet descriptions below only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved argon-outlet reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        data = root.find(NS + "sheetData")
        number = plan["new_exchange_row"]
        if data.find(f"{NS}row[@r='{number}']") is not None:
            raise ValueError("Argon outlet would overwrite an existing row")
        row = etree.Element(NS + "row", r=str(number))
        exchange = plan["new_exchange"]
        for column, field in (
            ("A", "name"),
            ("B", "amount"),
            ("D", "unit"),
            ("E", "categories"),
            ("F", "type"),
        ):
            cell = etree.SubElement(row, NS + "c", r=f"{column}{number}")
            value = exchange[field]
            if isinstance(value, (int, float)):
                etree.SubElement(cell, NS + "v").text = str(value)
            else:
                cell.set("t", "inlineStr")
                etree.SubElement(etree.SubElement(cell, NS + "is"), NS + "t").text = (
                    value
                )
        data.append(row)
        data[:] = sorted(data, key=lambda element: int(element.get("r")))
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original steel metadata changed")
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
    (steel,) = [d for d in old if d["name"] == plan["activity"]]
    if steel != plan["before_snapshot"]:
        raise ValueError("Original steel scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is steel else d for d in old]:
        raise ValueError("Unapproved steel or other source data changed")
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
        "new_exchange_row": plan["new_exchange_row"],
        "comment_cell": plan["comment_cell"],
        "argon_air_outlet_kg_per_kg_steel": plan["new_exchange"]["amount"],
        "other_130_activities_unchanged": True,
        "source_background_version": "3.12",
        "source_biosphere_version": "3.12",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
