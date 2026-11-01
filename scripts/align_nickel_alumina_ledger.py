"""Apply the approved catalyst reagent-stock water and oxygen ledger."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/nickel-alumina-ledger-adoption-20261027.json"
PLAN_SHA = "a7ac1aca3f79ae74d0702ed32ed267240425c3786072f7255e9769f25c4763a6"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED NICKEL-ALUMINA REAGENT-STOCK WATER/OXYGEN LEDGER, 2026-10-27. "
    "Per kg catalyst, the two qualified 50% nitric-acid and caustic stock "
    "assumptions require 1.4205358314581416 kg carrier water in addition "
    "to the existing 4.131754633808913 kg dilution water. Deionised water "
    "therefore becomes 5.552290465267054 kg. Add 0.05738652942776854 kg "
    "Oxygen emission to generic air, completing the nominal manufacturer "
    "mass ledger without oxygen sale or recovery credit. The oxygen outlet "
    "is calculated, not measured factory emissions. Stock concentrations, "
    "catalyst loading and retention, utilities and NOx capture remain "
    "qualified engineering assumptions. Preserve existing water/NOx/nitrogen "
    "outlets, other chemicals, energy, residue treatment, uncertainty and "
    "all consumer quantities and retirement ownership. Original technosphere "
    "and biosphere source declarations both remain 3.9. This qualification "
    "supersedes earlier missing-stock-water and oxygen-outlet descriptions "
    "below only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved nickel-alumina ledger reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Ledger changes require dependent formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        cell = root.find(f".//{NS}c[@r='{plan['water_quantity_cell']}']")
        if cell is None or cell.find(NS + "f") is not None:
            raise ValueError("Original catalyst water cell changed")
        value = cell.find(NS + "v")
        if value is None or value.text != str(plan["before_DI_water_kg"]):
            raise ValueError("Original catalyst water quantity changed")
        value.text = str(plan["after_DI_water_kg"])
        data = root.find(NS + "sheetData")
        number = plan["new_exchange_row"]
        if data.find(f"{NS}row[@r='{number}']") is not None:
            raise ValueError("Oxygen outlet would overwrite an existing row")
        row = etree.Element(NS + "row", r=str(number))
        for column, field in (
            ("A", "name"),
            ("B", "amount"),
            ("D", "unit"),
            ("E", "categories"),
            ("F", "type"),
            ("G", "database"),
        ):
            cell = etree.SubElement(row, NS + "c", r=f"{column}{number}")
            value = plan["new_exchange"][field]
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
            raise ValueError("Original catalyst metadata changed")
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
    (catalyst,) = [d for d in old if d["name"] == plan["activity"]]
    if catalyst != plan["before_snapshot"]:
        raise ValueError("Original catalyst scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    if new != [revised if d is catalyst else d for d in old]:
        raise ValueError("Unapproved catalyst or other source data changed")
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
        "new_exchange_row": 1314,
        "water_quantity_cell": "B1305",
        "comment_cell": "B1297",
        "other_52_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
