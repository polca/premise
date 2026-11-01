"""Adopt the approved lithium-foil internal manufacturing return convention."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/lithium-foil-return-adoption-20261027.json"
PLAN_SHA = "2024fe5edbe91da1ba40f7503117834445a61975454bbde9c9ae72965f9f27bb"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = "APPROVED LITHIUM-FOIL INTERNAL RETURN, 2026-10-27. Per kg foil, purchase 1 kg net external lithium from electrolysis of lithium chloride. Manufacturing offcuts return internally under the adopted source convention; remove the 0.020408163265306145 kg external hazardous-waste incineration purchase. Preserve the 4.95 kg density-adjusted rolling-service coefficient and its assumed 0.5 kWh/reference-kg processing intensity (2.475 kWh); this coefficient is not measured throughput or pass count, plus all other utilities, output and downstream consumers. This is a qualified engineering convention, not measured zero lithium loss. Actual losses and additional internal handling, remelting, atmosphere and lubrication burdens remain unquantified. No recovery credit, external scrap disposal, duplicate cell factory or measured closed-loop performance is claimed. Original technosphere and biosphere declarations both remain 3.9. This qualification supersedes earlier offcut-disposal descriptions within this foil reference. Historical exchange text follows unchanged.\n\n"


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved lithium foil reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Foil correction requires dependent formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        cell = root.find(f".//{NS}c[@r='{plan['quantity_cell']}']")
        if cell is None or cell.find(NS + "f") is not None:
            raise ValueError("Lithium quantity changed or became a formula")
        value = cell.find(NS + "v")
        if value is None or value.text != str(plan["before_lithium_kg"]):
            raise ValueError("Original lithium quantity changed")
        value.text = str(plan["after_lithium_kg"])
        data = root.find(NS + "sheetData")
        row = data.find(f"{NS}row[@r='{plan['removed_row']}']")
        if row is None:
            raise ValueError("Original foil waste row changed")
        data.remove(row)
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_rolling_comment"]:
            raise ValueError("Original rolling-exchange metadata changed")
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
    (foil,) = [d for d in old if d["name"] == plan["activity"]]
    if foil != plan["before_snapshot"]:
        raise ValueError("Original foil scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    (rolling,) = [
        e for e in revised["exchanges"] if e["name"] == plan["comment_exchange"]
    ]
    rolling[plan["comment_field"]] = NOTE + rolling[plan["comment_field"]]
    if new != [revised if d is foil else d for d in old]:
        raise ValueError("Unapproved foil or other source data changed")
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
        "removed_exchange_row": 303,
        "other_84_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
