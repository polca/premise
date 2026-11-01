"""Apply the approved ferritic-shell finishing convention to a 150 kW cabinet."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/charger-finishing-adoption-20261027.json"
PLAN_SHA = "715db7ec6217259f103f1b313fc45337e71223470ad673d02e16ca2582a55320"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED FERRITIC-SHELL FINISHING OWNERSHIP, 2026-10-27. For each "
    "150 kW charger cabinet, generic steel-working service decreases from "
    "742.6821116624471 to 593.6378316624472 kg, excluding the 149.04428 kg "
    "ferritic shell under the qualified melt/finish ownership convention "
    "already used for the 200 kW shell. This assigns the service boundary; "
    "it does not prove duplicate physical steel stock or complete actual "
    "finishing coverage. Retain all material purchases, native embedded "
    "stock and uncertainty, remaining ordinary-steel service, other services, "
    "assembly, parent doses and retirement. No fixed native-stock cancellation "
    "or recovery credit is added. Original technosphere and biosphere source "
    "declarations both remain 3.12. This qualification supersedes earlier "
    "generic steel-working descriptions below only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved cabinet-finishing reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Dependent formulas require cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        cell = root.find(f".//{NS}c[@r='{plan['quantity_cell']}']")
        if cell is None or cell.find(NS + "f") is not None:
            raise ValueError("Cabinet service cell changed or became a formula")
        value = cell.find(NS + "v")
        if value is None or value.text != str(plan["before_service_kg_per_cabinet"]):
            raise ValueError("Original cabinet service quantity changed")
        value.text = str(plan["after_service_kg_per_cabinet"])
        note = root.find(f".//{NS}c[@r='{plan['comment_cell']}']/{NS}is/{NS}t")
        if note is None or note.text != plan["before_snapshot"]["comment"]:
            raise ValueError("Original cabinet metadata changed")
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
    (cabinet,) = [d for d in old if d["name"] == plan["activity"]]
    if cabinet != plan["before_snapshot"]:
        raise ValueError("Original cabinet scientific reference changed")
    revised = deepcopy(plan["after_snapshot"])
    revised["comment"] = NOTE + revised["comment"]
    expected = [revised if d is cabinet else d for d in old]
    if new != expected:
        raise ValueError("Unapproved cabinet or other source data changed")
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
        "other_130_activities_unchanged": True,
        "quantity_cell": plan["quantity_cell"],
        "comment_cell": plan["comment_cell"],
        "before_service_kg_per_cabinet": plan["before_service_kg_per_cabinet"],
        "after_service_kg_per_cabinet": plan["after_service_kg_per_cabinet"],
        "source_background_version": "3.12",
        "source_biosphere_version": "3.12",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
