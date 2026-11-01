"""Align connector material feeds and remove an unsupported reject treatment."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/connector-reference-adoption-20261027.json"
PLAN_SHA = "a1d92c0624ec9a8ab069ce45c988757cb48498009e8449538fb2500cd9490020"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED MOLEX CONNECTOR MATERIAL/REJECT-FATE REFERENCE, 2026-10-27. "
    "Use the qualified five-Molex-header, 36-contact, 8.594 g cohort material "
    "reference, scaled by mass to the unchanged 20 g/module-board and "
    "100 g/central-board allowances. This material reference excludes the "
    "additional Samtec geometric estimate; it does not establish that a "
    "Samtec header is absent from fitted hardware or certify a complete "
    "header census. Corrected plating-volume bookkeeping subtracts coating "
    "volume from finished-contact volume before applying alloy density, "
    "then closes the housing residual. Retain the thin-layer approximation, "
    "geometry, alloy proportions, plating thickness and 70/30 nylon/glass "
    "housing as engineering assumptions. The 95% net material-handling "
    "reference is not measured deposition efficiency or manufacturing loss. "
    "Nominal rejects remain 0.05263157894736836 kg/kg finished connector; "
    "composition, physical destination and treatment burdens are unassigned. "
    "Remove the unsupported hazardous-waste incineration purchase without "
    "claiming zero physical loss, zero impact or environmental improvement. "
    "Retain 1 kWh/kg assembly, all downstream allowances, board masses and "
    "existing retirement ownership, with no extra component retirement or "
    "recovery credit. Both original source declarations stay 3.9. This "
    "qualification supersedes older Samtec-inclusive material and reject-"
    "treatment descriptions retained below as history.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved connector reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Connector changes require formula and cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        for address, value in plan["amount_cells"].items():
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.find(Q + "f") is not None:
                raise ValueError("Original connector amount moved or became a formula")
            cell.find(Q + "v").text = str(value)
        for address in plan["comment_cells"]:
            cell = root.find(f".//{Q}c[@r='{address}']")
            node = cell.find(f"{Q}is/{Q}t") if cell is not None else None
            if node is None:
                raise ValueError("Original connector/parent metadata moved")
            node.text = NOTE + node.text
        cell = root.find(f".//{Q}c[@r='{plan['source_cell']}']")
        node = cell.find(f"{Q}is/{Q}t") if cell is not None else None
        if node is None or node.text != plan["before_snapshot"]["source"]:
            raise ValueError("Original connector source changed")
        node.text = plan["after_snapshot"]["source"]
        data = root.find(Q + "sheetData")
        waste = data.find(f"{Q}row[@r='{plan['removed_disposal_row']}']")
        electricity = data.find(f"{Q}row[@r='{plan['relocated_electricity_from']}']")
        if waste is None or electricity is None:
            raise ValueError("Original connector exchange rows moved")
        moved = deepcopy(electricity)
        destination = str(plan["relocated_electricity_to"])
        moved.set("r", destination)
        for cell in moved:
            cell.set("r", cell.get("r").rstrip("0123456789") + destination)
        position = list(data).index(waste)
        data.remove(waste)
        data.remove(electricity)
        data.insert(position, moved)
        result = BytesIO()
        with ZipFile(result, "w") as target:
            target.comment = source.comment
            for info in source.infolist():
                target.writestr(
                    info,
                    (
                        etree.tostring(root)
                        if info.filename == PART
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
    if len(old) != len(new):
        raise ValueError("Activity count changed")
    parents = {
        "printed circuit board production, battery central control, mounted",
        "printed circuit board production, battery module control, mounted",
    }
    for a, b in zip(old, new, strict=True):
        if a["name"] == plan["before_snapshot"]["name"]:
            if a != plan["before_snapshot"]:
                raise ValueError("Original connector changed")
            expected = deepcopy(plan["after_snapshot"])
        else:
            expected = deepcopy(a)
        if a["name"] in parents | {plan["before_snapshot"]["name"]}:
            expected["comment"] = NOTE + a["comment"]
        if expected != b:
            raise ValueError("Unapproved connector or parent data changed")
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
        "activity_count": len(new),
        "changed_material_amount_cells": plan["amount_cells"],
        "changed_comment_cells": plan["comment_cells"],
        "changed_source_cell": plan["source_cell"],
        "removed_disposal_row": plan["removed_disposal_row"],
        "electricity_relocated_from": plan["relocated_electricity_from"],
        "electricity_relocated_to": plan["relocated_electricity_to"],
        "retained_assembly_kWh_per_kg": 1,
        "nominal_reject_kg_per_kg_finished": plan["nominal_reject_kg_per_kg_finished"],
        "reject_treatment_burdens_unassigned": True,
        "all_other_quantities_and_row_addresses_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
