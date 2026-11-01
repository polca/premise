"""Itemize four fitted TI packages within the fixed battery-monitor board budget."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/fitted-control-packages-adoption-20261027.json"
PLAN_SHA = "df06a24ebf88e2b404572c36797eca63f3b1476e8ab37232158305a172761a31"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
COLUMNS = (
    "name",
    "amount",
    "location",
    "unit",
    "categories",
    "type",
    "reference product",
    "comment",
    "product",
)
NOTE = (
    "APPROVED FITTED CONTROL-PACKAGE ITEMIZATION, 2026-10-27. The reviewed "
    "70 g monitoring-board reference includes one each of TPS3436BDADADDFRQ1, "
    "TPS71533QDCKRQ1, TPD6E004RSER and ISO7741FQDWRQ1. Their first-variant "
    "material declarations are qualified package-material proxies, not exact "
    "fitted-lot assays. Deduct their full combined declared mass, "
    "0.000716853378 kg, including chemically unresolved additives, from the "
    "current post-capacitor unresolved allowance: 0.005913802956 becomes "
    "0.005196949578 kg per board. This itemizes existing coverage rather than "
    "adding board mass. Preserve 70 g output, 134 capacitors, 128 resistors, "
    "existing package doses, parent counts, assembly and retirement. Unknown "
    "additives remain within each package mass; do not fabricate chemical "
    "forms or renormalize known ingredients to full package mass. Keep the "
    "270 micrometre wafer-allocation reference and purchase native fabrication/"
    "yields once. Package assembly at 1 kWh/kg remains an engineering allowance. "
    "The separate CC2662R1FRGZT and two ESD2CAN24DBZR devices remain "
    "unquantified within residual coverage. These material references do not "
    "certify electrical topology, vehicle qualification or complete trace-metal "
    "and manufacturing coverage. Both original source declarations stay 3.9. "
    "The four package references are active on this approved local route; "
    "historical inactive-status and residual descriptions below are superseded "
    "only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved fitted-package reference changed")
    return json.loads(payload)


def add_row(data, number, values):
    if data.find(f"{Q}row[@r='{number}']") is not None:
        raise ValueError("New fitted-package row would overwrite source data")
    row = etree.Element(Q + "row", r=str(number))
    for index, value in enumerate(values):
        if value is None:
            continue
        cell = etree.SubElement(row, Q + "c", r=f"{chr(65 + index)}{number}")
        if isinstance(value, (int, float)):
            etree.SubElement(cell, Q + "v").text = str(value)
        else:
            cell.set("t", "inlineStr")
            node = etree.SubElement(etree.SubElement(cell, Q + "is"), Q + "t")
            node.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
            node.text = str(value)
    data.append(row)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Board relocation requires formula and cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        data = root.find(Q + "sheetData")
        moved = []
        for number in range(plan["old_board_start"], plan["old_board_end"] + 1):
            row = data.find(f"{Q}row[@r='{number}']")
            if row is None:
                raise ValueError("Original module-board block changed")
            clone = deepcopy(row)
            destination = str(number + plan["board_row_delta"])
            clone.set("r", destination)
            for cell in clone:
                cell.set("r", cell.get("r").rstrip("0123456789") + destination)
            moved.append(clone)
            data.remove(row)
        for helper in plan["new_helpers"]:
            snapshot, start = helper["snapshot"], helper["start_row"]
            rows = [["Activity", snapshot["name"]]]
            rows.extend(
                [
                    [k, snapshot[k]]
                    for k in (
                        "location",
                        "production amount",
                        "reference product",
                        "unit",
                        "source",
                    )
                ]
            )
            rows.extend(
                [
                    ["comment", NOTE + snapshot["comment"]],
                    ["type", snapshot["type"]],
                    ["Exchanges"],
                    list(COLUMNS),
                ]
            )
            rows.extend([[e.get(k) for k in COLUMNS] for e in snapshot["exchanges"]])
            if start + len(rows) - 1 != helper["end_row"]:
                raise ValueError("Package helper row contract changed")
            for offset, values in enumerate(rows):
                add_row(data, start + offset, values)
        for row in moved:
            data.append(row)
        node = root.find(f".//{Q}c[@r='{plan['new_board_comment_cell']}']/{Q}is/{Q}t")
        if node is None or node.text != plan["before_board_snapshot"]["comment"]:
            raise ValueError("Relocated board metadata changed")
        node.text = NOTE + node.text
        cell = root.find(f".//{Q}c[@r='{plan['new_board_residual_cell']}']")
        if cell is None or cell.find(Q + "f") is not None:
            raise ValueError("Relocated board residual moved or became a formula")
        cell.find(Q + "v").text = str(plan["new_residual_kg_per_kg_board"])
        purchases = plan["after_board_snapshot"]["exchanges"][-4:]
        for number, exchange in zip(
            plan["new_package_purchase_rows"], purchases, strict=True
        ):
            add_row(data, number, [exchange.get(k) for k in COLUMNS])
        dimension = root.find(Q + "dimension")
        if dimension.get("ref") != plan["original_dimension"]:
            raise ValueError("Original worksheet extent changed")
        dimension.set("ref", plan["new_dimension"])
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


def expected_data(old, plan):
    board_name = plan["before_board_snapshot"]["name"]
    (original_board,) = [d for d in old if d["name"] == board_name]
    if original_board != plan["before_board_snapshot"]:
        raise ValueError("Original module-board scientific reference changed")
    expected = [deepcopy(d) for d in old if d["name"] != board_name]
    for helper in plan["new_helpers"]:
        revised = deepcopy(helper["snapshot"])
        revised["comment"] = NOTE + revised["comment"]
        expected.append(revised)
    board = deepcopy(plan["after_board_snapshot"])
    board["comment"] = NOTE + board["comment"]
    expected.append(board)
    return expected


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
    if expected_data(old, plan) != new:
        raise ValueError("Unapproved package, board or other source data changed")
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
        "new_helpers": [h["snapshot"]["name"] for h in plan["new_helpers"]],
        "relocated_board_rows": {"from": [1206, 1234], "to": [1832, 1864]},
        "other_80_activities_and_row_addresses_unchanged": True,
        "full_package_mass_deducted_kg_per_board": plan[
            "added_full_package_mass_kg_per_board"
        ],
        "remaining_unresolved_kg_per_board": plan["new_residual_kg_per_board"],
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
