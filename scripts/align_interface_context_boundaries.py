"""Separate standalone interface manufacture from embedded residual materials."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/interface-context-boundaries-adoption-20261027.json"
PLAN_SHA = "d82a3126efe7cbc77ee31cc31994390e2a09a09fc581ae17ac40d7b9767065da"
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
    "APPROVED STANDALONE INTERFACE / EMBEDDED RESIDUAL BOUNDARIES, 2026-10-27. "
    "The separate auxiliary interface buys 0.4/3.08 = 0.12987012987012989 m2 "
    "of native unmounted Pb-free through-hole board per kg interface instead "
    "of 0.24 kg glass fibre plus 0.16 kg epoxy. The 40% finished-board fraction "
    "and two-layer 3.08 kg/m2 reference are qualified engineering proxies. "
    "Its existing 18% copper now denotes external component stock, not known "
    "board tracks; exact component/wiring partition is unverified. Other "
    "standalone component allowances and 2 kWh/kg assembly stay unchanged. "
    "Within the already detailed central/module boards, the former complete "
    "interface purchase instead becomes direct unidentified-component stock: "
    "Cu/ferrite/aluminium/nylon fractions 0.34615384615384615/"
    "0.28846153846153844/0.19230769230769232/0.17307692307692307. "
    "Preserve the current residual mass and 2 kWh/kg residual preparation "
    "allowance, without another PCB laminate or fitted capacitor, resistor, "
    "IC or connector charge. This is unverified material-coverage sensitivity, "
    "not measured residual composition or evidence of zero missing precious "
    "metals or fabrication burdens. Keep the 490 g central and 70 g module "
    "board budgets, all other component doses, counts, assembly and retirement. "
    "Original technosphere and biosphere declarations both stay 3.9. This "
    "qualification supersedes earlier interface/residual descriptions below "
    "only within this scope.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved interface-boundary reference changed")
    return json.loads(payload)


def exchange_row(number, exchange):
    row = etree.Element(Q + "row", r=str(number))
    for index, key in enumerate(COLUMNS):
        value = exchange.get(key)
        if value is None:
            continue
        cell = etree.SubElement(row, Q + "c", r=f"{chr(65 + index)}{number}")
        if isinstance(value, (int, float)):
            etree.SubElement(cell, Q + "v").text = str(value)
        else:
            cell.set("t", "inlineStr")
            text = etree.SubElement(etree.SubElement(cell, Q + "is"), Q + "t")
            text.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
            text.text = str(value)
    return row


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Affected row references require formula/cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        data = root.find(Q + "sheetData")

        def remove(number):
            row = data.find(f"{Q}row[@r='{number}']")
            if row is None:
                raise ValueError(f"Source row {number} changed")
            data.remove(row)
            return row

        standalone = plan["standalone"]
        remove(standalone["replaced_row"])
        remove(standalone["removed_row"])
        for number in range(1606, 1616):
            row = remove(number)
            row.set("r", str(number - 1))
            for cell in row:
                cell.set("r", cell.get("r").rstrip("0123456789") + str(number - 1))
            data.append(row)
        data.append(exchange_row(1604, standalone["replacement_exchange"]))
        for board in plan["embedded_boards"]:
            remove(board["replaced_row"])
            for number, exchange in zip(
                [board["replaced_row"], *board["added_rows"]],
                board["replacement_exchanges"],
                strict=True,
            ):
                if data.find(f"{Q}row[@r='{number}']") is not None:
                    raise ValueError("New residual row would overwrite source data")
                data.append(exchange_row(number, exchange))
        for change in [standalone, *plan["embedded_boards"]]:
            node = root.find(f".//{Q}c[@r='{change['comment_cell']}']/{Q}is/{Q}t")
            if node is None or node.text != change["before_snapshot"]["comment"]:
                raise ValueError("Original activity metadata changed")
            node.text = NOTE + node.text
        data[:] = sorted(data, key=lambda row: int(row.get("r")))
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
    changes = {c["name"]: c for c in [plan["standalone"], *plan["embedded_boards"]]}
    expected = []
    for original in old:
        change = changes.get(original["name"])
        if change is None:
            expected.append(deepcopy(original))
            continue
        if original != change["before_snapshot"]:
            raise ValueError("Original scientific reference changed")
        revised = deepcopy(change["after_snapshot"])
        revised["comment"] = NOTE + revised["comment"]
        expected.append(revised)
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
        raise ValueError("Unapproved interface, board or other source data changed")
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
        "edited_activities": list(
            c["name"] for c in [plan["standalone"], *plan["embedded_boards"]]
        ),
        "other_82_activities_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
        "residual_mass_and_existing_board_budgets_preserved": True,
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
