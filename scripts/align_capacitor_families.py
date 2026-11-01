"""Apply the approved 39-part capacitor classification and coverage ledger."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/capacitor-family-adoption-20261027.json"
PLAN_SHA = "bf1cdcf4b4649c4ffc88a8fc0b21c89be2cf5dcd4482a8cbad0d4fc92fa88b21"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED 39-PART CAPACITOR FAMILY CORRECTION, 2026-10-27. Reclassify "
    "four 0402, two 0603 and thirty-three 0805 fitted parts from Class 2 to "
    "C0G. Preserve 134 capacitors and the 70 g monitor-board reference. "
    "Capacitor subtotal changes from 0.00127361 to 0.00098098 kg per board; "
    "the 0.00029263 kg difference increases the existing unresolved-component "
    "coverage allowance. This is proxy coverage bookkeeping, not additional "
    "measured interface hardware or laminate. The residual proxy purchase "
    "increases by 0.00029263/0.07 kg/kg board; all other parent doses, "
    "assembly and retirement stay unchanged. Two KEMET 0402 parts retain "
    "their inherited 1.06 mg allowance without an exact-part mass claim. "
    "Family compositions are cross-manufacturer engineering proxies; "
    "actual fitted-part assays, unknown dopants, 95% known-feed retention, "
    "mixed-waste treatment and process utilities remain qualified assumptions. "
    "Unknown retained material is not renormalized into measured ingredients. "
    "New C0G 0603/0805 helpers buy the existing calcium-zirconate precursor "
    "once; no extra populated board or packaged component is purchased. "
    "Technosphere and biosphere source declarations both remain 3.9. "
    "This correction supersedes older family/count descriptions retained "
    "below as history.\n\n"
)
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


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved capacitor reference changed")
    return json.loads(payload)


def add_row(data, number, values):
    if data.find(f"{Q}row[@r='{number}']") is not None:
        raise ValueError("New capacitor row would overwrite existing data")
    row = etree.Element(Q + "row", r=str(number))
    for index, value in enumerate(values):
        if value is None:
            continue
        address = f"{chr(65 + index)}{number}"
        cell = etree.SubElement(row, Q + "c", r=address)
        if isinstance(value, (int, float)):
            etree.SubElement(cell, Q + "v").text = str(value)
        else:
            cell.set("t", "inlineStr")
            text = etree.SubElement(etree.SubElement(cell, Q + "is"), Q + "t")
            text.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
            text.text = str(value)
    next_row = next((r for r in data if int(r.get("r")) > number), None)
    if next_row is None:
        data.append(row)
    else:
        data.insert(list(data).index(next_row), row)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        data = root.find(Q + "sheetData")
        replacement = plan["metadata_replacement"]
        for address in replacement["cells"]:
            cell = root.find(f".//{Q}c[@r='{address}']")
            node = cell.find(f"{Q}is/{Q}t") if cell is not None else None
            if node is None or node.text.count(replacement["before"]) != 1:
                raise ValueError("Original capacitor count description changed")
            node.text = node.text.replace(replacement["before"], replacement["after"])
            if address in {c["comment_cell"] for c in plan["cases"]}:
                node.text = NOTE + node.text
        for address, value in plan["amount_cells"].items():
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.find(Q + "f") is not None:
                raise ValueError("Original capacitor amount moved or became a formula")
            cell.find(Q + "v").text = str(value)
        for number, exchange in plan["new_bundle_exchange_rows"].items():
            exchange = dict(exchange, product=exchange["reference product"])
            add_row(data, int(number), [exchange.get(k) for k in COLUMNS])
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
                raise ValueError("New capacitor helper row contract changed")
            for offset, values in enumerate(rows):
                add_row(data, start + offset, values)
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
    cases = {c["before_snapshot"]["name"]: c for c in plan["cases"]}
    replacement = plan["metadata_replacement"]
    expected = []
    for activity in old:
        case = cases.get(activity["name"])
        if case is not None and activity != case["before_snapshot"]:
            raise ValueError("Original capacitor consumer changed")
        revised = deepcopy(case["after_snapshot"] if case else activity)
        if isinstance(revised.get("comment"), str):
            revised["comment"] = revised["comment"].replace(
                replacement["before"], replacement["after"]
            )
        if case:
            revised["comment"] = NOTE + revised["comment"]
        expected.append(revised)
    for helper in plan["new_helpers"]:
        revised = deepcopy(helper["snapshot"])
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
        raise ValueError("Unapproved capacitor, metadata or downstream data changed")
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
        "changed_amount_cells": plan["amount_cells"],
        "changed_metadata_cells": plan["metadata_replacement"]["cells"],
        "new_bundle_exchange_rows": [1411, 1412],
        "original_row_addresses_preserved": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
        "additional_unresolved_kg_per_board": plan[
            "additional_unresolved_kg_per_board"
        ],
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
