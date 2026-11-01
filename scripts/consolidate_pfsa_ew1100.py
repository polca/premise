"""Consolidate the two identical PFSA references into the approved CLIC source.

Offline, pinned authoring only. Preserve the addresses, formulas, cached values
and ZIP payloads outside the two removed blocks and the named consumer cells.
"""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import re
from zipfile import ZipFile

from lxml import etree
from openpyxl import Workbook, load_workbook
from openpyxl.formula.tokenizer import Tokenizer

ROOT = Path(__file__).resolve().parents[1]
INVENTORIES = ROOT / "premise/data/additional_inventories"
PLAN = ROOT / "scripts/data/pfsa-ew1100-adoption-20261027.json"
PLAN_SHA = "195062fc96b9ed8c4fe6bab08aa01af05207670736b03b808c438b77054b579b"
NEW_FILE = "lci-pfsa-ew1100.xlsx"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "SHARED PFSA REFERENCE, 2026-10-27. This purchase now uses the shared "
    "dry, unreinforced EW1100 engineering reference aligned with CLIC. The "
    "identical application-specific producer is consolidated, not purchased "
    "in addition. All quantities, uncertainty, factory, freight and lifetime "
    "allocations are unchanged. Both original source profiles and the shared "
    "source profile are 3.9. Older notes describing dedicated producers are "
    "retained as history; the shared reference is imported before its buyers. "
    "The qualified chemistry, PFAS/abatement gaps and CC BY-NC 4.0 foreground "
    "terms remain applicable. Previous exchange notes: "
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def reference():
    payload = PLAN.read_bytes()
    require(sha256(payload).hexdigest() == PLAN_SHA, "Adoption reference changed")
    return json.loads(payload)


def replace_parts(payload, replacements):
    result = BytesIO()
    with ZipFile(BytesIO(payload)) as source, ZipFile(result, "w") as target:
        target.comment = source.comment
        for info in source.infolist():
            target.writestr(
                info, replacements.get(info.filename, source.read(info.filename))
            )
    return result.getvalue()


def text_cell(root, address, value):
    cell = root.find(f".//{Q}c[@r='{address}']")
    require(cell is not None and cell.find(Q + "f") is None, "Target cell moved")
    for child in list(cell):
        cell.remove(child)
    cell.set("t", "inlineStr")
    node = etree.SubElement(etree.SubElement(cell, Q + "is"), Q + "t")
    node.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    node.text = value


def consolidate_workbook(payload, record, snapshot):
    require(sha256(payload).hexdigest() == record["sha256"], "Source workbook changed")
    start, end, row = record["start"], record["end_exclusive"], record["consumer_row"]
    book = load_workbook(BytesIO(payload), data_only=False)
    sheet = book.worksheets[0]
    require(sheet.title == record["sheet"], "Source sheet changed")
    require(
        sheet.cell(start, 1).value == "Activity"
        and sheet.cell(start, 2).value == record["original_name"],
        "Producer block moved",
    )
    require(
        sheet.cell(row, 1).value == record["original_name"]
        and sheet.cell(row, 7).value == record["original_product"],
        "Consumer moved",
    )
    previous_comment = sheet.cell(row, 8).value or ""
    for s in book:
        for cells in s:
            for cell in cells:
                if cell.data_type != "f" or s is sheet and start <= cell.row < end:
                    continue
                for token in Tokenizer(cell.value).items:
                    if token.type != "OPERAND" or token.subtype != "RANGE":
                        continue
                    value = token.value
                    if "!" in value:
                        name, value = value.rsplit("!", 1)
                        if name.strip("'") != sheet.title:
                            continue
                    elif s is not sheet:
                        continue
                    require(
                        not any(
                            start <= int(n) < end
                            for n in re.findall(r"\$?[A-Z]+\$?(\d+)", value)
                        ),
                        "Another formula depends on the removed block",
                    )
    book.close()
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        data = root.find(Q + "sheetData")
        for item in list(data):
            if start <= int(item.get("r")) < end:
                data.remove(item)
        text_cell(root, f"A{row}", snapshot["name"])
        text_cell(root, f"G{row}", snapshot["reference product"])
        text_cell(root, f"H{row}", NOTE + previous_comment)
        replacements = {PART: etree.tostring(root)}
        if "xl/calcChain.xml" in source.namelist():
            chain = etree.fromstring(source.read("xl/calcChain.xml"))
            workbook = etree.fromstring(source.read("xl/workbook.xml"))
            sheet_id = workbook.find(Q + "sheets")[0].get("sheetId")
            active, retained_active, removed = None, None, 0
            for item in list(chain):
                active = item.get("i", active)
                number = int(re.search(r"\d+", item.get("r"))[0])
                if active == sheet_id and start <= number < end:
                    chain.remove(item)
                    removed += 1
                    continue
                if retained_active != active and item.get("i") is None:
                    item.set("i", active)
                retained_active = active
            if removed:
                replacements["xl/calcChain.xml"] = etree.tostring(chain)
    return replace_parts(payload, replacements)


def workbook_bytes(plan):
    snapshot = plan["snapshot"]
    book = Workbook()
    sheet = book.active
    sheet.title = "lci"
    sheet.append(["Database", "premise shared PFSA foreground"])
    sheet.append([])
    sheet.append(["Activity", snapshot["name"]])
    metadata = {
        **{
            k: v
            for k, v in snapshot.items()
            if k not in {"name", "exchanges", "database"}
        },
        "production amount": 1,
        "activity type": "ordinary transforming activity",
        "background version": "3.9",
        "biosphere version": "3.9",
        "license": plan["license"],
        "compiled by": plan["compiler"],
        "CLIC reference": {
            "dataset": plan["CLIC_dataset_id"],
            "version": plan["CLIC_version_id"],
            "snapshot sha256": plan["CLIC_snapshot_sha256"],
        },
    }
    for key, value in metadata.items():
        sheet.append(
            [key, json.dumps(value) if isinstance(value, (dict, list)) else value]
        )
    sheet.append(["Exchanges"])
    columns = [
        "name",
        "amount",
        "location",
        "unit",
        "categories",
        "type",
        "reference product",
        "comment",
    ]
    sheet.append(columns)
    numeric = {}
    for exchange in snapshot["exchanges"]:
        sheet.append(
            [
                (
                    "::".join(exchange.get(key, []))
                    if key == "categories"
                    else exchange.get(key)
                )
                for key in columns
            ]
        )
        numeric[f"B{sheet.max_row}"] = exchange["amount"]
    sheet.append([])
    raw = BytesIO()
    book.save(raw)
    book.close()
    with ZipFile(BytesIO(raw.getvalue())) as source:
        root = etree.fromstring(source.read(PART))
    for address, amount in numeric.items():
        root.find(f".//{Q}c[@r='{address}']/{Q}v").text = repr(amount)
    return replace_parts(raw.getvalue(), {PART: etree.tostring(root)})


def scientific_exchange(exchange):
    result = {
        k: v
        for k, v in exchange.items()
        if k not in {"product", "input", "output", "code", "database", "comment"}
    }
    if isinstance(result.get("categories"), (list, tuple)):
        result["categories"] = "::".join(result["categories"])
    return result


def check_imports(before, staged, plan):
    from bw2io import ExcelImporter

    (shared,) = ExcelImporter(str(staged / NEW_FILE)).data
    expected = [scientific_exchange(e) for e in plan["snapshot"]["exchanges"]]
    require(
        [scientific_exchange(e) for e in shared["exchanges"]] == expected,
        "Shared recipe differs from CLIC",
    )
    receipts = []
    for record in plan["original_workbooks"]:
        name = record["workbook"]
        old, new = (
            ExcelImporter(str(before / name)).data,
            ExcelImporter(str(staged / name)).data,
        )
        (producer,) = [d for d in old if d["name"] == record["original_name"]]
        require(
            [
                scientific_exchange(e)
                for e in producer["exchanges"]
                if e["type"] != "production"
            ]
            == [e for e in expected if e["type"] != "production"],
            "Original recipes are no longer scientifically identical",
        )
        wanted = deepcopy([d for d in old if d is not producer])
        count = 0
        for dataset in wanted:
            for exchange in dataset["exchanges"]:
                if (
                    exchange["type"] == "technosphere"
                    and exchange["name"] == record["original_name"]
                ):
                    exchange["name"] = plan["snapshot"]["name"]
                    exchange["reference product"] = plan["snapshot"][
                        "reference product"
                    ]
                    exchange["comment"] = NOTE + exchange.get("comment", "")
                    count += 1
        require(count == 1 and new == wanted, "Unrelated imported data changed")
        receipts.append(
            {
                "workbook": name,
                "activities_before": len(old),
                "activities_after": len(new),
                "relabeled_purchases": count,
                "all_other_imported_data_unchanged": True,
            }
        )
    return receipts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    plan = reference()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    before, staged = args.artifacts / "before", args.artifacts / "staged"
    before.mkdir()
    staged.mkdir()
    original, outputs = {}, {}
    for record in plan["original_workbooks"]:
        name = record["workbook"]
        original[name] = (INVENTORIES / name).read_bytes()
        (before / name).write_bytes(original[name])
        outputs[name] = consolidate_workbook(original[name], record, plan["snapshot"])
    outputs[NEW_FILE] = workbook_bytes(plan)
    for name, payload in outputs.items():
        (staged / name).write_bytes(payload)
    checks = check_imports(before, staged, plan)
    require(not (INVENTORIES / NEW_FILE).exists(), "Shared workbook already exists")
    require(
        all(
            (INVENTORIES / name).read_bytes() == data for name, data in original.items()
        ),
        "Concurrent workbook edit",
    )
    if args.apply:
        written = []
        try:
            for name, payload in outputs.items():
                temp = INVENTORIES / (name + ".tmp")
                temp.write_bytes(payload)
                temp.replace(INVENTORIES / name)
                written.append(name)
        except BaseException:
            for name in written:
                if name in original:
                    (INVENTORIES / name).write_bytes(original[name])
                else:
                    (INVENTORIES / name).unlink()
            raise
    receipt = {
        "applied": args.apply,
        "checks": checks,
        "before_sha256": {n: sha256(v).hexdigest() for n, v in original.items()},
        "after_sha256": {n: sha256(v).hexdigest() for n, v in outputs.items()},
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
