"""Reproduce ALIGN-016 from the pinned CLIC economic-allocation iridium reference.

Offline authoring only; no database writes. Existing workbook formulas/caches
outside the retired ZA activity keep their original addresses and payloads.
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
PLAN = ROOT / "scripts/data/iridium-economic-proxy-adoption-20261027.json"
PLAN_SHA = "d1f342bb788983ffd924679db0c4bac322cbd3fc87971935de6045c343eb2c1e"
INVENTORIES = ROOT / "premise/data/additional_inventories"
NEW_FILE = "lci-iridium-economic-proxy.xlsx"
STACK = "electrolyzer production, 1MWe, PEM, Stack"
LEGACY = "platinum group metal, extraction and refinery operations"
NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
Q = "{" + NS + "}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED IRIDIUM SUPPLIER ALIGNMENT, 2026-10-27. The unchanged 0.8 kg "
    "iridium purchase uses the independent Argonne-derived economic-allocation "
    "proxy shared with CLIC, not the rescaled ecoinvent platinum recipe. "
    "The supplier is declared separately against ecoinvent/biosphere 3.12; "
    "this stack's source declaration stays 3.9. The normal Premise importer "
    "migrates each source to the selected output version. Final-refinery "
    "reagents, remaining emissions, resource flows, water returns and waste "
    "treatments are incomplete: omissions do not imply zero burdens. "
    "Stack amount, replacements and all other scientific inputs are unchanged."
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(payload):
    return sha256(payload).hexdigest()


def adoption():
    payload = PLAN.read_bytes()
    require(digest(payload) == PLAN_SHA, "Adoption reference changed")
    return json.loads(payload)


def rewrite_parts(payload, replacements):
    output = BytesIO()
    with ZipFile(BytesIO(payload)) as source, ZipFile(output, "w") as target:
        target.comment = source.comment
        require(set(replacements) <= set(source.namelist()), "Missing ZIP part")
        for entry in source.infolist():
            target.writestr(
                entry, replacements.get(entry.filename, source.read(entry.filename))
            )
    return output.getvalue()


def retire_legacy_za(payload):
    """Remove one source block without shifting any remaining formula/cell."""
    book = load_workbook(BytesIO(payload), data_only=False)
    require(book["PGM"]["A76"].value == "Activity", "ZA block moved")
    require(book["PGM"]["B76"].value == LEGACY, "ZA activity changed")
    require(book["PGM"]["B78"].value == "ZA", "ZA location changed")
    require(book["PGM"]["B80"].value == "iridium", "ZA product changed")
    for sheet in book:
        for row in sheet:
            for cell in row:
                if cell.data_type != "f" or (
                    sheet.title == "PGM" and 76 <= cell.row < 150
                ):
                    continue
                for token in Tokenizer(cell.value).items:
                    if token.type != "OPERAND" or token.subtype != "RANGE":
                        continue
                    value = token.value
                    if "!" in value:
                        name, value = value.rsplit("!", 1)
                        if name.strip("'") != "PGM":
                            continue
                    elif sheet.title != "PGM":
                        continue
                    require(
                        not any(
                            76 <= int(n) < 150
                            for n in re.findall(r"\$?[A-Z]+\$?(\d+)", value)
                        ),
                        "Another formula depends on the retired source block",
                    )
    book.close()
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        data = root.find(Q + "sheetData")
        for row in list(data):
            if 76 <= int(row.get("r")) < 150:
                data.remove(row)
        chain = etree.fromstring(source.read("xl/calcChain.xml"))
        for cell in list(chain):
            # Every original chain entry explicitly declares its sheet.
            require(cell.get("i") is not None, "Implicit calcChain sheet")
            number = int(re.search(r"\d+", cell.get("r"))[0])
            if cell.get("i") == "1" and 76 <= number < 150:
                chain.remove(cell)
        return rewrite_parts(
            payload,
            {PART: etree.tostring(root), "xl/calcChain.xml": etree.tostring(chain)},
        )


def set_text(root, address, text):
    cell = root.find(f".//{Q}c[@r='{address}']")
    require(cell is not None and cell.find(Q + "f") is None, "Unexpected target cell")
    for child in list(cell):
        cell.remove(child)
    cell.set("t", "inlineStr")
    node = etree.SubElement(etree.SubElement(cell, Q + "is"), Q + "t")
    node.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    node.text = text


def update_stack(payload, snapshot):
    book = load_workbook(BytesIO(payload), data_only=False)
    sheet = book["lci"]
    start = next(
        r[0].row for r in sheet if r[0].value == "Activity" and r[1].value == STACK
    )
    comment = next(r[1] for r in list(sheet)[start:] if r[0].value == "comment")
    require(sheet["A177"].value == LEGACY, "Iridium supplier moved")
    require(sheet["G177"].value == "iridium", "Iridium product changed")
    require(sheet["B177"].value == 0.8, "Iridium dose changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
    set_text(root, "A177", snapshot["name"])
    set_text(
        root,
        "H177",
        "electrocatalyst anode; qualified economic-allocation iridium proxy",
    )
    set_text(root, comment.coordinate, comment.value + "\n\n" + NOTE)
    book.close()
    return rewrite_parts(payload, {PART: etree.tostring(root)})


def proxy_workbook(plan):
    snapshot = plan["snapshot"]
    book = Workbook()
    sheet = book.active
    sheet.title = "lci"
    sheet.append(["Database", "premise iridium foreground"])
    sheet.append([])
    sheet.append(["Activity", snapshot["name"]])
    for key, value in snapshot.items():
        if key in {"name", "exchanges", "database"}:
            continue
        sheet.append(
            [key, json.dumps(value) if isinstance(value, (dict, list)) else value]
        )
    for key, value in {
        "background version": "3.12",
        "biosphere version": "3.12",
        "license": plan["license"],
        "compiled by": plan["compiler"],
        "CLIC reference": {
            "dataset": 3648,
            "version": 4579,
            "snapshot sha256": plan["CLIC_snapshot_sha256"],
        },
    }.items():
        sheet.append([key, json.dumps(value) if isinstance(value, dict) else value])
    sheet.append(["Exchanges"])
    columns = [
        "name",
        "amount",
        "unit",
        "location",
        "categories",
        "type",
        "reference product",
        "comment",
    ]
    sheet.append(columns)
    numeric = {}
    for exchange in snapshot["exchanges"]:
        values = [
            (
                "::".join(exchange.get(key, []))
                if key == "categories"
                else exchange.get(key)
            )
            for key in columns
        ]
        sheet.append(values)
        numeric[f"B{sheet.max_row}"] = exchange["amount"]
    sheet.append([])
    output = BytesIO()
    book.save(output)
    book.close()
    # Excel's authoring formatter otherwise rounds full-precision source floats.
    with ZipFile(BytesIO(output.getvalue())) as source:
        root = etree.fromstring(source.read(PART))
    for address, amount in numeric.items():
        root.find(f".//{Q}c[@r='{address}']/{Q}v").text = repr(amount)
    return rewrite_parts(output.getvalue(), {PART: etree.tostring(root)})


def scientific_exchange(exchange):
    result = {
        k: v
        for k, v in exchange.items()
        if k not in {"input", "output", "code", "database"}
    }
    if "categories" in result:
        result["categories"] = "::".join(result["categories"])
    return result


def check_imports(before, after, plan):
    from bw2io import ExcelImporter

    old_pgm, old_stack = (
        ExcelImporter(str(before / name)).data
        for name in plan["original_workbooks_sha256"]
    )
    pgm, stacks = (
        ExcelImporter(str(after / name)).data
        for name in plan["original_workbooks_sha256"]
    )
    remaining = [
        d
        for d in old_pgm
        if not (d["reference product"] == "iridium" and d["location"] == "ZA")
    ]
    require(pgm == remaining, "Unrelated PGM inventory changed")
    for old, new in zip(old_stack, stacks, strict=True):
        if old["name"] != STACK:
            require(old == new, "Another electrolysis inventory changed")
            continue
        expected = deepcopy(old)
        expected["comment"] += "\n\n" + NOTE
        matches = [
            e for e in expected["exchanges"] if e.get("reference product") == "iridium"
        ]
        require(
            len(matches) == 1 and matches[0]["amount"] == 0.8,
            "Unexpected iridium input",
        )
        matches[0]["name"] = plan["snapshot"]["name"]
        matches[0][
            "comment"
        ] = "electrocatalyst anode; qualified economic-allocation iridium proxy"
        require(expected == new, "Stack scientific data changed")
    (proxy,) = ExcelImporter(str(after / NEW_FILE)).data
    require(
        proxy["exchanges"]
        == [scientific_exchange(e) for e in plan["snapshot"]["exchanges"]],
        "Proxy exchanges differ from approved CLIC snapshot",
    )
    return {
        "original_PGM_activities": len(old_pgm),
        "retained_PGM_activities": len(pgm),
        "electrolysis_activities": len(stacks),
        "proxy_exchanges": len(proxy["exchanges"]),
        "iridium_stack_kg": 0.8,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    plan = adoption()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    before, staged = args.artifacts / "before", args.artifacts / "staged"
    before.mkdir()
    staged.mkdir()
    original = {
        name: (INVENTORIES / name).read_bytes()
        for name in plan["original_workbooks_sha256"]
    }
    for name, payload in original.items():
        require(
            digest(payload) == plan["original_workbooks_sha256"][name],
            "Source workbook changed",
        )
        (before / name).write_bytes(payload)
    output = {
        "lci-PGM.xlsx": retire_legacy_za(original["lci-PGM.xlsx"]),
        "lci-hydrogen-electrolysis.xlsx": update_stack(
            original["lci-hydrogen-electrolysis.xlsx"], plan["snapshot"]
        ),
        NEW_FILE: proxy_workbook(plan),
    }
    for name, payload in output.items():
        (staged / name).write_bytes(payload)
    checks = check_imports(before, staged, plan)
    require(not (INVENTORIES / NEW_FILE).exists(), "Proxy workbook already exists")
    require(
        all((INVENTORIES / n).read_bytes() == p for n, p in original.items()),
        "Concurrent source change",
    )
    if args.apply:
        written = []
        try:
            for name, payload in output.items():
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
        "before_sha256": {n: digest(p) for n, p in original.items()},
        "after_sha256": {n: digest(p) for n, p in output.items()},
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
