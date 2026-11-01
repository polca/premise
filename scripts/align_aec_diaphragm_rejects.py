"""Apply ALIGN-020 without shifting cells, altering caches or accessing databases."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/aec-diaphragm-reject-adoption-20261027.json"
PLAN_SHA = "9cbd2b74a6758af8c6091d6b211549857bdfbcf5c1a5bbd92466f88731ec58b7"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED COATING-REJECT TREATMENT SPLIT, 2026-10-27. Retain the existing "
    "5% coating-reject mass. Route its 85% zirconia fraction to the existing "
    "inert-disposal proxy and its 15% polysulfone fraction to the mixed-plastic "
    "proxy shared with CLIC. This is a composition-based treatment assumption, "
    "not evidence that the mixed rejects are separated in practice or that "
    "their actual treatment or hazardous classification is established. "
    "The two supplier identities exist in the original ecoinvent 3.9 catalogue; "
    "both source declarations remain 3.9. Ingredients, utilities, total reject "
    "mass, 121.8 kg stack purchase and finished-diaphragm retirement are unchanged. "
    "This split supersedes earlier notes routing the whole coating reject to "
    "inert disposal. Previous model notes are retained below.\n\n"
)
PLASTIC_NOTE = (
    "15% polysulfone share of the original coating reject; 85% zirconia stays "
    "in the inert-disposal flow. Composition-based engineering treatment proxy, "
    "not verified separation or actual disposal. No extra finished-diaphragm "
    "retirement or recovery credit."
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def reference():
    payload = PLAN.read_bytes()
    require(sha256(payload).hexdigest() == PLAN_SHA, "Approved reference changed")
    return json.loads(payload)


def set_value(row, column, value, template=None):
    address = column + row.get("r")
    cell = row.find(f"{Q}c[@r='{address}']")
    if cell is None:
        attrs = (
            {k: v for k, v in template.attrib.items() if k not in {"r", "t"}}
            if template is not None
            else {}
        )
        cell = etree.SubElement(row, Q + "c", r=address, **attrs)
    require(cell.find(Q + "f") is None, "Refusing to replace a formula")
    for child in list(cell):
        cell.remove(child)
    if isinstance(value, str):
        cell.set("t", "inlineStr")
        text = etree.SubElement(etree.SubElement(cell, Q + "is"), Q + "t")
        text.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        text.text = value
    else:
        cell.attrib.pop("t", None)
        etree.SubElement(cell, Q + "v").text = repr(value)
    row[:] = sorted(row, key=lambda c: c.get("r").rstrip("0123456789"))


def revised_workbook(payload, plan):
    require(
        sha256(payload).hexdigest() == plan["before_workbook_sha256"],
        "Source workbook changed",
    )
    inert = next(
        e
        for e in plan["snapshot"]["exchanges"]
        if e["name"] == "market for inert waste, for final disposal"
    )
    plastic = next(
        e
        for e in plan["snapshot"]["exchanges"]
        if e["name"] == "market for waste plastic, mixture"
    )
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        data = root.find(Q + "sheetData")
        original_row = data.find(f"{Q}row[@r='917']")
        require(original_row is not None, "Original reject row moved")
        amount = original_row.find(f"{Q}c[@r='B917']/{Q}v")
        require(
            amount is not None and float(amount.text) == -0.040912611614569605,
            "Original reject amount changed",
        )
        comment_row = data.find(f"{Q}row[@r='907']")
        require(comment_row is not None, "Diaphragm metadata moved")
        set_value(comment_row, "B", NOTE + plan["before_snapshot"]["comment"])
        set_value(original_row, "B", inert["amount"])
        row = data.find(f"{Q}row[@r='921']")
        if row is None:
            row = etree.Element(Q + "row", r="921")
            index = next(
                (i for i, existing in enumerate(data) if int(existing.get("r")) > 921),
                len(data),
            )
            data.insert(index, row)
        else:
            require(
                not any(
                    c.find(Q + "v") is not None
                    or c.find(Q + "is") is not None
                    or c.find(Q + "f") is not None
                    for c in row
                ),
                "Reserved blank exchange row is no longer empty",
            )
        values = {
            "A": plastic["name"],
            "B": plastic["amount"],
            "C": plastic["location"],
            "D": plastic["unit"],
            "F": "technosphere",
            "G": plastic["reference product"],
            "H": PLASTIC_NOTE,
            "I": plastic["reference product"],
        }
        for column, value in values.items():
            template = original_row.find(f"{Q}c[@r='{column}917']")
            set_value(row, column, value, template)
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


def scientific_key(exchange):
    return tuple(
        exchange.get(k, exchange.get("product") if k == "reference product" else None)
        for k in ("type", "name", "reference product", "location", "unit", "amount")
    )


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
    require(len(old) == len(new), "Activity count changed")
    for a, b in zip(old, new, strict=True):
        if a["name"] != plan["snapshot"]["name"]:
            require(a == b, "Another activity changed")
            continue
        wanted = deepcopy(a)
        wanted["comment"] = NOTE + a["comment"]
        next(
            e
            for e in wanted["exchanges"]
            if e["name"] == "market for inert waste, for final disposal"
        )["amount"] = next(
            e
            for e in plan["snapshot"]["exchanges"]
            if e["name"] == "market for inert waste, for final disposal"
        )[
            "amount"
        ]
        plastic = deepcopy(
            next(
                e
                for e in plan["snapshot"]["exchanges"]
                if e["name"] == "market for waste plastic, mixture"
            )
        )
        plastic["comment"] = PLASTIC_NOTE
        plastic["product"] = plastic["reference product"]
        wanted["exchanges"].append(plastic)
        require(wanted == b, "Unapproved diaphragm data changed")
        require(
            sorted(map(scientific_key, b["exchanges"]))
            == sorted(map(scientific_key, plan["snapshot"]["exchanges"])),
            "Diaphragm differs from CLIC science",
        )
    require(path.read_bytes() == original, "Concurrent source change")
    if args.apply:
        temp = path.with_suffix(".xlsx.tmp")
        temp.write_bytes(updated)
        temp.replace(path)
    receipt = {
        "applied": args.apply,
        "before_sha256": sha256(original).hexdigest(),
        "after_sha256": sha256(updated).hexdigest(),
        "activity_count_preserved": len(new),
        "diaphragm_exchanges": 11,
        "all_other_imported_data_unchanged": True,
        "existing_cells_changed": ["B907", "B917"],
        "added_exchange_row": 921,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
