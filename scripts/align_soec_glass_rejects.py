"""Apply the approved glass-reject supplier change without changing quantities."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/soec-glass-reject-adoption-20261027.json"
PLAN_SHA = "f69c953cab8213950f3b209c6b85979c8b4f5a2322bd72c230689846218b6e26"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED GLASS-MANUFACTURING REJECT ROUTE, 2026-10-27. Retain the "
    "original 95% retention and 0.05263157894736836 kg reject per kg finished "
    "glass. Use the RoW hazardous-waste underground-deposit market as the "
    "qualified containment proxy shared with CLIC, replacing incineration. "
    "The exact supplier exists in the original ecoinvent 3.9 catalogue; both "
    "source declarations remain 3.9. This does not establish actual disposal, "
    "hazardous classification, leaching or conservative impacts. Inert disposal "
    "remains a documented alternative at the same mass. Precursors, heat, "
    "electricity, finished-glass quantity and all other coefficients are "
    "unchanged. Finished-seal retirement and unquantified volatile losses "
    "remain separate. The correction takes precedence over older incineration "
    "notes retained below.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved glass-reject reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        replacements = {
            "B667": NOTE + plan["before_snapshot"]["comment"],
            "A678": "market for hazardous waste, for underground deposit",
            "G678": "hazardous waste, for underground deposit",
            "I678": "hazardous waste, for underground deposit",
        }
        for address, text in replacements.items():
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.find(Q + "f") is not None:
                raise ValueError("Original target cell moved or became a formula")
            for child in list(cell):
                cell.remove(child)
            cell.set("t", "inlineStr")
            node = etree.SubElement(etree.SubElement(cell, Q + "is"), Q + "t")
            node.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
            node.text = text
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
    for a, b in zip(old, new, strict=True):
        if a["name"] != plan["before_snapshot"]["name"]:
            if a != b:
                raise ValueError("Another activity changed")
            continue
        if a != plan["before_snapshot"]:
            raise ValueError("Original glass recipe changed")
        wanted = deepcopy(plan["after_snapshot"])
        wanted["comment"] = NOTE + a["comment"]
        if wanted != b:
            raise ValueError("Unapproved glass data changed")
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
        "changed_existing_cells": ["B667", "A678", "G678", "I678"],
        "all_other_imported_data_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
        "finished_glass_kg": plan["finished_glass_kg"],
        "manufacturing_reject_kg": plan["reject_kg_in_stack_reference"],
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
