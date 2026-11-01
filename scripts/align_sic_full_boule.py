"""Align SiC wafer net feed and rejects with the curated full-boule reference."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/sic-full-boule-adoption-20261027.json"
PLAN_SHA = "40d0b0462f959241c33bcd024b086748e81d1092c858814d5f1c414374f7c9c8"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED FULL-BOULE SiC MATERIAL ACCOUNTING, 2026-10-27. The physical "
    "wafer reference now counts the full 100 mm crystal, rather than only its "
    "66 mm usable section. Forty-seven 0.35 mm wafers retain 16.45 mm, giving "
    "a full-boule retention of 0.1645. Net SiC feed is 100/(47*0.35) = "
    "6.079027355623101 kg per kg physical wafer; crystal, kerf, polishing "
    "and unusable-region rejects total 5.079027355623101 kg. They enter "
    "the existing qualified mineral-waste proxy once, without a recovery "
    "credit. Actual waste classification and destination remain unverified. "
    "The published historical geometry is a reference scenario, not measured "
    "current-factory yield. Substrate electricity remains "
    "19932.858629976276 kWh/kg physical wafer: this material correction does "
    "not rescale the separately derived electricity reference. Production, "
    "supplier identities, uncertainty and all downstream fabrication/device "
    "coefficients remain unchanged. Purification, charge losses, process "
    "gases, direct emissions and industrial qualification remain unresolved. "
    "Technosphere and biosphere source declarations both remain 3.9. This "
    "qualification supersedes earlier usable-section material accounting "
    "in the historical notes retained below.\n\n"
)
ELECTRICITY_COMMENT = (
    "Substrate electricity retained at 19932.858629976276 kWh/kg physical "
    "wafer; the Warren reference is expressed on physical usable wafer area. "
    "Full-boule material accounting does not rescale this electricity input."
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved SiC full-boule reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Amount changes require formula and cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        for address, amount in plan["amount_cells"].items():
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.find(Q + "f") is not None:
                raise ValueError("Source amount cell moved or became a formula")
            cell.find(Q + "v").text = str(amount)
        for address, text in {
            "B1229": NOTE + plan["before_snapshot"]["comment"],
            "H1235": ELECTRICITY_COMMENT,
        }.items():
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.find(Q + "f") is not None:
                raise ValueError("Source documentation cell moved")
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
                raise ValueError("Another activity or downstream coefficient changed")
            continue
        if a != plan["before_snapshot"]:
            raise ValueError("Original SiC reference changed")
        expected = deepcopy(plan["after_snapshot"])
        expected["comment"] = NOTE + a["comment"]
        electricity = next(
            e for e in expected["exchanges"] if e["unit"] == "kilowatt hour"
        )
        electricity["comment"] = ELECTRICITY_COMMENT
        if expected != b:
            raise ValueError("Unapproved SiC data changed")
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
        "changed_cells": plan["changed_cells"],
        "all_other_imported_activities_unchanged": True,
        "activity_count": len(new),
        "retained_electricity_kWh_per_kg": plan["retained_electricity_kWh_per_kg"],
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
