"""Align the rail-module fabricated-wafer allocation with the curated reference."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/rail-wafer-allocation-adoption-20261027.json"
PLAN_SHA = "0070a0dc475a53611c0f917aaf69df7e0dcca24708d3779cf600c54fd4f54e03"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED RAIL-MODULE WAFER ALLOCATION, 2026-10-27. Represent the retained "
    "0.02 kg silicon per kg module with 0.03179144810046098 m2 of native "
    "fabricated wafer, using 2330 kg/m3 density and a 270 micrometre allocation "
    "reference: 0.02/(2330*270e-6). This replaces the earlier 300 micrometre "
    "allocation; 300 micrometres remains a diagnostic alternative. Neither "
    "value establishes measured rail-IGBT geometry or actual fabrication "
    "technology. Native wafer fabrication and yields are purchased once; "
    "no extra raw wafer or complete packaged-device input is added. The "
    "generic module composition remains a qualified engineering proxy. "
    "Other module materials, 1 kWh assembly electricity per kg, parent module "
    "purchases, masses, counts, lifetime and service denominators remain "
    "unchanged. Technosphere and biosphere source declarations both remain "
    "3.9. This correction supersedes the 300 micrometre allocation in the "
    "historical notes retained below.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved rail-module allocation reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Wafer change requires formula and cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        amount = root.find(f".//{Q}c[@r='{plan['amount_cell']}']")
        if amount is None or amount.find(Q + "f") is not None:
            raise ValueError("Original wafer amount moved or became a formula")
        amount.find(Q + "v").text = str(plan["wafer_area_m2_per_kg_module"])
        replacement = plan["metadata_replacement"]
        for address in [plan["source_comment_cell"], *replacement["cells"]]:
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.get("t") != "inlineStr":
                raise ValueError("Original rail metadata moved or changed encoding")
            node = cell.find(f"{Q}is/{Q}t")
            if node is None:
                raise ValueError("Original rail metadata structure changed")
            if address == plan["source_comment_cell"]:
                if node.text != plan["before_snapshot"]["comment"]:
                    raise ValueError("Original module comment changed")
                node.text = NOTE + node.text
            else:
                if node.text.count(replacement["before"]) != 1:
                    raise ValueError("Original parent wafer description changed")
                node.text = node.text.replace(
                    replacement["before"], replacement["after"]
                )
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
        if a["name"] == plan["before_snapshot"]["name"]:
            if a != plan["before_snapshot"]:
                raise ValueError("Original module reference changed")
            expected = deepcopy(plan["after_snapshot"])
            expected["comment"] = NOTE + a["comment"]
        else:
            expected = deepcopy(a)
            replacement = plan["metadata_replacement"]
            for field in replacement["fields"]:
                if isinstance(expected.get(field), str):
                    expected[field] = expected[field].replace(
                        replacement["before"], replacement["after"]
                    )
        if expected != b:
            raise ValueError("Unapproved rail data or downstream quantity changed")
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
        "amount_cell": plan["amount_cell"],
        "wafer_area_m2_per_kg_module": plan["wafer_area_m2_per_kg_module"],
        "all_other_imported_scientific_data_unchanged": True,
        "parent_metadata_substitutions": plan["metadata_replacement"],
        "activity_count": len(new),
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
