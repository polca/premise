"""Align four reviewed battery-control package wafer allocations to 270 um."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/control-package-wafer-adoption-20261027.json"
PLAN_SHA = "146de5097245f6549fcb6022bd557d84e0d7a323ae8b50d829f6036d13af930b"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED CONTROL-PACKAGE WAFER ALLOCATION, 2026-10-27. Use the "
    "270 micrometre native-wafer-compatible allocation reference at "
    "2330 kg/m3 for this reviewed package, preserving its declared silicon "
    "mass. Wafer area equals silicon mass/(2330*270e-6), increasing by "
    "300/270 relative to the earlier 300 micrometre reference. This is an "
    "allocation assumption, not measured fitted-die geometry; 300 micrometres "
    "remains a diagnostic alternative. Native fabrication and yields are "
    "purchased once, without another raw-silicon, wafer or complete-device "
    "input. All non-wafer quantities, package counts, parent purchases, board "
    "budgets, assembly and retirement remain unchanged. The monitoring ASIC "
    "retains the explicitly imperfect BQ79616 material proxy for the fitted "
    "BQ79718 role, without a functional replacement claim. Package chemistry, "
    "fused-silica manufacture and unmodelled phosphorus gaps remain outside "
    "this correction. Both original source declarations stay 3.9. This "
    "qualification supersedes earlier wafer-allocation descriptions in "
    "the historical notes retained below.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved control-package wafer reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Wafer changes require formula and cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        for address, value in plan["amount_cells"].items():
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.find(Q + "f") is not None:
                raise ValueError("Original wafer cell moved or became a formula")
            cell.find(Q + "v").text = str(value)
        for replacement in plan["metadata_replacements"]:
            for address in replacement["cells"]:
                cell = root.find(f".//{Q}c[@r='{address}']")
                node = cell.find(f"{Q}is/{Q}t") if cell is not None else None
                if node is None or node.text.count(replacement["before"]) != 1:
                    raise ValueError("Original wafer description changed")
                node.text = node.text.replace(
                    replacement["before"], replacement["after"]
                )
        for case in plan["cases"]:
            node = root.find(f".//{Q}c[@r='{case['comment_cell']}']/{Q}is/{Q}t")
            node.text = NOTE + node.text
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
    expected = []
    for activity in old:
        case = cases.get(activity["name"])
        if case is not None and activity != case["before_snapshot"]:
            raise ValueError("Original package reference changed")
        revised = deepcopy(case["after_snapshot"] if case else activity)
        for field in ("comment", "source", "description"):
            if isinstance(revised.get(field), str):
                for replacement in plan["metadata_replacements"]:
                    revised[field] = revised[field].replace(
                        replacement["before"], replacement["after"]
                    )
        if case:
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
        raise ValueError("Unapproved package, metadata or parent data changed")
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
        "changed_wafer_amount_cells": plan["amount_cells"],
        "changed_metadata_cells": plan["comment_cells"],
        "all_non_wafer_quantities_unchanged": True,
        "all_row_and_cell_addresses_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
