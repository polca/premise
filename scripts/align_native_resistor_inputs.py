"""Align three reviewed resistor purchases with native whole-component supply."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/native-resistor-inputs-adoption-20261027.json"
PLAN_SHA = "9e08f34a5413c7e19483bb448c3cfe1e1b9e396089dde88d2cd93f7cc92b732a"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED NATIVE WHOLE-RESISTOR MANUFACTURING PROXY, 2026-10-27. Use "
    "the existing GLO market for surface-mounted resistors for this reviewed "
    "purchase, at unchanged total mass. The 128-part bundle retains "
    "111 nonzero resistors (0.0004893 kg) and 17 zero-ohm jumpers "
    "(0.00004665 kg) as separate functional/count provenance; their combined "
    "native purchase is 0.00053595 kg per bundle. These counts apply only to "
    "that bundle, not the central-board or standalone interface allowances. "
    "Central supply remains 0.006122448979591837 kg/kg board (3 g per "
    "490 g reference); standalone interface supply remains 0.005 kg/kg. "
    "This generic whole-component manufacturing proxy does not certify exact "
    "Vishay/Yageo chemistry, jumper electrical equivalence or complete Ru/Pd "
    "coverage. The former custom film, lead-glass and Ru manufacturing "
    "assumptions are not adopted for these purchases. Native supplier "
    "uncertainty remains its own; no old joint statistical equivalence is "
    "claimed. Other components, parent purchases, board masses and existing "
    "retirement ownership remain unchanged; no additional EOL or recovery "
    "credit is added. Original technosphere and biosphere source declarations "
    "both stay 3.9. This qualification supersedes the old custom resistor/"
    "jumper manufacturing descriptions in the historical notes below.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved native-resistor reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Resistor aggregation requires formula and cache review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        for case in plan["cases"]:
            for col, text in {
                "A": plan["native_identity"][0],
                "G": plan["native_identity"][1],
                "I": plan["native_identity"][1],
            }.items():
                address = f"{col}{case['exchange_row']}"
                cell = root.find(f".//{Q}c[@r='{address}']")
                node = cell.find(f"{Q}is/{Q}t") if cell is not None else None
                if node is None:
                    raise ValueError("Original resistor identity cell moved")
                node.text = text
            cell = root.find(f".//{Q}c[@r='{case['comment_cell']}']")
            node = cell.find(f"{Q}is/{Q}t") if cell is not None else None
            if node is None or node.text != case["before_snapshot"]["comment"]:
                raise ValueError("Original resistor metadata changed")
            node.text = NOTE + node.text
        cell = root.find(f".//{Q}c[@r='{plan['amount_cell']}']")
        if cell is None or cell.find(Q + "f") is not None:
            raise ValueError("Original bundle purchase moved or became a formula")
        cell.find(Q + "v").text = str(plan["bundle_mass_kg"])
        row = root.find(f".//{Q}row[@r='{plan['removed_row']}']")
        if row is None:
            raise ValueError("Original jumper row moved")
        # This is the bundle's last input; the next activity retains its
        # original address and no surviving exchange follows a blank gap.
        row.getparent().remove(row)
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
    cases = {case["before_snapshot"]["name"]: case for case in plan["cases"]}
    observed = []
    for a, b in zip(old, new, strict=True):
        case = cases.get(a["name"])
        if case is None:
            expected = a
        else:
            if a != case["before_snapshot"]:
                raise ValueError("Original resistor consumer changed")
            observed.append(a["name"])
            expected = deepcopy(case["after_snapshot"])
            expected["comment"] = NOTE + a["comment"]
        if expected != b:
            raise ValueError("Unapproved component or downstream data changed")
    if len(observed) != 3:
        raise ValueError("Reviewed resistor consumers are not unique")
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
        "changed_identity_cells": plan["identity_cells"],
        "changed_amount_cell": plan["amount_cell"],
        "changed_comment_cells": plan["comment_cells"],
        "removed_final_jumper_row": plan["removed_row"],
        "other_row_addresses_unchanged": True,
        "changed_activities": observed,
        "other_imported_activities_unchanged": len(new) - len(observed),
        "activity_count": len(new),
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
        "existing_unused_helpers_preserved": True,
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
