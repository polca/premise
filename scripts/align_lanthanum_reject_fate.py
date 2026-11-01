"""Remove the unsupported La-reject treatment while retaining material losses."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/lanthanum-reject-fate-adoption-20261027.json"
PLAN_SHA = "82787d1bade021e556f95b46c15cc8a3e5469b8401fca5c7a8fb555f92541a14"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"
NOTE = (
    "APPROVED UNASSIGNED LANTHANUM-REJECT FATE, 2026-10-27. Remove only the "
    "unsupported inert-disposal purchase, aligned with the CLIC Crofer "
    "reference. The 95% La retention and the nominal 0.05263157894736836 kg "
    "La-equivalent loss per kg reference output are retained in the unchanged "
    "precursor budget. Reject composition, physical destination and treatment "
    "burdens remain unassigned; this is not zero physical loss, zero treatment "
    "impact or an environmental improvement. Graphite, reaction CO2, 10 kWh "
    "electricity and 3 MJ heat per kg La are unchanged. The Crofer purchase "
    "stays 0.001 kg La/kg alloy; alloy and native working quantities and "
    "uncertainty stay unchanged. Both source declarations remain 3.9. This "
    "qualification supersedes older inert-disposal notes retained below.\n\n"
)


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved La-reject reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if plan["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Exchange-row relocation needs a formula review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        data = root.find(Q + "sheetData")
        waste = data.find(f"{Q}row[@r='871']")
        carbon = data.find(f"{Q}row[@r='872']")
        if waste is None or carbon is None:
            raise ValueError("Original exchange rows moved")
        moved = deepcopy(carbon)
        moved.set("r", "871")
        for cell in moved:
            cell.set("r", cell.get("r").rstrip("0123456789") + "871")
        index = list(data).index(waste)
        data.remove(waste)
        data.remove(carbon)
        # Keep the final biosphere exchange contiguous with the remaining
        # inputs: a blank row would stop Excel extraction before its CO2.
        data.insert(index, moved)
        comment = root.find(f".//{Q}c[@r='B862']")
        if comment is None or comment.find(Q + "f") is not None:
            raise ValueError("Original source comment moved")
        for child in list(comment):
            comment.remove(child)
        comment.set("t", "inlineStr")
        node = etree.SubElement(etree.SubElement(comment, Q + "is"), Q + "t")
        node.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        node.text = NOTE + plan["before_snapshot"]["comment"]
        replacement = plan["legacy_comment_replacement"]
        for address in replacement["cells"]:
            cell = root.find(f".//{Q}c[@r='{address}']")
            if cell is None or cell.get("t") != "inlineStr":
                raise ValueError("Legacy comment moved")
            node = cell.find(f"{Q}is/{Q}t")
            if node is None:
                raise ValueError("Legacy comment encoding changed")
            text = node.text
            if text.count(replacement["before"]) != 1:
                raise ValueError("Legacy comment no longer matches")
            node.text = text.replace(replacement["before"], replacement["after"])
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
            expected = deepcopy(a)
            replacement = plan["legacy_comment_replacement"]
            for record in [expected, *expected["exchanges"]]:
                if isinstance(record.get("comment"), str):
                    record["comment"] = record["comment"].replace(
                        replacement["before"], replacement["after"]
                    )
            if expected != b:
                raise ValueError("Another activity changed")
            continue
        if a != plan["before_snapshot"]:
            raise ValueError("Original La reference changed")
        expected = deepcopy(plan["after_snapshot"])
        expected["comment"] = NOTE + a["comment"]
        if expected != b:
            raise ValueError("Unapproved La data changed or reaction CO2 was lost")
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
        "source_comment_cell": "B862",
        "legacy_comment_replacement": plan["legacy_comment_replacement"],
        "removed_exchange_row": 871,
        "preserved_CO2_exchange_relocated_from": 872,
        "preserved_CO2_exchange_relocated_to": 871,
        "unchanged_other_row_addresses": True,
        "all_other_imported_scientific_data_unchanged": True,
        "source_background_version": "3.9",
        "source_biosphere_version": "3.9",
        "retained_yield": 0.95,
        "unassigned_La_equivalent_loss_kg_per_kg_La": plan[
            "unassigned_La_equivalent_loss_kg_per_kg_La"
        ],
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
