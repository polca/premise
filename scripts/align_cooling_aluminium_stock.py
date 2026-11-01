"""Correct radiator/manifold aluminium feed without changing quantities."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/cooling-aluminium-stock-adoption-20261027.json"
PLAN_SHA = "0c3ec5d72d5bfd0bab0d867c6f1a19abee7fa7098d7aaa4641df0ecfb3588966"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved cooling aluminium-stock reference changed")
    return json.loads(payload)


def revised_workbook(payload, plan):
    if sha256(payload).hexdigest() != plan["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != plan["dimension"]:
            raise ValueError("Source worksheet extent changed")
        strings = etree.fromstring(source.read("xl/sharedStrings.xml"))

        def replace_text(address, before, after):
            cell = root.find(f".//{NS}c[@r='{address}']")
            if cell is None:
                raise ValueError("Approved cell missing")
            if cell.get("t") == "s":
                value = cell.find(NS + "v")
                if (
                    value is None
                    or "".join(strings[int(value.text)].itertext()) != before
                ):
                    raise ValueError("Original shared text changed")
                cell.remove(value)
                cell.set("t", "inlineStr")
                etree.SubElement(etree.SubElement(cell, NS + "is"), NS + "t").text = (
                    after
                )
            elif cell.get("t") == "inlineStr":
                text = cell.find(f"{NS}is/{NS}t")
                if text is None or text.text != before:
                    raise ValueError("Original inline text changed")
                text.text = after
            else:
                raise ValueError("Unsupported text storage")

        for change in plan["changes"]:
            replace_text(change["cell"], change["before"], change["after"])
        for note in plan["notes"]:
            replace_text(note["cell"], note["before"], note["prefix"] + note["before"])
        result = BytesIO()
        with ZipFile(result, "w") as target:
            target.comment = source.comment
            for info in source.infolist():
                target.writestr(
                    info,
                    (
                        etree.tostring(root)
                        if info.filename == SHEET
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

    old, new = ExcelImporter(before).data, ExcelImporter(staged).data
    expected = deepcopy(old)
    for item in plan["affected_activities"]:
        if old[item["index"]] != item["before"]:
            raise ValueError("Original scientific record changed")
        expected[item["index"]] = item["after"]
    if len(old) != plan["activity_count"] or new != expected:
        raise ValueError("Unapproved workbook data changed")
    if path.read_bytes() != original:
        raise ValueError("Concurrent source change")
    if args.apply:
        temp = path.with_suffix(".xlsx.tmp")
        temp.write_bytes(updated)
        temp.replace(path)
    receipt = {
        "case_id": "ALIGN-049",
        "applied": args.apply,
        "before_sha256": sha256(original).hexdigest(),
        "after_sha256": sha256(updated).hexdigest(),
        "activities": len(new),
        "affected_activities": 2,
        "unaffected_activities": len(new) - 2,
        "supplier_identity_cells": [x["cell"] for x in plan["changes"]],
        "note_cells": [x["cell"] for x in plan["notes"]],
        "source_background": plan["source_background"],
        "source_biosphere": plan["source_biosphere"],
        "CLIC_revision_candidate_ids": [
            x["dataset_id"] for x in plan["candidate_revisions"]
        ],
        "CLIC_database_or_published_versions_modified": False,
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
