"""Apply the approved detailed RER cooling reference and nominal retirement."""

import argparse
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/sixteen-cooling-adoption-20261027.json"
PLAN_SHA = "90c333ab0ef10b3e1de5a6c48670f202affd1f7672a3017f64abbc86f0dd1ab3"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved sixteen-cooling reference changed")
    return json.loads(payload)


def revised_workbook(payload, group):
    if sha256(payload).hexdigest() != group["before_workbook_sha256"]:
        raise ValueError("Source workbook changed")
    if group["formula_reference_audit"]["dependent_formula_cells"]:
        raise ValueError("Dependent formula caches require review")
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(SHEET))
        if root.find(NS + "dimension").get("ref") != group["dimension"]:
            raise ValueError("Source worksheet extent changed")
        strings = (
            etree.fromstring(source.read("xl/sharedStrings.xml"))
            if "xl/sharedStrings.xml" in source.namelist()
            else []
        )
        for change in group["changes"]:
            cell = root.find(f".//{NS}c[@r='{change['cell']}']")
            if cell is None or cell.find(NS + "f") is not None:
                raise ValueError("Approved nonformula cell missing or changed")
            value = cell.find(NS + "v")
            if change["kind"] == "numeric":
                if value is None or float(value.text) != change["before"]:
                    raise ValueError("Original amount changed")
                value.text = str(change["after"])
            else:
                if cell.get("t") == "inlineStr":
                    text = cell.find(f"{NS}is/{NS}t")
                    if text is None or text.text != change["before"]:
                        raise ValueError("Original supplier identity changed")
                    text.text = change["after"]
                elif cell.get("t") == "s":
                    if (
                        value is None
                        or "".join(strings[int(value.text)].itertext())
                        != change["before"]
                    ):
                        raise ValueError("Original shared supplier identity changed")
                    cell.remove(value)
                    cell.set("t", "inlineStr")
                    etree.SubElement(
                        etree.SubElement(cell, NS + "is"), NS + "t"
                    ).text = change["after"]
                else:
                    raise ValueError("Unsupported supplier identity storage")
        for note in group["notes"]:
            cell = root.find(f".//{NS}c[@r='{note['cell']}']")
            if cell is None:
                raise ValueError("Approved note cell missing")
            if cell.get("t") == "inlineStr":
                text = cell.find(f"{NS}is/{NS}t")
                if text is None or text.text != note["before"]:
                    raise ValueError("Original activity note changed")
                text.text = note["prefix"] + text.text
            elif cell.get("t") == "s":
                index = cell.find(NS + "v")
                if (
                    index is None
                    or "".join(strings[int(index.text)].itertext()) != note["before"]
                ):
                    raise ValueError("Original shared note changed")
                cell.remove(index)
                cell.set("t", "inlineStr")
                etree.SubElement(etree.SubElement(cell, NS + "is"), NS + "t").text = (
                    note["prefix"] + note["before"]
                )
            else:
                raise ValueError("Unsupported activity note storage")
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
    args.artifacts.mkdir(parents=True, exist_ok=False)
    from bw2io import ExcelImporter

    candidates = []
    for group in plan["workbooks"]:
        path = ROOT / "premise/data/additional_inventories" / group["workbook"]
        original = path.read_bytes()
        updated = revised_workbook(original, group)
        folder = args.artifacts / path.stem
        folder.mkdir()
        before, staged = folder / "before.xlsx", folder / "staged.xlsx"
        before.write_bytes(original)
        staged.write_bytes(updated)
        old, new = ExcelImporter(before).data, ExcelImporter(staged).data
        if len(old) != group["activity_count"]:
            raise ValueError("Original activity count changed")
        expected = deepcopy(old)
        for item in group["affected_activities"]:
            if old[item["index"]] != item["before"]:
                raise ValueError("Original affected scientific record changed")
            expected[item["index"]] = item["after"]
        if new != expected:
            raise ValueError("Unapproved workbook data changed")
        candidates.append((path, original, updated, group))
    # Validate every candidate before replacing any source; recheck all inputs.
    if any(path.read_bytes() != original for path, original, _, _ in candidates):
        raise ValueError("Concurrent source change")
    replaced = []
    try:
        if args.apply:
            for path, original, updated, group in candidates:
                temp = path.with_suffix(".xlsx.tmp")
                temp.write_bytes(updated)
                temp.replace(path)
                replaced.append((path, original, updated))
    except Exception:
        # Roll back only files still equal to this operation's own output.
        for path, original, updated in reversed(replaced):
            if path.read_bytes() == updated:
                temp = path.with_suffix(".xlsx.tmp")
                temp.write_bytes(original)
                temp.replace(path)
        raise
    receipt = {
        "case_id": "ALIGN-050",
        "applied": args.apply,
        "all_candidates_import_validated_before_source_replacement": True,
        "changed_cells": 6,
        "note_cells": 3,
        "workbooks": [
            {
                "workbook": path.name,
                "before_sha256": sha256(original).hexdigest(),
                "after_sha256": sha256(updated).hexdigest(),
                "activities": group["activity_count"],
                "affected_activities": len(group["affected_activities"]),
                "source_background": group["source_background"],
                "source_biosphere": group["source_biosphere"],
            }
            for path, original, updated, group in candidates
        ],
    }
    (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
