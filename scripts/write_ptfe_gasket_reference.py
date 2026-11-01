"""Write the approved 3.12-only CLIC resin/forming reference without a database.

The legacy electrolysis workbook is deliberately not rewritten: its original
gasket remains the source for older targets. Source selection is implemented
by premise.ptfe_gasket and NewDatabase's default-inventory import hook.
"""

import argparse
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from lxml import etree
from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/data/ptfe-gasket-native-312-adoption-20261027.json"
PLAN_SHA = "c071165edb5c9da195f4aa6d221882bd7f8a57598b87c0bbae7f0d8886369434"
Q = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
PART = "xl/worksheets/sheet1.xml"


def reference():
    payload = PLAN.read_bytes()
    if sha256(payload).hexdigest() != PLAN_SHA:
        raise ValueError("Approved PTFE adoption reference changed")
    return json.loads(payload)


def workbook_bytes(plan):
    snapshot = plan["snapshot"]
    book = Workbook()
    sheet = book.active
    sheet.title = "lci"
    sheet.append(["Database", "premise native PTFE gasket foreground"])
    sheet.append([])
    sheet.append(["Activity", snapshot["name"]])
    for key, value in {
        **{
            k: v
            for k, v in snapshot.items()
            if k not in {"name", "exchanges", "database"}
        },
        "production amount": 1,
        "activity type": "ordinary transforming activity",
        "background version": "3.12",
        "biosphere version": "3.12",
        "license": plan["license"],
        "compiled by": plan["compiler"],
        "CLIC reference": {
            "dataset": plan["CLIC_dataset_id"],
            "version": plan["CLIC_version_id"],
            "snapshot sha256": plan["CLIC_snapshot_sha256"],
        },
    }.items():
        sheet.append(
            [key, json.dumps(value) if isinstance(value, (dict, list)) else value]
        )
    sheet.append(["Exchanges"])
    columns = [
        "name",
        "amount",
        "location",
        "unit",
        "type",
        "reference product",
        "product",
        "comment",
    ]
    sheet.append(columns)
    numeric = {}
    for exchange in snapshot["exchanges"]:
        sheet.append([exchange.get(key) for key in columns])
        numeric[f"B{sheet.max_row}"] = exchange["amount"]
    sheet.append([])
    raw = BytesIO()
    book.save(raw)
    book.close()
    result = BytesIO()
    with ZipFile(BytesIO(raw.getvalue())) as source, ZipFile(result, "w") as target:
        root = etree.fromstring(source.read(PART))
        for address, amount in numeric.items():
            root.find(f".//{Q}c[@r='{address}']/{Q}v").text = repr(amount)
        for entry in source.infolist():
            target.writestr(
                entry,
                (
                    etree.tostring(root)
                    if entry.filename == PART
                    else source.read(entry.filename)
                ),
            )
    return result.getvalue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = reference()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        raise ValueError("Refusing to overwrite an existing workbook")
    payload = workbook_bytes(plan)
    args.output.write_bytes(payload)
    try:
        from bw2io import ExcelImporter

        (imported,) = ExcelImporter(str(args.output)).data
        if imported["exchanges"] != plan["snapshot"]["exchanges"]:
            raise ValueError("PTFE scientific exchanges failed exact Excel roundtrip")
    except BaseException:
        args.output.unlink()
        raise
    print(
        json.dumps(
            {
                "output": str(args.output),
                "sha256": sha256(payload).hexdigest(),
                "exact_approved_exchanges": len(imported["exchanges"]),
            }
        )
    )


if __name__ == "__main__":
    main()
