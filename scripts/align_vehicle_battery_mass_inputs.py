"""Apply the audited 77-car/truck battery proxy migration without database access.

Run with --artifacts <dir>; the approved adoption plan is stored in this repo.
--plan optionally selects an exact pinned copy of the adoption or original audit.
The default prepares and verifies staged workbooks; --apply atomically replaces
both sources after all preflights pass. --check verifies already migrated inputs.
Only the pinned source workbooks and approved CLIC battery quantities qualify.
"""

import argparse
from bisect import bisect_right
from collections import Counter
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import tempfile
from zipfile import ZipFile

from lxml import etree
import openpyxl

SPEC_KEY = "battery mass to capacity"
NOTE = (
    "AUTHORITATIVE STATIC BATTERY / PREMISE RUNTIME BOUNDARY, 2026-10-27. "
    "This additional inventory purchases one native NMC622 complete-pack mass "
    "proxy and one explicit matching used-battery retirement at the approved "
    "CLIC quantities. The proxy purchase is a lifetime manufacturing budget, "
    "not the physical installed pack mass or an NMC622 density calculation. "
    "The battery mass to capacity JSON preserves the exact original lifetime "
    "kWh purchase and its CURRENT capacity supplier, including dedicated "
    "high-power FCEV suppliers. Premise transport reactivates that capacity "
    "purchase before regional sizing and removes only this paired retirement, "
    "because the capacity supplier already includes end of life. Do not apply "
    "replacements or scenario density again in the baseline conversion. "
    "Physical vehicle battery sizing and source provenance remain unchanged. "
    "Earlier comments about active MIX purchases or removed pack/disposal "
    "exchanges describe historical inputs; this note and the JSON contract "
    "define the current static/runtime boundary."
)
MASS_NOTE = (
    "Static single NMC622 complete-pack manufacturing proxy at the approved "
    "CLIC lifetime mass; physical pack sizing remains separate. The root "
    "battery mass to capacity contract restores the original lifetime kWh "
    "and current supplier in Premise. Historical capacity exchange comment: "
)
RETIREMENT_NOTE = (
    "Explicit retirement paired only with the static NMC622 purchase. Exact "
    "approved CLIC negative kg amount; the used-battery market produces -1 kg. "
    "Premise removes this flow once when restoring capacity supply, whose "
    "boundary already includes end of life. Replacements are already included."
)
PLAN_SHA256 = "6f8ae172999419faefa4a5227e111ab90228036b785821f6c210823b660898fb"
AUDIT_PLAN_SHA256 = "2aaa8fcce919853bea28f5fd4169682f03e1199c33c1189b670ba71b1cc40166"
DEFAULT_PLAN = (
    Path(__file__).resolve().parents[1]
    / "scripts/data/vehicle-battery-mass-adoption-20261027.json"
)
SOURCES = {
    "lci-pass_cars.xlsx": (
        49,
        "bff6125713093e62bd460f0829ec16733e1ee9dac325280edc5f87b58fa8bd10",
    ),
    "lci-trucks.xlsx": (
        28,
        "e4531b32808c97f16c6ff1d15706cc9478b4433d26555eee19929dd614f4342e",
    ),
}
NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
Q = "{" + NS + "}"
PART = "xl/worksheets/sheet1.xml"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(payload):
    return sha256(payload).hexdigest()


def load_plan(path):
    payload = Path(path).read_bytes()
    require(
        digest(payload) in {PLAN_SHA256, AUDIT_PLAN_SHA256},
        "Approved adoption plan checksum differs",
    )
    content = json.loads(payload)
    plan = content["rows"] if isinstance(content, dict) else content
    require(len(plan) == 77, "Expected exactly 77 approved vehicle rows")
    require(len({r["dataset_id"] for r in plan}) == 77, "Duplicate CLIC dataset")
    require(
        Counter(r["workbook"] for r in plan) == {n: c for n, (c, _) in SOURCES.items()},
        "Unexpected workbook scope",
    )
    return plan


def value(cell):
    if cell is None:
        return None
    if cell.get("t") == "inlineStr":
        return "".join(cell.find(Q + "is").itertext())
    raw = cell.find(Q + "v")
    if raw is None:
        return None
    require(
        cell.get("t") not in {"s", "e"},
        "Unexpected shared string/error in pinned sources",
    )
    return raw.text if cell.get("t") == "str" else float(raw.text)


def cells(row):
    return {openpyxl.utils.cell.coordinate_from_string(c.get("r"))[0]: c for c in row}


def set_cell(row, column, content):
    address = column + row.get("r")
    existing = cells(row).get(column)
    if existing is None:
        existing = etree.Element(Q + "c", r=address)
        index = openpyxl.utils.column_index_from_string(column)
        following = next(
            (
                i
                for i, c in enumerate(row)
                if openpyxl.utils.column_index_from_string(
                    openpyxl.utils.cell.coordinate_from_string(c.get("r"))[0]
                )
                > index
            ),
            len(row),
        )
        row.insert(following, existing)
    require(existing.find(Q + "f") is None, f"Refusing to replace formula {address}")
    for child in list(existing):
        existing.remove(child)
    if isinstance(content, str):
        existing.set("t", "inlineStr")
        t = etree.SubElement(etree.SubElement(existing, Q + "is"), Q + "t")
        t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        t.text = content
    else:
        existing.attrib.pop("t", None)
        if content is not None:
            etree.SubElement(existing, Q + "v").text = repr(content)


def rewrite_row_number(row, number):
    row.set("r", str(number))
    for cell in row:
        column = openpyxl.utils.cell.coordinate_from_string(cell.get("r"))[0]
        cell.set("r", column + str(number))


def normalized_row(row, ignored=()):
    """Compare complete original cell payloads, allowing only address shifts."""
    result = deepcopy(row)
    rewrite_row_number(result, 1)
    for c in list(result):
        column = openpyxl.utils.cell.coordinate_from_string(c.get("r"))[0]
        if column in ignored:
            result.remove(c)

    def structure(node):
        return (
            node.tag,
            tuple(sorted(node.attrib.items())),
            node.text,
            node.tail,
            tuple(structure(c) for c in node),
        )

    return structure(result)


def migrate_workbook(payload, filename, records):
    require(filename in SOURCES, "Unapproved workbook")
    count, expected_hash = SOURCES[filename]
    require(digest(payload) == expected_hash, f"Source checksum differs: {filename}")
    require(len(records) == count, f"Incorrect target count: {filename}")
    changes, insertions = {}, {}
    with ZipFile(BytesIO(payload)) as source:
        root = etree.fromstring(source.read(PART))
        require(
            not root.findall(".//" + Q + "f"),
            "Row insertion needs a formula-reference review",
        )
        require(
            not any(
                root.find(Q + k) is not None
                for k in (
                    "mergeCells",
                    "autoFilter",
                    "hyperlinks",
                    "dataValidations",
                    "conditionalFormatting",
                    "tableParts",
                )
            ),
            "Row insertion needs a worksheet-reference review",
        )
        workbook = etree.fromstring(source.read("xl/workbook.xml"))
        require(
            len(workbook.find(Q + "sheets")) == 1
            and not list(workbook.find(Q + "definedNames")),
            "Unexpected sheet/defined-name references",
        )
        require(
            "xl/calcChain.xml" not in source.namelist(), "Unexpected calculation chain"
        )
        sheet_data = root.find(Q + "sheetData")
        originals = list(sheet_data)
        by_row = {int(row.get("r")): row for row in originals}
        receipts = []
        for record in records:
            start = record["activity_row"]
            purchase_row = record["capacity_row"]
            require(
                value(cells(by_row[start]).get("B")) == record["identity"]["name"],
                "Activity identity changed",
            )
            exchange_marker = next(
                n
                for n in sorted(by_row)
                if n > start and value(cells(by_row[n]).get("A")) == "Exchanges"
            )
            headers = {
                value(c): col for col, c in cells(by_row[exchange_marker + 1]).items()
            }
            require(
                set(record["original_capacity_exchange"]) <= headers.keys(),
                "Exchange columns changed",
            )
            old_row = by_row[purchase_row]
            old_cells = cells(old_row)
            for k, v in record["original_capacity_exchange"].items():
                require(
                    value(old_cells.get(headers[k])) == v,
                    f"Original capacity changed: {record['dataset_id']}:{k}",
                )
            comment_row = next(
                n
                for n in range(start + 1, exchange_marker)
                if value(cells(by_row[n]).get("A")) == "comment"
            )
            require(
                all(
                    value(cells(by_row[n]).get("A")) != SPEC_KEY
                    for n in range(start + 1, exchange_marker)
                ),
                "Contract already exists",
            )
            original_comment = value(cells(by_row[comment_row]).get("B"))
            changes[comment_row] = {"B": original_comment + "\n\n" + NOTE}
            contract = record["contract"]
            meta = etree.Element(Q + "row", r=str(exchange_marker))
            set_cell(meta, "A", SPEC_KEY)
            set_cell(meta, "B", json.dumps(contract, sort_keys=True, allow_nan=False))
            require(exchange_marker not in insertions, "Duplicate metadata insertion")
            insertions[exchange_marker] = meta
            mass = record["static_mass_purchase"]
            mass_values = {headers[k]: v for k, v in mass.items()}
            mass_values[headers["comment"]] = MASS_NOTE + record[
                "original_capacity_exchange"
            ].get("comment", "")
            changes[purchase_row] = mass_values
            retirement = deepcopy(old_row)
            for cell in retirement:
                for child in list(cell):
                    cell.remove(child)
                cell.attrib.pop("t", None)
            for k, v in {
                **record["static_retirement"],
                "comment": RETIREMENT_NOTE,
            }.items():
                set_cell(retirement, headers[k], v)
            require(
                purchase_row + 1 not in insertions, "Duplicate retirement insertion"
            )
            insertions[purchase_row + 1] = retirement
            receipts.append(
                {
                    "dataset_id": record["dataset_id"],
                    "identity": record["identity"],
                    "contract": contract,
                    "old_activity_row": start,
                    "old_capacity_row": purchase_row,
                    "old_metadata_insertion_row": exchange_marker,
                }
            )
        shifts = sorted(insertions)
        sheet_data.clear(keep_tail=True)
        row_map = {}
        for number in sorted(set(by_row) | set(insertions)):
            if number in insertions:
                added = insertions[number]
                rewrite_row_number(added, number + bisect_right(shifts, number) - 1)
                sheet_data.append(added)
            if number in by_row:
                row = deepcopy(by_row[number])
                for col, content in changes.get(number, {}).items():
                    set_cell(row, col, content)
                shifted = number + bisect_right(shifts, number)
                rewrite_row_number(row, shifted)
                sheet_data.append(row)
                row_map[number] = shifted
        dimension = root.find(Q + "dimension")
        require(dimension is not None, "Missing worksheet dimension")
        first, last = dimension.get("ref").split(":")
        col, number = openpyxl.utils.cell.coordinate_from_string(last)
        dimension.set("ref", f"{first}:{col}{number + len(shifts)}")
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
    migrated = result.getvalue()
    # Verify every pre-existing row/cell, including empty/style-only cells.
    with ZipFile(BytesIO(migrated)) as after, ZipFile(BytesIO(payload)) as before:
        require(
            after.namelist() == before.namelist(), "ZIP part membership/order changed"
        )
        for name in before.namelist():
            if name != PART:
                require(
                    before.read(name) == after.read(name),
                    f"Unrelated ZIP part changed: {name}",
                )
        new_rows = {
            int(r.get("r")): r
            for r in etree.fromstring(after.read(PART)).find(Q + "sheetData")
        }
        for number, old in by_row.items():
            require(
                normalized_row(old, changes.get(number, ()))
                == normalized_row(new_rows[row_map[number]], changes.get(number, ())),
                f"Unrelated cell payload changed at original row {number}",
            )
            for col, content in changes.get(number, {}).items():
                require(
                    value(cells(new_rows[row_map[number]]).get(col)) == content,
                    "Target cell did not roundtrip",
                )
        require(len(new_rows) == len(by_row) + 2 * count, "Unexpected inserted rows")
    for receipt in receipts:
        receipt["new_activity_row"] = row_map[receipt["old_activity_row"]]
        receipt["new_mass_row"] = row_map[receipt["old_capacity_row"]]
    return migrated, {
        "workbook": filename,
        "before_sha256": digest(payload),
        "after_sha256": digest(migrated),
        "targets": receipts,
        "unchanged_original_row_count": len(by_row) - len(changes),
        "all_other_original_cell_payloads_preserved": True,
        "unrelated_zip_parts_unchanged": True,
        "inserted_rows": 2 * count,
    }


def imported(path):
    # ExcelImporter reads files only; no project selection or database writing.
    from bw2io import ExcelImporter

    return ExcelImporter(str(path)).data


def validate_import(before, after, records):
    previous = {d["name"]: d for d in before}
    current = {d["name"]: d for d in after}
    require(previous.keys() == current.keys(), "Activity membership/order changed")
    require(
        [d["name"] for d in before] == [d["name"] for d in after],
        "Activity order changed",
    )
    targets = {r["identity"]["name"]: r for r in records}
    for name, old in previous.items():
        new = current[name]
        if name not in targets:
            require(old == new, f"Untargeted imported activity changed: {name}")
            continue
        r = targets[name]
        require(
            {
                k: v
                for k, v in new.items()
                if k not in {SPEC_KEY, "comment", "exchanges"}
            }
            == {k: v for k, v in old.items() if k not in {"comment", "exchanges"}},
            f"Original metadata changed: {name}",
        )
        require(
            new["comment"].endswith("\n\n" + NOTE)
            and new["comment"].removesuffix("\n\n" + NOTE).rstrip() == old["comment"],
            "Comment history changed",
        )
        require(json.loads(new[SPEC_KEY]) == r["contract"], "Imported contract changed")
        old_purchase = r["original_capacity_exchange"]
        index = next(i for i, e in enumerate(old["exchanges"]) if e == old_purchase)
        require(
            new["exchanges"][:index] == old["exchanges"][:index]
            and new["exchanges"][index + 2 :] == old["exchanges"][index + 1 :],
            f"Unrelated exchanges changed: {name}",
        )
        require(
            new["exchanges"][index]
            == {
                **r["static_mass_purchase"],
                "comment": MASS_NOTE + old_purchase.get("comment", ""),
            },
            "Static mass differs",
        )
        require(
            new["exchanges"][index + 1]
            == {**r["static_retirement"], "comment": RETIREMENT_NOTE},
            "Paired retirement differs",
        )
    return {
        "activities": len(after),
        "targets": len(records),
        "all_untargeted_activities_exchanges_and_metadata_unchanged": True,
    }


def atomic_write(path, payload):
    path = Path(path)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        if path.exists():
            temporary.chmod(path.stat().st_mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def run(repository, plan_path, artifacts, apply=False, check=False):
    plan = load_plan(plan_path)
    artifacts.mkdir(parents=True, exist_ok=True)
    inventories = repository / "premise/data/additional_inventories"
    if check:
        for filename, (count, _) in SOURCES.items():
            data = imported(inventories / filename)
            targets = {
                r["identity"]["name"]: r for r in plan if r["workbook"] == filename
            }
            marked = [d for d in data if SPEC_KEY in d]
            require(len(marked) == count, "Converted inventory count differs")
            for d in marked:
                require(
                    json.loads(d[SPEC_KEY]) == targets[d["name"]]["contract"],
                    "Converted contract differs",
                )
        return {"already_migrated": 77, "check_passed": True}
    prepared, receipts = [], []
    for filename in SOURCES:
        path = inventories / filename
        original = path.read_bytes()
        records = [r for r in plan if r["workbook"] == filename]
        output, receipt = migrate_workbook(original, filename, records)
        for folder in ("before", "staged"):
            (artifacts / folder).mkdir(exist_ok=True)
        saved = artifacts / "before" / filename
        require(
            not saved.exists() or saved.read_bytes() == original,
            "Pre-migration backup differs",
        )
        atomic_write(saved, original)
        staged = artifacts / "staged" / filename
        atomic_write(staged, output)
        old_data, new_data = imported(saved), imported(staged)
        atomic_write(
            artifacts / "before" / (filename + ".import.json"),
            (json.dumps(old_data, indent=2, default=str) + "\n").encode(),
        )
        receipt["import_verification"] = validate_import(old_data, new_data, records)
        prepared.append((path, original, output))
        receipts.append(receipt)
    # Recheck both source pins after all staging/import checks and before writing.
    for path, original, _ in prepared:
        require(path.read_bytes() == original, "Source changed during preparation")
    if apply:
        replaced = []
        try:
            for path, original, output in prepared:
                atomic_write(path, output)
                replaced.append((path, original))
        except BaseException:
            for path, original in reversed(replaced):
                atomic_write(path, original)
            raise
    report = {"applied": apply, "target_count": 77, "workbooks": receipts}
    atomic_write(
        artifacts / "migration-receipt.json",
        (json.dumps(report, indent=2) + "\n").encode(),
    )
    return {
        "applied": apply,
        "verified_targets": 77,
        "receipt": str(artifacts / "migration-receipt.json"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    require(not (args.apply and args.check), "Choose apply or check")
    print(
        json.dumps(
            run(args.repository, args.plan, args.artifacts, args.apply, args.check),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
