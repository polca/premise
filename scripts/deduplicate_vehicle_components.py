"""Apply the reviewed October 2026 shared-component cleanup without Brightway."""

import argparse
import hashlib
import json
import posixpath
import zipfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from xml.dom import minidom
from xml.etree import ElementTree

import openpyxl

AUDIT = Path("dev/data/vehicle_inventory_audit")
PLAN = AUDIT / "component-reuse-plan.json"
MANIFEST = AUDIT / "shared-component-reuse.json"
IGNORED_EXCHANGE_FIELDS = {"comment", "input", "output", "database"}
SHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def formula_cache_signature(path):
    entries = []
    with zipfile.ZipFile(path) as archive:
        for name in sorted(archive.namelist()):
            if not name.startswith("xl/worksheets/") or not name.endswith(".xml"):
                continue
            root = ElementTree.fromstring(archive.read(name))
            for cell in root.iter(f"{{{SHEET_NS}}}c"):
                formula = cell.find(f"{{{SHEET_NS}}}f")
                if formula is not None:
                    cached = cell.find(f"{{{SHEET_NS}}}v")
                    entries.append(
                        (
                            name,
                            cell.get("r"),
                            ElementTree.tostring(formula).decode(),
                            (
                                ElementTree.tostring(cached).decode()
                                if cached is not None
                                else None
                            ),
                        )
                    )
    return {
        "count": len(entries),
        "sha256": hashlib.sha256(json.dumps(entries).encode()).hexdigest(),
    }


def patch_workbook(path, updates, removals):
    replacement_parts = {}
    with zipfile.ZipFile(path) as archive:
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        relationships = ElementTree.fromstring(
            archive.read("xl/_rels/workbook.xml.rels")
        )
        targets = {node.get("Id"): node.get("Target") for node in relationships}
        for sheet in workbook.iter(f"{{{SHEET_NS}}}sheet"):
            title = sheet.get("name")
            if title not in updates and title not in removals:
                continue
            relationship = sheet.get(
                "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
            )
            target = targets[relationship]
            part = (
                target.lstrip("/")
                if target.startswith("/")
                else posixpath.normpath(posixpath.join("xl", target))
            )
            document = minidom.parseString(archive.read(part))
            sheet_data = document.getElementsByTagNameNS(SHEET_NS, "sheetData")[0]
            rows = {
                int(row.getAttribute("r")): row
                for row in sheet_data.childNodes
                if row.nodeType == row.ELEMENT_NODE and row.localName == "row"
            }
            for row_number, columns in updates.get(title, {}).items():
                row = rows[row_number]
                cells = {
                    cell.getAttribute("r"): cell
                    for cell in row.childNodes
                    if cell.nodeType == cell.ELEMENT_NODE and cell.localName == "c"
                }
                for column, value in columns.items():
                    address = f"{openpyxl.utils.get_column_letter(column)}{row_number}"
                    cell = cells.get(address)
                    if cell is None:
                        cell = document.createElementNS(SHEET_NS, "c")
                        cell.setAttribute("r", address)
                        following = next(
                            (
                                child
                                for child in row.childNodes
                                if child.nodeType == child.ELEMENT_NODE
                                and child.localName == "c"
                                and openpyxl.utils.cell.coordinate_from_string(
                                    child.getAttribute("r")
                                )[0]
                                and openpyxl.utils.cell.column_index_from_string(
                                    openpyxl.utils.cell.coordinate_from_string(
                                        child.getAttribute("r")
                                    )[0]
                                )
                                > column
                            ),
                            None,
                        )
                        (
                            row.insertBefore(cell, following)
                            if following
                            else row.appendChild(cell)
                        )
                    if cell.getElementsByTagNameNS(SHEET_NS, "f"):
                        raise ValueError(
                            f"Refusing to replace a formula: {path.name}:{address}"
                        )
                    for child in list(cell.childNodes):
                        cell.removeChild(child)
                    if cell.hasAttribute("t"):
                        cell.removeAttribute("t")
                    if value is not None:
                        cell.setAttribute("t", "inlineStr")
                        inline = document.createElementNS(SHEET_NS, "is")
                        text = document.createElementNS(SHEET_NS, "t")
                        text.setAttribute("xml:space", "preserve")
                        text.appendChild(document.createTextNode(str(value)))
                        inline.appendChild(text)
                        cell.appendChild(inline)
            for first, last in removals.get(title, []):
                for row_number in range(first, last + 1):
                    if row_number in rows:
                        sheet_data.removeChild(rows[row_number])
            replacement_parts[part] = document.toxml(encoding="utf-8")
            document.unlink()
        temporary = path.with_suffix(".cleanup.xlsx")
        try:
            with zipfile.ZipFile(temporary, "w") as output:
                for entry in archive.infolist():
                    output.writestr(
                        entry,
                        replacement_parts.get(
                            entry.filename, archive.read(entry.filename)
                        ),
                    )
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    temporary.replace(path)


@dataclass
class Block:
    filename: str
    sheet: str
    start: int
    end: int
    database: str
    metadata: dict
    metadata_rows: list
    columns: dict
    exchanges: list


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(record):
    return (
        str(record.get("name", "")).casefold(),
        str(
            record.get(
                "reference product",
                record.get("reference_product", record.get("product", "")),
            )
        ).casefold(),
        record.get("location"),
        record.get("unit"),
    )


def read_blocks(path):
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=False)
    blocks = []
    try:
        for sheet in workbook:
            rows = list(sheet.iter_rows(values_only=True))
            if not rows or str(rows[0][0]).casefold() == "skip":
                continue
            starts = []
            database = ""
            for position, row in enumerate(rows):
                marker = str(row[0] or "").casefold()
                if marker == "database":
                    database = row[1]
                elif marker == "activity" and isinstance(row[1], str):
                    starts.append((position, database))
            for offset, (start, database) in enumerate(starts):
                end = starts[offset + 1][0] if offset + 1 < len(starts) else len(rows)
                exchange_marker = next(
                    position
                    for position in range(start, end)
                    if str(rows[position][0] or "").casefold() == "exchanges"
                )
                header = next(
                    position
                    for position in range(exchange_marker + 1, end)
                    if any(value is not None for value in rows[position])
                )
                columns = {
                    str(value): column + 1
                    for column, value in enumerate(rows[header])
                    if value
                }
                if not {"name", "amount", "type"} <= columns.keys():
                    raise ValueError(
                        f"Invalid exchange header: {path.name}:{header + 1}"
                    )
                metadata_rows = [
                    list(row)
                    for row in rows[start:exchange_marker]
                    if any(value is not None for value in row)
                ]
                metadata = {"name": rows[start][1]}
                metadata.update(
                    {
                        row[0]: row[1]
                        for row in rows[start + 1 : exchange_marker]
                        if row[0]
                    }
                )
                exchanges = []
                for position in range(header + 1, end):
                    values = {
                        key: rows[position][column - 1]
                        for key, column in columns.items()
                        if rows[position][column - 1] is not None
                    }
                    if values.get("type") in {
                        "production",
                        "technosphere",
                        "biosphere",
                        "substitution",
                    }:
                        exchanges.append((position + 1, values))
                blocks.append(
                    Block(
                        path.name,
                        sheet.title,
                        start + 1,
                        end,
                        database,
                        metadata,
                        metadata_rows,
                        columns,
                        exchanges,
                    )
                )
    finally:
        workbook.close()
    return blocks


def recipe_hash(block, aliases):
    exchanges = []
    for _, exchange in block.exchanges:
        values = {
            key: value
            for key, value in exchange.items()
            if key not in IGNORED_EXCHANGE_FIELDS
        }
        alias = (
            aliases.get(identity(exchange))
            if exchange.get("type") == "technosphere"
            else None
        )
        if alias:
            canonical = alias["canonical"]
            values["name"] = canonical["name"]
            values["reference product"] = canonical["reference_product"]
            if "product" in values:
                values["product"] = canonical["reference_product"]
        exchanges.append(values)
    payload = {"metadata": block.metadata_rows, "exchanges": exchanges}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def recipe_index(blocks, aliases, exclude_aliases=False):
    return {
        (block.filename, block.sheet, block.metadata["name"]): recipe_hash(
            block, aliases
        )
        for block in blocks
        if not exclude_aliases or identity(block.metadata) not in aliases
    }


def apply_cleanup(repository, apply=False):
    plan = json.loads((repository / PLAN).read_text())
    inventory = repository / "premise/data/additional_inventories"
    paths = sorted(inventory.glob("*.xlsx"))
    blocks = [block for path in paths for block in read_blocks(path)]
    aliases = {identity(alias["source"]): alias for alias in plan["aliases"]}
    alias_names = {key[0] for key in aliases}
    retired = [block for block in blocks if identity(block.metadata) in aliases]
    existing_manifest = repository / MANIFEST
    if not retired and existing_manifest.exists():
        manifest = json.loads(existing_manifest.read_text())
        for filename, expected in manifest["after_workbook_sha256"].items():
            if file_hash(inventory / filename) != expected:
                raise ValueError(f"Post-cleanup workbook changed: {filename}")
        return {
            "status": "already_applied",
            "removed_activities": 0,
            "relinked_exchanges": 0,
        }
    if len(retired) != plan["expected_removed_activity_count"]:
        raise ValueError(
            f"Expected 28 reviewed source definitions, found {len(retired)}"
        )
    for filename, expected in plan["source_workbook_sha256"].items():
        if file_hash(inventory / filename) != expected:
            raise ValueError(f"Source changed since review: {filename}")
    canonical_blocks = {}
    for alias in plan["aliases"]:
        candidates = [
            block
            for block in blocks
            if identity(block.metadata) == identity(alias["canonical"])
        ]
        if len(candidates) != 1 or candidates[0].filename != alias["canonical_file"]:
            raise ValueError(
                f"Canonical supplier is not unique: {alias['canonical']['name']}"
            )
        canonical_blocks[identity(alias["canonical"])] = candidates[0]
    before = recipe_index(blocks, aliases, exclude_aliases=True)
    retired_keys = {(block.filename, block.sheet, block.start) for block in retired}
    changes = []
    for block in blocks:
        if (block.filename, block.sheet, block.start) in retired_keys:
            continue
        for row, exchange in block.exchanges:
            if exchange.get("type") != "technosphere":
                continue
            alias = aliases.get(identity(exchange))
            if exchange.get("name", "").casefold() in alias_names and not alias:
                raise ValueError(f"Unreviewed alias identity: {block.filename}:{row}")
            if alias:
                changes.append((block, row, exchange, alias))
    if len(changes) != plan["expected_relinked_retained_exchange_count"]:
        raise ValueError(f"Unexpected surviving consumer count: {len(changes)}")
    report = {
        "status": "ready",
        "removed_activities": len(retired),
        "relinked_exchanges": len(changes),
    }
    if not apply:
        return report
    changed_files = sorted(
        {block.filename for block in retired}
        | {block.filename for block, _, _, _ in changes}
    )
    formula_signatures = {
        filename: formula_cache_signature(inventory / filename)
        for filename in changed_files
    }
    for filename in changed_files:
        updates = {}
        removals = {}
        for block, row, exchange, alias in changes:
            if block.filename != filename:
                continue
            canonical = alias["canonical"]
            provider = canonical_blocks[identity(canonical)]
            values = {
                "name": canonical["name"],
                "reference product": canonical["reference_product"],
            }
            if "product" in block.columns:
                values["product"] = canonical["reference_product"]
            if "input" in block.columns:
                values["input"] = None
            if "database" in block.columns:
                values["database"] = provider.database
            if "comment" not in block.columns:
                raise ValueError(f"Cannot retain alias provenance: {filename}:{row}")
            note = f"Shared component reuse (2026-10-16): original model supplier '{exchange['name']}' uses the existing canonical recipe in {provider.filename}; its quantities and uncertainty are retained."
            previous = exchange.get("comment")
            values["comment"] = f"{previous}\n{note}" if previous else note
            updates.setdefault(block.sheet, {})[row] = {
                block.columns[key]: value for key, value in values.items()
            }
        for block in retired:
            if block.filename == filename:
                removals.setdefault(block.sheet, []).append((block.start, block.end))
        patch_workbook(inventory / filename, updates, removals)
        if (
            formula_cache_signature(inventory / filename)
            != formula_signatures[filename]
        ):
            raise ValueError(f"Formula caches changed: {filename}")
    after_blocks = [block for path in paths for block in read_blocks(path)]
    if recipe_index(after_blocks, aliases) != before:
        raise ValueError(
            "Cleanup changed a retained recipe, uncertainty or activity metadata"
        )
    for block in after_blocks:
        if block.metadata["name"].casefold() in alias_names:
            raise ValueError("A retired component definition remains")
        if any(
            exchange.get("name", "").casefold() in alias_names
            for _, exchange in block.exchanges
        ):
            raise ValueError("A consumer still references a retired model component")
    manifest = {
        "format": "premise-shared-component-reuse-v1",
        "date": plan["date"],
        "source_commit": plan["source_commit"],
        "policy": plan["canonical_policy"],
        "before_workbook_sha256": plan["source_workbook_sha256"],
        "after_workbook_sha256": {path.name: file_hash(path) for path in paths},
        "changed_workbooks": changed_files,
        "preserved_formula_caches": formula_signatures,
        "removed_activity_count": len(retired),
        "relinked_retained_exchange_count": len(changes),
        "relinked_exchanges_by_workbook": dict(
            Counter(block.filename for block, _, _, _ in changes)
        ),
        "retained_activity_count": len(after_blocks),
        "retained_recipe_preservation": "All retained activity metadata and ordered exchange fields are unchanged, except the reviewed supplier aliases, link namespaces and traceability comments. Amounts, uncertainty, formulas and parameters are unchanged.",
        "canonical_recipes": [
            {
                "file": block.filename,
                "identity": {
                    key: block.metadata[key]
                    for key in ("name", "reference product", "location", "unit")
                },
                "recipe_sha256": recipe_hash(block, aliases),
            }
            for block in canonical_blocks.values()
        ],
        "retired_components": [
            {
                "file": block.filename,
                "sheet": block.sheet,
                "source_row": block.start,
                "original_metadata": block.metadata,
                "original_recipe_sha256": recipe_hash(block, {}),
                "canonical": aliases[identity(block.metadata)]["canonical"],
                "canonical_file": aliases[identity(block.metadata)]["canonical_file"],
            }
            for block in retired
        ],
    }
    for stem in ("lci-pass_cars", "lci-trucks"):
        path = repository / AUDIT / f"{stem}.provenance.json"
        sidecar = json.loads(path.read_text())
        removed_names = [
            block.metadata["name"]
            for block in retired
            if block.filename == f"{stem}.xlsx"
        ]
        remaining = sum(block.filename == f"{stem}.xlsx" for block in after_blocks)
        if "auxiliaries" in sidecar:
            sidecar["auxiliaries"] = []
        if "auxiliary_count" in sidecar:
            sidecar["auxiliary_count"] = (
                remaining - sidecar["transport_count"] - sidecar["production_count"]
            )
        sidecar["shared component reuse"] = {
            "date": plan["date"],
            "manifest": MANIFEST.name,
            "removed model-specific definitions": removed_names,
            "retained activity count": remaining,
            "boundary": "Original generation provenance is unchanged. Canonical shared suppliers retain their own original sources, background profiles and uncertainty; model-specific metadata is preserved in the manifest and consumer provenance.",
        }
        path.write_text(json.dumps(sidecar, indent=2, ensure_ascii=False) + "\n")
    existing_manifest.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, default=str) + "\n"
    )
    return report | {
        "status": "applied",
        "changed_workbooks": changed_files,
        "retained_activities": len(after_blocks),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--apply", action="store_true")
    options = parser.parse_args()
    print(json.dumps(apply_cleanup(options.repository, apply=options.apply), indent=2))


if __name__ == "__main__":
    main()
