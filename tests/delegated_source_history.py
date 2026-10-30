"""Scoped historical views for component tests of independently evolving inventories."""

import json
from copy import deepcopy
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from inventory_assertions import scientific_record
from lxml import etree

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
REFERENCE = Path(__file__).parent / "data/delegated-source-history-20261027.json"
SUCCESSORS = Path(__file__).parent / "data/reviewed-source-successors-20261028.json"

# These historical tests cover shared controls and cooling. Cell manufacturing,
# lifetime and recycling recipes have separate live-inventory tests; their edits
# must not prevent the component fixtures from being constructed.
PROTECTED_RECORDS = {
    "lci-batteries-LiS.xlsx": (
        "printed circuit board production, battery module control, mounted",
        "printed circuit board production, battery central control, mounted",
        "battery central control and protection unit production",
        "battery control and cooling system production, 16 modules",
    ),
    "lci-batteries-NMC955-LTO.xlsx": tuple(
        "battery monitoring and central control system production, lithium-ion, "
        + chemistry
        for chemistry in (
            "nmc111",
            "nmc532",
            "nmc622",
            "nmc811",
            "nmc955",
            "nca",
            "lfp",
            "lto",
            "limn2o4",
        )
    ),
    "lci-batteries-ASSB.xlsx": tuple(
        f"battery pack production, all-solid-state battery, {chemistry}, without central controls"
        for chemistry in ("polymer", "oxidic", "sulfidic")
    ),
    "lci-batteries-LiO2.xlsx": (
        "battery cooling system production, lithium-oxygen",
        "cooling system production, for Li-O2 battery",
    ),
}


@lru_cache(maxsize=16)
def _records(payload):
    from bw2io import ExcelImporter
    from bw2io.importers.excel import valid_first_cell

    # Pure extraction avoids constructing a Brightway database. A temporary path
    # also works with bw2io releases whose extractor does not accept BytesIO.
    with NamedTemporaryFile(suffix=".xlsx") as file:
        file.write(payload)
        file.flush()
        importer = ExcelImporter.__new__(ExcelImporter)
        data = importer.extractor.extract(Path(file.name))
        data = [(name, rows) for name, rows in data if valid_first_cell(name, rows)]
        importer.db_name, importer.metadata = importer.get_database(data)
        return importer.process_activities(data)


def _assert_shared_components(payload, filename, predecessor):
    actual = _records(payload)
    expected = _records(predecessor)
    for name in PROTECTED_RECORDS[filename]:
        live = [record for record in actual if record["name"] == name]
        historical = [record for record in expected if record["name"] == name]
        assert (
            len(live) == len(historical) == 1
        ), f"Missing or duplicated shared component: {name}"
        assert scientific_record(live[0]) == scientific_record(
            historical[0]
        ), f"Shared component recipe changed: {filename}: {name}"


def _reviewed_predecessor(payload, filename):
    """Expose the earlier component view after comparing the live recipes."""
    entry = json.loads(SUCCESSORS.read_text())[filename]
    predecessor = (SUCCESSORS.parent / entry["predecessor_fixture"]).read_bytes()
    _assert_shared_components(payload, filename, predecessor)
    return predecessor


def reviewed_intermediate_path(filepath, stage="auxiliary-cooling-and-assembly"):
    path = Path(filepath)
    entry = json.loads(SUCCESSORS.read_text())[path.name]
    _reviewed_predecessor(path.read_bytes(), path.name)
    (view,) = [view for view in entry["intermediate_views"] if view["stage"] == stage]
    fixture = SUCCESSORS.parent / view["fixture"]
    _assert_shared_components(path.read_bytes(), path.name, fixture.read_bytes())
    return fixture


def _scientific_workbook_view(filepath):
    if isinstance(filepath, (str, Path)):
        path = Path(filepath)
        if path.name == "lci-batteries-LiO2.xlsx":
            _reviewed_predecessor(path.read_bytes(), path.name)
            entry = json.loads(SUCCESSORS.read_text())[path.name]
            return SUCCESSORS.parent / entry["predecessor_fixture"]
    return filepath


def reviewed_excel_importer(filepath):
    from bw2io import ExcelImporter

    return ExcelImporter(_scientific_workbook_view(filepath))


def reviewed_zipfile(filepath, *args, **kwargs):
    return ZipFile(_scientific_workbook_view(filepath), *args, **kwargs)


def previous_product_source(source, path):
    """Remove only reviewed redundant products before older snapshot guards."""
    entry = json.loads(SUCCESSORS.read_text())[path.name]
    _reviewed_predecessor(path.read_bytes(), path.name)
    source = deepcopy(source)
    for field in entry["added_product_fields"]:
        (record,) = [r for r in source.data if r["name"] == field["activity"]]
        exchange = record["exchanges"][field["exchange_index"]]
        if "product" in exchange:
            assert exchange["product"] == field["value"]
            assert exchange["product"] == exchange["reference product"]
            del exchange["product"]
    return source


def previous_product_xml(root, path, sheet=SHEET):
    """Remove exact reviewed added cells, preserving all other XML for guards."""
    entry = json.loads(SUCCESSORS.read_text())[path.name]
    _reviewed_predecessor(path.read_bytes(), path.name)
    root = deepcopy(root)
    cells = entry["added_xml_cells"]
    found = {address: root.find(f".//{NS}c[@r='{address}']") for address in cells}
    if all(cell is None for cell in found.values()):
        return root  # An already normalized view still authenticated the live file.
    assert all(cell is not None for cell in found.values())
    with ZipFile(path) as archive:
        current = ET.fromstring(archive.read(sheet))
    for address, cell in found.items():
        actual = current.find(f".//{NS}c[@r='{address}']")
        assert actual is not None
        assert ET.tostring(cell) == ET.tostring(actual)
        row_number = "".join(c for c in address if c.isdigit())
        row = root.find(f".//{NS}row[@r='{row_number}']")
        row.remove(cell)
    return root


def pre_delegation_workbook(payload, filename):
    """Restore the earlier pouch/passive view after checking shared recipes."""
    plan = json.loads(REFERENCE.read_text())[filename]
    payload = _reviewed_predecessor(payload, filename)
    with ZipFile(BytesIO(payload)) as archive:
        root = etree.fromstring(archive.read(SHEET))
        for address, change in plan["cells"].items():
            cell = root.find(f".//{NS}c[@r='{address}']")
            assert cell is not None and cell.find(NS + "f") is None
            if isinstance(change["after_value"], str):
                assert cell.get("t") == "inlineStr"
                assert "".join(cell.find(NS + "is").itertext()) == change["after_value"]
            else:
                assert float(cell.findtext(NS + "v")) == change["after_value"]
            before = etree.fromstring(change["before_xml"].encode())
            cell.attrib.clear()
            cell.attrib.update(before.attrib)
            for child in list(cell):
                cell.remove(child)
            for child in before:
                # Clone under the existing namespace map, not the ns0 map from
                # the independently serialized reference fragment.
                restored = etree.SubElement(cell, child.tag, child.attrib)
                restored.text = child.text
                for item in child:
                    restored.append(deepcopy(item))
        out = BytesIO()
        with ZipFile(out, "w") as target:
            target.comment = archive.comment
            for info in archive.infolist():
                target.writestr(
                    info,
                    (
                        etree.tostring(root)
                        if info.filename == SHEET
                        else archive.read(info.filename)
                    ),
                )
    result = out.getvalue()
    return result
