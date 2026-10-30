"""Historical component views compare scientific content, not file identity."""

import json
from io import BytesIO
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, ZipFile

import pytest
from delegated_source_history import (
    NS,
    PROTECTED_RECORDS,
    REFERENCE,
    SHEET,
    _records,
    _reviewed_predecessor,
    pre_delegation_workbook,
)
from inventory_assertions import scientific_record

from premise.filesystem_constants import INVENTORY_DIR

FILENAMES = tuple(PROTECTED_RECORDS)


def replace_zip_part(payload, name, replacement):
    output = BytesIO()
    with ZipFile(BytesIO(payload)) as source, ZipFile(output, "w") as target:
        for item in source.infolist():
            target.writestr(
                item,
                replacement if item.filename == name else source.read(item.filename),
            )
    return output.getvalue()


@pytest.mark.parametrize("filename", FILENAMES)
def test_shared_component_view_matches_live_supplier_quantities_and_uncertainty(
    filename,
):
    payload = (INVENTORY_DIR / filename).read_bytes()
    predecessor = _reviewed_predecessor(payload, filename)
    actual = {r["name"]: r for r in _records(payload)}
    expected = {r["name"]: r for r in _records(predecessor)}
    for name in PROTECTED_RECORDS[filename]:
        assert scientific_record(actual[name]) == scientific_record(expected[name])


@pytest.mark.parametrize("filename", FILENAMES)
def test_packaging_changes_do_not_invalidate_component_science(filename):
    payload = (INVENTORY_DIR / filename).read_bytes()
    with ZipFile(BytesIO(payload)) as archive:
        properties = archive.read("docProps/core.xml")
    changed = replace_zip_part(
        payload, "docProps/core.xml", properties.replace(b"</", b"\n</")
    )
    assert changed != payload
    assert _reviewed_predecessor(changed, filename) == _reviewed_predecessor(
        payload, filename
    )


@pytest.mark.parametrize("filename", FILENAMES)
def test_changed_protected_exchange_is_rejected(filename):
    payload = (INVENTORY_DIR / filename).read_bytes()
    protected = PROTECTED_RECORDS[filename][0]
    # Find the actual exchange amount by activity and column headings, rather
    # than depending on a worksheet row that shifts when records are inserted.
    with ZipFile(BytesIO(payload)) as archive:
        root = ET.fromstring(archive.read(SHEET))
        strings = (
            [
                "".join(item.itertext())
                for item in ET.fromstring(archive.read("xl/sharedStrings.xml"))
            ]
            if "xl/sharedStrings.xml" in archive.namelist()
            else []
        )
    from inventory_assertions import worksheet_value

    selected = False
    changed = False
    for row in root.find(NS + "sheetData"):
        address = "A" + row.get("r")
        if row.find(f"{NS}c[@r='{address}']") is None:
            continue
        value = worksheet_value(root, address, strings)
        if value == "Activity":
            selected = worksheet_value(root, "B" + row.get("r"), strings) == protected
        elif selected:
            amount = row.find(f"{NS}c[@r='B{row.get('r')}']")
            if (
                amount is not None
                and amount.find(NS + "v") is not None
                and amount.get("t") is None
            ):
                amount.find(NS + "v").text = "12345"
                formula = amount.find(NS + "f")
                if formula is not None:
                    amount.remove(formula)
                changed = True
                break
    assert changed, protected
    altered = replace_zip_part(payload, SHEET, ET.tostring(root))
    with pytest.raises(AssertionError, match="Shared component recipe changed"):
        _reviewed_predecessor(altered, filename)


@pytest.mark.parametrize(
    "filename", ("lci-batteries-ASSB.xlsx", "lci-batteries-NMC955-LTO.xlsx")
)
def test_bounded_reversal_restores_declared_cell_values(filename):
    payload = (INVENTORY_DIR / filename).read_bytes()
    restored = pre_delegation_workbook(payload, filename)
    plan = json.loads(REFERENCE.read_text())[filename]
    with ZipFile(BytesIO(restored)) as archive:
        root = ET.fromstring(archive.read(SHEET))
    for address, change in plan["cells"].items():
        actual = root.find(f".//{NS}c[@r='{address}']")
        expected = ET.fromstring(change["before_xml"])
        assert actual.attrib == expected.attrib, address
        assert list(actual.itertext()) == list(expected.itertext()), address


@pytest.mark.parametrize("filename", FILENAMES)
def test_malformed_workbook_is_rejected(filename):
    with pytest.raises(BadZipFile):
        _reviewed_predecessor(b"not an Excel workbook", filename)
