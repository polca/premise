"""Readable, scoped assertions for inventory records and worksheet edits."""

from copy import deepcopy
from xml.etree import ElementTree as ET
from zipfile import ZipFile

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def records_match_review(records, review, phase="before"):
    """Compare the reviewed activities, allowing unrelated records to evolve."""
    for activity in review["affected_activities"]:
        expected = activity[phase]
        matches = [record for record in records if record["name"] == expected["name"]]
        if matches != [expected]:
            return False
    return True


def worksheet_value(root, address, shared_strings=()):
    cell = root.find(f".//{NS}c[@r='{address}']")
    assert cell is not None, f"Missing worksheet cell {address}"
    kind = cell.get("t")
    if kind == "inlineStr":
        return "".join(cell.find(NS + "is").itertext())
    value = cell.findtext(NS + "v")
    if kind == "s":
        return shared_strings[int(value)]
    if kind in ("str", "e"):
        return value
    if value is None:
        return None
    return float(value)


def assert_review_cells(root, review, phase, workbook):
    """Check declared values, formulas and notes, without freezing Excel XML."""
    with ZipFile(workbook) as archive:
        strings = (
            [
                "".join(item.itertext())
                for item in ET.fromstring(archive.read("xl/sharedStrings.xml"))
            ]
            if "xl/sharedStrings.xml" in archive.namelist()
            else []
        )
    for change in review["changes"]:
        address = change["cell"]
        expected = change[phase]
        observed = worksheet_value(root, address, strings)
        assert observed == expected, (address, observed, expected)
        cell = root.find(f".//{NS}c[@r='{address}']")
        formula_key = f"{phase}_formula"
        if formula_key in change:
            assert cell.findtext(NS + "f") == change[formula_key], address
    for note in review.get("notes", []):
        expected = note["before"]
        if phase == "after":
            expected = note["prefix"] + expected
        assert worksheet_value(root, note["cell"], strings) == expected, note["cell"]


def review_cells_match(root, review, phase, workbook):
    try:
        assert_review_cells(root, review, phase, workbook)
    except AssertionError:
        return False
    return True


def scientific_record(record):
    """Retain supplier identities, quantities, uncertainty and reference output."""
    result = {
        key: deepcopy(record[key])
        for key in (
            "name",
            "reference product",
            "location",
            "unit",
            "production amount",
        )
        if key in record
    }
    result["exchanges"] = [
        {
            key: deepcopy(value)
            for key, value in exchange.items()
            if key.lower() not in ("comment", "source", "database", "input", "code")
            and not (key == "product" and value == exchange.get("reference product"))
        }
        for exchange in record["exchanges"]
    ]
    return result


def formula_caches(root):
    """Return explicit formula/cache pairs for diagnostic equality failures."""
    result = {}
    for cell in root.iter(NS + "c"):
        formula = cell.findtext(NS + "f")
        if formula is not None:
            value = cell.findtext(NS + "v")
            assert value is not None, f"Missing cached value for {cell.get('r')}"
            result[cell.get("r")] = (formula, value)
    return result


def assert_same_cells(actual, expected, addresses):
    """Compare just the selected formulas and payloads; Excel styles may vary."""

    def content(root, address):
        cell = root.find(f".//{NS}c[@r='{address}']")
        if cell is None:
            return None
        inline = cell.find(NS + "is")
        return (
            cell.findtext(NS + "f"),
            cell.findtext(NS + "v"),
            tuple(inline.itertext()) if inline is not None else None,
        )

    for address in addresses:
        assert content(actual, address) == content(expected, address), address


def recipe_quantities(record):
    """Compare physical recipe vectors across raw and prepared importers."""
    rows = []
    for exchange in record["exchanges"]:
        categories = exchange.get("categories") or ()
        if isinstance(categories, str):
            categories = categories.split("::")
        rows.append(
            (
                exchange["type"],
                exchange["name"],
                exchange.get("reference product", exchange.get("product")),
                exchange.get("location"),
                exchange["unit"],
                tuple(categories),
                exchange["amount"],
            )
        )
    return sorted(rows, key=repr)


def inventory_cell_address(workbook, activity, field, supplier=None):
    """Find a unique metadata/exchange cell without assuming an Excel row."""
    from openpyxl import load_workbook

    matches = []
    source = load_workbook(workbook, read_only=True, data_only=True)
    try:
        for sheet in source:
            selected = False
            exchanges = False
            columns = None
            for row in sheet.iter_rows():
                values = [cell.value for cell in row]
                if not any(value is not None for value in values):
                    continue
                if values[0] == "Activity":
                    selected = values[1] == activity
                    exchanges, columns = False, None
                    continue
                if not selected:
                    continue
                if values[0] == "Exchanges":
                    exchanges = True
                    continue
                if supplier is None and not exchanges and values[0] == field:
                    matches.append(row[1].coordinate)
                elif supplier is not None and exchanges:
                    if columns is None:
                        columns = {value: i for i, value in enumerate(values) if value}
                    elif values[columns["name"]] == supplier:
                        matches.append(row[columns[field]].coordinate)
    finally:
        source.close()
    assert len(matches) == 1, (activity, field, supplier, matches)
    return matches[0]


def assert_declared_classifications(importer, dataset):
    """Classifications are populated only when the import registry declares them."""
    from premise.inventory_imports import get_classification_entry

    expected = get_classification_entry(
        importer.classifications, dataset["name"], dataset["reference product"]
    )
    if expected:
        assert dict(dataset["classifications"]) == expected
