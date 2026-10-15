"""Attach constant-range capacity metadata to the installed 2020 electric buses.

Run from the repository root with ``python scripts/update_bus_battery_sizing.py``.
Only metadata rows are changed; all inventory exchanges remain untouched.
Nominal capacity, lifetime and occupancy are checked against the existing
inventory. Rounded comments are not used to infer physical pack mass or wear.
"""

import json
import math
import re
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

from bw2io import ExcelImporter
from lxml import etree
from openpyxl import load_workbook

from premise.vehicle_battery import SPEC_KEY, electricity_per_service

PATH = (
    Path(__file__).resolve().parents[1]
    / "premise/data/additional_inventories/lci-buses.xlsx"
)


def annotate(path=PATH):
    datasets = ExcelImporter(path).data
    by_name = {d["name"]: d for d in datasets}
    specifications = {}
    for service in datasets:
        if not service["name"].startswith("transport, passenger bus, battery electric"):
            continue
        vehicles = [
            e
            for e in service["exchanges"]
            if e["type"] == "technosphere"
            and e["unit"] == "unit"
            and e["name"].startswith("Passenger bus,")
        ]
        if len(vehicles) != 1:
            raise ValueError(f"Expected one bus supplier: {service['name']}")
        vehicle = by_name[vehicles[0]["name"]]
        purchases = [
            e
            for e in vehicle["exchanges"]
            if e["type"] == "technosphere"
            and e.get("reference product") == "electricity storage capacity"
            and e["unit"] == "kilowatt hour"
        ]
        if len(purchases) != 1:
            raise ValueError(f"Expected one capacity purchase: {vehicle['name']}")
        comment = service["comment"]
        capacity = float(
            re.search(r"Battery capacity: ([\d.]+) \[kWh\]", vehicle["comment"])[1]
        )
        lifetime = float(re.search(r"Km over lifetime: ([\d.]+) \[km\]", comment)[1])
        passengers = float(
            re.search(r"Number of passengers onboard: ([\d.]+) \[unit\]", comment)[1]
        )
        # These inventories purchase one pack. Fail if a future refresh changes
        # this policy rather than infer replacements from rounded comments.
        if not math.isclose(capacity, purchases[0]["amount"], rel_tol=1e-9):
            raise ValueError(
                f"Nominal and purchased capacity differ: {vehicle['name']}"
            )
        if not math.isclose(
            vehicles[0]["amount"], 1 / (lifetime * passengers), rel_tol=1e-9
        ):
            raise ValueError(f"Bus lifetime/occupancy disagree: {service['name']}")
        spec = {
            "version": 1,
            "baseline year": 2020,
            "family": "bus",
            "powertrain": "BEV",
            "sizing scope": "capacity only",
            "capacity kWh": purchases[0]["amount"],
            "replacements": 0,
            "grid kWh per service": electricity_per_service(service),
            "lifetime km": lifetime,
            "passengers": passengers,
            "source": "Installed 2020 lci-buses inventory: exact capacity purchase "
            "and operating-electricity exchanges; documented lifetime and occupancy. "
            "Retain range, passenger occupancy and the source replacement policy. "
            "Density and chemistry are applied by the capacity market; no physical "
            "bus mass or wear adjustment is inferred from rounded specifications.",
        }
        for dataset in (service, vehicle):
            specifications[dataset["name"]] = json.dumps(spec, sort_keys=True)
    if len(specifications) != 20:
        raise ValueError("Expected ten electric bus services and ten producers")
    workbook = load_workbook(path, read_only=True)
    if len(workbook.worksheets) != 1:
        raise ValueError("Expected the single-sheet bus inventory")
    edits = {}
    for sheet in workbook:
        name = None
        metadata_row = None
        for row in sheet:
            if row[0].value == "Activity":
                name = row[1].value
                metadata_row = None
            elif row[0].value == SPEC_KEY:
                metadata_row = row[0].row
            elif row[0].value == "special remark" and not row[1].value:
                metadata_row = row[0].row
            elif row[0].value == "Exchanges" and name in specifications:
                if metadata_row is None:
                    raise ValueError(f"No empty metadata row available: {name}")
                edits[metadata_row] = specifications[name]
    if len(edits) != len(specifications):
        raise ValueError("Could not locate all bus metadata blocks")
    workbook.close()
    # Reuse the empty special-remark field and patch only its two XML cells.
    # This preserves every numerical literal, formula and cached result exactly,
    # avoiding floating-point rounding from a whole-workbook serialization.
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    output = BytesIO()
    with ZipFile(path) as source, ZipFile(output, "w") as target:
        for entry in source.infolist():
            payload = source.read(entry.filename)
            if entry.filename == "xl/worksheets/sheet1.xml":
                root = etree.fromstring(payload)
                for row in root.find(f"{ns}sheetData"):
                    index = int(row.get("r"))
                    if index not in edits:
                        continue
                    for column, value in (("A", SPEC_KEY), ("B", edits[index])):
                        address = f"{column}{index}"
                        cell = next((c for c in row if c.get("r") == address), None)
                        if cell is None:
                            cell = etree.SubElement(row, f"{ns}c", r=address)
                        for child in list(cell):
                            cell.remove(child)
                        cell.set("t", "inlineStr")
                        text = etree.SubElement(
                            etree.SubElement(cell, f"{ns}is"), f"{ns}t"
                        )
                        text.text = value
                payload = etree.tostring(root, xml_declaration=True, encoding="UTF-8")
            target.writestr(entry, payload)
    path.write_bytes(output.getvalue())
    print(f"Updated {len(edits)} bus sizing metadata records in {path}")


if __name__ == "__main__":
    annotate()
