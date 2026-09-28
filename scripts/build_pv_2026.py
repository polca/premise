"""Rebuild numeric PV country adapters and the legacy CIGS supplement.

Run from the repository root. The reviewed lci-PV-2026.xlsx core and the
versioned solar parameter files are the inputs; no Brightway project is opened.
"""

import csv
import importlib.util
import json
from pathlib import Path

import openpyxl
from openpyxl.styles import Font

ROOT = Path(__file__).resolve().parents[1]
INVENTORIES = ROOT / "premise/data/additional_inventories"
spec = importlib.util.spec_from_file_location(
    "photovoltaic", ROOT / "premise/photovoltaic.py"
)
pv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pv)
FIELDS = [
    "name",
    "amount",
    "location",
    "unit",
    "categories",
    "type",
    "uncertainty type",
    "loc",
    "scale",
    "negative",
    "reference product",
    "comment",
    "production volume",
    "minimum",
    "maximum",
]


def read_workbook(path):
    data = []
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    for sheet in workbook:
        rows = iter(sheet.values)
        first = next(rows, ())
        if first and str(first[0]).lower() == "skip":
            continue
        activity, fields = None, None
        for row in (first, *rows):
            if not row or row[0] is None:
                continue
            label = str(row[0]).lower()
            if label == "activity":
                activity = {"name": row[1], "exchanges": []}
                data.append(activity)
                fields = None
            elif activity is not None:
                if label == "exchanges":
                    continue
                if label == "name":
                    fields = row
                elif fields:
                    activity["exchanges"].append(
                        {k: v for k, v in zip(fields, row) if k and v is not None}
                    )
                elif len(row) > 1 and row[1] is not None:
                    activity[label] = row[1]
    workbook.close()
    return data


def write_workbook(path, data):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Inventory"
    sheet.append(["Database", path.stem])
    sheet.append([])
    for activity in data:
        sheet.append(["Activity", activity["name"]])
        for key, value in activity.items():
            if key not in ("name", "exchanges", "code", "database"):
                sheet.append([key, value])
        sheet.append(["Exchanges"])
        sheet.append(FIELDS)
        for exchange in activity["exchanges"]:
            row = [exchange.get(k) for k in FIELDS]
            if isinstance(row[4], (tuple, list)):
                row[4] = "::".join(row[4])
            sheet.append(row)
        sheet.append([])
    for row in sheet:
        if row[0].value in ("Activity", "Exchanges", "name"):
            for cell in row:
                cell.font = Font(bold=True)
        for cell in row:
            if isinstance(cell.value, str) and cell.value.startswith("="):
                cell.data_type = "s"
    sheet.column_dimensions["A"].width = 85
    sheet.column_dimensions["B"].width = 24
    sheet.column_dimensions["L"].width = 80
    workbook.save(path)


def main():
    core_path = INVENTORIES / "lci-PV-2026.xlsx"
    core = read_workbook(core_path)
    for activity in core:
        activity.update(pv.installation_metadata(activity))
        if activity["location"] == "REF":
            activity["pv reference location"] = "REF"
            activity["location"] = "GLO"
        for exchange in activity["exchanges"]:
            if exchange.get("location") == "REF":
                exchange["location"] = "GLO"
    parameters = json.loads(
        (ROOT / "premise/data/solar/pv_2026_parameters.json").read_text()
    )
    with (ROOT / "premise/data/solar/pv_generation_2023.csv").open() as stream:
        generation = {
            row["location"]: float(row["generation_gwh"])
            for row in csv.DictReader(stream)
        }
    adapters = pv.country_adapters(core, parameters, generation)
    legacy = read_workbook(INVENTORIES / "lci-PV.xlsx")
    # One backwards-compatible functional unit used by the electrolysis workbook.
    old_name = "electricity production, photovoltaic, at 280 kWp flat-roof, single-Si"
    old = next(d for d in legacy if d["name"] == old_name)
    target = next(
        d
        for d in core
        if d.get("pv reference location") == "REF"
        and "250 kwp flat-roof, single-crystalline" in d["name"]
        and "annual yield 1000 " in d["name"]
    )
    adapter = {k: old[k] for k in ("name", "reference product", "location", "unit")}
    adapter.update(
        type="process",
        source=parameters["source"],
        **{
            "comment": "Compatibility supplier for the legacy electrolysis PV electricity input. One kWh is supplied by the 250 kWp single-crystalline silicon flat-roof reference system at 1000 kWh/kWp/year. The old 280 kWp label is retained for linking only; the electricity functional unit is not scaled by plant capacity.",
            "exchanges": [
                dict(adapter, type="production", amount=1),
                {
                    **{
                        k: target[k]
                        for k in ("name", "reference product", "location", "unit")
                    },
                    "type": "technosphere",
                    "amount": 1,
                },
            ],
        },
    )
    adapters.append(adapter)
    write_workbook(core_path, core)
    write_workbook(INVENTORIES / "lci-PV-2026-electricity.xlsx", adapters)
    # Retain distinct CIS/CIGS manufacturing and all foreground dependencies.
    lookup = {(d["name"], d["location"], d["unit"]): d for d in legacy}
    pending = [d for d in legacy if "CIS" in d["name"] and d["unit"] == "square meter"]
    supplement = {}
    while pending:
        activity = pending.pop()
        key = (activity["name"], activity["location"], activity["unit"])
        if key in supplement:
            continue
        supplement[key] = activity
        for exchange in activity["exchanges"]:
            supplier = lookup.get(
                (exchange["name"], exchange.get("location"), exchange["unit"])
            )
            if exchange["type"] == "technosphere" and supplier:
                exchange["reference product"] = supplier["reference product"]
                pending.append(supplier)
        activity.setdefault(
            "source",
            "IEA PVPS Task 12 legacy CIS/CIGS inventory, retained from premise lci-PV.xlsx; https://iea-pvps.org/key-topics/life-cycle-inventories-and-life-cycle-assessments-of-photovoltaic-systems/",
        )
    write_workbook(INVENTORIES / "lci-PV-CIGS.xlsx", list(supplement.values()))
    print(
        f"Core: {len(core)}; adapters: {len(adapters)}; CIGS supplement: {len(supplement)}"
    )


if __name__ == "__main__":
    main()
