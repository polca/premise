"""Shared vehicle components resolve to one canonical bundled supplier."""

import json
from collections import Counter
from pathlib import Path

from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR


def _load(stem):
    return ExcelImporter(str(INVENTORY_DIR / f"{stem}.xlsx")).data


def test_refreshed_truck_components_use_canonical_names():
    trucks = _load("lci-trucks")
    names = {dataset["name"] for dataset in trucks}
    canonical = {
        "assembly operation, for lorry",
        "exhaust system, for lorry",
        "frame, blanks and saddle, for lorry",
        "fuel tank assembly, compressed natural gas, 200 bar",
        "gearbox, for lorry",
        "internal combustion engine, for lorry",
        "lead acid battery, for lorry",
        "other components, for hybrid electric lorry",
        "power electronics, for lorry",
        "retarder, for lorry",
        "suspension, for lorry",
        "tires and wheels, for lorry",
        "transmission, for lorry",
        "fuel tank, for diesel vehicle",
        "EV charger, level 3, plugin, 200 kW",
        "storage battery",
        "charger",
        "charging stand",
        "fuel tank, compressed hydrogen gas, 700bar, with HDPE liner",
        "other components, for electric lorry",
    }
    suffix = ", carculator regional truck 2020"

    assert canonical <= names
    assert not {f"{name}{suffix}" for name in canonical} & names


def test_shared_component_suppliers_are_unique_and_consumers_use_them():
    inventories = {
        stem: _load(stem)
        for stem in ("lci-trucks", "lci-buses", "lci-final-energy", "lci-ships")
    }
    suppliers = {
        "EV charger, level 3, plugin, 200 kW": "lci-trucks",
        "Electrical equipment": "lci-buses",
        "Transformers": "lci-buses",
    }

    for name, expected_workbook in suppliers.items():
        producers = [
            stem
            for stem, datasets in inventories.items()
            if any(dataset["name"] == name for dataset in datasets)
        ]
        assert producers == [expected_workbook]

    old_tank = "fuel tank assembly, compressed hydrogen gas, 700bar"
    for datasets in inventories.values():
        assert not any(
            exchange.get("name") == old_tank
            for dataset in datasets
            for exchange in dataset.get("exchanges", [])
        )

    buses = inventories["lci-buses"]
    (catenary,) = [dataset for dataset in buses if dataset["name"] == "Catenary system"]
    catenary_suppliers = {
        exchange["name"]
        for exchange in catenary["exchanges"]
        if exchange["type"] == "technosphere"
    }
    assert {"Electrical equipment", "Transformers"} <= catenary_suppliers


def test_all_bundled_hydrogen_tank_consumers_resolve():
    """Check cross-workbook closure, including rail, without an ecoinvent licence."""
    from collections import Counter

    datasets = [
        dataset
        for stem in (
            "lci-trucks",
            "lci-buses",
            "lci-pass_cars",
            "lci-rail-freight",
            "lci-final-energy",
            "lci-ships",
        )
        for dataset in _load(stem)
    ]

    def identity(record):
        return (
            record["name"].lower(),
            record["reference product"].lower(),
            record["location"],
            record["unit"],
        )

    suppliers = Counter(identity(dataset) for dataset in datasets)
    checked = []
    for dataset in datasets:
        for exchange in dataset["exchanges"]:
            if exchange["type"] != "technosphere":
                continue
            if (
                not exchange.get("reference product", "")
                .lower()
                .startswith(("hydrogen tank", "hydrogen fuel tank"))
            ):
                continue
            assert suppliers[identity(exchange)] == 1, (
                dataset["name"],
                identity(exchange),
                suppliers[identity(exchange)],
            )
            checked.append(dataset["name"])
    assert "production of locomotive, fuel cell, hydrogen" in checked
    assert "hydrogen, used in a fuel cell in a passenger train" in checked


def test_retired_aliases_are_absent_and_canonical_suppliers_are_unique():
    contract = json.loads(
        (Path(__file__).parent / "data/vehicle_component_suppliers.json").read_text()
    )
    fields = ("name", "reference product", "location", "unit")

    def identity(record):
        values = [
            record.get(k, record.get("product") if k == "reference product" else None)
            for k in fields
        ]
        return (str(values[0]).casefold(), str(values[1]).casefold(), *values[2:])

    retired = {identity(a["retired"]) for a in contract["aliases"]}
    canonical = {identity(a["canonical"]) for a in contract["aliases"]}
    records = [
        (path.name, r)
        for path in sorted(INVENTORY_DIR.glob("*.xlsx"))
        for r in ExcelImporter(path).data
    ]
    suppliers = Counter(identity(r) for _, r in records)
    assert len(retired) == 28 and len(canonical) == 15
    assert all(suppliers[key] == 1 for key in canonical)
    consumers = set()
    for filename, record in records:
        assert identity(record) not in retired
        for edge in record["exchanges"]:
            if edge["type"] != "technosphere":
                continue
            assert identity(edge) not in retired
            if identity(edge) in canonical:
                assert suppliers[identity(edge)] == 1
                consumers.add(filename)
    assert {
        "lci-pass_cars.xlsx",
        "lci-trucks.xlsx",
        "lci-final-energy.xlsx",
        "lci-rail-freight.xlsx",
    } <= consumers


def authored_records(filename):
    """Read quantity expressions directly, retaining formulas and uncertainty."""
    import openpyxl

    result = []
    workbook = openpyxl.load_workbook(
        INVENTORY_DIR / filename, read_only=True, data_only=False
    )
    try:
        for sheet in workbook:
            current, columns, header = None, None, False
            for row in sheet.iter_rows(values_only=True):
                marker = str(row[0] or "").casefold()
                if marker == "activity" and isinstance(row[1], str):
                    current = {"name": row[1], "exchanges": []}
                    result.append(current)
                    columns = None
                elif current is None:
                    continue
                elif marker == "exchanges":
                    header = True
                elif header and any(v is not None for v in row):
                    columns = {str(v): i for i, v in enumerate(row) if v}
                    assert {"name", "amount", "type"} <= columns.keys()
                    header = False
                elif columns is None and row[0]:
                    current[row[0]] = row[1]
                elif columns:
                    edge = {k: row[i] for k, i in columns.items() if row[i] is not None}
                    if edge.get("type") in {
                        "production",
                        "technosphere",
                        "biosphere",
                        "substitution",
                    }:
                        current["exchanges"].append(
                            {
                                k: v
                                for k, v in edge.items()
                                if k not in {"comment", "input", "output", "database"}
                            }
                        )
    finally:
        workbook.close()
    return result


def test_canonical_component_quantities_formulas_and_uncertainty():
    references = json.loads(
        (Path(__file__).parent / "data/vehicle_component_suppliers.json").read_text()
    )["recipes"]
    cached = {}
    for reference in references:
        if reference["workbook"] not in cached:
            cached[reference["workbook"]] = authored_records(reference["workbook"])
        records = cached[reference["workbook"]]
        (record,) = [
            r
            for r in records
            if r["name"].casefold() == reference["identity"]["name"].casefold()
        ]
        for key, value in reference["identity"].items():
            assert record[key] == value
        if reference["production_amount"] is not None:
            assert record["production amount"] == reference["production_amount"]
        assert record["exchanges"] == reference["exchanges"]
