"""Curate public stock observations without normalising away missing cohorts.

Inputs remain local and are pinned by SHA-256. Outputs retain unnormalised stock
quantities, date semantics and unknown mass. No survival weighting or lifetime
inference is applied to already-observed surviving stock.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from decimal import Decimal
import hashlib
from io import BytesIO, TextIOWrapper
import json
import math
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

PINNED = {
    "veh1111.ods": "bc3529975f7fa9e12181b64397cb1e619ae78926130146b64f5c35b9e268d1cc",
    "canada-infrastructure.zip": "4a5cac2fde5dcbf08da296449125b4f1e21b955074bcdaccf5f430edefaaa09b",
    "eia8602020.zip": "ce519fd0b02c74b8c4231dc023bf07f922ae5f2a018fcaf88914502f61aa89ea",
    "eia8602021.zip": "ac35e582335ed5f82c1177361d6e8e82cff083ae85a5ec668815d21e0aef3ee5",
    "eia8602022.zip": "a6f6b00f480ccfb7436804f5cb9984f27c3e0c20cfcbe9b4ed057285b5a68980",
}
NS = {
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
}


def source(path: Path, source_id, url):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if path.name in PINNED and digest != PINNED[path.name]:
        raise ValueError(
            f"Source changed: {path.name}; review a new version explicitly"
        )
    return {
        "source_id": source_id,
        "filename": path.name,
        "sha256": digest,
        "bytes": path.stat().st_size,
        "url": url,
    }


def ods_rows(path, sheet):
    with ZipFile(path) as archive:
        root = ET.fromstring(archive.read("content.xml"))
    table = next(
        t
        for t in root.findall(".//table:table", NS)
        if t.get(f"{{{NS['table']}}}name") == sheet
    )
    for row in table.findall("table:table-row", NS):
        cells = []
        elements = row.findall("table:table-cell", NS)
        # Spreadsheet files pad rows to the application's column limit.
        # Remove only trailing truly empty cells; never collapse interior gaps.
        while (
            elements
            and not "".join(elements[-1].itertext())
            and elements[-1].get(f"{{{NS['office']}}}value") is None
        ):
            elements.pop()
        for cell in elements:
            display = "".join(cell.itertext())
            raw = cell.get(f"{{{NS['office']}}}value")
            repeat = int(cell.get(f"{{{NS['table']}}}number-columns-repeated", "1"))
            if repeat > 10000:
                raise ValueError("Unexpected repeated ODS column count")
            cells.extend([(display, raw)] * repeat)
        for _ in range(int(row.get(f"{{{NS['table']}}}number-rows-repeated", "1"))):
            yield cells


def vehicle_count(cell):
    display, raw = cell
    if raw is None:
        raise ValueError(f"Missing numeric vehicle count: {display!r}")
    value = Decimal(raw) * 1000
    count = int(value.to_integral_value())
    if count < 0 or abs(value - count) > Decimal("0.000001"):
        raise ValueError(f"Unexpected non-integer vehicle count: {raw!r}")
    return count


def uk_vehicles(path: Path):
    rows = list(ods_rows(path, "VEH1111"))
    header_index = next(
        i for i, row in enumerate(rows) if row and row[0][0].startswith("Geography")
    )
    header = [cell[0] for cell in rows[header_index]]
    cohort_columns = [(i, int(name)) for i, name in enumerate(header) if name.isdigit()]
    unknown_col = next(i for i, name in enumerate(header) if name.startswith("Unknown"))
    total_col = header.index("Total")
    observations = []
    seen = set()
    selections = {
        ("Cars", "BATTERY ELECTRIC"): "passenger_bev",
        ("Heavy goods vehicles", "Total"): "heavy_trucks",
    }
    for row_number, row in enumerate(rows[header_index + 1 :], start=header_index + 2):
        if len(row) < len(header):
            continue
        values = [cell[0] for cell in row]
        if values[0] != "United Kingdom" or (values[3], values[4]) not in selections:
            continue
        year = int(values[1][:4])
        if values[1] != f"{year} Q4 (end December)" or values[2] != "Thousands":
            raise ValueError("Unexpected vehicle observation date or unit")
        cohorts = []
        low_count = 0
        for column, cohort in cohort_columns:
            if cohort > year:
                if row[column][0] != "[z]":
                    raise ValueError(
                        "Unexpected future cohort in observed vehicle stock"
                    )
                continue
            count = vehicle_count(row[column])
            if row[column][0] == "[low]":
                low_count += count
            if count:
                cohorts.append({"cohort_year": cohort, "stock": count})
        unknown = vehicle_count(row[unknown_col])
        total = vehicle_count(row[total_col])
        residual = total - unknown - sum(c["stock"] for c in cohorts)
        if residual != 0:
            raise ValueError(f"VEH1111 row {row_number} does not reconcile: {residual}")
        group = selections[(values[3], values[4])]
        if (group, year) in seen:
            raise ValueError("Duplicate selected vehicle observation")
        seen.add((group, year))
        observations.append(
            {
                "group": group,
                "geography": "UK",
                "observation_year": year,
                "observation_date": f"{year}-12-31",
                "unit": "vehicle",
                "source_id": "S02",
                "sheet": "VEH1111",
                "source_row": row_number,
                "body_type": values[3],
                "fuel": values[4],
                "cohort_date_semantics": "year of first use; not necessarily manufacture",
                "cohorts": sorted(cohorts, key=lambda c: c["cohort_year"]),
                "unknown_stock": unknown,
                "total_stock": total,
                "stock_balance_residual": residual,
                "unknown_share": unknown / total,
                "displayed_low_stock_retained": low_count,
                "assumptions": [
                    "Numeric ODS cell values, not rounded display strings, multiplied by 1000",
                    "No survival weighting applied to observed surviving stock",
                    "Unknown first use includes imported vehicles; retained as unknown",
                    "HGV table has no gross-weight, EURO class or mileage-by-age split",
                ],
            }
        )
    if not observations:
        raise ValueError("No selected vehicle observations")
    return observations


def canadian_pipes(path: Path):
    selected = defaultdict(list)
    with ZipFile(path) as archive:
        with TextIOWrapper(
            archive.open("34100289.csv"), encoding="utf-8-sig"
        ) as stream:
            for row_number, row in enumerate(csv.DictReader(stream), start=2):
                if (
                    row["GEO"] == "Quebec"
                    and row["Core public infrastructure assets"]
                    == "Total linear potable water assets"
                    and row["Public organizations"] == "All public organizations"
                    and row["Measures"] == "Number"
                ):
                    selected[int(row["REF_DATE"])].append((row_number, row))
    observations = []
    for year, rows in sorted(selected.items()):
        cohorts, unknown = [], None
        labels = set()
        for row_number, row in rows:
            label = row["Year of completed construction"]
            if (
                label in labels
                or row["SCALAR_FACTOR"] != "units"
                or row["UOM"] != "Number"
            ):
                raise ValueError("Duplicate pipe bin or unexpected source unit")
            labels.add(label)
            if not row["VALUE"] or row["STATUS"] in {"F", "x"}:
                raise ValueError(f"Missing/suppressed selected pipe stock: {row}")
            value = float(row["VALUE"])
            record = {
                "construction_bin": label,
                "stock": value,
                "source_row": row_number,
                "quality_flag": row["STATUS"],
                "vector": row["VECTOR"],
                "coordinate": row["COORDINATE"],
            }
            if label == "Year of completed construction unknown":
                unknown = record
            else:
                cohorts.append(record)
        if len(cohorts) != 6 or unknown is None:
            raise ValueError(
                "Expected six construction bins and explicit unknown pipe stock"
            )
        total = math.fsum(c["stock"] for c in cohorts) + unknown["stock"]
        observations.append(
            {
                "group": "potable_water_pipes",
                "geography": "CA-QC",
                "observation_year": year,
                "observation_date": f"{year}-12-31",
                "unit": "kilometer",
                "source_id": "S14",
                "source_uom": "Number",
                "cohorts": cohorts,
                "unknown_record": unknown,
                "unknown_stock": unknown["stock"],
                "total_stock": total,
                "unknown_share": unknown["stock"] / total,
                "total_definition": "sum of mutually exclusive construction bins, not an independent stock total",
                "cohort_date_semantics": "completed construction",
                "unit_evidence": "https://www.statcan.gc.ca/en/statistical-programs/instrument/5173_Q12_V3#s2",
                "assumptions": [
                    "Survey questions 13 and 15 specify kilometres for linear potable-water assets",
                    "Selected aggregate only; do not add local/transmission/unknown-diameter children again",
                    "2022 includes federal organisations unlike 2020; changes are not pure additions/retirements",
                    "Open pre-1940 bin and unknown dates are retained, not converted to an assumed age here",
                ],
            }
        )
    return observations


def xlsx_records(archive, name, sheet):
    import openpyxl

    workbook = openpyxl.load_workbook(
        BytesIO(archive.read(name)), read_only=True, data_only=True
    )
    rows = workbook[sheet].iter_rows(values_only=True)
    next(rows)
    headers = next(rows)
    for row_number, values in enumerate(rows, start=3):
        if values[0] is not None:
            yield row_number, dict(zip(headers, values))
    workbook.close()


def eia_generators(path: Path, year: int, nerc_region: str = "WECC"):
    selections = {
        "Solar Photovoltaic": "pv",
        "Natural Gas Fired Combined Cycle": "gas_power",
    }
    groups = {
        key: {
            "capacity": defaultdict(float),
            "count": Counter(),
            "unknown_capacity": 0.0,
            "excluded_status_capacity": defaultdict(float),
            "ids": set(),
            "rows": [],
        }
        for key in selections.values()
    }
    with ZipFile(path) as archive:
        plant_regions = {}
        for _, plant in xlsx_records(archive, f"2___Plant_Y{year}.xlsx", "Plant"):
            code = plant["Plant Code"]
            if code in plant_regions:
                raise ValueError(f"Duplicate plant identity: {code}")
            plant_regions[code] = plant["NERC Region"]
        for row_number, row in xlsx_records(
            archive, f"3_1_Generator_Y{year}.xlsx", "Operable"
        ):
            if row["Technology"] not in selections:
                continue
            if row["Plant Code"] not in plant_regions:
                raise ValueError("Generator has no matching plant record")
            if plant_regions[row["Plant Code"]] != nerc_region:
                continue
            group = selections[row["Technology"]]
            state = groups[group]
            capacity = float(row["Nameplate Capacity (MW)"])
            if not math.isfinite(capacity) or capacity <= 0:
                raise ValueError("Invalid selected generator nameplate capacity")
            if row["Status"] != "OP":
                state["excluded_status_capacity"][str(row["Status"])] += capacity
                continue
            if group == "gas_power" and (
                row["Energy Source 1"] != "NG"
                or row["Associated with Combined Heat and Power System"] != "N"
            ):
                state["excluded_status_capacity"]["CHP_or_non_NG"] += capacity
                continue
            key = (row["Plant Code"], row["Generator ID"])
            if key in state["ids"]:
                raise ValueError(f"Duplicate selected generator: {key}")
            state["ids"].add(key)
            cohort = row["Operating Year"]
            if not isinstance(cohort, (int, float)) or not math.isfinite(cohort):
                state["unknown_capacity"] += capacity
                continue
            if int(cohort) != cohort or not 1850 <= cohort <= year:
                raise ValueError(f"Invalid operating year: {cohort}")
            cohort = int(cohort)
            state["capacity"][cohort] += capacity
            state["count"][cohort] += 1
            state["rows"].append(
                {
                    "source_row": row_number,
                    "plant_code": key[0],
                    "generator_id": key[1],
                    "cohort_year": cohort,
                    "capacity_mw": capacity,
                    "state": row["State"],
                    "prime_mover": row["Prime Mover"],
                    "unit_code": row["Unit Code"],
                }
            )
    observations = []
    for group, state in groups.items():
        total = math.fsum(state["capacity"].values()) + state["unknown_capacity"]
        if total <= 0:
            raise ValueError(f"No selected EIA stock for {group}")
        observations.append(
            {
                "group": group,
                "geography": f"US-{nerc_region}",
                "geography_selection": f"Join Plant Code to Plant file NERC Region == {nerc_region}",
                "observation_year": year,
                "observation_date": f"{year}-12-31",
                "unit": "megawatt AC nameplate",
                "source_id": "S05",
                "sheet": "Operable",
                "file": f"3_1_Generator_Y{year}.xlsx",
                "cohort_date_semantics": "initial commercial operation of generator",
                "cohorts": [
                    {
                        "cohort_year": cohort,
                        "stock": capacity,
                        "generator_count": state["count"][cohort],
                    }
                    for cohort, capacity in sorted(state["capacity"].items())
                ],
                "unknown_stock": state["unknown_capacity"],
                "unknown_share": state["unknown_capacity"] / total,
                "total_stock": total,
                "generator_count": len(state["ids"]),
                "excluded_capacity_mw": dict(state["excluded_status_capacity"]),
                "generator_records": state["rows"],
                "assumptions": [
                    "Operating (OP) generators only; standby/out-of-service capacity remains separately reported",
                    "Survey covers plants with at least 1 MW combined nameplate capacity, not all rooftop PV",
                    "AC nameplate weighting is not observed electricity generation weighting",
                    "CCGT excludes CHP and non-NG primary fuel; component generator capacities sum once",
                    "Generator operating years do not identify component replacements or a whole-block construction date",
                ],
            }
        )
    return observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.input_dir
    sources = [
        source(
            root / "veh1111.ods",
            "S02",
            "https://assets.publishing.service.gov.uk/media/69ef3555606c20d41216341a/veh1111.ods",
        ),
        source(
            root / "canada-infrastructure.zip",
            "S14",
            "https://www150.statcan.gc.ca/n1/tbl/csv/34100289-eng.zip",
        ),
    ]
    sources.extend(
        source(
            root / f"eia860{year}.zip",
            "S05",
            f"https://www.eia.gov/electricity/data/eia860/archive/xls/eia860{year}.zip",
        )
        for year in (2020, 2021, 2022)
    )
    observations = uk_vehicles(root / "veh1111.ods") + canadian_pipes(
        root / "canada-infrastructure.zip"
    )
    for year in (2020, 2021, 2022):
        observations.extend(eia_generators(root / f"eia860{year}.zip", year))
    data = {
        "schema_version": 1,
        "stage": "observations_only_not_approved_profiles",
        "sources": sources,
        "observations": observations,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
    print(
        json.dumps(
            {
                "observations": len(observations),
                "reference_2022": [
                    {
                        key: row[key]
                        for key in (
                            "group",
                            "geography",
                            "unit",
                            "total_stock",
                            "unknown_share",
                        )
                    }
                    for row in observations
                    if row["observation_year"] == 2022
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
