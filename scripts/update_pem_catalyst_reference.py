"""Reproduce the matched PEM catalyst CSV rows without rewriting other records.

Run from any directory with Python's standard library. Historical observations
remain in the source workbook and the versioned reference JSON.
"""

import csv
import io
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "premise/data/metals/pem_catalyst_reference.json"
TARGET = ROOT / "premise/data/metals/metals_db.csv"


def reference_rows(reference):
    ref, app = reference["reference"], reference["application"]
    # Sum of all cell active areas, not area of one cell. mg -> kg = 1e-6.
    area = ref["rated_stack_DC_power_W"] / (
        ref["cell_voltage_V"] * ref["current_density_A_cm2"]
    )
    power_MW = ref["rated_stack_DC_power_W"] / 1e6
    return {
        (metal, float(year), app["technology"]): {
            "metal": metal,
            "unit": app["intensity_unit"],
            "year": str(float(year)),
            **{
                key: str(loading * area * 1e-6 / power_MW)
                for key in ("mean", "median", "min", "max")
            },
            "origin_var": app["technology"],
        }
        for metal, loading in ref["contained_metal_loadings_mg_cm2"].items()
        for year in app["years"]
    }


def update(path=TARGET, reference_path=REFERENCE):
    reference = json.loads(reference_path.read_text())
    expected = reference_rows(reference)
    historical = {
        (row["metal"], float(row["year"]), row["origin_var"]): row
        for row in reference["previous_runtime_catalyst_rows"]
    }
    # csv.reader.line_num locates complete records with embedded newlines.
    with path.open(newline="") as stream:
        raw = stream.read()
    lines = raw.splitlines(keepends=True)
    reader = csv.reader(io.StringIO(raw, newline=""))
    header = next(reader)
    previous_line = reader.line_num
    output = ["".join(lines[:previous_line])]
    found = set()
    for record in reader:
        end = reader.line_num
        row = dict(zip(header, record))
        key = (row["metal"], float(row["year"]), row["origin_var"])
        if row["origin_var"] == "PEMEL" and row["metal"] in {
            "Iridium",
            "Platinum",
            "Ruthenium",
        }:
            if key not in expected or key in found:
                raise ValueError(f"Unexpected or duplicate catalyst row: {key}")
            if row != expected[key] and row != historical.get(key):
                raise ValueError(f"Unrecognized catalyst source row: {key}")
            found.add(key)
            output.append(serialize(header, expected[key]))
        else:
            output.append("".join(lines[previous_line:end]))
        previous_line = end
    if not set(historical) <= found:
        raise ValueError("Missing historical catalyst rows; refusing partial update")
    for key in expected.keys() - found:
        output.append(serialize(header, expected[key]))
    corrected = "".join(output)
    if corrected != raw:
        with path.open("w", newline="") as stream:
            stream.write(corrected)
    return expected


def serialize(header, row):
    stream = io.StringIO(newline="")
    csv.writer(stream, lineterminator="\n").writerow([row[k] for k in header])
    return stream.getvalue()


if __name__ == "__main__":
    rows = update()
    for (metal, year, _), row in rows.items():
        print(f"{metal}, {year:g}: {row['median']} kg/MW")
