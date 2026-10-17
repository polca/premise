"""Reproduce matched SOEC ceramic oxides, preserving unrelated CSV/Excel data.

Offline authoring only. Reference compositions, source access limitations and
prospective assumptions are documented in soec_ceramic_reference.json.
"""

import argparse
import csv
import io
import json
from pathlib import Path
import re
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "premise/data/metals/soec_ceramic_reference.json"
CSV_PATH = ROOT / "premise/data/metals/metals_db.csv"
WORKBOOK = ROOT / "premise/data/additional_inventories/lci-hydrogen-electrolysis.xlsx"
SCENARIOS = ("prospective", "fixed-baseline", "linear-capacity")
NOTE = (
    "SOEC matched ceramic reference v1. YSZ: 8 mol% Y2O3 among binary oxides; "
    "YDC: Ce0.8Y0.2O1.9 (20% Y on cation sites). Gerloff 2021 capacity scaling "
    "of the attributed Haefele 2016 component masses, not concentrate mass "
    "treated as pure oxide. Mixed Sm/Eu/Gd proxy replaced by stoichiometric "
    "Y2O3. See data/metals/soec_ceramic_reference.json for source-access limits, "
    "explicit composition/scaling assumptions and remaining whole-stack gaps."
)


def reference_recipe(reference, exponent=None):
    """Pure oxide kg/nominal stack, split by physical ceramic component."""
    r = reference["reference"]
    a = r["atomic_masses_g_mol"]
    molar = {
        "yttrium oxide": 2 * a["Y"] + 3 * a["O"],
        "zirconium oxide": a["Zr"] + 2 * a["O"],
        "cerium oxide": a["Ce"] + 2 * a["O"],
    }
    exponent = r["stack_capacity_exponent"] if exponent is None else exponent
    scale = (r["target_power_kW"] / r["source_power_kW"]) ** exponent
    ysz = r["YSZ_kg_per_source_kW"] * r["source_power_kW"] * scale
    ydc = r["YDC_kg_per_source_kW"] * r["source_power_kW"] * scale
    x = r["YSZ_Y2O3_oxide_mole_fraction"]
    # YSZ uses oxide mole fraction. YDC uses cation fraction: two Y per Y2O3.
    w_ysz = (
        x
        * molar["yttrium oxide"]
        / (x * molar["yttrium oxide"] + (1 - x) * molar["zirconium oxide"])
    )
    x = r["YDC_Y_cation_fraction"]
    w_ydc = (
        (x / 2)
        * molar["yttrium oxide"]
        / ((x / 2) * molar["yttrium oxide"] + (1 - x) * molar["cerium oxide"])
    )
    components = {
        "YSZ": {"yttrium oxide": ysz * w_ysz, "zirconium oxide": ysz * (1 - w_ysz)},
        "YDC": {"yttrium oxide": ydc * w_ydc, "cerium oxide": ydc * (1 - w_ydc)},
    }
    factors = {}
    for element, info in reference["application"]["elements"].items():
        atoms = 2 if element == "Yttrium" else 1
        factors[element] = molar[info["product"]] / (atoms * a[info["symbol"]])
    return components, factors


def reference_rows(reference, scenario="prospective"):
    if scenario not in SCENARIOS:
        raise ValueError(f"Unknown SOEC scenario: {scenario!r}")
    components, factors = reference_recipe(
        reference, exponent=1 if scenario == "linear-capacity" else None
    )
    app, projection = reference["application"], reference["projection"]
    baseline = {
        element: sum(c.get(info["product"], 0) for c in components.values())
        / factors[element]
        / reference["reference"]["nominal_power_MW"]
        for element, info in app["elements"].items()
    }
    result = {}
    for year in app["years"]:
        fraction = (year - projection["baseline_year"]) / (
            projection["endpoint_year"] - projection["baseline_year"]
        )
        end_scale = projection["yttrium_kg_MW_2050"] / baseline["Yttrium"]
        scale = 1 if scenario == "fixed-baseline" else 1 + fraction * (end_scale - 1)
        for element, amount in baseline.items():
            result[(element, float(year), app["technology"])] = {
                "metal": element,
                "unit": app["intensity_unit"],
                "year": str(float(year)),
                **{k: str(amount * scale) for k in ("mean", "median", "min", "max")},
                "origin_var": app["technology"],
            }
    return result


def csv_update(path, reference, scenario="prospective"):
    """Return a guarded edit: preserve every unrelated record byte for byte."""
    expected = reference_rows(reference, scenario)
    historical = {
        (r["metal"], float(r["year"]), r["origin_var"]): r
        for r in reference["historical_runtime_rows"]
    }
    recognized = [historical, *(reference_rows(reference, s) for s in SCENARIOS)]
    raw = path.read_bytes().decode()
    lines = raw.splitlines(keepends=True)
    reader = csv.reader(io.StringIO(raw, newline=""))
    header = next(reader)
    last = reader.line_num
    output, found = ["".join(lines[:last])], set()
    for record in reader:
        end = reader.line_num
        row = dict(zip(header, record))
        key = (row["metal"], float(row["year"]), row["origin_var"])
        if (
            row["origin_var"] == reference["application"]["technology"]
            and row["metal"] in reference["application"]["elements"]
        ):
            if key not in expected or key in found:
                raise ValueError(f"Unexpected or duplicate ceramic row: {key}")
            if not any(row == known.get(key) for known in recognized):
                raise ValueError(f"Unrecognized ceramic source row: {key}")
            found.add(key)
            output.append(serialize(header, expected[key]))
        else:
            output.append("".join(lines[last:end]))
        last = end
    if not set(historical) <= found:
        raise ValueError("Missing historical ceramic rows; refusing partial update")
    for key, row in expected.items():
        if key not in found:
            output.append(serialize(header, row))
    return "".join(output).encode()


def serialize(header, row):
    stream = io.StringIO(newline="")
    csv.writer(stream, lineterminator="\n").writerow([row[k] for k in header])
    return stream.getvalue()


def workbook_updates(path, reference):
    """Verify the target block, then replace its four ceramic rows only."""
    w = load_workbook(path, data_only=True, read_only=True)
    s = w["lci"]
    app = reference["application"]
    if s["B464"].value != app["activity"] or s["B469"].value != "unit":
        raise ValueError("Unexpected SOEC workbook layout or functional unit")
    components, _ = reference_recipe(reference)
    updates = {}
    old = {
        477: ("zirconium oxide", 170.7),
        478: ("samarium-europium-gadolinium oxide", 14.8),
        479: ("cerium oxide", 91.5),
        480: ("samarium-europium-gadolinium oxide", 22.9),
    }
    specifications = {
        477: ("YSZ", "zirconium oxide"),
        478: ("YSZ", "yttrium oxide"),
        479: ("YDC", "cerium oxide"),
        480: ("YDC", "yttrium oxide"),
    }
    for row, (component, product) in specifications.items():
        amount = components[component][product]
        previous_product, previous_amount = old[row]
        identity = tuple(s[f"{c}{row}"].value for c in ("A", "B", "G"))
        if identity not in (
            (f"market for {previous_product}", previous_amount, previous_product),
            (f"market for {product}", amount, product),
        ):
            raise ValueError(f"Unrecognized SOEC ceramic row {row}: {identity}")
        if (s[f"C{row}"].value, s[f"D{row}"].value, s[f"F{row}"].value) != (
            "GLO",
            "kilogram",
            "technosphere",
        ):
            raise ValueError(f"Unexpected SOEC exchange boundary at row {row}")
        updates.update(
            {
                f"A{row}": f"market for {product}",
                f"B{row}": amount,
                f"G{row}": product,
                f"H{row}": f"{component} ceramic component. {NOTE}",
            }
        )
    comment = s["B466"].value
    updates["B466"] = comment if NOTE in comment else comment + "\n" + NOTE
    w.close()
    return updates


def patch_workbook(path, updates):
    """Edit selected existing XML cells; keep all other cells/caches verbatim."""
    part = "xl/worksheets/sheet1.xml"
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    with ZipFile(path) as archive:
        entries = [(i, archive.read(i.filename)) for i in archive.infolist()]
    if part not in {info.filename for info, _ in entries}:
        raise ValueError(f"Missing inventory worksheet part: {part}")
    changed = False
    for index, (info, content) in enumerate(entries):
        if info.filename != part:
            continue
        for address, value in updates.items():
            pattern = rb'<c\b[^>]*\br="' + address.encode() + rb'"[^>]*(?:/>|>.*?</c>)'
            cell = ET.Element("c", {"r": address})
            if isinstance(value, (int, float)):
                ET.SubElement(cell, "v").text = repr(value)
            else:
                cell.set("t", "inlineStr")
                ET.SubElement(ET.SubElement(cell, "is"), "t").text = value
            replacement = ET.tostring(cell, encoding="utf-8")
            content, count = re.subn(
                pattern, lambda _: replacement, content, flags=re.S
            )
            if count != 1:
                raise ValueError(f"Missing or duplicate target XML cell {address}")
        # No edited cells contain formulas; unrelated formula caches stay intact.
        assert ET.fromstring(content).tag == f"{{{ns}}}worksheet"
        changed = content != entries[index][1]
        entries[index] = info, content
    if changed:
        with ZipFile(path, "w") as archive:
            for info, content in entries:
                archive.writestr(info, content)
    return changed


def update(
    scenario="prospective", output_path=None, csv_path=CSV_PATH, workbook=WORKBOOK
):
    reference = json.loads(REFERENCE.read_text())
    corrected = csv_update(csv_path, reference, scenario)
    destination = output_path or csv_path
    if output_path is not None and destination.resolve() in {
        csv_path.resolve(),
        workbook.resolve(),
    }:
        raise ValueError("Sensitivity export must use a separate path")
    # Export sensitivities without touching runtime CSV or source workbook.
    if output_path is None:
        updates = workbook_updates(workbook, reference)
        patch_workbook(workbook, updates)
    if destination != csv_path or corrected != csv_path.read_bytes():
        destination.write_bytes(corrected)
    return reference_rows(reference, scenario)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=SCENARIOS, default="prospective")
    parser.add_argument(
        "--output", type=Path, help="Export CSV without editing runtime data"
    )
    args = parser.parse_args()
    for key, row in update(args.scenario, args.output).items():
        print(f"{key[0]}, {key[1]:g}: {float(row['median']):.9g} kg metal/MW")
