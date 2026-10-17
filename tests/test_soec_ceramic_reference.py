"""Physical oxide ratios, capacity/lifetime basis and complete-design application."""

from collections import defaultdict
from copy import deepcopy
import csv
import importlib.util
import json
from pathlib import Path
from shutil import copyfile
from zipfile import ZipFile
from xml.etree import ElementTree as ET

from bw2io import ExcelImporter
import numpy as np
import pytest

from premise.data_collection import get_metals_intensity_factors_data
from premise.metals import Metals
from premise.metals_rules import (
    MetalsConfigError,
    load_material_rules,
    load_technology_conversions,
)

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "soec_updater", ROOT / "scripts/update_soec_ceramic_reference.py"
)
updater = importlib.util.module_from_spec(spec)
spec.loader.exec_module(updater)
REF = json.loads(updater.REFERENCE.read_text())
TECH = REF["application"]["technology"]
STACK = REF["application"]["activity"]
PRODUCTS = {"yttrium oxide", "zirconium oxide", "cerium oxide"}


def rules():
    return [
        r
        for r in load_material_rules().enabled_rules
        if r.design_group == REF["design_id"]
    ]


@pytest.fixture(scope="module")
def inventory():
    data = ExcelImporter(updater.WORKBOOK).data
    for ds in data:
        for e in ds["exchanges"]:
            if "reference product" in e:
                e["product"] = e.pop("reference product")
    return data


def stack(inventory):
    return deepcopy(next(d for d in inventory if d["name"] == STACK))


def model(year=2035, version="3.12"):
    m = object.__new__(Metals)
    m.year, m.version = year, version
    factors = get_metals_intensity_factors_data()
    selected = min(2050, max(2020, year))
    m.precomputed_medians = (
        factors.sel(year=selected)
        if selected in factors.year
        else factors.interp(year=selected)
    )
    m.material_rules_by_technology = {TECH: rules()}
    m.material_policies = load_material_rules().policies
    m.technology_conversions_by_name = {
        c.activity_name: c for c in load_technology_conversions()
    }
    m.material_update_diagnostics, m.material_decisions = [], []
    m._validation_targets = {}
    m.db_index = defaultdict(lambda: defaultdict(list))
    for product in PRODUCTS:
        ds = {
            "name": f"market for {product}",
            "reference product": product,
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [],
        }
        m.db_index[ds["name"]][product].append(ds)
    m.is_in_index = lambda ds: True
    return m


def amounts(ds):
    return {
        p: sum(
            e["amount"]
            for e in ds["exchanges"]
            if e["type"] == "technosphere" and e.get("product") == p
        )
        for p in PRODUCTS
    }


def test_reference_uses_moles_and_cation_sites_not_mass_percent(inventory):
    ds = stack(inventory)
    components = {
        name: {
            e["product"]: e["amount"]
            for e in ds["exchanges"]
            if e.get("product") in PRODUCTS and e["comment"].startswith(name)
        }
        for name in ("YSZ", "YDC")
    }
    # Independently convert the imported physical oxide masses to moles.
    Y2O3, ZrO2, CeO2 = 225.80868, 123.222, 172.114
    yttria = components["YSZ"]["yttrium oxide"] / Y2O3
    zirconia = components["YSZ"]["zirconium oxide"] / ZrO2
    assert yttria / (yttria + zirconia) == pytest.approx(0.08)
    yttria = components["YDC"]["yttrium oxide"] / Y2O3
    ceria = components["YDC"]["cerium oxide"] / CeO2
    assert 2 * yttria / (2 * yttria + ceria) == pytest.approx(0.20)
    scale = 1000**0.88
    assert sum(components["YSZ"].values()) == pytest.approx(0.425 * scale)
    assert sum(components["YDC"].values()) == pytest.approx(0.262 * scale)
    assert sum(amounts(ds).values()) == pytest.approx(0.687 * scale)
    assert not any("samarium-europium-gadolinium" in e["name"] for e in ds["exchanges"])
    # Linear-capacity chemistry reproduces the independently attributed
    # Haefele elemental observations; it is not the Gerloff scaled stack.
    linear = updater.reference_rows(REF, "linear-capacity")
    for element, expected in REF["historical_attributed_haefele_kg_MW"].items():
        assert float(linear[element, 2020.0, TECH]["median"]) == pytest.approx(
            expected, abs=0.001
        )


def test_csv_rules_and_source_workbook_share_the_same_pure_oxide_basis(inventory):
    source = amounts(stack(inventory))
    expected = updater.reference_rows(REF)
    runtime = {
        (r["metal"], float(r["year"]), r["origin_var"]): r
        for r in csv.DictReader(updater.CSV_PATH.open())
    }
    assert {r.element for r in rules()} == {"Yttrium", "Zirconium", "Cerium"}
    for key, row in expected.items():
        assert runtime[key] == row
        assert len({row[k] for k in ("mean", "median", "min", "max")}) == 1
    for rule in rules():
        metal = float(expected[rule.element, 2020.0, TECH]["median"])
        assert metal * rule.exchange_amount_factor == pytest.approx(
            source[rule.provider.reference_product]
        )


@pytest.mark.parametrize("year", [2020, 2022, 2025, 2030, 2035, 2040, 2045, 2050])
@pytest.mark.parametrize("version", ["3.8", "3.9", "3.10", "3.11", "3.12"])
def test_runtime_keeps_composition_and_applies_future_reduction_once(
    inventory, year, version
):
    ds = stack(inventory)
    before, baseline = deepcopy(ds), amounts(ds)
    m = model(year, version)
    m.update_metal_use(ds, TECH)
    after = amounts(ds)
    y_factor = 225.80868 / (2 * 88.90584)
    baseline_y = baseline["yttrium oxide"] / y_factor
    target_y = baseline_y + (year - 2020) / 30 * (6.016672460842715 - baseline_y)
    expected_scale = target_y / baseline_y
    for product, amount in after.items():
        assert amount == pytest.approx(baseline[product] * expected_scale)
        assert len([e for e in ds["exchanges"] if e.get("product") == product]) == 1
    assert after["yttrium oxide"] / y_factor == pytest.approx(target_y)

    def other(d):
        return [e for e in d["exchanges"] if e.get("product") not in PRODUCTS]

    assert other(ds) == other(before)
    assert all(d["status"] == "updated" for d in m.material_decisions)
    assert all(d["design group"] == REF["design_id"] for d in m.material_decisions)
    previous = deepcopy(ds)
    m.update_metal_use(ds, TECH)
    assert ds["exchanges"] == previous["exchanges"]


@pytest.mark.parametrize(
    "problem",
    [
        "missing Y",
        "missing Ce",
        "missing Zr",
        "wrong unit",
        "missing member",
        "missing coefficient",
        "invalid bounds",
        "unsupported member",
        "2019",
        "2051",
    ],
)
def test_incomplete_ceramic_design_preserves_all_source_inputs(inventory, problem):
    ds = stack(inventory)
    m = model(int(problem) if problem.isdigit() else 2035)
    missing = {
        "missing Y": "yttrium",
        "missing Ce": "cerium",
        "missing Zr": "zirconium",
    }
    if problem in missing:
        m.db_index.pop(f"market for {missing[problem]} oxide")
    elif problem == "wrong unit":
        m.db_index["market for zirconium oxide"]["zirconium oxide"][0]["unit"] = "gram"
    elif problem == "missing member":
        m.material_rules_by_technology[TECH] = rules()[:2]
    elif problem in {"missing coefficient", "invalid bounds", "unsupported member"}:
        m.precomputed_medians = m.precomputed_medians.copy(deep=True)
        if problem == "unsupported member":
            m.precomputed_medians.source_year_max.loc[
                dict(metal="Cerium", origin_var=TECH)
            ] = 2020
        else:
            m.precomputed_medians.loc[
                dict(metal="Cerium", origin_var=TECH, variable="median")
            ] = (np.nan if problem == "missing coefficient" else -1)
    before = deepcopy(ds)
    if problem == "wrong unit":
        with pytest.raises(MetalsConfigError, match="kilogram provider"):
            m.update_metal_use(ds, TECH)
    else:
        m.update_metal_use(ds, TECH)
        assert all(
            d["reason code"] == "metals.material_rule.design_incomplete"
            for d in m.material_decisions
        )
    assert ds == before


def test_hydrogen_allocation_back_calculates_stack_oxides_once(inventory):
    hydrogen = next(
        d
        for d in inventory
        if d["name"]
        == "hydrogen production, gaseous, 1 bar, from SOEC electrolysis, from grid electricity"
    )
    feed = next(e for e in hydrogen["exchanges"] if e["name"] == STACK)
    # Source lifetime H2 output and replacement count: inspect the workbook
    # independently of kg/MW overlays, then solve H2 -> stacks -> oxides.
    from openpyxl import load_workbook

    with_workbook = load_workbook(updater.WORKBOOK, read_only=True, data_only=True)
    specs = with_workbook["Final specifications"]
    # The SOEC specification column is D; one initial plus seven replacements.
    assert specs["D7"].value == 20
    assert specs["D8"].value == 7
    lifetime = 20 * 8000 * specs["D13"].value
    assert feed["amount"] == pytest.approx(8 / lifetime)
    with_workbook.close()
    ds = stack(inventory)
    model(2050).update_metal_use(ds, TECH)
    actual = amounts(ds)
    matrix = np.eye(5)
    matrix[1, 0] = -feed["amount"]
    for index, product in enumerate(sorted(PRODUCTS), 2):
        matrix[index, 1] = -actual[product]
    activity = np.linalg.solve(matrix, [lifetime, 0, 0, 0, 0])
    assert activity[1] == pytest.approx(8)
    assert activity[2:] == pytest.approx([8 * actual[p] for p in sorted(PRODUCTS)])


def test_offline_authoring_is_idempotent_and_sensitivity_exports_are_isolated(tmp_path):
    csv_path, workbook = tmp_path / "runtime.csv", tmp_path / "inventory.xlsx"
    copyfile(updater.CSV_PATH, csv_path)
    copyfile(updater.WORKBOOK, workbook)
    before_csv, before_book = csv_path.read_bytes(), workbook.read_bytes()
    updater.update(csv_path=csv_path, workbook=workbook)
    assert csv_path.read_bytes() == before_csv
    assert workbook.read_bytes() == before_book
    for destination in (csv_path, workbook):
        with pytest.raises(ValueError, match="separate path"):
            updater.update("fixed-baseline", destination, csv_path, workbook)
    assert csv_path.read_bytes() == before_csv
    assert workbook.read_bytes() == before_book
    export = tmp_path / "fixed.csv"
    updater.update("fixed-baseline", export, csv_path, workbook)
    assert csv_path.read_bytes() == before_csv
    assert workbook.read_bytes() == before_book
    fixed = {
        r["metal"]: r
        for r in csv.DictReader(export.open())
        if r["origin_var"] == TECH and float(r["year"]) == 2050
    }
    baseline = updater.reference_rows(REF)
    for element in REF["application"]["elements"]:
        assert fixed[element]["median"] == baseline[element, 2020.0, TECH]["median"]
    updater.update("linear-capacity", export, csv_path, workbook)
    assert csv_path.read_bytes() == before_csv
    assert workbook.read_bytes() == before_book


def test_guarded_csv_edit_preserves_unrelated_records_and_rejects_unknown_inputs(
    tmp_path,
):
    path = tmp_path / "historical.csv"
    expected = updater.reference_rows(REF)
    historical = REF["historical_runtime_rows"]
    header = list(next(iter(expected.values())))
    unrelated = '"Other",kg/MW,2020.0,1,1,1,1,"Other technology"\r\n'
    raw = updater.serialize(header, dict(zip(header, header))) + unrelated
    raw += "".join(updater.serialize(header, r) for r in historical)
    path.write_bytes(raw.encode())
    output = updater.csv_update(path, REF)
    assert unrelated.encode() in output
    path.write_bytes(output)
    assert updater.csv_update(path, REF) == output
    path.write_bytes(raw.replace("183.23997806968148", "999").encode())
    with pytest.raises(ValueError, match="Unrecognized ceramic"):
        updater.csv_update(path, REF)


def test_xml_patch_preserves_other_cells_and_archived_parts(tmp_path):
    path = tmp_path / "inventory.xlsx"
    copyfile(updater.WORKBOOK, path)
    with ZipFile(path) as z:
        before = {n: z.read(n) for n in z.namelist()}
    updater.patch_workbook(path, {"B477": 123.45})
    with ZipFile(path) as z:
        after = {n: z.read(n) for n in z.namelist()}
    part = "xl/worksheets/sheet1.xml"
    for name in before:
        if name != part:
            assert before[name] == after[name]
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

    def cells(text):
        return {c.get("r"): ET.tostring(c) for c in ET.fromstring(text).iter(ns + "c")}

    old, new = cells(before[part]), cells(after[part])
    assert {k: v for k, v in old.items() if k != "B477"} == {
        k: v for k, v in new.items() if k != "B477"
    }
