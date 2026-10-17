"""Independent power/metal checks and fail-closed matched catalyst application."""

from copy import deepcopy
import csv
import importlib.util
import json
from pathlib import Path
import io

import numpy as np
import pytest
import yaml

from premise.metals_rules import MetalsConfigError, load_material_rules
import premise.metals_rules as rules_module
import test_pem_iridium_provider as fallback_tests
from test_pem_iridium_provider import iridium, model


@pytest.fixture(scope="module")
def source():
    return fallback_tests.source.__wrapped__()


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "premise/data/metals/pem_catalyst_reference.json"
CSV_PATH = ROOT / "premise/data/metals/metals_db.csv"
spec = importlib.util.spec_from_file_location(
    "pem_updater", ROOT / "scripts/update_pem_catalyst_reference.py"
)
updater = importlib.util.module_from_spec(spec)
spec.loader.exec_module(updater)


def catalysts():
    return [r for r in load_material_rules().enabled_rules if r.design_group]


def platinum_market():
    return {
        "name": "market for platinum",
        "reference product": "platinum",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [],
    }


def grouped_model(providers, year=2050):
    m = model(providers, year)
    m.material_rules_by_technology = {"PEMEL": catalysts()}
    return m


def amounts(ds):
    return {
        product: sum(
            e["amount"]
            for e in ds["exchanges"]
            if e["type"] == "technosphere" and e.get("product") == product
        )
        for product in ("iridium", "platinum", "ruthenium")
    }


def test_published_loading_and_power_basis():
    ref = json.loads(REFERENCE.read_text())["reference"]
    assert ref["contained_metal_loadings_mg_cm2"] == {
        "Iridium": 2,
        "Platinum": 1,
        "Ruthenium": 0,
    }
    # One square metre at 2 A/cm2 and 1.9 V supplies 38 kW;
    # 1 MW needs 26.3158 m2. Loadings are 20 g Ir/m2 and 10 g Pt/m2.
    area_m2 = 1000 / 38
    assert area_m2 * 0.020 == pytest.approx(0.5263157894736842)
    assert area_m2 * 0.010 == pytest.approx(0.2631578947368421)
    rounded_W = (
        ref["reported_cells"]
        * ref["reported_cell_active_area_cm2"]
        * ref["current_density_A_cm2"]
        * ref["cell_voltage_V"]
    )
    assert rounded_W == pytest.approx(999780)
    assert (1e6 - rounded_W) / 1e6 == pytest.approx(0.00022)


@pytest.mark.parametrize("year", [2020, 2021, 2022, 2035, 2050])
@pytest.mark.parametrize("execution", ["activity", "compiled"])
def test_complete_design_replaces_mixed_inputs_without_ru_supplier(
    source, year, execution
):
    ds, providers = deepcopy(source)
    # An inherited experimental Ru input must disappear, without a Ru market.
    ds["exchanges"].append(
        dict(
            type="technosphere",
            name="experimental Ru source",
            product="ruthenium",
            amount=0.4171298386604351,
            unit="kilogram",
            location="GLO",
        )
    )
    before = deepcopy(ds)
    providers.append(platinum_market())
    providers_before = deepcopy(providers)
    m = grouped_model(providers, year)
    if execution == "activity":
        m.update_metal_use(ds, "PEMEL")
    else:
        m.activities_metals_map = {"PEMEL": [ds]}
        m.material_update_plan = m._compile_material_update_plan()
        m.material_update_metrics = {"executed pairs": 0, "provider index lookups": 0}
        m.database = [ds]
        m.update_metals_use_in_database()
    actual = amounts(ds)
    fraction = max(0, (year - 2022) / 28)
    ir_loading = 2 * (1 - fraction) + 0.4 * fraction
    pt_loading = 1 * (1 - fraction) + 0.1 * fraction
    assert actual == pytest.approx(
        {"iridium": ir_loading / 3.8, "platinum": pt_loading / 3.8, "ruthenium": 0}
    )
    assert actual["iridium"] / actual["platinum"] == pytest.approx(
        ir_loading / pt_loading
    )
    assert iridium(ds)[0]["location"] == "ZA"

    def other(d):
        return [e for e in d["exchanges"] if e.get("product") not in actual]

    assert other(ds) == other(before)
    assert providers == providers_before
    assert all(d["status"] == "updated" for d in m.material_decisions)
    assert all(
        d["design group"] == catalysts()[0].design_group for d in m.material_decisions
    )
    assert all(
        d["temporal treatment"] == "matched design within configured support"
        for d in m.material_decisions
    )
    after = deepcopy(ds)
    m.update_metal_use(ds, "PEMEL")
    assert ds["exchanges"] == after["exchanges"]


@pytest.mark.parametrize(
    "problem",
    [
        "missing Pt",
        "missing Ir",
        "wrong Pt unit",
        "wrong Ir unit",
        "unknown Ir source",
        "missing member",
        "missing series",
        "invalid bounds",
        "unsupported member",
        "2019",
        "2051",
    ],
)
def test_no_partial_design_is_applied(source, problem):
    ds, providers = deepcopy(source)
    providers.append(platinum_market())
    m = grouped_model(providers, int(problem) if problem.isdigit() else 2050)
    if problem == "missing Pt":
        m.db_index.pop("market for platinum")
    elif problem == "missing Ir":
        m.db_index.clear()
    elif problem == "wrong Pt unit":
        providers[-1]["unit"] = "gram"
    elif problem == "wrong Ir unit":
        next(p for p in providers if p["location"] == "ZA")["unit"] = "gram"
    elif problem == "unknown Ir source":
        iridium(ds)[0]["name"] = "unknown refinery"
    elif problem == "missing member":
        m.material_rules_by_technology["PEMEL"] = catalysts()[:2]
    elif problem in {"missing series", "invalid bounds"}:
        m.precomputed_medians = m.precomputed_medians.copy(deep=True)
        m.precomputed_medians.loc[
            dict(metal="Platinum", origin_var="PEMEL", variable="median")
        ] = (np.nan if problem == "missing series" else -1)
    elif problem == "unsupported member":
        m.precomputed_medians = m.precomputed_medians.copy(deep=True)
        m.precomputed_medians.source_year_max.loc[
            dict(metal="Platinum", origin_var="PEMEL")
        ] = 2040
    before = deepcopy(ds)
    if problem == "wrong Pt unit":
        with pytest.raises(MetalsConfigError, match="kilogram provider"):
            m.update_metal_use(ds, "PEMEL")
    else:
        m.update_metal_use(ds, "PEMEL")
        assert all(
            d["reason code"] == "metals.material_rule.design_incomplete"
            for d in m.material_decisions
        )
    assert ds == before


def test_both_catalysts_back_calculate_through_existing_hydrogen_allocation(source):
    from bw2io import ExcelImporter
    from openpyxl import load_workbook
    from premise.filesystem_constants import INVENTORY_DIR

    path = INVENTORY_DIR / "lci-hydrogen-electrolysis.xlsx"
    hydrogen = next(
        d
        for d in ExcelImporter(path).data
        if d["name"]
        == "hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity"
    )
    specs = load_workbook(path, data_only=True)["Final specifications"]
    assert [specs[f"C{r}"].value for r in (7, 8, 10, 13)] == [20, 3, 54, 18.52]
    lifetime_kg = 20 * 8000 * 18.52
    feed = next(e for e in hydrogen["exchanges"] if e["name"] == fallback_tests.STACK)
    assert feed["amount"] == pytest.approx(4 / lifetime_kg)
    ds, providers = deepcopy(source)
    providers.append(platinum_market())
    grouped_model(providers, 2035).update_metal_use(ds, "PEMEL")
    actual = amounts(ds)
    # Independent direct-path solve: H2 -> replacement-inclusive stacks -> metals.
    matrix = np.eye(4)
    matrix[1, 0] = -feed["amount"]
    matrix[2, 1] = -actual["iridium"]
    matrix[3, 1] = -actual["platinum"]
    activity = np.linalg.solve(matrix, [lifetime_kg, 0, 0, 0])
    fraction = 13 / 28  # 2035 lies 13 years into the 2022-2050 scenario.
    ir_kg = (2 * (1 - fraction) + 0.4 * fraction) / 3.8
    pt_kg = (1 * (1 - fraction) + 0.1 * fraction) / 3.8
    assert activity == pytest.approx([lifetime_kg, 4, 4 * ir_kg, 4 * pt_kg])


@pytest.mark.parametrize("change", ["disabled", "technology", "target", "bad text"])
def test_inconsistent_design_configuration_is_rejected(tmp_path, monkeypatch, change):
    config = yaml.safe_load(rules_module.MATERIAL_RULES_PATH.read_text())
    target = next(r for r in config["rules"] if r["id"] == "pemel-platinum-platinum")
    if change == "disabled":
        target["enabled"] = False
    elif change == "technology":
        target["technology"] = "AEL"
    elif change == "target":
        target["target"] = dict(
            kind="exact_activity", name="other", reference_product="other"
        )
    else:
        target["design_group"] = True
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    monkeypatch.setattr(rules_module, "MATERIAL_RULES_PATH", path)
    load_material_rules.cache_clear()
    try:
        with pytest.raises(MetalsConfigError):
            load_material_rules()
    finally:
        load_material_rules.cache_clear()


def test_authoring_preserves_unrelated_csv_records_and_is_guarded(tmp_path):
    reference = json.loads(REFERENCE.read_text())
    header = ["metal", "unit", "year", "mean", "median", "min", "max", "origin_var"]
    stream = io.StringIO(newline="")
    csv.DictWriter(stream, fieldnames=header, lineterminator="\n").writerows(
        reference["previous_runtime_catalyst_rows"]
    )
    # Unusual quoting and an embedded newline must survive byte-for-byte.
    sentinel = '"Other metal",kg/MW,2020.0,1,1,1,1,"Other\ntechnology"\n'
    original = (",".join(header) + "\n" + sentinel + stream.getvalue()).encode()
    path = tmp_path / "metals.csv"
    path.write_bytes(original)
    updater.update(path)
    corrected = path.read_bytes()
    assert sentinel.encode() in corrected
    parsed = list(csv.DictReader(io.StringIO(corrected.decode(), newline="")))
    actual = [
        r
        for r in csv.DictReader(CSV_PATH.open())
        if r["origin_var"] == "PEMEL"
        and r["metal"] in {"Iridium", "Platinum", "Ruthenium"}
    ]
    assert sorted(parsed[1:], key=lambda r: (r["metal"], r["year"])) == sorted(
        actual, key=lambda r: (r["metal"], r["year"])
    )
    updater.update(path)
    assert path.read_bytes() == corrected
    path.write_bytes(CSV_PATH.read_bytes())
    updater.update(path)
    assert path.read_bytes() == CSV_PATH.read_bytes()
    path.write_bytes(corrected.replace(b"0.5263157894736843", b"7.777"))
    unknown = path.read_bytes()
    with pytest.raises(ValueError, match="Unrecognized"):
        updater.update(path)
    assert path.read_bytes() == unknown


def test_fixed_baseline_sensitivity_round_trip_and_real_runtime(
    source, tmp_path, monkeypatch
):
    import premise.data_collection as data_module

    ds, providers = deepcopy(source)
    providers.append(platinum_market())
    m = grouped_model(providers, 2050)
    original = CSV_PATH.read_bytes()
    folder = tmp_path / "metals"
    folder.mkdir()
    fixed = folder / "metals_db.csv"
    updater.update(CSV_PATH, scenario="fixed-baseline", output_path=fixed)
    fixed_bytes = fixed.read_bytes()
    assert CSV_PATH.read_bytes() == original
    updater.update(fixed, scenario="fixed-baseline")
    assert fixed.read_bytes() == fixed_bytes

    # Read the alternative through the actual loader without replacing runtime
    # source files or disturbing its process-local cache.
    monkeypatch.setattr(data_module, "DATA_DIR", tmp_path)
    factors = data_module.get_metals_intensity_factors_data.__wrapped__()
    m.precomputed_medians = factors.sel(year=2050)
    m.update_metal_use(ds, "PEMEL")
    assert amounts(ds) == pytest.approx(
        {"iridium": 20 / 38, "platinum": 10 / 38, "ruthenium": 0}
    )
    assert all(d["status"] == "updated" for d in m.material_decisions)
    updater.update(fixed, scenario="prospective")
    assert fixed.read_bytes() == original


def test_previous_fixed_reference_migrates_and_unknown_scenario_is_rejected(tmp_path):
    reference = json.loads(REFERENCE.read_text())
    path = tmp_path / "v1.csv"
    with path.open("w", newline="") as stream:
        rows = reference["previous_fixed_reference_rows"]
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    updater.update(path)
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 9
    assert {float(r["year"]) for r in rows} == {2020, 2022, 2050}
    ir = next(r for r in rows if r["metal"] == "Iridium" and r["year"] == "2050.0")
    assert float(ir["median"]) == pytest.approx(0.4 / 3.8)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Unknown PEM catalyst scenario"):
        updater.update(path, scenario="unknown")
    assert path.read_bytes() == before
