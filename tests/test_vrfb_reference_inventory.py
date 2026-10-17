"""VRFB concentration, redox charge, lifetime and signed-disposal contracts."""

from copy import deepcopy
import hashlib
from pathlib import Path
import runpy
import shutil
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from bw2io import ExcelImporter
import numpy as np
from openpyxl import load_workbook
import pytest

from premise.battery import Battery, load_cell_energy_density
from premise.filesystem_constants import INVENTORY_DIR

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/update_vanadium_reference_inventory.py"
AUTHOR = runpy.run_path(str(SCRIPT))
CAPACITY = "market for battery capacity, redox-flow, Vanadium, stationary"
PACK = "market for battery, redox-flow, Vanadium"
ELECTROLYTE = "vanadium electrolyte solution production"
ASSEMBLY = "vanadium-redox flow battery system assembly, 8.3 megawatt hour"
STACK = "vanadium-redox flow battery stack assembly"
SERVICE = "electricity supply, high voltage, from vanadium-redox flow battery system"


def activity(data, name):
    (ds,) = [d for d in data if d["name"] == name]
    return ds


def amount(ds, name):
    (exc,) = [e for e in ds["exchanges"] if e["name"] == name]
    return exc["amount"]


@pytest.fixture(scope="module")
def data():
    items = ExcelImporter(INVENTORY_DIR / "lci-batteries-vanadium-redox-flow.xlsx").data
    items += ExcelImporter(INVENTORY_DIR / "lci-vrfb-reference-conditioning.xlsx").data
    items += [
        activity(
            ExcelImporter(INVENTORY_DIR / "lci-battery-capacity.xlsx").data, CAPACITY
        )
    ]
    return items


def test_solution_assay_redox_and_complete_mass_balance(data):
    electrolyte = activity(data, ELECTROLYTE)
    conditioning = activity(data, AUTHOR["CONDITIONING"])
    oxide = amount(electrolyte, "vanadium pentoxide production")
    acid = amount(electrolyte, "market for sulfuric acid")
    phosphate = amount(
        electrolyte,
        "market for phosphoric acid, industrial grade, without water, in 85% solution state",
    )
    water = amount(conditioning, "market for water, deionised")
    oxygen = amount(conditioning, "Oxygen")
    mol_V = oxide / (2 * 0.0509415 + 5 * 0.015999) * 2
    # Independent manufacturer specs: 1 kg occupies 1/1.36 litres.
    assert mol_V * 1.36 == pytest.approx(1.6)
    assert acid / 0.098079 * 1.36 == pytest.approx(4.2)
    assert phosphate / 0.097994 * 1.36 == pytest.approx(0.05)
    assert oxygen / (2 * 0.015999) * 4 == pytest.approx(mol_V * 1.5)
    assert oxide + acid + phosphate / 0.85 + water == pytest.approx(1 + oxygen)
    # Table 6's denominator is elemental V, not kg electrolyte or V2O5.
    assert amount(
        conditioning, "market for electricity, medium voltage"
    ) == pytest.approx(mol_V * 0.0509415 * 1.69)
    assert amount(electrolyte, AUTHOR["CONDITIONING"]) == 1
    assert not any(
        e["name"] == "market for water, deionised" for e in electrolyte["exchanges"]
    )
    assert conditioning["unit"] == "unit"
    assert "remaining-error warning is unresolved" in electrolyte["comment"]


def test_freight_follows_corrected_pentoxide_mass(data):
    electrolyte = activity(data, ELECTROLYTE)
    oxide = amount(electrolyte, "vanadium pentoxide production")
    for name, original in [
        ("market for transport, freight, lorry 16-32 metric ton, EURO5", 0.020),
        ("market for transport, freight train", 0.121),
        ("market for transport, freight, sea, container ship", 3.813),
        ("market for transport, freight, inland waterways, barge", 0.173),
        ("market for transport, freight, lorry, unspecified", 0.187),
    ]:
        assert amount(electrolyte, name) / oxide == pytest.approx(original / 0.286)


def solve(data, demand):
    keys = {
        (d["name"], d["reference product"], d["location"], d["unit"]): i
        for i, d in enumerate(data)
    }
    assert len(keys) == len(data)
    matrix = np.zeros((len(data), len(data)))
    for col, ds in enumerate(data):
        for exc in ds["exchanges"]:
            if exc["type"] == "production":
                matrix[col, col] += exc["amount"]
            elif exc["type"] == "technosphere":
                key = (
                    exc["name"],
                    exc.get("product") or exc.get("reference product"),
                    exc["location"],
                    exc["unit"],
                )
                if key in keys:
                    matrix[keys[key], col] -= exc["amount"]
    vector = np.array([float(d["name"] == demand) for d in data])
    x = np.linalg.solve(matrix, vector)
    assert min(x) >= -1e-12
    return {d["name"]: x[i] for i, d in enumerate(data)}


def test_capacity_is_supported_by_both_reservoirs_without_double_counting_V(data):
    capacity = activity(data, CAPACITY)
    mass = amount(capacity, PACK)
    assert mass == pytest.approx(59.64062663729406)
    x = solve(data, CAPACITY)
    electrolyte_kg = x[ELECTROLYTE]
    mol_V = electrolyte_kg / 1.36 * 1.6
    # Paired half reactions: one electron per PAIR of V atoms across tanks.
    assert mol_V / 2 * 96485.33212 / 3600 * 1.25 == pytest.approx(1000)
    assert mol_V * 0.0509415 == pytest.approx(3.04111551002453, rel=1e-7)
    assert x[ASSEMBLY] == pytest.approx(1 / 5364.6317626034315)
    assert x[STACK] == pytest.approx(2 / 5364.6317626034315)
    assert x["treatment of electrolyte solution, for VRFB system"] == pytest.approx(
        electrolyte_kg
    )
    assert x["treatment of stack, for VRFB system"] == pytest.approx(x[STACK] * 10016)
    assert amount(capacity, AUTHOR["USED_MARKET"]) == pytest.approx(-mass)
    waste = activity(data, AUTHOR["USED_MARKET"])
    assert waste["production amount"] == -1
    assert sum(
        -e["amount"] for e in waste["exchanges"] if e["type"] == "technosphere"
    ) == pytest.approx(1 + 10016 / 319950)
    assert capacity["production amount"] == 1
    assert all(
        e.get("uncertainty type") == 0 and "minimum" not in e and "maximum" not in e
        for e in capacity["exchanges"]
        if e["type"] == "technosphere"
    )


def test_service_and_capacity_allocate_the_same_lifetime_materials(data):
    installed, delivered = solve(data, CAPACITY), solve(data, SERVICE)
    output_per_installed_kWh = 0.95 * (20 * 365 * 1.12)
    for name in [
        ASSEMBLY,
        STACK,
        ELECTROLYTE,
        "treatment of stack, for VRFB system",
        "treatment of electrolyte solution, for VRFB system",
        "treatment of electrolyte tank, for VRFB system",
        "treatment of periphericals, for VRFB system",
    ]:
        assert delivered[name] == pytest.approx(
            installed[name] / output_per_installed_kWh
        )
    assert amount(
        activity(data, SERVICE), "market group for electricity, high voltage"
    ) == pytest.approx(1 / 0.75)


@pytest.mark.parametrize("year", [2010, 2020, 2035, 2050, 2100])
def test_reference_design_does_not_shrink_with_year(data, year):
    capacity = deepcopy(activity(data, CAPACITY))
    before = deepcopy(capacity["exchanges"])
    battery = object.__new__(Battery)
    battery.database = [capacity]
    battery.year = year
    battery.write_log = lambda *args, **kwargs: None
    battery.adjust_battery_mass()
    assert capacity["exchanges"] == before
    targets = load_cell_energy_density()[CAPACITY]
    for target in targets.values():
        assert target == pytest.approx(
            dict.fromkeys(("mean", "minimum", "maximum"), 5364.6317626034315 / 319950)
        )


def formulas(path):
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    result = {}
    with ZipFile(path) as z:
        for name in z.namelist():
            if name.startswith("xl/worksheets/") and name.endswith(".xml"):
                for c in ET.fromstring(z.read(name)).iter(ns + "c"):
                    f, v = c.find(ns + "f"), c.find(ns + "v")
                    if f is not None:
                        result[name, c.get("r")] = (
                            f.text,
                            v.text if v is not None else None,
                        )
    return result


@pytest.mark.parametrize(
    "filename,patch",
    [
        ("lci-batteries-vanadium-redox-flow.xlsx", "patch_vrfb"),
        ("lci-battery-capacity.xlsx", "patch_capacity"),
        ("lci-vrfb-reference-conditioning.xlsx", "write_auxiliary"),
    ],
)
def test_authoring_is_idempotent_and_retains_formula_caches(tmp_path, filename, patch):
    path = tmp_path / filename
    shutil.copyfile(INVENTORY_DIR / filename, path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    formula_before = formulas(path)
    assert AUTHOR[patch](path) is False
    assert formulas(path) == formula_before
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


@pytest.mark.parametrize(
    "filename,patch,sheet,cell",
    [
        ("lci-batteries-vanadium-redox-flow.xlsx", "patch_vrfb", "Sheet1", "B106"),
        ("lci-batteries-vanadium-redox-flow.xlsx", "patch_vrfb", "Sheet1", "B107"),
        ("lci-battery-capacity.xlsx", "patch_capacity", "Battery - stationary", "B90"),
    ],
)
def test_authoring_rejects_unrecognized_source_doses(
    tmp_path, filename, patch, sheet, cell
):
    path = tmp_path / filename
    shutil.copyfile(INVENTORY_DIR / filename, path)
    workbook = load_workbook(path)
    workbook[sheet][cell] = 123
    workbook.save(path)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Unknown"):
        AUTHOR[patch](path)
    assert path.read_bytes() == before


def test_cache_fingerprints_include_both_vrfb_workbooks(tmp_path, monkeypatch):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = "3.12"
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    original = obj._database_cache_path("test", inventories=True)
    source = obj._database_cache_path("test")
    for constant in ["FILEPATH_VRFB_REFERENCE", "FILEPATH_VANADIUM_REDOX_BATTERY"]:
        path = tmp_path / Path(getattr(module, constant)).name
        path.write_bytes(b"changed VRFB reference")
        with monkeypatch.context() as patch:
            patch.setattr(module, constant, path)
            assert obj._database_cache_path("test", inventories=True) != original
            assert obj._database_cache_path("test") == source


def test_import_dependency_order():
    generator = runpy.run_path(
        str(ROOT / "scripts/generate_documentation_reference.py")
    )
    import ast

    tree = ast.parse((ROOT / "premise/new_database.py").read_text())
    order = [x[0] for x in generator["inventory_selection"](tree, "3.12", "cutoff")]
    assert (
        order.index("lci-vrfb-reference-conditioning.xlsx")
        < order.index("lci-batteries-vanadium-redox-flow.xlsx")
        < order.index("lci-battery-capacity.xlsx")
    )


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
def test_oxygen_biosphere_exchange_survives_import_linking(version):
    from premise.inventory_imports import DefaultInventory

    importer = DefaultInventory(
        [],
        "3.10",
        version,
        INVENTORY_DIR / "lci-vrfb-reference-conditioning.xlsx",
        "cutoff",
        True,
    )
    conditioning = activity(importer.import_db.data, AUTHOR["CONDITIONING"])
    before = amount(conditioning, "Oxygen")
    importer.add_biosphere_links()
    (oxygen,) = [e for e in conditioning["exchanges"] if e["name"] == "Oxygen"]
    assert oxygen["amount"] == before
    assert oxygen["categories"] == ("air",)
    assert oxygen["input"][1] == "9ec076d9-6d9f-4a0b-9851-730626ed4319"


@pytest.mark.parametrize(
    "filename", ["lci-batteries-vanadium-redox-flow.xlsx", "lci-battery-capacity.xlsx"]
)
def test_excel_calculation_chain_does_not_reference_replaced_formulas(filename):
    path = INVENTORY_DIR / filename
    formula_cells = formulas(path)
    with ZipFile(path) as archive:
        chain = ET.fromstring(archive.read("xl/calcChain.xml"))
    sheet_id = None
    for cell in chain:
        sheet_id = cell.get("i", sheet_id)
        # These source workbooks retain their original sheet IDs/order.
        assert (
            "xl/worksheets/sheet" + sheet_id + ".xml",
            cell.get("r"),
        ) in formula_cells
    assert len(chain) == len(formula_cells)
