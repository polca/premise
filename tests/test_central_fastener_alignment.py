"""ALIGN-047: central fastener mass and coupled original-profile bookkeeping."""

import json
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest
from delegated_source_history import pre_delegation_workbook
from delegated_source_history import (
    reviewed_excel_importer as ExcelImporter,
)
from delegated_source_history import (
    reviewed_zipfile as ZipFile,
)
from inventory_assertions import assert_review_cells, formula_caches
from test_cooling_aluminium_stock import previous_stock_source, previous_stock_xml
from test_sixteen_monitor_alignment import previous_monitor_source, previous_monitor_xml

from premise.filesystem_constants import INVENTORY_DIR

ROOT = Path(__file__).resolve().parents[1]
LIS = "lci-batteries-LiS.xlsx"
NMC = "lci-batteries-NMC955-LTO.xlsx"
LIO2 = "lci-batteries-LiO2.xlsx"
CAPACITY = "lci-battery-capacity.xlsx"
WORKBOOKS = (LIS, NMC, LIO2, CAPACITY)
CENTRAL = "battery central control and protection unit production"
USED = "market for used Li-ion battery"
OLD_MASS, NEW_MASS, REDUCTION = 4.518, 4.4928, 0.0252


NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
PINS = {
    "lci-batteries-LiS.xlsx": {
        "other_parts_count": 12,
        "formulas_unchanged_count": 127,
    },
    "lci-batteries-NMC955-LTO.xlsx": {
        "other_parts_count": 12,
        "formulas_unchanged_count": 6,
    },
    "lci-batteries-LiO2.xlsx": {
        "other_parts_count": 12,
        "formulas_unchanged_count": 175,
    },
    "lci-battery-capacity.xlsx": {
        "other_parts_count": 15,
        "formulas_unchanged_count": 159,
    },
}
ORIGINAL_LITERALS = {
    "lci-batteries-LiS.xlsx": {"B1093": [None, "0.028"], "B1134": [None, "-15.6176"]},
    "lci-batteries-NMC955-LTO.xlsx": {
        "B781": [None, "-6.668"],
        "B823": [None, "-6.548"],
        "B865": [None, "-6.538"],
        "B907": [None, "-6.538"],
        "B949": [None, "-6.568"],
        "B991": [None, "-6.538"],
        "B1033": [None, "-6.548"],
        "B1075": [None, "-6.538"],
        "B1117": [None, "-6.478"],
    },
    "lci-batteries-LiO2.xlsx": {
        "B282": ["0.002651932002076728", "0.002651932002076728"],
        "B604": [None, "0.8853474988933157"],
    },
    "lci-battery-capacity.xlsx": {
        "B282": [None, "0.12078060465157239"],
        "B283": [None, "0.12078060465157239"],
        "B284": [None, "6.566284025961444"],
        "B285": [None, "6.566284025961444"],
        "B286": [None, "0.19742983452660873"],
        "B287": [None, "0.025549743291678775"],
        "B288": [None, "0.21833416994707314"],
        "B289": [None, "0.21833416994707314"],
        "B306": [None, "0.03689062848050915"],
        "B307": [None, "0.03689062848050915"],
        "B308": [None, "2.005573206046141"],
        "B309": [None, "2.005573206046141"],
        "B310": [None, "0.06030198886237072"],
        "B311": [None, "0.007803786793953858"],
        "B312": [None, "0.06668690533015115"],
        "B313": [None, "0.06668690533015115"],
        "B314": [None, "0.2490117422434367"],
        "B315": [None, "0.2490117422434367"],
    },
}


def reference():
    payload = (
        ROOT / "scripts/data/central-fastener-adoption-20261027.json"
    ).read_bytes()

    return json.loads(payload)


def plan(filename):
    (match,) = [w for w in reference()["workbooks"] if w["workbook"] == filename]
    return match


def activity(data, name):
    (match,) = [r for r in data if r["name"] == name]
    return match


def exchange(record, name):
    (match,) = [e for e in record["exchanges"] if e["name"] == name]
    return match


def science(record):
    return sorted(
        [
            [
                e["type"],
                e["name"],
                e.get("reference product"),
                e.get("location"),
                e["unit"],
                e["amount"],
            ]
            for e in record["exchanges"]
        ]
    )


def physical_materials(record):
    return [
        e
        for e in record["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "kilogram"
        and e["amount"] > 0
        and "metal working" not in e["name"]
        and "injection moulding" not in e["name"]
    ]


def expected_coupled_amount(filename, change, before):
    """Independent mass rules, rather than accepting the planned after amount."""
    e = before["exchanges"][change["exchange_index"]]
    if filename == LIS and e["name"] == "IBIS fasteners production, for LiS battery":
        return 0.0028
    if filename in (LIS, NMC):
        assert e["name"] == USED and e["amount"] < 0
        return e["amount"] + REDUCTION * exchange(before, CENTRAL)["amount"]
    if filename == LIO2:
        assert e["name"] == CENTRAL and e["unit"] == "unit"
        return (
            4 / NEW_MASS / 333.85
            if before["name"] == "battery production, Li-O2"
            else 4 / NEW_MASS
        )
    assert filename == CAPACITY
    mass = 44.6 if "passenger car" in before["name"] else 238
    assert e["name"] != CENTRAL and e["unit"] == "kilogram" and e["amount"] > 0
    return e["amount"] * ((0.3 * mass - NEW_MASS) / (0.3 * mass - OLD_MASS))


def validate_current_fasteners(source, filename):
    w = plan(filename)
    PINS[filename]
    assert len(source.data) == w["activity_count"]

    expected = {a["index"]: deepcopy(a["before"]) for a in w["affected_activities"]}
    for change in w["changes"]:
        before = expected[change["activity_index"]]
        e = before["exchanges"][change["exchange_index"]]
        assert e == change["before_exchange"] and e["amount"] == change["before"]
        assert e.get("uncertainty type", 0) == 0
        assert not {"loc", "scale", "shape", "minimum", "maximum"} & e.keys()
        independent = expected_coupled_amount(filename, change, before)
        assert change["after"] == pytest.approx(independent, rel=1e-14, abs=0)
        e["amount"] = change["after"]
    for note in w["notes"]:
        record = expected[note["activity_index"]]
        assert record["comment"] == note["before"]
        assert note["prefix"].startswith(
            "APPROVED CENTRAL-CONTROL FASTENER AND MASS RECONCILIATION"
        )
        record["comment"] = note["prefix"] + note["before"]
    for a in w["affected_activities"]:
        assert a["after"] == expected[a["index"]]
        assert source.data[a["index"]] == expected[a["index"]]
    # Restoring only approved records must reproduce the entire independent
    # original workbook, including unmodified worksheets and field spelling.
    historical = list(source.data)
    for a in w["affected_activities"]:
        historical[a["index"]] = deepcopy(a["before"])

    if filename == LIS:
        assert science(activity(source.data, CENTRAL)) == science(
            reference()["CLIC_reference"]["snapshot"]
        )
    return w, historical


def previous_fastener_source(source, filename):
    """One reversal at extraction; no markers, recursive layers or second pass."""
    source = previous_stock_source(source, filename)
    source = previous_monitor_source(source, filename)
    _, historical = validate_current_fasteners(source, filename)
    source.data = historical
    return source


def xml_scope(root, w):
    addresses = {c["cell"] for c in w["changes"]} | {c["cell"] for c in w["notes"]}
    return [
        [a, ET.tostring(root.find(f".//{NS}c[@r='{a}']"), encoding="unicode")]
        for a in sorted(addresses)
    ]


def previous_fastener_xml(root, filename, sheet=SHEET):
    if sheet != SHEET:
        return root
    root = previous_stock_xml(root, filename, sheet)
    root = previous_monitor_xml(root, filename, sheet)
    w, _ = validate_current_fasteners(
        previous_monitor_source(
            previous_stock_source(ExcelImporter(INVENTORY_DIR / filename), filename),
            filename,
        ),
        filename,
    )
    assert_review_cells(root, w, "after", INVENTORY_DIR / filename)
    for address, (formula, value) in ORIGINAL_LITERALS[filename].items():
        cell = root.find(f".//{NS}c[@r='{address}']")
        node = cell.find(NS + "f")
        assert (node is None) == (formula is None)
        if node is not None:
            node.text = formula
        cell.find(NS + "v").text = value
    for note in w["notes"]:
        root.find(f".//{NS}c[@r='{note['cell']}']/{NS}is/{NS}t").text = note["before"]
    assert_review_cells(root, w, "before", INVENTORY_DIR / filename)
    return root


@pytest.fixture(scope="module", autouse=True)
def original_delegated_science_view(tmp_path_factory):
    """Keep this older approval's independent before hashes unchanged."""
    global INVENTORY_DIR
    original = INVENTORY_DIR
    view = tmp_path_factory.mktemp("pre-delegated-source-view")
    for filename in (*WORKBOOKS, "lci-batteries-ASSB.xlsx"):
        path = original / filename
        if filename in (NMC, "lci-batteries-ASSB.xlsx"):
            (view / filename).write_bytes(
                pre_delegation_workbook(path.read_bytes(), filename)
            )
        else:
            (view / filename).symlink_to(path)
    INVENTORY_DIR = view
    try:
        yield
    finally:
        INVENTORY_DIR = original


@pytest.fixture(scope="module")
def current():
    return {
        name: previous_monitor_source(
            previous_stock_source(ExcelImporter(INVENTORY_DIR / name), name), name
        )
        for name in WORKBOOKS
    }


def test_original_profiles_exact_review_scope_and_independent_clic_reference():
    p = reference()
    assert (
        p["amount_cell_count"] == sum(len(w["changes"]) for w in p["workbooks"]) == 31
    )
    assert p["note_cell_count"] == sum(len(w["notes"]) for w in p["workbooks"]) == 15

    assert (p["CLIC_reference"]["dataset_id"], p["CLIC_reference"]["version_id"]) == (
        3709,
        5630,
    )
    assert p["CLIC_reference"]["background"]["version"] == "3.12"
    assert p["full_graph_CLIC_equivalence_claimed"] is False
    for name, version in (
        (LIS, "3.9"),
        (LIO2, "3.9"),
        (NMC, "3.8"),
        (CAPACITY, "3.10"),
    ):
        w = plan(name)
        assert (
            w["source_background"]["version"]
            == w["source_biosphere"]["version"]
            == version
        )
        assert w["formula_reference_audit"]["dependent_formula_cells"] == []


def test_central_unit_uses_2_8g_fasteners_and_retains_board_HV_LV_counts(current):
    central = activity(current[LIS].data, CENTRAL)
    assert central["unit"] == "unit" and exchange(central, CENTRAL)["amount"] == 1
    assert [e["amount"] for e in physical_materials(central)] == [
        0.49,
        2.8,
        1.2,
        0.0028,
    ]
    assert sum(e["amount"] for e in physical_materials(central)) == pytest.approx(
        NEW_MASS, rel=1e-15
    )
    assert OLD_MASS - NEW_MASS == pytest.approx(REDUCTION, abs=1e-15)
    assert science(central) == science(reference()["CLIC_reference"]["snapshot"])


def test_LiS_and_nine_NMC_retirements_follow_actual_installed_component_mass(current):
    wrapper = activity(
        current[LIS].data, "battery control and cooling system production, 16 modules"
    )
    cooling = activity(
        current[LIS].data, "battery cooling system production, liquid cooling"
    )
    material = physical_materials(cooling)
    assert len(material) == 8
    assert sum(e["amount"] for e in material) == pytest.approx(9.9996, rel=1e-15)
    assert exchange(wrapper, CENTRAL)["amount"] == 1
    assert (
        exchange(
            wrapper, "printed circuit board production, battery module control, mounted"
        )["amount"]
        == 1.1
    )
    assert -exchange(wrapper, USED)["amount"] == pytest.approx(
        NEW_MASS + 1.1 + 9.9996, rel=1e-15
    )
    assert (
        exchange(wrapper, USED)["amount"] == -15.5924
        and exchange(wrapper, USED)["amount"] != -15.6128
    )
    for a in plan(NMC)["affected_activities"]:
        r = current[NMC].data[a["index"]]
        assert r["unit"] == "unit" and exchange(r, CENTRAL)["amount"] == 1
        retained = sum(e["amount"] for e in physical_materials(r)) + NEW_MASS
        assert -exchange(r, USED)["amount"] == pytest.approx(retained, rel=1e-15)
        assert exchange(r, USED)["amount"] - exchange(a["before"], USED)[
            "amount"
        ] == pytest.approx(REDUCTION, abs=1e-15)


def test_LiO2_four_kg_allowance_and_existing_cooling_assembly_retirement_stay_fixed(
    current,
):
    parent = activity(current[LIO2].data, "battery production, Li-O2")
    auxiliary = activity(
        current[LIO2].data,
        "battery auxiliaries supply and end-of-life treatment, lithium-oxygen",
    )
    assert exchange(parent, CENTRAL)["amount"] * NEW_MASS * 333.85 == pytest.approx(
        4, rel=1e-15
    )
    assert exchange(auxiliary, CENTRAL)["amount"] * NEW_MASS == pytest.approx(
        4, rel=1e-15
    )
    assert (
        exchange(parent, "market group for electricity, medium voltage")["amount"]
        == 0.00034113790291714506
    )
    assert (
        exchange(parent, "cooling system production, for Li-O2 battery")["amount"]
        == 0.08707503369776846
    )
    assert exchange(parent, "treatment of LiO2 battery, for recycling")["amount"] == 0
    assert (
        exchange(auxiliary, "treatment of LiO2 battery, for recycling")["amount"]
        == -88.07
    )


@pytest.mark.parametrize(
    "kind,mass,energy", (("passenger car", 44.6, 1.2432), ("truck", 238, 25))
)
def test_FCEV_buffer_physical_mass_and_working_service_pairing_stay_fixed(
    current, kind, mass, energy
):
    r = activity(
        current[CAPACITY].data,
        "market for electricity storage capacity, battery, high power, fuel cell "
        + kind,
    )
    before = next(
        a["before"]
        for a in plan(CAPACITY)["affected_activities"]
        if a["before"]["name"] == r["name"]
    )
    count = exchange(r, CENTRAL)["amount"]
    assert count == exchange(before, CENTRAL)[
        "amount"
    ] and count * energy == pytest.approx(1, rel=1e-15)
    cells = exchange(r, "market for battery cell, Li-ion, NMC622")["amount"]
    assert cells * energy == pytest.approx(0.7 * mass, rel=1e-15)
    stock = [
        e
        for e in physical_materials(r)
        if e["name"] != "market for battery cell, Li-ion, NMC622"
    ]
    physical = cells + count * NEW_MASS + sum(e["amount"] for e in stock)
    assert physical * energy == pytest.approx(mass, rel=1e-15)
    assert -exchange(r, USED)["amount"] == pytest.approx(physical, rel=1e-15)
    ratio = (0.3 * mass - NEW_MASS) / (0.3 * mass - OLD_MASS)
    for e in stock:
        assert e["amount"] == pytest.approx(
            exchange(before, e["name"])["amount"] * ratio, rel=1e-14, abs=0
        )
    for metal, service in (
        ("copper, cathode", "copper"),
        ("aluminium, wrought alloy", "aluminium"),
        ("steel, low-alloyed", "steel"),
    ):
        assert (
            exchange(r, "market for " + metal)["amount"]
            == exchange(
                r, "metal working, average for " + service + " product manufacturing"
            )["amount"]
        )


def test_indirect_ASSB_counts_and_historical143_minus6_538_partition_are_not_reallocated():
    path = INVENTORY_DIR / "lci-batteries-ASSB.xlsx"
    # This entire unchanged source retains its inherited 143-6.538 partition;
    # this approval must not absorb the lighter shared controller into ASSB.

    with ZipFile(path) as archive:
        root = ET.fromstring(archive.read(SHEET))
    for address, value in (
        ("B258", 0.0563994258855426),
        ("B319", 0.030469957683857463),
        ("B380", 0.030425553240408496),
    ):
        assert float(root.find(f".//{NS}c[@r='{address}']/{NS}v").text) == value


@pytest.mark.parametrize("filename", WORKBOOKS)
def test_current_full_raw_scope_and_historical_reversal_match_independent_before(
    current, filename
):
    w, historical = validate_current_fasteners(current[filename], filename)

    restored = previous_fastener_source(deepcopy(current[filename]), filename)
    assert restored.data == historical


@pytest.mark.parametrize("filename", WORKBOOKS)
def test_exact_XML_preserves_all_outside_cells_caches_parts_and_capacity_second_sheet(
    filename,
):
    w = plan(filename)
    pin = PINS[filename]
    with ZipFile(INVENTORY_DIR / filename) as archive:
        root = previous_monitor_xml(ET.fromstring(archive.read(SHEET)), filename)
        parts = [[n, archive.read(n)] for n in sorted(archive.namelist()) if n != SHEET]
    assert len(parts) == pin["other_parts_count"]
    assert root.find(NS + "dimension").get("ref") == w["dimension"]
    changed = {c["cell"] for c in w["changes"]}
    formulas = [c for c in root.iter(NS + "c") if c.find(NS + "f") is not None]
    assert formula_caches(root)
    [
        [c.get("r"), ET.tostring(c, encoding="unicode")]
        for c in formulas
        if c.get("r") not in changed
    ]
    before_caches = {
        address: (cell, value)
        for address, (cell, value) in formula_caches(root).items()
        if address not in changed
    }
    for c in w["changes"]:
        cell = root.find(f".//{NS}c[@r='{c['cell']}']")
        assert float(cell.find(NS + "v").text) == c["after"]
        f = cell.find(NS + "f")
        assert (f.text if f is not None else None) == c["after_formula"]
    previous_fastener_xml(root, filename)
    assert {
        address: value
        for address, value in formula_caches(root).items()
        if address not in changed
    } == before_caches


def test_guard_rejects_changed_amount_uncertainty_note_and_XML_before_reversal(current):
    for problem in ("amount", "uncertainty", "note"):
        damaged = deepcopy(current[LIS])
        r = activity(damaged.data, CENTRAL)
        if problem == "amount":
            exchange(r, "IBIS fasteners production, for LiS battery")[
                "amount"
            ] += 0.0001
        elif problem == "uncertainty":
            exchange(r, "IBIS fasteners production, for LiS battery")[
                "uncertainty type"
            ] = 2
        else:
            r["comment"] += "Unapproved note"
        untouched = deepcopy(damaged.data)
        with pytest.raises(AssertionError):
            previous_fastener_source(damaged, LIS)
        assert damaged.data == untouched
    with ZipFile(INVENTORY_DIR / LIO2) as archive:
        root = ET.fromstring(archive.read(SHEET))
    root.find(f".//{NS}c[@r='B282']/{NS}f").text = "4/4.5/333.85"
    with pytest.raises(AssertionError):
        previous_fastener_xml(root, LIO2)
