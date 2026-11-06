"""Scientific reference checks for the approved delegated baseline corrections."""

from functools import lru_cache

import pytest
from bw2io import ExcelImporter
from bw2io.importers.excel import valid_first_cell

from premise.filesystem_constants import INVENTORY_DIR


@lru_cache(maxsize=None)
def inventory(filename):
    # Pure importer extraction avoids constructing a Brightway Database.
    importer = ExcelImporter.__new__(ExcelImporter)
    data = importer.extractor.extract(INVENTORY_DIR / filename)
    data = [(name, rows) for name, rows in data if valid_first_cell(name, rows)]
    importer.db_name, importer.metadata = importer.get_database(data)
    return importer.process_activities(data)


def activity(filename, name):
    (record,) = [r for r in inventory(filename) if r["name"] == name]
    return record


def feed(record, name):
    (exchange,) = [
        e
        for e in record["exchanges"]
        if e["type"] == "technosphere" and e["name"] == name
    ]
    return exchange["amount"]


@pytest.mark.parametrize(
    "filename",
    (
        "lci-batteries-NMC111-811-NCA-LFP.xlsx",
        "lci-batteries-NMC622-NMC532.xlsx",
        "lci-batteries-NMC955-LTO.xlsx",
    ),
)
def test_schmidt_pouch_keeps_source_material_rounding_and_forming(filename):
    record = activity(filename, "Multilayer pouch")
    aluminium = feed(
        record, "aluminium ingot, primary, to aluminium, wrought alloy market"
    )
    assert aluminium == 0.5
    assert feed(record, "market for sheet rolling, aluminium") == 0.5
    polymers = {
        "market for nylon 6": 0.08,
        "market for packaging film, low density polyethylene": 0.025,
        "market for polyethylene terephthalate, granulate, amorphous": 0.078,
        "market for polypropylene, granulate": 0.32,
    }
    for name, amount in polymers.items():
        assert feed(record, name) == amount
    assert aluminium + sum(polymers.values()) == pytest.approx(1.003)
    assert feed(record, "market for injection moulding") == 0.47


def test_complete_nmc532_market_and_bms_reference_ownership():
    records = inventory("lci-batteries-NMC622-NMC532.xlsx")
    (market,) = [
        r
        for r in records
        if r["name"].startswith("market for battery,") and "NMC532" in r["name"]
    ]
    (pack,) = [
        e
        for e in market["exchanges"]
        if e["type"] == "technosphere" and e["name"].startswith("battery production")
    ]
    assert pack["amount"] == 1
    (bms,) = [
        r
        for r in records
        if r["name"] == "battery management system production, for Li-ion battery"
    ]
    (output,) = [e for e in bms["exchanges"] if e["type"] == "production"]
    assert output["reference product"] == bms["reference product"]
    assert output["amount"] == 1


@pytest.mark.parametrize("chemistry", ("polymer", "oxidic", "sulfidic"))
def test_assb_passive_materials_reuse_curated_reference_at_fixed_cell_fraction(
    chemistry,
):
    record = activity(
        "lci-batteries-ASSB.xlsx",
        f"battery pack production, all-solid-state battery, {chemistry}, without central controls",
    )
    materials = {
        "market for copper, cathode": 0.0031769662225882432,
        "market for aluminium, wrought alloy": 0.18053721514900498,
        "market for polyethylene, high density, granulate": 0.0010997190770497766,
        "market for glass fibre reinforced plastic, polyester resin, hand lay-up": 0.004887640342443451,
        "market for steel, low-alloyed": 0.006231741436615401,
        "ethylene glycols production, thermal hydrolysis of ethylene oxide": 0.021688904019592815,
        "market for water, deionised": 0.021688904019592815,
    }
    for name, amount in materials.items():
        assert feed(record, name) == pytest.approx(amount, rel=1e-13)
    cell = feed(
        record, f"battery cell production, all-solid-state battery, {chemistry}"
    )
    assert cell == 0.7606889097331125
    assert cell + sum(materials.values()) == pytest.approx(1)
    assert feed(record, "market group for electricity, medium voltage") == 1


@pytest.mark.parametrize(
    "chemistry,capacity,cell_mass,delivery",
    (
        ("LFP", 14.4, 90.0, 88.36128),
        ("NMC811", 19.5, 78.34821428571428, 84.86574428571429),
    ),
)
def test_home_storage_delivery_uses_approved_manufactured_components(
    chemistry, capacity, cell_mass, delivery
):
    filename = "lci-home-batteries.xlsx"
    module = activity(
        filename,
        f"battery module production, home storage, lithium-ion, {chemistry.lower()}, {capacity} kwh",
    )
    # The NMC811 module now expands its former cell market into the approved
    # manufacturer and freight legs. The delivered cell mass stays unchanged.
    supplier = {
        "LFP": "market for battery cell, Li-ion, LFP",
        "NMC811": "battery cell production, lithium-ion, nmc811, silicon-graphite anode",
    }[chemistry]
    assert feed(module, supplier) == cell_mass
    assert feed(module, "market for transport, freight train") == 0
    assert feed(
        module, "market for transport, freight, lorry 16-32 metric ton, EURO5"
    ) == pytest.approx(cell_mass + 54.8352 + 6.3504)
    system = activity(
        filename,
        f"home electricity storage system, {capacity} kWh, {chemistry} battery",
    )
    periphery = 18.9 + 9.936 + 0.756 + 83.52 + 22.68 + 7.56
    assert 0.3 * (cell_mass + 54.8352 + 6.3504 + periphery) == pytest.approx(delivery)
    assert feed(
        system, "market for transport, freight, lorry 16-32 metric ton, EURO5"
    ) == pytest.approx(delivery)
    assert feed(system, "market for electricity, medium voltage") == 34.56


def test_legacy_cis_uses_ordinary_diode_at_unchanged_mass():
    record = activity("lci-PV.xlsx", "photovoltaic laminate, CIS, at plant")
    assert (
        feed(record, "market for diode, glass-, for through-hole mounting") == 0.00144
    )
    assert not any(
        e["name"] == "market for light emitting diode" for e in record["exchanges"]
    )
