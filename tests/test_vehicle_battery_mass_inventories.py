"""Approved CLIC mass purchases and their exact pre-migration capacity boundary.

Reference quantities and source fingerprints are pinned to the accepted read-only
2026-10-27 audit, independently of the migration implementation. No database access.
"""

import json
from collections import Counter
from copy import deepcopy

import pytest
from bw2io import ExcelImporter

from premise.battery_mass import (
    MASS_PROVIDER,
    RETIREMENT_PROVIDER,
    SPEC_KEY,
    STATE_KEY,
    convert_battery_mass_inputs,
)
from premise.filesystem_constants import INVENTORY_DIR

EXPECTED = [
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Large, EURO-6d",
        77.32118739836278,
        12.0,
        -77.32118739836278,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Large, EURO-6d",
        77.32118739836278,
        12.0,
        -77.32118739836278,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Large",
        65.5766360969924,
        1.827911972999573,
        -65.57663609699242,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Large",
        627.8480082462546,
        97.43999481201172,
        -627.8480082462546,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Large, EURO-6d",
        35.7223895366536,
        5.544000148773193,
        -35.7223895366536,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Large, EURO-6d",
        35.7223895366536,
        5.544000148773193,
        -35.7223895366536,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Large SUV, EURO-6d",
        154.64237479672556,
        24.0,
        -154.64237479672556,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Large SUV, EURO-6d",
        154.64237479672556,
        24.0,
        -154.64237479672556,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Large SUV",
        72.20450223949732,
        2.012660026550293,
        -72.20450223949733,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Large SUV",
        714.447753863457,
        110.879997253418,
        -714.4477538634569,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Large SUV, EURO-6d",
        40.0523725160586,
        6.215999603271484,
        -40.0523725160586,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Large SUV, EURO-6d",
        40.0523725160586,
        6.215999603271484,
        -40.0523725160586,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Lower medium, EURO-6d",
        83.76461968155968,
        13.0,
        -83.76461968155968,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Lower medium, EURO-6d",
        83.76461968155968,
        13.0,
        -83.76461968155968,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Lower medium",
        42.292662760772615,
        1.178884267807007,
        -42.292662760772615,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Lower medium",
        389.6987815381792,
        60.47999954223633,
        -389.6987815381792,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Lower medium, EURO-6d",
        21.649931795598633,
        3.359999895095825,
        -21.649931795598633,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Lower medium, EURO-6d",
        21.649931795598633,
        3.359999895095825,
        -21.649931795598633,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Medium, EURO-6d",
        77.32118739836278,
        12.0,
        -77.32118739836278,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Medium, EURO-6d",
        77.32118739836278,
        12.0,
        -77.32118739836278,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Medium",
        57.31315995198574,
        1.597572207450867,
        -57.31315995198573,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Medium",
        432.9986297670368,
        67.19999694824219,
        -432.99862976703685,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Medium, EURO-6d",
        24.8974228707373,
        3.864000082015991,
        -24.8974228707373,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Medium, EURO-6d",
        24.8974228707373,
        3.864000082015991,
        -24.8974228707373,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Medium SUV, EURO-6d",
        116.62612678383825,
        18.10000038146973,
        -116.62612678383823,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Medium SUV, EURO-6d",
        116.62612678383825,
        18.10000038146973,
        -116.62612678383823,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Medium SUV",
        48.394663165895174,
        1.348974108695984,
        -48.39466316589518,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Medium SUV",
        497.9484389799384,
        77.27999877929688,
        -497.9484389799384,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Medium SUV, EURO-6d",
        24.8974228707373,
        3.864000082015991,
        -24.8974228707373,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Medium SUV, EURO-6d",
        24.8974228707373,
        3.864000082015991,
        -24.8974228707373,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Micro",
        64.94979692302984,
        10.07999992370605,
        -64.94979692302984,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Mini, EURO-6d",
        56.702205321119884,
        8.800000190734863,
        -56.70220532111989,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Mini, EURO-6d",
        56.702205321119884,
        8.800000190734863,
        -56.70220532111989,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Mini",
        23.531604342479042,
        0.6559302806854248,
        -23.531604342479042,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Mini",
        205.6743589712399,
        31.92000007629395,
        -205.67435897123988,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Mini, EURO-6d",
        12.989958155618801,
        2.015999794006348,
        -12.989958155618801,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Mini, EURO-6d",
        12.989958155618801,
        2.015999794006348,
        -12.989958155618801,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Small, EURO-6d",
        56.702205321119884,
        8.800000190734863,
        -56.70220532111989,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Small, EURO-6d",
        56.702205321119884,
        8.800000190734863,
        -56.70220532111989,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Small",
        33.51061257079454,
        0.9340895414352417,
        -33.51061257079454,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Small",
        313.9240041231273,
        48.71999740600586,
        -313.9240041231273,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Small, EURO-6d",
        20.5674352826304,
        3.191999912261963,
        -20.5674352826304,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Small, EURO-6d",
        20.5674352826304,
        3.191999912261963,
        -20.5674352826304,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin gasoline hybrid, Van, EURO-6d",
        87.6306815094522,
        13.60000038146973,
        -87.6306815094522,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, plugin diesel hybrid, Van, EURO-6d",
        87.6306815094522,
        13.60000038146973,
        -87.6306815094522,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, fuel cell electric, Van",
        47.49666451640722,
        1.323942899703979,
        -47.49666451640722,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car",
        "electricity storage capacity, high power, fuel cell passenger car",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, battery electric, Van",
        627.8480082462546,
        97.43999481201172,
        -627.8480082462546,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, gasoline hybrid, Van, EURO-6d",
        35.7223895366536,
        5.544000148773193,
        -35.7223895366536,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-pass_cars.xlsx",
        "Passenger car, diesel hybrid, Van, EURO-6d",
        35.7223895366536,
        5.544000148773193,
        -35.7223895366536,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, battery electric, 18 metric ton",
        2146.231233974563,
        333.0881958007812,
        -2146.231233974563,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, fuel cell electric, 18 metric ton",
        496.1770877075195,
        52.11944198608398,
        -496.17708770751955,
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, diesel hybrid, 18 metric ton",
        56.67531508171781,
        8.79582691192627,
        -56.67531508171781,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, plugin diesel hybrid, 18 metric ton",
        301.54006568873154,
        46.79804992675781,
        -301.5400656887315,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, battery electric, 26 metric ton",
        2613.6138770266243,
        405.6244812011719,
        -2613.6138770266243,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, fuel cell electric, 26 metric ton",
        570.4802911376951,
        59.92440032958984,
        -570.4802911376952,
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, diesel hybrid, 26 metric ton",
        70.47939917457532,
        10.93817643717163,
        -70.47939917457532,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, plugin diesel hybrid, 26 metric ton",
        366.90586248082946,
        56.94261169433594,
        -366.90586248082946,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, battery electric, 3.5 metric ton",
        862.1541674764932,
        133.8035583496094,
        -862.1541674764933,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, fuel cell electric, 3.5 metric ton",
        312.7966278076172,
        32.85678863525391,
        -312.79662780761726,
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, diesel hybrid, 3.5 metric ton",
        24.14936756539649,
        3.747904300689697,
        -24.14936756539649,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, plugin diesel hybrid, 3.5 metric ton",
        121.78337482828528,
        18.90038871765137,
        -121.78337482828528,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, battery electric, 32 metric ton",
        3338.9682038406913,
        518.1971435546875,
        -3338.968203840691,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, fuel cell electric, 32 metric ton",
        835.546109008789,
        87.76744842529297,
        -835.546109008789,
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, diesel hybrid, 32 metric ton",
        90.40629026151109,
        14.03076594709802,
        -90.40629026151109,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, plugin diesel hybrid, 32 metric ton",
        467.90078404529277,
        72.61669921875,
        -467.90078404529277,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, battery electric, 40 metric ton",
        3549.461261759116,
        550.864990234375,
        -3549.461261759116,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, fuel cell electric, 40 metric ton",
        707.453837890625,
        74.3123779296875,
        -707.4538378906249,
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, diesel hybrid, 40 metric ton",
        104.1345861971528,
        16.16135339370504,
        -104.13458619715281,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, plugin diesel hybrid, 40 metric ton",
        498.22722243085093,
        77.3232650756836,
        -498.22722243085093,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, battery electric, 60 metric ton",
        4672.943919409556,
        725.2258911132812,
        -4672.943919409556,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, fuel cell electric, 60 metric ton",
        909.8088201904296,
        95.56815338134766,
        -909.8088201904296,
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, diesel hybrid, 60 metric ton",
        135.89146015962837,
        21.08991825893855,
        -135.89146015962837,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, plugin diesel hybrid, 60 metric ton",
        660.339725397268,
        102.4826049804688,
        -660.339725397268,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, battery electric, 7.5 metric ton",
        1309.5199522831874,
        203.2332916259766,
        -1309.5199522831876,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, fuel cell electric, 7.5 metric ton",
        469.04837066650384,
        49.2697868347168,
        -469.0483706665039,
        "market for electricity storage capacity, battery, high power, fuel cell truck",
        "electricity storage capacity, high power, fuel cell truck",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, diesel hybrid, 7.5 metric ton",
        26.222248002521923,
        4.069608688354492,
        -26.222248002521923,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
    (
        "lci-trucks.xlsx",
        "lorry production, plugin diesel hybrid, 7.5 metric ton",
        184.36263983766378,
        28.61248970031738,
        -184.36263983766375,
        "market for battery capacity (MIX scenario)",
        "electricity storage capacity",
    ),
]

FILENAMES = ("lci-pass_cars.xlsx", "lci-trucks.xlsx")


def matches(exchange, provider):
    return exchange.get("type") == "technosphere" and all(
        exchange.get(k, exchange.get("product") if k == "reference product" else None)
        == v
        for k, v in provider.items()
    )


@pytest.fixture(scope="module")
def inventories():
    return {
        filename: ExcelImporter(str(INVENTORY_DIR / filename)).data
        for filename in FILENAMES
    }


@pytest.mark.parametrize("expected", EXPECTED, ids=[r[1] for r in EXPECTED])
def test_exact_approved_mass_retirement_and_current_supplier(inventories, expected):
    filename, name, mass, capacity, retirement, supplier_name, product = expected
    dataset = next(d for d in inventories[filename] if d["name"] == name)
    contract = json.loads(dataset[SPEC_KEY])
    assert contract == {
        "version": 1,
        "vehicle family": "car" if filename == "lci-pass_cars.xlsx" else "truck",
        "baseline mass kg": mass,
        "baseline capacity kWh": capacity,
        "retirement amount kg": retirement,
        "capacity supplier": {
            "name": supplier_name,
            "reference product": product,
            "location": "GLO",
            "unit": "kilowatt hour",
        },
        "capacity supplier includes end of life": True,
        "mass supplier": MASS_PROVIDER,
        "retirement supplier": RETIREMENT_PROVIDER,
    }
    purchases = [e for e in dataset["exchanges"] if matches(e, MASS_PROVIDER)]
    retirements = [e for e in dataset["exchanges"] if matches(e, RETIREMENT_PROVIDER)]
    assert len(purchases) == len(retirements) == 1
    assert purchases[0]["amount"] == mass
    assert retirements[0]["amount"] == retirement
    assert retirement == pytest.approx(-mass, rel=1e-14)
    assert not any(
        e["type"] == "technosphere"
        and e["unit"] == "kilowatt hour"
        and "storage capacity" in e.get("reference product", "")
        for e in dataset["exchanges"]
    )
    assert (
        "AUTHORITATIVE STATIC BATTERY / PREMISE RUNTIME BOUNDARY" in dataset["comment"]
    )
    assert "CURRENT capacity supplier" in dataset["comment"]
    assert "Historical capacity exchange comment:" in purchases[0]["comment"]


@pytest.mark.parametrize("filename", FILENAMES)
def test_only_declared_vehicle_inventories_carry_mass_conversion_contracts(
    inventories, filename
):
    targets = {row[1] for row in EXPECTED if row[0] == filename}
    assert {
        record["name"] for record in inventories[filename] if SPEC_KEY in record
    } == targets


def test_current_scope_and_physical_sizing_remain_distinct(inventories):
    marked = [d for group in inventories.values() for d in group if SPEC_KEY in d]
    suppliers = Counter(
        json.loads(d[SPEC_KEY])["capacity supplier"]["name"] for d in marked
    )
    assert suppliers == {
        "market for battery capacity (MIX scenario)": 62,
        "market for electricity storage capacity, battery, high power, fuel cell passenger car": 8,
        "market for electricity storage capacity, battery, high power, fuel cell truck": 7,
    }
    assert len(marked) == 77
    assert not any("EURO-6ab" in d["name"] for d in marked)
    physical_sizing_count = 0
    for dataset in marked:
        contract = json.loads(dataset[SPEC_KEY])
        provenance = json.loads(dataset["vehicle inventory provenance"])
        parameters = provenance["baseline vehicle parameters"]
        assert contract["baseline capacity kWh"] == pytest.approx(
            parameters["electric energy stored"]
            * (1 + parameters["battery lifetime replacements"]),
            rel=2e-6,
        )
        if "vehicle battery sizing" in dataset:
            physical_sizing_count += 1
            sizing = json.loads(dataset["vehicle battery sizing"])
            assert sizing["pack mass kg"] == parameters["energy battery mass"]
            assert sizing["capacity kWh"] == parameters["electric energy stored"]
    assert physical_sizing_count == 39
    phev = next(
        d
        for d in inventories["lci-pass_cars.xlsx"]
        if d["name"] == "Passenger car, plugin gasoline hybrid, Large, EURO-6d"
    )
    assert json.loads(phev["vehicle battery sizing"])["pack mass kg"] == 118.5
    assert json.loads(phev[SPEC_KEY])["baseline mass kg"] == 77.32118739836278
    assert json.loads(phev[SPEC_KEY])["baseline capacity kWh"] == 12


@pytest.mark.parametrize("family,count", [("car", 49), ("truck", 28)])
def test_actual_imported_packs_restore_exact_capacity_and_are_idempotent(
    inventories, family, count
):
    database = deepcopy([d for group in inventories.values() for d in group])
    original = deepcopy(database)
    providers = {}
    for dataset in database:
        if SPEC_KEY in dataset:
            supplier = json.loads(dataset[SPEC_KEY])["capacity supplier"]
            providers[supplier["name"]] = supplier
    for supplier in providers.values():
        database.append(
            {**supplier, "exchanges": [{**supplier, "type": "production", "amount": 1}]}
        )
    assert convert_battery_mass_inputs(database, family) == count
    for before, after in zip(original, database):
        if (
            SPEC_KEY not in before
            or json.loads(before[SPEC_KEY])["vehicle family"] != family
        ):
            assert after == before
            continue
        spec = json.loads(before[SPEC_KEY])
        assert {
            k: v for k, v in after.items() if k not in (STATE_KEY, "exchanges")
        } == {k: v for k, v in before.items() if k != "exchanges"}
        capacity = [
            e for e in after["exchanges"] if matches(e, spec["capacity supplier"])
        ]
        assert len(capacity) == 1
        assert capacity[0]["amount"] == spec["baseline capacity kWh"]
        assert not any(
            matches(e, MASS_PROVIDER) or matches(e, RETIREMENT_PROVIDER)
            for e in after["exchanges"]
        )
        assert [
            e for e in after["exchanges"] if not matches(e, spec["capacity supplier"])
        ] == [
            e
            for e in before["exchanges"]
            if not matches(e, MASS_PROVIDER) and not matches(e, RETIREMENT_PROVIDER)
        ]
        assert all(
            k not in capacity[0] for k in ("input", "output", "database", "code")
        )
    converted = deepcopy(database)
    assert convert_battery_mass_inputs(database, family) == 0
    assert database == converted


def test_repository_adoption_plan_reproduces_all_approved_inputs():
    """Future migration/check runs need only this checkout, not task artifacts."""
    import runpy
    from pathlib import Path

    repository = Path(__file__).resolve().parents[1]
    migration = runpy.run_path(
        str(repository / "scripts/align_vehicle_battery_mass_inputs.py")
    )
    plan_path = migration["DEFAULT_PLAN"]
    assert plan_path.is_relative_to(repository)
    rows = migration["load_plan"](plan_path)
    actual = []
    for r in rows:
        c = r["contract"]
        actual.append(
            (
                r["workbook"],
                r["identity"]["name"],
                c["baseline mass kg"],
                c["baseline capacity kWh"],
                c["retirement amount kg"],
                c["capacity supplier"]["name"],
                c["capacity supplier"]["reference product"],
            )
        )
    assert actual == EXPECTED
