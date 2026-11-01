"""ALIGN-018: shared PFSA science, physical adoption and import boundaries.

These checks use packaged workbooks and supplier identities only. They neither
load licensed background inventories nor build or write a Brightway database.
The frozen XML slices were captured before the approved consolidation, not
recomputed from the adoption script or its output.
"""

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
import yaml
from bw2io import ExcelImporter

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import (
    DefaultInventory,
    check_for_datasets_compliance_with_consequential_database,
    get_consequential_blacklist,
    migrate_import_db,
)

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "lci-pfsa-ew1100.xlsx"
HYDROGEN = "lci-hydrogen-electrolysis.xlsx"
VRFB = "lci-batteries-vanadium-redox-flow.xlsx"
HFB = "lci-batteries-organic-and-hybrid-redox-flow.xlsx"
FUEL_CELL = "lci-fuel_cell.xlsx"
NAME = "perfluorosulfonic acid membrane production, ew1100, engineering reference"
PRODUCT = "PFSA membrane, EW1100, dry acid form, engineering reference"
PEM_STACK = "electrolyzer production, 1MWe, PEM, Stack"
WRAPPER = "nafion membrane production"
VRFB_STACK = "vanadium-redox flow battery stack assembly"
HFB_STACK = (
    "power subsystem production, cell stack, for hybrid redox flow battery (HFB), "
    "5kW/ 40kWh"
)

OLD_NAMES = {
    "perfluorosulfonic acid membrane production, electrolysis, ew1100",
    "perfluorosulfonic acid membrane production, vanadium redox-flow battery, ew1100",
}
OLD_PRODUCTS = {
    "PFSA membrane, electrolysis, EW1100",
    "PFSA membrane, vanadium redox-flow battery, EW1100",
}
VERSIONS = tuple(
    yaml.safe_load((ROOT / "premise/iam_variables_mapping/constants.yaml").read_text())[
        "SUPPORTED_EI_VERSIONS"
    ]
)
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
# Original addresses, resolved shared strings, formula bodies and cached values.
# Only helper rows and the consumer's A/G/H cells are excluded from these pins.
ORIGINAL = {
    HYDROGEN: {
        "start": 636,
        "end": 661,
        "consumer": 175,
        "cells": 14752,
        "formulas": 484,
        "comment_length": 2922,
    },
    VRFB: {
        "start": 494,
        "end": 519,
        "consumer": 14,
        "cells": 3129,
        "formulas": 56,
        "comment_length": 1446,
    },
}


# ALIGN-022 before.xlsx (b5487ca74316126370d9580014165e11b11d9204ac880f9cc
# f1aadb25e43d45d) reproduces AEC_GLASS_UNRELATED_XML before exemptions.
# Only B862, rows 871/872 and the five exact legacy-comment replacements are
# additionally excluded. test_lanthanum_reject_fate protects all those payloads.

# ALIGN-023 before.xlsx (0d76174f4eed4b850a2e4df0e9c2519cec3f1b4dc8ddbc9e
# 9119ae81c232d0ec) reproduces the preceding pin; only four approved SiC cells
# are further excluded, with complete coverage in test_sic_full_boule.
SIC_CELLS = {"B1229", "B1234", "B1236", "H1235"}

CONTROLLER = "battery management system assembly, for VRFB system"


CONTROLLER_CHANGES = (
    (
        "B304",
        "market for polypropylene, granulate",
        0.15254237288135591,
        0.15302049244620264,
    ),
    ("B305", "market for injection moulding", 0.15254237288135591, 0.15302049244620264),
    (
        "B306",
        "market for liquid crystal display, unmounted",
        0.576271186440678,
        0.5750483805084746,
    ),
    (
        "B307",
        "market for electronics, for control units",
        0.27118644067796605,
        0.27284925,
    ),
)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def snapshot(value):
    return json.loads(json.dumps(value, sort_keys=True))


def xml_cells(path):
    """Read active worksheet cells, resolving shared strings without saving."""
    result = {}
    with ZipFile(path) as archive:
        strings = (
            ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(NS + "si")
            if "xl/sharedStrings.xml" in archive.namelist()
            else []
        )
        for sheet in archive.namelist():
            if not (sheet.startswith("xl/worksheets/sheet") and sheet.endswith(".xml")):
                continue
            for cell in ET.fromstring(archive.read(sheet)).iter(NS + "c"):
                value = cell.find(NS + "v")
                string = strings[int(value.text)] if cell.get("t") == "s" else None
                inline = cell.find(NS + "is")
                text = (
                    "".join(string.itertext())
                    if string is not None
                    else (
                        "".join(inline.itertext())
                        if inline is not None
                        else value.text if value is not None else None
                    )
                )
                payload = [
                    ET.tostring(cell, encoding="unicode"),
                    (
                        ET.tostring(string, encoding="unicode")
                        if string is not None
                        else None
                    ),
                ]
                result[sheet, cell.get("r")] = (cell, text, payload)
    return result


def activity(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def exchange_key(exc):
    categories = exc.get("categories", ())
    if isinstance(categories, str):
        categories = categories.split("::")
    return (
        exc["type"],
        exc["name"],
        exc.get("product", exc.get("reference product")),
        exc.get("location"),
        exc["unit"],
        tuple(categories),
    )


def previous_controller_cells(cells, actual):
    """Validate ALIGN-041 before reversing its five cells for the PFSA oracle."""
    approved = json.loads(
        (
            ROOT / "tests/data/inventory_review/vrfb-controller-adoption-20261027.json"
        ).read_text()
    )
    before = approved["before_snapshot"]

    assert approved["activity"] == CONTROLLER
    assert (approved["CLIC_dataset_id"], approved["CLIC_version_id"]) == (1394, 5554)
    assert approved["source_background"]["version"] == "3.9"
    assert approved["source_biosphere"]["version"] == "3.9"
    expected = deepcopy(before)
    for address, name, old, new in CONTROLLER_CHANGES:
        exc = exchange(expected, name)
        assert exc["amount"] == old
        exc["amount"] = new
    assert approved["after_snapshot"] == expected
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    # Retain production, duplicates and every supplier identity/quantity. Raw
    # fields above additionally guard aliases, uncertainty and flow comments.
    sorted(
        [
            e["type"],
            e["name"],
            e["reference product"],
            e.get("location"),
            e["unit"],
            e["amount"],
        ]
        for e in actual["exchanges"]
    )

    old_note = before["comment"]
    assert actual["comment"].endswith(old_note)
    prefix = actual["comment"][: -len(old_note)]
    assert prefix.startswith("APPROVED VRFB CONTROLLER MASS/MOULDING REFERENCE")
    for term in (
        "uncertainty are retained",
        "Unallocated native polymer-residual fate remains unknown",
        "15-inch LCD is a mass proxy for a 7-inch device",
        "13 kg parent allocation",
        "91% monitoring and 9% BMS",
        "both remain 3.9",
    ):
        assert term in prefix

    sheet = "xl/worksheets/sheet1.xml"
    addresses = ["B296", *(c[0] for c in CONTROLLER_CHANGES)]

    def payloads(view):
        return [[a, view[sheet, a][2][0]] for a in addresses]

    # This approved current pin includes the entire new note and all XML
    # attributes; restoring values cannot hide an altered style or note.

    assert cells[sheet, "B296"][1] == actual["comment"]
    previous = cells.copy()
    replacements = {"B296": old_note}
    replacements.update(
        {address: str(old) for address, _, old, _ in CONTROLLER_CHANGES}
    )
    for address, text in replacements.items():
        cell = deepcopy(cells[sheet, address][0])
        node = cell.find(f"{NS}is/{NS}t") if address == "B296" else cell.find(NS + "v")
        node.text = text
        previous[sheet, address] = (
            cell,
            text,
            [ET.tostring(cell, encoding="unicode"), None],
        )

    return previous


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (
            ROOT / "tests/data/inventory_review/pfsa-ew1100-adoption-20261027.json"
        ).read_text()
    )


@pytest.fixture(scope="module")
def catalogues():
    return {
        filename: ExcelImporter(INVENTORY_DIR / filename).data
        for filename in (WORKBOOK, HYDROGEN, VRFB, HFB, FUEL_CELL)
    }


def test_portable_fixture_pins_clic_approval_license_and_source_profiles(adoption):

    assert adoption["CLIC_dataset_id"] == 3642
    assert adoption["CLIC_version_id"] == 4564
    assert adoption["approval"] == "ALIGN-018; approved by Romain Sacchi, 2026-10-27"
    assert adoption["license"] == "CC-BY-NC-4.0"
    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["consumer_membrane_kg"] == {
        "PEM_electrolyser_stack": 16,
        "vanadium_flow_battery_stack": 150,
        "hybrid_flow_battery_stack": 0.132,
    }


def test_shared_workbook_matches_all_thirteen_approved_exchanges(catalogues, adoption):
    (actual,) = catalogues[WORKBOOK]
    snapshot = adoption["snapshot"]
    for field in ("name", "reference product", "location", "unit", "type"):
        assert actual[field] == snapshot[field]
    assert (
        actual["name"],
        actual["reference product"],
        actual["location"],
        actual["unit"],
    ) == (NAME, PRODUCT, "GLO", "kilogram")
    assert actual["production amount"] == 1
    assert actual["license"] == adoption["license"] == "CC-BY-NC-4.0"
    assert actual["background version"] == actual["biosphere version"] == "3.9"
    assert snapshot["comment"] in actual["comment"]
    assert snapshot["source"] in actual["source"]
    expected = {exchange_key(e): e["amount"] for e in snapshot["exchanges"]}
    observed = {exchange_key(e): e["amount"] for e in actual["exchanges"]}
    assert len(actual["exchanges"]) == len(observed) == len(expected) == 13
    assert observed == expected
    assert Counter(e["type"] for e in actual["exchanges"]) == {
        "production": 1,
        "technosphere": 11,
        "biosphere": 1,
    }
    for exc in actual["exchanges"]:
        if exc["type"] != "biosphere":
            assert (
                exc.get("product", exc["reference product"]) == exc["reference product"]
            )
        assert not exc.get("formula")
        assert not any(k.startswith("replacement ") for k in exc)


def test_unique_shared_producer_and_no_active_legacy_helpers_anywhere():
    producers, consumers = [], []
    for path in sorted(INVENTORY_DIR.glob("*.xlsx")):
        cells = xml_cells(path)
        for (sheet, address), (_, text, _) in cells.items():
            assert text not in OLD_NAMES | OLD_PRODUCTS, (path.name, address, text)
            if text == "Activity":
                name = cells.get((sheet, "B" + address[1:]), (None, None, None))[1]
                if name == NAME:
                    producers.append(path.name)
            if text == NAME and address.startswith("A"):
                row = address[1:]
                if (
                    cells.get((sheet, "F" + row), (None, None, None))[1]
                    == "technosphere"
                ):
                    consumers.append((path.name, address))
    assert producers == [WORKBOOK]
    assert sorted(consumers) == sorted([(HYDROGEN, "A175"), (VRFB, "A14")])


@pytest.mark.parametrize("filename", (HYDROGEN, VRFB))
def test_physical_deletion_preserves_all_other_addresses_payloads_and_caches(
    filename, catalogues
):
    baseline = ORIGINAL[filename]
    cells = xml_cells(INVENTORY_DIR / filename)
    if filename == VRFB:
        cells = previous_controller_cells(cells, activity(catalogues[VRFB], CONTROLLER))
    outside, formulas = [], []
    consumer = baseline["consumer"]
    for (sheet, address), (cell, text, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        # Even sparse/styled cells must be physically removed in these rows.
        assert not baseline["start"] <= row < baseline["end"]
        aec_adoption = (
            filename == HYDROGEN
            and sheet == "xl/worksheets/sheet1.xml"
            and (address in {"B907", "B917"} or row == 921)
        )
        glass_adoption = (
            filename == HYDROGEN
            and sheet == "xl/worksheets/sheet1.xml"
            and address in {"B667", "A678", "G678", "I678"}
        )
        la_adoption = (
            filename == HYDROGEN
            and sheet == "xl/worksheets/sheet1.xml"
            and (
                address in {"B862", "B466", "H472", "H473", "H474", "B881"}
                or row in {871, 872}
            )
        )
        sic_adoption = (
            filename == HYDROGEN
            and sheet == "xl/worksheets/sheet1.xml"
            and address in SIC_CELLS
        )
        if not (
            aec_adoption or glass_adoption or la_adoption or sic_adoption
        ) and address not in {
            f"A{consumer}",
            f"G{consumer}",
            f"H{consumer}",
        }:
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert cell.find(NS + "v") is not None
            assert cell.find(NS + "v").text is not None
            formulas.append([sheet, address, payload])
        if address == f"A{consumer}":
            assert text == NAME
        elif address == f"G{consumer}":
            assert text == PRODUCT
        elif address == f"H{consumer}":
            assert len(text) > baseline["comment_length"]
            text[-baseline["comment_length"] :]

    assert cells


def test_controller_historical_view_rejects_unapproved_science_and_note(catalogues):
    cells = xml_cells(INVENTORY_DIR / VRFB)
    actual = activity(catalogues[VRFB], CONTROLLER)
    damaged = deepcopy(actual)
    exchange(damaged, "market for polypropylene, granulate")["amount"] += 1e-6
    with pytest.raises(AssertionError):
        previous_controller_cells(cells, damaged)
    damaged_cells = deepcopy(cells)
    sheet = "xl/worksheets/sheet1.xml"
    cell, text, payload = damaged_cells[sheet, "B296"]
    cell.find(f"{NS}is/{NS}t").text = "Altered note"
    damaged_cells[sheet, "B296"] = (
        cell,
        "Altered note",
        [ET.tostring(cell, encoding="unicode"), payload[1]],
    )
    with pytest.raises(AssertionError):
        previous_controller_cells(damaged_cells, actual)
    assert cells[sheet, "B296"][1] == actual["comment"]


def test_consumers_keep_finished_mass_and_wrapper_factory_and_freight(catalogues):
    assert len(catalogues[HYDROGEN]) == 60
    assert len(catalogues[VRFB]) == 30
    for filename, name, amount in ((HYDROGEN, PEM_STACK, 16), (VRFB, WRAPPER, 1)):
        exc = exchange(activity(catalogues[filename], name), NAME)
        assert exchange_key(exc) == (
            "technosphere",
            NAME,
            PRODUCT,
            "GLO",
            "kilogram",
            (),
        )
        assert exc["amount"] == amount
    wrapper = activity(catalogues[VRFB], WRAPPER)
    assert (wrapper["reference product"], wrapper["location"], wrapper["unit"]) == (
        "nafion membrane",
        "RER",
        "kilogram",
    )
    assert len(wrapper["exchanges"]) == 5
    for name, amount, location, unit in (
        (WRAPPER, 1, "RER", "kilogram"),
        ("chemical factory construction, organics", 4e-10, "RER", "unit"),
        (
            "market for transport, freight, lorry, unspecified",
            0.87,
            "RER",
            "ton kilometer",
        ),
        (
            "market for transport, freight train",
            5.22,
            "Europe without Switzerland",
            "ton kilometer",
        ),
    ):
        exc = exchange(wrapper, name)
        assert (exc["amount"], exc["location"], exc["unit"]) == (amount, location, unit)
        assert exc["type"] == ("production" if name == WRAPPER else "technosphere")
    for filename, name, amount in ((VRFB, VRFB_STACK, 150), (HFB, HFB_STACK, 0.132)):
        exc = exchange(activity(catalogues[filename], name), WRAPPER)
        assert exchange_key(exc) == (
            "technosphere",
            WRAPPER,
            "nafion membrane",
            "RER",
            "kilogram",
            (),
        )
        assert exc["amount"] == amount
        assert not any(
            e["name"] == NAME for e in activity(catalogues[filename], name)["exchanges"]
        )


def test_fuel_cell_membrane_recipe_and_its_consumers_remain_unchanged(catalogues):

    membrane = activity(
        catalogues[FUEL_CELL], "polymer electrolyte membrane (Nafion) production"
    )
    assert len(membrane["exchanges"]) == 17
    assert (
        exchange(membrane, "market for tetrafluoroethylene film, on glass")["amount"]
        == 0.93
    )
    assert not any(
        e["name"] == NAME for d in catalogues[FUEL_CELL] for e in d["exchanges"]
    )


def supplier_identities(snapshot, version, system_model):
    # Migrate only identities to supply product lookup fields. These empty
    # supplier bodies establish preparation, not successful native linking.
    holder = SimpleNamespace(data=[deepcopy(snapshot)])
    migrate_import_db(holder, "3.9", version)
    if system_model == "consequential":
        holder.data = check_for_datasets_compliance_with_consequential_database(
            holder.data, get_consequential_blacklist(), version
        )
    return [
        {
            "name": e["name"],
            "reference product": e["reference product"],
            "location": e["location"],
            "unit": e["unit"],
            "exchanges": [],
        }
        for e in holder.data[0]["exchanges"]
        if e["type"] == "technosphere"
    ]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_migrates_39_reference_for_every_target(
    adoption, version, system_model
):
    background = supplier_identities(adoption["snapshot"], version, system_model)
    before = deepcopy(background)
    importer = DefaultInventory(
        background, "3.9", version, INVENTORY_DIR / WORKBOOK, system_model, True
    )
    importer.prepare_inventory()
    assert importer.version_in == "3.9"
    assert importer.version_out == version
    assert importer.list_unlinked == []
    assert background == before
    (prepared,) = importer.import_db.data
    assert (
        prepared["name"],
        prepared["reference product"],
        prepared["location"],
        prepared["unit"],
    ) == (NAME, PRODUCT, "GLO", "kilogram")
    # Normal migrations split NaOH geographically from 3.10, waste treatment
    # from 3.11, and NaOH again in 3.12. The source recipe remains 13 flows;
    # migrated regional shares must conserve each signed physical purchase.
    assert len(prepared["exchanges"]) == {
        "3.10": 14,
        "3.10.1": 14,
        "3.11": 15,
        "3.12": 16,
    }.get(version, 13)

    def totals_without_location(exchanges):
        totals = {}
        for exc in exchanges:
            key = exchange_key(exc)
            key = (*key[:3], *key[4:])
            totals[key] = totals.get(key, 0) + exc["amount"]
        return totals

    expected = totals_without_location(adoption["snapshot"]["exchanges"])
    actual = totals_without_location(prepared["exchanges"])
    assert actual.keys() == expected.keys()
    for key, amount in expected.items():
        assert actual[key] == pytest.approx(amount, rel=1e-14, abs=1e-15), key
    assert exchange(prepared, NAME)["amount"] == 1
    carbon = exchange(prepared, "Carbon dioxide, fossil")
    assert carbon["type"] == "biosphere"
    assert carbon["amount"] == 0.08890707070707071
    assert carbon["categories"] == ("air", "non-urban air or from high stacks")
    assert carbon["input"][1] == "aa7cac3a-3625-41d4-bc54-33e2cf11ec46"


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_actual_default_caller_registers_39_shared_source_before_both_buyers(
    version, system_model, monkeypatch
):
    import premise.new_database as module

    # These mock payloads exercise registry ordering/the sector hook below,
    # not unrelated native-selection guards with their own dedicated tests.
    for hook in (
        "_select_nmc955_modern_references",
        "_select_nmc532_native_312",
        "use_native_pipeline_coating",
        "use_native_ptfe_gasket",
        "use_native_fkm_gasket",
    ):
        monkeypatch.setattr(module, hook, lambda *args, **kwargs: None)

    selected, observed = [], []

    def extract(filepaths):
        selected.extend((Path(path).name, source) for path, source in filepaths)
        return [SimpleNamespace(data=[]) for _ in filepaths]

    class ObserveInventory:
        def __init__(self, **kwargs):
            assert kwargs["version_out"] == version
            assert kwargs["system_model"] == system_model
            observed.append((Path(kwargs["path"]).name, kwargs["version_in"]))
            self.list_unlinked = []

        def merge_inventory(self):
            return []

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract)
    monkeypatch.setattr(module, "DefaultInventory", ObserveInventory)
    # PTFE's real hook is covered separately against real Excel extraction.
    monkeypatch.setattr(module, "use_native_ptfe_gasket", lambda *args: None)
    monkeypatch.setattr(module, "use_native_fkm_gasket", lambda *args, **kwargs: None)
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, system_model
    obj.keep_imports_uncertainty = True
    obj._database = []
    assert obj._NewDatabase__import_inventories() == []
    assert selected == observed
    assert module.FILEPATH_PFSA_EW1100 == INVENTORY_DIR / WORKBOOK
    for filename in (WORKBOOK, HYDROGEN, VRFB):
        assert observed.count((filename, "3.9")) == 1
    assert observed[observed.index((VRFB, "3.9")) - 1] == (WORKBOOK, "3.9")
    assert observed.index((WORKBOOK, "3.9")) < observed.index((HYDROGEN, "3.9"))


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize(
    "constant",
    (
        "FILEPATH_PFSA_EW1100",
        "FILEPATH_HYDROGEN_INVENTORIES",
        "FILEPATH_VANADIUM_REDOX_BATTERY",
    ),
)
def test_shared_and_buyer_bytes_invalidate_import_cache_only(
    version, constant, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    original_source = obj._database_cache_path("ALIGN018")
    original_imports = obj._database_cache_path("ALIGN018", inventories=True)
    path = Path(getattr(module, constant))
    original_bytes = path.read_bytes()
    copy = tmp_path / path.name
    copy.write_bytes(original_bytes)
    monkeypatch.setattr(module, constant, copy)
    assert obj._database_cache_path("ALIGN018", inventories=True) == original_imports
    copy.write_bytes(original_bytes + b"changed approved PFSA supply")
    assert obj._database_cache_path("ALIGN018", inventories=True) != original_imports
    assert obj._database_cache_path("ALIGN018") == original_source
    copy.write_bytes(original_bytes)
    assert obj._database_cache_path("ALIGN018", inventories=True) == original_imports
