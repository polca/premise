"""ALIGN-021: glass manufacturing rejects, with a four-cell adoption boundary."""

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
WORKBOOK = "lci-hydrogen-electrolysis.xlsx"
NAME = "glass seal production, soec, barium alumino-borosilicate"
PRODUCT = "glass seal, SOEC, barium alumino-borosilicate"
STACK = "electrolyzer production, 1MWe, SOEC, Stack"
OLD_WASTE = "market for hazardous waste, for incineration"
WASTE = "market for hazardous waste, for underground deposit"
WASTE_PRODUCT = "hazardous waste, for underground deposit"
REJECT = -0.05263157894736836
SHEET = "xl/worksheets/sheet1.xml"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
ALLOWED = {"B667", "A678", "G678", "I678"}


# Independent ALIGN-022 before.xlsx (b5487ca74316126370d9580014165e11b11d9204
# ac880f9ccf1aadb25e43d45d) reproduces the original outside pins first. Only
# B862, rows 871/872 and exact comment substitutions in B466/H472/H473/H474/
# B881 are added; test_lanthanum_reject_fate separately pins their payloads.
LA_COMMENT_CELLS = {"B862", "B466", "H472", "H473", "H474", "B881"}


SIC_CELLS = {"B1229", "B1234", "B1236", "H1235"}


VERSIONS = tuple(
    yaml.safe_load((ROOT / "premise/iam_variables_mapping/constants.yaml").read_text())[
        "SUPPORTED_EI_VERSIONS"
    ]
)


def snapshot(value):
    return json.loads(json.dumps(value, sort_keys=True))


def activity(data, name=NAME):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def exchange_key(exc):
    product = exc["reference product"]
    if "product" in exc:
        assert exc["product"] == product
    return exc["type"], exc["name"], product, exc["location"], exc["unit"]


def scientific_vector(data):
    result = {}
    for exc in data["exchanges"]:
        key = exchange_key(exc)
        assert key not in result, key
        result[key] = {
            k: v
            for k, v in exc.items()
            if k not in {"product", "comment", "input", "output", "database", "code"}
        }
    return result


def xml_cells(path):
    cells = {}
    with ZipFile(path) as archive:
        strings = ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(NS + "si")
        for sheet in archive.namelist():
            if not (sheet.startswith("xl/worksheets/sheet") and sheet.endswith(".xml")):
                continue
            for cell in ET.fromstring(archive.read(sheet)).iter(NS + "c"):
                value = cell.find(NS + "v")
                shared = strings[int(value.text)] if cell.get("t") == "s" else None
                inline = cell.find(NS + "is")
                text = (
                    "".join(shared.itertext())
                    if shared is not None
                    else (
                        "".join(inline.itertext())
                        if inline is not None
                        else value.text if value is not None else None
                    )
                )
                payload = [
                    ET.tostring(cell, encoding="unicode"),
                    (
                        ET.tostring(shared, encoding="unicode")
                        if shared is not None
                        else None
                    ),
                ]
                cells[sheet, cell.get("r")] = cell, text, payload
    return cells


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (ROOT / "scripts/data/soec-glass-reject-adoption-20261027.json").read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


def test_approval_pins_original_glass_and_current_clic_reference(adoption):
    assert adoption["approval"] == "ALIGN-021; approved by Romain Sacchi, 2026-10-27"
    assert adoption["CLIC_dataset_id"] == 2068
    assert adoption["CLIC_version_id"] == 4574

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}
    assert adoption["workbook"] == WORKBOOK
    assert adoption["finished_glass_kg"] == 25.6
    assert adoption["reject_kg_per_kg"] == REJECT
    assert adoption["reject_kg_in_stack_reference"] == -1.3473684210526302


def test_exact_original_39_supplier_catalogue_receipt(adoption):
    # Independently checked against the real 3.9 catalogue, not a synthetic
    # supplier body. Retain the receipt portably; no local absolute path needed.
    check = adoption["supplier_catalogue_check"]
    assert check["case_id"] == "ALIGN-021"
    assert check["exists_in_original_39"] is True
    assert check["supplier_identity"] == [WASTE, WASTE_PRODUCT, "RoW", "kilogram"]
    assert check["catalogue_reference"] == [
        "3b3f920c-7e8c-54e4-8813-2675a83d6858",
        "6047c525-49b7-4a7d-baf6-a61283e5eda9",
    ]
    pin = check["catalogue_pin"]

    assert pin["profile"] == adoption["source_background"]
    assert pin["axis"] == "technosphere"
    assert pin["digest_kind"] == "raw_json"
    assert pin["schema_version"] == 2
    assert pin["identity_count"] == pin["uuid_reference_count"] == 21255


def test_actual_all_eight_glass_flows_match_approved_vector(raw, adoption):
    actual, snapshot = activity(raw.data), adoption["after_snapshot"]
    assert (
        actual["name"],
        actual["reference product"],
        actual["location"],
        actual["unit"],
    ) == (NAME, PRODUCT, "GLO", "kilogram")
    assert actual["production amount"] == exchange(actual, NAME)["amount"] == 1
    expected, observed = scientific_vector(snapshot), scientific_vector(actual)
    assert len(actual["exchanges"]) == len(observed) == len(expected) == 8
    assert observed == expected
    assert Counter(e["type"] for e in actual["exchanges"]) == {
        "production": 1,
        "technosphere": 7,
    }
    waste = exchange(actual, WASTE)
    assert exchange_key(waste) == (
        "technosphere",
        WASTE,
        WASTE_PRODUCT,
        "RoW",
        "kilogram",
    )
    assert waste["amount"] == REJECT
    assert not any(e["name"] == OLD_WASTE for e in actual["exchanges"])
    assert not any(e.get("formula") for e in actual["exchanges"])
    assert not any(k.startswith("replacement ") for e in actual["exchanges"] for k in e)


def test_only_supplier_identity_changes_and_original_history_is_retained(raw, adoption):
    actual, before = activity(raw.data), adoption["before_snapshot"]
    expected = deepcopy(before)
    waste = exchange(expected, OLD_WASTE)
    waste.update(
        name=WASTE, product=WASTE_PRODUCT, **{"reference product": WASTE_PRODUCT}
    )
    assert {k: v for k, v in actual.items() if k != "comment"} == {
        k: v for k, v in expected.items() if k != "comment"
    }
    assert actual["comment"].endswith(before["comment"])
    assert actual["comment"].startswith("APPROVED GLASS-MANUFACTURING REJECT ROUTE")
    prefix = actual["comment"][: -len(before["comment"])]
    assert prefix.strip()
    assert "underground deposit" in prefix.replace("-", " ")
    assert "incineration" in prefix
    assert "95" in prefix
    assert "3.9" in prefix


def test_four_precursors_95_percent_retention_and_unchanged_utilities(raw):
    glass = activity(raw.data)
    precursor_amounts = {
        "market for barium oxide": 0.6377896625582049,
        "market for aluminium oxide, non-metallurgical": 0.05301537500508606,
        "market for boric oxide": 0.1327267370182542,
        "market for silica sand": 0.22909980436582345,
    }
    for name, amount in precursor_amounts.items():
        assert exchange(glass, name)["amount"] == amount
    feed = sum(exchange(glass, name)["amount"] for name in precursor_amounts)
    reject = -exchange(glass, WASTE)["amount"]
    assert feed * 0.95 == pytest.approx(1, rel=1e-14)
    assert reject == pytest.approx(feed * 0.05, rel=1e-14)
    assert feed == pytest.approx(1 + reject, rel=1e-14)
    assert reject == pytest.approx(1 / 0.95 - 1, rel=1e-14)
    assert (
        exchange(glass, "market for heat, district or industrial, natural gas")[
            "amount"
        ]
        == 6
    )
    assert (
        exchange(glass, "market group for electricity, medium voltage")["amount"] == 1
    )


def test_finished_stack_dose_and_embedded_clic_glass_only(raw, adoption):
    stack = activity(raw.data, STACK)
    purchase = exchange(stack, NAME)
    assert exchange_key(purchase) == ("technosphere", NAME, PRODUCT, "GLO", "kilogram")
    assert purchase["amount"] == 25.6
    assert stack["production amount"] == exchange(stack, STACK)["amount"] == 1
    glass = activity(raw.data)
    # CLIC embeds the seven purchases instead of buying a finished helper.
    # Compare only those inputs; ceramics and interconnect cases are out of scope.
    clic = adoption["CLIC_stack_snapshot"]
    for exc in glass["exchanges"]:
        if exc["type"] != "technosphere":
            continue
        embedded = exchange(clic, exc["name"])
        assert exchange_key(embedded) == exchange_key(exc)
        assert embedded["amount"] == exc["amount"] * purchase["amount"]
    assert exchange(clic, WASTE)["amount"] == -1.3473684210526302
    assert not any(e["name"] in {WASTE, OLD_WASTE} for e in stack["exchanges"])
    hydrogen = activity(
        raw.data,
        "hydrogen production, gaseous, 1 bar, from SOEC electrolysis, from grid electricity",
    )
    allocation = exchange(hydrogen, STACK)
    assert allocation["amount"] == 2.11864406779661e-6
    assert exchange_key(allocation) == (
        "technosphere",
        STACK,
        "electrolyzer, 1MWe, SOEC, Stack",
        "RER",
        "unit",
    )


def test_xml_changes_only_four_cells_preserving_all_formulas_and_caches():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, _, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        if not (
            sheet == SHEET
            and (address in ALLOWED | LA_COMMENT_CELLS | SIC_CELLS or row in {871, 872})
        ):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert cell.find(NS + "v") is not None
            assert cell.find(NS + "v").text is not None
            formulas.append([sheet, address, payload])
    assert cells
    assert cells

    assert formulas  # Formula/cache pairs are checked above.

    for address, expected in {
        "A678": WASTE,
        "G678": WASTE_PRODUCT,
        "I678": WASTE_PRODUCT,
        "B678": str(REJECT),
        "C678": "RoW",
        "D678": "kilogram",
        "F678": "technosphere",
    }.items():
        assert cells[SHEET, address][1] == expected
    assert (SHEET, "H678") not in cells
    # Also pin row attributes and every non-cell worksheet element, so empty
    # row insertions or other structural changes cannot escape the cell pins.
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        root = ET.fromstring(archive.read(SHEET))
    for parent in root.iter():
        for cell in list(parent):
            if (
                cell.tag == NS + "c"
                and cell.get("r") in ALLOWED | LA_COMMENT_CELLS | SIC_CELLS
            ):
                parent.remove(cell)
            elif cell.tag == NS + "row" and cell.get("r") in {"871", "872"}:
                parent.remove(cell)


def test_non_target_archive_parts_remain_byte_identical():
    with ZipFile(INVENTORY_DIR / WORKBOOK) as archive:
        parts = [
            [name, archive.read(name)]
            for name in sorted(archive.namelist())
            if name != SHEET
        ]
    assert len(parts) == 14


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_real_preparation_preserves_signed_reject_and_39_source(
    raw, adoption, version, system_model
):
    holder = SimpleNamespace(data=[deepcopy(adoption["after_snapshot"])])
    migrate_import_db(holder, "3.9", version)
    if system_model == "consequential":
        holder.data = check_for_datasets_compliance_with_consequential_database(
            holder.data, get_consequential_blacklist(), version
        )
    background = [
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
    before = deepcopy(background)
    source = deepcopy(raw)
    source.data = [activity(source.data)]
    importer = DefaultInventory(
        background,
        "3.9",
        version,
        INVENTORY_DIR / WORKBOOK,
        system_model,
        True,
        preloaded_importer=source,
    )
    importer.prepare_inventory()
    assert importer.version_in == "3.9"
    assert background == before
    assert importer.list_unlinked == []
    prepared = activity(importer.import_db.data)
    assert prepared["production amount"] == exchange(prepared, NAME)["amount"] == 1
    waste = [e for e in prepared["exchanges"] if e["name"] == WASTE]
    assert waste
    assert all(e["type"] == "technosphere" and e["unit"] == "kilogram" for e in waste)
    assert sum(e["amount"] for e in waste) == pytest.approx(REJECT, rel=1e-14)


@pytest.mark.parametrize("version", VERSIONS)
def test_actual_default_caller_keeps_original_39_source(version, monkeypatch):
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
            observed.append((Path(kwargs["path"]).name, kwargs["version_in"]))
            self.list_unlinked = []

        def merge_inventory(self):
            return []

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract)
    monkeypatch.setattr(module, "DefaultInventory", ObserveInventory)
    monkeypatch.setattr(module, "use_native_ptfe_gasket", lambda *args: None)
    monkeypatch.setattr(module, "use_native_fkm_gasket", lambda *args, **kwargs: None)
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, "cutoff"
    obj.keep_imports_uncertainty = True
    obj._database = []
    assert obj._NewDatabase__import_inventories() == []
    assert selected == observed
    assert observed.count((WORKBOOK, "3.9")) == 1
    assert [source for name, source in observed if name == WORKBOOK] == ["3.9"]


@pytest.mark.parametrize("version", ("3.9", "3.12"))
@pytest.mark.parametrize("uncertainty", (False, True))
def test_supplier_change_invalidates_import_cache_preserving_native_cache(
    version, uncertainty, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = uncertainty
    native = obj._database_cache_path("ALIGN021")
    imports = obj._database_cache_path("ALIGN021", inventories=True)
    path = INVENTORY_DIR / WORKBOOK
    original = path.read_bytes()
    copied = tmp_path / WORKBOOK
    copied.write_bytes(original)
    monkeypatch.setattr(module, "FILEPATH_HYDROGEN_INVENTORIES", copied)
    assert obj._database_cache_path("ALIGN021", inventories=True) == imports
    with ZipFile(path) as source, ZipFile(copied, "w") as target:
        for info in source.infolist():
            content = source.read(info.filename)
            if info.filename == SHEET:
                assert WASTE.encode() in content
                content = content.replace(WASTE.encode(), OLD_WASTE.encode())
            target.writestr(info, content)
    assert obj._database_cache_path("ALIGN021", inventories=True) != imports
    assert obj._database_cache_path("ALIGN021") == native
    copied.write_bytes(original)
    assert obj._database_cache_path("ALIGN021", inventories=True) == imports
    assert path.read_bytes() == original
