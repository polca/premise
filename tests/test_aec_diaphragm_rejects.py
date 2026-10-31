"""ALIGN-020: physical coating-reject split and unchanged import boundaries.

Scientific values come from the approved CLIC snapshot. XML pins were captured
independently from the before.xlsx backup, never from the modified workbook.
Preparation uses supplier identities; it neither establishes native linking or
LCIA equivalence nor builds or writes a Brightway database.
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
WORKBOOK = "lci-hydrogen-electrolysis.xlsx"
NAME = (
    "diaphragm production, alkaline electrolysis, zirconia-polysulfone "
    "with polyphenylene sulfide mesh"
)
PRODUCT = "diaphragm, alkaline electrolysis, zirconia-polysulfone with PPS mesh"
STACK = "electrolyzer production, 1MWe, AEC, Stack"
INERT = "market for inert waste, for final disposal"
PLASTIC = "market for waste plastic, mixture"


OLD_INERT = -0.040912611614569605
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
SHEET = "xl/worksheets/sheet1.xml"
VERSIONS = tuple(
    yaml.safe_load((ROOT / "premise/iam_variables_mapping/constants.yaml").read_text())[
        "SUPPORTED_EI_VERSIONS"
    ]
)
# These include all sheets, resolved shared strings, cell attributes, formulas
# and cached values. ALIGN-021 separately permits four glass text cells, tested
# in test_soec_glass_rejects. This pin is from its independent before.xlsx;
# the original ALIGN-020 adoption hash and scientific tests remain unchanged.
GLASS_CELLS = {"B667", "A678", "G678", "I678"}


SIC_CELLS = {"B1229", "B1234", "B1236", "H1235"}


OLD_COMMENT_LENGTH = 2511


def snapshot(value):
    return json.loads(json.dumps(value, sort_keys=True))


def activity(data, name=NAME):
    (match,) = [d for d in data if d["name"] == name]
    return match


def exchange(data, name):
    (match,) = [e for e in data["exchanges"] if e["name"] == name]
    return match


def exchange_key(exc):
    categories = exc.get("categories", ())
    if isinstance(categories, str):
        categories = categories.split("::")
    product = exc.get("reference product")
    if "product" in exc:
        assert exc["product"] == product
    return (
        exc["type"],
        exc["name"],
        product,
        exc.get("location"),
        exc["unit"],
        tuple(categories),
    )


def scientific_vector(data):
    """Compare full scientific payloads, including any unexpected extra fields."""
    result = {}
    for exc in data["exchanges"]:
        key = exchange_key(exc)
        assert key not in result, key
        result[key] = {
            k: v
            for k, v in exc.items()
            if k not in {"comment", "product", "input", "output", "database", "code"}
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
        (ROOT / "scripts/data/aec-diaphragm-reject-adoption-20261027.json").read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / WORKBOOK)


def test_approval_pins_full_snapshot_and_original_source_profile(adoption):
    assert adoption["CLIC_dataset_id"] == 3705
    assert adoption["CLIC_version_id"] == 5620

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.9",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.9"}

    assert adoption["approval"] == "ALIGN-020; approved by Romain Sacchi, 2026-10-27"
    assert adoption["CLIC_reference_version"] == "3.12"
    assert adoption["workbook"] == WORKBOOK
    assert adoption["source_diaphragm_purchase_kg"] == 121.8


def test_portable_supplier_evidence_pins_exact_original_and_target_catalogues(adoption):
    checks = {
        entry["version"]: entry for entry in adoption["supplier_catalogue_checks"]
    }
    assert len(checks) == len(adoption["supplier_catalogue_checks"]) == 2
    expected = {
        "3.9": (
            "8750ad69-0d9a-5f75-988e-114772b6118e",
            "d3577fd1-83e6-5570-a4e4-02dbb288d14d",
        ),
        "3.12": (
            "97799ad6-8ebb-58d6-9ddd-63aea51297f8",
            "204c1265-61f7-5cf9-ac32-ad22dfbe5e24",
        ),
    }
    assert checks.keys() == expected.keys()
    for version, (plastic_uuid, inert_uuid) in expected.items():
        check = checks[version]
        assert check["plastic_exact"] is check["inert_exact"] is True
        assert check["plastic_reference"] == [
            plastic_uuid,
            "88afd721-07ec-4910-a004-fa3fecb6bf70",
        ]
        assert check["inert_reference"] == [
            inert_uuid,
            "240c1a3c-1aba-4528-afc3-3f27f56583be",
        ]
        pin = check["catalogue_pin"]

        assert pin["profile"] == {
            "family": "ecoinvent",
            "version": version,
            "system_model": "cutoff",
        }
        assert pin["axis"] == "technosphere"
        assert pin["digest_kind"] == "raw_json"
        assert pin["schema_version"] == 2


def test_actual_full_scientific_vector_matches_eleven_clic_flows(raw, adoption):
    actual, snapshot = activity(raw.data), adoption["snapshot"]
    for field in (
        "name",
        "reference product",
        "location",
        "unit",
        "production amount",
        "type",
    ):
        assert actual[field] == snapshot[field]
    assert (
        actual["name"],
        actual["reference product"],
        actual["location"],
        actual["unit"],
        actual["production amount"],
    ) == (NAME, PRODUCT, "GLO", "kilogram", 1)
    actual_vector, expected = scientific_vector(actual), scientific_vector(snapshot)
    assert len(actual["exchanges"]) == len(actual_vector) == len(expected) == 11
    assert actual_vector == expected
    assert Counter(e["type"] for e in actual["exchanges"]) == {
        "production": 1,
        "technosphere": 10,
    }
    for name, product, location, amount in (
        (INERT, "inert waste, for final disposal", "RoW", -0.03477571987238416),
        (PLASTIC, "waste plastic, mixture", "DE", -0.00613689174218544),
    ):
        exc = exchange(actual, name)
        assert exchange_key(exc) == (
            "technosphere",
            name,
            product,
            location,
            "kilogram",
            (),
        )
        assert exc["amount"] == amount
    assert not any(e.get("formula") for e in actual["exchanges"])
    assert not any(k.startswith("replacement ") for e in actual["exchanges"] for k in e)


def test_five_percent_coating_loss_85_15_partition_and_dry_mass_closure(raw):
    data = activity(raw.data)
    zirconia = exchange(data, "market for zirconium oxide")["amount"]
    polysulfone = exchange(data, "market for polysulfone")["amount"]
    mesh = exchange(data, "market for polyphenylene sulfide")["amount"]
    inert = -exchange(data, INERT)["amount"]
    plastic = -exchange(data, PLASTIC)["amount"]
    assert inert == pytest.approx(0.05 * zirconia, rel=1e-14, abs=1e-16)
    assert plastic == pytest.approx(0.05 * polysulfone, rel=1e-14, abs=1e-16)
    assert inert / (inert + plastic) == pytest.approx(0.85, rel=1e-14)
    assert plastic / (inert + plastic) == pytest.approx(0.15, rel=1e-14)
    assert inert + plastic == pytest.approx(-OLD_INERT, rel=1e-14, abs=1e-16)
    assert 0.95 * (zirconia + polysulfone) + mesh == pytest.approx(1, rel=1e-14)
    assert zirconia + polysulfone + mesh == pytest.approx(
        1 + inert + plastic, rel=1e-14
    )
    solvent = exchange(data, "market for N-methyl-2-pyrrolidone")["amount"]
    assert exchange(data, "market for spent solvent mixture")["amount"] == -solvent


def test_only_rejects_change_with_eight_other_inputs_and_source_history_preserved(
    raw, adoption
):
    actual, before = activity(raw.data), adoption["before_snapshot"]
    assert exchange(before, INERT)["amount"] == OLD_INERT
    assert not any(e["name"] == PLASTIC for e in before["exchanges"])
    old, current = scientific_vector(before), scientific_vector(actual)
    unchanged = {key: exc for key, exc in old.items() if key[1] != INERT}
    assert len(unchanged) == 9  # Production plus the eight other inputs.
    assert {
        key: exc for key, exc in current.items() if key[1] not in {INERT, PLASTIC}
    } == unchanged
    assert {
        key: value
        for key, value in actual.items()
        if key not in {"exchanges", "comment"}
    } == {
        key: value
        for key, value in before.items()
        if key not in {"exchanges", "comment"}
    }
    assert actual["comment"].endswith(before["comment"])
    assert len(actual["comment"]) > len(before["comment"])
    # Existing flow order and product aliases stay intact; the added row is last.
    assert [e["name"] for e in actual["exchanges"]] == [
        e["name"] for e in before["exchanges"]
    ] + [PLASTIC]


def test_finished_stack_purchase_and_full_stack_reject_allocation(raw, adoption):
    stack = activity(raw.data, STACK)
    purchase = exchange(stack, NAME)
    assert exchange_key(purchase) == (
        "technosphere",
        NAME,
        PRODUCT,
        "GLO",
        "kilogram",
        (),
    )
    assert purchase["amount"] == adoption["source_diaphragm_purchase_kg"] == 121.8
    assert stack["production amount"] == exchange(stack, STACK)["amount"] == 1
    hydrogen = activity(
        raw.data,
        "hydrogen production, gaseous, 20 bar, from AEC electrolysis, from grid electricity",
    )
    allocation = exchange(hydrogen, STACK)
    assert exchange_key(allocation) == (
        "technosphere",
        STACK,
        "electrolyzer, 1MWe, AEC, Stack",
        "RER",
        "unit",
        (),
    )
    # Preserve the full cached allocation, including its existing uncertainty;
    # a rounded 9.39e-7 passes a loose approximate assertion but changes science.
    assert allocation["amount"] == allocation["loc"] == 9.391435011269722e-7
    assert allocation["uncertainty type"] == 5
    assert allocation["minimum"] == 7.946598855689764e-7
    assert allocation["maximum"] == 1.147842056932966e-6
    diaphragm = activity(raw.data)
    inert = -exchange(diaphragm, INERT)["amount"] * purchase["amount"]
    plastic = -exchange(diaphragm, PLASTIC)["amount"] * purchase["amount"]
    assert inert == 4.2356826804563905
    assert plastic == 0.7474734141981866
    assert inert + plastic == pytest.approx(4.983156094654578, rel=1e-14)
    assert not any(e["name"] in {INERT, PLASTIC} for e in stack["exchanges"])


def test_xml_changes_only_root_note_inert_amount_and_new_row_921():
    cells = xml_cells(INVENTORY_DIR / WORKBOOK)
    outside, formulas = [], []
    for (sheet, address), (cell, text, payload) in sorted(cells.items()):
        row = int("".join(c for c in address if c.isdigit()))
        if not (
            sheet == SHEET
            and (
                address
                in (
                    {"B907", "B917", "B862", "B466", "H472", "H473", "H474", "B881"}
                    | GLASS_CELLS
                    | SIC_CELLS
                )
                or row in {871, 872, 921}
            )
        ):
            outside.append([sheet, address, payload])
        if cell.find(NS + "f") is not None:
            assert cell.find(NS + "v") is not None
            assert cell.find(NS + "v").text is not None
            formulas.append([sheet, address, payload])
    assert cells

    assert formulas  # Formula/cache pairs are checked above.

    row_921 = {
        address: text
        for (sheet, address), (_, text, _) in cells.items()
        if sheet == SHEET and address.lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ") == "921"
    }
    assert row_921.keys() == {f"{column}921" for column in "ABCDFGHI"}
    for column, value in {
        "A": PLASTIC,
        "B": "-0.00613689174218544",
        "C": "DE",
        "D": "kilogram",
        "F": "technosphere",
        "G": "waste plastic, mixture",
        "I": "waste plastic, mixture",
    }.items():
        assert row_921[f"{column}921"] == value
    assert row_921["H921"]
    assert (SHEET, "H917") not in cells
    assert not any(
        sheet == SHEET and address.lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ") == "922"
        for sheet, address in cells
    )
    root_comment = cells[SHEET, "B907"][1]
    assert root_comment.startswith("APPROVED COATING-REJECT TREATMENT SPLIT")
    assert len(root_comment) > OLD_COMMENT_LENGTH

    assert cells[SHEET, "B917"][1] == "-0.03477571987238416"


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
def test_real_preparation_keeps_existing_39_recipe_path(
    raw, adoption, version, system_model
):
    holder = SimpleNamespace(data=[deepcopy(adoption["snapshot"])])
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
    for name, amount in (
        (INERT, -0.03477571987238416),
        (PLASTIC, -0.00613689174218544),
    ):
        matches = [e for e in prepared["exchanges"] if e["name"] == name]
        assert matches
        assert all(
            e["type"] == "technosphere" and e["unit"] == "kilogram" for e in matches
        )
        assert sum(e["amount"] for e in matches) == pytest.approx(amount, rel=1e-14)


@pytest.mark.parametrize("version", VERSIONS)
def test_actual_default_caller_keeps_hydrogen_source_at_39(version, monkeypatch):
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
def test_physical_split_invalidates_import_cache_but_preserves_native_cache(
    version, uncertainty, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = uncertainty
    native = obj._database_cache_path("ALIGN020")
    imports = obj._database_cache_path("ALIGN020", inventories=True)
    path = INVENTORY_DIR / WORKBOOK
    original = path.read_bytes()
    copied = tmp_path / WORKBOOK
    copied.write_bytes(original)
    monkeypatch.setattr(module, "FILEPATH_HYDROGEN_INVENTORIES", copied)
    assert obj._database_cache_path("ALIGN020", inventories=True) == imports
    with ZipFile(path) as source, ZipFile(copied, "w") as target:
        for info in source.infolist():
            content = source.read(info.filename)
            if info.filename == SHEET:
                assert b"-0.03477571987238416" in content
                content = content.replace(
                    b"-0.03477571987238416", b"-0.040912611614569605"
                )
            target.writestr(info, content)
    assert obj._database_cache_path("ALIGN020", inventories=True) != imports
    assert obj._database_cache_path("ALIGN020") == native
    copied.write_bytes(original)
    assert obj._database_cache_path("ALIGN020", inventories=True) == imports
    assert path.read_bytes() == original
