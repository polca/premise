"""ALIGN-017: raw-workbook adoption and the default-import boundary only.

These tests use real Excel extraction and DefaultInventory preparation against
supplier identities. They do not build a licensed native database, run LCIA, or
write to Brightway. The caller test replaces merging to observe its boundary;
it is not evidence of a successful complete native build.
"""

import ast
import importlib
import json
import runpy
from copy import deepcopy
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest
from bw2io import ExcelImporter
from packaging.version import Version

from premise.filesystem_constants import INVENTORY_DIR
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
LEGACY = "lci-hydrogen-electrolysis.xlsx"
NATIVE = "lci-ptfe-gasket-native-312.xlsx"
OLD_NAME = "polytetrafluoroethylene gasket production, polymerization and forming"
OLD_PRODUCT = "PTFE gasket, polymerization and forming"
NEW_NAME = "polytetrafluoroethylene gasket production, forming, resin supply proxy"
NEW_PRODUCT = "PTFE gasket, unfilled, engineering reference"
STACK = "electrolyzer production, 1MWe, AEC, Stack"


# Frozen before ALIGN-018: producer rows 1206:1222 and the AEC purchase row 312.
# The original whole-file hash above remains adoption-time provenance only.

LEGACY_VERSIONS = ("3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11")
LINK_FIELDS = ("input", "output", "database", "code")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def importer_state(importer, *, include_data=True):
    # Strategies contain functools.partial objects: deepcopy changes their
    # identity despite no importer mutation. Retain callable identities while
    # freezing data and metadata as canonical bytes, including NaN sentinels.
    return {
        key: tuple(value) if key == "strategies" else canonical(value)
        for key, value in importer.__dict__.items()
        if include_data or key != "data"
    }


def dataset(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def gasket_input(data, name=OLD_NAME):
    (exchange,) = [e for e in dataset(data, STACK)["exchanges"] if e["name"] == name]
    return exchange


def expected_adoption(data):
    """Independent oracle: one deletion and one exchange's identity only."""
    expected = deepcopy(data)
    expected.remove(dataset(expected, OLD_NAME))
    exchange = gasket_input(expected)
    for field in LINK_FIELDS:
        exchange.pop(field, None)
    exchange.update(
        {
            "name": NEW_NAME,
            "reference product": NEW_PRODUCT,
            "location": "GLO",
            "unit": "kilogram",
        }
    )
    if "product" in exchange:
        exchange["product"] = NEW_PRODUCT
    return expected


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (
            ROOT / "scripts/data/ptfe-gasket-native-312-adoption-20261027.json"
        ).read_text()
    )


@pytest.fixture(scope="module")
def raw_source():
    return ExcelImporter(INVENTORY_DIR / LEGACY)


@pytest.fixture
def source(raw_source):
    return deepcopy(raw_source)


@pytest.fixture
def use_native():
    # Keep fixture/provenance tests collectable while the implementation is
    # developed separately. A missing helper is a failure, never a skipped test.
    return importlib.import_module("premise.ptfe_gasket").use_native_ptfe_gasket


def exchange_key(exchange):
    return (
        exchange["type"],
        exchange["name"],
        exchange.get("product", exchange.get("reference product")),
        exchange["location"],
        exchange["unit"],
    )


def assert_native_recipe(actual, snapshot):
    for field in ("name", "reference product", "location", "unit"):
        assert actual[field] == snapshot[field]
    expected = {exchange_key(e): e["amount"] for e in snapshot["exchanges"]}
    observed = {exchange_key(e): e["amount"] for e in actual["exchanges"]}
    assert len(actual["exchanges"]) == len(observed) == len(expected) == 5
    assert observed.keys() == expected.keys()
    for key, amount in expected.items():
        # Excel serialization can round the last decimal of the resin/reject.
        assert observed[key] == pytest.approx(amount, rel=1e-14, abs=1e-15), key
    for exchange in actual["exchanges"]:
        assert exchange["product"] == exchange["reference product"]
        assert not exchange.get("formula")
        assert not any(key.startswith("replacement ") for key in exchange)


def test_portable_fixture_pins_approval_profiles_and_original_license(adoption):

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.12",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.12"}
    assert adoption["license"] == "CC-BY-4.0"
    assert adoption["legacy_source_workbook"] == LEGACY
    assert adoption["legacy_source_version"] == "3.9"

    assert adoption["consumer_gasket_kg"] == 144.2


def test_legacy_ptfe_block_purchase_and_cached_formulas_are_preserved(raw_source):
    path = INVENTORY_DIR / LEGACY

    producer = dataset(raw_source.data, OLD_NAME)
    assert producer["reference product"] == OLD_PRODUCT
    assert (producer["location"], producer["unit"]) == ("GLO", "kilogram")
    assert len(producer["exchanges"]) == 5
    assert {e["name"]: e["amount"] for e in producer["exchanges"]} == {
        OLD_NAME: 1,
        "market for tetrafluoroethylene": 1.0526315789473684,
        "market for heat, district or industrial, natural gas": 10.5,
        "market group for electricity, medium voltage": 0.7,
        "market for waste plastic, mixture": -0.05263157894736836,
    }
    assert gasket_input(raw_source.data)["amount"] == 144.2
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    formula_cells = []
    with ZipFile(path) as archive:
        for name in archive.namelist():
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml"):
                sheet = ET.fromstring(archive.read(name))
                formula_cells.extend(
                    c
                    for c in sheet.findall(".//m:c", ns)
                    if c.find("m:f", ns) is not None
                )
    # Approved R10 D02 adds only two hydroxide-allocation formulas outside
    # the unchanged 77-cell PTFE slice pinned above; historical count was484.
    assert len(formula_cells) == 486
    assert all(c.find("m:v", ns) is not None for c in formula_cells)
    assert all(c.find("m:v", ns).text is not None for c in formula_cells)


@pytest.mark.parametrize("version", LEGACY_VERSIONS)
def test_legacy_versions_are_byte_and_canonical_data_noops(source, use_native, version):
    before = canonical(source.data)
    state = importer_state(source)
    original_data = source.data
    use_native(source, version)
    assert source.data is original_data
    assert canonical(source.data) == before
    assert importer_state(source) == state


@pytest.mark.parametrize("version", ("3.12", "3.12.0"))
def test_actual_source_loses_only_old_producer_and_rewires_one_input(
    source, use_native, version
):
    before = deepcopy(source.data)
    state = importer_state(source, include_data=False)
    expected = expected_adoption(before)
    use_native(source, version)
    assert len(source.data) == len(before) - 1
    assert canonical(source.data) == canonical(expected)
    assert importer_state(source, include_data=False) == state
    assert not any(d["name"] in (OLD_NAME, NEW_NAME) for d in source.data)
    exchange = gasket_input(source.data, NEW_NAME)
    assert exchange["amount"] == 144.2
    assert exchange["reference product"] == NEW_PRODUCT
    assert exchange.get("product", NEW_PRODUCT) == NEW_PRODUCT
    assert (exchange["location"], exchange["unit"]) == ("GLO", "kilogram")
    assert sum(e["name"] == NEW_NAME for d in source.data for e in d["exchanges"]) == 1


def test_repeated_preparation_of_same_raw_importer_is_idempotent(source, use_native):
    original_count = len(source.data)
    use_native(source, "3.12")
    before = canonical(source.data)
    state = importer_state(source)
    use_native(source, "3.12")
    use_native(source, "3.12")
    assert canonical(source.data) == before
    assert importer_state(source) == state
    assert len(source.data) == original_count - 1


def test_stale_link_fields_are_cleared_but_uncertainty_and_metadata_survive(
    source, use_native
):
    exchange = gasket_input(source.data)
    exchange.update(
        {
            "input": ("old database", "old producer"),
            "output": ("old database", "stack"),
            "database": "old database",
            "code": "old producer",
            "product": OLD_PRODUCT,
            "uncertainty type": 3,
            "loc": 144.2,
            "scale": 1.5,
            "minimum": 140,
            "maximum": 150,
            "negative": False,
            "comment": "Keep the approved lifetime purchase and its attribution.",
        }
    )
    expected = expected_adoption(source.data)
    use_native(source, "3.12")
    assert canonical(source.data) == canonical(expected)
    assert all(
        field not in gasket_input(source.data, NEW_NAME) for field in LINK_FIELDS
    )


@pytest.mark.parametrize("in_stack", (False, True))
def test_unrelated_product_alias_conflicts_are_preserved(source, use_native, in_stack):
    parent = (
        dataset(source.data, STACK)
        if in_stack
        else next(d for d in source.data if d["name"] not in (STACK, OLD_NAME))
    )
    unrelated = next(
        e
        for e in parent["exchanges"]
        if e["type"] == "technosphere" and e["name"] != OLD_NAME
    )
    unrelated["product"] = "unrelated pre-existing alias conflict"
    expected = expected_adoption(source.data)
    use_native(source, "3.12")
    assert canonical(source.data) == canonical(expected)


def test_native_provider_must_not_be_embedded_in_hydrogen_source(source, use_native):
    (native,) = ExcelImporter(INVENTORY_DIR / NATIVE).data
    source.data.append(native)
    before = importer_state(source)
    with pytest.raises(ValueError):
        use_native(source, "3.12")
    assert importer_state(source) == before


@pytest.mark.parametrize(
    "problem",
    (
        "missing producer",
        "duplicate producer",
        "missing stack",
        "duplicate stack",
        "missing purchase",
        "duplicate purchase",
        "other consumer",
    ),
)
def test_missing_or_ambiguous_producer_and_consumers_fail_atomically(
    source, use_native, problem
):
    producer = dataset(source.data, OLD_NAME)
    stack = dataset(source.data, STACK)
    exchange = gasket_input(source.data)
    if problem == "missing producer":
        source.data.remove(producer)
    elif problem == "duplicate producer":
        source.data.append(deepcopy(producer))
    elif problem == "missing stack":
        source.data.remove(stack)
    elif problem == "duplicate stack":
        source.data.append(deepcopy(stack))
    elif problem == "missing purchase":
        stack["exchanges"].remove(exchange)
    elif problem == "duplicate purchase":
        stack["exchanges"].append(deepcopy(exchange))
    else:
        other = deepcopy(stack)
        other["name"] = "another consumer outside the approved AEC stack"
        source.data.append(other)
    before = importer_state(source)
    with pytest.raises(ValueError):
        use_native(source, "3.12")
    assert importer_state(source) == before


@pytest.mark.parametrize(
    "target,field,value",
    [
        ("producer", "reference product", "other PTFE product"),
        ("producer", "location", "RER"),
        ("producer", "unit", "gram"),
        ("stack", "reference product", "other electrolyzer"),
        ("stack", "location", "GLO"),
        ("stack", "unit", "kilogram"),
        ("purchase", "reference product", "other PTFE product"),
        ("purchase", "product", "conflicting product"),
        ("purchase", "location", "RER"),
        ("purchase", "unit", "gram"),
        ("purchase", "type", "production"),
        ("purchase", "formula", "144.2 * replacements"),
        ("purchase", "amount", 144.3),
        ("purchase", "amount", 144.2 * 3),
        ("purchase", "amount", 0),
        ("purchase", "amount", -144.2),
        ("purchase", "amount", "144.2"),
        ("purchase", "amount", float("nan")),
        ("purchase", "amount", float("inf")),
    ],
)
def test_changed_source_identity_formula_or_dose_fails_atomically(
    source, use_native, target, field, value
):
    objects = {
        "producer": dataset(source.data, OLD_NAME),
        "stack": dataset(source.data, STACK),
        "purchase": gasket_input(source.data),
    }
    objects[target][field] = value
    before = importer_state(source)
    with pytest.raises(ValueError):
        use_native(source, "3.12")
    assert importer_state(source) == before


@pytest.mark.parametrize(
    "flow,field,value",
    [
        (0, "amount", 2),
        (1, "amount", 1),
        (2, "amount", 3),
        (3, "amount", 0.2),
        (4, "amount", 0.05263157894736836),
        (1, "name", "market for polytetrafluoroethylene"),
        (1, "reference product", "polytetrafluoroethylene"),
        (1, "product", "polytetrafluoroethylene"),
        (1, "location", "RER"),
        (1, "unit", "gram"),
        (1, "formula", "1 / yield"),
    ],
)
def test_changed_legacy_producer_recipe_fails_atomically(
    source, use_native, flow, field, value
):
    dataset(source.data, OLD_NAME)["exchanges"][flow][field] = value
    before = importer_state(source)
    with pytest.raises(ValueError):
        use_native(source, "3.12")
    assert importer_state(source) == before


@pytest.mark.parametrize("problem", ("missing flow", "duplicate flow", "extra flow"))
def test_legacy_producer_requires_exactly_five_flows(source, use_native, problem):
    exchanges = dataset(source.data, OLD_NAME)["exchanges"]
    if problem == "missing flow":
        exchanges.pop()
    else:
        extra = deepcopy(exchanges[-1])
        if problem == "extra flow":
            extra["name"] = "unapproved processing aid"
        exchanges.append(extra)
    before = importer_state(source)
    with pytest.raises(ValueError):
        use_native(source, "3.12")
    assert importer_state(source) == before


@pytest.mark.parametrize(
    "problem",
    ("old input", "wrong dose", "duplicate new input", "reintroduced producer"),
)
def test_partial_or_corrupt_adoption_is_not_mistaken_for_idempotence(
    source, use_native, problem
):
    producer = deepcopy(dataset(source.data, OLD_NAME))
    use_native(source, "3.12")
    exchange = gasket_input(source.data, NEW_NAME)
    if problem == "old input":
        exchange["name"] = OLD_NAME
        exchange["reference product"] = exchange["product"] = OLD_PRODUCT
    elif problem == "wrong dose":
        exchange["amount"] = 1
    elif problem == "duplicate new input":
        dataset(source.data, STACK)["exchanges"].append(deepcopy(exchange))
    else:
        source.data.append(producer)
    before = importer_state(source)
    with pytest.raises(ValueError):
        use_native(source, "3.12")
    assert importer_state(source) == before


def test_native_workbook_matches_five_approved_exchanges_and_profiles(adoption):
    (native,) = ExcelImporter(INVENTORY_DIR / NATIVE).data
    assert_native_recipe(native, adoption["snapshot"])
    assert (native["name"], native["reference product"], native["location"]) == (
        NEW_NAME,
        NEW_PRODUCT,
        "GLO",
    )
    assert native["license"] == "CC-BY-4.0"
    assert native["type"] == adoption["snapshot"]["type"]
    assert native["background version"] == native["biosphere version"] == "3.12"
    assert adoption["snapshot"]["source"] in native["source"]
    assert adoption["snapshot"]["comment"] in native["comment"]
    (resin,) = [
        e
        for e in native["exchanges"]
        if e["name"] == "market for polytetrafluoroethylene"
    ]
    assert exchange_key(resin) == (
        "technosphere",
        "market for polytetrafluoroethylene",
        "polytetrafluoroethylene",
        "GLO",
        "kilogram",
    )
    assert resin["amount"] == pytest.approx(1 / 0.95, rel=1e-14)
    # Exactly five matched flows excludes monomer, extra polymerization energy,
    # filler, an additional residual, and an extra end-of-life purchase.
    amounts = {e["name"]: e["amount"] for e in native["exchanges"]}
    assert amounts[NEW_NAME] == 1
    assert amounts["market for heat, district or industrial, natural gas"] == 3
    assert amounts["market group for electricity, medium voltage"] == 0.2
    assert amounts["market for waste plastic, mixture"] == pytest.approx(1 - 1 / 0.95)


def test_real_preparation_preserves_native_supplier_match_fields(adoption):
    # Identity-only suppliers resolve product fields. No supplier inventory
    # bodies are provided, so this establishes preparation, not native linking.
    background = [
        {
            "name": e["name"],
            "reference product": e["reference product"],
            "location": e["location"],
            "unit": e["unit"],
            "exchanges": [],
        }
        for e in adoption["snapshot"]["exchanges"]
        if e["type"] == "technosphere"
    ]
    before = deepcopy(background)
    importer = DefaultInventory(
        background, "3.12", "3.12", INVENTORY_DIR / NATIVE, "cutoff", True
    )
    importer.prepare_inventory()
    assert importer.version_in == importer.version_out == "3.12"
    assert importer.list_unlinked == []
    (prepared,) = importer.import_db.data
    assert_native_recipe(prepared, adoption["snapshot"])
    assert background == before
    for exchange in prepared["exchanges"]:
        assert exchange["product"] == exchange["reference product"]


@pytest.mark.parametrize("version", (*LEGACY_VERSIONS, "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_ast_actual_default_selection_is_versioned_and_precedes_hydrogen(
    version, system_model
):
    generator = runpy.run_path(
        str(ROOT / "scripts/generate_documentation_reference.py")
    )
    tree = ast.parse((ROOT / "premise/new_database.py").read_text())
    selection = generator["inventory_selection"](tree, version, system_model)
    assert selection.count((LEGACY, "3.9")) == 1
    native = [
        (path, source_version) for path, source_version in selection if path == NATIVE
    ]
    if Version(version) < Version("3.12"):
        assert native == []
    else:
        assert native == [(NATIVE, "3.12")]
        index = selection.index((LEGACY, "3.9"))
        assert selection[index - 1] == (NATIVE, "3.12")


@pytest.mark.parametrize("version", ("3.9", "3.11", "3.12"))
def test_new_workbook_content_invalidates_import_cache_only(
    version, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    source_cache = obj._database_cache_path("ALIGN017")
    imports_cache = obj._database_cache_path("ALIGN017", inventories=True)
    workbook = module.FILEPATH_PTFE_GASKET_NATIVE_312
    assert workbook == INVENTORY_DIR / NATIVE
    contents = workbook.read_bytes()
    copy = tmp_path / workbook.name
    copy.write_bytes(contents)
    monkeypatch.setattr(module, "FILEPATH_PTFE_GASKET_NATIVE_312", copy)
    assert obj._database_cache_path("ALIGN017", inventories=True) == imports_cache
    copy.write_bytes(contents + b"changed forming recipe")
    assert obj._database_cache_path("ALIGN017", inventories=True) != imports_cache
    assert obj._database_cache_path("ALIGN017") == source_cache
    copy.write_bytes(contents)
    assert obj._database_cache_path("ALIGN017", inventories=True) == imports_cache


@pytest.mark.parametrize("version", ("3.11", "3.12"))
def test_actual_caller_hooks_only_hydrogen_before_default_inventory(
    raw_source, use_native, monkeypatch, version
):
    """Run the real caller with a merge observer, not a full build mock."""
    import premise.new_database as module

    events = []
    extracted = {}
    observed = []

    def extract(filepaths):
        importers = []
        for path, _ in filepaths:
            # Decoy workbooks deliberately have the identical legacy payload.
            # Content-based or unscoped hooks would corrupt these too.
            importer = deepcopy(raw_source)
            extracted[path] = importer
            importers.append(importer)
        events.append("extracted")
        return importers

    def hook(importer, target_version):
        assert events[0] == "extracted"
        assert importer is extracted[INVENTORY_DIR / LEGACY]
        assert target_version == version
        events.append("hook")
        return use_native(importer, target_version)

    class ObserveDefaultInventory:
        def __init__(self, **kwargs):
            path = kwargs["path"]
            importer = kwargs["preloaded_importer"]
            assert importer is extracted[path]
            if path == INVENTORY_DIR / LEGACY:
                assert "hook" in events
                expected = (
                    expected_adoption(raw_source.data)
                    if Version(version) >= Version("3.12")
                    else raw_source.data
                )
                assert canonical(importer.data) == canonical(expected)
                assert kwargs["version_in"] == "3.9"
            else:
                assert canonical(importer.data) == canonical(raw_source.data)
            observed.append((path.name, kwargs["version_in"]))
            self.list_unlinked = []

        def merge_inventory(self):
            return []

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract)
    monkeypatch.setattr(module, "use_native_ptfe_gasket", hook)
    # Decoy payloads isolate this PTFE caller from the other reviewed selectors.
    monkeypatch.setattr(module, "_select_nmc955_modern_references", lambda *args: None)
    monkeypatch.setattr(module, "use_native_pipeline_coating", lambda *args: None)
    monkeypatch.setattr(module, "_select_nmc532_native_312", lambda *args: None)
    # This caller observer supplies PTFE decoys, not the separate FKM source.
    monkeypatch.setattr(module, "use_native_fkm_gasket", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "DefaultInventory", ObserveDefaultInventory)
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, "cutoff"
    obj.keep_imports_uncertainty = True
    obj._database = []
    assert obj._NewDatabase__import_inventories() == []
    assert events.count("hook") == 1
    assert observed.count((LEGACY, "3.9")) == 1
    assert observed.count((NATIVE, "3.12")) == (version == "3.12")
