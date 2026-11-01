"""ALIGN019 source selection: real workbooks, graph guards and import boundary.

Supplier identities suffice for preparation tests. These tests neither build a
licensed background database nor establish LCIA results or native linking.
"""

import ast
import json
import runpy
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter
from packaging.version import Version

from premise.filesystem_constants import INVENTORY_DIR
from premise.fkm_gasket import (
    GASKET_NAME,
    HELPER_PRODUCTS,
    use_native_fkm_gasket,
)
from premise.inventory_imports import DefaultInventory

ROOT = Path(__file__).resolve().parents[1]
LEGACY = "lci-batteries-vanadium-redox-flow.xlsx"
NATIVE = "lci-fkm-gasket-native-312.xlsx"
HYBRID = "lci-batteries-organic-and-hybrid-redox-flow.xlsx"
HYDROGEN = "lci-hydrogen-electrolysis.xlsx"
PFSA = "lci-pfsa-ew1100.xlsx"
LEGACY_ACTIVITIES = (GASKET_NAME, *HELPER_PRODUCTS)
STACK = "vanadium-redox flow battery stack assembly"
SEAL = "boundary seal production, for RFB"
AEC = "electrolyzer production, 1MWe, AEC, Stack"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def snapshot(value):
    return json.loads(json.dumps(value, sort_keys=True))


def state(importer):
    # Keep strategy callable identities; deepcopy creates new partial objects.
    return {
        key: tuple(value) if key == "strategies" else canonical(value)
        for key, value in vars(importer).items()
    }


def activity(data, name):
    (match,) = [d for d in data if d["name"] == name]
    return match


def purchase(data, buyer, supplier=GASKET_NAME):
    (exchange,) = [
        e for e in activity(data, buyer)["exchanges"] if e["name"] == supplier
    ]
    return exchange


@pytest.fixture(scope="module")
def adoption():
    return json.loads(
        (
            ROOT
            / "tests/data/inventory_review/fkm-gasket-native-312-adoption-20261027.json"
        ).read_text()
    )


@pytest.fixture(scope="module")
def raw():
    return ExcelImporter(INVENTORY_DIR / LEGACY)


@pytest.fixture
def source(raw):
    return deepcopy(raw)


def assert_atomic_failure(source, others=()):
    before = [state(i) for i in (source, *others)]
    original = source.data
    with pytest.raises(ValueError, match="FKM gasket"):
        use_native_fkm_gasket(source, "3.12", other_importers=others)
    assert source.data is original
    assert [state(i) for i in (source, *others)] == before


def test_portable_approval_pins_snapshot_profiles_and_five_scientific_digests(
    adoption, raw
):
    assert adoption["CLIC_dataset_id"] == 1388
    assert adoption["CLIC_version_id"] == 5555

    assert adoption["source_background"] == {
        "family": "ecoinvent",
        "system_model": "cutoff",
        "version": "3.12",
    }
    assert adoption["source_biosphere"] == {"family": "ecoinvent", "version": "3.12"}
    assert adoption["license"] == "CC-BY-4.0"
    assert adoption["legacy_source_version"] == "3.9"
    assert adoption["legacy_source_workbook"] == LEGACY

    # Independent projection of actual source records, not the runtime helper.
    for name, expected in adoption["legacy_science"].items():
        actual = activity(raw.data, name)
        science = {key: actual[key] for key in expected if key != "exchanges"}
        science["exchanges"] = [
            {k: v for k, v in e.items() if k != "comment"} for e in actual["exchanges"]
        ]
        assert science == expected


@pytest.mark.parametrize("version", ("3.9", "3.11"))
def test_old_targets_are_byte_and_entire_importer_state_noops(source, version):
    source_bytes = (INVENTORY_DIR / LEGACY).read_bytes()
    before, original = state(source), source.data
    assert use_native_fkm_gasket(source, version, other_importers=(object(),)) == 0
    assert source.data is original
    assert state(source) == before
    # The version gate precedes even accessing the importer or other sources.
    assert use_native_fkm_gasket(object(), version, other_importers=None) == 0
    assert (INVENTORY_DIR / LEGACY).read_bytes() == source_bytes


def test_selection_deletes_only_five_records_preserving_three_consumer_activities(
    source,
):
    source_bytes = (INVENTORY_DIR / LEGACY).read_bytes()
    hybrid = ExcelImporter(INVENTORY_DIR / HYBRID)
    hydrogen = ExcelImporter(INVENTORY_DIR / HYDROGEN)
    native = ExcelImporter(INVENTORY_DIR / NATIVE)
    others = (hybrid, hydrogen, native, source)
    before_others = [state(i) for i in others[:-1]]
    before = state(source)
    expected = [d for d in source.data if d["name"] not in LEGACY_ACTIVITIES]
    expected_bytes = canonical(expected)
    consumers = [
        activity(source.data, STACK),
        activity(hybrid.data, SEAL),
        activity(hydrogen.data, AEC),
    ]
    consumers_before = canonical(consumers)
    assert purchase(source.data, STACK)["amount"] == 428
    assert purchase(hybrid.data, SEAL)["amount"] == 1
    assert use_native_fkm_gasket(source, "3.12", other_importers=others) == 5
    assert canonical(source.data) == expected_bytes
    assert all(actual is old for actual, old in zip(source.data, expected))
    assert canonical(consumers) == consumers_before
    assert [state(i) for i in others[:-1]] == before_others
    assert {k: v for k, v in state(source).items() if k != "data"} == {
        k: v for k, v in before.items() if k != "data"
    }
    selected_state, selected_data = state(source), source.data
    assert use_native_fkm_gasket(source, "3.12.0", other_importers=others) == 0
    assert source.data is selected_data
    assert state(source) == selected_state
    assert (INVENTORY_DIR / LEGACY).read_bytes() == source_bytes


@pytest.mark.parametrize("name", tuple(LEGACY_ACTIVITIES))
def test_each_partially_missing_block_fails_atomically(source, name):
    source.data.remove(activity(source.data, name))
    assert_atomic_failure(source)


def test_duplicate_selected_block_fails_atomically(source):
    source.data.append(deepcopy(activity(source.data, GASKET_NAME)))
    assert_atomic_failure(source)


@pytest.mark.parametrize("name", tuple(LEGACY_ACTIVITIES))
def test_each_selected_blocks_recipe_is_checked_before_deletion(source, name):
    activity(source.data, name)["exchanges"][-1]["amount"] += 0.001
    assert_atomic_failure(source)


@pytest.mark.parametrize(
    "target,field,value",
    (
        ("activity", "location", "GLO"),
        ("activity", "reference product", "changed gasket"),
        ("activity", "unit", "gram"),
        ("activity", "production amount", 2),
        ("production", "amount", 2),
        ("input", "product", "conflicting alias"),
        ("input", "reference product", "changed compound"),
        ("input", "formula", "1 / yield"),
        ("input", "uncertainty type", 3),
        ("input", "replacement name", "unreviewed proxy"),
    ),
)
def test_scientific_identity_basis_aliases_and_added_exchange_fields_are_guarded(
    source, target, field, value
):
    gasket = activity(source.data, GASKET_NAME)
    record = {
        "activity": gasket,
        "production": gasket["exchanges"][0],
        "input": gasket["exchanges"][1],
    }[target]
    record[field] = value
    assert_atomic_failure(source)


def test_descriptive_metadata_and_native_link_codes_are_allowed_during_selection(
    source,
):
    for name in LEGACY_ACTIVITIES:
        ds = activity(source.data, name)
        ds.update(comment="curation note", source="citation", code=name, database="db")
        for e in ds["exchanges"]:
            e.update(comment="annotation", input=("db", "code"), output=("db", name))
    expected = [d for d in source.data if d["name"] not in LEGACY_ACTIVITIES]
    assert use_native_fkm_gasket(source, "3.12") == 5
    assert source.data == expected


@pytest.mark.parametrize(
    "where,alias",
    (("own", "name"), ("other", "product"), ("other", "reference product")),
)
def test_unreviewed_helper_consumers_fail_atomically_before_and_after_selection(
    source, where, alias
):
    helper, product = next(iter(HELPER_PRODUCTS.items()))
    buyer = {
        "name": "unreviewed buyer",
        "exchanges": [
            {
                "type": "technosphere",
                "amount": 1,
                alias: helper if alias == "name" else product,
            }
        ],
    }
    other = SimpleNamespace(data=[])
    (source if where == "own" else other).data.append(buyer)
    assert_atomic_failure(source, (source, other))
    (source if where == "own" else other).data.remove(buyer)
    assert use_native_fkm_gasket(source, "3.12", (source, other)) == 5
    (source if where == "own" else other).data.append(buyer)
    assert_atomic_failure(source, (source, other))


def test_link_only_helper_reference_is_guarded_without_rewriting_other_importer(source):
    helper = activity(source.data, next(iter(HELPER_PRODUCTS)))
    helper.update(database="source", code="private-helper")
    other = SimpleNamespace(
        data=[
            {
                "name": "linked consumer",
                "exchanges": [
                    {
                        "type": "technosphere",
                        "amount": 1,
                        "input": ["source", "private-helper"],
                    }
                ],
            }
        ]
    )
    assert_atomic_failure(source, (other,))


def test_equal_payload_in_other_importer_is_not_exempted_as_selected_source(source):
    other = deepcopy(source)
    assert_atomic_failure(source, (source, other))


def test_all_workbook_source_closure_has_only_reviewed_helper_users_and_gasket_buyers():
    helper_users, gasket_buyers = [], []
    for path in sorted(INVENTORY_DIR.glob("*.xlsx")):
        for ds in ExcelImporter(path).data:
            for e in ds["exchanges"]:
                if e["type"] != "technosphere":
                    continue
                if e["name"] in HELPER_PRODUCTS:
                    helper_users.append((path.name, ds["name"], e["name"]))
                if e["name"] == GASKET_NAME:
                    gasket_buyers.append((path.name, ds["name"], e["amount"]))
    vdf, hfp, polymer, compound = HELPER_PRODUCTS
    assert set(helper_users) == {
        (LEGACY, GASKET_NAME, compound),
        (LEGACY, compound, polymer),
        (LEGACY, polymer, vdf),
        (LEGACY, polymer, hfp),
    }
    assert len(helper_users) == 4
    assert sorted(gasket_buyers) == sorted([(LEGACY, STACK, 428), (HYBRID, SEAL, 1)])


def assert_recipe(actual, snapshot):
    for key in ("name", "reference product", "location", "unit", "production amount"):
        assert actual[key] == snapshot[key]
    assert len(actual["exchanges"]) == len(snapshot["exchanges"]) == 20
    # Keep repeated utility/waste rows distinct; a dict keyed by identity would
    # silently collapse the expanded HFP, polymer and compounding contributions.
    for observed, expected in zip(actual["exchanges"], snapshot["exchanges"]):
        for field in ("type", "name", "reference product", "location", "unit"):
            assert observed[field] == expected[field]
        assert observed["amount"] == pytest.approx(expected["amount"], rel=1e-14)
        assert (
            observed.get("product", observed["reference product"])
            == expected["reference product"]
        )
        assert not observed.get("formula")
        assert not any(k.startswith("replacement ") for k in observed)


def test_native_workbook_exact_expanded_recipe_quantities_waste_and_profiles(adoption):
    (native,) = ExcelImporter(INVENTORY_DIR / NATIVE).data
    assert_recipe(native, adoption["snapshot"])
    assert native["license"] == "CC-BY-4.0"
    assert native["background version"] == native["biosphere version"] == "3.12"
    assert native["name"] == GASKET_NAME
    assert native["reference product"] == "gasket, FKM"
    assert (native["unit"], native["location"], native["production amount"]) == (
        "kilogram",
        "RER",
        1,
    )
    assert native["type"] == adoption["snapshot"]["type"]
    assert adoption["snapshot"]["comment"] in native["comment"]
    assert adoption["snapshot"]["source"] in native["source"]
    assert purchase([native], GASKET_NAME, "market for vinylidene fluoride")[
        "amount"
    ] == pytest.approx(0.44434630923297097)
    waste = [
        e
        for e in native["exchanges"]
        if e["name"] == "market for hazardous waste, for incineration"
    ]
    assert len(waste) == 2
    assert sum(e["amount"] for e in waste) == pytest.approx(-0.04917459346181717)
    assert all(e["amount"] < 0 for e in waste)
    (production,) = [e for e in native["exchanges"] if e["type"] == "production"]
    assert production["amount"] == 1


def test_real_default_preparation_preserves_native_recipe_and_background(adoption):
    suppliers = {
        (e["name"], e["reference product"], e["location"], e["unit"])
        for e in adoption["snapshot"]["exchanges"]
        if e["type"] == "technosphere"
    }
    background = [
        dict(zip(("name", "reference product", "location", "unit"), key), exchanges=[])
        for key in sorted(suppliers)
    ]
    before = deepcopy(background)
    importer = DefaultInventory(
        background, "3.12", "3.12", INVENTORY_DIR / NATIVE, "cutoff", True
    )
    importer.prepare_inventory()
    assert importer.version_in == importer.version_out == "3.12"
    assert importer.list_unlinked == []
    (prepared,) = importer.import_db.data
    assert_recipe(prepared, adoption["snapshot"])
    assert background == before


@pytest.mark.parametrize("version", ("3.9", "3.11", "3.12"))
@pytest.mark.parametrize("system_model", ("cutoff", "consequential"))
def test_ast_default_selection_keeps_legacy_profile_and_native_before_pfsa_buyer(
    version, system_model
):
    generator = runpy.run_path(
        str(ROOT / "docs/_tools/generate_documentation_reference.py")
    )
    tree = ast.parse((ROOT / "premise/database/new_database.py").read_text())
    selected = generator["inventory_selection"](tree, version, system_model)
    assert selected.count((LEGACY, "3.9")) == 1
    native = [item for item in selected if item[0] == NATIVE]
    if Version(version) < Version("3.12"):
        assert native == []
    else:
        assert native == [(NATIVE, "3.12")]
        i = selected.index((LEGACY, "3.9"))
        assert selected[i - 2 : i] == [(NATIVE, "3.12"), (PFSA, "3.9")]


@pytest.mark.parametrize("version", ("3.9", "3.12"))
def test_native_workbook_change_invalidates_only_import_cache(
    version, tmp_path, monkeypatch
):
    import premise.new_database as module

    obj = object.__new__(module.NewDatabase)
    obj.version = version
    obj.keep_source_db_uncertainty = obj.keep_imports_uncertainty = False
    source_cache = obj._database_cache_path("ALIGN019")
    imports_cache = obj._database_cache_path("ALIGN019", inventories=True)
    workbook = module.FILEPATH_FKM_GASKET_NATIVE_312
    assert workbook == INVENTORY_DIR / NATIVE
    contents = workbook.read_bytes()
    copy = tmp_path / workbook.name
    copy.write_bytes(contents)
    monkeypatch.setattr(module, "FILEPATH_FKM_GASKET_NATIVE_312", copy)
    assert obj._database_cache_path("ALIGN019", inventories=True) == imports_cache
    copy.write_bytes(contents + b"changed expanded native recipe")
    assert obj._database_cache_path("ALIGN019", inventories=True) != imports_cache
    assert obj._database_cache_path("ALIGN019") == source_cache


@pytest.mark.parametrize("version", ("3.9", "3.12"))
def test_actual_caller_scopes_hook_and_passes_all_preloaded_sources_before_preparation(
    raw, version, monkeypatch
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

    extracted, observed, events = {}, [], []
    preloaded = []

    def extract(filepaths):
        for path, _ in filepaths:
            # An identically named standalone gasket is a deliberate decoy:
            # content-based selection would incorrectly try to remove it.
            importer = (
                deepcopy(raw)
                if path.name == LEGACY
                else (
                    ExcelImporter(path)
                    if path.name in (NATIVE, HYBRID, HYDROGEN)
                    else SimpleNamespace(data=[{"name": GASKET_NAME, "exchanges": []}])
                )
            )
            extracted[path] = (importer, state(importer))
            preloaded.append(importer)
        events.append("extracted")
        return preloaded

    def hook(importer, target_version, *, other_importers=()):
        assert importer is extracted[INVENTORY_DIR / LEGACY][0]
        assert target_version == version
        assert len(other_importers) == len(preloaded)
        assert all(a is b for a, b in zip(other_importers, preloaded))
        events.append("hook")
        return use_native_fkm_gasket(importer, target_version, other_importers)

    class ObserveDefaultInventory:
        def __init__(self, **kwargs):
            path = kwargs["path"]
            importer, before = extracted[path]
            assert kwargs["preloaded_importer"] is importer
            if path.name == LEGACY:
                assert events.count("hook") == 1
                expected = [
                    d
                    for d in raw.data
                    if version != "3.12" or d["name"] not in LEGACY_ACTIVITIES
                ]
                assert canonical(importer.data) == canonical(expected)
                assert kwargs["version_in"] == "3.9"
            else:
                assert state(importer) == before
            observed.append((path.name, kwargs["version_in"]))
            self.list_unlinked = []

        def merge_inventory(self):
            return []

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract)
    monkeypatch.setattr(module, "use_native_fkm_gasket", hook)
    monkeypatch.setattr(module, "_select_nmc532_native_312", lambda *args: None)
    monkeypatch.setattr(module, "use_native_ptfe_gasket", lambda *args, **kwargs: 0)
    monkeypatch.setattr(module, "DefaultInventory", ObserveDefaultInventory)
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, "cutoff"
    obj.keep_imports_uncertainty = True
    obj._database = []
    assert obj._NewDatabase__import_inventories() == []
    assert events.count("hook") == 1
    assert observed.count((LEGACY, "3.9")) == 1
    assert observed.count((NATIVE, "3.12")) == (version == "3.12")
