"""Approved D01: modern coating recipes without re-profiling pipeline parents."""

import json
import math
import os
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter
from bw2io.importers.excel import valid_first_cell
from inventory_assertions import recipe_quantities

import premise.new_database as module
from premise.inventory_imports import DefaultInventory
from premise.pipeline_coating import (
    BUYERS,
    LEGACY,
    REFERENCES,
    _identity,
    use_native_pipeline_coating,
)

LEGACY_PATH = module.FILEPATH_HYDROGEN_DISTRI_INVENTORIES
COMPANION = module.FILEPATH_PIPELINE_COATING_NATIVE_312


def extract(path):
    imp = ExcelImporter.__new__(ExcelImporter)
    rows = [(n, r) for n, r in imp.extractor.extract(path) if valid_first_cell(n, r)]
    imp.db_name, imp.metadata = imp.get_database(rows)
    return imp.process_activities(rows)


def worker(version="3.12", model="cutoff"):
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, model
    obj._database = []
    obj.keep_imports_uncertainty = obj.keep_source_db_uncertainty = False
    return obj


@pytest.fixture(scope="module")
def source():
    return extract(LEGACY_PATH)


@pytest.fixture(scope="module")
def companion():
    return extract(COMPANION)


@pytest.fixture(scope="module")
def catalog_bank():
    directory = os.environ.get("PREMISE_REFERENCE_CATALOG_DIR")
    if directory is None:
        try:
            from importlib.resources import files

            directory = files("brightpath").joinpath("data/export/reference_catalogs")
        except ModuleNotFoundError:
            pytest.skip("Native metadata requires brightpath reference catalogs")
    cat = json.loads((Path(directory) / "ecoinvent__3.12__cutoff.json").read_text())[
        "technosphere"
    ]
    rows = [
        {
            "name": r["name"],
            "reference product": r["reference_product"],
            "location": r["location"],
            "unit": r["unit"],
            "exchanges": [],
        }
        for r in cat
    ]
    # Declared existing foreground, imported before the distribution workbook.
    # Metadata identities suffice for linking; no native LCI is read/copied.
    for record in extract(LEGACY_PATH.parent / "lci-carbon-fiber.xlsx"):
        rows.append(
            {k: v for k, v in record.items() if k != "exchanges"} | {"exchanges": []}
        )
    return rows


@pytest.mark.parametrize(
    "version", ["3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.13"]
)
@pytest.mark.parametrize("model", ["cutoff", "consequential", "apos"])
def test_selection_registration_and_original_parent_profile(
    version, model, monkeypatch
):
    selected = []

    class Captured(Exception):
        pass

    def capture(paths):
        selected.extend(paths)
        raise Captured

    monkeypatch.setattr(module, "_extract_default_inventory_importers", capture)
    with pytest.raises(Captured):
        worker(version, model)._NewDatabase__import_inventories()
    assert selected.count((LEGACY_PATH, "3.7")) == 1
    assert selected.count((COMPANION, "3.12")) == int(
        (version, model) == ("3.12", "cutoff")
    )
    if (version, model) == ("3.12", "cutoff"):
        assert selected.index((COMPANION, "3.12")) + 1 == selected.index(
            (LEGACY_PATH, "3.7")
        )


@pytest.mark.parametrize(
    "version,model",
    [
        ("3.7", "cutoff"),
        ("3.11", "cutoff"),
        ("3.12", "consequential"),
        ("3.13", "cutoff"),
    ],
)
def test_older_and_other_models_are_exact_noops(source, version, model):
    imp = SimpleNamespace(data=deepcopy(source))
    assert use_native_pipeline_coating(imp, version, model, []) == 0
    assert imp.data == source


def test_two_exact_purchases_preserve_entire_other_recipe_and_uncertainty(
    source, companion
):
    imp = SimpleNamespace(data=deepcopy(source))
    assert use_native_pipeline_coating(imp, "3.12", "cutoff", companion) == 2
    before = {_identity(r): r for r in source}
    after = {_identity(r): r for r in imp.data}
    assert set(after) == set(before) - {LEGACY}
    for identity, old in before.items():
        if identity == LEGACY:
            continue
        new = after[identity]
        if identity not in [buyer[:4] for buyer in BUYERS]:
            assert new == old
            continue
        changed = 0
        for a, b in zip(old["exchanges"], new["exchanges"], strict=True):
            if _identity(a) == LEGACY:
                changed += 1
                index = next(
                    i for i, buyer in enumerate(BUYERS) if identity == buyer[:4]
                )
                assert _identity(b) == REFERENCES[index]
                assert a["amount"] == b["amount"] == BUYERS[index][4]
                assert {
                    k: v
                    for k, v in a.items()
                    if k not in {"name", "product", "reference product", "location"}
                } == {
                    k: v
                    for k, v in b.items()
                    if k not in {"name", "product", "reference product", "location"}
                }
            else:
                assert a == b
        assert changed == 1
    repeated = deepcopy(imp.data)
    assert use_native_pipeline_coating(imp, "3.12", "cutoff", companion) == 0
    assert imp.data == repeated


@pytest.mark.parametrize(
    "problem",
    [
        "extra_buyer",
        "dose",
        "formula",
        "buyer_location",
        "legacy_recipe",
        "missing_reference",
        "duplicate_reference",
        "reference_recipe",
        "product_conflict",
        "partial",
    ],
)
def test_invalid_or_expanded_scope_fails_atomically(source, companion, problem):
    data, bank = deepcopy(source), deepcopy(companion)
    buyer = next(r for r in data if r["name"] == BUYERS[0][0])
    edge = next(e for e in buyer["exchanges"] if e["name"] == LEGACY[0])
    if problem == "extra_buyer":
        extra = deepcopy(buyer)
        extra["name"] += ", outside scope"
        data.append(extra)
    elif problem == "dose":
        edge["amount"] += 1
    elif problem == "formula":
        edge["formula"] = "reviewed_dose"
    elif problem == "buyer_location":
        buyer["location"] = "GLO"
    elif problem == "legacy_recipe":
        next(r for r in data if r["name"] == LEGACY[0])["exchanges"][1]["amount"] += 1
    elif problem == "missing_reference":
        bank.pop()
    elif problem == "duplicate_reference":
        bank.append(deepcopy(bank[0]))
    elif problem == "reference_recipe":
        bank[0]["exchanges"][1]["amount"] += 1
    elif problem == "product_conflict":
        edge["product"] = "different product"
    elif problem == "partial":
        edge.update(
            name=REFERENCES[0][0],
            **{"reference product": REFERENCES[0][1]},
            location="RER",
        )
    snapshot = deepcopy(data)
    imp = SimpleNamespace(data=data)
    with pytest.raises(ValueError):
        use_native_pipeline_coating(imp, "3.12", "cutoff", bank)
    assert imp.data == snapshot


@pytest.mark.parametrize(
    "version,model", [("3.7", "cutoff"), ("3.12", "cutoff"), ("3.12", "consequential")]
)
def test_cache_tracks_parent_companion_and_full_adapter_only_when_selected(
    version, model, monkeypatch, tmp_path
):
    obj = worker(version, model)
    native = obj._database_cache_path("source")
    modern = (version, model) == ("3.12", "cutoff")
    for attr, expected in [
        ("FILEPATH_HYDROGEN_DISTRI_INVENTORIES", True),
        ("FILEPATH_PIPELINE_COATING_NATIVE_312", modern),
    ]:
        original = getattr(module, attr)
        copied = tmp_path / original.name
        copied.write_bytes(original.read_bytes())
        monkeypatch.setattr(module, attr, copied)
        before = obj._database_cache_path("source", inventories=True)
        copied.write_bytes(copied.read_bytes() + b"changed source")
        assert (
            obj._database_cache_path("source", inventories=True) != before
        ) == expected
        assert obj._database_cache_path("source") == native
    adapter = tmp_path / "pipeline_coating.py"
    adapter.write_text("adapter revision one")
    original_getfile = module.inspect.getfile
    monkeypatch.setattr(
        module.inspect,
        "getfile",
        lambda value: (
            str(adapter)
            if value is use_native_pipeline_coating
            else original_getfile(value)
        ),
    )
    before = obj._database_cache_path("source", inventories=True)
    adapter.write_text("adapter revision two")
    assert (obj._database_cache_path("source", inventories=True) != before) == modern
    assert obj._database_cache_path("source") == native


def semantic(exchange):
    return {
        k: v
        for k, v in exchange.items()
        if not (isinstance(v, float) and math.isnan(v))
    }


@pytest.mark.parametrize("keep_uncertainty", [False, True])
def test_actual_ordered_caller_migration_and_no_unrelated_buyer_changes(
    source, companion, catalog_bank, keep_uncertainty, monkeypatch
):
    for name in (
        "_select_nmc955_modern_references",
        "_select_nmc532_native_312",
        "use_native_ptfe_gasket",
        "use_native_fkm_gasket",
    ):
        monkeypatch.setattr(module, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_extract_default_inventory_importers",
        lambda paths: [
            SimpleNamespace(data=extract(p) if p in (LEGACY_PATH, COMPANION) else [])
            for p, _ in paths
        ],
    )
    seen = []

    class Prepared:
        def __init__(self, **kwargs):
            self.data, self.list_unlinked = [], []
            if kwargs["path"] not in (LEGACY_PATH, COMPANION):
                return
            seen.append((kwargs["path"], kwargs["version_in"]))
            actual = DefaultInventory(**kwargs)
            actual.prepare_inventory()
            self.data, self.list_unlinked = actual.import_db.data, actual.list_unlinked

        def merge_inventory(self):
            return self.data

    baseline = DefaultInventory(
        database=deepcopy(catalog_bank),
        version_in="3.7",
        version_out="3.12",
        path=LEGACY_PATH,
        system_model="cutoff",
        keep_uncertainty_data=keep_uncertainty,
        preloaded_importer=SimpleNamespace(data=deepcopy(source)),
    )
    baseline.prepare_inventory()
    assert not baseline.list_unlinked
    monkeypatch.setattr(module, "DefaultInventory", Prepared)
    obj = worker()
    obj.keep_imports_uncertainty = keep_uncertainty
    obj._database = deepcopy(catalog_bank)
    imported = obj._NewDatabase__import_inventories()
    assert seen == [(COMPANION, "3.12"), (LEGACY_PATH, "3.7")]
    assert len(imported) == len(source) + 1
    before, after = (
        {_identity(r): r for r in data} for data in (baseline.import_db.data, imported)
    )
    assert set(after) == set(before) - {LEGACY} | set(REFERENCES)
    companion_by_identity = {_identity(record): record for record in companion}
    for identity in REFERENCES:
        assert recipe_quantities(after[identity]) == recipe_quantities(
            companion_by_identity[identity]
        )
    for identity, old in before.items():
        if identity == LEGACY:
            continue
        new = after[identity]
        if identity not in [b[:4] for b in BUYERS]:
            assert {k: v for k, v in old.items() if k != "code"} == {
                k: v for k, v in new.items() if k != "code"
            }
            continue
        for a, b in zip(old["exchanges"], new["exchanges"], strict=True):
            if a["name"] != LEGACY[0]:
                assert semantic(a) == semantic(b)
                continue
            assert semantic(
                {
                    k: v
                    for k, v in a.items()
                    if k
                    not in {"name", "product", "reference product", "location", "input"}
                }
            ) == semantic(
                {
                    k: v
                    for k, v in b.items()
                    if k
                    not in {"name", "product", "reference product", "location", "input"}
                }
            )
            assert sum(_identity(r) == _identity(b) for r in obj._database) == 1
    assert all(
        len(r["exchanges"]) == 29 for r in imported if _identity(r) in REFERENCES
    )
