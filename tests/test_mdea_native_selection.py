"""Qualified MDEA burden proxy: scoped source selection and boundary preservation.

The retained coefficient is a legacy burden assumption, not measured make-up,
solution composition or promoter demand. Native supplier data are metadata only.
"""

import json
import math
import os
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter
from bw2io.importers.excel import valid_first_cell
import premise.new_database as module
from premise.inventory_imports import DefaultInventory

LEGACY = module.FILEPATH_CC_INVENTORIES
COMPANION = module.FILEPATH_BIOMETHANE_CAPTURE_MDEA_312
CAPTURE = "carbon dioxide, captured and stored, from a hydrogen production plant using steam methane reforming of biomethane"
STORAGE = "carbon dioxide compression, transport and storage"
MDEA = "market for methyldiethanolamine"
VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12")
MODELS = ("cutoff", "consequential")


@pytest.fixture(scope="module")
def catalog_dir():
    override = os.environ.get("PREMISE_REFERENCE_CATALOG_DIR")
    if override:
        return Path(override)
    try:
        return files("brightpath").joinpath("data", "export", "reference_catalogs")
    except ModuleNotFoundError:
        pytest.skip(
            "Native metadata checks require brightpath or PREMISE_REFERENCE_CATALOG_DIR"
        )


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


def key(record):
    return tuple(
        record.get(k, record.get("product") if k == "reference product" else None)
        for k in ("name", "reference product", "location", "unit")
    )


def semantic_exchange(exchange):
    # Migrations materialize empty uncertainty columns as NaN and a default
    # deterministic loc. The companion's writer omits them. Compare executable
    # fields, retaining real distribution parameters and the resolved input.
    result = {
        k: v
        for k, v in exchange.items()
        if k != "database" and not (isinstance(v, float) and math.isnan(v))
    }
    if result.get("uncertainty type", 0) in (None, 0):
        result.pop("uncertainty type", None)
        if result.get("loc") == result.get("amount"):
            result.pop("loc", None)
    return result


def one(records, name):
    (record,) = [r for r in records if r["name"] == name]
    return record


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("model", MODELS)
def test_registration_and_cache_only_modern_cutoff(
    version, model, monkeypatch, tmp_path
):
    selected = []

    class Captured(Exception):
        pass

    def capture(paths):
        selected.extend(paths)
        raise Captured

    monkeypatch.setattr(module, "_extract_default_inventory_importers", capture)
    obj = worker(version, model)
    with pytest.raises(Captured):
        obj._NewDatabase__import_inventories()
    modern = (version, model) == ("3.12", "cutoff")
    assert selected.count((LEGACY, "3.9")) == 1
    assert selected.count((COMPANION, "3.12")) == int(modern)
    if modern:
        assert selected.index((COMPANION, "3.12")) + 1 == selected.index(
            (LEGACY, "3.9")
        )
    copy = tmp_path / COMPANION.name
    copy.write_bytes(COMPANION.read_bytes())
    monkeypatch.setattr(module, "FILEPATH_BIOMETHANE_CAPTURE_MDEA_312", copy)
    before = obj._database_cache_path("source", inventories=True)
    native = obj._database_cache_path("source")
    copy.write_bytes(copy.read_bytes() + b"changed reference")
    assert (obj._database_cache_path("source", inventories=True) != before) == modern
    assert obj._database_cache_path("source") == native


@pytest.mark.parametrize("keep_uncertainty", [False, True])
def test_real_ordered_preparation_biosphere_and_storage(
    monkeypatch, tmp_path, catalog_dir, keep_uncertainty
):
    # Isolate unrelated sources while using the actual constructor caller and
    # real DefaultInventory preparation for the complete carbon-capture book.
    monkeypatch.setattr(module, "_select_nmc955_modern_references", lambda *args: None)
    monkeypatch.setattr(module, "_select_nmc532_native_312", lambda *args: None)
    monkeypatch.setattr(module, "use_native_ptfe_gasket", lambda *args: None)
    # This source-isolated caller supplies no pipeline coating importers.
    monkeypatch.setattr(module, "use_native_pipeline_coating", lambda *args: None)
    monkeypatch.setattr(module, "use_native_fkm_gasket", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_extract_default_inventory_importers",
        lambda paths: [
            SimpleNamespace(data=extract(p) if p in (LEGACY, COMPANION) else [])
            for p, _ in paths
        ],
    )
    cat = json.loads((catalog_dir / "ecoinvent__3.12__cutoff.json").read_text())[
        "technosphere"
    ]
    database = [
        {
            "name": r["name"],
            "reference product": r["reference_product"],
            "location": r["location"],
            "unit": r["unit"],
            "exchanges": [],
        }
        for r in cat
    ]
    baseline = DefaultInventory(
        database=list(database),
        version_in="3.9",
        version_out="3.12",
        path=LEGACY,
        system_model="cutoff",
        keep_uncertainty_data=keep_uncertainty,
        preloaded_importer=SimpleNamespace(data=extract(LEGACY)),
    )
    baseline.prepare_inventory()
    assert baseline.list_unlinked == []
    observed = []

    class Prepared:
        def __init__(self, **kwargs):
            self.data, self.list_unlinked = [], []
            if kwargs["path"] in (LEGACY, COMPANION):
                observed.append((str(kwargs["path"]), kwargs["version_in"]))
                actual = DefaultInventory(**kwargs)
                actual.prepare_inventory()
                self.data, self.list_unlinked = (
                    actual.import_db.data,
                    actual.list_unlinked,
                )

        def merge_inventory(self):
            return self.data

    monkeypatch.setattr(module, "DefaultInventory", Prepared)
    obj = worker()
    obj._database = database
    obj.keep_imports_uncertainty = keep_uncertainty
    imported = obj._NewDatabase__import_inventories()
    assert observed == [(str(COMPANION), "3.12"), (str(LEGACY), "3.9")]
    before, after = (
        {key(r): r for r in records} for records in (baseline.import_db.data, imported)
    )
    assert len(before) == len(baseline.import_db.data)
    assert len(after) == len(imported) and before.keys() == after.keys()
    biosphere_count = 0
    for identity, old in before.items():
        new = after[identity]
        old_bio = [e for e in old["exchanges"] if e["type"] == "biosphere"]
        assert [semantic_exchange(e) for e in old_bio] == [
            semantic_exchange(e) for e in new["exchanges"] if e["type"] == "biosphere"
        ], identity
        biosphere_count += len(old_bio)
        if old["name"] != CAPTURE:
            assert {k: v for k, v in old.items() if k != "code"} == {
                k: v for k, v in new.items() if k != "code"
            }, identity
    old, new = one(baseline.import_db.data, CAPTURE), one(imported, CAPTURE)
    assert len(old["exchanges"]) == len(new["exchanges"]) == 10
    for old_edge, new_edge in zip(old["exchanges"], new["exchanges"], strict=True):
        if new_edge["name"] == MDEA:
            assert {
                k: v
                for k, v in semantic_exchange(old_edge).items()
                if k
                not in ("name", "product", "reference product", "location", "comment")
            } == {
                k: v
                for k, v in semantic_exchange(new_edge).items()
                if k
                not in ("name", "product", "reference product", "location", "comment")
            }
            assert (new_edge["amount"], new_edge["location"], new_edge["unit"]) == (
                0.000034,
                "RER",
                "kilogram",
            )
        else:
            assert semantic_exchange(old_edge) == semantic_exchange(new_edge)
    assert sum(e["name"] == MDEA for e in new["exchanges"]) == 1
    assert not any("transport, freight" in e["name"] for e in new["exchanges"])
    storage = one(imported, STORAGE)
    (edge,) = [e for e in new["exchanges"] if e["name"] == STORAGE]
    assert edge["amount"] == 1 and key(edge) == key(storage)
    for e in new["exchanges"]:
        if e["type"] == "technosphere":
            assert sum(key(r) == key(e) for r in obj._database) == 1, key(e)
    (tmp_path / f"full-capture-proof-{keep_uncertainty}.json").write_text(
        json.dumps(
            {
                "live_DefaultInventory": True,
                "keep_uncertainty": keep_uncertainty,
                "sources": observed,
                "before_records": len(before),
                "after_records": len(after),
                "other_records_unchanged_except_generated_code": len(before) - 1,
                "biosphere_exchanges_exactly_preserved": biosphere_count,
                "unexpected_biosphere_drops": 0,
                "one_capture_recipe": True,
                "one_storage_purchase": True,
                "MDEA_kg_per_kg_CO2_legacy_burden_proxy": 0.000034,
                "no_added_MDEA_delivery_freight": True,
                "native_and_foreground_unique_closure": True,
            },
            indent=2,
        )
    )


def test_companion_qualified_contract(catalog_dir):
    (record,) = extract(COMPANION)
    assert (record["name"], record["unit"], record["production amount"]) == (
        CAPTURE,
        "kilogram",
        1,
    )
    assert "not a measured neat-MDEA make-up rate" in record["comment"]
    assert "activator/promoter composition" in record["comment"]
    (edge,) = [e for e in record["exchanges"] if e["name"] == MDEA]
    cat = json.loads((catalog_dir / "ecoinvent__3.12__cutoff.json").read_text())[
        "technosphere"
    ]
    assert (
        sum(
            r["name"] == MDEA
            and r["reference_product"] == "methyldiethanolamine"
            and r["location"] == "RER"
            and r["unit"] == "kilogram"
            for r in cat
        )
        == 1
    )
    assert edge["amount"] == 0.000034
