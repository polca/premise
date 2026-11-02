"""EPR native M5 selection, ordinary deduplication and legacy preservation.

Uses packaged native supplier metadata and real DefaultInventory preparation;
no licensed native inventories, IAM builds or LCIA calculations are needed.
"""

import json
import os
from pathlib import Path
from importlib.resources import files
from types import SimpleNamespace

import pytest
from bw2io import ExcelImporter
from bw2io.importers.excel import valid_first_cell
import premise.new_database as module
from premise.inventory_imports import DefaultInventory

LEGACY = module.FILEPATH_NUCLEAR_EPR
COMPANION = module.FILEPATH_EPR_M5_NATIVE_312
M5 = "zirconium cladding alloy production, epr, m5 type"
KROLL = "zirconium metal production, cladding, kroll process"
NB = "niobium metal production, ferroniobium separation"
ASSEMBLY = "fuel assembly component production, epr, without nuclear fuel"
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
    data = imp.extractor.extract(path)
    data = [(n, r) for n, r in data if valid_first_cell(n, r)]
    imp.db_name, imp.metadata = imp.get_database(data)
    return imp.process_activities(data)


def worker(version="3.12", model="cutoff"):
    obj = object.__new__(module.NewDatabase)
    obj.version, obj.system_model = version, model
    obj._database = []
    obj.keep_imports_uncertainty = obj.keep_source_db_uncertainty = False
    return obj


def key(r):
    return tuple(
        r.get(k, r.get("product") if k == "reference product" else None)
        for k in ("name", "reference product", "location", "unit")
    )


def byname(data, name):
    (record,) = [r for r in data if r["name"] == name]
    return record


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("model", MODELS)
def test_actual_registration_scope_order_and_cache(
    version, model, monkeypatch, tmp_path
):
    observed = []

    class Done(Exception):
        pass

    def capture(paths):
        observed.extend(paths)
        raise Done

    monkeypatch.setattr(module, "_extract_default_inventory_importers", capture)
    monkeypatch.setattr(module, "FILEPATH_EPR_M5_NATIVE_312", COMPANION)
    obj = worker(version, model)
    with pytest.raises(Done):
        obj._NewDatabase__import_inventories()
    modern = (version, model) == ("3.12", "cutoff")
    assert observed.count((LEGACY, "3.8")) == 1
    assert observed.count((COMPANION, "3.12")) == int(modern)
    if modern:
        assert observed.index((COMPANION, "3.12")) + 1 == observed.index(
            (LEGACY, "3.8")
        )
    companion = tmp_path / COMPANION.name
    companion.write_bytes(COMPANION.read_bytes())
    monkeypatch.setattr(module, "FILEPATH_EPR_M5_NATIVE_312", companion)
    before = obj._database_cache_path("source", inventories=True)
    native = obj._database_cache_path("source")
    companion.write_bytes(companion.read_bytes() + b"changed")
    assert (obj._database_cache_path("source", inventories=True) != before) == modern
    assert obj._database_cache_path("source") == native


@pytest.mark.parametrize("keep_uncertainty", [False, True])
def test_real_ordered_newdatabase_preparation_and_catalog_closure(
    monkeypatch, keep_uncertainty, tmp_path, catalog_dir
):
    monkeypatch.setattr(module, "FILEPATH_EPR_M5_NATIVE_312", COMPANION)
    # Isolate unrelated selectors but exercise the real NewDatabase caller and
    # real DefaultInventory preparation for both EPR sources, in order.
    monkeypatch.setattr(
        module.inventory_profiles, "select_companion_profiles", lambda *args: None
    )
    # This source-isolated caller supplies no pipeline coating importers.

    def extract_sources(paths):
        return [
            SimpleNamespace(data=extract(p) if p in (LEGACY, COMPANION) else [])
            for p, _ in paths
        ]

    monkeypatch.setattr(module, "_extract_default_inventory_importers", extract_sources)
    catalog = json.loads((catalog_dir / "ecoinvent__3.12__cutoff.json").read_text())[
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
        for r in catalog
    ]
    # Compare the whole prepared legacy EPR source against its modern view.
    # Use the same actual DefaultInventory pipeline and native metadata bank.
    baseline = DefaultInventory(
        database=list(database),
        version_in="3.8",
        version_out="3.12",
        path=LEGACY,
        system_model="cutoff",
        keep_uncertainty_data=keep_uncertainty,
        preloaded_importer=SimpleNamespace(data=extract(LEGACY)),
    )
    baseline.prepare_inventory()
    assert baseline.list_unlinked == []
    seen = []

    class Prepared:
        def __init__(self, **kwargs):
            self.data, self.list_unlinked = [], []
            if kwargs["path"] in (LEGACY, COMPANION):
                seen.append((str(kwargs["path"]), kwargs["version_in"]))
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
    assert seen == [(str(COMPANION), "3.12"), (str(LEGACY), "3.8")]
    before_by_identity = {key(r): r for r in baseline.import_db.data}
    after_by_identity = {key(r): r for r in imported}
    assert len(before_by_identity) == len(baseline.import_db.data)
    assert len(after_by_identity) == len(imported)
    assert before_by_identity.keys() == after_by_identity.keys()

    def without_generated_code(record):
        return {k: v for k, v in record.items() if k != "code"}

    bio_counts = []
    for identity, old_record in before_by_identity.items():
        new_record = after_by_identity[identity]
        old_bio = [e for e in old_record["exchanges"] if e["type"] == "biosphere"]
        new_bio = [e for e in new_record["exchanges"] if e["type"] == "biosphere"]
        # Equality covers identity, amount, sign, categories, input link and
        # all preserved uncertainty fields, including zero-valued flows.
        assert old_bio == new_bio, identity
        if old_record["name"] != M5:
            assert without_generated_code(old_record) == without_generated_code(
                new_record
            ), identity
        bio_counts.append(
            {"identity": identity, "before": len(old_bio), "after": len(new_bio)}
        )
    biosphere_proof = {
        "status": "LIVE_SCOPED_VALIDATION",
        "actual_DefaultInventory_preparation": True,
        "keep_uncertainty": keep_uncertainty,
        "full_legacy_record_count": len(before_by_identity),
        "final_modern_record_count": len(after_by_identity),
        "non_M5_records_unchanged_except_generated_code": len(before_by_identity) - 1,
        "all_biosphere_exchange_dicts_exactly_equal": True,
        "before_biosphere_exchange_count": sum(r["before"] for r in bio_counts),
        "after_biosphere_exchange_count": sum(r["after"] for r in bio_counts),
        "unexpected_biosphere_drops": 0,
        "Kroll_CO2_retained_in_qualified_inactive_legacy_provider": True,
        "Kroll_active_technosphere_buyers": 0,
        "authored_sources": [[str(COMPANION), "3.12"], [str(LEGACY), "3.8"]],
        "four_fuel_buyers_authored_source": "unchanged legacy3.8 workbook",
        "record_biosphere_counts": bio_counts,
    }
    (tmp_path / f"full-EPR-biosphere-uncertainty-{keep_uncertainty}.json").write_text(
        json.dumps(biosphere_proof, indent=2)
    )
    source_alloy = byname(extract(LEGACY), M5)
    authored_alloy = byname(extract(COMPANION), M5)
    assert key(source_alloy) == key(authored_alloy)
    assert source_alloy["production amount"] == authored_alloy["production amount"] == 1
    assert [
        e
        for e in source_alloy["exchanges"]
        if e["type"] != "production" and e["name"] != KROLL
    ] == [
        e
        for e in authored_alloy["exchanges"]
        if e["type"] != "production"
        and e["name"] != "market for zirconium sponge, nuclear-grade"
    ]
    component = byname(imported, ASSEMBLY)
    assert (
        next(e["amount"] for e in component["exchanges"] if e["name"] == M5)
        == 0.8923034558722291
    )
    buyers = [
        e
        for r in imported
        for e in r["exchanges"]
        if e["type"] == "technosphere" and e["name"] == ASSEMBLY
    ]
    assert len(buyers) == 4 and all(e["amount"] == 0.344218009478673 for e in buyers)
    alloy = byname(imported, M5)
    niobium = byname(imported, NB)
    assert sum(r["name"] == KROLL for r in imported) == 1
    assert not any(
        e["name"] == KROLL
        for r in imported
        for e in r["exchanges"]
        if e["type"] == "technosphere"
    )
    assert sum(r["name"] == M5 for r in imported) == 1
    # Resolve native and foreground products uniquely in the complete prepared
    # view. Native records contain metadata only, no proprietary coefficients.
    for record in (alloy, niobium, byname(imported, ASSEMBLY)):
        for e in record["exchanges"]:
            if e["type"] == "technosphere":
                matches = [r for r in obj._database if key(r) == key(e)]
                assert len(matches) == 1, (record["name"], key(e), len(matches))
    (sponge,) = [
        e
        for e in alloy["exchanges"]
        if e["name"] == "market for zirconium sponge, nuclear-grade"
    ]
    assert sponge["amount"] == 0.9884
    (
        tmp_path / f"real-preparation-proof-uncertainty-{keep_uncertainty}.json"
    ).write_text(
        json.dumps(
            {
                "status": "LIVE_SCOPED_VALIDATION",
                "sources": seen,
                "imported_record_count": len(imported),
                "M5_count": 1,
                "Kroll_count": 1,
                "Kroll_active_buyers": 0,
                "ordinary_duplicate_suppression": True,
                "keep_uncertainty": keep_uncertainty,
                "native_or_foreground_closure": True,
                "sponge_amount": sponge["amount"],
                "LCIA_or_full_IAM_run": False,
            },
            indent=2,
        )
    )


def test_catalog_five_target_evidence(tmp_path, catalog_dir):
    evidence = {}
    for version in VERSIONS:
        cat = json.loads(
            (catalog_dir / f"ecoinvent__{version}__cutoff.json").read_text()
        )["technosphere"]
        hits = [
            r
            for r in cat
            if r["name"] == "market for zirconium sponge, nuclear-grade"
            and r["reference_product"] == "zirconium sponge, nuclear-grade"
            and r["location"] == "GLO"
            and r["unit"] == "kilogram"
        ]
        evidence[version] = {
            "sponge_exact_matches": hits,
            "companion_selected": version == "3.12",
        }
        assert len(hits) == (0 if version == "3.8" else 1)
    (tmp_path / "catalog-five-targets.json").write_text(json.dumps(evidence, indent=2))
