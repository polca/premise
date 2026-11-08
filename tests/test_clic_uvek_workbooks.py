"""Owned workbook graphs use separate source and UVEK destination contexts."""

from copy import deepcopy
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from premise.inventory import clic_hybrid as hybrid, clic_user
from premise.inventory.clic_archive import ClicInventoryError


@pytest.fixture
def identity_migration(monkeypatch):
    """A synthetic exact migration isolates graph adaptation from rule science."""
    from brightpath import BrightwayInventory
    from brightpath.core.policies import PolicyAction

    def migrate(inventory, target, *, policy, **kwargs):
        assert policy.on_heuristic_mapping == PolicyAction.ERROR
        assert policy.validate_source and policy.validate_target
        assert policy.minimum_coverage == 1.0
        assert target.technosphere.family == "uvek"
        assert target.biosphere.version == "3.10"
        return SimpleNamespace(data=inventory.data, last_migration_report=None)

    monkeypatch.setattr(BrightwayInventory, "migrate_background", migrate)


def activity(name, **fields):
    return {
        "name": name,
        "reference product": name,
        "location": "GLO",
        "unit": "kilogram",
        "database": "source",
        "code": name,
        "exchanges": [
            {
                "name": name,
                "product": name,
                "location": "GLO",
                "unit": "kilogram",
                "type": "production",
                "amount": 1,
            }
        ],
        **fields,
    }


def purchase(name, **fields):
    return {
        "name": name,
        "product": name,
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": -3.5,
        "formula": "-dose",
        "uncertainty type": 3,
        "loc": -3.5,
        "scale": 0.2,
        **fields,
    }


@pytest.fixture
def catalogs():
    from brightpath import BiosphereProfile, TechnosphereProfile
    from brightpath.background import (
        BiosphereCatalog,
        InMemoryCatalogProvider,
        TechnosphereCatalog,
    )

    emission = ("Carbon dioxide, fossil", ("air",), "kilogram")
    return InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                TechnosphereProfile(family, version, "cutoff"), frozenset()
            )
            for family, version in (("ecoinvent", "3.12"), ("uvek", "2026"))
        ],
        biosphere=[
            BiosphereCatalog(
                BiosphereProfile("ecoinvent", version),
                frozenset({emission}),
                references={emission: "11111111-1111-4111-8111-111111111111"},
            )
            for version in ("3.12", "3.10")
        ],
    )


@pytest.fixture
def retained(monkeypatch, tmp_path):
    path = tmp_path / "retained.xlsx"
    path.write_bytes(b"synthetic retained workbook")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    root, private = activity("root"), activity("private")
    root["parameters"] = [{"name": "dose", "amount": 3.5}]
    root["exchanges"] += [
        purchase("private"),
        purchase("public"),
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air",),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.7,
        },
    ]
    data = [root, private, activity("unused")]
    public = activity(
        "public [CLIC published]", database="premise-CLIC", code="public-code"
    )
    public["reference product"] = "public"
    public["clic provenance"] = {
        "slug": "published",
        "activity_code": "public-code",
        "name": "public",
        "reference_product": "public",
        "location": "GLO",
        "unit": "kilogram",
    }
    nodes = [
        {
            "id": i,
            "file": path.name,
            "identity": list(hybrid.activity_identity(record)),
            "clic_slug": None,
            "inputs": [],
            "science_hash": str(i),
        }
        for i, record in enumerate(data)
    ]
    nodes.append(
        {
            "id": 3,
            "file": path.name,
            "identity": ["public", "public", "GLO", "kilogram"],
            "clic_slug": "published",
            "inputs": [],
            "science_hash": "public",
        }
    )
    specification = {
        "sector_mappings": {},
        "hybrid_sources": {
            "nodes": nodes,
            "files": {path.name: {"sha256": digest}},
            "roles": {},
        },
    }
    monkeypatch.setattr(hybrid, "registry", lambda: specification)
    monkeypatch.setattr(clic_user, "registry", lambda: specification)
    import bw2io

    monkeypatch.setattr(
        bw2io, "ExcelImporter", lambda *a: SimpleNamespace(data=deepcopy(data))
    )
    from premise.inventory import inventory_imports

    monkeypatch.setattr(
        inventory_imports,
        "DefaultInventory",
        Mock(side_effect=AssertionError("legacy constructor")),
    )
    plan = hybrid.HybridInventoryPlan(
        (),
        frozenset({0, 1}),
        ((path, "3.12"),),
        frozenset(),
        "plan",
        {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"},
    )
    return plan, data, public


def test_retained_uvek_migrates_selected_records_and_preserves_science(
    retained, catalogs, tmp_path, identity_migration
):
    plan, raw, public = retained
    before = deepcopy(raw)
    background = [public]
    options = dict(
        version="2026",
        family="uvek",
        system_model="cutoff",
        biosphere_name="actual-biosphere",
        catalog_provider=catalogs,
        cache_dir=tmp_path / "cache",
    )
    records, metadata, _ = hybrid.import_retained_inventories(
        plan, background, [public], **options
    )
    assert len(records) == 2 and len(background) == 3
    root = next(a for a in records if a["name"] == "root")
    private = next(a for a in records if a["name"] == "private")
    assert root["exchanges"][1]["input"] == (private["database"], private["code"])
    assert root["exchanges"][2]["input"] == (public["database"], public["code"])
    assert root["exchanges"][2]["name"] == public["name"]
    assert root["exchanges"][3]["input"] == (
        "actual-biosphere",
        "11111111-1111-4111-8111-111111111111",
    )
    for edge in root["exchanges"][1:3]:
        assert {
            k: edge[k]
            for k in ("amount", "formula", "uncertainty type", "loc", "scale")
        } == {
            "amount": -3.5,
            "formula": "-dose",
            "uncertainty type": 3,
            "loc": -3.5,
            "scale": 0.2,
        }
    assert root["parameters"] == raw[0]["parameters"]
    assert raw == before
    assert all(hybrid.managed_inventory_key(a)[0] == "workbook" for a in records)
    provenance = root["inventory provenance"]
    assert provenance["target"]["technosphere"]["family"] == "uvek"
    assert provenance["target"]["biosphere"]["version"] == "3.10"
    assert provenance["migration_policy"]["minimum_coverage"] == 1
    assert (
        "migration.heuristic_mapping"
        not in provenance["migration_policy"]["permitted_information_losses"]
    )
    assert (
        metadata[0]["source_version"] == "3.12"
        and metadata[0]["target_family"] == "uvek"
    )
    root["exchanges"][1]["amount"] = 123
    repeated, _, _ = hybrid.import_retained_inventories(
        plan, [public], [public], **options
    )
    assert (
        next(a for a in repeated if a["name"] == "root")["exchanges"][1]["amount"]
        == -3.5
    )
    assert all(
        c["cache_hit"] for c in repeated[0]["inventory provenance"]["migration_cohorts"]
    )


def test_missing_selected_record_cannot_be_silently_omitted(
    retained, monkeypatch, catalogs, tmp_path
):
    plan, raw, public = retained
    raw.pop(1)
    with pytest.raises(ClicInventoryError, match="lost selected records"):
        hybrid.import_retained_inventories(
            plan,
            [public],
            [public],
            version="2026",
            family="uvek",
            system_model="cutoff",
            catalog_provider=catalogs,
            cache_dir=tmp_path,
        )


def test_plan_uses_reviewed_source_baseline_separate_from_target(retained, monkeypatch):
    plan, _, _ = retained
    from premise.inventory import profiles

    observed = []
    monkeypatch.setattr(
        profiles,
        "profiles",
        lambda version, model: observed.append((version, model)) or [],
    )
    monkeypatch.setattr(hybrid, "required_roots", lambda *a, **k: ())
    selected = hybrid.plan_hybrid_inventories(
        [],
        [],
        plan.sources,
        version="2026",
        family="uvek",
        source_version="3.12",
        system_model="cutoff",
        extra_nodes=[0],
    )
    assert selected.source_context == {
        "family": "ecoinvent",
        "version": "3.12",
        "system_model": "cutoff",
    }
    assert selected.selected_nodes == frozenset({0})
    assert observed and set(observed) == {("3.12", "cutoff")}


def test_user_uvek_discovers_public_root_without_legacy_migration(
    monkeypatch, tmp_path, catalogs, identity_migration
):
    from premise.inventory import inventory_imports
    from premise.inventory.clic_archive import ClicRecipe
    from test_clic_archive import catalog, native

    path = tmp_path / "user.xlsx"
    path.write_bytes(b"synthetic user inventory")
    raw = activity("user")
    raw["exchanges"].append(purchase("public"))
    raw["parameters"] = [{"name": "dose", "amount": 3.5}]
    importer = SimpleNamespace(data=[raw])
    monkeypatch.setattr(inventory_imports, "ExcelImporter", lambda *a: importer)
    monkeypatch.setattr(
        inventory_imports,
        "migrate_import_db",
        Mock(side_effect=AssertionError("legacy migration")),
    )
    monkeypatch.setattr(
        clic_user, "registry", lambda: {"hybrid_sources": {"nodes": []}}
    )
    user = clic_user.ClicUserInventories(
        [{"filepath": path, "ecoinvent version": "3.12"}],
        [],
        version="2026",
        family="uvek",
        system_model="cutoff",
        catalog_provider=catalogs,
        cache_dir=tmp_path,
        biosphere_name="actual-bio",
    )
    assert user.required_roots(
        catalog(native(1, 1, "public")), ClicRecipe.read(None), []
    ) == {"dataset-1"}
    public = activity(
        "public [CLIC dataset-1]", database="premise-CLIC", code="public-code"
    )
    public["reference product"] = "public"
    public["clic provenance"] = {
        "slug": "dataset-1",
        "activity_code": "public-code",
        "name": "public",
        "reference_product": "public",
        "location": "GLO",
        "unit": "kilogram",
    }
    records, metadata = user.prepare([public], [public])
    edge = records[0]["exchanges"][1]
    assert edge["input"] == ("premise-CLIC", "public-code")
    assert (
        edge["amount"] == -3.5 and edge["formula"] == "-dose" and edge["scale"] == 0.2
    )
    assert raw["exchanges"][1]["name"] == "public"
    records[0]["exchanges"][1]["amount"] = 99
    repeated, _ = user.prepare([public], [public])
    assert repeated[0]["exchanges"][1]["amount"] == -3.5
    assert metadata[0]["activity_count"] == 1


def test_retained_source_still_requires_approved_migration_route(
    retained, catalogs, tmp_path
):
    plan, _, public = retained
    database = [public]
    before = deepcopy(database)
    with pytest.raises(ClicInventoryError, match="heuristic compatibility resource"):
        hybrid.import_retained_inventories(
            plan,
            database,
            [public],
            version="2026",
            family="uvek",
            system_model="cutoff",
            catalog_provider=catalogs,
            cache_dir=tmp_path,
        )
    assert database == before


def test_user_purchase_of_retained_workbook_is_a_foreground_boundary(
    monkeypatch, tmp_path, catalogs, identity_migration
):
    from premise.inventory import inventory_imports

    path = tmp_path / "user.xlsx"
    path.write_bytes(b"user fixture")
    raw = activity("buyer")
    raw["exchanges"].append(purchase("retained"))
    monkeypatch.setattr(
        inventory_imports, "ExcelImporter", lambda *a: SimpleNamespace(data=[raw])
    )
    supplied = activity("retained", database="premise-CLIC", code="retained-code")
    supplied["inventory provenance"] = {
        "provider": "retained workbook",
        "record_id": "reviewed-record",
    }
    user = clic_user.ClicUserInventories(
        [{"filepath": path, "ecoinvent version": "3.12"}],
        [],
        version="2026",
        family="uvek",
        system_model="cutoff",
        catalog_provider=catalogs,
        cache_dir=tmp_path,
    )
    records, _ = user.prepare([supplied], [])
    assert records[0]["exchanges"][1]["input"] == ("premise-CLIC", "retained-code")
    assert records[0]["exchanges"][1]["amount"] == -3.5


def test_uvek_adapter_rejects_missing_destination_supplier(
    retained, catalogs, tmp_path, identity_migration
):
    plan, raw, public = retained
    raw[0]["exchanges"].append(purchase("missing supplier"))
    database = [public]
    with pytest.raises(ClicInventoryError, match="Expected one source supplier"):
        hybrid.import_retained_inventories(
            plan,
            database,
            [public],
            version="2026",
            family="uvek",
            system_model="cutoff",
            catalog_provider=catalogs,
            cache_dir=tmp_path,
        )
    assert database == [public]


@pytest.mark.parametrize("system_model", ["consequential", "EN15804"])
def test_uvek_workbook_cross_model_refusal_precedes_file_access(system_model):
    with pytest.raises(ValueError, match="requires system_model='cutoff'"):
        clic_user.ClicUserInventories(
            [{"filepath": "/does/not/exist", "ecoinvent version": "3.12"}],
            [],
            version="2026",
            family="uvek",
            system_model=system_model,
        )


def test_uvek_source_role_baseline_must_be_reviewed(retained):
    plan, _, _ = retained
    with pytest.raises(
        ClicInventoryError, match="reviewed ecoinvent 3.12 source-role baseline"
    ):
        hybrid.plan_hybrid_inventories(
            [],
            [],
            plan.sources,
            version="2026",
            family="uvek",
            source_version="3.11",
            system_model="cutoff",
        )


@pytest.fixture
def shared_helper_migration(monkeypatch):
    """Both independent sources invoke the same deterministic synthetic recipe."""
    from brightpath import BrightwayInventory
    from brightpath.core.policies import PolicyAction

    def migrate(inventory, target, *, policy, **kwargs):
        assert policy.on_heuristic_mapping == PolicyAction.ERROR
        records = deepcopy(inventory.data)
        needed = False
        for record in records:
            for edge in record["exchanges"]:
                if edge.get("name") == "recipe trigger":
                    needed = True
                    edge.update(name="shared helper", product="shared helper")
                    edge["reference product"] = "shared helper"
                    edge["input"] = ("__clic__", "shared-helper")
        if needed:
            helper = activity(
                "shared helper", database="__clic__", code="shared-helper"
            )
            records.append(helper)
        return SimpleNamespace(data=records, last_migration_report=None)

    monkeypatch.setattr(BrightwayInventory, "migrate_background", migrate)


def _prepared_public_helper(catalogs, tmp_path):
    from premise.inventory.clic import ClicInventoryProvider
    from test_clic_archive import catalog, native

    client = ClicInventoryProvider(
        archive=catalog(
            native(1, 1, "public root", exchanges=[purchase("recipe trigger")])
        ),
        catalog_provider=catalogs,
        options={"cache_dir": tmp_path / "clic", "offline": True},
    )
    return client.prepare(["dataset-1"], [], version="2026", family="uvek")


def test_workbook_reuses_verified_equal_helper_from_separate_clic_call(
    retained, catalogs, tmp_path, shared_helper_migration
):
    plan, raw, public = retained
    raw[0]["exchanges"].append(purchase("recipe trigger"))
    prepared = _prepared_public_helper(catalogs, tmp_path)
    helper = next(a for a in prepared if a["code"] == "shared-helper")
    before = deepcopy(helper)
    destination = [public, *prepared]
    records, metadata, diagnostics = hybrid.import_retained_inventories(
        plan,
        destination,
        [public],
        version="2026",
        family="uvek",
        system_model="cutoff",
        catalog_provider=catalogs,
        cache_dir=tmp_path / "retained",
    )
    assert len(records) == 2
    assert helper == before
    assert sum(a["code"] == "shared-helper" for a in destination) == 1
    root = next(a for a in records if a["name"] == "root")
    assert root["exchanges"][-1]["input"] == ("premise-CLIC", "shared-helper")
    assert root["exchanges"][-1]["name"] == helper["name"]
    assert any(row["code"] == "clic.workbook_helper_reused" for row in diagnostics)
    migration = next(
        row["manifest"]
        for row in diagnostics
        if row["code"] == "clic.workbook_harmonized"
    )
    assert migration["target"]["technosphere"]["family"] == "uvek"
    assert metadata[0]["migration_cohorts"]
    for reference in metadata[0]["migration_cohorts"]:
        cohort = next(
            c
            for c in migration["cohorts"]
            if c["fingerprint"] == reference["fingerprint"]
        )
        assert (
            reference["audit_sha256"]
            == hashlib.sha256(Path(cohort["audit_file"]).read_bytes()).hexdigest()
        )
        assert "audit_file" not in reference
    import json

    json.dumps(metadata, allow_nan=False)
    json.dumps(diagnostics, allow_nan=False)


def test_conflicting_helper_from_separate_clic_call_fails_atomically(
    retained, catalogs, tmp_path, shared_helper_migration
):
    plan, raw, public = retained
    raw[0]["exchanges"].append(purchase("recipe trigger"))
    prepared = _prepared_public_helper(catalogs, tmp_path)
    helper = next(a for a in prepared if a["code"] == "shared-helper")
    helper["exchanges"][0]["amount"] = 2
    destination = [public, *prepared]
    before = deepcopy(destination)
    with pytest.raises(
        ClicInventoryError, match="Conflicting generated workbook helper"
    ):
        hybrid.import_retained_inventories(
            plan,
            destination,
            [public],
            version="2026",
            family="uvek",
            system_model="cutoff",
            catalog_provider=catalogs,
            cache_dir=tmp_path / "retained",
        )
    assert destination == before


def test_user_inventory_reuses_public_helper_and_exposes_portable_audits(
    monkeypatch, catalogs, tmp_path, shared_helper_migration
):
    from premise.inventory import inventory_imports

    path = tmp_path / "user.xlsx"
    path.write_bytes(b"synthetic user workbook")
    raw = activity("user")
    raw["exchanges"].append(purchase("recipe trigger"))
    monkeypatch.setattr(
        inventory_imports, "ExcelImporter", lambda *a: SimpleNamespace(data=[raw])
    )
    public = _prepared_public_helper(catalogs, tmp_path)
    before = deepcopy(public)
    destination = list(public)
    user = clic_user.ClicUserInventories(
        [{"filepath": path, "ecoinvent version": "3.12"}],
        [],
        version="2026",
        family="uvek",
        system_model="cutoff",
        catalog_provider=catalogs,
        cache_dir=tmp_path / "user",
    )
    records, metadata = user.prepare(destination, public)
    assert len(records) == 1
    assert public == before
    assert sum(a["code"] == "shared-helper" for a in destination) == 1
    assert records[0]["exchanges"][1]["input"] == ("premise-CLIC", "shared-helper")
    assert metadata[0]["migration_manifest"]["generated_helpers"] == ["shared-helper"]
    assert all("audit_file" not in row for row in metadata[0]["migration_cohorts"])
    assert any(row["code"] == "clic.workbook_harmonized" for row in user.diagnostics)
