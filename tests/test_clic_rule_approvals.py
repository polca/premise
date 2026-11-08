"""The public harmonizer admits reviewed cases without a heuristic bypass."""

import copy
import gzip
import json
from pathlib import Path

import pytest

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
from premise.inventory.clic_target import resolve_inventory_target
from test_clic_archive import catalog, native


def material_case(label="fan"):
    from brightpath import DATA_DIR

    rows = json.loads(
        (DATA_DIR / "export/uvek_pilot_material_review.json").read_text()
    )["rules"]
    return next(row for row in rows if row["label"] == label)


def graph(*sources):
    record = native(
        1,
        1,
        "synthetic foreground",
        exchanges=[
            {**source, "type": "technosphere", "amount": -2, "uncertainty type": 0}
            for source in sources
        ],
    )
    record.update(background_version="3.9", biosphere_version="3.9")
    return catalog(record).resolve(["dataset-1"])


def test_only_exact_packaged_profile_decisions_enter_the_approval_set():
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    target = resolve_inventory_target("2026", family="uvek")
    scopes = approved_compatibility_rule_scopes(target)
    resource = load_uvek_technosphere_resource()
    decision = material_case()
    rule = next(r for r in resource["replace"] if r["id"] == decision["rule_id"])
    bridge = TechnosphereProfile("uvek", "2025", "cutoff")
    assert (
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            bridge,
        )
        in scopes
    )
    assert (
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
            bridge,
        )
        not in scopes
    )
    assert approved_compatibility_rule_scopes(resolve_inventory_target("3.12")) == ()


@pytest.mark.parametrize("change", ["target", "factor", "reference"])
def test_ledger_drift_requires_review(monkeypatch, change):
    import brightpath.migrations.resources as resources

    original = resources.load_uvek_technosphere_resource()
    decision = material_case()
    resource = {
        **original,
        "replace": [
            copy.deepcopy(r) if r["id"] == decision["rule_id"] else r
            for r in original["replace"]
        ],
    }
    rule = next(r for r in resource["replace"] if r["id"] == decision["rule_id"])
    if change == "target":
        rule["target"]["name"] += " changed"
    elif change == "factor":
        rule["conversion_factor"] = 3
    else:
        rule["reference_flow_sign_review"]["profiles"][0]["review"][
            "status"
        ] = "unresolved"
    monkeypatch.setattr(resources, "load_uvek_technosphere_resource", lambda: resource)
    with pytest.raises(ClicInventoryError, match="approval"):
        approved_compatibility_rule_scopes(
            resolve_inventory_target("2026", family="uvek")
        )


def test_public_harmonizer_and_cache_accept_only_the_reviewed_inventory(tmp_path):
    from brightpath.background import PackageCatalogProvider

    decision = material_case()
    resolved = graph(decision["source"])
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert result.manifest["policy"]["approved_compatibility_rules"]
        assert result.manifest["policy"]["overlapping_rule_policy"] == "error"
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == -2
        assert all(edge[key] == value for key, value in decision["target"].items())
        assert any(
            i["code"] == "migration.approved_correspondence" for i in result.diagnostics
        )
    assert resolved.activities == before


def test_unreviewed_supplier_fails_with_audit_and_no_cached_target(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    unreviewed = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-49e229b952f6983c"
    )
    resolved = graph(material_case()["source"], unreviewed["source"])
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
    assert not list(tmp_path.glob("*.inventory.json.gz"))
    audit = json.loads(
        gzip.decompress(next(tmp_path.glob("*.audit.json.gz")).read_bytes())
    )
    issues = [issue for stage in audit["stages"] for issue in stage.get("issues", ())]
    assert any(
        i["code"] == "migration.unapproved_correspondence"
        and i["details"]["rule_id"] == unreviewed["id"]
        for i in issues
    )


def test_existing_packed_wool_approval_preserves_complete_generated_inventory(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-555e0c3ac464c43f"
    )
    resolved = graph(rule["source"])
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    root = next(r for r in result.activities if r["name"] == "synthetic foreground")
    edge = root["exchanges"][1]
    assert edge["amount"] == -2 and edge["unit"] == "kilogram"
    supplier = next(r for r in result.activities if r["code"] == edge["input"][1])
    assert supplier["code"] in result.manifest["generated_helpers"]
    assert len([e for e in supplier["exchanges"] if e["type"] == "technosphere"]) > 2
    assert any(
        d["code"] == "migration.approved_correspondence" for d in result.diagnostics
    )
    assert not any(
        d["code"] == "migration.heuristic_mapping" for d in result.diagnostics
    )


def test_existing_helper_recipe_drift_does_not_inherit_approval(monkeypatch):
    import brightpath.migrations.resources as resources

    original = resources.load_uvek_technosphere_resource()
    rule = next(r for r in original["replace"] if r["id"] == "bp-555e0c3ac464c43f")
    profile = next(
        p
        for p in rule["reference_flow_sign_review"]["profiles"]
        if p["source_profile"]["version"] == "3.6"
    )
    recipe_id = profile["target"]["recipe_ref"]
    recipe = copy.deepcopy(original["recipes"][recipe_id])
    recipe["inputs"][0]["amount"] *= 2
    changed = {**original, "recipes": {**original["recipes"], recipe_id: recipe}}
    monkeypatch.setattr(resources, "load_uvek_technosphere_resource", lambda: changed)
    with pytest.raises(ClicInventoryError, match="helper recipe"):
        approved_compatibility_rule_scopes(
            resolve_inventory_target("2026", family="uvek")
        )
