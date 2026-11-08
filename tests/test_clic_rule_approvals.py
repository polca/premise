"""The public harmonizer admits reviewed cases without a heuristic bypass."""

import copy
import gzip
import json
import math
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


def graph(*sources, version="3.9"):
    record = native(
        1,
        1,
        "synthetic foreground",
        exchanges=[
            {**source, "type": "technosphere", "amount": -2, "uncertainty type": 0}
            for source in sources
        ],
    )
    record.update(background_version=version, biosphere_version=version)
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


@pytest.mark.parametrize(
    "change", ["profile_override", "shared_review", "missing_rule"]
)
def test_material_approval_binds_the_complete_rule(monkeypatch, change):
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
    if change == "profile_override":
        # Keep the default target and all reference-status checks unchanged.
        rule["reference_flow_sign_review"]["profiles"][0]["target"] = {
            **rule["target"],
            "name": "unreviewed override",
        }
    elif change == "shared_review":
        resource["reviews"] = {
            **original["reviews"],
            rule["review_ref"]: {"rationale": "a newly proposed boundary"},
        }
    else:
        resource["replace"] = [
            r for r in resource["replace"] if r["id"] != decision["rule_id"]
        ]
    monkeypatch.setattr(resources, "load_uvek_technosphere_resource", lambda: resource)
    with pytest.raises(ClicInventoryError, match="approval"):
        approved_compatibility_rule_scopes(
            resolve_inventory_target("2026", family="uvek")
        )


def test_changed_packaged_decision_ledger_requires_review(monkeypatch):
    original = Path.read_bytes

    def changed(path):
        raw = original(path)
        if path.name == "uvek_pilot_material_review.json":
            return raw + b"\n"
        return raw

    monkeypatch.setattr(Path, "read_bytes", changed)
    with pytest.raises(ClicInventoryError, match="changed packaged review"):
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


@pytest.mark.parametrize("version", ["3.6", "3.9"])
def test_existing_road_approval_uses_complete_heavy_fleet_with_warnings(
    tmp_path, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-d6b9d771677aa51e"
    )
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(graph(rule["source"], version=version), version="2026", family="uvek")
    exchange = result.activities[0]["exchanges"][1]
    assert exchange["name"] == "Transport, freight, lorry, fleet average"
    assert exchange["location"] == "RER"
    assert exchange["unit"] == "ton kilometer" and exchange["amount"] == -2
    assert any(
        d["code"] == "migration.technosphere_information_loss"
        for d in result.diagnostics
    )


def test_older_road_approval_does_not_extend_to_an_uninspected_release():
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == "bp-d6b9d771677aa51e")
    assert compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    ) not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )


@pytest.mark.parametrize(
    "rule_id",
    [
        "bp-74209bb3aff5ad90",
        "bp-a5ed3251310b7e55",
        "bp-89a9e0030212f423",
        "bp-0227e3e438bfcee6",
    ],
)
@pytest.mark.parametrize("amount", [-2, 2])
def test_inspected_wet_waste_reference_preserves_treatment_and_credit_signs(
    tmp_path, rule_id, amount
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version="3.6")
    source = resolved.activities[0]["exchanges"][1]
    source.update(
        amount=amount,
        **{
            "uncertainty type": 2,
            "loc": math.log(abs(amount)),
            "scale": 0.2,
            "negative": amount < 0,
            "formula": "signed_waste",
        },
    )
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    target = result.activities[0]["exchanges"][1]
    assert target["amount"] == -amount
    assert target["unit"] == "kilogram"
    assert all(target[k] == v for k, v in rule["target"].items())
    assert target["negative"] is (amount > 0)
    assert target["loc"] == source["loc"] and target["scale"] == source["scale"]
    assert target["formula"] == "(signed_waste) * (-1.0)"
    assert resolved.activities == before
    assert any(
        d["code"] == "migration.approved_correspondence"
        and d["details"]["rule_id"] == rule_id
        for d in result.diagnostics
    )
