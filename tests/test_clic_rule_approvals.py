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
        "bp-4764655d2d8cd10b",
        "bp-44fac4cd9c9b9108",
        "bp-f21a6d77f5c0d093",
        "bp-90806fced8456f16",
        "bp-19cb447154bde8c5",
        "bp-1be0fde93275f330",
        "bp-45e5a117313cb104",
        "bp-369049fd465b0d65",
        "bp-b8b81a3c16e46627",
        "bp-4a4052a90a846364",
        "bp-b617041a5d9cf1f3",
        "bp-30e27d637bf8fed7",
        "bp-faa8d193360beec8",
        "bp-8532ff8d4ef700a9",
        "bp-71990e8bf71d4f67",
        "bp-71560f8ef7f77d14",
        "bp-f7129a04e584ba78",
        "bp-263e52a8ecb636b1",
        "bp-62bb723c9eabee6f",
        "bp-798954a60c5e190c",
        "bp-b65045228b50f40f",
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
    assert any(
        d["code"] == "clic.compatibility_proxy_limitations"
        and d["details"]["rule_id"] == rule_id
        for d in result.diagnostics
    )


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-ab192df1c5f33d7e", "3.6"),
        ("bp-8a6394dd395bb22d", "3.12"),
        ("bp-64033d1871a0085f", "3.12"),
        ("bp-8b6f1fcdfadf34af", "3.12"),
    ],
)
@pytest.mark.parametrize("amount", [-2, 2])
def test_direct_processing_reference_keeps_the_foreground_quantity(
    tmp_path, rule_id, version, amount
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    source = resolved.activities[0]["exchanges"][1]
    source.update(
        amount=amount,
        **{
            "uncertainty type": 2,
            "loc": math.log(abs(amount)),
            "scale": 0.2,
            "negative": amount < 0,
            "formula": "processing_demand",
        },
    )
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    assert len(result.activities) == 1
    target = result.activities[0]["exchanges"][1]
    assert all(target[k] == v for k, v in rule["target"].items())
    for key in ("amount", "unit", "loc", "scale", "negative", "formula"):
        assert target[key] == source[key]
    assert resolved.activities == before


def test_review_limitations_survive_cache_and_invalidate_stale_warning(
    tmp_path, monkeypatch
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource
    from premise.inventory import clic_rule_approvals as approvals

    rule_id = "bp-0227e3e438bfcee6"
    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version="3.6")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        warnings = [
            i
            for i in result.diagnostics
            if i["code"] == "clic.compatibility_proxy_limitations"
        ]
        assert len(warnings) == 1
        warning = warnings[0]
        assert warning["severity"] == "warning"
        assert warning["details"]["rule_id"] == rule_id
        assert "wet-kg" in warning["message"]
        audit = json.loads(
            gzip.decompress(
                Path(result.manifest["cohorts"][0]["audit_file"]).read_bytes()
            )
        )
        assert audit["premise_compatibility_reviews"] == [warning["details"]]
        assert not any(
            r["rule_id"] != rule_id for r in audit["premise_compatibility_reviews"]
        )
    fingerprint = result.manifest["cohorts"][0]["fingerprint"]
    ledger = approvals._approval_ledger()
    next(d for d in ledger["operations"] if d["rule_id"] == rule_id)[
        "limitations"
    ].append("Additional reviewed delivery-boundary limitation.")
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: copy.deepcopy(ledger))
    result = harmonizer.harmonize(resolved, family="uvek", version="2026")
    assert result.manifest["cohorts"][0]["cache_hit"] is False
    assert result.manifest["cohorts"][0]["fingerprint"] != fingerprint
    assert any(
        "delivery-boundary limitation" in i["message"] for i in result.diagnostics
    )


def test_refused_transaction_retains_review_limitations_without_target(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    indexed = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    reviewed = indexed["bp-0227e3e438bfcee6"]
    unreviewed = indexed["bp-4438f1d4119c16e4"]
    resolved = graph(reviewed["source"], unreviewed["source"], version="3.6")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before
    assert not list(tmp_path.glob("*.inventory.json.gz"))
    audit = json.loads(
        gzip.decompress(next(tmp_path.glob("*.audit.json.gz")).read_bytes())
    )
    assert [r["rule_id"] for r in audit["premise_compatibility_reviews"]] == [
        reviewed["id"]
    ]
    issues = [i for s in audit["stages"] for i in s["issues"]]
    assert any(i["code"] == "clic.compatibility_proxy_limitations" for i in issues)
    assert any(i["code"] == "migration.unapproved_correspondence" for i in issues)


@pytest.mark.parametrize(
    "rule_id",
    [
        "bp-3aedb5762ff23e68",  # No matching source composition documented.
        "bp-8cde42cb022d74ed",  # Different waste composition and landfill class.
        "bp-1b20be09e53aaa84",  # Source omits composition and landfill emissions.
        "bp-bcbf9e1008a58e4b",  # Source omits composition and landfill emissions.
    ],
)
def test_unverified_landfill_coordinates_do_not_inherit_waste_approval(rule_id):
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == rule_id)
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.6", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )


@pytest.mark.parametrize(
    "rule_id,version,factor",
    [
        ("bp-9f7197c47047750a", "3.8", 1),
        ("bp-a4fb957edae493e5", "3.9", -1),
        ("bp-0f79ee18ac90436d", "3.9", -1),
        ("bp-f538bbeedc48b671", "3.8", 1),
        ("bp-4b1d84179efa58e8", "3.9", -1),
        ("bp-815270d3fcbfb53b", "3.9", -1),
    ],
)
@pytest.mark.parametrize("amount", [-2, 2])
def test_prior_helper_approval_returns_complete_owned_graph(
    tmp_path, rule_id, version, factor, amount
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    source = resolved.activities[0]["exchanges"][1]
    source.update(
        amount=amount,
        **{
            "uncertainty type": 2,
            "loc": math.log(abs(amount)),
            "scale": 0.2,
            "negative": amount < 0,
            "formula": "service_demand",
        },
    )
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, family="uvek", version="2026")
    owned = {a["code"]: a for a in result.activities}
    foreground = owned[resolved.activities[0]["code"]]
    demand = foreground["exchanges"][1]
    assert demand["amount"] == amount * factor
    assert demand["unit"] == source["unit"]
    assert demand["loc"] == source["loc"] and demand["scale"] == source["scale"]
    assert demand["negative"] is (amount * factor < 0)
    if factor == 1:
        assert demand["formula"] == "service_demand"
    else:
        assert demand["formula"] == "(service_demand) * (-1.0)"
    assert len(owned) > 1
    assert demand["input"][0] == "__clic__"
    assert demand["input"][1] in owned
    for activity in result.activities:
        for exchange in activity["exchanges"]:
            reference = exchange.get("input", ())
            if reference and reference[0] == "__clic__":
                assert reference[1] in owned
    assert resolved.activities == before
    assert any(
        i["code"] == "clic.compatibility_proxy_limitations"
        and i["details"]["rule_id"] == rule_id
        for i in result.diagnostics
    )
