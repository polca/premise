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


def test_supplier_without_scope_fails_with_audit_and_no_cached_target(
    tmp_path, monkeypatch
):
    from brightpath.background import PackageCatalogProvider
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource
    import premise.inventory.clic_harmonize as harmonize

    resource = load_uvek_technosphere_resource()
    unreviewed = next(
        r for r in resource["replace"] if r["id"] == "bp-49e229b952f6983c"
    )
    # This supplier now has a titanium review. Withhold its exact scope so this
    # refusal contract remains independent of the growing real approval ledger.
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        unreviewed,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
    monkeypatch.setattr(
        harmonize,
        "approved_compatibility_rule_scopes",
        lambda target: tuple(
            scope
            for scope in approved_compatibility_rule_scopes(target)
            if scope != fingerprint
        ),
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


def test_heat_hydrogen_proxy_preserves_exact_scope_limits_and_cache(tmp_path):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = json.loads(
        (DATA_DIR / "export/uvek_heat_hydrogen_market_review.json").read_text()
    )
    resolved = graph(review["source"])
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        exchange = result.activities[0]["exchanges"][1]
        assert exchange["amount"] == -2
        assert all(exchange[k] == v for k, v in review["target"].items())
        assert any(
            item["code"] == "clic.compatibility_proxy_limitations"
            and "Refinery integration" in item["message"]
            for item in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("index", range(8))
def test_heat_dependency_batch_keeps_coordinate_and_warnings_in_cache(tmp_path, index):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = json.loads(
        (DATA_DIR / "export/uvek_heat_dependency_batch_review.json").read_text()
    )
    row = review["records"][index]
    resolved = graph(row["source"])
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == -2 * row["conversion_factor"]
        assert all(edge[k] == v for k, v in row["target"].items())
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and row["rule_id"] in str(d["details"])
            for d in result.diagnostics
        )
    assert resolved.activities == before


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
    "rule_id,version",
    [
        (rule_id, "3.6")
        for rule_id in [
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
            "bp-6ac94483d465e40b",
            "bp-ee23a489d49b64ab",
            "bp-34d216163d085df9",
            "bp-1824e49db175bc6e",
            "bp-e51642145510fc9c",
            "bp-76fc3358de421a01",
            "bp-fa607ec8232396e4",
            "bp-7393e642acca8e35",
            "bp-b73d5db62046c9cf",
            "bp-8c3b3ceb3e9c8bef",
            "bp-87f7a46df5981dc9",
            "bp-6c847c1ca1076c30",
            "bp-d60ada0a6741edb5",
            "bp-3aedb5762ff23e68",
            "bp-b39070e2c2728597",
            "bp-fb5a0385ef4ba237",
            "bp-8cde42cb022d74ed",
        ]
    ]
    + [
        ("bp-9ac96dd0500cd5a9", "3.9"),
        ("bp-4d389c0e2269e7e9", "3.9"),
        ("bp-e4747a1d81615ef4", "3.9"),
        ("bp-cc3516074957a931", "3.9"),
        ("bp-ccaa5bfc3f126b6f", "3.9"),
        ("bp-00d0820dc5481d5d", "3.9"),
        ("bp-cccd7819bb6cea4b", "3.9"),
        ("bp-4a8d844635b7637e", "3.9"),
        ("bp-9f820f97f9f3c080", "3.9"),
        ("bp-281c3f362d35e83f", "3.9"),
    ],
)
@pytest.mark.parametrize("amount", [-2, 2])
def test_inspected_waste_reference_preserves_treatment_and_credit_signs(
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
            "formula": "signed_waste",
        },
    )
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    target = result.activities[0]["exchanges"][1]
    assert target["amount"] == -amount
    assert target["unit"] == source["unit"] == rule["target"]["unit"]
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
        ("bp-5c3aff1455ed47f3", "3.6"),
        ("bp-e151524e59c9a873", "3.6"),
        ("bp-189511c4a470ca1b", "3.12"),
        ("bp-7107efa3d84d4a67", "3.6"),
        ("bp-1804a6da97afa32c", "3.6"),
        ("bp-8048c2b73279c472", "3.9"),
        ("bp-a06513ceb8310f53", "3.6"),
        ("bp-d39c48f8295cfba2", "3.6"),
        ("bp-fa7ab6947d6de491", "3.6"),
        ("bp-9f2331704b45610e", "3.6"),
        ("bp-83df3d1c3262d314", "3.6"),
        ("bp-66069e6b556917d1", "3.6"),
        ("bp-4460a6ac3f5e4c5c", "3.6"),
        ("bp-3b1b2eaa1788c5c2", "3.6"),
        ("bp-15c42019546101f4", "3.12"),
        ("bp-7b718a88076307fe", "3.6"),
        ("bp-e91d186584958663", "3.6"),
        ("bp-cf3ed0dd40541ac8", "3.6"),
        ("bp-8ec82825a525039d", "3.6"),
        ("bp-67f651b7065701f4", "3.9"),
        ("bp-6af8519b6eae52db", "3.12"),
        ("bp-3f19534eb7d97f64", "3.6"),
        ("bp-657f6ad4eabd8846", "3.9"),
        ("bp-f5891204a3feaf32", "3.7"),
        ("bp-f5891204a3feaf32", "3.9"),
        ("bp-202b754502368791", "3.6"),
        ("bp-61babdae6914f05a", "3.12"),
        ("bp-61babdae6914f05a", "3.9"),
        ("bp-49b843b95c0c6223", "3.9"),
        ("bp-deb7a57e4b2bc6e7", "3.6"),
        ("bp-7128907317fe7019", "3.6"),
        ("bp-ff3e7e592e354217", "3.6"),
        ("bp-eb04730488ad462a", "3.9"),
        ("bp-eb04730488ad462a", "3.12"),
        ("bp-263c185cdbca7a8c", "3.9"),
        ("bp-e1c5c137f358792d", "3.12"),
        ("bp-1d02d57e06e24b45", "3.12"),
        ("bp-46961b2d2543df96", "3.6"),
        ("bp-02c0322dff7270a9", "3.9"),
        ("bp-bf1073514e708c92", "3.9"),
        ("bp-374adbf07a0ef554", "3.12"),
        ("bp-6c86005a2117819c", "3.6"),
        ("bp-d8aa748d3c6d8667", "3.9"),
        ("bp-365d3df4ba55618f", "3.9"),
        ("bp-b80976e787fd9cd2", "3.9"),
        ("bp-14d8fca280d9c0bb", "3.12"),
        ("bp-99faa5629490252a", "3.12"),
        ("bp-d9521ecfbb6552d6", "3.12"),
        ("bp-c63a1747fd444679", "3.12"),
        ("bp-3080b1f0605165b9", "3.6"),
        ("bp-3080b1f0605165b9", "3.9"),
        ("bp-0028acc612584b3f", "3.9"),
        ("bp-0028acc612584b3f", "3.12"),
        ("bp-3957e7cc9358ee62", "3.6"),
        ("bp-3957e7cc9358ee62", "3.12"),
        ("bp-412f090ea47af121", "3.12"),
        ("bp-2db59faccae4349c", "3.12"),
        ("bp-1f0fa3bbee140c45", "3.12"),
        ("bp-cdf8909801f26981", "3.6"),
        ("bp-cdf8909801f26981", "3.9"),
        ("bp-3c4fc855bcc17210", "3.12"),
        ("bp-edf1518533a281df", "3.6"),
        ("bp-edf1518533a281df", "3.9"),
        ("bp-2bab3a3e3d0b3ad7", "3.9"),
        ("bp-2bab3a3e3d0b3ad7", "3.12"),
        ("bp-8d83e6b7c6fd6e59", "3.9"),
        ("bp-8afbd7be259066bf", "3.9"),
        ("bp-be4b125b435b18ca", "3.9"),
        ("bp-834f96388a124aa3", "3.9"),
        ("bp-aa2fb09531ebfe9d", "3.9"),
        ("bp-7431264e8037a627", "3.6"),
        ("bp-be3d5782d2d74037", "3.6"),
        ("bp-9b6b68753c5e8cb5", "3.6"),
        ("bp-2c87af773f68847e", "3.6"),
        ("bp-b2ae35d1623aa77f", "3.6"),
        ("bp-6983448ac196d4e1", "3.6"),
        ("bp-295a2857acc1f338", "3.6"),
        ("bp-c4181fc2830439b1", "3.6"),
        ("bp-a55991d52eed7e95", "3.6"),
        ("bp-735f825c980b81f7", "3.6"),
        ("bp-c5ad7d30b2b18c4c", "3.6"),
        ("bp-fac4a1f43671b603", "3.6"),
        ("bp-f15e4a6a029e8ddb", "3.6"),
        ("bp-995a85c7009a9601", "3.6"),
        ("bp-91b9cff76b3562bb", "3.6"),
        ("bp-6f58476b963abe8c", "3.9"),
        ("bp-1d075913a8c7a9b6", "3.6"),
        ("bp-1d075913a8c7a9b6", "3.8"),
        ("bp-1d075913a8c7a9b6", "3.9"),
        ("bp-7ef361e0dafa8736", "3.8"),
        ("bp-6f1466d334d7b18c", "3.6"),
        ("bp-c8d707ac24ebb7e4", "3.9"),
        ("bp-c8d707ac24ebb7e4", "3.12"),
        ("bp-688374518770ccba", "3.9"),
        ("bp-a50439c1a11df652", "3.6"),
        ("bp-6a97c5362cbe7587", "3.9"),
        ("bp-7740a48a01851669", "3.6"),
        ("bp-7740a48a01851669", "3.9"),
        ("bp-129394609f0fae8e", "3.6"),
        ("bp-7bb87cea8e133871", "3.12"),
        ("bp-b7b01a65bd699382", "3.12"),
        ("bp-49570b4c4014afaf", "3.9"),
        ("bp-476511d499d06401", "3.6"),
        ("bp-7ca3587cbfe4ce9b", "3.6"),
        ("bp-7ca3587cbfe4ce9b", "3.9"),
        ("bp-714b90acb435b476", "3.9"),
        ("bp-8bbb01130472bd02", "3.9"),
        ("bp-d57552347b187336", "3.12"),
        ("bp-fef3bd133ce6cadc", "3.9"),
        ("bp-e1f8de5b5a35b0d1", "3.12"),
        ("bp-e1e1fd1c04f327da", "3.9"),
        ("bp-7cf08aa845443b7b", "3.6"),
        ("bp-01903a2b3725bd56", "3.9"),
        ("bp-382c78d4731734d5", "3.9"),
        ("bp-036ac993c46b613c", "3.6"),
        ("bp-1d21220403ae07ee", "3.9"),
        ("bp-65d60275b4f9e27a", "3.6"),
        ("bp-c712c8a159d0bec5", "3.9"),
        ("bp-5497e694c10240a5", "3.6"),
        ("bp-01b951ae68a063a7", "3.12"),
        ("bp-29a5a345f9b595ed", "3.9"),
        ("bp-29a5a345f9b595ed", "3.12"),
        ("bp-82cb48b8f46820d5", "3.9"),
        ("bp-82cb48b8f46820d5", "3.12"),
        ("bp-18e8ed0207bbd5f8", "3.7"),
        ("bp-18e8ed0207bbd5f8", "3.9"),
        ("bp-182cd513cb8969f1", "3.9"),
        ("bp-b5f627281d517e19", "3.12"),
        ("bp-c58e827fcace3923", "3.6"),
        ("bp-1979ee200d36825e", "3.9"),
        ("bp-6ce52de989145df6", "3.12"),
        ("bp-8a37fb9191501441", "3.9"),
        ("bp-a3a951644bd5ed9c", "3.6"),
        ("bp-2d474e1cc4ae1395", "3.8"),
        ("bp-7b9f2134fcb65b6d", "3.6"),
        ("bp-08f33c5236eee787", "3.12"),
        ("bp-155787c4bf1ee018", "3.6"),
        ("bp-793f007705cc43d3", "3.6"),
        ("bp-194bdbdd9a8f228f", "3.6"),
        ("bp-7dd6b6497336da23", "3.12"),
        ("bp-eb6086f7739eb966", "3.12"),
        ("bp-c6f0c708990e4109", "3.6"),
        ("bp-90d9ae3ee389f465", "3.6"),
        ("bp-90d9ae3ee389f465", "3.9"),
        ("bp-4438f1d4119c16e4", "3.6"),
        ("bp-c291034cfcb83f1c", "3.6"),
        ("bp-8e58bbf6b2e31687", "3.6"),
        ("bp-4389bb6ee9ab35e9", "3.8"),
        ("bp-ac05197a0852076a", "3.6"),
        ("bp-1cb5578cf49cfd48", "3.6"),
        ("bp-cac4eeaf7215b33d", "3.6"),
        ("bp-ac5d1249cfc2626f", "3.6"),
        ("bp-84bd9cf473cdeee9", "3.6"),
        ("bp-84bd9cf473cdeee9", "3.9"),
        ("bp-4529f104ab14ba19", "3.12"),
        ("bp-07d1ab8592869587", "3.6"),
        ("bp-84737a1a1790ee76", "3.6"),
        ("bp-2d7bd6f9508a4c17", "3.6"),
        ("bp-b16c64b2ed0f8102", "3.12"),
        ("bp-f0abe48d8c3176d4", "3.9"),
    ],
)
@pytest.mark.parametrize("amount", [-2, 2])
def test_direct_reference_keeps_the_foreground_quantity(
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


def test_refused_transaction_retains_review_limitations_without_target(
    tmp_path, monkeypatch
):
    from brightpath import TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    reviewed = indexed["bp-0227e3e438bfcee6"]
    withheld = indexed["bp-4438f1d4119c16e4"]
    withheld_scope = compatibility_rule_fingerprint(
        resource,
        "replace",
        withheld,
        TechnosphereProfile("ecoinvent", "3.6", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )

    # The turbine now has a separate review. Withhold only this test's policy
    # admission so the rollback assertion stays independent of the live queue.
    def policy_without_turbine(target):
        return tuple(
            scope
            for scope in approved_compatibility_rule_scopes(target)
            if scope != withheld_scope
        )

    monkeypatch.setattr(
        "premise.inventory.clic_harmonize.approved_compatibility_rule_scopes",
        policy_without_turbine,
    )
    resolved = graph(reviewed["source"], withheld["source"], version="3.6")
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
        "bp-8cde42cb022d74ed",  # Old endpoint had a different waste/landfill class.
        "bp-1b20be09e53aaa84",  # Direct composition-specific endpoint remains unapproved.
        "bp-bcbf9e1008a58e4b",  # Only the generic operation/facility helper is approved.
    ],
)
def test_unverified_landfill_coordinates_do_not_inherit_waste_approval(rule_id):
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == rule_id)
    if rule["mapping_method"] == "reviewed_pilot_inert_landfill":
        from brightpath import DATA_DIR

        review = json.loads(
            (DATA_DIR / "export/uvek_pilot_inert_landfill_review.json").read_text()
        )
        rule = next(
            row["previous_rule"]
            for row in review["records"]
            if row["rule_id"] == rule_id
        )
        assert "recipe_ref" not in rule["target"]
    elif rule["mapping_method"] == "reviewed_pilot_sanitary_inert":
        from brightpath import DATA_DIR

        review = json.loads(
            (DATA_DIR / "export/uvek_pilot_sanitary_inert_review.json").read_text()
        )
        rule = next(
            row["previous_rule"] for row in review["rules"] if row["rule_id"] == rule_id
        )
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
        ("bp-1b20be09e53aaa84", "3.6", -1),
        ("bp-bcbf9e1008a58e4b", "3.6", -1),
        ("bp-9caeb1b3bd66b612", "3.6", 1),
        ("bp-9caeb1b3bd66b612", "3.9", 1),
        ("bp-9caeb1b3bd66b612", "3.12", 1),
        ("bp-1d429dc5bcb36b3e", "3.9", -1),
        ("bp-0930282b4ba7e381", "3.12", -1),
        ("bp-22c3a52062d0cb16", "3.9", -1),
        ("bp-e172491d6da0b115", "3.9", -1),
        ("bp-b7606642f4256898", "3.9", -1),
        ("bp-5c29c14c8825b76f", "3.12", -1),
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


@pytest.mark.parametrize(
    "rule_id",
    [
        "bp-9c3675cc3a3f003c",  # Different cement strength-class coverage.
        "bp-18d0fd79bfe6791b",  # TFE has conflicting PTFE CAS/synonym metadata.
        "bp-08f2432b62895931",  # Epoxy resin grade is not verified by this group.
    ],
)
def test_unreviewed_native_boundaries_do_not_inherit_direct_approval(rule_id):
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


@pytest.mark.parametrize("rule_id", ["bp-1b20be09e53aaa84", "bp-bcbf9e1008a58e4b"])
def test_bare_inert_landfill_retains_source_boundary_and_cached_gap_warning(
    tmp_path, rule_id
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version="3.6")
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    resolved.activities[0]["exchanges"].append(emission)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for result in (
        harmonizer.harmonize(resolved, family="uvek", version="2026"),
        harmonizer.harmonize(resolved, family="uvek", version="2026"),
    ):
        foreground = next(
            a for a in result.activities if a["code"] == resolved.activities[0]["code"]
        )
        edge = foreground["exchanges"][1]
        assert edge["amount"] == 2
        helper = next(a for a in result.activities if a["code"] == edge["input"][1])
        tech = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        assert {e["name"]: e["amount"] for e in tech} == {
            "Process-specific burdens, inert material landfill": 1,
            "xx Inert material landfill facility": 1.48e-9,
        }
        assert not any(e["type"] == "biosphere" for e in helper["exchanges"])
        assert foreground["exchanges"][-1]["amount"] == 7
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
            and "retain that source-data gap" in json.dumps(d)
            for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "version,electric,converter",
    [
        ("3.6", 0.4328572718177839, 0.5671427281822161),
        ("3.9", 0.2578923451030648, 0.7421076548969346),
        ("3.12", 0.25789234510306475, 0.7421076548969346),
    ],
)
def test_low_alloyed_cast_stock_keeps_source_routes_delivery_and_cached_warning(
    tmp_path, version, electric, converter
):
    from collections import defaultdict

    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-9caeb1b3bd66b612"
    )
    resolved = graph(rule["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = 2.5
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.375,
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        foreground = next(
            a for a in result.activities if a["code"] == resolved.activities[0]["code"]
        )
        demand = foreground["exchanges"][1]
        assert demand["amount"] == 2.5
        helper = next(a for a in result.activities if a["code"] == demand["input"][1])
        quantities = defaultdict(float)
        for exchange in helper["exchanges"]:
            if exchange["type"] == "technosphere":
                quantities[exchange["name"]] += exchange["amount"]
        assert dict(quantities) == pytest.approx(
            {
                "Steel, electric, low-alloyed, at plant": electric,
                "Steel, converter, low-alloyed, at plant": converter,
                "Transport, barge": 0.0201,
                "Transport, freight, lorry, fleet average": 0.2065,
                "Transport, freight, rail": 0.1903,
                "Transport, transoceanic freight ship": 0.4409,
            }
        )
        assert not any(
            "rolling" in e["name"].lower() or e["type"] == "biosphere"
            for e in helper["exchanges"]
        )
        assert foreground["exchanges"][-1]["amount"] == 0.375
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule["id"]
            and "without extra rolling" in d["message"]
            for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id",
    [
        "bp-eb04730488ad462a",
        "bp-263c185cdbca7a8c",
        "bp-e1f8de5b5a35b0d1",
        "bp-3aedb5762ff23e68",
        "bp-b39070e2c2728597",
        "bp-18e8ed0207bbd5f8",
        "bp-182cd513cb8969f1",
        "bp-b5f627281d517e19",
        "bp-ccaa5bfc3f126b6f",
        "bp-00d0820dc5481d5d",
        "bp-cccd7819bb6cea4b",
        "bp-4a8d844635b7637e",
        "bp-c58e827fcace3923",
        "bp-1979ee200d36825e",
        "bp-6ce52de989145df6",
        "bp-8a37fb9191501441",
        "bp-a3a951644bd5ed9c",
        "bp-2d474e1cc4ae1395",
        "bp-7b9f2134fcb65b6d",
        "bp-08f33c5236eee787",
        "bp-155787c4bf1ee018",
        "bp-793f007705cc43d3",
        "bp-194bdbdd9a8f228f",
        "bp-7dd6b6497336da23",
        "bp-eb6086f7739eb966",
        "bp-07968f74bde91ad7",
        "bp-c6f0c708990e4109",
        "bp-90d9ae3ee389f465",
        "bp-4438f1d4119c16e4",
        "bp-c291034cfcb83f1c",
        "bp-8e58bbf6b2e31687",
        "bp-4389bb6ee9ab35e9",
        "bp-ac05197a0852076a",
        "bp-1cb5578cf49cfd48",
        "bp-cac4eeaf7215b33d",
        "bp-ac5d1249cfc2626f",
        "bp-fb5a0385ef4ba237",
        "bp-84bd9cf473cdeee9",
        "bp-4529f104ab14ba19",
        "bp-07d1ab8592869587",
        "bp-84737a1a1790ee76",
        "bp-9f820f97f9f3c080",
        "bp-2d7bd6f9508a4c17",
        "bp-b16c64b2ed0f8102",
        "bp-f0abe48d8c3176d4",
        "bp-9caeb1b3bd66b612",
        "bp-1b20be09e53aaa84",
        "bp-bcbf9e1008a58e4b",
        "bp-8cde42cb022d74ed",
        "bp-281c3f362d35e83f",
        "bp-1d429dc5bcb36b3e",
        "bp-0930282b4ba7e381",
        "bp-22c3a52062d0cb16",
        "bp-e172491d6da0b115",
        "bp-b7606642f4256898",
        "bp-5c29c14c8825b76f",
    ],
)
@pytest.mark.parametrize("version", ["3.10", "3.9.1"])
def test_existing_material_review_does_not_broaden_pilot_source_profiles(
    rule_id, version
):
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == rule_id)
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", version, "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )


@pytest.mark.parametrize(
    "rule_id,flow_name,warning_terms",
    [
        ("bp-deb7a57e4b2bc6e7", "Nitrogen oxides", ("85%", "80%", "unquantified")),
        (
            "bp-7b9f2134fcb65b6d",
            "Sulfur dioxide",
            ("4.1 kg", "0.0024 m3", "unquantified"),
        ),
        (
            "bp-ff3e7e592e354217",
            "Carbon dioxide, fossil",
            ("upstream", "preserve separately listed caller fuel-use emissions"),
        ),
    ],
)
def test_reviewed_service_preserves_separate_emissions_and_cache_warnings(
    tmp_path, rule_id, flow_name, warning_terms
):
    from brightpath import TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == rule_id)
    resolved = graph(rule["source"], version="3.6")
    resolved.activities[0]["exchanges"][1]["amount"] = 2
    resolved.activities[0]["exchanges"].append(
        {
            "name": flow_name,
            "categories": ("air", "non-urban air or from high stacks"),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.037,
            "uncertainty type": 0,
            "formula": "source_stack_emissions",
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert result.activities[0]["exchanges"][1]["amount"] == 2
        emissions = [
            e for e in result.activities[0]["exchanges"] if e["type"] == "biosphere"
        ]
        assert len(emissions) == 1
        assert emissions[0]["name"] == flow_name
        assert emissions[0]["amount"] == 0.037
        assert emissions[0]["formula"] == "source_stack_emissions"
        warning = next(
            i
            for i in result.diagnostics
            if i["code"] == "clic.compatibility_proxy_limitations"
            and i["details"]["rule_id"] == rule["id"]
        )
        assert all(term in warning["message"] for term in warning_terms)
    assert resolved.activities == before
    unreviewed = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert unreviewed not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )


@pytest.mark.parametrize(
    "rule_id,version,expected_name,warning_term",
    [
        (
            "bp-fa607ec8232396e4",
            "3.6",
            "Disposal, polypropylene, 15.9% water, to municipal incineration",
            "No wrapper freight is inserted",
        ),
        (
            "bp-b39070e2c2728597",
            "3.6",
            "Disposal, mineral wool, 0% water, to construction waste landfill",
            "Do not add the wrapper collection freight",
        ),
        (
            "bp-3aedb5762ff23e68",
            "3.6",
            "Disposal, inert waste, 5% water, to construction waste landfill",
            "without a collection/dismantling wrapper",
        ),
        (
            "bp-ccaa5bfc3f126b6f",
            "3.9",
            "Disposal, copper, 0% water, to municipal incineration",
            "market/producer-boundary difference",
        ),
        (
            "bp-00d0820dc5481d5d",
            "3.9",
            "Disposal, plastic, industr. electronics, 15.3% water, to municipal incineration",
            "market/producer-boundary difference",
        ),
        (
            "bp-cccd7819bb6cea4b",
            "3.9",
            "Disposal, rubber, unspecified, 0% water, to municipal incineration",
            "market/producer-boundary difference",
        ),
        (
            "bp-4a8d844635b7637e",
            "3.9",
            "Disposal, rubber, unspecified, 0% water, to municipal incineration",
            "market/producer-boundary difference",
        ),
    ],
)
def test_waste_endpoint_preserves_separate_caller_transport(
    tmp_path, rule_id, version, expected_name, warning_term
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    endpoint = rules[rule_id]
    freight = rules["bp-d6b9d771677aa51e"]
    resolved = graph(endpoint["source"], freight["source"], version=version)
    resolved.activities[0]["exchanges"][2]["amount"] = 3.25
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 3
        waste, road = activity["exchanges"][1:]
        assert waste["name"] == expected_name
        assert waste["amount"] == 2
        assert road["name"] == "Transport, freight, lorry, fleet average"
        assert road["amount"] == 3.25 and road["unit"] == "ton kilometer"
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == endpoint["id"]
        )
        assert warning_term in warning["message"]
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-18e8ed0207bbd5f8", "3.9"),
        ("bp-182cd513cb8969f1", "3.9"),
        ("bp-b5f627281d517e19", "3.12"),
    ],
)
def test_tap_water_proxy_keeps_delivered_kg_and_cached_loss_warning(
    tmp_path, rule_id, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = 2.5
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 2
        water = activity["exchanges"][1]
        assert water["name"] == "Tap water, at user"
        assert water["amount"] == 2.5 and water["unit"] == "kilogram"
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
        )
        assert "Do not multiply the delivered-kg demand" in warning["message"]
        assert "Regionalized water-stress" in warning["message"]
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version,warning_term",
    [
        ("bp-c58e827fcace3923", "3.6", "kg-to-MJ conversion"),
        ("bp-4438f1d4119c16e4", "3.6", "80000h"),
        ("bp-c291034cfcb83f1c", "3.6", "16000m3"),
        ("bp-8e58bbf6b2e31687", "3.6", "40W"),
        ("bp-4389bb6ee9ab35e9", "3.8", "no added loss factor"),
        ("bp-ac05197a0852076a", "3.6", "Add no rolling"),
        ("bp-1cb5578cf49cfd48", "3.6", "primary/secondary shares"),
        ("bp-cac4eeaf7215b33d", "3.6", "0.367102654107643%"),
        ("bp-ac5d1249cfc2626f", "3.6", "kg of residue"),
        ("bp-84bd9cf473cdeee9", "3.6", "kg dry matter"),
        ("bp-84bd9cf473cdeee9", "3.9", "kg dry matter"),
        ("bp-4529f104ab14ba19", "3.12", "Do not add hot rolling"),
        ("bp-07d1ab8592869587", "3.6", "net tkm"),
        ("bp-84737a1a1790ee76", "3.6", "secondary billets"),
        ("bp-2d7bd6f9508a4c17", "3.6", "positive oxide reference"),
        ("bp-b16c64b2ed0f8102", "3.12", "without a 1.53 hydroxide-feed multiplier"),
        ("bp-f0abe48d8c3176d4", "3.9", "without adding hot/cold rolling"),
        ("bp-6ce52de989145df6", "3.12", "without wood-density"),
        (
            "bp-8a37fb9191501441",
            "3.9",
            "internal model assumptions, not conversion factors",
        ),
    ],
)
def test_complete_supply_service_keeps_demand_and_cached_basis_warning(
    tmp_path, rule_id, version, warning_term
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = 2.5
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 2
        edge = activity["exchanges"][1]
        assert edge["amount"] == 2.5 and edge["unit"] == rule["source"]["unit"]
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
        )
        assert warning_term in warning["message"]
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-194bdbdd9a8f228f", "3.6"),
        ("bp-7dd6b6497336da23", "3.12"),
        ("bp-eb6086f7739eb966", "3.12"),
    ],
)
def test_useful_heat_proxy_keeps_caller_mj_without_repeating_burner_factor(
    tmp_path, rule_id, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = 2.5
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 2
        heat = activity["exchanges"][1]
        assert heat["amount"] == 2.5 and heat["unit"] == "megajoule"
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
        )
        assert "native burner multiplier is internal" in warning["message"]
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-6cada1fa986bcf8b", "3.9"),
        ("bp-7fe8308839f735bb", "3.6"),
    ],
)
def test_unresolved_gas_volume_bases_still_refuse_migration(tmp_path, rule_id, version):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-4f69327dbf9d3e42", "3.6"),
        ("bp-4f69327dbf9d3e42", "3.9"),
        ("bp-4f69327dbf9d3e42", "3.12"),
    ],
)
def test_legacy_additive_boundary_remains_unapproved(tmp_path, rule_id, version):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [("bp-8cde42cb022d74ed", "3.6"), ("bp-281c3f362d35e83f", "3.9")],
)
def test_sanitary_inert_keeps_wet_mass_and_cached_native_boundary_warning(
    tmp_path, rule_id, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air", "non-urban air or from high stacks"),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.037,
            "uncertainty type": 0,
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 3
        disposal, emission = activity["exchanges"][1:]
        assert disposal["amount"] == 2.5
        assert disposal["name"] == rule["target"]["name"]
        assert emission["amount"] == 0.037
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
        )
        assert all(
            token in warning["message"]
            for token in ("sanitary", "manganese", "42", "unquantified")
        )
    assert resolved.activities == before


@pytest.mark.parametrize("amount", [-2, 2])
def test_approved_generator_factor_preserves_fuel_draw_and_separate_emissions(
    tmp_path, amount
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_pilot_generator_fuel_review.json").read_text()
    )
    (decision,) = review["rules"]
    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == decision["rule_id"]
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
            "formula": "generator_service",
        },
    )
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air", "non-urban air or from high stacks"),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.037,
            "uncertainty type": 0,
            "formula": "separate_stack_emissions",
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    factor = decision["conversion_factor"]
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 3
        generator, emissions = activity["exchanges"][1:]
        assert generator["amount"] == pytest.approx(amount * factor)
        assert generator["unit"] == "megajoule"
        assert generator["loc"] == pytest.approx(source["loc"] + math.log(factor))
        assert generator["scale"] == source["scale"]
        assert generator["negative"] is (amount < 0)
        assert generator["formula"] == f"(generator_service) * ({factor})"
        assert generator["amount"] * decision[
            "native_diesel_kg_per_reference"
        ] == pytest.approx(amount * decision["source_diesel_kg_per_reference"])
        assert emissions["amount"] == 0.037
        assert emissions["formula"] == "separate_stack_emissions"
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule["id"]
        )
        assert all(
            term in warning["message"]
            for term in ("18.5kW", "10MW", "electrical-output", "unquantified")
        )
    assert resolved.activities == before


@pytest.mark.parametrize("rule_id", ["bp-365d3df4ba55618f", "bp-b80976e787fd9cd2"])
def test_forming_service_keeps_the_separate_caller_polymer(tmp_path, rule_id):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    forming = rules[rule_id]
    polymer = rules["bp-02c0322dff7270a9"]
    resolved = graph(forming["source"], polymer["source"], version="3.9")
    for exchange in resolved.activities[0]["exchanges"][1:]:
        exchange["amount"] = 2
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    (activity,) = result.activities
    assert len(activity["exchanges"]) == 3
    service, material = activity["exchanges"][1:]
    assert all(service[k] == v for k, v in forming["target"].items())
    assert material["name"] == "Polypropylene, granulate, at plant"
    assert service["amount"] == material["amount"] == 2
    warning = next(
        d
        for d in result.diagnostics
        if d["code"] == "clic.compatibility_proxy_limitations"
        and d["details"]["rule_id"] == rule_id
    )
    assert "add no full kg of plastic" in warning["message"]
    assert resolved.activities == before


def test_complete_electronic_components_keep_kg_without_extra_assembly(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    chosen = [
        rules[r]
        for r in ("bp-14d8fca280d9c0bb", "bp-99faa5629490252a", "bp-d9521ecfbb6552d6")
    ]
    amounts = (-2, 0.5, 1.25)
    resolved = graph(*(r["source"] for r in chosen), version="3.12")
    for exchange, amount in zip(resolved.activities[0]["exchanges"][1:], amounts):
        exchange["amount"] = amount
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    (activity,) = result.activities
    assert len(activity["exchanges"]) == 4
    for exchange, rule, amount in zip(activity["exchanges"][1:], chosen, amounts):
        assert all(exchange[k] == v for k, v in rule["target"].items())
        assert exchange["unit"] == "kilogram" and exchange["amount"] == amount
    assert any(
        d["code"] == "clic.compatibility_proxy_limitations"
        and d["details"]["rule_id"] == chosen[0]["id"]
        and "0.7 g example device" in d["message"]
        for d in result.diagnostics
    )
    assert resolved.activities == before


def test_cold_rolling_keeps_separate_hot_rolled_stock_once(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    stock = rules["bp-263c185cdbca7a8c"]
    rolling = rules["bp-8afbd7be259066bf"]
    resolved = graph(stock["source"], rolling["source"], version="3.9")
    for exchange in resolved.activities[0]["exchanges"][1:]:
        exchange["amount"] = 2
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    (activity,) = result.activities
    assert len(activity["exchanges"]) == 3
    material, process = activity["exchanges"][1:]
    assert material["name"] == "Chromium steel 18/8, at plant"
    assert process["name"] == "Sheet rolling, chromium steel"
    assert material["amount"] == process["amount"] == 2
    assert any(
        d["code"] == "clic.compatibility_proxy_limitations"
        and d["details"]["rule_id"] == rolling["id"]
        and "Bulk caller metal remains separate" in d["message"]
        for d in result.diagnostics
    )
    assert resolved.activities == before


def test_active_chemical_basis_preserves_separate_water_and_cached_warning(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    chemical = rules["bp-1d075913a8c7a9b6"]
    water = next(
        r
        for r in rules.values()
        if r["source"]["name"] == "market for water, decarbonised"
        and r["source"]["location"] == "CH"
    )
    resolved = graph(chemical["source"], water["source"], version="3.6")
    resolved.activities[0]["exchanges"][2]["amount"] = 3.25
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 3
        caustic, separate_water = activity["exchanges"][1:]
        assert caustic["amount"] == -2  # Active kg; the label's 50% is not a factor.
        assert separate_water["amount"] == 3.25
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == chemical["id"]
        )
        assert "active NaOH" in warning["message"]
        assert "without a 50% multiplier" in warning["message"]
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-c7c88174c5f11ce8", "3.6"),
        ("bp-c7c88174c5f11ce8", "3.9"),
        ("bp-0ad47155a89ab7bb", "3.9"),
        ("bp-a51655f7a16cfaa3", "3.11"),
        ("bp-e1e1fd1c04f327da", "3.12"),
    ],
)
def test_unresolved_chemical_bases_do_not_inherit_active_product_approval(
    rule_id, version
):
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == rule_id)
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", version, "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )


@pytest.mark.parametrize("amount", [-2, 2])
def test_inspected_paint_market_reuses_water_based_producer_scaling(tmp_path, amount):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-a51655f7a16cfaa3"
    )
    resolved = graph(rule["source"], version="3.12")
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        **{
            "uncertainty type": 2,
            "loc": math.log(abs(amount)),
            "scale": 0.2,
            "negative": amount < 0,
            "formula": "dry_paint_demand",
        },
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        edge = result.activities[0]["exchanges"][1]
        assert edge["name"] == "Alkyd paint, white, 60% in H2O, at plant"
        assert edge["amount"] == pytest.approx(amount / 0.60)
        assert edge["loc"] == pytest.approx(math.log(abs(amount)) + math.log(1 / 0.60))
        assert edge["scale"] == 0.2 and edge["negative"] is (amount < 0)
        assert (
            "dry_paint_demand" in edge["formula"] and str(1 / 0.60) in edge["formula"]
        )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule["id"]
            and "+1/0.60" in d["message"]
            for d in result.diagnostics
        )
    assert resolved.activities == before


def test_zinc_coating_keeps_steel_separate_and_does_not_repeat_area_normalization(
    tmp_path,
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    steel = rules["bp-7ca3587cbfe4ce9b"]
    coating = rules["bp-fef3bd133ce6cadc"]
    resolved = graph(steel["source"], coating["source"], version="3.9")
    resolved.activities[0]["exchanges"][1]["amount"] = 2
    resolved.activities[0]["exchanges"][2]["amount"] = 0.125
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 3
        stock, service = activity["exchanges"][1:]
        assert stock["name"] == "Reinforcing steel, at plant"
        assert stock["amount"] == 2 and stock["unit"] == "kilogram"
        assert service["name"] == "Zinc coating, pieces"
        assert service["amount"] == 0.125 and service["unit"] == "square meter"
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == coating["id"]
            and "60 m2/t" in d["message"]
            and "exclude the steel substrate" in d["message"]
            for d in result.diagnostics
        )
    assert resolved.activities == before


def test_approved_dense_soda_proxy_keeps_missing_densification_warning_in_cache(
    tmp_path,
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-e1e1fd1c04f327da"
    )
    resolved = graph(rule["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 2
        edge = activity["exchanges"][1]
        assert edge["name"] == "Soda, powder, at plant"
        assert edge["unit"] == "kilogram" and edge["amount"] == -2
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule["id"]
        )
        assert all(
            text in warning["message"]
            for text in (
                "light soda",
                "densification",
                "unquantified",
                "33%",
                "density",
            )
        )
        assert warning["details"]["review_path"].endswith(
            "uvek_pilot_dense_soda_review.json"
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-e6c831a29eb4d1fa", "3.6"),
        ("bp-ef0d29b6fa71b9c8", "3.9"),
        ("bp-dc3ac2fe6b0e61bf", "3.9"),
        # Plastic DE/RoW and PE collection now preserve their source fates;
        # their old flat endpoints remain refused in the fingerprint test below.
        ("bp-7df5d08d5bab9d33", "3.9"),
    ],
)
def test_unreviewed_waste_fate_changes_still_refuse_migration(
    tmp_path, rule_id, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_wastewater_endpoint_keeps_treatment_volume_and_cached_stage_warning(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-fb5a0385ef4ba237"
    )
    resolved = graph(rule["source"], version="3.6")
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air", "non-urban air or from high stacks"),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.037,
            "uncertainty type": 0,
            "formula": "separate_caller_emissions",
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 3
        treatment, emission = activity["exchanges"][1:]
        assert (
            treatment["name"] == "Treatment, sewage, to wastewater treatment, class 2"
        )
        assert treatment["amount"] == 2.5 and treatment["unit"] == "cubic meter"
        assert (
            emission["amount"] == 0.037
            and emission["formula"] == "separate_caller_emissions"
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule["id"]
        )
        assert "Residential sewer grid" in warning["message"]
        assert "internal sewer-grid infrastructure" in warning["message"]
        assert "ash extraction/recovery" in warning["message"]
    assert resolved.activities == before


@pytest.mark.parametrize("rule_id", ["bp-3c10f9aaee8ffe42"])
def test_other_wastewater_market_boundaries_remain_unapproved(tmp_path, rule_id):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "review_file,rule_id,version",
    [
        ("uvek_pilot_inert_composition_review.json", "bp-1d429dc5bcb36b3e", "3.9"),
        ("uvek_pilot_european_scrap_steel_review.json", "bp-22c3a52062d0cb16", "3.9"),
        ("uvek_pilot_uncontrolled_market_review.json", "bp-e172491d6da0b115", "3.9"),
        ("uvek_pilot_uncontrolled_market_review.json", "bp-b7606642f4256898", "3.9"),
        ("uvek_pilot_uncontrolled_market_review.json", "bp-5c29c14c8825b76f", "3.12"),
        ("uvek_pilot_pe_collection_review.json", "bp-ccc007a13f79d195", "3.9"),
    ],
)
def test_old_flat_waste_endpoint_remains_unapproved(review_file, rule_id, version):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads((DATA_DIR / "export" / review_file).read_text())
    row = next(r for r in review["records"] if r["rule_id"] == rule_id)
    resource = load_uvek_technosphere_resource()
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        row["previous_rule"],
        TechnosphereProfile("ecoinvent", version, "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )


@pytest.mark.parametrize(
    "rule_id,version",
    [("bp-1d429dc5bcb36b3e", "3.9"), ("bp-0930282b4ba7e381", "3.12")],
)
def test_inert_compositions_keep_components_freight_and_cached_warnings(
    tmp_path, rule_id, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        foreground, helper = result.activities
        assert foreground["exchanges"][1]["amount"] == 2.5
        tech = [x for x in helper["exchanges"] if x["type"] == "technosphere"]
        assert len(tech) == (5 if version == "3.12" else 2)
        biosphere = [x for x in helper["exchanges"] if x["type"] == "biosphere"]
        assert len(biosphere) == (1 if version == "3.12" else 0)
        assert sorted(x["amount"] for x in tech if x["unit"] != "ton kilometer") == [
            1.48e-9,
            1,
        ]
        roads = [x for x in tech if x["unit"] == "ton kilometer"]
        if version == "3.12":
            assert len(roads) == 3
            assert sum(x["amount"] for x in roads) == pytest.approx(0.077)
            omissions = [
                d
                for d in result.diagnostics
                if d["code"] == "migration.recipe_input_omission"
            ]
            assert omissions == []
            assert biosphere[0]["name"] == "Non-hazardous waste disposed"
            assert biosphere[0]["amount"] == 1
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
        )
        assert "retain that source-data gap" in warning["message"]
        assert "unquantified score" in warning["message"]
    assert resolved.activities == before


def test_european_scrap_steel_preserves_mixed_fates_recovery_and_cache_warning(
    tmp_path,
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule_id = "bp-22c3a52062d0cb16"
    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version="3.9")
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 3
        foreground = next(
            a for a in result.activities if a["code"] == resolved.activities[0]["code"]
        )
        assert foreground["exchanges"][1]["amount"] == 2.5
        helpers = [a for a in result.activities if a is not foreground]
        stock = next(a for a in helpers if len(a["exchanges"]) == 1)
        assert (
            stock["exchanges"][0]["amount"] == 1
            and stock["exchanges"][0]["type"] == "production"
        )
        parent = next(a for a in helpers if a is not stock)
        inputs = [x for x in parent["exchanges"] if x["type"] == "technosphere"]
        assert len(inputs) == 6
        fields = ("name", "reference product", "location", "unit")
        recovery = next(x for x in inputs if all(x[f] == stock[f] for f in fields))
        assert recovery["amount"] == -0.47289 * 0.00621659000549189
        assert recovery["negative"] is True
        quantities = {x["name"]: x["amount"] for x in inputs if x is not recovery}
        assert quantities == pytest.approx(
            {
                "Process-specific burdens, inert material landfill": 0.993783409994508,
                "xx Inert material landfill facility": 1.48e-9 * 0.993783409994508,
                "Disposal, steel, 0% water, to municipal incineration": 0.00621659000549189,
                "Transport, freight, rail": 0.0112,
                "Transport, freight, lorry, fleet average": 0.01932,
            }
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
        )
        assert all(
            token in warning["message"]
            for token in (
                "source water/oxygen",
                "manganese",
                "unquantified",
                "-0.47289",
            )
        )
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-e172491d6da0b115", "3.9"),
        ("bp-b7606642f4256898", "3.9"),
        ("bp-5c29c14c8825b76f", "3.12"),
    ],
)
def test_mixed_disposal_markets_keep_complete_owned_graph_and_cached_warnings(
    tmp_path, rule_id, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == (3 if rule_id == "bp-e172491d6da0b115" else 13)
        foreground = next(
            a for a in result.activities if a["code"] == resolved.activities[0]["code"]
        )
        assert foreground["exchanges"][1]["amount"] == 2.5
        root = next(
            a
            for a in result.activities
            if a.get("brightpath_conversion", {})
            .get("recipe_id", "")
            .startswith("pilot-uncontrolled-market-")
        )
        leaves = [
            a
            for a in result.activities
            if a.get("brightpath_conversion", {})
            .get("recipe_id", "")
            .startswith("pilot-uncontrolled-source-")
        ]
        assert len(leaves) == (1 if rule_id == "bp-e172491d6da0b115" else 11)
        assert all(
            a["brightpath_conversion"]["biosphere_binding_profile"]
            == {"family": "ecoinvent", "version": "3.10"}
            for a in leaves
        )
        inputs = [x for x in root["exchanges"] if x["type"] == "technosphere"]
        assert sum(
            x["amount"] for x in inputs if x["unit"] == "kilogram"
        ) == pytest.approx(1, abs=1e-14)
        assert sum(
            x["amount"] for x in inputs if x["unit"] == "ton kilometer"
        ) == pytest.approx(
            0.0683546083228227
            if rule_id == "bp-e172491d6da0b115"
            else 0.077 if version == "3.12" else 0.03052
        )
        if version == "3.12":
            total = sum(
                x["amount"]
                for x in root["exchanges"]
                if x["type"] == "biosphere"
                and x["name"] == "Non-hazardous waste disposed"
            )
            fields = ("name", "reference product", "location", "unit")
            for leaf in leaves:
                link = next(x for x in inputs if all(x[f] == leaf[f] for f in fields))
                total += link["amount"] * sum(
                    x["amount"]
                    for x in leaf["exchanges"]
                    if x["type"] == "biosphere"
                    and x["name"] == "Non-hazardous waste disposed"
                )
            assert total == pytest.approx(1, abs=1e-14)
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule_id
        )
        assert "CAS/label metadata changes" in warning["message"]
        assert "whole-country traction mixes" in warning["message"]
    assert resolved.activities == before


@pytest.mark.parametrize("version", ["3.9", "3.12"])
def test_paperboard_does_not_inherit_uncontrolled_market_approval(tmp_path, version):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-3e43100f56430f0c"
    )
    resolved = graph(rule["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id,version",
    [
        ("bp-9c3675cc3a3f003c", "3.6"),
        ("bp-953779a9d7354634", "3.6"),
        ("bp-67e316270a112622", "3.9"),
        ("bp-1104956dff1c701e", "3.12"),
        ("bp-5968433ef239f628", "3.12"),
        ("bp-7c580ffe9daa9693", "3.12"),
    ],
)
def test_unapproved_material_grades_do_not_inherit_supply_approval(
    tmp_path, rule_id, version
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_pcb_preparation_keeps_service_quantity_and_cached_allocation_warning(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r
        for r in load_uvek_technosphere_resource()["replace"]
        if r["id"] == "bp-9f820f97f9f3c080"
    )
    resolved = graph(rule["source"], version="3.9")
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air", "non-urban air or from high stacks"),
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.037,
            "uncertainty type": 0,
            "formula": "separate_caller_emissions",
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        (activity,) = result.activities
        assert len(activity["exchanges"]) == 3
        preparation, emission = activity["exchanges"][1:]
        assert preparation["name"] == "Disposal, treatment of printed wiring boards"
        assert preparation["amount"] == 2.5 and preparation["unit"] == "kilogram"
        assert (
            emission["amount"] == 0.037
            and emission["formula"] == "separate_caller_emissions"
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == rule["id"]
        )
        assert all(
            term in warning["message"]
            for term in ("100%", "production-only", "0.5tkm", "0.144MJ")
        )
    assert resolved.activities == before


@pytest.mark.parametrize("rule_id", ["bp-6046387e041ea5c2", "bp-0baf74e5f1f09452"])
def test_xanthate_and_grain_oriented_steel_do_not_inherit_processing_approval(
    tmp_path, rule_id
):
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version="3.12")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before
