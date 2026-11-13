"""Reviewed equipment source profiles survive harmonization and caching."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory import clic_rule_approvals as approvals


@pytest.mark.parametrize("rule_id", ["bp-7cf08aa845443b7b", "bp-4438f1d4119c16e4"])
@pytest.mark.parametrize("version", ["3.6", "3.9", "3.12"])
def test_equipment_profile_preserves_signed_units_and_review_in_fresh_and_cached_runs(
    tmp_path, version, rule_id
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = next(
        row
        for row in json.loads(
            (DATA_DIR / "export/uvek_cdr_equipment_profiles_review.json").read_text()
        )["records"]
        if row["rule_id"] == rule_id
    )
    resolved = graph(review["source"], version=version)
    edge = resolved.activities[0]["exchanges"][1]
    edge.update(
        amount=-2.5,
        formula="signed_equipment",
        **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        migrated = result.activities[0]["exchanges"][1]
        assert migrated["amount"] == migrated["loc"] == -2.5
        assert migrated["scale"] == 0.3 and migrated["uncertainty type"] == 3
        assert migrated["formula"] == "signed_equipment"
        assert all(migrated[key] == value for key, value in review["target"].items())
        assert len(result.activities) == 1
        text = json.dumps(result.manifest, ensure_ascii=False)
        assert "uvek_cdr_equipment_profiles_review.json" in text
        assert all(limit in text for limit in review["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("rule_id", ["bp-7cf08aa845443b7b", "bp-4438f1d4119c16e4"])
def test_equipment_39_cannot_borrow_another_profile_approval(
    tmp_path, monkeypatch, rule_id
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    ledger = copy.deepcopy(approvals._approval_ledger())
    row = next(row for row in ledger["operations"] if row["rule_id"] == rule_id)
    assert any(scope["source"]["version"] == "3.9" for scope in row["scopes"])
    row["scopes"] = [
        scope for scope in row["scopes"] if scope["source"]["version"] != "3.9"
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    review = next(
        r
        for r in json.loads(
            (DATA_DIR / "export/uvek_cdr_equipment_profiles_review.json").read_text()
        )["records"]
        if r["rule_id"] == row["rule_id"]
    )
    resolved = graph(review["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=rule_id):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
