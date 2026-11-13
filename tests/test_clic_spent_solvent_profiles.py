"""Reviewed wet-solvent disposal preserves demand through harmonization and caching."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory import clic_rule_approvals as approvals


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("version", ["3.6", "3.7", "3.9", "3.12"])
def test_solvent_profile_preserves_signed_units_and_review_in_fresh_and_cached_runs(
    tmp_path, version, amount
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = json.loads(
        (DATA_DIR / "export/uvek_spent_solvent_profiles_review.json").read_text()
    )
    resolved = graph(review["rule"]["source"], version=version)
    edge = resolved.activities[0]["exchanges"][1]
    edge.update(
        amount=amount,
        formula="signed_solvent",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        migrated = result.activities[0]["exchanges"][1]
        assert migrated["amount"] == migrated["loc"] == -amount
        assert migrated["scale"] == 0.3 and migrated["uncertainty type"] == 3
        assert migrated["formula"] == "(signed_solvent) * (-1.0)"
        assert all(
            migrated[key] == value for key, value in review["rule"]["target"].items()
        )
        assert len(result.activities) == 1
        text = json.dumps(result.manifest)
        assert "uvek_spent_solvent_profiles_review.json" in text
        assert all(limit in text for limit in review["limitations"])
    assert resolved.activities == before


def test_solvent_39_cannot_borrow_another_profile_approval(tmp_path, monkeypatch):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    ledger = copy.deepcopy(approvals._approval_ledger())
    row = next(
        row for row in ledger["operations"] if row["rule_id"] == "bp-1be0fde93275f330"
    )
    assert any(scope["source"]["version"] == "3.9" for scope in row["scopes"])
    row["scopes"] = [
        scope for scope in row["scopes"] if scope["source"]["version"] != "3.9"
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    review = json.loads(
        (DATA_DIR / "export/uvek_spent_solvent_profiles_review.json").read_text()
    )
    resolved = graph(review["rule"]["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="bp-1be0fde93275f330"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
