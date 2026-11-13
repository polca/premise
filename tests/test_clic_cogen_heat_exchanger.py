"""The Jakobsberg component correction survives harmonization and caching."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory import clic_rule_approvals as approvals


@pytest.mark.parametrize("version", ["3.8", "3.9", "3.12"])
def test_heat_exchanger_profile_preserves_signed_units_and_review_in_fresh_and_cached_runs(
    tmp_path, version
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = next(
        row
        for row in json.loads(
            (DATA_DIR / "export/uvek_cogen_heat_exchanger_review.json").read_text()
        )["rules"]
        if row["rule_id"] == "bp-3e3963da2137593c"
    )
    resolved = graph(review["source"], version=version)
    edge = resolved.activities[0]["exchanges"][1]
    edge.update(
        amount=-2.5,
        formula="signed_heat_exchanger",
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
        assert migrated["formula"] == "signed_heat_exchanger"
        assert all(migrated[key] == value for key, value in review["target"].items())
        assert len(result.activities) == 1
        text = json.dumps(result.manifest, ensure_ascii=False)
        assert "uvek_cogen_heat_exchanger_review.json" in text
        assert review["limitations"] in text
    assert resolved.activities == before


def test_heat_exchanger_39_cannot_borrow_another_profile_approval(
    tmp_path, monkeypatch
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    ledger = copy.deepcopy(approvals._approval_ledger())
    row = next(
        row for row in ledger["operations"] if row["rule_id"] == "bp-3e3963da2137593c"
    )
    assert any(scope["source"]["version"] == "3.9" for scope in row["scopes"])
    row["scopes"] = [
        scope for scope in row["scopes"] if scope["source"]["version"] != "3.9"
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    review = next(
        r
        for r in json.loads(
            (DATA_DIR / "export/uvek_cogen_heat_exchanger_review.json").read_text()
        )["rules"]
        if r["rule_id"] == row["rule_id"]
    )
    resolved = graph(review["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="bp-3e3963da2137593c"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
