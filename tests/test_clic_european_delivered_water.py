"""Explicit RER water grants preserve delivered kg and supersede only CH grants."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_european_delivered_water_review.json").read_text()
)["rules"][0]


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.9", "3.12"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_rer_delivered_water_preserves_signed_kg_and_warnings_through_cache(
    tmp_path, version, amount
):
    resolved = graph(REVIEW["source"], version=version)
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="water",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert all(edge[k] == v for k, v in REVIEW["target"].items())
        assert edge["location"] == "RER" and edge["amount"] == edge["loc"] == amount
        assert edge["formula"] == "water" and edge["scale"] == 0.2
        assert len(result.activities) == 1
        text = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in text for limit in REVIEW["limitation_items"])
    assert resolved.activities == before


def test_no_other_version_permission_can_replace_the_new_312_rer_grant(
    tmp_path, monkeypatch
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    decision = next(
        o for o in ledger["operations"] if o["rule_id"] == REVIEW["rule_id"]
    )
    decision["scopes"] = [
        s for s in decision["scopes"] if s["source"]["version"] != "3.12"
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(REVIEW["source"], version="3.12")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=REVIEW["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
