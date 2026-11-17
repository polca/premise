"""Fresh and cached imports retain shared market decisions and withdrawal."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_casting_utilities_39_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_exact_shared_markets_preserve_signed_demand_and_cached_proxy_warnings(
    tmp_path, amount
):
    resolved = graph(*(r["source"] for r in ROWS), version="3.9")
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="dose",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 2
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        for edge, row in zip(result.activities[0]["exchanges"][1:], ROWS, strict=True):
            target = row["target"]
            if "recipe_ref" in target:
                target = helpers[target["recipe_ref"]]
            assert all(
                edge[k] == target[k]
                for k in ("name", "reference product", "location", "unit")
            )
            assert edge["amount"] == edge["loc"] == amount and edge["formula"] == "dose"
            assert edge["scale"] == 0.2
        manifest = json.dumps(result.manifest)
        assert "1.04MJfuel" in manifest
        assert "Quebec" in manifest
        assert "no new zero authority" in manifest
    assert resolved.activities == before


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_shared_market_approval_refuses_before_cache_reuse(
    tmp_path, monkeypatch, cached
):
    resolved = graph(ROWS[0]["source"], version="3.9")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        op for op in ledger["operations"] if op["review_path"] != PATH
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=ROWS[0]["rule_id"]):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
