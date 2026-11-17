"""Reviewed source 3.8 market helpers survive cache replay and permission withdrawal."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_trucks_five_markets_38_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
ROWS = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_market_helpers_keep_signed_caller_quantities_and_warnings_on_cache_replay(
    tmp_path, amount
):
    resolved = graph(*(row["source"] for row in ROWS), version="3.8")
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
        assert len(result.activities) == 6
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        for edge, row in zip(result.activities[0]["exchanges"][1:], ROWS, strict=True):
            factor = row["conversion_factor"]
            assert (
                edge["amount"] == edge["loc"] == amount * factor
                and edge["scale"] == 0.2
            )
            assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
            assert edge["name"] == helpers[row["target_recipe"]]["name"]
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for row in ROWS for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_market_approval_is_checked_before_cache_reuse(
    tmp_path, monkeypatch, cached
):
    resolved = graph(ROWS[0]["source"], version="3.8")
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
