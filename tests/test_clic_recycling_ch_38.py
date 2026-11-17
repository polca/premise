"""Recycled supply retains review warnings and exact admission on cache replay."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_recycling_ch_38_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
ROWS = [r for r in REVIEW["records"] if not r.get("parent_only_zero_authority")]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_recycling_import_preserves_signed_demand_and_proxy_warnings(tmp_path, amount):
    resolved = graph(*(r["source"] for r in ROWS), version="3.8")
    for e in resolved.activities[0]["exchanges"][1:]:
        e.update(
            amount=amount,
            formula="dose",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
        )
    original = copy.deepcopy(resolved.activities)
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = h.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        for e in result.activities[0]["exchanges"][1:]:
            assert (
                e["amount"] == e["loc"] == amount
                and e["formula"] == "dose"
                and e["scale"] == 0.2
            )
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(s in manifest for s in ROWS[0]["limitations"])
        assert "zero_burden_supplier" in manifest
    assert resolved.activities == original


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_parent_permission_refuses_fresh_and_cached_recycling(
    tmp_path, monkeypatch, cached
):
    resolved = graph(ROWS[0]["source"], version="3.8")
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        h.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [o for o in ledger["operations"] if o["review_path"] != PATH]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=ROWS[0]["rule_id"]):
        h.harmonize(resolved, version="2026", family="uvek")


def test_zero_authority_does_not_independently_admit_its_direct_interface(tmp_path):
    row = next(r for r in REVIEW["records"] if r.get("parent_only_zero_authority"))
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(row["source"], version="3.8"), version="2026", family="uvek"
        )
