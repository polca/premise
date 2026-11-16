"""Tin source3.8 review survives caches without changing source3.9 permission."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_truck_tin_38_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_tin_keeps_signed_quantity_and_review_on_fresh_and_cached_import(
    tmp_path, amount
):
    resolved = graph(ROW["source"], version="3.8")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="tin_dose",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    original = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 1
        edge = result.activities[0]["exchanges"][1]
        assert all(edge[k] == value for k, value in ROW["target"].items())
        assert edge["amount"] == edge["loc"] == amount
        assert edge["formula"] == "tin_dose" and edge["scale"] == 0.2
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for limit in ROW["limitations"])
    assert resolved.activities == original


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_source38_review_refuses_it_while_source39_stays_approved(
    tmp_path, monkeypatch, cached
):
    resolved = graph(ROW["source"], version="3.8")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        op for op in ledger["operations"] if op["review_path"] != PATH
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=ROW["rule_id"]):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    prior = harmonizer.harmonize(
        graph(ROW["source"], version="3.9"), version="2026", family="uvek"
    )
    assert len(prior.activities) == 1
    assert prior.activities[0]["exchanges"][1]["name"] == ROW["target"]["name"]
