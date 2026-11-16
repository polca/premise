"""New dolomite permissions preserve the existing source3.8 conversion."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_battery_dolomite_39_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_dolomite_delivery_and_native_producer_keep_signs_and_cache_warnings(
    tmp_path, amount
):
    rows = REVIEW["records"]
    resolved = graph(*(row["compiled_rule"]["source"] for row in rows), version="3.9")
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="mineral_dose",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 2
        for edge in result.activities[0]["exchanges"][1:]:
            assert edge["amount"] == edge["loc"] == amount
            assert edge["scale"] == 0.2 and edge["formula"] == "mineral_dose"
        assert len(result.activities[1]["exchanges"]) == 11
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for row in rows for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_source39_review_refuses_new_conversion_but_preserves_source38(
    tmp_path, monkeypatch, cached
):
    source = REVIEW["records"][0]["compiled_rule"]["source"]
    resolved = graph(source, version="3.9")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        operation
        for operation in ledger["operations"]
        if operation["review_path"] != PATH
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=REVIEW["records"][0]["rule_id"]):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    old = harmonizer.harmonize(
        graph(source, version="3.8"), version="2026", family="uvek"
    )
    assert (
        old.activities[1]["brightpath_conversion"]["recipe_id"]
        == "ships-38-dolomite-market"
    )
