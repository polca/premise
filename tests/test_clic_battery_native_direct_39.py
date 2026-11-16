"""Fourteen exact native supply reviews survive harmonization and cache replay."""

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
    (DATA_DIR / "export/uvek_battery_native_direct_39_review.json").read_text()
)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_native_supplies_keep_caller_quantities_and_warnings_on_cache_replay(
    tmp_path, amount
):
    rows = REVIEW["records"]
    resolved = graph(*(row["source"] for row in rows), version="3.9")
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="material_dose",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 1
        for edge, row in zip(result.activities[0]["exchanges"][1:], rows, strict=True):
            assert all(edge[k] == value for k, value in row["target"].items())
            assert edge["amount"] == edge["loc"] == amount
            assert edge["scale"] == 0.2 and edge["formula"] == "material_dose"
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for row in rows for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_review_refuses_fresh_or_previously_cached_conversion(
    tmp_path, monkeypatch, cached
):
    row = REVIEW["records"][0]
    resolved = graph(row["source"], version="3.9")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        operation
        for operation in ledger["operations"]
        if operation["review_path"]
        != "export/uvek_battery_native_direct_39_review.json"
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
