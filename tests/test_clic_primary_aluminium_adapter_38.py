"""The primary-metal relabelling correction remains scoped on cache replay."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW_PATH = "export/uvek_primary_aluminium_adapter_38_review.json"
REVIEW = json.loads((DATA_DIR / REVIEW_PATH).read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_primary_metal_correction_keeps_signed_caller_fields(tmp_path, amount):
    resolved = graph(ROW["source"], version="3.8")
    edge = resolved.activities[0]["exchanges"][1]
    edge.update(
        amount=amount,
        formula="metal_dose",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 1
        converted = result.activities[0]["exchanges"][1]
        assert all(converted[k] == value for k, value in ROW["target"].items())
        for key in ("amount", "loc", "scale", "formula"):
            assert converted[key] == edge[key]
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for limit in ROW["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_correction_refuses_fresh_or_cached_import(
    tmp_path, monkeypatch, cached
):
    resolved = graph(ROW["source"], version="3.8")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        operation
        for operation in ledger["operations"]
        if operation["review_path"] != REVIEW_PATH
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=ROW["rule_id"]):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
