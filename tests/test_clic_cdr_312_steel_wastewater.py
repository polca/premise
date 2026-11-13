"""Exact 3.12 approvals retain steel mass and signed wastewater treatment demand."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

RECORDS = json.loads(
    (DATA_DIR / "export/uvek_cdr_312_steel_wastewater_review.json").read_text()
)["records"]


@pytest.mark.parametrize("record", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_complete_native_supplier_and_review_survive_fresh_and_cached_runs(
    tmp_path, record, amount
):
    resolved = graph(record["source"], version="3.12")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="dose",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        factor = record["conversion_factor"]
        assert all(edge[k] == v for k, v in record["target"].items())
        assert edge["amount"] == edge["loc"] == amount * factor
        assert edge["scale"] == 0.2
        assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        text = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in text for limit in record["limitations"])
        assert len(result.activities) == 1
    assert resolved.activities == before


@pytest.mark.parametrize("record", RECORDS)
def test_another_profile_cannot_replace_the_312_grant(tmp_path, monkeypatch, record):
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        o
        for o in ledger["operations"]
        if not (
            o["rule_id"] == record["rule_id"]
            and o["review_path"].endswith("uvek_cdr_312_steel_wastewater_review.json")
        )
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(record["source"], version="3.12")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=record["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
