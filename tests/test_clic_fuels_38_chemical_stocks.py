"""Exact chemical-stock approvals preserve signed dose and retain proxy warnings."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_fuels_38_chemical_stocks_review.json").read_text()
)
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_native_stock_fresh_and_cached_keeps_dose_and_warnings(
    tmp_path, rid, amount, uncertainty
):
    from brightpath.background import PackageCatalogProvider

    row = RECORDS[rid]
    resolved = graph(row["rule"]["source"], version="3.8")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=amount,
        formula="signed_chemical",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
            "scale": 0.3,
            "negative": amount < 0,
        },
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 1
        edge = result.activities[0]["exchanges"][1]
        assert all(edge[k] == v for k, v in row["rule"]["target"].items())
        assert edge["amount"] == amount and edge["formula"] == "signed_chemical"
        assert edge["uncertainty type"] == uncertainty
        assert edge["loc"] == incoming["loc"] and edge["scale"] == 0.3
        if uncertainty == 2:
            assert edge["negative"] is (amount < 0)
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["message"] == " ".join(row["limitations"])
            for d in result.diagnostics
        )
        assert not any(
            d["code"] == "migration.recipe_source_input_omission"
            for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("rid", RECORDS)
def test_exact_profile_does_not_pass_without_its_new_review(tmp_path, monkeypatch, rid):
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals

    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        row
        for row in ledger["operations"]
        if row.get("review_path") != "export/uvek_fuels_38_chemical_stocks_review.json"
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(RECORDS[rid]["rule"]["source"], version="3.8"),
            version="2026",
            family="uvek",
        )
