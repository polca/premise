"""Strict CLIC preserves allocated compost handover without clearing treatment."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_compost_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_compost_fresh_and_cached_keeps_quantity_allocation_and_delivery(
    tmp_path, rid, amount
):
    from brightpath.background import PackageCatalogProvider

    row = RECORDS[rid]
    resolved = graph(row["original_rule"]["source"], version="3.8")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="signed_compost",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == amount
        assert edge["formula"] == "signed_compost" and edge["scale"] == 0.3
        ds = next(a for a in result.activities if a["code"] == edge["input"][1])
        stock = next(
            a
            for a in result.activities
            if a.get("brightpath_conversion", {}).get("zero_burden")
        )
        assert len(stock["exchanges"]) == 1
        assert stock["exchanges"][0]["type"] == "production"
        assert stock["exchanges"][0]["amount"] == 1
        if row["role"] == "delivered_compost_market":
            entries = [e for e in ds["exchanges"] if e["type"] != "production"]
            assert len(entries) == 8
            assert len([e for e in entries if e["unit"] == "kilogram"]) == 6
            for actual, retained in zip(entries, row["retained_rows"], strict=True):
                for field, value in retained["statistics"].items():
                    assert actual[field] == value
        else:
            assert ds == stock
            assert row["cutoff_pair_witness"]["waste_reference_amounts"] == [-1]
            assert row["cutoff_pair_witness"]["waste_nonzero_exchange_count"] > 0
        assert any(
            d["code"] == "migration.zero_burden_supplier" for d in result.diagnostics
        )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["message"] == " ".join(row["limitations"])
            for d in result.diagnostics
        )
        assert not any(
            d["code"] == "migration.recipe_source_input_omission"
            for d in result.diagnostics
        )
        assert not any(a["name"].startswith("xx Compost") for a in result.activities)
    assert resolved.activities == before


@pytest.mark.parametrize("rid", RECORDS)
def test_allocated_stock_or_market_needs_its_individual_approval(
    tmp_path, monkeypatch, rid
):
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals

    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        row for row in ledger["operations"] if row["rule_id"] != rid
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(RECORDS[rid]["original_rule"]["source"], version="3.8"),
            version="2026",
            family="uvek",
        )
