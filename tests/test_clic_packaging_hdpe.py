"""Exact HDPE profile audits survive caching without cross-profile omission approval."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer


@pytest.mark.parametrize("version,omissions", [("3.8", 4), ("3.11", 0)])
def test_hdpe_stock_delivery_and_exact_omissions_fresh_and_cached(
    tmp_path, version, omissions
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = json.loads(
        (DATA_DIR / "export/uvek_packaging_hdpe_review.json").read_text()
    )
    row = next(
        r for r in review["records"] if r["source_profile"]["version"] == version
    )
    resolved = graph(review["original_rule"]["source"], version=version)
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=-2.5,
        formula="signed_hdpe",
        **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == -2.5
        assert edge["scale"] == 0.3 and edge["formula"] == "signed_hdpe"
        ds = next(a for a in result.activities if a["code"] == edge["input"][1])
        actual = [e for e in ds["exchanges"] if e["type"] != "production"]
        expected = review["recipes"][row["new_recipe_id"]]["inputs"]
        assert len(actual) == 5
        assert [e["amount"] for e in actual] == [e["amount"] for e in expected]
        loss_rows = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        assert len(loss_rows) == omissions
        assert all(
            d["details"]["source_profile"] == row["source_profile"] for d in loss_rows
        )
        assert (
            ds.get("brightpath_conversion", {}).get("source_input_omissions", [])
            == row["source_omissions"]
        )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            for d in result.diagnostics
        )
    assert resolved.activities == before


def test_38_omissions_need_exact_separate_approval(tmp_path, monkeypatch):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals

    review = json.loads(
        (DATA_DIR / "export/uvek_packaging_hdpe_review.json").read_text()
    )
    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    for operation in ledger["operations"]:
        if (
            operation["rule_id"] == review["rule_id"]
            and operation["inspection"].get("source_profile", {}).get("version")
            == "3.8"
        ):
            operation["inspection"]["approved_recipe_source_omissions"] = {}
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    with pytest.raises(ClicInventoryError, match="unapproved losses"):
        harmonizer.harmonize(
            graph(review["original_rule"]["source"], version="3.8"),
            version="2026",
            family="uvek",
        )
