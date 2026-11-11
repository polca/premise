"""Waste-oil recipe needs independent exact approval of its source omission."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_fuels_37_waste_oil_review.json").read_text()
)
RECIPE_ID = "fuels-37-allocated-waste-mineral-oil"


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_fresh_cached_treatment_sign_statistics_and_literal_omission(
    tmp_path, amount, uncertainty
):
    from brightpath.background import PackageCatalogProvider

    resolved = graph(REVIEW["original_rule"]["source"], version="3.7")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=amount,
        formula="signed_quantity",
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
        assert len(result.activities) == 2
        edge = result.activities[0]["exchanges"][1]
        assert (
            edge["amount"] == -amount
            and edge["formula"] == "(signed_quantity) * (-1.0)"
        )
        assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.3
        assert edge["loc"] == (incoming["loc"] if uncertainty == 2 else -amount)
        if uncertainty == 2:
            assert edge["negative"] is (-amount < 0)
        helper = next(a for a in result.activities if a.get("code") == edge["input"][1])
        assert (
            helper["brightpath_conversion"]["source_input_omissions"]
            == REVIEW["recipes"][RECIPE_ID]["source_input_omissions"]
        )
        assert [
            e["amount"] for e in helper["exchanges"] if e["type"] != "production"
        ] == [0.01932, 0.0112, 0.397600148816246, 0.397566014085865]
        assert any(
            d["code"] == "migration.recipe_source_input_omission"
            and d["severity"] == "warning"
            for d in result.diagnostics
        )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["message"] == " ".join(REVIEW["limitations"])
            for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("change", ["withdraw", "alter"])
def test_cached_conversion_cannot_bypass_literal_omission_approval(
    tmp_path, monkeypatch, change
):
    from brightpath.background import PackageCatalogProvider
    from premise.inventory import clic_rule_approvals

    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    resolved = graph(REVIEW["original_rule"]["source"], version="3.7")
    harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    operation = next(
        o for o in ledger["operations"] if o["rule_id"] == REVIEW["rule_id"]
    )
    if change == "withdraw":
        operation["inspection"]["approved_recipe_source_omissions"] = {}
    else:
        operation["inspection"]["approved_recipe_source_omissions"][RECIPE_ID][0][
            "source_exchange"
        ]["amount"] = -0.2
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="unapproved losses"):
        harmonizer.harmonize(resolved, version="2026", family="uvek")


def test_rule_without_review_is_refused(tmp_path, monkeypatch):
    from brightpath.background import PackageCatalogProvider
    from premise.inventory import clic_rule_approvals

    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        o for o in ledger["operations"] if o["rule_id"] != REVIEW["rule_id"]
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(REVIEW["original_rule"]["source"], version="3.7"),
            version="2026",
            family="uvek",
        )
