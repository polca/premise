"""Exact gas/wood approvals preserve caller statistics and all three older scopes."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_fuels_38_gas_wood_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
RECORDS = {row["rule_id"]: row for row in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_strict_fresh_cache_preserves_market_statistics(
    tmp_path, rid, amount, uncertainty
):
    from brightpath.background import PackageCatalogProvider

    row = RECORDS[rid]
    target = row["new_profile"].get("target", row["compiled_rule"]["target"])
    source = graph(row["original_rule"]["source"], version="3.8")
    incoming = source.activities[0]["exchanges"][1]
    incoming.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
            "scale": 0.3,
            "negative": amount < 0,
        },
    )
    before = copy.deepcopy(source.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(source, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == (2 if "recipe_ref" in target else 1)
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == amount and edge["formula"] == "dose"
        assert (
            edge["uncertainty type"] == uncertainty and edge["loc"] == incoming["loc"]
        )
        assert edge["scale"] == 0.3
        target = row["new_profile"].get("target", row["compiled_rule"]["target"])
        if "recipe_ref" in target:
            from brightpath.migrations.resources import load_uvek_technosphere_resource

            helper = result.activities[1]
            expected_rows = load_uvek_technosphere_resource()["recipes"][
                target["recipe_ref"]
            ]["inputs"]
            rows = [e for e in helper["exchanges"] if e["type"] != "production"]
            assert len(rows) == len(expected_rows) == 2
            for actual, expected in zip(rows, expected_rows, strict=True):
                assert all(actual.get(k) == v for k, v in expected.items())
        else:
            assert all(edge[k] == v for k, v in target.items())
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["message"] == " ".join(row["limitations"])
            for d in result.diagnostics
        )
    assert source.activities == before


@pytest.mark.parametrize("rid", RECORDS)
def test_new_profile_requires_its_own_approval(tmp_path, monkeypatch, rid):
    from brightpath.background import PackageCatalogProvider
    from premise.inventory import clic_rule_approvals

    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        o
        for o in ledger["operations"]
        if not (o["rule_id"] == rid and o["review_path"] == PATH)
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(RECORDS[rid]["original_rule"]["source"], version="3.8"),
            version="2026",
            family="uvek",
        )


@pytest.mark.parametrize(
    "rid,version",
    [
        ("bp-745c517f85a78ba7", "3.9"),
        ("bp-84bd9cf473cdeee9", "3.6"),
        ("bp-84bd9cf473cdeee9", "3.9"),
    ],
)
def test_three_previous_approved_profiles_keep_their_direct_suppliers(
    tmp_path, rid, version
):
    from brightpath.background import PackageCatalogProvider

    row = RECORDS[rid]
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(
        graph(row["original_rule"]["source"], version=version),
        version="2026",
        family="uvek",
    )
    target = row["original_rule"]["target"]
    assert len(result.activities) == 1
    edge = result.activities[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in target.items())
