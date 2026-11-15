"""NMC532 screening approval retain signs and audit warnings through cold and warm caches."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.scaling import scale_exchange
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_nmc532_battery_312_review.json").read_text()
)
ROWS = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_nmc532_source_keep_statistics_and_visible_limits_on_cached_conversion(
    tmp_path, amount, uncertainty
):
    resolved = graph(*(r["original_rule"]["source"] for r in ROWS), version="3.12")
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="source_demand",
            **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
        )
        if uncertainty == 2:
            edge.update(loc=math.log(abs(amount)), negative=amount < 0)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 2
        for i, row in enumerate(ROWS, 1):
            expected = copy.deepcopy(before[0]["exchanges"][i])
            scale_exchange(expected, row["compiled_rule"]["conversion_factor"])
            actual = result.activities[0]["exchanges"][i]
            for key in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "formula",
            ):
                if key in expected:
                    assert actual[key] == expected[key]
            warning = next(
                item
                for item in result.diagnostics
                if item["code"] == "clic.compatibility_proxy_limitations"
                and item["details"]["rule_id"] == row["rule_id"]
            )
            assert warning["details"]["limitations"] == row["limits"]
            assert all(limit in warning["message"] for limit in row["limits"])
    assert resolved.activities == before


@pytest.mark.parametrize("rid", [r["rule_id"] for r in ROWS])
def test_withdrawing_nmc532_scope_refuses_before_losing_source(
    tmp_path, monkeypatch, rid
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for row in ledger["operations"]:
        if row["rule_id"] == rid:
            row["scopes"] = []
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(*(r["original_rule"]["source"] for r in ROWS), version="3.12")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
