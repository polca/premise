"""Concrete sorting preserves physical waste demand across reference signs."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

ROW = json.loads((DATA_DIR / "export/uvek_deferred_sorting_review.json").read_text())[
    "rules"
][0]


@pytest.mark.parametrize("amount", [-2, 2])
def test_sorting_cold_and_cached_sign_and_portable_limitations(tmp_path, amount):
    resolved = graph(ROW["source"], version="3.6")
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": 3,
            "loc": amount,
            "scale": 0.2,
            "minimum": amount - 1,
            "maximum": amount + 1,
        },
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        actual = result.activities[0]["exchanges"][1]
        assert all(actual[k] == v for k, v in ROW["target"].items())
        assert actual["amount"] == -amount and actual["loc"] == -amount
        assert actual["minimum"] == -(amount + 1) and actual["maximum"] == -(amount - 1)
        assert actual["scale"] == 0.2 and actual["formula"] == "(dose) * (-1.0)"
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["message"] == ROW["limitations"]
            for d in result.diagnostics
        )
    assert resolved.activities == before
