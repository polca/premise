"""Source-3.12 train markets retain shares, freight uncertainty and waste signs."""

import copy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph


@pytest.mark.parametrize("amount", [-2, 2])
def test_train_market_helpers_keep_current_inventory_and_warnings_on_cache_hits(
    tmp_path, amount
):
    review = json.loads(
        (
            DATA_DIR / "export/uvek_hydrogen_train_three_markets_312_review.json"
        ).read_text()
    )
    rows = review["records"]
    resolved = graph(*(row["source"] for row in rows), version="3.12")
    for exchange in resolved.activities[0]["exchanges"][1:]:
        exchange.update(
            amount=amount,
            **{
                "uncertainty type": 3,
                "loc": amount,
                "scale": 0.1,
                "minimum": amount - 0.2,
                "maximum": amount + 0.2,
                "formula": "dose",
            },
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 4
        helpers = {
            a.get("brightpath_conversion", {}).get("recipe_id"): a
            for a in result.activities[1:]
        }
        for row, exchange in zip(
            rows, result.activities[0]["exchanges"][1:], strict=True
        ):
            factor = row["source_reference_amount"]
            assert exchange["amount"] == exchange["loc"] == amount * factor
            assert exchange["scale"] == 0.1
            assert exchange["minimum"] == pytest.approx(amount * factor - 0.2)
            assert exchange["maximum"] == pytest.approx(amount * factor + 0.2)
            assert "dose" in exchange["formula"]
            if factor < 0:
                assert "-1" in exchange["formula"]
            helper = helpers[row["recipe_id"]]
            expected = review["recipes"][row["recipe_id"]]["inputs"]
            actual = [e for e in helper["exchanges"] if e["type"] != "production"]
            assert len(actual) == len(expected)
            for got, want in zip(actual, expected, strict=True):
                assert all(got[k] == v for k, v in want.items())
            assert any(
                issue["code"] == "clic.compatibility_proxy_limitations"
                and issue["details"]["rule_id"] == row["rule_id"]
                for issue in result.diagnostics
            )
    assert resolved.activities == before
