"""Waste markets retain caller reference signs, original shares and explicit empty-terminal audits."""

import copy
import json
import math

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_deferred_concrete_review.json").read_text()
)


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source"]["location"]
)
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_concrete_market_market_keeps_fates_and_warnings_on_cold_and_warm_cache(
    tmp_path, row, amount, uncertainty
):
    resolved = graph(row["source"], version="3.9")
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount) or 1) if uncertainty == 2 else amount,
            "scale": 0.2,
        },
    )
    if uncertainty == 2:
        exchange["negative"] = amount < 0
    else:
        exchange.update(minimum=amount - 1, maximum=amount + 1)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        actual = result.activities[0]["exchanges"][1]
        assert actual["amount"] == -amount
        assert actual["formula"] == "(dose) * (-1.0)"
        assert actual["scale"] == exchange["scale"]
        if uncertainty == 2:
            assert actual["loc"] == exchange["loc"]
            assert actual["negative"] is (not exchange["negative"])
        else:
            assert actual["loc"] == -exchange["loc"]
            assert actual["minimum"] == -exchange["maximum"]
            assert actual["maximum"] == -exchange["minimum"]
        helper = next(
            a for a in result.activities if a.get("code") == actual["input"][1]
        )
        inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
        expected = row["profiles"][0]["inputs"]
        assert len(inputs) == len(expected)
        assert len(result.activities) == row["expected_foreground_records"]
        assert all(
            all(actual_input[k] == v for k, v in original.items() if k != "recipe")
            for actual_input, original in zip(inputs, expected, strict=True)
        )
        assert (
            sum(
                x["type"] == "biosphere"
                for a in result.activities[1:]
                for x in a["exchanges"]
            )
            == row["expected_foreground_biosphere_exchanges"]
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == row["rule_id"]
        )
        assert warning["message"] == row["limitations"]
        assert warning["details"]["inspection"] == row
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
        losses = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        helper_ids = {
            a.get("brightpath_conversion", {}).get("recipe_id")
            for a in result.activities
        }
        approved = {
            rid: values
            for rid, values in row["approved_recipe_source_omissions"].items()
            if rid in helper_ids
        }
        assert len(losses) == sum(len(v) for v in approved.values())
        assert all(
            d["details"]["omission"] in approved[d["details"]["recipe_id"]]
            for d in losses
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source"]["location"]
)
@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_concrete_market_review_does_not_admit_other_releases(tmp_path, row, version):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before
