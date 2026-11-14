"""Chemical batches keep all caller quantities, original delivery entries and warnings through cache."""

import copy
import json
import math

import pytest
from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_chemical_markets_39_review.json").read_text()
)
ROWS = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_nine_chemicals_keep_caller_fields_delivery_and_limits_in_fresh_and_cached_batches(
    tmp_path, amount, uncertainty
):
    resolved = graph(*(r["original_rule"]["source"] for r in ROWS), version="3.9")
    for i, e in enumerate(resolved.activities[0]["exchanges"][1:]):
        e.update(
            amount=amount,
            formula=f"chemical_demand_{i}",
            **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
        )
        if uncertainty == 2:
            e.update(loc=math.log(abs(amount)), negative=amount < 0)
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cache_hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cache_hit
        assert len(result.activities) == 7
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        for i, row in enumerate(ROWS, 1):
            e = result.activities[0]["exchanges"][i]
            for key in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "formula",
            ):
                if key in before[0]["exchanges"][i]:
                    assert e[key] == before[0]["exchanges"][i][key]
            target = (
                helpers[row["recipe_id"]]
                if row["recipe_id"]
                else row["compiled_rule"]["target"]
            )
            assert all(
                e[k] == target[k]
                for k in ["name", "reference product", "unit", "location"]
            )
            if row["recipe_id"]:
                actual = [e for e in target["exchanges"] if e["type"] != "production"]
                expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
                assert len(actual) == len(expected)
                assert all(
                    all(e[k] == v for k, v in r.items())
                    for e, r in zip(actual, expected, strict=True)
                )
                assert not target["brightpath_conversion"].get("source_input_omissions")
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        manifest = json.dumps(result.manifest)
        assert all(limit in manifest for row in ROWS for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("rid", [r["rule_id"] for r in ROWS])
def test_one_missing_approval_refuses_the_whole_batch_without_mutation(
    tmp_path, monkeypatch, rid
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for op in ledger["operations"]:
        if op["rule_id"] == rid:
            op["scopes"] = [s for s in op["scopes"] if s["source"]["version"] != "3.9"]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(*(r["original_rule"]["source"] for r in ROWS), version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=rid):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
