"""The pilot preserves the approved minor foam-glass feedstock proxy, operating quantities and warnings."""

import copy
import json
import math
from pathlib import Path

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import compatibility_approval_metadata
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_pilot_nitrate_foam_review.json").read_text()
)


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source"]["location"]
)
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_foam_market_keeps_producers_and_warnings_on_cold_and_warm_cache(
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
        assert actual["amount"] == amount
        assert actual["formula"] == "dose"
        assert actual["scale"] == exchange["scale"]
        if uncertainty == 2:
            assert actual["loc"] == exchange["loc"]
            assert actual["negative"] is exchange["negative"]
        else:
            assert actual["loc"] == exchange["loc"]
            assert actual["minimum"] == exchange["minimum"]
            assert actual["maximum"] == exchange["maximum"]
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
    assert resolved.activities == before


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source"]["location"]
)
@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_foam_review_does_not_admit_other_releases(tmp_path, row, version):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_foam_warnings_keep_independent_producer_witnesses_in_portable_metadata():
    ledger = json.loads(
        (
            Path(__file__).parents[1] / "premise/data/clic/approved_rule_scopes.json"
        ).read_text()
    )
    fingerprints = [
        s["fingerprint"]
        for d in ledger["operations"]
        if d["review_path"] == "export/uvek_pilot_nitrate_foam_review.json"
        for s in d["scopes"]
    ]
    records = compatibility_approval_metadata(fingerprints)
    assert len(records) == 1
    assert all(r["inspection"]["source_producer_entries"] for r in records)
