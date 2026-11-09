"""The pilot preserves generic cleanfill final disposal, collection and portable warnings."""

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
    (DATA_DIR / "export/uvek_pilot_nitrate_inert39_review.json").read_text()
)


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source"]["location"]
)
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_inert39_market_keeps_fates_and_warnings_on_cold_and_warm_cache(
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
            assert actual["negative"] is not exchange["negative"]
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
        assert len(result.activities) == 2 + sum("recipe" in x for x in expected)
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
            == 0
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
@pytest.mark.parametrize("version", ["3.6", "3.10"])
def test_inert39_review_does_not_admit_other_releases(tmp_path, row, version):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_inert39_warnings_keep_independent_treatment_witnesses_in_portable_metadata():
    ledger = json.loads(
        (
            Path(__file__).parents[1] / "premise/data/clic/approved_rule_scopes.json"
        ).read_text()
    )
    fingerprints = [
        s["fingerprint"]
        for d in ledger["operations"]
        if d["review_path"] == "export/uvek_pilot_nitrate_inert39_review.json"
        for s in d["scopes"]
    ]
    records = compatibility_approval_metadata(fingerprints)
    assert len(records) == 1
    assert all(r["inspection"]["source_components"] for r in records)


def test_inert39_reissue_preserves_the_old_scientific_approval_and_records_why():
    ledger = json.loads(
        (
            Path(__file__).parents[1] / "premise/data/clic/approved_rule_scopes.json"
        ).read_text()
    )
    decisions = [
        d
        for d in ledger["operations"]
        if d["rule_id"] == REVIEW["records"][0]["rule_id"]
    ]
    assert len(decisions) == 2
    old = next(d for d in decisions if d["scopes"][0]["source"]["version"] == "3.12")
    extension = old["inspection"]["raw_rule_context_extension"]
    assert (
        extension["retained_recipe_sha256"] == REVIEW["retained_312"]["recipe_sha256"]
    )
    assert extension["old_selected_profile_decision_unchanged"]
    assert extension["public_facade_scientific_graph_unchanged"]
    assert extension["only_changed_result_field"] == "/0/exchanges/1/comment"
    assert old["review_path"] == REVIEW["retained_312"]["prior_review_path"]
    assert old["scopes"][0]["fingerprint"] != extension["previous_fingerprint"]
