"""Hazardous/paint scopes preserve caller data, reference signs, recovered handovers and warnings."""

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
    (DATA_DIR / "export/uvek_pilot_hazardous_paint_review.json").read_text()
)


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source"]["location"]
)
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_hazardous_paint_market_keeps_fates_and_warnings_on_cold_and_warm_cache(
    tmp_path, row, amount, uncertainty
):
    resolved = graph(row["source"], version=row["source_profile"]["version"])
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
        expected_omissions = row["profiles"][0].get("source_input_omissions", [])
        losses = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        assert len(losses) == len(expected_omissions)
        assert (
            helper["brightpath_conversion"].get("source_input_omissions", [])
            == expected_omissions
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source"]["location"]
)
@pytest.mark.parametrize("version", ["3.7", "3.12"])
def test_hazardous_paint_review_does_not_admit_other_releases(tmp_path, row, version):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_hazardous_paint_portable_metadata_keeps_exact_source_omission_authority():
    ledger = json.loads(
        (
            Path(__file__).parents[1] / "premise/data/clic/approved_rule_scopes.json"
        ).read_text()
    )
    fingerprints = [
        s["fingerprint"]
        for d in ledger["operations"]
        if d["review_path"] == "export/uvek_pilot_hazardous_paint_review.json"
        for s in d["scopes"]
    ]
    records = compatibility_approval_metadata(fingerprints)
    assert len(records) == 6
    assert sum(r["inspection"]["omitted_source_link_count"] for r in records) == 5
    assert all("unquantified" in r["inspection"]["limitations"] for r in records)


@pytest.mark.parametrize(
    "row",
    [r for r in REVIEW["records"] if r["omitted_source_link_count"]],
    ids=lambda r: r["source"]["location"],
)
def test_paint_omission_quantity_drift_invalidates_its_exact_scope(monkeypatch, row):
    from brightpath.migrations import resources
    from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
    from premise.inventory.clic_target import resolve_inventory_target

    resource = copy.deepcopy(resources.load_uvek_technosphere_resource())
    (recipe_id,) = row["approved_recipe_source_omissions"]
    resource["recipes"][recipe_id]["source_input_omissions"][0]["source_exchange"][
        "amount"
    ] *= 2
    monkeypatch.setattr(resources, "load_uvek_technosphere_resource", lambda: resource)
    with pytest.raises(ClicInventoryError, match="no longer matches"):
        approved_compatibility_rule_scopes(
            resolve_inventory_target("2026", family="uvek")
        )
