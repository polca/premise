"""Heat market scopes preserve product credits, supplier shares and limitations in caches."""

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
    (DATA_DIR / "export/uvek_pilot_nitrate_market_review.json").read_text()
)
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("profile", ROW["profiles"], ids=lambda p: p["label"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_nitrate_keeps_credits_quantities_and_limitations_in_cold_and_warm_cache(
    tmp_path, profile, amount, uncertainty
):
    resolved = graph(ROW["source"], version=profile["source_profile"]["version"])
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
        for field in (
            "amount",
            "formula",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
        ):
            if field in exchange:
                assert actual[field] == exchange[field]
        helper = next(
            a for a in result.activities if a.get("code") == actual["input"][1]
        )
        inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
        assert len(result.activities) == ROW["expected_foreground_records"] and len(
            inputs
        ) == len(profile["inputs"])
        assert all(
            all(x[k] == v for k, v in expected.items() if k != "recipe")
            for x, expected in zip(inputs, profile["inputs"], strict=True)
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == ROW["rule_id"]
        )
        assert warning["message"] == ROW["limitations"]
        assert warning["details"]["inspection"] == ROW
        losses = [
            d["details"]["omission"]
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        expected_losses = [
            o
            for entries in ROW["approved_recipe_source_omissions"].values()
            for o in entries
        ]
        assert sorted(losses, key=lambda x: json.dumps(x, sort_keys=True)) == sorted(
            expected_losses, key=lambda x: json.dumps(x, sort_keys=True)
        )
        for group in ROW["inherited_proxy_limitations"]:
            assert group["limitations"] in warning["message"]

    assert resolved.activities == before


@pytest.mark.parametrize("version", ["3.6", "3.10"])
def test_nitrate_scope_refuses_uninspected_versions(tmp_path, version):
    resolved = graph(ROW["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_exact_nitrate_profile_keep_portable_limitations():
    ledger = json.loads(
        (
            Path(__file__).parents[1] / "premise/data/clic/approved_rule_scopes.json"
        ).read_text()
    )
    fingerprints = [
        s["fingerprint"]
        for d in ledger["operations"]
        if d["review_path"] == "export/uvek_pilot_nitrate_market_review.json"
        for s in d["scopes"]
    ]
    records = compatibility_approval_metadata(fingerprints)
    assert len(records) == 1
    assert all(r["inspection"]["profiles"] == ROW["profiles"] for r in records)
    assert all(
        "unquantified" in r["inspection"]["limitations"]
        and "allocated" in r["inspection"]["limitations"]
        for r in records
    )


def test_nitrate_inherited_omission_drift_invalidates_its_scope(monkeypatch):
    from brightpath.migrations import resources
    from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
    from premise.inventory.clic_target import resolve_inventory_target

    resource = copy.deepcopy(resources.load_uvek_technosphere_resource())
    rid = next(iter(ROW["approved_recipe_source_omissions"]))
    resource["recipes"][rid]["source_input_omissions"][0]["source_exchange"][
        "amount"
    ] *= 2
    monkeypatch.setattr(resources, "load_uvek_technosphere_resource", lambda: resource)
    with pytest.raises(ClicInventoryError, match="no longer matches"):
        approved_compatibility_rule_scopes(
            resolve_inventory_target("2026", family="uvek")
        )
