"""Two exact source profiles admit only their own literal final branch omissions."""

import copy
import json
import math
from pathlib import Path

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory import clic_rule_approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_partial_paperboard_review.json").read_text()
)
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("profile", ROW["profiles"], ids=lambda p: p["label"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_partial_source_scope_keeps_losses_explicit_on_cold_and_warm_runs(
    tmp_path, profile, amount, uncertainty
):
    resolved = graph(ROW["source"], version=profile["source_profile"]["version"])
    x = resolved.activities[0]["exchanges"][1]
    x.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount) or 1) if uncertainty == 2 else amount,
            "scale": 0.2,
        },
    )
    if uncertainty == 2:
        x["negative"] = amount < 0
    else:
        x.update(minimum=amount - 1, maximum=amount + 1)
    before = copy.deepcopy(resolved.activities)
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = h.harmonize(resolved, family="uvek", version="2026")
        assert (
            result.manifest["cohorts"][0]["cache_hit"] is cached
            and len(result.activities) == 2
        )
        actual = result.activities[0]["exchanges"][1]
        assert (
            actual["amount"] == -amount
            and actual["formula"] == "(dose) * (-1.0)"
            and actual["scale"] == x["scale"]
        )
        if uncertainty == 2:
            assert actual["loc"] == x["loc"] and actual["negative"] is (
                not x["negative"]
            )
        else:
            assert (
                actual["loc"] == -x["loc"]
                and actual["minimum"] == -x["maximum"]
                and actual["maximum"] == -x["minimum"]
            )
        helper = result.activities[1]
        assert helper["name"].startswith("PARTIAL SCREENING:")
        assert (
            helper["brightpath_conversion"]["recipe_id"]
            == profile["generated_recipe_id"]
        )
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert all(
            all(a[k] == v for k, v in e.items())
            for a, e in zip(inputs, profile["inputs"], strict=True)
        )
        losses = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        assert (
            len(losses) == 11
            and [
                d["details"]["omission"]
                for d in sorted(
                    losses, key=lambda item: item["details"]["omission_index"]
                )
            ]
            == profile["source_input_omissions"]
        )
        assert all(
            d["details"]["source_profile"] == profile["source_profile"] for d in losses
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == ROW["rule_id"]
        )
        inspection = warning["details"]["inspection"]
        assert inspection["source_profile"] == profile["source_profile"]
        assert set(inspection["approved_recipe_source_omissions"]) == {
            profile["generated_recipe_id"]
        }
        assert warning["message"] == ROW["limitations"]
        (coverage,) = result.manifest["partial_mappings"]
        assert coverage["source_profile"] == profile["source_profile"]
        assert coverage["recipe_id"] == profile["generated_recipe_id"]
        assert coverage["omitted_branch_count"] == 11
        assert coverage["represented_treatment_mass_fraction"] == 0.125163193694194
        assert (
            coverage["final_unsupported_mass_fraction"]
            == profile["final_unsupported_mass_fraction"]
        )
        assert not result.manifest.get("approved_clic_omissions")
    assert resolved.activities == before


@pytest.mark.parametrize("version", ["3.6", "3.11"])
def test_other_releases_remain_refused(tmp_path, version):
    resolved = graph(ROW["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


@pytest.mark.parametrize("profile", ROW["profiles"], ids=lambda p: p["label"])
def test_another_profiles_inspection_cannot_authorize_this_loss(
    tmp_path, monkeypatch, profile
):
    ledger = json.loads(
        (
            Path(__file__).parents[1] / "premise/data/clic/approved_rule_scopes.json"
        ).read_text()
    )
    (operation,) = [
        o
        for o in ledger["operations"]
        if o["rule_id"] == ROW["rule_id"]
        and o["inspection"]["source_profile"] == profile["source_profile"]
    ]
    operation["inspection"]["source_profile"] = {
        **profile["source_profile"],
        "version": "3.12" if profile["source_profile"]["version"] == "3.9" else "3.9",
    }
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(ROW["source"], version=profile["source_profile"]["version"])
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_unused_partial_approvals_do_not_label_an_unrelated_applied_mapping(tmp_path):
    wafer = json.loads(
        (DATA_DIR / "export/uvek_deferred_wafer_review.json").read_text()
    )["records"][0]
    resolved = graph(wafer["source"], version="3.12")
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, family="uvek", version="2026")
    assert "partial_mappings" not in result.manifest
