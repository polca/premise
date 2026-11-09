"""Admit only the inspected Swiss concrete market with full treatment closure."""

import copy
import json
import math

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import compatibility_approval_metadata
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_pilot_nitrate_concrete_review.json").read_text()
)
(ROW,) = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_concrete_market_keeps_signed_treatment_and_audit_on_cold_and_warm_cache(
    tmp_path, amount, uncertainty
):
    resolved = graph(ROW["source"], version="3.9")
    original = resolved.activities[0]["exchanges"][1]
    original.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount) or 1) if uncertainty == 2 else amount,
            "scale": 0.2,
        },
    )
    if uncertainty == 2:
        original["negative"] = amount < 0
    else:
        original.update(minimum=amount - 1, maximum=amount + 1)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 4
        actual = result.activities[0]["exchanges"][1]
        assert actual["amount"] == -amount
        assert actual["formula"] == "(dose) * (-1.0)"
        assert actual["scale"] == original["scale"]
        if uncertainty == 2:
            assert actual["loc"] == original["loc"]
            assert actual["negative"] is not original["negative"]
        else:
            assert actual["loc"] == -original["loc"]
            assert actual["minimum"] == -original["maximum"]
            assert actual["maximum"] == -original["minimum"]
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == ROW["rule_id"]
        )
        assert warning["message"] == ROW["limitations"]
        assert (
            warning["details"]["review_path"]
            == "export/uvek_pilot_nitrate_concrete_review.json"
        )
        assert warning["details"]["inspection"] == ROW
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("version", ROW["uninspected_profile_guards"])
def test_concrete_review_does_not_admit_other_releases(tmp_path, version):
    resolved = graph(ROW["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_concrete_review_and_full_recipe_closure_are_portable():
    (metadata,) = compatibility_approval_metadata([ROW["fingerprint"]])
    assert (
        metadata["inspection"]["recipe_closure_sha256"] == ROW["recipe_closure_sha256"]
    )
    assert metadata["limitations"] == [ROW["limitations"]]
