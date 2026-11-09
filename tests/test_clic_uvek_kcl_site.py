"""The UVEK pilot preserves industrial KCl and complete PVF routes through caches."""

import copy
import json
import math

import pytest

from brightpath import DATA_DIR, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.migrations.resources import load_uvek_technosphere_resource
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
from premise.inventory.clic_target import resolve_inventory_target
from test_clic_rule_approvals import graph

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_kcl_site_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_kcl_cold_and_warm_cache_preserve_signed_kg_and_treatment(
    tmp_path, amount, uncertainty
):
    resolved = graph(ROW["source"], version="3.9")
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
        assert len(result.activities) == 4
        actual = result.activities[0]["exchanges"][1]
        for field in (
            "amount",
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
        ):
            if field in exchange:
                assert actual[field] == exchange[field]
        helpers = result.activities[1:]
        assert all(
            next(x for x in a["exchanges"] if x["type"] == "production")["amount"] == 1
            for a in helpers
        )
        assert (
            sum(x["type"] == "biosphere" for a in helpers for x in a["exchanges"]) == 5
        )
        assert (
            sum(
                x["type"] == "technosphere" and x["unit"] == "ton kilometer"
                for a in helpers
                for x in a["exchanges"]
            )
            == 15
        )
        (treatment,) = [
            x
            for a in helpers
            for x in a["exchanges"]
            if x["name"]
            == "Disposal, polyvinylfluoride, 0.2% water, to municipal incineration"
        ]
        assert treatment["amount"] == 1
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == ROW["rule_id"]
        )
        assert "kgK2O" in warning["message"] and "unquantified" in warning["message"]
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_kcl_other_releases_remain_unapproved(tmp_path, version):
    resolved = graph(ROW["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "rule_id", [None, "bp-155787c4bf1ee018", "bp-fc87385b1c0a40f4"]
)
def test_kcl_approval_does_not_admit_old_nutrient_or_uninspected_operating_profiles(
    rule_id,
):
    resource = load_uvek_technosphere_resource()
    rule = (
        ROW["previous_rule"]
        if rule_id is None
        else next(r for r in resource["replace"] if r["id"] == rule_id)
    )
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        # The two 3.9 operating services now have a separate direct review.
        # Their uninspected 3.12 profiles still cannot inherit the KCl approval.
        TechnosphereProfile(
            "ecoinvent", "3.9" if rule_id is None else "3.12", "cutoff"
        ),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
