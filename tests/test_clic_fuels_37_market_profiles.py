"""Exact 3.7 stock approvals preserve signed quantities and proxy warnings."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

RECORDS = {
    row["rule_id"]: {**row, "_review_path": f"export/{name}"}
    for name in ("uvek_fuels_37_market_profiles_review.json",)
    for row in json.loads((DATA_DIR / "export" / name).read_text())["records"]
}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_native_stock_fresh_and_cached_keeps_dose_and_warnings(
    tmp_path, rid, amount, uncertainty
):
    from brightpath.background import PackageCatalogProvider

    row = RECORDS[rid]
    resolved = graph(row["compiled_rule"]["source"], version="3.7")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=amount,
        formula="signed_quantity",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
            "scale": 0.3,
            "negative": amount < 0,
        },
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) >= 2
        edge = result.activities[0]["exchanges"][1]
        factor = row["conversion_factor"]
        assert edge["amount"] == amount * factor
        assert edge["formula"] == (
            "signed_quantity" if factor == 1 else "(signed_quantity) * (-1.0)"
        )
        assert edge["uncertainty type"] == uncertainty
        assert edge["loc"] == (incoming["loc"] if uncertainty == 2 else amount * factor)
        assert edge["scale"] == 0.3
        if uncertainty == 2:
            assert edge["negative"] is (amount * factor < 0)
        recipe_id = row["new_profile"]["target"]["recipe_ref"]
        helper = next(
            a
            for a in result.activities
            if a.get("brightpath_conversion", {}).get("recipe_id") == recipe_id
        )
        assert edge["input"] == (helper["database"], helper["code"])
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        recipe = load_uvek_technosphere_resource()["recipes"][recipe_id]
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert [e["amount"] for e in actual] == [e["amount"] for e in recipe["inputs"]]
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["message"] == " ".join(row["limitations"])
            for d in result.diagnostics
        )
        assert not any(
            d["code"] == "migration.recipe_source_input_omission"
            for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("rid", RECORDS)
def test_exact_profile_does_not_pass_without_its_new_review(tmp_path, monkeypatch, rid):
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals

    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        row
        for row in ledger["operations"]
        if not (
            row.get("review_path") == RECORDS[rid]["_review_path"]
            and row["rule_id"] == rid
        )
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(RECORDS[rid]["compiled_rule"]["source"], version="3.7"),
            version="2026",
            family="uvek",
        )


@pytest.mark.parametrize(
    "rid,version",
    [("bp-22c3a52062d0cb16", "3.9"), ("bp-e172491d6da0b115", "3.9")]
    + [("bp-9caeb1b3bd66b612", version) for version in ("3.12", "3.6", "3.9", "3.8")],
)
def test_previously_approved_profiles_still_harmonize(tmp_path, rid, version):
    from brightpath.background import PackageCatalogProvider

    row = RECORDS[rid]
    resolved = graph(row["original_rule"]["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    edge = result.activities[0]["exchanges"][1]
    selected = next(
        p
        for p in row["original_rule"]["reference_flow_sign_review"]["profiles"]
        if p["source_profile"]["version"] == version
    )
    target = selected.get("target", row["original_rule"]["target"])
    if "recipe_ref" in target:
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        recipe = load_uvek_technosphere_resource()["recipes"][target["recipe_ref"]]
        fields = ("name", "reference product", "location", "unit")
        helper = next(
            a
            for a in result.activities
            if a.get("brightpath_conversion", {}).get("recipe_id")
            == target["recipe_ref"]
        )
        assert edge["input"] == (helper["database"], helper["code"])
        assert all(helper[k] == recipe[k] for k in fields if k != "name")
        assert helper["name"].startswith(
            recipe["name"] + " [BrightPath UVEK conversion "
        )
        assert all(edge[k] == helper[k] for k in fields)
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert [e["amount"] for e in actual] == [e["amount"] for e in recipe["inputs"]]
    else:
        assert all(edge[k] == v for k, v in target.items())
    assert resolved.activities == before
