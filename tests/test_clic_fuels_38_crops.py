"""CLIC admits only reviewed crop purpose, fresh-kg quantities and country proxies."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_crops_review.json").read_text())
ROWS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", ROWS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_crop_fresh_and_cached_keeps_physical_mass_and_explicit_warnings(
    tmp_path, rid, amount, uncertainty
):
    from brightpath.background import PackageCatalogProvider

    row = ROWS[rid]
    resolved = graph(row["original_rule"]["source"], version="3.8")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=amount,
        formula="fresh_seed_dose",
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
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == amount and edge["unit"] == "kilogram"
        assert edge["formula"] == "fresh_seed_dose"
        assert edge["loc"] == incoming["loc"] and edge["scale"] == 0.3
        assert edge["uncertainty type"] == uncertainty
        if uncertainty == 2:
            assert edge["negative"] is (amount < 0)
        if row["resource_action"] == "unchanged_rule":
            assert len(result.activities) == 1
            assert edge["name"] == "Sugar beet seed IP, at regional storehouse"
        else:
            ds = next(a for a in result.activities if a["code"] == edge["input"][1])
            entries = [e for e in ds["exchanges"] if e["type"] != "production"]
            assert len(entries) == 10
            assert len([e for e in entries if e["unit"] == "kilogram"]) == 6
            for actual, retained in zip(entries, row["retained_rows"], strict=True):
                for field, value in retained["statistics"].items():
                    assert actual[field] == value
                assert all(actual[k] == v for k, v in retained["target"].items())
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


@pytest.mark.parametrize("rid", ROWS)
def test_each_crop_needs_its_separate_review(tmp_path, monkeypatch, rid):
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals

    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        row for row in ledger["operations"] if row["rule_id"] != rid
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(ROWS[rid]["original_rule"]["source"], version="3.8"),
            version="2026",
            family="uvek",
        )
