"""Recycling material supplies retain signs, warnings and withdrawal on cache replay."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_trucks_materials_312_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_complete_native_supplies_keep_signed_quantities_and_cache_warnings(
    tmp_path, amount
):
    resolved = graph(*(row["source"] for row in ROWS), version="3.12")
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="dose",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 14
        for edge, row in zip(result.activities[0]["exchanges"][1:], ROWS, strict=True):
            target = row["target"]
            if "recipe_ref" in target:
                target = next(
                    a
                    for a in result.activities[1:]
                    if a["brightpath_conversion"]["recipe_id"] == target["recipe_ref"]
                )
            assert all(
                edge[k] == target[k]
                for k in ("name", "reference product", "location", "unit")
            )
            assert (
                edge["amount"] == edge["loc"] == amount
                and edge["formula"] == "dose"
                and edge["scale"] == 0.2
            )
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for row in ROWS for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_material_permission_refuses_even_a_previously_cached_import(
    tmp_path, monkeypatch, cached
):
    resolved = graph(ROWS[0]["source"], version="3.12")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        op for op in ledger["operations"] if op["review_path"] != PATH
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=ROWS[0]["rule_id"]):
        harmonizer.harmonize(resolved, version="2026", family="uvek")


def test_previous_36_salt_still_works_without_new_312_permissions(
    tmp_path, monkeypatch
):
    row = next(r for r in ROWS if r["rule_id"] == "bp-0cdecef35c6741e6")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        op for op in ledger["operations"] if op["review_path"] != PATH
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    resolved = graph(row["source"], version="3.6")
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert (
            result.manifest["cohorts"][0]["cache_hit"] is cached
            and len(result.activities) == 1
        )
        assert (
            result.activities[0]["exchanges"][1]["name"]
            == "Sodium chloride, powder, at plant"
        )
