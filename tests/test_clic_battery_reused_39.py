"""Exact reviewed 3.9 cut-off services retain their physical interfaces."""

import copy

import pytest
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.resources import load_uvek_technosphere_resource
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

CASES = [
    ("bp-cd8dc6a127ecae94", "whole-freight-market-composition-93435fe40fb2280e0903"),
    ("bp-46a8f88f9ecae2d1", "chemical-heat-water-8a841bd2e16662e9e3a9"),
]


@pytest.mark.parametrize("rid,recipe", CASES)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_exact_helpers_and_caller_survive_fresh_and_cached_preparation(
    tmp_path, rid, recipe, amount
):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == rid)
    resolved = graph(rule["source"], version="3.9")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="dose",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = copy.deepcopy(resolved.activities)
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = h.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        caller = result.activities[0]
        edge = caller["exchanges"][1]
        assert edge["amount"] == edge["loc"] == amount
        assert edge["unit"] == rule["source"]["unit"]
        assert edge["formula"] == "dose" and edge["scale"] == 0.2
        assert caller["exchanges"][-1]["amount"] == 0.5
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        helper = helpers[recipe]
        rows = [e for e in helper["exchanges"] if e["type"] != "production"]
        expected = resource["recipes"][recipe]["inputs"]
        assert [e["amount"] for e in rows] == [e["amount"] for e in expected]
        assert len(rows) == len(expected)
        if rid == "bp-cd8dc6a127ecae94":
            assert len(helpers) == 2
            stock = helpers["cutoff-stock-f7f6921e17b3a8acac22"]
            assert [e["type"] for e in stock["exchanges"]] == ["production"]
            assert stock["exchanges"][0]["amount"] == 1
            assert [e["amount"] for e in rows[1:]] == [0.112, 0.0199, 0.1932, 0.2115]
        else:
            assert len(helpers) == 1
            assert rows[0]["name"] == "Heat, unspecific, in chemical plant"
            assert rows[0]["amount"] == 1 and rows[0]["unit"] == "megajoule"
            assert len([e for e in rows if e["name"] == "Tap water, at user"]) == 7
            water = [e for e in rows if e["type"] == "biosphere"]
            assert [(e["name"], tuple(e["categories"]), e["unit"]) for e in water] == [
                ("Water", ("air",), "cubic meter"),
                ("Water", ("water",), "cubic meter"),
            ]
    assert resolved.activities == before


@pytest.mark.parametrize("rid,recipe", CASES)
def test_removing_exact_scope_refuses_without_mutating_source(
    tmp_path, monkeypatch, rid, recipe
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == rid:
            operation["scopes"] = [
                s for s in operation["scopes"] if s["source"]["version"] != "3.9"
            ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rid
    )
    resolved = graph(rule["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=rid):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
