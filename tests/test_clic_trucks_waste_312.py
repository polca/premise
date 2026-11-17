"""Current waste helpers retain canonical emissions and exact cache admission."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_trucks_waste_312_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
MODELS = json.loads((DATA_DIR / REVIEW["model_review_path"]).read_text())
ROWS = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_current_waste_ports_keep_signs_canonical_emissions_and_cache_warnings(
    tmp_path, amount
):
    resolved = graph(*(r["source"] for r in ROWS), version="3.12")
    for e in resolved.activities[0]["exchanges"][1:]:
        e.update(
            amount=amount,
            formula="dose",
            **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
        )
    original = copy.deepcopy(resolved.activities)
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = h.harmonize(resolved, version="2026", family="uvek")
        assert (
            result.manifest["cohorts"][0]["cache_hit"] is cached
            and len(result.activities) == 37
        )
        for e in result.activities[0]["exchanges"][1:]:
            assert e["amount"] == e["loc"] == -amount and e["scale"] == 0.2
            assert e["formula"] == "(dose) * (-1.0)" and e["unit"] == "kilogram"
        for a in result.activities[1:]:
            rid = a["brightpath_conversion"]["recipe_id"]
            expected = [
                e
                for e in MODELS["recipes"][rid]["inputs"]
                if e.get("type") == "biosphere"
            ]
            actual = [e for e in a["exchanges"] if e["type"] == "biosphere"]
            assert len(actual) == len(expected)
            for e, raw in zip(actual, expected, strict=True):
                assert all(
                    tuple(e[k]) == tuple(v) if k == "categories" else e[k] == v
                    for k, v in raw.items()
                )
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(s in manifest for s in ROWS[0]["limitations"])
    assert resolved.activities == original


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_current_waste_permission_refuses_fresh_and_cached_import(
    tmp_path, monkeypatch, cached
):
    resolved = graph(ROWS[0]["source"], version="3.12")
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        h.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [o for o in ledger["operations"] if o["review_path"] != PATH]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=ROWS[0]["rule_id"]):
        h.harmonize(resolved, version="2026", family="uvek")


def test_current_waste_permissions_do_not_change_existing_38_pe_import(
    tmp_path, monkeypatch
):
    row = next(r for r in ROWS if "polyethylene" in r["source"]["name"])
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [o for o in ledger["operations"] if o["review_path"] != PATH]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(graph(row["source"], version="3.8"), version="2026", family="uvek")
    assert result.activities[0]["exchanges"][1]["amount"] == 2
