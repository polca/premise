"""Required MnO2 reconstruction keeps its exact permissions and cache warnings."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_battery_manganese_dioxide_39_review.json").read_text()
)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_complete_manganese_supply_preserves_signed_demand_and_review_on_cache_replay(
    tmp_path, amount
):
    row = REVIEW["records"][0]
    resolved = graph(row["compiled_rule"]["source"], version="3.9")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="manganese_dose",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == amount
        assert edge["formula"] == "manganese_dose" and edge["scale"] == 0.2
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        assert (
            len(helpers["battery-39-manganese-dioxide-production"]["exchanges"]) == 23
        )
        assert (
            len(helpers["battery-39-manganese-dioxide-recovered-stock"]["exchanges"])
            == 1
        )
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("cached", [False, True])
def test_removed_manganese_approval_refuses_even_a_previously_cached_import(
    tmp_path, monkeypatch, cached
):
    row = REVIEW["records"][0]
    resolved = graph(row["compiled_rule"]["source"], version="3.9")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    if cached:
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        operation
        for operation in ledger["operations"]
        if operation["review_path"]
        != "export/uvek_battery_manganese_dioxide_39_review.json"
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
