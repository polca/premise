"""A scoped MDEA screening review survives preparation and cache replay."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads((DATA_DIR / "export/uvek_mdea_312_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_mdea_signed_kg_and_logged_limits_survive_cache(tmp_path, amount):
    resolved = graph(ROW["original_rule"]["source"], version="3.12")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="solvent",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == amount
        assert edge["reference product"] == "methyldiethanolamine"
        assert edge["unit"] == "kilogram" and edge["formula"] == "solvent"
        assert edge["scale"] == 0.2
        assert len(result.activities) == 4
        text = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in text for limit in ROW["limitations"])
    assert resolved.activities == before


def test_removing_the_one_scope_refuses_without_mutation(tmp_path, monkeypatch):
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        o for o in ledger["operations"] if o["rule_id"] != ROW["rule_id"]
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(ROW["original_rule"]["source"], version="3.12")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=ROW["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
