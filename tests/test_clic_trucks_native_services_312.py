"""Current service permissions retain warnings, cache validity and old routes."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

PATH = "export/uvek_trucks_native_services_312_review.json"
REVIEW = json.loads((DATA_DIR / PATH).read_text())
ROWS = REVIEW["additional_approvals"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_current_lorry_services_keep_signs_units_and_cache_warnings(tmp_path, amount):
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
            and len(result.activities) == 1
        )
        for e, r in zip(result.activities[0]["exchanges"][1:], ROWS, strict=True):
            assert (
                e["amount"] == e["loc"] == amount * r["conversion_factor"]
                and e["scale"] == 0.2
            )
            assert e["unit"] == r["source"]["unit"]
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(s in manifest for r in ROWS for s in r["limitations"])
    assert resolved.activities == original


@pytest.mark.parametrize("cached", [False, True])
def test_withdrawn_current_service_permission_refuses_on_cache_replay(
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


def test_current_permissions_do_not_change_existing_38_service_approval(
    tmp_path, monkeypatch
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [o for o in ledger["operations"] if o["review_path"] != PATH]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(graph(ROWS[0]["source"], version="3.8"), version="2026", family="uvek")
    assert (
        len(result.activities) == 1
        and result.activities[0]["exchanges"][1]["amount"] == 2
    )
