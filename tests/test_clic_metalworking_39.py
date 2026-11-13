"""Native metalworking keeps processing demand, signs and caller-owned bulk metal."""

import copy
import math
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.scaling import scale_exchange
from brightpath.migrations.resources import load_uvek_technosphere_resource
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads((DATA_DIR / "export/uvek_metalworking_39_review.json").read_text())
ACTIVE_ROWS = REVIEW["records"]
CASES = [r["rule_id"] for r in ACTIVE_ROWS]


@pytest.mark.parametrize("rid", CASES)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_whole_services_keep_signed_functional_units_and_caller_fields(
    tmp_path, rid, amount, uncertainty
):
    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rid
    )
    resolved = graph(rule["source"], version="3.9")
    source = resolved.activities[0]["exchanges"][1]
    source.update(
        amount=amount,
        formula="service_demand",
        **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
    )
    if uncertainty == 2:
        source.update(loc=math.log(abs(amount)), negative=amount < 0)
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
    row = next(r for r in ACTIVE_ROWS if r["rule_id"] == rid)
    expected = copy.deepcopy(source)
    scale_exchange(expected, row["conversion_factor"])
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert len(result.activities) == 1
        assert len(result.activities[0]["exchanges"]) == 3
        assert all(
            edge[key] == value for key, value in row["compiled_rule"]["target"].items()
        )
        for key in (
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "formula",
        ):
            if key in expected:
                assert edge[key] == expected[key]
        assert edge["unit"] == rule["source"]["unit"]
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        manifest = json.dumps(result.manifest)
        assert all(limit in manifest for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("rid", CASES)
def test_older_approved_profile_does_not_authorize_unapproved_39(
    tmp_path, monkeypatch, rid
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


@pytest.mark.parametrize("row", ACTIVE_ROWS, ids=lambda r: r["rule_id"])
def test_owned_bulk_metal_stays_separate_through_preparation_and_cache(tmp_path, row):
    from test_clic_archive import catalog, native, purchase

    bulk = purchase(
        "Caller-owned semi-finished stock",
        amount=0.9,
        input=["original", "old-2"],
        **{"uncertainty type": 3, "loc": 0.9, "scale": 0.05, "formula": "bulk_kg"},
    )
    root = native(
        1,
        1,
        "Finished component",
        exchanges=[
            {**row["original_rule"]["source"], "type": "technosphere", "amount": 1},
            bulk,
        ],
    )
    stock = native(2, 2, "Caller-owned semi-finished stock")
    for record in (root, stock):
        record.update(background_version="3.9", biosphere_version="3.9")
    resolved = catalog(root, stock).resolve(["dataset-1"])
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 2
        parent = next(a for a in result.activities if a["name"] == "Finished component")
        assert len(parent["exchanges"]) == 3
        actual = parent["exchanges"][2]
        assert all(actual[k] == v for k, v in bulk.items() if k != "input")
        assert actual["input"] == before[0]["exchanges"][2]["input"]
        supplier = next(
            a
            for a in result.activities
            if a["name"] == "Caller-owned semi-finished stock"
        )
        assert len(supplier["exchanges"]) == 1
    assert resolved.activities == before
