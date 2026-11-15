"""Wet-process PE separator source manufacturing and signed delivery retain strict scoped approvals."""

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

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_separator_wet_pe_39_review.json").read_text()
)
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
        assert len(result.activities) == 12
        assert len(result.activities[0]["exchanges"]) == 3
        nodes = {
            a.get("brightpath_conversion", {}).get("recipe_id"): a
            for a in result.activities[1:]
        }
        market = nodes[row["recipe_id"]]
        entries = [e for e in market["exchanges"] if e["type"] != "production"]
        assert len(entries) == 4
        assert [e["amount"] for e in entries[:2]] == REVIEW[
            "source_market_producer_shares"
        ]
        assert len([e for e in entries if e["unit"] == "ton kilometer"]) == 2
        for proof in REVIEW["source_production_reconstruction"]:
            producer = nodes[proof["recipe_id"]]
            original = REVIEW["recipes"][proof["recipe_id"]]["inputs"]
            converted = [e for e in producer["exchanges"] if e["type"] != "production"]
            assert (
                len(converted) == len(original) == proof["retained_nonproduction_count"]
            )
            for actual, expected_entry in zip(converted, original, strict=True):
                for field in (
                    "amount",
                    "uncertainty type",
                    "loc",
                    "scale",
                    "negative",
                    "minimum",
                    "maximum",
                    "pedigree",
                    "scale without pedigree",
                ):
                    if field in expected_entry:
                        assert actual[field] == expected_entry[field]
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
        omissions = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        assert len(omissions) == 4
        hdpe = nodes["pilot-nitrate-hdpe-dfcfe7b3600a00f94f69"]
        assert (
            hdpe["brightpath_conversion"]["source_input_omissions"]
            == REVIEW["recipe_dependencies"]["pilot-nitrate-hdpe-dfcfe7b3600a00f94f69"][
                "source_input_omissions"
            ]
        )
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
