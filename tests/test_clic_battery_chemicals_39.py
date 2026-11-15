"""Six chemical supplies retain signed statistics, native production routes and final delivery."""

import copy
import math
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.scaling import scale_exchange
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_battery_chemicals_39_review.json").read_text()
)
ACTIVE_ROWS = REVIEW["records"]
CASES = [r["rule_id"] for r in ACTIVE_ROWS]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_whole_services_keep_signed_functional_units_and_caller_fields(
    tmp_path, amount, uncertainty
):
    resolved = graph(
        *(row["original_rule"]["source"] for row in ACTIVE_ROWS), version="3.9"
    )
    for source in resolved.activities[0]["exchanges"][1:]:
        source.update(
            amount=amount,
            formula="chemical_demand",
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
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 6
        nodes = {
            a.get("brightpath_conversion", {}).get("recipe_id"): a
            for a in result.activities[1:]
        }
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        for i, row in enumerate(ACTIVE_ROWS, 1):
            edge = result.activities[0]["exchanges"][i]
            expected = copy.deepcopy(before[0]["exchanges"][i])
            scale_exchange(expected, 1)
            for k in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "formula",
            ):
                if k in expected:
                    assert edge[k] == expected[k]
            if row["recipe_id"]:
                actual = [
                    e
                    for e in nodes[row["recipe_id"]]["exchanges"]
                    if e["type"] != "production"
                ]
                recipe = REVIEW["recipes"][row["recipe_id"]]
                assert len(actual) == row["boundary_checks"]["source_market_entries"]
                for got, want in zip(actual, recipe["inputs"], strict=True):
                    for k in (
                        "name",
                        "reference product",
                        "location",
                        "unit",
                        "amount",
                        "uncertainty type",
                        "loc",
                        "scale",
                        "scale without pedigree",
                        "negative",
                        "pedigree",
                    ):
                        if k in want:
                            assert got[k] == want[k]
            else:
                assert edge["name"] == "Propylene oxide, liquid, at plant"
            warning = next(
                d
                for d in result.diagnostics
                if d["code"] == "clic.compatibility_proxy_limitations"
                and d["details"]["rule_id"] == row["rule_id"]
            )
            assert warning["details"]["limitations"] == row["limitations"]
            assert all(limit in warning["message"] for limit in row["limitations"])
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
    resolved = graph(
        *(row["original_rule"]["source"] for row in ACTIVE_ROWS), version="3.9"
    )
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=rid):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
