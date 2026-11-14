"""Direct disposal services flip waste references once without helpers or lost caller fields."""

import copy
import json
import math

import pytest
from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_direct_waste_services_39_review.json").read_text()
)
ROWS = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_direct_services_keep_uncertainty_and_caller_emissions_through_cache(
    tmp_path, amount, uncertainty
):
    resolved = graph(*(r["rule"]["source"] for r in ROWS), version="3.9")
    for i, e in enumerate(resolved.activities[0]["exchanges"][1:]):
        e.update(
            amount=amount,
            formula=f"waste_demand_{i}",
            **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
        )
        if uncertainty == 2:
            e.update(loc=math.log(abs(amount)), negative=amount < 0)
        else:
            e.update(minimum=amount - 0.3, maximum=amount + 0.4)
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
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 1
        for i, row in enumerate(ROWS, 1):
            actual = result.activities[0]["exchanges"][i]
            expected = copy.deepcopy(before[0]["exchanges"][i])
            scale_exchange(expected, -1)
            assert actual["amount"] == -amount
            for key in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "formula",
                "minimum",
                "maximum",
            ):
                if key in expected:
                    assert actual[key] == expected[key]
            assert all(actual[k] == v for k, v in row["rule"]["target"].items())
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for row in ROWS for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("rid", [r["rule_id"] for r in ROWS])
def test_one_missing_approval_refuses_all_services_atomically(
    tmp_path, monkeypatch, rid
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == rid:
            operation["scopes"] = [
                s for s in operation["scopes"] if s["source"]["version"] != "3.9"
            ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(*(r["rule"]["source"] for r in ROWS), version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=rid):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before


def test_earlier_residue_36_scope_keeps_its_exact_supplier_and_sign(tmp_path):
    row = next(r for r in ROWS if r["prior_approvals_unchanged"])
    (prior,) = row["prior_approvals_unchanged"]
    version = prior["scopes"][0]["source"]["version"]
    resolved = graph(row["rule"]["source"], version=version)
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    before = copy.deepcopy(resolved.activities)
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = h.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 1
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == 2.5
        assert all(edge[k] == v for k, v in row["rule"]["target"].items())
    assert resolved.activities == before
