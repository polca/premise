"""Native metals utilities require their own source-3.8 review scopes."""

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
    (DATA_DIR / "export/uvek_metals_38_native_services_review.json").read_text()
)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_exact_38_services_keep_signed_quantities_and_review_provenance(
    tmp_path, row, amount
):
    resolved = graph(row["rule"]["source"], version="3.8")
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        formula="service_quantity",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cache_hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is cache_hit
        assert len(result.activities) == 1
        output = result.activities[0]["exchanges"][1]
        assert all(output[k] == v for k, v in row["rule"]["target"].items())
        for key in ("amount", "formula", "uncertainty type", "loc", "scale"):
            assert output[key] == exchange[key]
        assert all(limit in json.dumps(result.manifest) for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_removing_exact_38_review_restores_refusal(tmp_path, monkeypatch, row):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == row["rule_id"]:
            operation["scopes"] = [
                s for s in operation["scopes"] if s["source"]["version"] != "3.8"
            ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(row["rule"]["source"], version="3.8")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before
