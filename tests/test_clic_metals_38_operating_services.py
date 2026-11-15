"""Reviewed operating-service bases propagate through normal CLIC harmonization."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.scaling import scale_exchange
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_metals_38_operating_services_review.json").read_text()
)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_five_services_preserve_scoped_units_scaling_and_warning_provenance(
    tmp_path, amount, uncertainty
):
    resolved = graph(
        *(r["original_rule"]["source"] for r in REVIEW["records"]), version="3.8"
    )
    for exchange in resolved.activities[0]["exchanges"][1:]:
        exchange.update(
            amount=amount,
            formula="service_quantity",
            **{
                "uncertainty type": uncertainty,
                "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
                "scale": 0.2,
            },
        )
        if uncertainty == 2:
            exchange["negative"] = amount < 0
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 1
        for edge, original, row in zip(
            result.activities[0]["exchanges"][1:],
            before[0]["exchanges"][1:],
            REVIEW["records"],
            strict=True,
        ):
            assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())
            expected = copy.deepcopy(original)
            scale_exchange(expected, row["conversion_factor"])
            for key in (
                "amount",
                "formula",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
            ):
                if key in expected:
                    assert edge[key] == expected[key]
            assert all(
                limit in json.dumps(result.manifest, ensure_ascii=False)
                for limit in row["limitations"]
            )
    assert resolved.activities == before


def test_machine_requires_its_exact_hour_to_MJ_review(tmp_path, monkeypatch):
    row = next(r for r in REVIEW["records"] if r["kind"] == "diesel_machine_fuel_proxy")
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == row["rule_id"]:
            operation["scopes"] = [
                s for s in operation["scopes"] if s["source"]["version"] != "3.8"
            ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(row["original_rule"]["source"], version="3.8")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before
