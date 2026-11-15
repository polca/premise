"""Market delivery, functional bases and warnings survive the CLIC/cache path."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_metals_38_chemical_markets_review.json").read_text()
)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_chemical_market_signs_original_delivery_and_basis_warnings_survive_cache(
    tmp_path, amount, uncertainty
):
    resolved = graph(
        *(row["original_rule"]["source"] for row in REVIEW["records"]), version="3.8"
    )
    owner = resolved.activities[0]["code"]
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=amount,
            formula="chemical_dose",
            **{
                "uncertainty type": uncertainty,
                "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
                "scale": 0.2,
            },
        )
        if uncertainty == 2:
            edge["negative"] = amount < 0
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 5
        by_code = {a["code"]: a for a in result.activities}
        for edge, original, row in zip(
            by_code[owner]["exchanges"][1:],
            before[0]["exchanges"][1:],
            REVIEW["records"],
            strict=True,
        ):
            for key in (
                "amount",
                "formula",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
            ):
                if key in original:
                    assert edge[key] == original[key]
            if row["recipe_id"]:
                helper = by_code[edge["input"][1]]
                (production,) = [
                    e for e in helper["exchanges"] if e["type"] == "production"
                ]
                assert production["amount"] == 1 and helper["unit"] == "kilogram"
                inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
                expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
                assert len(inputs) == len(expected)
                assert all(
                    all(e[k] == v for k, v in ref.items())
                    for e, ref in zip(inputs, expected, strict=True)
                )
                assert (
                    sum(e["unit"] == "ton kilometer" for e in inputs)
                    == row["boundary_checks"]["freight_entries_retained"]
                )
            else:
                assert all(
                    edge[k] == v for k, v in row["compiled_rule"]["target"].items()
                )
            text = json.dumps(result.manifest, ensure_ascii=False)
            assert all(limit in text for limit in row["limitations"])
    assert resolved.activities == before


def test_market_review_does_not_grant_its_nested_freight_global_permission(tmp_path):
    freight = next(
        r
        for r in REVIEW["freight_correspondences"]
        if r["rule_id"] == "bp-55c88a17f0ffb64f"
    )
    with pytest.raises(ClicInventoryError, match=freight["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(freight["source"], version="3.8"), family="uvek", version="2026"
        )


def test_other_approved_profiles_cannot_authorize_unreviewed_38_chemicals(
    tmp_path, monkeypatch
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    ids = {r["rule_id"] for r in REVIEW["records"]}
    for operation in ledger["operations"]:
        if operation["rule_id"] in ids:
            operation["scopes"] = []
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    row = REVIEW["records"][0]
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(row["original_rule"]["source"], version="3.8"),
            family="uvek",
            version="2026",
        )
