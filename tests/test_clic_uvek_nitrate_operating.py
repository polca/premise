"""Direct nitrate services need their own exact approvals and portable cache warnings."""

import copy
import json
import math

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import compatibility_approval_metadata
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_pilot_nitrate_operating_review.json").read_text()
)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_direct_services_preserve_signed_quantities_and_review_on_cold_and_warm_cache(
    tmp_path, row, amount, uncertainty
):
    resolved = graph(row["source"], version="3.9")
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount) or 1) if uncertainty == 2 else amount,
            "scale": 0.2,
        },
    )
    if uncertainty == 2:
        exchange["negative"] = amount < 0
    else:
        exchange.update(minimum=amount - 1, maximum=amount + 1)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 1
        actual = result.activities[0]["exchanges"][1]
        assert all(actual[k] == v for k, v in row["target"].items())
        for field in (
            "amount",
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
        ):
            if field in exchange:
                assert actual[field] == exchange[field]
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == row["rule_id"]
        )
        assert warning["message"] == row["limitations"]
        assert (
            warning["details"]["review_path"]
            == "export/uvek_pilot_nitrate_operating_review.json"
        )
        assert warning["details"]["inspection"]["fingerprint"] == row["fingerprint"]
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "row,version",
    [
        (row, version)
        for row in REVIEW["records"]
        for version in ("3.6", "3.12")
        # The diesel market already has a separate exact 3.6 fuel-service review.
        if not (row["rule_id"] == "bp-155787c4bf1ee018" and version == "3.6")
    ],
    ids=lambda value: value["rule_id"] if isinstance(value, dict) else value,
)
def test_direct_service_review_does_not_admit_uninspected_versions(
    tmp_path, row, version
):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_direct_admissions_keep_their_own_evidence_and_do_not_expand_kcl_parent_review():
    records = compatibility_approval_metadata(
        [row["fingerprint"] for row in REVIEW["records"]]
    )
    assert len(records) == 8
    assert {r["rule_id"] for r in records} == {r["rule_id"] for r in REVIEW["records"]}
    assert all(
        r["review_path"] == "export/uvek_pilot_nitrate_operating_review.json"
        for r in records
    )
    kcl = json.loads((DATA_DIR / "export/uvek_pilot_kcl_site_review.json").read_text())
    assert {r["rule_id"] for r in kcl["records"]} == {"bp-8c9638f0b5439efb"}
