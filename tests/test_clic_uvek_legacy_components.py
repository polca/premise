"""Direct legacy component scopes preserve caller data and portable limitations."""

import copy
import json
import math
from pathlib import Path

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import compatibility_approval_metadata
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_pilot_legacy_components_review.json").read_text()
)


@pytest.mark.parametrize("row", REVIEW["rules"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_legacy_component_cold_and_warm_cache_preserve_units_and_warnings(
    tmp_path, row, amount, uncertainty
):
    resolved = graph(row["source"], version="3.6")
    x = resolved.activities[0]["exchanges"][1]
    x.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount) or 1) if uncertainty == 2 else amount,
            "scale": 0.2,
        },
    )
    if uncertainty == 2:
        x["negative"] = amount < 0
    else:
        x.update(minimum=amount - 1, maximum=amount + 1)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert (
            result.manifest["cohorts"][0]["cache_hit"] is hit
            and len(result.activities) == row["expected_foreground_records"]
        )
        actual = result.activities[0]["exchanges"][1]
        target = row.get("effective_target", row["target"])
        assert all(actual[k] == v for k, v in target.items() if k != "name")
        if "generated_recipe_id" in row:
            assert actual["name"].startswith(
                target["name"] + " [BrightPath UVEK conversion "
            )
        else:
            assert actual["name"] == target["name"]
        assert all(
            actual[k] == v
            for k, v in x.items()
            if k
            in (
                "amount",
                "formula",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "minimum",
                "maximum",
            )
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == row["rule_id"]
        )
        assert (
            warning["message"] == row["limitations"]
            and warning["details"]["inspection"] == row
        )
    assert resolved.activities == before


@pytest.mark.parametrize("row", REVIEW["rules"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version", ["3.9", "3.12"])
def test_legacy_component_other_source_versions_are_refused(tmp_path, row, version):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_component_metadata_retains_the_three_scoped_inspections():
    ledger = json.loads(
        (
            Path(__file__).parents[1] / "premise/data/clic/approved_rule_scopes.json"
        ).read_text()
    )
    fps = [
        s["fingerprint"]
        for r in ledger["operations"]
        if r["review_path"] == "export/uvek_pilot_legacy_components_review.json"
        for s in r["scopes"]
    ]
    metadata = compatibility_approval_metadata(fps)
    assert len(metadata) == 3
    assert {m["rule_id"] for m in metadata} == {r["rule_id"] for r in REVIEW["rules"]}
    assert all("unquantified" in m["inspection"]["limitations"] for m in metadata)
