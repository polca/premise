"""Exact packaging approvals retain complete recipes on cold and warm CLIC runs."""

import copy
import json

import pytest

from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph


@pytest.mark.parametrize("rule_id", ["bp-cf2ed054ced71820", "bp-6073dd107b7b9ea8"])
def test_packaging_complete_service_keeps_basis_fates_and_exact_omission_scope(
    tmp_path, rule_id
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = json.loads(
        (DATA_DIR / "export/uvek_packaging_38_complete_review.json").read_text()
    )
    row = next(r for r in review["records"] if r["rule_id"] == rule_id)
    resolved = graph(row["original_rule"]["source"], version="3.8")
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        helpers = {
            a.get("brightpath_conversion", {}).get("recipe_id"): a
            for a in result.activities
        }
        foreground = next(
            a for a in result.activities if a["code"] == resolved.activities[0]["code"]
        )
        assert foreground["exchanges"][1]["amount"] == -2
        factory = helpers[row["factory_recipe_id"]]
        inputs = [e for e in factory["exchanges"] if e["type"] != "production"]
        assert len(inputs) == 10
        assert len([e for e in inputs if "waste polyethylene" in e["name"]]) == 8
        market = helpers[row["root_recipe_id"]]
        assert (
            len(
                [
                    e
                    for e in market["exchanges"]
                    if e["type"] == "technosphere" and e["unit"] == "ton kilometer"
                ]
            )
            == 3
        )
        public = [
            a
            for key, a in helpers.items()
            if key and key.startswith("packaging-38-public-pe-")
        ]
        assert len(public) == 6
        assert (
            sum(e["type"] == "biosphere" for a in public for e in a["exchanges"]) == 612
        )
        omissions = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        assert len(omissions) == 4
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and "PACKED PRODUCT" in d["message"]
            for d in result.diagnostics
        )
    assert resolved.activities == before
