"""CLIC uses the direct European wool grid on cache misses and hits."""

import copy
import json

import pytest

from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph


@pytest.mark.parametrize("rule_id", ["bp-c9b874c8b5ebebe0", "bp-cfcab3636e7a220d"])
def test_wool_grid_simplification_preserves_statistics_and_retires_helper(
    tmp_path, rule_id
):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_wool_europe_direct_grid_review.json").read_text()
    )
    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rule_id
    )
    resolved = graph(rule["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        helpers = {
            a.get("brightpath_conversion", {}).get("recipe_id"): a
            for a in result.activities
        }
        assert review["retired_recipe_id"] not in helpers
        inputs = [
            e
            for e in helpers[review["owner_recipe_id"]]["exchanges"]
            if e["type"] != "production"
        ]
        expected = review["compiled_recipe"]["inputs"][review["input_index"]]
        electricity = inputs[review["input_index"]]
        for key, value in expected.items():
            assert electricity[key] == value
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and "supersedes the earlier requirement to retain the 39-country mix"
            in d["message"]
            for d in result.diagnostics
        )
    assert resolved.activities == before
