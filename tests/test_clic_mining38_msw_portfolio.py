"""MSW portfolio retains treatment signs, nested helpers and explicit proxy warnings."""

import copy
import gzip
import json
from pathlib import Path

import pytest

from test_clic_rule_approvals import graph

from premise.inventory.clic_harmonize import ClicHarmonizer


@pytest.mark.parametrize("version", ["3.8"])
def test_msw_portfolio_warnings_and_signs_survive_cache(tmp_path, version):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.reference_signs import select_profile_sign_rules
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_mining_38_msw_portfolio_review.json").read_text()
    )
    rows = (
        review["records"]
        if version == "3.8"
        else [
            row
            for row in review["records"]
            if any(
                p["source_profile"]["version"] == version
                for p in row["original_rule"]
                .get("reference_flow_sign_review", {})
                .get("profiles", [])
            )
        ]
    )
    assert rows
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(),
        TechnosphereProfile("ecoinvent", version, "cutoff"),
    )
    rules = {r["id"]: r for r in selected["replace"]}
    resolved = graph(*(r["original_rule"]["source"] for r in rows), version=version)
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=-2.5,
            formula="signed_material",
            **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        cohort = result.manifest["cohorts"][0]
        assert cohort["cache_hit"] is cached
        for row, edge in zip(rows, result.activities[0]["exchanges"][1:], strict=True):
            factor = row["conversion_factor"]
            assert edge["amount"] == edge["loc"] == -2.5 * factor
            assert edge["scale"] == 0.3
            assert edge["formula"] == (
                "signed_material" if factor == 1 else "(signed_material) * (-1.0)"
            )
            target = rules[row["rule_id"]]["target"]
            if "recipe_ref" not in target:
                assert all(edge[key] == value for key, value in target.items())
        audit = json.loads(gzip.decompress(Path(cohort["audit_file"]).read_bytes()))
        matched = audit["premise_compatibility_reviews"]
        decisions = {r["rule_id"]: r for r in matched}
        for row in rows:
            if version == "3.8":
                assert decisions[row["rule_id"]]["limitations"] == row["limitations"]
            else:
                assert (
                    decisions[row["rule_id"]]["review_path"]
                    != "export/uvek_mining_38_msw_portfolio_review.json"
                )
        warnings = [
            i
            for stage in audit["stages"]
            for i in stage.get("issues", [])
            if i["code"] == "clic.compatibility_proxy_limitations"
        ]
        assert {w["details"]["rule_id"] for w in warnings} == {
            r["rule_id"] for r in rows
        }
    assert resolved.activities == before
