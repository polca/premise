"""Reviewed production heat conversions keep exact profiles, signs and warnings."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_harmonize import ClicHarmonizer


@pytest.mark.parametrize("version", ["3.8", "3.9", "3.12"])
def test_fresh_and_cached_heat_batch_retains_reference_and_limitations(
    tmp_path, version
):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.reference_signs import select_profile_sign_rules
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_remaining_production_heat_review.json").read_text()
    )
    profile = {"family": "ecoinvent", "version": version, "system_model": "cutoff"}
    rows = [r for r in review["records"] if profile in r["source_profiles"]]
    assert rows
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile(**profile)
    )
    rules = {r["id"]: r for r in selected["replace"]}
    resolved = graph(*(r["original_rule"]["source"] for r in rows), version=version)
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update(
            amount=-2.5,
            formula="signed_heat",
            **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        for row, edge in zip(rows, result.activities[0]["exchanges"][1:], strict=True):
            assert edge["amount"] == edge["loc"] == -2.5
            assert edge["scale"] == 0.3
            assert edge["formula"] == "signed_heat"
            target = rules[row["rule_id"]]["target"]
            if "recipe_ref" in target:
                helper = next(
                    a
                    for a in result.activities
                    if a.get("brightpath_conversion", {}).get("recipe_id")
                    == target["recipe_ref"]
                )
                assert helper["unit"] == "megajoule"
                inputs = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
                assert len(inputs) == 57
                assert sum(e["amount"] for e in inputs) == pytest.approx(1)
                assert all("district" not in e["name"].lower() for e in inputs)
            else:
                assert all(edge[key] == value for key, value in target.items())
        serialized = json.dumps(result.manifest, ensure_ascii=False)
        assert all(
            message in serialized for row in rows for message in row["limitations"]
        )
    assert resolved.activities == before
