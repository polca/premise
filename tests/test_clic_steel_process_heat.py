"""Exact steel prerequisite reviews survive strict harmonization and caching."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer


def test_steel_process_heat_fresh_cached_and_withdrawn_approvals(tmp_path, monkeypatch):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    from premise.inventory import clic_rule_approvals
    from premise.inventory.clic_target import resolve_inventory_target

    review = json.loads(
        (DATA_DIR / "export/uvek_steel_39_process_heat_review.json").read_text()
    )
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    approved = clic_rule_approvals.approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
    new_scopes = {
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in review["records"]
    }
    assert len(new_scopes) == 3 and new_scopes.issubset(approved)
    resolved = graph(
        *(row["source_witness"]["identity"] for row in review["records"]),
        version="3.9",
    )
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update({"uncertainty type": 3, "loc": -2, "scale": 0.3, "formula": "dose"})
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 1  # Reuse the native allocated-process burden.
        for row, edge in zip(
            review["records"], result.activities[0]["exchanges"][1:], strict=True
        ):
            factor = row["conversion_factor"]
            assert edge["amount"] == edge["loc"] == pytest.approx(-2 * factor)
            assert edge["scale"] == pytest.approx(0.3 * factor)
            assert edge["formula"] == f"(dose) * ({factor})"
        reviewed_ids = {
            item["details"]["rule_id"]
            for item in result.diagnostics
            if item["code"] == "clic.compatibility_proxy_limitations"
        }
        assert {row["rule_id"] for row in review["records"]}.issubset(reviewed_ids)
    monkeypatch.setattr(
        "premise.inventory.clic_harmonize.approved_compatibility_rule_scopes",
        lambda target: tuple(scope for scope in approved if scope not in new_scopes),
    )
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    assert resolved.activities == before


@pytest.mark.parametrize("regionalized", [False, True])
def test_shared_relinking_keeps_process_identity_and_allocated_demand(
    tmp_path, regionalized
):
    from brightpath import DATA_DIR
    from premise.core.transformation import BaseTransformation

    review = json.loads(
        (DATA_DIR / "export/uvek_steel_39_process_heat_review.json").read_text()
    )
    resolved = graph(
        *(row["source_witness"]["identity"] for row in review["records"]),
        version="3.9",
    )
    result = ClicHarmonizer(tmp_path).harmonize(resolved, version="2026", family="uvek")
    data = copy.deepcopy(result.activities)
    before = copy.deepcopy(data[0]["exchanges"][1:])
    transformation = object.__new__(BaseTransformation)
    transformation.model = "remind"
    transformation.database = data
    transformation._validation_relinked_targets = {}
    semantics = {(e["name"], e["product"], e["location"]): 1 for e in before}
    transformation._get_provider_semantic_index = lambda: (
        {} if regionalized else semantics
    )
    transformation.cache = {}
    if regionalized:
        for edge in before:
            transformation.cache.setdefault(data[0]["location"], {}).setdefault(
                "remind", {}
            )[(edge["name"], edge["product"], edge["location"], edge["unit"])] = [
                (edge["name"], edge["product"], "NEU", edge["unit"], 1.0)
            ]
    transformation.relink_datasets()
    expected_totals = {}
    actual_totals = {}
    for edge in before:
        key = (edge["name"], edge["product"], edge["unit"])
        expected_totals[key] = expected_totals.get(key, 0) + edge["amount"]
    for edge in data[0]["exchanges"][1:]:
        key = (edge["name"], edge["product"], edge["unit"])
        actual_totals[key] = actual_totals.get(key, 0) + edge["amount"]
        assert edge["location"] == ("NEU" if regionalized else "RER")
        assert "market" not in edge["name"].lower()
        assert "converter" not in edge["name"].lower()
    assert actual_totals == pytest.approx(expected_totals)
