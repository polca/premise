"""Only inspected positive production-only stock handovers enter Premise."""

import copy

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

RULE_IDS = {
    "bp-cd0e2e0252556e0e",
    "bp-febf9f5de6b7b989",
    "bp-b74f6df51cca91ae",
    "bp-72e20647991ff00e",
    "bp-679e09d6edfabf75",
    "bp-c63b19dd1ce08539",
}
VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]


@pytest.mark.parametrize("version", VERSIONS)
def test_exact_stocks_fresh_cached_and_withdrawn_approvals(
    tmp_path, monkeypatch, version
):
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
    from premise.inventory.clic_target import resolve_inventory_target

    resource = load_uvek_technosphere_resource()
    rules = sorted(
        (r for r in resource["replace"] if r["id"] in RULE_IDS), key=lambda r: r["id"]
    )
    assert len(rules) == 6
    approved = approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
    exact_scopes = {
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for rule in rules
    }
    assert len(exact_scopes) == 6 and exact_scopes.issubset(approved)
    resolved = graph(*(r["source"] for r in rules), version=version)
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update({"uncertainty type": 3, "loc": -2, "scale": 0.3, "formula": "dose"})
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path)
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert len(result.activities) == 7
        for edge in result.activities[0]["exchanges"][1:]:
            assert edge["amount"] == edge["loc"] == -2
            assert edge["scale"] == 0.3 and edge["formula"] == "dose"
        for helper in result.activities[1:]:
            assert helper["brightpath_conversion"]["zero_burden"] is True
            assert len(helper["exchanges"]) == 1
            assert helper["exchanges"][0]["type"] == "production"
            assert helper["exchanges"][0]["amount"] == 1
        reviewed = {
            i.get("details", {}).get("rule_id")
            for i in result.diagnostics
            if i["code"] == "migration.approved_correspondence"
        }
        assert RULE_IDS.issubset(reviewed)
    monkeypatch.setattr(
        "premise.inventory.clic_harmonize.approved_compatibility_rule_scopes",
        lambda target: tuple(s for s in approved if s not in exact_scopes),
    )
    with pytest.raises(ClicInventoryError, match="requires independent review"):
        harmonizer.harmonize(resolved, version="2026", family="uvek")
    assert resolved.activities == before
