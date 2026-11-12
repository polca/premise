"""Complete PCC prerequisite reviews survive strict harmonization and caching."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer


@pytest.mark.parametrize("version", ["3.9"])
def test_pcc_market_fresh_cached_and_withdrawn_approvals(
    tmp_path, monkeypatch, version
):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    from premise.inventory import clic_rule_approvals
    from premise.inventory.clic_target import resolve_inventory_target

    review = json.loads((DATA_DIR / "export/uvek_pcc_market_review.json").read_text())
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
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in review["records"]
    }
    assert len(new_scopes) == 1 and new_scopes.issubset(approved)
    resolved = graph(
        *(row["compiled_rule"]["source"] for row in review["records"]),
        version=version,
    )
    for edge in resolved.activities[0]["exchanges"][1:]:
        edge.update({"uncertainty type": 3, "loc": -2, "scale": 0.3, "formula": "dose"})
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        assert (
            len(result.activities) == 32
        )  # One source consumer plus31reconstruction/dependency helpers.
        for row, edge in zip(
            review["records"], result.activities[0]["exchanges"][1:], strict=True
        ):
            factor = 1
            assert edge["amount"] == edge["loc"] == -2 * factor
            assert edge["scale"] == 0.3
            assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
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


def test_inherited_omissions_need_literal_root_approval(tmp_path, monkeypatch):
    from brightpath import DATA_DIR

    from premise.inventory import clic_rule_approvals

    review = json.loads((DATA_DIR / "export/uvek_pcc_market_review.json").read_text())
    assert sum(map(len, review["inherited_source_omissions"].values())) == 46
    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    decision = next(
        o for o in ledger["operations"] if o["rule_id"] == "bp-391c1f3aaffca472"
    )
    del decision["inspection"]["approved_recipe_source_omissions"]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(review["records"][0]["compiled_rule"]["source"], version="3.9")
    with pytest.raises(ClicInventoryError, match="unapproved losses"):
        ClicHarmonizer(tmp_path).harmonize(resolved, version="2026", family="uvek")
