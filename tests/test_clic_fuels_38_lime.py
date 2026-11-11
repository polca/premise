"""Reviewed limestone delivery and its mining burden survive strict CLIC caching."""

import copy
import json

from test_clic_rule_approvals import graph

from premise.inventory.clic_harmonize import ClicHarmonizer


def test_lime_delivery_fresh_and_cached_preserves_mass_and_nested_mining(tmp_path):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_38_lime_review.json").read_text()
    )
    resolved = graph(review["original_rule"]["source"], version="3.8")
    resolved.activities[0]["exchanges"][1].update(
        amount=-2.5,
        formula="signed_lime",
        **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    recipe = review["recipes"][review["compiled_rule"]["target"]["recipe_ref"]]
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == -2.5
        assert edge["formula"] == "signed_lime" and edge["scale"] == 0.3
        ds = next(a for a in result.activities if a["code"] == edge["input"][1])
        rows = [e for e in ds["exchanges"] if e["type"] != "production"]
        assert len(rows) == 6
        assert [e["amount"] for e in rows] == [e["amount"] for e in recipe["inputs"]]
        grinding = [
            e for e in rows if e["name"] == "Limestone, milled, loose, at plant"
        ]
        assert len(grinding) == 2
        assert len([e for e in rows if e["unit"] == "ton kilometer"]) == 3
        mining_edge = next(
            e for e in rows if e["unit"] == "kilogram" and e not in grinding
        )
        mining = next(
            a
            for a in result.activities
            if all(
                a.get(k) == mining_edge.get(k)
                for k in ("name", "reference product", "location", "unit")
            )
        )
        burden = [e for e in mining["exchanges"] if e["type"] != "production"]
        assert len(burden) == 1
        assert burden[0]["amount"] == 0.59 * (0.117 / 0.38)
        assert not any(
            d["code"] == "migration.recipe_source_input_omission"
            for d in result.diagnostics
        )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            for d in result.diagnostics
        )
    assert resolved.activities == before


def test_lime_needs_its_exact_reviewed_approval(tmp_path, monkeypatch):
    import pytest
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals
    from premise.inventory.clic_archive import ClicInventoryError

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_38_lime_review.json").read_text()
    )
    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ledger["operations"] = [
        row for row in ledger["operations"] if row["rule_id"] != review["rule_id"]
    ]
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    with pytest.raises(
        ClicInventoryError, match="bp-42ca176032e4b754 requires independent review"
    ):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            graph(review["original_rule"]["source"], version="3.8"),
            version="2026",
            family="uvek",
        )
