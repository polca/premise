"""The current Mixed-plastic caller can use its explicitly reviewed older manufacturing model."""

import copy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph


@pytest.mark.parametrize("amount", [-2, 2])
def test_plastic_current_market_and_model_audits_survive_cache(tmp_path, amount):
    review = json.loads(
        (DATA_DIR / "export/uvek_hydrogen_train_plastic_312_review.json").read_text()
    )
    row = review["records"][0]
    resolved = graph(row["source"], version="3.12")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount, **{"uncertainty type": 3, "loc": amount, "scale": 0.2}
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 13
        exchange = result.activities[0]["exchanges"][1]
        assert exchange["amount"] == exchange["loc"] == -amount
        assert exchange["scale"] == 0.2
        helpers = result.activities[1:]
        market = next(
            a
            for a in helpers
            if a["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
        )
        inputs = [e for e in market["exchanges"] if e["type"] == "technosphere"]
        assert len(inputs) == 13
        for got, want in zip(
            inputs, review["recipes"][row["recipe_id"]]["inputs"], strict=True
        ):
            for key in ("amount", "uncertainty type", "loc", "scale", "pedigree"):
                if key in want:
                    assert got[key] == want[key]
        models = [a for a in helpers if a is not market]
        assert len(models) == 11
        assert all(
            a["brightpath_conversion"]["inventory_source_profile"]
            == review["inventory_model_profile"]
            for a in models
        )
        assert all(
            a["brightpath_conversion"]["caller_source_profile"]
            == review["source_profile"]
            for a in models
        )
        assert (
            sum(
                len(a["brightpath_conversion"].get("source_input_omissions", []))
                for a in models
            )
            == 0
        )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == row["rule_id"]
            for d in result.diagnostics
        )
    assert resolved.activities == before
