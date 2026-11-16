"""Required older train supplies retain their signed reference quantities."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider

from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph


@pytest.mark.parametrize("amount", [-2, 2])
def test_train_direct_39_suppliers_and_limitations_survive_cache(tmp_path, amount):
    review = json.loads(
        (DATA_DIR / "export/uvek_hydrogen_train_direct_39_review.json").read_text()
    )
    resolved = graph(review["rules"][0]["source"], version="3.9")
    edges = resolved.activities[0]["exchanges"]
    edges[1:] = [
        {
            **row["source"],
            "type": "technosphere",
            "amount": amount,
            "uncertainty type": 3,
            "loc": amount,
            "scale": 0.2,
        }
        for row in review["rules"]
    ]
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 1
        converted = result.activities[0]["exchanges"][1:]
        for row, edge in zip(review["rules"], converted, strict=True):
            assert all(edge[k] == v for k, v in row["target"].items())
            assert edge["amount"] == edge["loc"] == amount
            assert edge["scale"] == 0.2
        warnings = {
            d["details"]["rule_id"]
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
        }
        assert warnings >= {r["rule_id"] for r in review["rules"]}
    assert resolved.activities == before
