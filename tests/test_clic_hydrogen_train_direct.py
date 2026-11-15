"""Reviewed train suppliers keep signed coordinates and portable warnings."""

import copy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph


@pytest.mark.parametrize("amount", [-2, 2])
def test_train_direct_suppliers_keep_signed_quantities_and_warnings_on_cache_hits(
    tmp_path, amount
):
    review = json.loads(
        (DATA_DIR / "export/uvek_hydrogen_train_direct_312_review.json").read_text()
    )
    rows = review["rules"]
    assert len(rows) == 13
    resolved = graph(*(r["source"] for r in rows), version="3.12")
    for exchange in resolved.activities[0]["exchanges"][1:]:
        exchange.update(
            amount=amount, **{"uncertainty type": 3, "loc": amount, "scale": 0.1}
        )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        edges = result.activities[0]["exchanges"][1:]
        assert len(edges) == len(rows)
        for row, edge in zip(rows, edges, strict=True):
            assert all(edge[k] == v for k, v in row["native_target"].items())
            assert edge["amount"] == edge["loc"] == amount
            assert edge["uncertainty type"] == 3 and edge["scale"] == 0.1
            assert any(
                d["code"] == "clic.compatibility_proxy_limitations"
                and d["details"]["rule_id"] == row["rule_id"]
                for d in result.diagnostics
            )
    assert resolved.activities == before
