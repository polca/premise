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


@pytest.mark.parametrize("amount", [-2, 2])
def test_train_new_aluminium_scrap_keeps_positive_reference_cutoff_handover(
    tmp_path, amount
):
    review = json.loads((DATA_DIR / "export/uvek_cutoff_stock_review.json").read_text())
    source = next(
        r["source"]
        for r in review["rules"]
        if r["source"]["name"] == "aluminium scrap, new, Recycled Content cut-off"
    )
    resolved = graph(source, version="3.12")
    resolved.activities[0]["exchanges"][1]["amount"] = amount
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        owner = result.activities[0]
        exchange = owner["exchanges"][1]
        assert exchange["amount"] == amount
        helpers = [a for a in result.activities[1:] if a["name"] == exchange["name"]]
        assert len(helpers) == 1
        edges = helpers[0]["exchanges"]
        assert len(edges) == 1
        assert edges[0]["type"] == "production" and edges[0]["amount"] == 1
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == "bp-37b9b46586e881e7"
            for d in result.diagnostics
        )
    assert resolved.activities == before
