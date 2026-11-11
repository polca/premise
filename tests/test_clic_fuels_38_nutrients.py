"""Exact nutrient and pesticide approvals retain signed demand through fresh/cache conversion."""

import copy
import json

import pytest

from test_clic_rule_approvals import graph

from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
from premise.inventory.clic_target import resolve_inventory_target


@pytest.mark.parametrize(
    "rule_id",
    [
        "bp-ab4e383b592be518",
        "bp-1e5665194f1a7a40",
        "bp-342b0acc67aa560e",
        "bp-498d9c2e6fa6b060",
        "bp-a7832124e7202895",
        "bp-debda812c6313c05",
        "bp-9bcfc79d3362fe6f",
        "bp-4d440a5ab156b17c",
        "bp-89125291fb94d47e",
        "bp-bd7ba1d16a722d39",
        "bp-0cb5cbfb2134d167",
        "bp-41b83536c4b66e37",
    ],
)
def test_heat_recycling_fresh_cache_exact_scopes_and_delivery(tmp_path, rule_id):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_38_nutrients_review.json").read_text()
    )
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
    bridge = TechnosphereProfile("uvek", "2025", "cutoff")
    for row in review["records"]:
        for version, model, approved in [
            ("3.8", "cutoff", True),
            ("3.8", "consequential", False),
        ]:
            fingerprint = compatibility_rule_fingerprint(
                resource,
                "replace",
                rules[row["rule_id"]],
                TechnosphereProfile("ecoinvent", version, model),
                bridge,
            )
            assert (fingerprint in scopes) is approved
    root = rules[rule_id]
    resolved = graph(root["source"], version="3.8")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=-2.5,
        formula="signed_service",
        **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        row = next(r for r in review["records"] if r["rule_id"] == rule_id)
        factor = row["source_witness"]["reference_amounts"][0]
        assert edge["amount"] == edge["loc"] == -2.5 * factor
        assert edge["scale"] == 0.3
        assert edge["formula"] == (
            "signed_service" if factor == 1 else "(signed_service) * (-1.0)"
        )
        for field, value in row["new_target"].items():
            actual = (
                edge.get("product", edge.get(field))
                if field == "reference product"
                else edge[field]
            )
            assert actual == value
        assert len(result.activities) == 1
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == root["id"]
            for d in result.diagnostics
        )
    assert resolved.activities == before
