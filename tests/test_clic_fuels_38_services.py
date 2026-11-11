"""Exact 3.8 native-service approvals survive strict fresh/cached CLIC conversion."""

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
        "bp-fc9e4184d070b645",
        "bp-8ad41ac985efe967",
        "bp-3f19534eb7d97f64",
        "bp-aa3f7acdc2cf5750",
        "bp-25557ccca7d5a61f",
        "bp-a3a951644bd5ed9c",
        "bp-28ca31beb771b312",
        "bp-155787c4bf1ee018",
        "bp-182cd513cb8969f1",
        "bp-7881a1c501dadd9f",
        "bp-afd07e4d1223d368",
        "bp-7af323921dc1714c",
        "bp-d0e6ca1d19a58c1d",
        "bp-dc0174f7c0a93cdd",
        "bp-f5c20db2989094da",
        "bp-ab9ef0df411ba0fe",
    ],
)
def test_heat_recycling_fresh_cache_exact_scopes_and_delivery(tmp_path, rule_id):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_38_services_review.json").read_text()
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
        if "recipe_ref" in row["new_target"]:
            ds = next(a for a in result.activities if a["code"] == edge["input"][1])
            assert ds["name"].startswith(
                review["recipes"][row["new_target"]["recipe_ref"]]["name"]
            )
            assert len([e for e in ds["exchanges"] if e["type"] == "technosphere"]) == 4
            assert len([e for e in ds["exchanges"] if e["type"] == "biosphere"]) == 21
        else:
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
