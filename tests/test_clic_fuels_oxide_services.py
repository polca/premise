"""Oxide delivery and signed services survive fresh/cached CLIC conversion."""

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
        "bp-57c9160454525453",
        "bp-3977d3526b1a0617",
        "bp-9f742d14ef4d8ca9",
        "bp-fa607ec8232396e4",
        "bp-d11cb973ed24a79a",
        "bp-f7cc2346dd145561",
        "bp-af821ca7d28786a8",
        "bp-eb7ad12f2fa498ed",
        "bp-aa17e516d5f814ea",
    ],
)
def test_oxide_services_fresh_cache_exact_scopes_and_delivery(tmp_path, rule_id):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_39_oxide_services_review.json").read_text()
    )
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
    bridge = TechnosphereProfile("uvek", "2025", "cutoff")
    for row in review["records"]:
        for version, model, approved in [
            ("3.9", "cutoff", True),
            ("3.10", "cutoff", False),
            ("3.9", "consequential", False),
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
    resolved = graph(root["source"], version="3.9")
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
        if "new_recipe_id" not in row:
            for field, value in row["new_target"].items():
                actual = (
                    edge.get("product", edge.get(field))
                    if field == "reference product"
                    else edge[field]
                )
                assert actual == value
        for recipe_id in row.get("new_recipe_ids", []):
            recipe = review["recipes"][recipe_id]
            ds = next(
                a
                for a in result.activities
                if a["name"].startswith(
                    recipe["name"] + " [BrightPath UVEK conversion "
                )
            )
            inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
            for actual, expected in zip(inputs, recipe["inputs"], strict=True):
                for key in (
                    "amount",
                    "uncertainty type",
                    "loc",
                    "scale",
                    "negative",
                    "minimum",
                    "maximum",
                    "pedigree",
                    "scale without pedigree",
                    "formula",
                ):
                    if key in expected:
                        assert actual[key] == expected[key]
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == root["id"]
            for d in result.diagnostics
        )
    assert resolved.activities == before
