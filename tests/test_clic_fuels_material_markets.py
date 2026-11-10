"""Material market delivery survives fresh and cached conversion."""

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
        "bp-5b8870937f5c5812",
        "bp-00172801eed4fadd",
        "bp-bfe333cadde4fa6d",
        "bp-1104956dff1c701e",
        "bp-9860f8d2b0136d04",
        "bp-9995a288c793dd83",
        "bp-b7a1aa51acf41a9e",
        "bp-e25771265d4d36c5",
    ],
)
def test_material_market_fresh_cache_exact_scopes_and_delivery(tmp_path, rule_id):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_39_material_markets_review.json").read_text()
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
        formula="signed_material",
        **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == -2.5
        assert edge["scale"] == 0.3 and edge["formula"] == "signed_material"
        for row in [r for r in review["records"] if r["rule_id"] == rule_id]:
            recipe = review["recipes"][row["new_recipe_id"]]
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
