"""Heat/recycling and observed empty gas stock survive fresh/cached CLIC conversion."""

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
        "bp-3619fd839406a61f",
        "bp-a6429485fc43a215",
        "bp-ae076e1077dfff2c",
        "bp-126729272c9c1747",
        "bp-35c7a09fc71ff9db",
        "bp-f6f59e6dec650096",
        "bp-08e2003181e3e140",
    ],
)
def test_heat_recycling_fresh_cache_exact_scopes_and_delivery(tmp_path, rule_id):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_39_heat_recycling_review.json").read_text()
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
        if row["resource_action"] != "unchanged_rule":
            for field, value in row["new_target"].items():
                actual = (
                    edge.get("product", edge.get(field))
                    if field == "reference product"
                    else edge[field]
                )
                assert actual == value
        if row["resource_action"] == "unchanged_rule":
            ds = next(a for a in result.activities if a["code"] == edge["input"][1])
            assert ds["reference product"] == "biogas" and ds["unit"] == "cubic meter"
            assert len(ds["exchanges"]) == 1
            assert (
                ds["exchanges"][0]["type"] == "production"
                and ds["exchanges"][0]["amount"] == 1
            )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == root["id"]
            for d in result.diagnostics
        )
    assert resolved.activities == before
