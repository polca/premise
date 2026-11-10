"""Granular-carbon reconstruction retains its inputs and audited empty ash receivers."""

import copy
import json

import pytest
from test_clic_rule_approvals import graph

from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
from premise.inventory.clic_target import resolve_inventory_target


def test_carbon_closed_scopes_and_full_fresh_cached_inventory(tmp_path):
    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background import PackageCatalogProvider
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_39_carbon_review.json").read_text()
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
    root = rules["bp-dd3e07b2dd28af80"]
    resolved = graph(root["source"], version="3.9")
    incoming = resolved.activities[0]["exchanges"][1]
    incoming.update(
        amount=-2.5,
        formula="signed_carbon",
        **{"uncertainty type": 3, "loc": -2.5, "scale": 0.3},
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == edge["loc"] == -2.5
        assert edge["scale"] == 0.3 and edge["formula"] == "signed_carbon"
        for row in review["records"]:
            recipe = review["recipes"][row["new_recipe_id"]]
            ds = next(
                a
                for a in result.activities
                if a["name"].startswith(
                    recipe["name"] + " [BrightPath UVEK conversion "
                )
                and a["location"] == recipe["location"]
            )
            if "parameters" in recipe:
                assert ds["parameters"] == recipe["parameters"]
            if "source_input_omissions" in recipe:
                assert (
                    ds["brightpath_conversion"]["source_input_omissions"]
                    == recipe["source_input_omissions"]
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
        omissions = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        assert len(omissions) == 51
        assert all(
            d["details"]["source_profile"] == review["source_profile"]
            for d in omissions
        )
        assert all(
            d["details"]["omission"]["source_exchange"]["amount"] < 0 for d in omissions
        )
    assert resolved.activities == before


def test_carbon_omissions_require_the_exact_separate_approval(tmp_path, monkeypatch):
    from brightpath import DATA_DIR
    from brightpath.background import PackageCatalogProvider

    from premise.inventory import clic_rule_approvals

    review = json.loads(
        (DATA_DIR / "export/uvek_fuels_39_carbon_review.json").read_text()
    )
    ledger = copy.deepcopy(clic_rule_approvals._approval_ledger())
    ids = {row["rule_id"] for row in review["records"]}
    for operation in ledger["operations"]:
        if operation["rule_id"] in ids:
            operation["inspection"]["approved_recipe_source_omissions"] = {}
    monkeypatch.setattr(clic_rule_approvals, "_approval_ledger", lambda: ledger)
    root = next(r for r in review["records"] if r["rule_id"] == "bp-dd3e07b2dd28af80")
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    with pytest.raises(ClicInventoryError, match="unapproved losses"):
        harmonizer.harmonize(
            graph(root["original_rule"]["source"], version="3.9"),
            version="2026",
            family="uvek",
        )
