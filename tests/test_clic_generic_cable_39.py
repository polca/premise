"""Cable routing, service signs, scoped omissions and cache replay stay explicit."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.scaling import scale_exchange
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads((DATA_DIR / "export/uvek_generic_cable_39_review.json").read_text())
ROLLING = next(
    r
    for r in REVIEW["source_component_witnesses"]
    if r["rule_id"] == "bp-479b3316baa53263"
)
CASES = REVIEW["records"] + [
    {
        "rule_id": ROLLING["rule_id"],
        "original_rule": ROLLING["original_rule"],
        "recipe_id": None,
        "conversion_factor": 1,
    }
]


def resolved_case(row, amount, uncertainty=3):
    resolved = graph(row["original_rule"]["source"], version="3.9")
    edge = resolved.activities[0]["exchanges"][1]
    edge.update(
        amount=amount,
        formula="dose",
        **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
    )
    if uncertainty == 2:
        edge.update(loc=math.log(abs(amount)), negative=amount < 0)
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return resolved


def verify_preparation(tmp_path, row, amount, uncertainty):
    resolved = resolved_case(row, amount, uncertainty)
    before = copy.deepcopy(resolved.activities)
    scaled = copy.deepcopy(before[0]["exchanges"][1])
    scale_exchange(scaled, row["conversion_factor"])
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    allrecipes = {**REVIEW["recipes"], **REVIEW["recipe_dependencies"]}
    wanted = set()

    def collect(key):
        if key in wanted:
            return
        wanted.add(key)
        for entry in allrecipes[key]["inputs"]:
            if "recipe" in entry:
                collect(entry["recipe"])

    if row["recipe_id"]:
        collect(row["recipe_id"])
    for cached in (False, True):
        result = h.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        caller = result.activities[0]
        actual = caller["exchanges"][1]
        assert caller["exchanges"][-1]["amount"] == 0.5
        for field in (
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "formula",
        ):
            if field in scaled:
                assert actual[field] == scaled[field]
        assert actual["unit"] == row["original_rule"]["source"]["unit"]
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        assert set(helpers) == wanted
        if row["recipe_id"]:
            root = helpers[row["recipe_id"]]
            rows = [e for e in root["exchanges"] if e["type"] != "production"]
            assert [e["amount"] for e in rows] == [
                e["amount"] for e in allrecipes[row["recipe_id"]]["inputs"]
            ]
        else:
            assert all(
                actual[k] == v for k, v in row["original_rule"]["target"].items()
            )
        manifest = json.dumps(result.manifest)
        if row["rule_id"] in ["bp-5cf391e403d6bc65", "bp-9a89a1def5ba6bba"]:
            assert "migration.recipe_source_input_omission" in manifest
            assert "pilot-nitrate-hdpe-dfcfe7b3600a00f94f69" in manifest
        else:
            assert "migration.recipe_source_input_omission" not in manifest
    assert resolved.activities == before


@pytest.mark.parametrize("row", CASES, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_signed_units_and_literal_helper_graph_survive_fresh_and_cached_preparation(
    tmp_path, row, amount
):
    verify_preparation(tmp_path, row, amount, 3)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_negative_reference_waste_service_retains_lognormal_sign_through_cache(
    tmp_path, amount
):
    row = next(r for r in CASES if r["rule_id"] == "bp-735e189847cfb1be")
    verify_preparation(tmp_path, row, amount, 2)


@pytest.mark.parametrize("row", CASES, ids=lambda r: r["rule_id"])
def test_each_new_exact_scope_is_still_required(tmp_path, monkeypatch, row):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == row["rule_id"]:
            operation["scopes"] = [
                s for s in operation["scopes"] if s["source"]["version"] != "3.9"
            ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = resolved_case(row, -2.5)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before


@pytest.mark.parametrize("rid", ["bp-5cf391e403d6bc65", "bp-9a89a1def5ba6bba"])
def test_compatibility_permission_alone_does_not_authorize_recipe_omissions(
    tmp_path, monkeypatch, rid
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == rid:
            operation["inspection"]["approved_recipe_source_omissions"] = {}
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    row = next(r for r in CASES if r["rule_id"] == rid)
    resolved = resolved_case(row, 2.5)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(
        ClicInventoryError, match="unapproved losses.*recipe_source_input_omission"
    ):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
