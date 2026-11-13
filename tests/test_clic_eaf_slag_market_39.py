"""Slag treatment keeps service signs and literal cut-off handoff omission permissions through cache."""

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

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_eaf_slag_market_39_review.json").read_text()
)
CASES = REVIEW["records"]


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
        assert "migration.recipe_source_input_omission" in manifest
        root = helpers[row["recipe_id"]]
        assert (
            root["brightpath_conversion"]["source_input_omissions"]
            == REVIEW["approved_recipe_source_omissions"][row["recipe_id"]]
        )
        assert not any(
            a["brightpath_conversion"].get("zero_burden") for a in helpers.values()
        )
    assert resolved.activities == before


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_units_and_literal_graph_survive_fresh_and_cached_preparation(
    tmp_path, amount, uncertainty
):
    verify_preparation(tmp_path, CASES[0], amount, uncertainty)


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


@pytest.mark.parametrize("change", ["all", "first", "signed_amount", "index"])
def test_compatibility_permission_alone_never_authorizes_changed_omission_audits(
    tmp_path, monkeypatch, change
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    row = CASES[0]
    op = next(o for o in ledger["operations"] if o["rule_id"] == row["rule_id"])
    mapping = op["inspection"]["approved_recipe_source_omissions"]
    if change == "all":
        mapping.clear()
    elif change == "first":
        mapping[row["recipe_id"]].pop(0)
    elif change == "signed_amount":
        mapping[row["recipe_id"]][0]["source_exchange"]["amount"] *= -1
    else:
        mapping[row["recipe_id"]][0]["original_index"] += 1
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = resolved_case(row, -2.5)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(
        ClicInventoryError, match="unapproved losses.*recipe_source_input_omission"
    ):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
