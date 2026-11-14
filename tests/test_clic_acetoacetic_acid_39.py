"""Retained acetoacetic chemistry and audited child omissions survive strict preparation/cache."""

import copy
import json
import math

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_acetoacetic_acid_39_review.json").read_text()
)
ROWS = REVIEW["records"]
RECIPES = {**REVIEW["recipe_dependencies"], **REVIEW["recipes"]}


def closure(key):
    result = set()

    def walk(recipe):
        if recipe in result:
            return
        result.add(recipe)
        for e in RECIPES[recipe]["inputs"]:
            if "recipe" in e:
                walk(e["recipe"])

    if key:
        walk(key)
    return result


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_chemistry_quantities_uncertainty_and_audits_survive(
    tmp_path, row, amount, uncertainty
):
    resolved = graph(row["original_rule"]["source"], version="3.9")
    source = resolved.activities[0]["exchanges"][1]
    source.update(
        amount=amount,
        formula="acid_or_water_dose",
        **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
    )
    if uncertainty == 2:
        source.update(loc=math.log(abs(amount)), negative=amount < 0)
    resolved.activities[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        actual = result.activities[0]["exchanges"][1]
        for key in (
            "amount",
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
        ):
            if key in source:
                assert actual[key] == source[key]
        assert len(result.activities[0]["exchanges"]) == 3
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        assert set(helpers) == closure(row["recipe_id"])
        for key, helper in helpers.items():
            inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
            assert len(inputs) == len(RECIPES[key]["inputs"])
            for edge, expected in zip(inputs, RECIPES[key]["inputs"], strict=True):
                for field in (
                    "amount",
                    "uncertainty type",
                    "loc",
                    "scale",
                    "minimum",
                    "maximum",
                    "negative",
                    "pedigree",
                    "scale without pedigree",
                    "formula",
                ):
                    if field in expected:
                        assert edge[field] == expected[field]
                assert "Chloroacetic acid" not in edge.get("name", "")
            assert helper["brightpath_conversion"].get(
                "source_input_omissions", []
            ) == RECIPES[key].get("source_input_omissions", [])
        manifest = json.dumps(result.manifest)
        assert all(limit in manifest for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_no_inherited_scope_can_replace_the_new_exact_review(
    tmp_path, monkeypatch, row
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == row["rule_id"]:
            operation["scopes"] = [
                s for s in operation["scopes"] if s["source"]["version"] != "3.9"
            ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(row["original_rule"]["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before


def test_literal_inherited_omissions_are_required_not_blanket_permission(
    tmp_path, monkeypatch
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    root = next(
        o
        for o in ledger["operations"]
        if o["rule_id"] == ROWS[0]["rule_id"]
        and any(s["source"]["version"] == "3.9" for s in o["scopes"])
    )
    root["inspection"].pop("nested_recipe_inspections")
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(ROWS[0]["original_rule"]["source"], version="3.9")
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
