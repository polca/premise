"""Three scoped fluoride decisions survive harmonization and cache replay."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_battery_fluoride_39_review.json").read_text()
)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_signed_kg_and_caller_emission_survive_fresh_and_cached_preparation(
    tmp_path, row, amount
):
    resolved = graph(row["original_rule"]["source"], version="3.9")
    resolved.activities[0]["exchanges"][1].update(
        amount=amount,
        formula="material_dose",
        **{"uncertainty type": 3, "loc": amount, "scale": 0.2},
    )
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
        assert actual["amount"] == actual["loc"] == amount
        assert actual["scale"] == 0.2 and actual["formula"] == "material_dose"
        assert actual["unit"] == "kilogram"
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        assert result.activities[0]["exchanges"][-1]["type"] == "biosphere"
        expected_recipes = {**REVIEW["recipes"], **REVIEW["recipe_dependencies"]}
        wanted = set()

        def collect(key):
            if key in wanted:
                return
            wanted.add(key)
            for entry in expected_recipes[key]["inputs"]:
                if "recipe" in entry:
                    collect(entry["recipe"])

        collect(row["recipe_id"])
        assert len(result.activities) == len(wanted) + 1
        assert {
            a["brightpath_conversion"]["recipe_id"] for a in result.activities[1:]
        } == wanted
        if row["recipe_id"]:
            helper = next(
                a
                for a in result.activities[1:]
                if a["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
            )
            assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
            assert (
                actual["reference product"]
                == row["source_witness"]["identity"]["reference product"]
            )
            expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
            edges = [e for e in helper["exchanges"] if e["type"] != "production"]
            assert len(edges) == len(expected)
            assert [e["amount"] for e in edges] == [e["amount"] for e in expected]
        else:
            assert all(
                actual[k] == v for k, v in row["compiled_rule"]["target"].items()
            )
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_each_material_still_requires_its_own_scoped_approval(
    tmp_path, monkeypatch, row
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    ledger["operations"] = [
        o for o in ledger["operations"] if o["rule_id"] != row["rule_id"]
    ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(row["original_rule"]["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=row["rule_id"]):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before
