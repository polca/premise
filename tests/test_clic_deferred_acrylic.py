"""Wet recipes retain source quantities, direct flows and literal inherited audits."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_acrylic_review.json").read_text())


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source_profile"]["version"]
)
@pytest.mark.parametrize("amount", [-2, 0, 2])
def test_acrylic_cold_and_warm_preserves_caller_basis_and_records_energy_proxy(
    tmp_path, row, amount
):
    resolved = graph(row["source"], version=row["source_profile"]["version"])
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": 3,
            "loc": amount,
            "scale": 0.2,
            "minimum": amount - 1,
            "maximum": amount + 1,
        },
    )
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert (
            result.manifest["cohorts"][0]["cache_hit"] is cached
            and len(result.activities) == row["expected_foreground_records"]
        )
        x = result.activities[0]["exchanges"][1]
        assert all(
            x[k] == exchange[k]
            for k in (
                "amount",
                "unit",
                "formula",
                "uncertainty type",
                "loc",
                "scale",
                "minimum",
                "maximum",
            )
        )
        helper = next(
            a
            for a in result.activities
            if a.get("brightpath_conversion", {}).get("recipe_id")
            == row["generated_recipe_id"]
        )
        inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
        expected = row["profiles"][0]["inputs"]
        assert len(inputs) == len(expected)
        for actual, original in zip(inputs, expected, strict=True):
            assert all(actual[k] == v for k, v in original.items() if k != "recipe")
        assert (
            sum(
                x["type"] == "biosphere"
                for a in result.activities[1:]
                for x in a["exchanges"]
            )
            == 9
        )
        losses = [
            d
            for d in result.diagnostics
            if d["code"] == "migration.recipe_source_input_omission"
        ]
        assert len(losses) == 2
        assert all(
            d["details"]["omission"]
            in row["approved_recipe_source_omissions"][d["details"]["recipe_id"]]
            for d in losses
        )
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["inspection"] == row
            for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source_profile"]["version"]
)
@pytest.mark.parametrize("version", ["3.7", "3.11", "3.12"])
def test_acrylic_does_not_extend_property_assumptions_to_other_releases(
    tmp_path, row, version
):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before
