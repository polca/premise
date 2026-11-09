"""Exact gas scopes retain source volume, energy-basis limitations and cached results."""

import copy
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_deferred_delivered_gas_review.json").read_text()
)


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source_profile"]["version"]
)
@pytest.mark.parametrize("amount", [-2, 0, 2])
def test_delivered_gas_cold_and_warm_preserves_caller_basis_and_records_energy_proxy(
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
            and len(result.activities) == 2
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
        (y,) = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert all(y[k] == v for k, v in row["profiles"][0]["inputs"][0].items())
        assert any(
            d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["inspection"] == row
            for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "row", REVIEW["records"], ids=lambda r: r["source_profile"]["version"]
)
@pytest.mark.parametrize("version", ["3.7", "3.12"])
def test_delivered_gas_does_not_extend_property_assumptions_to_other_releases(
    tmp_path, row, version
):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before
