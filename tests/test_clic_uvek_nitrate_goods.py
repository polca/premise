"""The public pilot retains market delivery and the corrected 3.9 generator basis."""

import copy
import json
import math

import pytest

from brightpath import DATA_DIR, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.migrations.resources import load_uvek_technosphere_resource
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
from premise.inventory.clic_target import resolve_inventory_target
from test_clic_rule_approvals import graph

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_pilot_nitrate_goods_review.json").read_text()
)
ROWS = REVIEW["records"] + REVIEW["direct_records"]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_market_cold_and_warm_caches_preserve_delivery_uncertainty_and_units(
    tmp_path, row, amount, uncertainty
):
    resolved = graph(row["source"], version="3.9")
    exchange = resolved.activities[0]["exchanges"][1]
    exchange.update(
        amount=amount,
        formula="dose",
        **{
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount) or 1) if uncertainty == 2 else amount,
            "scale": 0.2,
        },
    )
    if uncertainty == 2:
        exchange["negative"] = amount < 0
    else:
        exchange.update(minimum=amount - 1, maximum=amount + 1)
    before = copy.deepcopy(resolved.activities)
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = harmonizer.harmonize(resolved, family="uvek", version="2026")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        actual = result.activities[0]["exchanges"][1]
        for field in (
            "amount",
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
        ):
            if field in exchange:
                assert actual[field] == exchange[field]
        if row in REVIEW["direct_records"]:
            assert len(result.activities) == 1
            assert all(actual[k] == v for k, v in row["target"].items())
            assert (
                row["conversion_factor"] == 1 and row["thermal_MJ_reference_explicit"]
            )
        else:
            assert len(result.activities) == 2
            helper = result.activities[1]
            assert helper["unit"] == row["source"]["unit"]
            (production,) = [
                x for x in helper["exchanges"] if x["type"] == "production"
            ]
            assert production["amount"] == 1
            inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
            expected = row["profiles"][0]["inputs"]
            assert len(inputs) == len(expected)
            assert all(
                all(actual[k] == v for k, v in original.items())
                for actual, original in zip(inputs, expected, strict=True)
            )
            assert sum(x["unit"] == "ton kilometer" for x in inputs) == len(
                row["freight_witnesses"]
            )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == row["rule_id"]
        )
        assert warning["message"] == row["limitations"]
        assert (
            warning["details"]["review_path"]
            == "export/uvek_pilot_nitrate_goods_review.json"
        )
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_review_does_not_admit_uninspected_market_releases(tmp_path, row, version):
    resolved = graph(row["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_old_36_generator_normalization_is_separate_and_39_provider_is_not_admitted(
    tmp_path,
):
    old = json.loads(
        (DATA_DIR / "export/uvek_pilot_generator_fuel_review.json").read_text()
    )["rules"][0]
    source = graph(old["source"], version="3.6")
    original_amount = source.activities[0]["exchanges"][1]["amount"]
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(source, family="uvek", version="2026")
    assert result.activities[0]["exchanges"][1]["amount"] == pytest.approx(
        original_amount * old["conversion_factor"]
    )
    resource = load_uvek_technosphere_resource()
    provider = next(r for r in resource["replace"] if r["id"] == old["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        provider,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
