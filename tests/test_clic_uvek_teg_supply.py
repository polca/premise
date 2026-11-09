"""The public UVEK harmonizer retains TEG supply shares and allocation."""

import copy
import json
import math

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from premise.inventory.clic_rule_approvals import approved_compatibility_rule_scopes
from premise.inventory.clic_target import resolve_inventory_target
from test_clic_rule_approvals import graph

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_teg_supply_review.json").read_text())
ROW = REVIEW["records"][0]
STOCK = REVIEW["stock"]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_teg_supply_preserves_allocation_delivery_and_cached_warnings(
    tmp_path, amount, uncertainty
):
    resolved = graph(ROW["source"], version="3.6")
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
        assert len(result.activities) == 3
        actual = result.activities[0]["exchanges"][1]
        for field in (
            "amount",
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "minimum",
            "maximum",
            "negative",
        ):
            if field in exchange:
                assert actual[field] == exchange[field]
        stock = next(
            a
            for a in result.activities
            if a.get("brightpath_conversion", {}).get("recipe_id") == STOCK["recipe_id"]
        )
        assert len(stock["exchanges"]) == 1
        assert stock["exchanges"][0]["amount"] == 1
        helper = next(a for a in result.activities[1:] if a is not stock)
        inputs = [x for x in helper["exchanges"] if x["type"] == "technosphere"]
        assert {x["name"]: x["amount"] for x in inputs} == pytest.approx(
            {
                "Triethylene glycol, at plant": 0.647131654973173,
                stock["name"]: 0.352868345026827,
                "Transport, freight, rail": 0.0438,
                "Transport, barge": 0.0219,
                "Transport, freight, lorry, fleet average": 0.1713,
            }
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == ROW["rule_id"]
        )
        assert "same-activity negative-waste-reference" in warning["message"]
        assert "unquantified" in warning["message"]
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("source", [ROW["source"], STOCK["source"]])
@pytest.mark.parametrize("version", ["3.9", "3.12"])
def test_other_teg_releases_remain_unapproved(tmp_path, source, version):
    resolved = graph(source, version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


@pytest.mark.parametrize("row", [ROW, STOCK])
def test_old_flat_teg_correspondences_remain_unapproved(row):
    from brightpath import TechnosphereProfile
    from brightpath.background.rule_approvals import compatibility_rule_fingerprint
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    fingerprint = compatibility_rule_fingerprint(
        load_uvek_technosphere_resource(),
        "replace",
        row["previous_rule"],
        TechnosphereProfile("ecoinvent", "3.6", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
