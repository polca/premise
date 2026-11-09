"""The public harmonizer preserves PE collection topology and cached warnings."""

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
    (DATA_DIR / "export/uvek_pilot_pe_collection_review.json").read_text()
)
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_collection_cold_and_warm_cache_preserves_nested_uncertainty(
    tmp_path, amount, uncertainty
):
    resolved = graph(ROW["source"], version="3.9")
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
        assert len(result.activities) == 12
        actual = result.activities[0]["exchanges"][1]
        assert actual["amount"] == -amount
        assert actual["formula"] == "(dose) * (-1.0)"
        assert actual["scale"] == 0.2
        root = next(
            a
            for a in result.activities
            if a.get("brightpath_conversion", {})
            .get("recipe_id", "")
            .startswith("pilot-pe-collection-")
        )
        (edge,) = [x for x in root["exchanges"] if x["type"] != "production"]
        assert edge["amount"] == 1 and edge["uncertainty type"] == 2
        assert edge["scale"] == ROW["source_group_exchange"]["scale"]
        assert edge["pedigree"] == ROW["source_group_exchange"]["pedigree"]
        assert (
            sum(
                x["type"] == "biosphere"
                for a in result.activities[1:]
                for x in a["exchanges"]
            )
            == 943
        )
        warning = next(
            d
            for d in result.diagnostics
            if d["code"] == "clic.compatibility_proxy_limitations"
            and d["details"]["rule_id"] == ROW["rule_id"]
        )
        assert "original PE proxy" in warning["message"]
        assert "unquantified" in warning["message"]
        assert not any(
            d["code"] == "migration.recipe_input_omission" for d in result.diagnostics
        )
    assert resolved.activities == before


@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_collection_other_releases_remain_unapproved(tmp_path, version):
    resolved = graph(ROW["source"], version=version)
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match="independent review"):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, family="uvek", version="2026"
        )
    assert resolved.activities == before


def test_previous_flat_building_disposal_correspondence_remains_unapproved():
    fingerprint = compatibility_rule_fingerprint(
        load_uvek_technosphere_resource(),
        "replace",
        ROW["previous_rule"],
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert fingerprint not in approved_compatibility_rule_scopes(
        resolve_inventory_target("2026", family="uvek")
    )
