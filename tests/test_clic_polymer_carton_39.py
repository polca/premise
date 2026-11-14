"""Polymer and converted-carton mappings retain reference units, delivery, uncertainty and older approved suppliers."""

import copy
import json
import math

import pytest
from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer
from test_clic_rule_approvals import graph

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_polymer_carton_39_review.json").read_text()
)
ACTIVE_ROWS = REVIEW["records"]
CASES = [r["rule_id"] for r in ACTIVE_ROWS]


@pytest.mark.parametrize("rid", CASES)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_whole_services_keep_signed_functional_units_and_caller_fields(
    tmp_path, rid, amount, uncertainty
):
    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rid
    )
    resolved = graph(rule["source"], version="3.9")
    source = resolved.activities[0]["exchanges"][1]
    source.update(
        amount=amount,
        formula="service_demand",
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
    row = next(r for r in ACTIVE_ROWS if r["rule_id"] == rid)
    expected = copy.deepcopy(source)
    scale_exchange(expected, row["conversion_factor"])
    harmonizer = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for cached in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cached
        edge = result.activities[0]["exchanges"][1]
        assert len(result.activities) == (2 if row["recipe_id"] else 1)
        assert len(result.activities[0]["exchanges"]) == 3
        if row["recipe_id"]:
            helper = result.activities[1]
            assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
            actual = [e for e in helper["exchanges"] if e["type"] != "production"]
            expected_inputs = REVIEW["recipes"][row["recipe_id"]]["inputs"]
            assert len(actual) == len(expected_inputs)
            for actual_edge, expected_edge in zip(actual, expected_inputs, strict=True):
                for key, value in expected_edge.items():
                    assert actual_edge[key] == value
            assert not helper["brightpath_conversion"].get("source_input_omissions")
        else:
            assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())
        for key in (
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "formula",
        ):
            if key in expected:
                assert edge[key] == expected[key]
        assert edge["unit"] == rule["source"]["unit"]
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        manifest = json.dumps(result.manifest)
        assert all(limit in manifest for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("rid", CASES)
def test_older_approved_profile_does_not_authorize_unapproved_39(
    tmp_path, monkeypatch, rid
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for operation in ledger["operations"]:
        if operation["rule_id"] == rid:
            operation["scopes"] = [
                s for s in operation["scopes"] if s["source"]["version"] != "3.9"
            ]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    rule = next(
        r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == rid
    )
    resolved = graph(rule["source"], version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=rid):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before


@pytest.mark.parametrize(
    "prior",
    REVIEW["preserved_existing_scopes"],
    ids=lambda p: p["rule_id"] + p["source_profile"]["version"],
)
def test_older_approved_suppliers_stay_usable_after_profile_extension(tmp_path, prior):
    row = next(r for r in ACTIVE_ROWS if r["rule_id"] == prior["rule_id"])
    resolved = graph(
        row["original_rule"]["source"], version=prior["source_profile"]["version"]
    )
    before = copy.deepcopy(resolved.activities)
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    expected = prior["unchanged_physical_binding"]["target"]
    if "recipe_ref" in expected:
        assert (
            result.activities[1]["brightpath_conversion"]["recipe_id"]
            == expected["recipe_ref"]
        )
    else:
        assert len(result.activities) == 1
        assert all(
            result.activities[0]["exchanges"][1][k] == v for k, v in expected.items()
        )
    assert result.activities[0]["exchanges"][1]["amount"] == -2
    assert resolved.activities == before
