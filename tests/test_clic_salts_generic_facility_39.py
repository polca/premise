"""Salt delivery and whole generic/facility references retain their scoped permissions."""

import copy
import math
import json

import pytest
from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.migrations.scaling import scale_exchange
from brightpath.migrations.resources import load_uvek_technosphere_resource
from test_clic_rule_approvals import graph

from premise.inventory import clic_rule_approvals as approvals
from premise.inventory.clic_archive import ClicInventoryError
from premise.inventory.clic_harmonize import ClicHarmonizer

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_salts_generic_facility_39_review.json").read_text()
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
            delivery = next(
                a for a in result.activities if a.get("code") == edge["input"][1]
            )
            assert delivery["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
            entries = [e for e in delivery["exchanges"] if e["type"] != "production"]
            assert (
                len(entries) == row["boundary_checks"]["source_market_entries_retained"]
            )
            assert (
                len([e for e in entries if e["unit"] == "ton kilometer"])
                == row["boundary_checks"]["final_delivery_entries_retained"]
            )
            assert all(
                e["name"] == row["boundary_checks"]["correct_native_product"]
                for e in entries
                if e["unit"] == "kilogram"
            )
        else:
            assert edge["name"] == row["boundary_checks"]["correct_native_product"]
            assert edge["unit"] == row["boundary_checks"]["reference_unit"]
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


def test_earlier_generic_36_scope_still_converts_without_a_helper(tmp_path):
    row = ACTIVE_ROWS[2]
    resolved = graph(row["original_rule"]["source"], version="3.6")
    result = ClicHarmonizer(
        tmp_path, catalog_provider=PackageCatalogProvider()
    ).harmonize(resolved, version="2026", family="uvek")
    assert len(result.activities) == 1
    assert result.activities[0]["exchanges"][1]["name"] == "Chemicals organic, at plant"


@pytest.mark.parametrize(
    "name,product",
    [
        ("market for tetraethyl orthosilicate", "tetraethyl orthosilicate"),
        ("market for hexafluoroethane", "hexafluoroethane"),
    ],
)
def test_generic_chemical_scope_does_not_admit_named_substances(
    tmp_path, name, product
):
    identity = {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "unit": "kilogram",
    }
    resolved = graph(identity, version="3.9")
    with pytest.raises(ClicInventoryError):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
