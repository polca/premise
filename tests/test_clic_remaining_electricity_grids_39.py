"""Chemical batches keep all caller quantities, original delivery entries and warnings through cache."""

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

REVIEW = json.loads(
    (DATA_DIR / "export/uvek_remaining_electricity_grids_39_review.json").read_text()
)
ROWS = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_sodium_methoxide_keep_caller_fields_delivery_and_limits_in_fresh_and_cached_batches(
    tmp_path, amount, uncertainty
):
    resolved = graph(*(r["original_rule"]["source"] for r in ROWS), version="3.9")
    for i, e in enumerate(resolved.activities[0]["exchanges"][1:]):
        e.update(
            amount=amount,
            formula=f"chemical_demand_{i}",
            **{"uncertainty type": uncertainty, "loc": amount, "scale": 0.2},
        )
        if uncertainty == 2:
            e.update(loc=math.log(abs(amount)), negative=amount < 0)
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
    for cache_hit in (False, True):
        result = harmonizer.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is cache_hit
        assert len(result.activities) == 1
        helpers = {
            a["brightpath_conversion"]["recipe_id"]: a for a in result.activities[1:]
        }
        for i, row in enumerate(ROWS, 1):
            e = result.activities[0]["exchanges"][i]
            for key in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "formula",
            ):
                if key in before[0]["exchanges"][i]:
                    assert e[key] == before[0]["exchanges"][i][key]
            target = (
                helpers[row["recipe_id"]]
                if row["recipe_id"]
                else row["compiled_rule"]["target"]
            )
            assert all(
                e[k] == target[k]
                for k in ["name", "reference product", "unit", "location"]
            )
            if row["recipe_id"]:
                actual = [e for e in target["exchanges"] if e["type"] != "production"]
                expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
                assert len(actual) == len(expected)
                assert all(
                    all(e[k] == v for k, v in r.items())
                    for e, r in zip(actual, expected, strict=True)
                )
                assert not target["brightpath_conversion"].get("source_input_omissions")
        assert result.activities[0]["exchanges"][-1]["amount"] == 0.5
        manifest = json.dumps(result.manifest, ensure_ascii=False)
        assert all(limit in manifest for row in ROWS for limit in row["limitations"])
    assert resolved.activities == before


@pytest.mark.parametrize("rid", [r["rule_id"] for r in ROWS])
def test_one_missing_approval_refuses_the_whole_batch_without_mutation(
    tmp_path, monkeypatch, rid
):
    ledger = copy.deepcopy(approvals._approval_ledger())
    for op in ledger["operations"]:
        if op["rule_id"] == rid:
            op["scopes"] = [s for s in op["scopes"] if s["source"]["version"] != "3.9"]
    monkeypatch.setattr(approvals, "_approval_ledger", lambda: ledger)
    resolved = graph(*(r["original_rule"]["source"] for r in ROWS), version="3.9")
    before = copy.deepcopy(resolved.activities)
    with pytest.raises(ClicInventoryError, match=rid):
        ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider()).harmonize(
            resolved, version="2026", family="uvek"
        )
    assert resolved.activities == before


def test_older_38_European_permission_keeps_complete_grid(tmp_path):
    (prior,) = REVIEW["preserved_existing_scopes"]
    resolved = graph(prior["unchanged_physical_binding"]["source"], version="3.8")
    resolved.activities[0]["exchanges"][1]["amount"] = -2.5
    before = copy.deepcopy(resolved.activities)
    h = ClicHarmonizer(tmp_path, catalog_provider=PackageCatalogProvider())
    for hit in (False, True):
        result = h.harmonize(resolved, version="2026", family="uvek")
        assert result.manifest["cohorts"][0]["cache_hit"] is hit
        assert len(result.activities) == 1
        edge = result.activities[0]["exchanges"][1]
        assert edge["amount"] == -2.5
        assert all(
            edge[k] == v
            for k, v in prior["unchanged_physical_binding"]["target"].items()
        )
    assert resolved.activities == before


def test_shared_market_update_redirects_native_grids_and_preserves_global_industry():
    from unittest.mock import Mock

    import yaml
    from premise.core.filesystem_constants import VARIABLES_DIR
    from premise.electricity.electricity import Electricity

    def record(identity):
        return {
            **identity,
            "exchanges": [
                {**identity, "type": "production", "amount": 1},
                {
                    "name": "Existing generator",
                    "reference product": "electricity",
                    "location": "GLO",
                    "unit": "kilowatt hour",
                    "type": "technosphere",
                    "amount": 1.04,
                },
            ],
        }

    source_cobalt = record(ROWS[0]["original_rule"]["source"])
    native_industry = record(ROWS[0]["original_rule"]["target"])
    source_aluminium = record(
        {
            "name": "market for electricity, high voltage, aluminium industry",
            "reference product": "electricity, high voltage, aluminium industry",
            "location": "GLO",
            "unit": "kilowatt hour",
        }
    )
    protected = [source_cobalt, native_industry, source_aluminium]
    protected_before = copy.deepcopy(protected)
    grids = [record(r["original_rule"]["target"]) for r in ROWS[1:]]
    owner = object.__new__(Electricity)
    owner.database = [*protected, *grids]
    owner.market_mapping = yaml.safe_load(
        (VARIABLES_DIR / "electricity_markets.yaml").read_text()
    )["uvek"]
    owner.ecoinvent_to_iam_loc = {"ENTSO-E": "EUR", "DE": "EUR", "ZA": "SSA"}
    for method in (
        "write_log",
        "remove_from_index",
        "add_new_entry_to_cache",
        "create_new_markets_high_voltage",
        "create_new_markets_medium_voltage",
        "create_new_markets_low_voltage",
    ):
        setattr(owner, method, Mock())
    owner.adjust_aluminium_electricity_markets()
    owner.update_electricity_markets()
    assert protected == protected_before
    for grid, row in zip(grids, ROWS[1:], strict=True):
        (purchase,) = [e for e in grid["exchanges"] if e["type"] == "technosphere"]
        voltage = row["boundary_checks"]["delivered_voltage"]
        assert purchase["name"] == f"market group for electricity, {voltage}"
        assert purchase["location"] == owner.ecoinvent_to_iam_loc[grid["location"]]
        assert purchase["amount"] == 1
    owner.create_new_markets_high_voltage.assert_called_once()
    owner.create_new_markets_medium_voltage.assert_called_once()
    owner.create_new_markets_low_voltage.assert_called_once()
