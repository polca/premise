"""Native callback atomicity, contract scope, cache invalidation and exported audit."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest

import premise.energy.uvek_heat as native
from premise.core.utils import scenario_metadata
from premise.energy.heat import _update_heat
from premise.energy.uvek_heat_consumers import AUDIT_FIELD
from premise.inventory.inventory_store import (
    create_inventory_store,
    get_scenario_inventory,
)
from premise.validation.sector_validation import validate_sector_contract
from test_uvek_heat_consumers import graph  # noqa: F401


@pytest.fixture
def callback(graph, monkeypatch):  # noqa: F811 - imported pytest fixture
    data, heat, caller, sources = graph
    added = [a for a in data if a.get("premise heat")]
    data = [a for a in data if not a.get("premise heat")]
    heat = {
        **heat,
        "activities": added,
        "suppliers": {},
        "diagnostics": {
            "positive_roles": ["synthetic role"],
            "market_counts": {k: len(v) for k, v in heat["markets"].items()},
        },
    }
    planner = Mock(return_value=heat)
    monkeypatch.setattr(native, "plan_native_heat_markets", planner)
    monkeypatch.setattr(
        native,
        "Geomap",
        lambda model: SimpleNamespace(ecoinvent_to_iam_location=lambda location: "EUR"),
    )
    scenario = {
        "model": "remind",
        "pathway": "test",
        "year": 2050,
        "database": data,
        "iam data": SimpleNamespace(secondary_heat_supply=object()),
        "cache": {"stale row": "old supplier"},
        "index": {"stale": ["old provider"]},
    }
    return scenario, heat, caller, planner


def update(scenario):
    return _update_heat(scenario, "2026", "cutoff", "uvek", "3.10")


def test_callback_commits_verified_copies_and_declares_its_exact_scope(callback):
    scenario, heat, caller, _ = callback
    old = scenario["database"]
    snapshot = copy.deepcopy(old)
    with pytest.warns(UserWarning):
        result = update(scenario)
    assert result is scenario and old == snapshot
    assert len(scenario["database"]) == len(old) + len(heat["activities"])
    changed = next(a for a in scenario["database"] if a["code"] == caller["code"])
    assert len(changed[AUDIT_FIELD]["substitutions"]) == 3
    assert "index" not in scenario and "stale row" not in scenario["cache"]
    phase = validate_sector_contract(scenario, "heat")
    assert phase.valid
    intent = scenario["_validation_intents"]["heat"]
    assert len(intent["allowed_added_keys"]) == len(heat["activities"])
    assert list(caller.values()) == list(snapshot[-1].values())
    assert scenario["native sector audits"]["heat"]["retained_inputs"]
    metadata = scenario_metadata(scenario, "2026", "cutoff")
    assert metadata["scientific_status"] == "screening_only"
    assert (
        metadata["native_sector_audits"]["heat"]
        == scenario["native sector audits"]["heat"]
    )
    metadata["native_sector_audits"]["heat"]["retained_inputs"].clear()
    assert scenario["native sector audits"]["heat"]["retained_inputs"]


def test_failure_during_graph_verification_does_not_commit_partial_changes(callback):
    scenario, heat, _, _ = callback
    old = scenario["database"]
    before = copy.deepcopy(old)
    market = heat["activities"][0]
    market["exchanges"].append(
        {
            "type": "technosphere",
            "name": "Missing",
            "product": "Missing",
            "location": "CH",
            "unit": "kilogram",
            "amount": 1,
            "input": ("absent", "supplier"),
        }
    )
    with pytest.raises(ValueError, match="incorrect supplier link"):
        update(scenario)
    assert scenario["database"] is old and old == before
    assert "heat diagnostics" not in scenario and "native sector audits" not in scenario
    assert scenario["cache"] == {"stale row": "old supplier"}


def test_independent_check_catches_planner_quantity_errors(callback, monkeypatch):
    scenario, _, _, _ = callback
    original = native.plan_native_heat_consumers

    def corrupt(*args, **kwargs):
        plan = original(*args, **kwargs)
        a = plan["replacements"][0]["activity"]
        i = a[AUDIT_FIELD]["substitutions"][0]["index"]
        a["exchanges"][i]["amount"] *= 2
        return plan

    monkeypatch.setattr(native, "plan_native_heat_consumers", corrupt)
    old = scenario["database"]
    with pytest.raises(ValueError, match="caller quantity"):
        update(scenario)
    assert scenario["database"] is old


@pytest.mark.parametrize("context", ["wrong_background", "consequential"])
def test_wrong_context_and_background_fail_before_planning(callback, context):
    scenario, _, _, planner = callback
    if context == "wrong_background":
        scenario["background context"] = {"family": "ecoinvent"}
        with pytest.raises(ValueError, match="background context"):
            update(scenario)
    else:
        with pytest.raises(ValueError, match="UVEK2026"):
            _update_heat(scenario, "2026", "consequential", "uvek", "3.10")
    planner.assert_not_called()


def test_absent_data_records_not_applicable_without_inventory_checkout(callback):
    scenario, _, _, planner = callback
    scenario["iam data"] = SimpleNamespace()
    before = scenario["database"]
    assert update(scenario) is scenario
    planner.assert_not_called()
    assert scenario["database"] is before
    phase = validate_sector_contract(scenario, "heat")
    assert phase.valid and phase.rule_results[0].applicability == "not_applicable"


def test_zero_selected_heat_is_not_a_false_success_or_contract_failure(callback):
    scenario, heat, _, _ = callback
    heat["markets"] = {k: [] for k in heat["markets"]}
    heat["activities"] = heat["deliveries"] = []
    heat["diagnostics"]["positive_roles"] = []
    before = scenario["database"]
    update(scenario)
    assert scenario["database"] is before
    assert (
        validate_sector_contract(scenario, "heat").rule_results[0].applicability
        == "not_applicable"
    )


def test_store_bridge_preserves_source_store_and_literal_audits(callback):
    scenario, _, _, _ = callback
    data = scenario.pop("database")
    store = create_inventory_store(data, backend="compact")
    before = store.materialize()
    # The sector bridge requires an exclusively owned working store. Shared
    # compact forks are deliberately refused by InventoryStore itself.
    scenario["_inventory_store"] = create_inventory_store(before, backend="compact")
    with pytest.warns(UserWarning):
        update(scenario)
    assert store.materialize() == before
    assert any(a.get(AUDIT_FIELD) for a in get_scenario_inventory(scenario))


def test_native_heat_admitted_while_unverified_sectors_stay_guarded():
    from premise import NewDatabase
    from premise.database.background import validate_uvek_scope

    assert validate_uvek_scope("heat") == ["heat"]

    owner = object.__new__(NewDatabase)
    owner.source_family = "uvek"
    owner.prepare_inventories = Mock()
    with pytest.raises(ValueError, match="unsupported sectors"):
        owner.update("cement")
    owner.prepare_inventories.assert_not_called()


def test_numpy_audit_scalars_compare_with_equivalent_json_values():
    from premise.energy.uvek_heat_consumers import _same

    value = np.float32(-2.3)
    assert _same(
        {"amount": value, "input": ("db", "code")},
        {"amount": value.item(), "input": ["db", "code"]},
    )
