import copy
from types import SimpleNamespace

import numpy as np
import pytest

from premise.mining import load_tailings_config
from premise.inventory_store import IndexedInventoryList
from premise.sector_validation import validate_sector_contract


def market(name="market for direct drive", amount=1.0):
    return {
        "name": name,
        "reference product": "product",
        "location": "CH",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": name,
                "product": "product",
                "location": "CH",
                "unit": "kilogram",
                "type": "production",
                "amount": 1.0,
            },
            {
                "name": "supplier",
                "product": "product",
                "location": "CH",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": amount,
            },
        ],
    }


def scenario_for(dataset, sector="renewable"):
    iam_data = SimpleNamespace()
    if sector == "battery":
        iam_data.battery_mobile_scenarios = object()
        iam_data.battery_stationary_scenarios = None
    return {
        "iam data": iam_data,
        "mapping": {sector: {"technology": [dataset]}},
        "_inventory_working_copy": [dataset],
    }


def issues(phase, rule_id):
    return tuple(issue for issue in phase.issues if issue.rule_id == rule_id)


def test_sector_contract_accepts_a_complete_market_and_records_its_intent():
    dataset = market()
    scenario = scenario_for(dataset)

    phase = validate_sector_contract(scenario, "renewable")

    assert phase.valid
    assert phase.phase_id == "sector:renewable:contract"
    assert scenario["_validation_intents"]["renewable"]["expected_match_count"] == 1
    assert phase.to_dict() in scenario["_validation_phase_results"]


@pytest.mark.parametrize(
    ("sector", "mutation", "rule_id"),
    [
        (
            "renewable",
            lambda dataset: dataset["exchanges"][0].update(amount=float("nan")),
            "METHOD.RENEWABLE.PHYSICAL_BOUNDS",
        ),
        (
            "battery",
            lambda dataset: dataset["exchanges"][1].update(amount=0.8),
            "METHOD.BATTERY.MARKET_COMPOSITION",
        ),
    ],
)
def test_sector_contract_rejects_invalid_target_values(sector, mutation, rule_id):
    dataset = market()
    mutation(dataset)

    phase = validate_sector_contract(scenario_for(dataset, sector), sector)

    assert issues(phase, rule_id)


def test_applicable_sector_with_no_targets_is_rejected_explicitly():
    scenario = {
        "iam data": SimpleNamespace(),
        "mapping": {},
        "_inventory_working_copy": [],
    }

    phase = validate_sector_contract(scenario, "renewable")

    assert issues(phase, "METHOD.RENEWABLE.TARGET_COVERAGE")
    result = phase.rule_results[0]
    assert result.expected == ">= 1 target"
    assert result.actual == 0
    assert result.checked_object_count == 0


def test_unavailable_sector_data_is_reported_as_not_applicable():
    scenario = {
        "iam data": SimpleNamespace(battery_mobile_scenarios=None),
        "mapping": {},
        "_inventory_working_copy": [],
    }

    phase = validate_sector_contract(scenario, "battery")

    assert phase.valid
    assert phase.rule_results[0].applicability == "not_applicable"
    assert phase.rule_results[0].checked_object_count == 0


def test_tailings_market_accepts_the_waste_negative_unit_convention():
    dataset = market(
        name="market for sulfidic tailings, from zinc mine operation",
        amount=-1.0,
    )
    scenario = scenario_for(dataset, sector="mining")

    phase = validate_sector_contract(scenario, "mining")

    assert phase.valid
    assert not issues(phase, "METHOD.MINING.MARKET_COMPOSITION")


@pytest.mark.parametrize("model", ["image", "remind", "tiam-ucl"])
def test_tailings_configuration_has_complete_mean_share_vectors(model):
    shares = load_tailings_config(model)["mean"].sum(dim="technology")

    assert float(abs(shares - 1.0).max()) == pytest.approx(0.0, abs=1e-12)


def test_contract_validation_is_read_only():
    dataset = market()
    scenario = scenario_for(dataset)
    before = copy.deepcopy(dataset)

    validate_sector_contract(scenario, "renewable")

    assert dataset == before


def test_structural_additions_are_declared_in_the_sector_intent():
    original = market()
    added = market(name="new regional market")
    additions = {}
    database = IndexedInventoryList([original])
    database.track_validation_additions(additions)
    database.append(added)
    scenario = scenario_for(original)
    scenario["_inventory_working_copy"] = database
    scenario["cache"] = {
        "__premise_validation_added_targets_v1__": additions,
    }

    validate_sector_contract(scenario, "renewable")

    intent = scenario["_validation_intents"]["renewable"]
    assert ["new regional market", "product", "CH"] in intent["allowed_added_keys"]


def test_scalar_numpy_amount_is_accepted_without_normalization():
    dataset = market()
    dataset["exchanges"][1]["amount"] = np.array(1.0)
    before = dataset["exchanges"][1]["amount"]

    phase = validate_sector_contract(scenario_for(dataset), "renewable")

    assert phase.valid
    assert dataset["exchanges"][1]["amount"] is before
