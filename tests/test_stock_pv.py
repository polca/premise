"""Observed zero-service stock, parent/component chronology and PV balance."""

import math

import pytest

from dev.stock_vintage.generate_pv_cohorts import component_case, initial_cohorts
from dev.stock_vintage.export_pv_pilot import mix_records
from premise.stock_cohorts import SurvivalLaw


def test_panel_maintenance_and_handling_keep_distinct_quantities_and_dates():
    # 103 panel equivalents contain 100 installed, one handling loss and two
    # lifetime maintenance replacements. Keep the already-amortised total.
    parent = [{"service_year": 2005, "event_years": [2000], "weights": [1]}]
    end = [{"service_year": 2005, "event_years": [2030], "weights": [1]}]
    instant = [{"service_year": 2005, "event_years": [2005], "weights": [1]}]
    manufacture = mix_records((101 / 103, parent), (2 / 103, instant))[0]
    disposal = mix_records((100 / 103, end), (2 / 103, instant), (1 / 103, parent))[0]
    assert dict(
        zip(manufacture["event_years"], [w * 103 for w in manufacture["weights"]])
    ) == pytest.approx({2000: 101, 2005: 2})
    assert dict(
        zip(disposal["event_years"], [w * 103 for w in disposal["weights"]])
    ) == pytest.approx({2000: 1, 2005: 2, 2030: 100})
    assert sum(manufacture["weights"]) == pytest.approx(sum(disposal["weights"]))
    # Explicit endpoints of unresolved factory/end-of-life polymer proportions
    # must not introduce an unexplained intermediate date or drop signed mass.
    assert mix_records((0, [disposal]), (1, [manufacture])) == [manufacture]
    assert mix_records((1, [disposal]), (0, [manufacture])) == [disposal]
    with pytest.raises(ValueError, match="fractions"):
        mix_records((0.8, parent), (0.1, end))


def test_pv_observations_retain_zero_output_stock_and_distinct_capacity_units():
    rows = [
        {
            "plant_code": 1,
            "cohort_year": 2010,
            "selected_capacity_mw_ac": 10,
            "capacity_mw_dc": 12,
            "net_pv_generation_mwh": 15000,
        },
        {
            "plant_code": 2,
            "cohort_year": 2015,
            "selected_capacity_mw_ac": 5,
            "capacity_mw_dc": 7,
            "net_pv_generation_mwh": 0,
        },
    ]
    assert initial_cohorts(rows) == (
        {2010: 10, 2015: 5},
        {2010: 12, 2015: 7},
        {2010: 15000, 2015: 0},
    )
    with pytest.raises(ValueError, match="Duplicate"):
        initial_cohorts(rows + rows)
    with pytest.raises(ValueError, match="pre-reference"):
        initial_cohorts([{**rows[0], "cohort_year": 2022}])
    with pytest.raises(ValueError, match="annual AC bound"):
        initial_cohorts([{**rows[0], "net_pv_generation_mwh": -1}])


def test_pv_parent_stock_and_inverter_generations_are_distinct():
    iam = {
        k: dict.fromkeys([2020, 2025, 2030, 2035], v)
        for k, v in [("capacity", 1), ("generation", 1), ("additions", 0)]
    }
    case = component_case(
        {2005: 1, 2015: 3},
        {2005: 1000, 2015: 3000},
        iam,
        end_year=2030,
        survival=SurvivalLaw("fixed", 30),
    )
    assert case["annual"][0]["weights"] == pytest.approx([0.25, 0.75])
    manufacture = case["inverter_manufacture"][0]
    assert dict(
        zip(manufacture["event_years"], manufacture["weights"])
    ) == pytest.approx({2015: 0.75, 2020: 0.25})
    disposal = case["inverter_retirement"][0]
    assert dict(zip(disposal["event_years"], disposal["weights"])) == pytest.approx(
        {2030: 0.75, 2035: 0.25}
    )
    for row in case["balances"]:
        assert row["balance_residual"] == pytest.approx(0)
        assert row["capacity_target_residual_mw_ac"] == pytest.approx(0)
    for row in case["inverter_joint_events"]:
        assert math.fsum(e["weight"] for e in row["events"]) == pytest.approx(1)
        assert all(
            e["manufacture_year"] <= row["service_year"] < e["retirement_year"]
            for e in row["events"]
        )
    assert case["exchange_amount_changed"] is False


def test_pv_forced_operating_exit_is_not_physical_component_disposal():
    iam = {
        k: {2020: 1, 2025: 0.5, 2030: 0.2, 2035: 0.2}
        for k in ["capacity", "generation", "additions"]
    }
    with pytest.raises(ValueError, match="service exits"):
        component_case(
            {2015: 1},
            {2015: 1000},
            iam,
            end_year=2030,
            survival=SurvivalLaw("fixed", 30),
        )
