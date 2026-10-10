import math

import pytest

from premise.stock_cohorts import (
    AnnualVehicleSurvival,
    SurvivalLaw,
    active_component_records,
    evolve_stock,
    retirement_record,
    service_weights,
)


def test_vehicle_kernel_uses_included_terminal_age_and_differs_from_power_law():
    law = AnnualVehicleSurvival(20)
    assert law.conditional(0, 0) == 1
    assert law.conditional(0, 20) == pytest.approx(1 - (39 / 40) ** 4)
    assert law.conditional(0, 21) == 0
    assert law.conditional(20, 1) == 0
    assert SurvivalLaw("quartic_capacity", 20).conditional(20, 1) > 0
    assert law.conditional(10, 1) == pytest.approx(
        (1 - (21 / 40) ** 4) / (1 - (19 / 40) ** 4)
    )


def test_vehicle_overage_survivors_need_an_explicit_remaining_life_proxy():
    with pytest.raises(ValueError, match="exceeds"):
        AnnualVehicleSurvival(15).conditional(16, 0)
    law = AnnualVehicleSurvival(15, overage_remaining_years=3)
    assert law.conditional(40, 1) == pytest.approx(math.exp(-1 / 3))
    p = evolve_stock({1980: 10}, 2022, {2023: 0}, law, mode="gross_additions")
    assert p.stocks[2022] == {1980: 10}
    assert p.stocks[2023][1980] == pytest.approx(10 * math.exp(-1 / 3))
    record = retirement_record(p, 2022, {1980: 1})
    assert min(record["event_years"]) == 2023
    assert sum(record["weights"]) == pytest.approx(1)


@pytest.mark.parametrize("life", [0, -1, 15.5, float("nan"), True])
def test_vehicle_kernel_rejects_invalid_service_life(life):
    with pytest.raises(ValueError):
        AnnualVehicleSurvival(life)


def component_projection(survival=None):
    return evolve_stock(
        {2000: 1},
        2000,
        {year: 0 for year in range(2001, 2030)},
        survival or SurvivalLaw("fixed", 30),
        mode="gross_additions",
    )


@pytest.mark.parametrize(
    "year,birth,end",
    [
        (2005, 2000, 2015),
        (2015, 2015, 2030),
        (2029, 2015, 2030),
    ],
)
def test_active_component_dates_current_generation(year, birth, end):
    result = active_component_records(
        component_projection(),
        year,
        {2000: 1},
        interval_years=15,
    )
    assert result["manufacture"]["event_years"] == [birth]
    assert result["retirement"]["event_years"] == [end]
    assert result["manufacture"]["weights"] == [1]
    assert result["retirement"]["weights"] == [1]
    assert result["exchange_amount_changed"] is False


def test_common_amortisation_recovers_two_inverters_over_thirty_services():
    # Two equivalent inverters already amortised over thirty equal annual
    # services: reapplying a one-half factor would undercount both components.
    p = component_projection()
    production, disposal = {}, {}
    coefficient = 2 / 30
    for year in range(2000, 2030):
        result = active_component_records(p, year, {2000: 1}, interval_years=15)
        for role, totals in [("manufacture", production), ("retirement", disposal)]:
            record = result[role]
            for event, weight in zip(record["event_years"], record["weights"]):
                totals[event] = totals.get(event, 0) + coefficient * weight
        assert all(
            row["manufacture_year"] <= year < row["retirement_year"]
            for row in result["joint_events"]
        )
    assert production == pytest.approx({2000: 1, 2015: 1})
    assert disposal == pytest.approx({2015: 1, 2030: 1})


def test_different_parent_cohorts_can_share_active_component_dates():
    p = evolve_stock(
        {2000: 2, 2015: 3},
        2020,
        {},
        SurvivalLaw("fixed", 30),
        mode="gross_additions",
    )
    result = active_component_records(
        p, 2020, {2000: 0.2, 2015: 0.8}, interval_years=15
    )
    assert result["manufacture"]["event_years"] == [2015]
    assert result["manufacture"]["weights"] == pytest.approx([1])
    assert result["retirement"]["event_years"] == [2030]
    assert result["retirement"]["weights"] == pytest.approx([1])
    assert {row["component_generation"] for row in result["joint_events"]} == {0, 1}


def test_component_retires_at_parent_failure_or_next_replacement():
    p = component_projection(SurvivalLaw("weibull", 30, 3))
    result = active_component_records(p, 2012, {2000: 1}, interval_years=15)
    scale = 30 / math.gamma(1 + 1 / 3)
    r13 = math.exp(-((13 / scale) ** 3 - (12 / scale) ** 3))
    r14 = math.exp(-((14 / scale) ** 3 - (12 / scale) ** 3))
    record = result["retirement"]
    assert dict(zip(record["event_years"], record["weights"])) == pytest.approx(
        {2013: 1 - r13, 2014: r13 - r14, 2015: r14}
    )
    assert result["manufacture"]["event_years"] == [2000]


@pytest.mark.parametrize(
    "year,birth,end",
    [
        (2008, 2000, 2009),
        (2009, 2009, 2017),
        (2017, 2017, 2025),
        (2025, 2025, 2030),
    ],
)
def test_fractional_component_milestones_use_first_following_year(year, birth, end):
    result = active_component_records(
        component_projection(),
        year,
        {2000: 1},
        interval_years=25 / 3,
    )
    assert result["manufacture"]["event_years"] == [birth]
    assert result["retirement"]["event_years"] == [end]


@pytest.mark.parametrize("interval", [0, 0.5, -1, float("nan"), True])
def test_invalid_component_interval_rejected(interval):
    with pytest.raises(ValueError):
        active_component_records(
            component_projection(),
            2010,
            {2000: 1},
            interval_years=interval,
        )


def test_component_disposal_rejects_territorial_parent_exit():
    p = evolve_stock(
        {2000: 1},
        2020,
        {2021: 0.5},
        SurvivalLaw("fixed", 30),
        mode="stock_target",
        early_exit_kind="territorial_exit",
    )
    with pytest.raises(ValueError, match="Territorial exits"):
        active_component_records(p, 2020, {2000: 1}, interval_years=15)


def test_observed_initial_stock_is_not_survival_weighted_twice():
    law = SurvivalLaw("weibull", 15, 3)
    p = evolve_stock({2000: 100}, 2020, {2021: 0}, law, mode="gross_additions")
    assert p.stocks[2020] == {2000: 100}
    scale = 15 / math.gamma(1 + 1 / 3)
    expected = 100 * math.exp(-((21 / scale) ** 3 - (20 / scale) ** 3))
    assert p.stocks[2021][2000] == pytest.approx(expected)
    assert p.balances[0]["natural_retirements"] == pytest.approx(100 - expected)


def test_stationary_fixed_life_has_uniform_stock_not_failure_density():
    p = evolve_stock(
        {year: 1 for year in range(2016, 2021)},
        2020,
        {2021: 1, 2022: 1},
        SurvivalLaw("fixed", 5),
        mode="gross_additions",
    )
    for year, stock in p.stocks.items():
        assert stock == {cohort: 1 for cohort in range(year - 4, year + 1)}
        assert service_weights(stock)["weights"] == {cohort: 0.2 for cohort in stock}
    assert all(row["balance_residual"] == 0 for row in p.balances)


def test_stock_targets_reconcile_additions_and_explicit_early_retirements():
    law = SurvivalLaw("fixed", 10)
    p = evolve_stock(
        {2019: 40, 2020: 60},
        2020,
        {2021: 120, 2022: 60},
        law,
        mode="stock_target",
        early_exit_kind="physical_retirement",
    )
    assert p.stocks[2021] == {2019: 40, 2020: 60, 2021: 20}
    assert p.stocks[2022] == {2019: 20, 2020: 30, 2021: 10}
    assert p.balances[0]["gross_additions"] == 20
    assert p.balances[1]["early_exits"] == 60
    record = retirement_record(p, 2020, {2019: 0.4, 2020: 0.6})
    # Half of each initial cohort exits in 2022; the other half reaches its
    # fixed retirement year. Additions after the service year do not enter.
    assert dict(zip(record["event_years"], record["weights"])) == pytest.approx(
        {2022: 0.5, 2029: 0.2, 2030: 0.3}
    )


def test_future_service_weights_can_differ_without_changing_amortisation():
    output = service_weights({2010: 10, 2020: 10}, {2010: 1, 2020: 3})
    assert output["service_by_cohort"] == {2010: 10, 2020: 30}
    assert output["weights"] == {2010: 0.25, 2020: 0.75}
    assert output["exchange_amount_changed"] is False
    assert output["allocation_basis"] == "common_amortisation"


def test_exponential_remaining_life_is_memoryless_for_old_and_new_survivors():
    p = evolve_stock(
        {1950: 1, 2020: 1},
        2020,
        {},
        SurvivalLaw("weibull", 10, 1),
        mode="gross_additions",
    )
    old = retirement_record(p, 2020, {1950: 1})
    young = retirement_record(p, 2020, {2020: 1})
    assert old["event_years"] == young["event_years"]
    assert old["weights"] == pytest.approx(young["weights"])
    assert old["weights"][0] == pytest.approx(1 - math.exp(-0.1))
    assert sum(old["weights"]) == pytest.approx(1)
    assert 0 <= old["tail_mass_folded_into_last_year"] <= 1e-12
    assert min(old["event_years"]) > 2020


def test_older_than_mean_is_not_retired_before_current_service():
    p = evolve_stock(
        {1980: 1}, 2020, {}, SurvivalLaw("weibull", 15, 3), mode="gross_additions"
    )
    record = retirement_record(p, 2020, {1980: 1})
    assert min(record["event_years"]) == 2021
    assert sum(record["weights"]) == pytest.approx(1)


def test_retirement_of_new_cohort_can_start_from_zero_initial_stock():
    p = evolve_stock(
        {}, 2020, {2021: 10}, SurvivalLaw("fixed", 5), mode="gross_additions"
    )
    record = retirement_record(p, 2021, {2021: 1})
    assert record["event_years"] == [2026]
    assert record["weights"] == [1]


def test_fixed_survival_does_not_create_roundoff_retirements_before_first_exit():
    p = evolve_stock(
        {2005: 3, 2015: 1},
        2020,
        {2021: 5, 2022: 6},
        SurvivalLaw("fixed", 20),
        mode="stock_target",
    )
    record = retirement_record(p, 2022, service_weights(p.stocks[2022])["weights"])
    assert record["event_years"] == [2025, 2035, 2041, 2042]
    assert record["weights"] == pytest.approx([0.5, 1 / 6, 1 / 6, 1 / 6])


def test_territorial_exit_is_not_silently_used_as_physical_disposal():
    p = evolve_stock(
        {2020: 10},
        2020,
        {2021: 5},
        SurvivalLaw("fixed", 10),
        mode="stock_target",
        early_exit_kind="territorial_exit",
    )
    with pytest.raises(ValueError, match="Territorial exits"):
        retirement_record(p, 2020, {2020: 1})


@pytest.mark.parametrize(
    "case",
    [
        "missing_year",
        "future_cohort",
        "negative",
        "impossible_fixed",
        "unexplained_exit",
    ],
)
def test_invalid_cohort_reconstructions_fail(case):
    initial, inputs, law, mode = (
        {2020: 1},
        {2021: 1},
        SurvivalLaw("fixed", 5),
        "gross_additions",
    )
    if case == "missing_year":
        inputs = {2022: 1}
    elif case == "future_cohort":
        initial = {2021: 1}
    elif case == "negative":
        inputs = {2021: -1}
    elif case == "impossible_fixed":
        initial = {2010: 1}
    else:
        inputs, mode = {2021: 0}, "stock_target"
    with pytest.raises(ValueError):
        evolve_stock(initial, 2020, inputs, law, mode=mode)


def test_unresolved_retirement_tail_cannot_be_normalised_away():
    p = evolve_stock(
        {2020: 10}, 2020, {}, SurvivalLaw("weibull", 100, 1), mode="gross_additions"
    )
    with pytest.raises(ValueError, match="excessive tail"):
        retirement_record(p, 2020, {2020: 1}, max_years=10)


def test_quartic_capacity_is_conditional_and_retains_declared_overage_survivors():
    law = SurvivalLaw("quartic_capacity", 35, overage_remaining_years=5)
    assert law.conditional(0, 0) == 1
    assert law.conditional(20, 10) == pytest.approx(
        (1 - (30 / 43.75) ** 4) / (1 - (20 / 43.75) ** 4)
    )
    assert law.conditional(43, 1) == 0
    assert law.conditional(47, 1) == pytest.approx(math.exp(-1 / 5))
    with pytest.raises(ValueError, match="exceeds quartic"):
        SurvivalLaw("quartic_capacity", 35).conditional(47, 0)
    p = evolve_stock({1975: 290}, 2022, {2023: 0}, law, mode="gross_additions")
    assert p.stocks[2022][1975] == 290
    assert p.stocks[2023][1975] == pytest.approx(290 * math.exp(-1 / 5))


def test_operating_capacity_exit_is_not_a_physical_disposal_date():
    p = evolve_stock(
        {2020: 10},
        2022,
        {2023: 1},
        SurvivalLaw("weibull", 35, 4),
        mode="stock_target",
        early_exit_kind="service_exit",
    )
    assert p.balances[0]["early_exits"] > 0
    with pytest.raises(ValueError, match="service exits"):
        retirement_record(p, 2022, {2020: 1})
