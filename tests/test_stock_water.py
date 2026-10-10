"""Analytical tests of binned survivors, missing mass and conditional disposal."""

import math
import pytest

from dev.stock_vintage.generate_water_cohorts import (
    BIN_RANGES,
    OPEN_BIN,
    annualise,
    project,
)


def observation():
    return {
        "observation_year": 2022,
        "cohorts": [
            {"construction_bin": key, "stock": value}
            for key, value in zip([*BIN_RANGES, OPEN_BIN], [3, 10, 10, 30, 30, 90])
        ],
        "unknown_stock": 17.3,
        "total_stock": 190.3,
        "total_definition": "mutually exclusive construction bins plus unknown",
    }


def test_bins_and_unknown_mass_survive_annualisation_without_survival_reweighting():
    stock, audit = annualise(observation())
    assert set(stock) == set(range(1850, 2023))
    assert list(stock.values()) == pytest.approx([1.1] * 173)
    assert math.fsum(stock.values()) == pytest.approx(190.3)
    assert audit["known_stock"] == 173
    assert math.fsum(audit["unknown_assignment"].values()) == pytest.approx(17.3)
    assert all(
        r["source_stock"] == r["assigned_stock"] for r in audit["bin_reconciliation"]
    )
    young, _ = annualise(observation(), unknown="newest")
    old, _ = annualise(observation(), unknown="oldest")
    assert young[2022] == pytest.approx(18.3)
    assert old[1850] == pytest.approx(18.3)
    assert young[1850] == old[2022] == 1


@pytest.mark.parametrize(
    "mode,first,last", [("oldest", 1850, 2020), ("newest", 1939, 2022)]
)
def test_bin_extremes_do_not_change_total(mode, first, last):
    stock, audit = annualise(observation(), within_bin=mode)
    assert len(stock) == 6
    assert min(stock) == first and max(stock) == last
    assert math.fsum(stock.values()) == pytest.approx(audit["total_stock"])


def test_unknown_changed_bins_and_inconsistent_totals_fail():
    data = observation()
    data["cohorts"][0]["construction_bin"] = "new age definition"
    with pytest.raises(ValueError, match="bins changed"):
        annualise(data)
    data = observation()
    data["total_stock"] += 1
    with pytest.raises(ValueError, match="reconcile"):
        annualise(data)


def test_stock_balance_and_conditional_retirement_are_independent_of_amortisation():
    stock = {1950: 60, 2020: 40}
    result = project(stock, end_year=2025, lifetime=70, shape=3)
    assert result["annual"][0]["weights"] == pytest.approx([0.6, 0.4])
    scale = 70 / math.gamma(4 / 3)
    s = lambda a: math.exp(-((a / scale) ** 3))
    expected_survivors = 60 * s(73) / s(72) + 40 * s(3) / s(2)
    assert result["balances"][0]["gross_additions"] == pytest.approx(
        100 - expected_survivors
    )
    assert result["retirement"][0]["weights"][0] == pytest.approx(
        1 - expected_survivors / 100
    )
    for row in result["annual"]:
        assert row["total_stock"] == pytest.approx(100)
        assert row["service_proxy_total"] == pytest.approx(100)
    for row in result["retirement"]:
        assert min(row["event_years"]) > row["service_year"]
        assert math.fsum(row["weights"]) == pytest.approx(1)
        assert row["tail_mass_folded_into_last_year"] <= 1e-12
    assert all(abs(r["balance_residual"]) < 1e-12 for r in result["balances"])
    weighted = project(stock, end_year=2025, utilisation_age_scale=50)
    assert weighted["annual"][0]["weights"][0] < 0.6
    growing = project(stock, end_year=2025, growth=0.01)
    assert growing["annual"][-1]["total_stock"] == pytest.approx(100 * 1.01**3)
