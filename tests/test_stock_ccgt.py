"""Independent balance, period, source and service-allocation checks for CCGT."""

import csv
import hashlib
import math

import pytest

from dev.stock_vintage.generate_ccgt_cohorts import (
    VARIABLES,
    centred_additions,
    initial_cohorts,
    linear,
    project_case,
    read_iam,
)
from premise.stock_cohorts import SurvivalLaw


@pytest.fixture
def iam():
    return {
        key: dict.fromkeys([2020, 2025, 2030, 2035], value)
        for key, value in [
            ("capacity", 1),
            ("generation", 1),
            ("additions", 0.05),
            ("lifetime", 35),
        ]
    }


def test_centred_rates_preserve_period_integral_and_reject_gaps():
    annual, periods = centred_additions({2025: 2, 2030: 4}, range(2023, 2031), 100)
    assert annual == {
        **dict.fromkeys(range(2023, 2028), 200),
        **dict.fromkeys(range(2028, 2031), 400),
    }
    assert [p["expanded_period_sum_mw"] for p in periods] == [1000, 2000]
    assert periods[1]["selected_period_years"] == [2028, 2029, 2030]
    with pytest.raises(ValueError, match="cover"):
        centred_additions({2025: 2, 2035: 4}, range(2023, 2031), 1)
    with pytest.raises(ValueError, match="Overlapping"):
        centred_additions({2025: 2, 2026: 4}, range(2023, 2031), 1)
    assert linear({2020: 100, 2025: 200}, 2022) == 140
    with pytest.raises(ValueError, match="extrapolate"):
        linear({2020: 1, 2025: 2}, 2026)


def test_capacity_constraint_observed_service_and_reported_additions_are_distinct(iam):
    stock, generation = {1995: 60, 2015: 40}, {1995: 60000, 2015: 140000}
    law = SurvivalLaw("quartic_capacity", 35, overage_remaining_years=5)
    primary = project_case(
        stock,
        generation,
        iam,
        end_year=2030,
        survival=law,
        mode="stock_target",
        weighting="observed_output",
    )
    observed = primary["annual"][0]
    assert observed["cohort_capacity_mw_ac"] == stock
    assert observed["weights"] == pytest.approx([0.3, 0.7])
    assert observed["mean_age_years"] == pytest.approx(13)
    s = lambda a: max(0, 1 - (a / 43.75) ** 4)
    survivors = 60 * s(28) / s(27) + 40 * s(8) / s(7)
    balance = primary["balances"][0]
    assert balance["gross_additions"] == pytest.approx(100 - survivors)
    assert balance["iam_scaled_reported_additions_mw_ac"] == 5
    assert balance["additions_residual_mw_ac"] == pytest.approx(95 - survivors)
    for row in primary["balances"]:
        assert abs(row["balance_residual"]) < 1e-12
        assert abs(row["capacity_target_residual_mw_ac"]) < 1e-12
    for row in primary["annual"]:
        assert math.fsum(row["cohort_generation_mwh"].values()) == pytest.approx(200000)
        assert sum(row["weights"]) == pytest.approx(1)
        assert max(row["event_years"]) <= row["service_year"]
    additions = project_case(
        stock,
        generation,
        iam,
        end_year=2030,
        survival=law,
        mode="gross_additions",
        weighting="observed_output",
    )
    assert all(r["additions_residual_mw_ac"] == 0 for r in additions["balances"])
    assert additions["balances"][0]["capacity_target_residual_mw_ac"] == pytest.approx(
        survivors - 95
    )
    equal = project_case(
        stock,
        generation,
        iam,
        end_year=2030,
        survival=law,
        mode="stock_target",
        weighting="equal_capacity",
    )
    assert equal["annual"][0]["weights"] == pytest.approx([0.6, 0.4])


def test_output_allocation_does_not_silently_exceed_nameplate(iam):
    iam["generation"] = {2020: 1, 2025: 20, 2030: 50, 2035: 50}
    with pytest.raises(ValueError, match="exceeds 8760"):
        project_case(
            {2000: 100},
            {2000: 800000},
            iam,
            end_year=2030,
            survival=SurvivalLaw("quartic_capacity", 35),
            mode="stock_target",
            weighting="observed_output",
        )


def test_block_cohorts_retain_observed_output_and_reject_double_counting():
    block = {
        "plant_code": 1,
        "unit_code": "CC",
        "cohort_year": 2000,
        "capacity_mw_ac": 10,
        "net_generation_mwh": 10000,
    }
    assert initial_cohorts([block]) == ({2000: 10}, {2000: 10000})
    with pytest.raises(ValueError, match="Duplicate"):
        initial_cohorts([block, block])
    with pytest.raises(ValueError, match="Partial"):
        initial_cohorts([{**block, "cohort_year": 2022}])


def test_iam_reader_rejects_changed_bytes_units_and_duplicate_leaf_rows(tmp_path):
    path = tmp_path / "test.mif"
    header = [
        "Model",
        "Scenario",
        "Region",
        "Variable",
        "Unit",
        "2020",
        "2025",
        "2030",
        "2035",
    ]
    rows = [
        ["REMIND", "test", "USA", var, unit, *([35 if key == "lifetime" else 1] * 4)]
        for key, (var, unit) in VARIABLES.items()
    ]

    def write(records):
        with path.open("w", newline="") as stream:
            writer = csv.writer(stream, delimiter=";")
            writer.writerow(header)
            writer.writerows(records)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    digest = write(rows)
    assert set(read_iam(path, digest, "test", 2030)) == set(VARIABLES)
    with pytest.raises(ValueError, match="manifest"):
        read_iam(path, "0" * 64, "test", 2030)
    digest = write(rows + [rows[0]])
    with pytest.raises(ValueError, match="Duplicate"):
        read_iam(path, digest, "test", 2030)
    rows[0][4] = "MW"
    digest = write(rows)
    with pytest.raises(ValueError, match="context or unit"):
        read_iam(path, digest, "test", 2030)
