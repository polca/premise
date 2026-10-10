"""Vehicle observations, territorial balance and component attribution checks."""

import csv
import hashlib
import math

import pytest

from dev.stock_vintage.generate_vehicle_cohorts import (
    initial_cohorts,
    project_case,
    read_iam,
    variables,
)
from premise.stock_cohorts import AnnualVehicleSurvival


def observation(group="passenger_bev"):
    return {
        "group": group,
        "observation_year": 2022,
        "unit": "vehicle",
        "cohorts": [
            {"cohort_year": 2000, "stock": 1},
            {"cohort_year": 2012, "stock": 3},
            {"cohort_year": 2022, "stock": 6},
        ],
        "unknown_stock": 2,
        "total_stock": 12,
    }


def iam():
    return {
        "stock": {2020: 1, 2025: 1, 2030: 1},
        "sales": {2020: 0, 2025: 0.5, 2030: 1},
        "service": {2020: 10, 2025: 20, 2030: 40},
    }


def weights(record):
    return dict(zip(record["event_years"], record["weights"]))


def test_bev_scope_reconciles_exclusions_without_dating_conversion_batteries():
    stock, selection = initial_cohorts(observation())
    assert stock == {2012: 3, 2022: 6}
    assert selection["excluded_pre2010_bev_vehicles"] == 1
    assert selection["source_unknown_vehicles"] == 2
    assert selection["selected_share"] == 0.75
    oldest, _ = initial_cohorts(observation(), unknown_policy="oldest")
    newest, _ = initial_cohorts(observation(), unknown_policy="newest")
    assert oldest == {2012: 5, 2022: 6}
    assert newest == {2012: 3, 2022: 8}
    assert math.fsum(oldest.values()) == math.fsum(newest.values()) == 11


def test_truck_open_bin_and_unknown_count_preserve_the_entire_population():
    row = observation("heavy_trucks_32_40t")
    row.update(
        total_stock=13,
        unresolved_bins=[
            {
                "label": "Before 1980",
                "start_year": None,
                "end_year": 1979,
                "stock": 1,
            }
        ],
    )
    stock, selection = initial_cohorts(row)
    assert math.fsum(stock.values()) == pytest.approx(13)
    assert stock[1950] == pytest.approx((1 / 30) * (13 / 11))
    assert stock[1979] == pytest.approx(stock[1950])
    assert selection["selected_share"] == pytest.approx(1)
    assert selection["open_bin_vehicles"] == 1
    assert selection["unknown_policy"] == "proportional"
    with pytest.raises(ValueError, match="reconcile"):
        initial_cohorts({**row, "total_stock": 14})


def test_reported_sales_are_annual_entries_not_a_centred_five_year_rate():
    case = project_case(
        {2022: 100},
        iam(),
        end_year=2025,
        survival=AnnualVehicleSurvival(20),
        mode="gross_additions",
    )
    assert [b["gross_additions"] for b in case["balances"]] == pytest.approx(
        [30, 40, 50]
    )
    assert [b["sales_residual_vehicles"] for b in case["balances"]] == [0, 0, 0]
    assert case["balances"][-1]["stock_target_residual_vehicles"] > 100
    assert all(abs(b["balance_residual"]) < 1e-9 for b in case["balances"])
    constrained = project_case(
        {2022: 100},
        iam(),
        end_year=2025,
        survival=AnnualVehicleSurvival(20),
    )
    assert all(
        abs(b["stock_target_residual_vehicles"]) < 1e-9 for b in constrained["balances"]
    )
    assert constrained["balances"][-1]["sales_residual_vehicles"] < 0


def test_territorial_contraction_does_not_scrap_the_reference_serving_fleet():
    base = iam()
    decline = {**base, "stock": {2020: 1, 2025: 0.1, 2030: 0.01}}
    options = dict(
        end_year=2025, survival=AnnualVehicleSurvival(20), battery_interval=10
    )
    stable = project_case({2010: 10}, base, **options)
    falling = project_case({2010: 10}, decline, **options)
    assert any(b["early_exits"] > 0 for b in falling["balances"])
    assert falling["parent_retirement"][0] == stable["parent_retirement"][0]
    assert falling["battery_retirement"][0] == stable["battery_retirement"][0]
    assert all(y > 2022 for y in falling["parent_retirement"][0]["event_years"])
    delayed = project_case({2010: 10}, base, disposal_delay=5, **options)
    for plain, later in zip(stable["parent_retirement"], delayed["parent_retirement"]):
        assert later["event_years"] == [y + 5 for y in plain["event_years"]]
        assert later["weights"] == plain["weights"]


def test_service_index_does_not_convert_pkm_to_km_or_change_common_amortisation():
    options = dict(end_year=2025, survival=AnnualVehicleSurvival(20))
    case = project_case({2012: 50, 2022: 50}, iam(), **options)
    assert weights(case["annual"][0]) == {2012: 0.5, 2022: 0.5}
    assert case["annual"][0]["total_service_index"] == pytest.approx(100)
    assert case["annual"][-1]["total_service_index"] == pytest.approx(100 * 20 / 14)
    half = project_case(
        {2012: 50, 2022: 50}, iam(), weighting="half_year_entrants", **options
    )
    assert weights(half["annual"][0]) == pytest.approx({2012: 2 / 3, 2022: 1 / 3})
    assert half["annual"][0]["total_service_index"] == pytest.approx(100)
    assert case["allocation_basis"] == "common_amortisation"
    assert case["exchange_amount_changed"] is False


def test_active_battery_profiles_retain_original_equivalent_pack_burdens():
    case = project_case(
        {2005: 1, 2020: 3},
        iam(),
        end_year=2025,
        survival=AnnualVehicleSurvival(20),
        battery_interval=20 * 2 / 3,
    )
    # The first parent's replacement milestone is 2018.333..., rounded to 2019.
    assert weights(case["battery_manufacture"][0]) == pytest.approx(
        {2019: 0.25, 2020: 0.75}
    )
    assert weights(case["annual"][0]) == pytest.approx({2005: 0.25, 2020: 0.75})
    q = 262 * 1.5 / 150000
    for record in case["battery_manufacture"] + case["battery_retirement"]:
        assert math.fsum(q * w for w in record["weights"]) == pytest.approx(0.00262)
    for row in case["battery_joint_events"]:
        assert math.fsum(e["weight"] for e in row["events"]) == pytest.approx(1)
        assert all(
            e["manufacture_year"] <= row["service_year"] < e["retirement_year"]
            for e in row["events"]
        )


def write_iam(path, *, change=None):
    rows = []
    for key, (variable, unit) in variables("passenger_bev").items():
        rows.append(
            {
                "Model": "REMIND",
                "Scenario": "SSP2-NPi2025",
                "Region": "EUR",
                "Variable": variable,
                "Unit": unit,
                "2020": "0" if key == "sales" else "1",
                "2025": "2",
            }
        )
    if change == "duplicate":
        rows.append(dict(rows[0]))
    elif change == "missing":
        rows[0]["2025"] = ""
    elif change == "units":
        rows[0]["Unit"] = "vehicle"
    elif change == "aggregate":
        rows[0]["Variable"] = "Stock|Transport|Pass"
    elif change == "region":
        rows[0]["Region"] = "NEU"
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_vehicle_reader_preserves_reported_zero_and_rejects_changed_source(tmp_path):
    path = tmp_path / "scenario.mif"
    sha = write_iam(path)
    result = read_iam(path, sha, "SSP2-NPi2025", "passenger_bev", 2025)
    assert result["sales"][2020] == 0
    with pytest.raises(ValueError, match="manifest"):
        read_iam(path, "changed", "SSP2-NPi2025", "passenger_bev", 2025)


@pytest.mark.parametrize(
    "change", ["duplicate", "missing", "units", "aggregate", "region"]
)
def test_vehicle_reader_rejects_ambiguous_or_incomplete_scenario_data(tmp_path, change):
    path = tmp_path / "scenario.mif"
    sha = write_iam(path, change=change)
    with pytest.raises(ValueError):
        read_iam(path, sha, "SSP2-NPi2025", "passenger_bev", 2025)
