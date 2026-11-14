import pandas as pd
import pytest

from dev.generate_trails_empirical_profiles import vehicle_survival_profiles
from premise.temporal_evidence import (
    decode_jsonstat,
    eia_operating_fleet_profiles,
    eia_retirement_age_profiles,
    eurostat_age_band_profile,
)


def eurostat_fixture():
    return {
        "id": ["age", "geo", "time"],
        "size": [5, 3, 1],
        "dimension": {
            "age": {
                "category": {
                    "index": {
                        "Y_LT2": 0,
                        "Y2-5": 1,
                        "Y5-10": 2,
                        "Y10-20": 3,
                        "Y_GT20": 4,
                    }
                }
            },
            "geo": {"category": {"index": {"AT": 0, "BE": 1, "BG": 2}}},
            "time": {"category": {"index": {"2024": 0}}},
        },
        # AT and BE have all five bands; BG is missing the final band.
        "value": {
            **{str(age * 3): age + 1 for age in range(5)},
            **{str(age * 3 + 1): 2 * (age + 1) for age in range(5)},
            **{str(age * 3 + 2): 100 for age in range(4)},
        },
    }


def test_jsonstat_decode_and_eurostat_aggregation_use_common_country_coverage():
    document = eurostat_fixture()
    assert len(decode_jsonstat(document)) == 14
    result = eurostat_age_band_profile(document, countries={"AT", "BE", "BG"})
    assert result["countries"] == ["AT", "BE"]
    assert result["country_count"] == 2
    assert result["observations"] == 45
    assert [band["count"] for band in result["bands"]] == [3, 6, 9, 12, 15]
    assert sum(band["weight"] for band in result["bands"]) == pytest.approx(1)
    assert result["bands"][-1]["maximum_years"] is None


def test_eia_operating_profiles_are_capacity_weighted_and_status_filtered():
    frame = pd.DataFrame(
        [
            ["OP", "Wind", 2020, 10],
            ["OP", "Wind", 2024, 30],
            ["SB", "Wind", 2000, 1000],
            ["OP", "Solar", 2024, 5],
        ],
        columns=[
            "Status",
            "Technology",
            "Operating Year",
            "Summer Capacity (MW)",
        ],
    )
    profiles = eia_operating_fleet_profiles(frame, reference_year=2025)
    assert profiles["Wind"]["offsets_years"] == [-5, -1]
    assert profiles["Wind"]["weights"] == [0.25, 0.75]
    assert profiles["Wind"]["mean_age_years"] == 2
    assert profiles["Wind"]["weight_total_mw"] == 40


def test_eia_retirement_profiles_exclude_precoverage_and_invalid_rows():
    frame = pd.DataFrame(
        [
            ["RE", "Coal", 1980, 2020, 100],
            ["RE", "Coal", 1990, 2000, 1000],
            ["RE", "Coal", 2020, 2019, 1000],
            ["CN", "Coal", 1980, 2020, 1000],
        ],
        columns=[
            "Status",
            "Technology",
            "Operating Year",
            "Retirement Year",
            "Summer Capacity (MW)",
        ],
    )
    profiles = eia_retirement_age_profiles(frame)
    assert profiles["Coal"]["offsets_years"] == [-40]
    assert profiles["Coal"]["weights"] == [1]
    assert profiles["Coal"]["unit_count"] == 1


def test_vehicle_survival_profiles_preserve_the_censored_tail():
    profiles = vehicle_survival_profiles()
    passenger_cars = profiles["eu-passenger-car-survival-uk-mot"]
    parameters = passenger_cars["distribution"]["parameters"]

    assert passenger_cars["quantity"] == "service_life"
    assert parameters["ages_years"] == list(range(1, 20))
    assert parameters["survival_probabilities"][-1] == 0.04
    assert parameters["right_censored_after_years"] == 19
    assert "service_life_years" not in passenger_cars
