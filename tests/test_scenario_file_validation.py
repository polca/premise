"""External scenario validation preserves inputs and diagnostic precedence."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
import yaml
from premise.external_data_validation import check_scenario_data_file


def package(rows, variables=("power",), scenarios=("baseline",)):
    headers = ["scenario", "region", "variables", "unit", "2030", "2040", "2050"]
    config = {"pathways": [{"variable": variable} for variable in variables]}
    resources = {
        "config": SimpleNamespace(raw_read=lambda: yaml.safe_dump(config)),
        "scenario_data": SimpleNamespace(headers=headers, read=lambda: rows),
    }
    return SimpleNamespace(
        descriptor={"scenarios": list(scenarios)},
        get_resource=resources.__getitem__,
    )


def test_valid_scenario_returns_original_package_without_changing_rows():
    rows = [["baseline", "EUR", "power", "kWh", 1, 2, 3]]
    before = deepcopy(rows)
    data = package(rows)

    assert check_scenario_data_file(data, "baseline") is data
    assert rows == before


def test_missing_cells_precede_missing_scenarios_and_variables():
    rows = [["other", "EUR", "power", "kWh", None, 2, None]]
    data = package(rows, variables=("missing",))

    with pytest.raises(ValueError) as error:
        check_scenario_data_file(data, "baseline")

    message = str(error.value)
    assert message.startswith(
        "The following columns in the scenario data contains empty cells.\n"
    )
    assert message.index("2030") < message.index("2050")
    assert "2040" not in message
    assert rows[0][4] is None and rows[0][6] is None


def test_missing_variables_preserve_order_and_repeated_references():
    rows = [["baseline", "EUR", "power", "kWh", 1, 2, "invalid"]]
    data = package(rows, variables=("missing", "missing", "other"))

    with pytest.raises(ValueError) as error:
        check_scenario_data_file(data, "baseline")

    assert str(error.value) == (
        "The following variables from the configuration file cannot be found "
        "in the scenario file.: ['missing', 'missing', 'other']"
    )


def test_non_numeric_time_series_is_rejected_after_variable_validation():
    rows = [["baseline", "EUR", "power", "kWh", 1, 2, "invalid"]]

    with pytest.raises(TypeError, match="All values provided.*must be numerical"):
        check_scenario_data_file(package(rows), "baseline")
