"""Resource validation preserves error priority and diagnoses inconsistent packages."""

import pytest
from datapackage import Package
from datapackage.exceptions import ValidationError
from premise.external_data_validation import check_datapackage


def package(names, version="3.12"):
    return Package(
        {
            "ecoinvent": {"version": version},
            "resources": [{"name": name, "data": [{"value": 1}]} for name in names],
        }
    )


def test_duplicate_resource_names_rejected():
    with pytest.raises(ValueError, match="Two or more resources"):
        check_datapackage(package(["config", "scenario_data", "extra", "extra"]))


def test_missing_pair_precedes_unsupported_version():
    with pytest.raises(ValueError, match="scenario_data"):
        check_datapackage(package(["config"], version="unsupported"))


def test_empty_descriptor_still_fails_schema_validation():
    with pytest.raises(ValidationError):
        check_datapackage(package([]))


def test_edited_empty_runtime_list_has_clear_error():
    data = package(["inventory"])
    data.resources.clear()
    with pytest.raises(ValueError, match="contains no resources"):
        check_datapackage(data)
