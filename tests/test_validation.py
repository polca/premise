from premise.geomap import Geomap
from premise.validation import BaseDatasetValidator


def _validator_for_locations(database_locations, regions=None, extra_regions=None):
    validator = object.__new__(BaseDatasetValidator)
    validator.original_database = [{"location": "GLO"}]
    validator.database = [{"location": location} for location in database_locations]
    validator.regions = regions or []
    validator.valid_regions = set(validator.regions) | set(extra_regions or [])
    validator.geo = Geomap("remind")
    validator.major_issues_log = []
    validator.minor_issues_log = []
    return validator


def test_check_new_location_accepts_extra_superstructure_regions():
    validator = _validator_for_locations(
        database_locations=["JAP"],
        regions=["JPN"],
        extra_regions=["JAP"],
    )

    validator.check_new_location()

    assert validator.major_issues_log == []


def test_check_new_location_logs_unregistered_location_as_major_issue():
    validator = _validator_for_locations(
        database_locations=["not-a-location"],
        regions=["JPN"],
    )

    validator.check_new_location()

    assert len(validator.major_issues_log) == 1
    assert validator.major_issues_log[0]["location"] == "not-a-location"
