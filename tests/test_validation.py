from premise.geomap import Geomap
from premise.inventory_imports import canonicalize_classification_key
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


def test_fast_export_checks_add_missing_classifications(tmp_path, monkeypatch):
    dataset = {
        "name": "fuel cell system assembly, 1 kWe, proton exchange membrane (PEM)",
        "reference product": "fuel cell system, 1 kWe, proton exchange membrane (PEM)",
        "location": "GLO",
        "classifications": [],
        "exchanges": [],
    }
    expected = [
        (
            "ISIC rev.4 ecoinvent",
            "4322:Plumbing, heat and air-conditioning installation",
        ),
        ("CPC", "46410: Primary cells and primary batteries"),
    ]

    validator = object.__new__(BaseDatasetValidator)
    validator.database = [dataset]
    validator.classifications = {
        canonicalize_classification_key(
            dataset["name"], dataset["reference product"]
        ): {
            "ISIC rev.4 ecoinvent": expected[0][1],
            "CPC": expected[1][1],
        }
    }

    for method_name in (
        "check_matrix_squareness",
        "validate_dataset_structure",
        "verify_data_consistency",
        "check_relinking_logic",
        "check_for_orphaned_datasets",
        "check_for_duplicates",
        "check_for_circular_references",
        "check_database_name",
        "remove_unused_fields",
        "correct_fields_format",
        "check_amount_format",
        "reformat_parameters",
        "check_uncertainty",
        "_finalize_logs",
    ):
        monkeypatch.setattr(
            BaseDatasetValidator,
            method_name,
            lambda self: None,
        )
    monkeypatch.chdir(tmp_path)

    validator.run_fast_export_checks()

    assert dataset["classifications"] == expected
