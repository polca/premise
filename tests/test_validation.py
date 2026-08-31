from premise.geomap import Geomap
from premise.inventory_imports import canonicalize_classification_key
from premise.validation import BaseDatasetValidator, FuelsValidation


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


def _fuels_validator(database):
    validator = object.__new__(FuelsValidation)
    validator.database = database
    validator.regions = ["EUR"]
    validator.major_issues_log = []
    validator.minor_issues_log = []
    return validator


def _technosphere_exchange(name, product, amount, unit="kilogram"):
    return {
        "name": name,
        "product": product,
        "amount": amount,
        "unit": unit,
        "type": "technosphere",
    }


def _fuel_market(name, reference_product, exchanges, unit="kilogram"):
    return {
        "name": name,
        "reference product": reference_product,
        "location": "EUR",
        "unit": unit,
        "exchanges": exchanges,
    }


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


def test_hydrogen_market_mass_check_excludes_logistics_exchanges():
    market = _fuel_market(
        "market for hydrogen, gaseous, low pressure, for steel",
        "hydrogen, gaseous, low pressure",
        [
            _technosphere_exchange(
                "hydrogen production, electrolysis",
                "hydrogen, gaseous, low pressure",
                0.6,
            ),
            _technosphere_exchange(
                "hydrogen production, steam methane reforming",
                "hydrogen, gaseous, 30 bar",
                0.4,
            ),
            _technosphere_exchange(
                "hydrogen supply, distributed by pipeline",
                "hydrogen, gaseous, from pipeline",
                0.7,
            ),
            _technosphere_exchange(
                "transport, freight, sea, tanker for liquefied ammonia",
                "transport, freight, sea, tanker for liquefied ammonia",
                0.75,
                unit="ton kilometer",
            ),
            _technosphere_exchange(
                "ammonia cracking",
                "ammonia cracking",
                0.3,
                unit="unit",
            ),
        ],
    )
    validator = _fuels_validator([market])

    validator.check_fuel_market_composition()

    assert validator.major_issues_log == []


def test_hydrogen_market_mass_check_reports_under_supply():
    market = _fuel_market(
        "market for hydrogen, gaseous, low pressure, for transport",
        "hydrogen, gaseous, low pressure",
        [
            _technosphere_exchange(
                "hydrogen production, electrolysis",
                "hydrogen, gaseous, low pressure",
                0.8,
            ),
            _technosphere_exchange(
                "hydrogen supply, distributed by pipeline",
                "hydrogen, gaseous, from pipeline",
                0.2,
            ),
        ],
    )
    validator = _fuels_validator([market])

    validator.check_fuel_market_composition()

    assert len(validator.major_issues_log) == 1
    assert validator.major_issues_log[0]["reason"] == (
        "hydrogen production inputs do not sum to 1"
    )
    assert "0.8 kg instead of 1 kg" in validator.major_issues_log[0]["message"]


def test_hydrogen_market_mass_check_reports_over_supply():
    market = _fuel_market(
        "market for hydrogen, gaseous, low pressure, for heating",
        "hydrogen, gaseous, low pressure",
        [
            _technosphere_exchange(
                "hydrogen production, electrolysis",
                "hydrogen, gaseous, low pressure",
                1.2,
            ),
            _technosphere_exchange(
                "hydrogen supply, distributed by pipeline",
                "hydrogen, gaseous, from pipeline",
                1.0,
            ),
        ],
    )
    validator = _fuels_validator([market])

    validator.check_fuel_market_composition()

    assert len(validator.major_issues_log) == 1
    assert "1.2 kg instead of 1 kg" in validator.major_issues_log[0]["message"]


def test_non_hydrogen_fuel_market_keeps_existing_composition_tolerance():
    market = _fuel_market(
        "market for diesel, low-sulfur",
        "diesel, low-sulfur",
        [
            _technosphere_exchange(
                "diesel production",
                "diesel, low-sulfur",
                1.5,
            )
        ],
    )
    validator = _fuels_validator([market])

    validator.check_fuel_market_composition()

    assert validator.major_issues_log == []


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
