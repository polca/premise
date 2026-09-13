import copy

import numpy as np
import pytest
import xarray as xr
from types import SimpleNamespace

import premise.validation as validation_module
from premise.geomap import Geomap
from premise.inventory_imports import canonicalize_classification_key
from premise.validation import (
    BaseDatasetValidator,
    DatasetNormalizer,
    PremiseValidationError,
    TransportValidation,
    normalize_exact_deterministic_exchange_duplicates,
    normalize_inventory_numeric_types,
    normalize_inventory_uncertainty,
)


def test_combined_fast_export_session_matches_sequential_preparation():
    inventory = [
        {
            "name": "market for service",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "database": "source-db",
            "code": "service-code",
            "exchanges": [
                {
                    "name": "market for service",
                    "product": "service",
                    "location": "GLO",
                    "unit": "unit",
                    "type": "production",
                    "amount": 1.0,
                }
            ],
        }
    ]

    expected_validator = BaseDatasetValidator(
        model="image",
        scenario="SSP2-Base",
        year=2050,
        regions=["World"],
        database=copy.deepcopy(inventory),
        original_database=[],
        db_name="target-db",
        biosphere_name="biosphere3",
        version="3.12",
    )
    expected_validator.database = (
        expected_validator.make_normalizer().prepare_fast_export_fields()
    )
    for dataset in expected_validator.database:
        dataset["exchanges"] = [
            validation_module.prepare_fast_exchange_payload(exchange)
            for exchange in dataset["exchanges"]
        ]
    expected_report = expected_validator.run_export_schema_checks()

    combined_validator = BaseDatasetValidator(
        model="image",
        scenario="SSP2-Base",
        year=2050,
        regions=["World"],
        database=copy.deepcopy(inventory),
        original_database=[],
        db_name="target-db",
        biosphere_name="biosphere3",
        version="3.12",
    )
    combined_database, combined_report = combined_validator.run_fast_export_session()

    assert combined_database == expected_validator.database
    assert combined_report.rule_results == expected_report.rule_results
    assert combined_report.phase_results == expected_report.phase_results


def test_transport_energy_filters_require_technosphere_exchanges():
    validator = object.__new__(TransportValidation)
    validator.database = [
        {
            "name": "transport, passenger car, unspecified",
            "location": "EUR",
            "exchanges": [
                {
                    "name": "market group for electricity, low voltage",
                    "type": "production",
                    "unit": "kilowatt hour",
                    "amount": 5.0,
                },
                {
                    "name": "market for diesel",
                    "type": "biosphere",
                    "unit": "kilogram",
                    "amount": 5.0,
                },
                {
                    "name": "market for natural gas",
                    "type": "production",
                    "unit": "kilogram",
                    "amount": 5.0,
                },
            ],
        }
    ]
    validator.regions = ["EUR"]
    validator.major_issues_log = []
    validator.minor_issues_log = []
    validator.validation_issues = []

    validator.check_vehicle_efficiency("transport, passenger car")

    assert validator.major_issues_log == []


def test_consequential_validation_recomputes_and_caches_an_independent_mix(
    monkeypatch,
):
    raw = xr.DataArray(
        np.array([[[10.0, 12.0], [20.0, 18.0]]]),
        dims=("region", "variables", "year"),
        coords={
            "region": ["World"],
            "variables": ["technology A", "technology B"],
            "year": [2040, 2050],
        },
    )
    average_mix = raw / raw.sum(dim="variables")
    iam_data = SimpleNamespace(
        electricity_mix=average_mix,
        system_model_args={"range time": 2},
        _validation_market_inputs={"electricity": raw.copy(deep=True)},
        _validation_market_oracles={},
    )
    calls = []

    def oracle(data, year, args, sector):
        calls.append((year, args, sector))
        assert data.identical(raw)
        data.values[:] = 0.0
        data.loc[dict(variables="technology B")] = 1.0
        return data

    monkeypatch.setattr(validation_module, "consequential_method", oracle)

    first = validation_module.independent_consequential_mix(
        iam_data, "electricity", 2050
    )
    second = validation_module.independent_consequential_mix(
        iam_data, "electricity", 2050
    )

    assert calls == [(2050, {"range time": 2}, "electricity")]
    assert first.identical(second)
    assert first.sel(variables="technology A").sum().item() == 0.0
    assert first.sel(variables="technology B").sum().item() == 2.0
    assert iam_data._validation_market_inputs["electricity"].identical(raw)
    assert not first.identical(iam_data.electricity_mix)


def _export_validator(amount):
    validator = object.__new__(BaseDatasetValidator)
    validator.database = [
        {
            "name": "activity",
            "reference product": "product",
            "location": "GLO",
            "unit": "kilogram",
            "database": "test-db",
            "code": "activity",
            "exchanges": [
                {
                    "name": "activity",
                    "product": "product",
                    "location": "GLO",
                    "unit": "kilogram",
                    "type": "production",
                    "amount": amount,
                    "input": ("test-db", "activity"),
                }
            ],
        }
    ]
    validator.db_name = "test-db"
    validator.biosphere_name = "biosphere3"
    validator.model = "image"
    validator.scenario = "path"
    validator.year = 2050
    validator.major_issues_log = []
    validator.minor_issues_log = []
    validator.validation_issues = []
    return validator


def test_fast_export_schema_accepts_writer_compatible_numpy_scalar():
    report = _export_validator(np.array([1.0])).run_export_schema_checks()

    assert report.valid


def test_fast_export_preparation_preserves_writer_compatible_numpy_scalar():
    validator = _export_validator(np.array([1.0]))
    normalizer = DatasetNormalizer.from_validator(validator)

    database = normalizer.prepare_fast_export_fields()

    assert database[0]["database"] == "test-db"
    assert isinstance(database[0]["exchanges"][0]["amount"], np.ndarray)


def test_fast_export_preparation_assigns_canonical_provider_input():
    validator = _export_validator(1.0)
    validator.database[0]["exchanges"][0]["input"] = (
        "source-db",
        "activity",
    )

    database = DatasetNormalizer.from_validator(validator).prepare_fast_export_fields()

    assert database[0]["exchanges"][0]["input"] == (
        "test-db",
        "activity",
    )


def test_fast_export_schema_rejects_wrong_provider_input():
    validator = _export_validator(1.0)
    validator.database[0]["exchanges"][0]["input"] = (
        "wrong-db",
        "activity",
    )

    with pytest.raises(PremiseValidationError) as error:
        validator.run_export_schema_checks()

    assert any(
        issue.rule_id == "LEGACY.EXPORT_SCHEMA_PROVIDER_INPUT"
        for issue in error.value.report.errors
    )


def test_fast_export_schema_rejects_non_scalar_numpy_array():
    with pytest.raises(PremiseValidationError) as error:
        _export_validator(np.array([1.0, 2.0])).run_export_schema_checks()

    assert any(
        issue.rule_id == "LEGACY.EXPORT_SCHEMA_AMOUNT"
        for issue in error.value.report.errors
    )


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


def test_expected_iam_location_does_not_remap_an_iam_region():
    validator = _validator_for_locations(database_locations=["ME"], regions=["ME"])

    assert validator.expected_iam_location("ME") == "ME"


def test_export_normalizer_adds_missing_classifications(tmp_path, monkeypatch):
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

    normalizer = object.__new__(DatasetNormalizer)
    normalizer.database = [dataset]
    normalizer.classifications = {
        canonicalize_classification_key(
            dataset["name"], dataset["reference product"]
        ): {
            "ISIC rev.4 ecoinvent": expected[0][1],
            "CPC": expected[1][1],
        }
    }

    monkeypatch.chdir(tmp_path)

    normalizer.add_missing_classifications()

    assert dataset["classifications"] == expected


def test_numeric_normalization_converts_scalar_arrays_but_not_vectors():
    scalar = np.array(0.25)
    singleton = np.array([2.0])
    vector = np.array([1.0, 2.0])
    database = [
        {
            "name": "activity",
            "log parameters": {"share": scalar},
            "exchanges": [
                {"amount": scalar, "loc": singleton},
                {"amount": vector},
            ],
        }
    ]

    normalize_inventory_numeric_types(database)

    assert database[0]["log parameters"]["share"] == 0.25
    assert database[0]["exchanges"][0]["amount"] == 0.25
    assert database[0]["exchanges"][0]["loc"] == 2.0
    assert database[0]["exchanges"][1]["amount"] is vector


def test_uncertainty_normalization_repairs_lognormal_sign_metadata():
    database = [
        {
            "exchanges": [
                {
                    "amount": -2.0,
                    "uncertainty type": 2,
                    "loc": np.log(2.0),
                    "scale": 0.2,
                    "negative": False,
                }
            ]
        }
    ]

    normalize_inventory_uncertainty(database)

    assert database[0]["exchanges"][0]["negative"] is True


def test_uncertainty_normalization_makes_zero_lognormal_deterministic():
    database = [
        {
            "exchanges": [
                {
                    "amount": 0.0,
                    "uncertainty type": 2,
                    "loc": -10.0,
                    "scale": 0.2,
                    "negative": False,
                }
            ]
        }
    ]

    normalize_inventory_uncertainty(database)

    exchange = database[0]["exchanges"][0]
    assert exchange == {"amount": 0.0, "uncertainty type": 0, "loc": 0.0}


def test_exact_deterministic_duplicates_are_summed_but_stochastic_rows_remain():
    deterministic = {
        "name": "market for fuel",
        "product": "fuel",
        "location": "World",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 0.25,
        "uncertainty type": 0,
    }
    stochastic = {
        **deterministic,
        "amount": 0.5,
        "uncertainty type": 2,
        "loc": np.log(0.5),
        "scale": 0.2,
    }
    database = [
        {
            "exchanges": [
                dict(deterministic),
                dict(deterministic),
                dict(stochastic),
                dict(stochastic),
            ]
        }
    ]

    normalize_exact_deterministic_exchange_duplicates(database)

    assert len(database[0]["exchanges"]) == 3
    assert database[0]["exchanges"][0]["amount"] == 0.5
    assert database[0]["exchanges"][1:] == [stochastic, stochastic]
