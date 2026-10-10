"""Exact annual reader, provenance, encryption/cache and export-profile checks."""

from copy import deepcopy
import hashlib
import json

from cryptography.fernet import Fernet
import numpy as np
import pandas as pd
import pytest

from premise.data_collection import IAMDataCollection
from premise.iam_stock_vintage import PREFIX, stock_vintage_weights, verified_report
from premise.stock_vintage import StockVintageExport


@pytest.fixture
def iam_files(tmp_path):
    rows = []

    def add(variable, values, unit="1"):
        rows.append(
            dict(
                Model="REMIND",
                Scenario="test",
                Region="EUR",
                Variable=PREFIX + "cars|" + variable,
                Unit=unit,
                **dict(zip(("2020", "2021", "2022"), values)),
            )
        )

    add("Coverage", [1, 1, 1])
    add("Stock", [10, 10, 10], "million veh")
    add("Stock|Vintage|2010", [10, 5, 0], "million veh")
    add("Stock|Vintage|2021", [0, 5, 10], "million veh")
    add("Stock Share|Vintage|2010", [1, 0.5, 0])
    add("Stock Share|Vintage|2021", [0, 0.5, 1])
    add("Service Share|Vintage|2010", [1, 0.25, 0])
    add("Service Share|Vintage|2021", [0, 0.75, 1])
    path = tmp_path / "remind_test.csv"
    pd.DataFrame(rows).to_csv(path, sep=";", index=False)
    report = {
        "model": "REMIND",
        "input_sha256": "source-pin",
        "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "assumptions": {
            "stock_vintage_schema_version": 1,
            "origin": "reconstructed_not_native",
            "allocation_basis": "common_amortisation",
            "exchange_amount_changed": False,
            "scenario": "test",
            "reference_year": 2020,
            "end_year": 2022,
            "flodym_version": "1.1.0",
            "service_target_effect": "total_only",
            "assets": [{"asset": "cars", "region": "EUR", "configuration": {}}],
        },
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))
    return path, report_path, report


def load(path, key=None):
    obj = IAMDataCollection.__new__(IAMDataCollection)
    obj.model, obj.pathway, obj.year = "remind", "test", 2021
    obj.data = obj._IAMDataCollection__get_iam_data(
        key=key, filedir=path.parent, variables=[]
    )
    return obj


def test_reader_keeps_stock_and_service_distinct(iam_files):
    path, _, _ = iam_files
    obj = load(path)
    assert obj.stock_vintage_weights("cars", "EUR")["weights"] == [0.25, 0.75]
    assert obj.stock_vintage_weights("cars", "EUR", basis="stock")["weights"] == [
        0.5,
        0.5,
    ]
    for year in (2022, 2020, 2021, 2020):
        result = obj.stock_vintage_weights("cars", "EUR", year)
        assert sum(result["weights"]) == 1
        assert max(result["event_years"]) <= year
    with pytest.raises(ValueError, match="exact"):
        obj.stock_vintage_weights("cars", "EUR", 2023)
    with pytest.raises(ValueError, match="exact"):
        obj.stock_vintage_weights("cars", "USA", 2021)


def test_profile_passes_existing_export_contract_without_changing_amounts(iam_files):
    path, report_path, _ = iam_files
    profile = load(path).stock_vintage_profile(
        asset_id="cars",
        region="EUR",
        years=range(2020, 2023),
        report_path=report_path,
        asset_unit="unit",
        service_unit="kilometer",
    )
    caller = dict(
        name="service",
        **{"reference product": "service"},
        unit="kilometer",
        location="EUR",
    )
    supplier = dict(
        name="asset", **{"reference product": "asset"}, unit="unit", location="GLO"
    )
    payload = {
        "schema_version": 1,
        "inventory_context": {
            "source_database": "synthetic",
            "source_version": "3.12",
            "system_model": "cutoff",
            "model": "remind",
            "pathway": "test",
        },
        "profiles": [profile],
        "bindings": [dict(profile_id="cars", caller=caller, supplier=supplier)],
    }
    export = StockVintageExport(payload)
    assert export.profiles["cars"]["allocation_basis"] == "common_amortisation"
    assert export.annual["cars"][2021] == ([2010, 2021], [0.25, 0.75])
    assert profile["provenance"]["exchange_amount_changed"] is False


@pytest.mark.parametrize("encrypted", [False, True])
def test_source_report_binding_survives_cache_and_encryption(iam_files, encrypted):
    path, report_path, report = iam_files
    key = None
    if encrypted:
        key = Fernet.generate_key()
        path.write_bytes(Fernet(key).encrypt(path.read_bytes()))
        report["encrypted_output_sha256"] = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        report_path.write_text(json.dumps(report))
    for _ in range(2):
        obj = load(path, key=key)
        assert obj.iam_source_path == path
        assert obj.stock_vintage_weights("cars", "EUR")["weights"] == [0.25, 0.75]
        verified_report(report_path, path)
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="does not match"):
        verified_report(report_path, path)


@pytest.mark.parametrize(
    "problem", ["nan", "negative", "sum", "future", "unit", "stock", "coverage"]
)
def test_corrupt_profiles_fail_closed(iam_files, problem):
    data = load(iam_files[0]).data.copy(deep=True)
    var = PREFIX + "cars|Service Share|Vintage|2010"
    if problem == "nan":
        data.loc[dict(region="EUR", variables=var, year=2021)] = np.nan
    elif problem == "negative":
        data.loc[dict(region="EUR", variables=var, year=2021)] = -0.25
    elif problem == "sum":
        data.loc[dict(region="EUR", variables=var, year=2021)] = 0.5
    elif problem == "future":
        data = data.assign_coords(
            variables=[
                v.replace("Vintage|2021", "Vintage|2031") for v in data.variables.values
            ]
        )
    elif problem == "unit":
        data.attrs = deepcopy(data.attrs)
        data.attrs["unit"][var] = "percent"
    elif problem == "stock":
        data.loc[dict(region="EUR", variables=PREFIX + "cars|Stock", year=2021)] = 20
    else:
        data.loc[dict(region="EUR", variables=PREFIX + "cars|Coverage", year=2021)] = 0
    with pytest.raises(ValueError):
        stock_vintage_weights(data, "cars", "EUR", 2021)


def test_mismatched_report_context_rejected(iam_files):
    path, report_path, report = iam_files
    obj = load(path)
    report["assumptions"]["scenario"] = "different"
    report_path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="scenario"):
        obj.stock_vintage_profile(
            asset_id="cars",
            region="EUR",
            years=[2020],
            report_path=report_path,
            asset_unit="unit",
            service_unit="km",
        )
