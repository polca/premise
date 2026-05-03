from copy import deepcopy
import sys
import types

import pytest

try:
    import xarray  # noqa: F401
except ModuleNotFoundError:
    xarray_stub = types.ModuleType("xarray")
    xarray_stub.DataArray = object
    xarray_stub.Dataset = object
    xarray_stub.__version__ = "0.0.0"
    sys.modules["xarray"] = xarray_stub

try:
    import schema  # noqa: F401
except ModuleNotFoundError:
    schema_stub = types.ModuleType("schema")

    class DummySchema:
        def __init__(self, *args, **kwargs):
            pass

        def validate(self, value):
            return value

    schema_stub.And = lambda *args, **kwargs: ("And", args, kwargs)
    schema_stub.Optional = lambda *args, **kwargs: ("Optional", args, kwargs)
    schema_stub.Schema = DummySchema
    schema_stub.Use = lambda *args, **kwargs: ("Use", args, kwargs)
    sys.modules["schema"] = schema_stub

import premise.new_database as new_database_module
from premise.new_database import NewDatabase
from premise.ozone import OzoneDepletingSubstances


class DummyIAMData:
    regions = ["World"]


def production_exchange(name, product, amount=1.0):
    return {
        "name": name,
        "product": product,
        "amount": amount,
        "type": "production",
        "unit": "kilogram",
        "production volume": 1.0,
    }


def dataset(name, reference_product, location, exchanges=None, unit="kilogram"):
    exchanges = exchanges or []
    return {
        "name": name,
        "reference product": reference_product,
        "location": location,
        "unit": unit,
        "exchanges": [
            production_exchange(name, reference_product),
            *exchanges,
        ],
    }


def build_transformer(database, year=2030):
    return OzoneDepletingSubstances(
        database=deepcopy(database),
        iam_data=DummyIAMData(),
        model="image",
        pathway="SSP2-Base",
        year=year,
        version="3.10",
        system_model="cutoff",
    )


def test_hcfc_article5_servicing_allowance_is_applied():
    transformer = build_transformer([], year=2030)

    assert (
        transformer.get_allowed_fraction(
            "hcfcs_annex_c_i", location="CN", application="refrigeration"
        )
        == 0.025
    )
    assert (
        transformer.get_allowed_fraction(
            "hcfcs_annex_c_i", location="US", application="refrigeration"
        )
        == 0.0
    )


def test_party_group_uses_iam_region_ecoinvent_equivalencies():
    transformer = build_transformer([], year=2030)
    transformer.iam_to_ecoinvent_loc["A5_TEST"] = ["CN", "IN"]
    transformer.iam_to_ecoinvent_loc["NON_A5_TEST"] = ["US", "JP"]
    transformer.iam_to_ecoinvent_loc["MIXED_TEST"] = ["CN", "US"]

    assert transformer.get_party_group("A5_TEST") == "article5"
    assert transformer.get_party_group("NON_A5_TEST") == "non_article5"
    assert transformer.get_party_group("MIXED_TEST") == "article5"


def test_party_group_maps_ecoinvent_region_through_iam_equivalencies():
    transformer = build_transformer([], year=2030)
    transformer.ecoinvent_to_iam_loc["RER"] = "WEU"
    transformer.iam_to_ecoinvent_loc["WEU"] = ["AT", "BE", "CH", "DE", "FR"]

    assert transformer.get_party_group("RER") == "non_article5"


def test_cfc_technosphere_exchange_is_substituted():
    database = [
        dataset(
            "market for refrigerant R134a",
            "refrigerant R134a",
            "GLO",
        ),
        dataset(
            "refrigeration equipment production",
            "refrigeration equipment",
            "US",
            exchanges=[
                {
                    "name": "market for refrigerant R-12",
                    "product": "refrigerant R-12",
                    "location": "GLO",
                    "unit": "kilogram",
                    "amount": 2.0,
                    "type": "technosphere",
                }
            ],
        ),
    ]

    transformer = build_transformer(database, year=2020)
    transformer.update_database()

    consumer = next(
        ds
        for ds in transformer.database
        if ds["name"] == "refrigeration equipment production"
    )
    technosphere = [
        exc for exc in consumer["exchanges"] if exc["type"] == "technosphere"
    ]

    assert technosphere == [
        {
            "name": "market for refrigerant R134a",
            "product": "refrigerant R134a",
            "location": "GLO",
            "unit": "kilogram",
            "amount": 2.0,
            "type": "technosphere",
        }
    ]
    assert transformer.summary["technosphere substituted"] == 1


def test_hcfc_refrigeration_exchange_keeps_article5_servicing_residual():
    database = [
        dataset("market for ammonia, liquid", "ammonia, liquid", "GLO"),
        dataset(
            "commercial refrigeration system production",
            "commercial refrigeration system",
            "CN",
            exchanges=[
                {
                    "name": "market for refrigerant R-22",
                    "product": "refrigerant R-22",
                    "location": "GLO",
                    "unit": "kilogram",
                    "amount": 10.0,
                    "type": "technosphere",
                }
            ],
        ),
    ]

    transformer = build_transformer(database, year=2030)
    transformer.update_database()

    consumer = next(
        ds
        for ds in transformer.database
        if ds["name"] == "commercial refrigeration system production"
    )
    technosphere = [
        exc for exc in consumer["exchanges"] if exc["type"] == "technosphere"
    ]

    assert len(technosphere) == 2
    assert technosphere[0]["product"] == "refrigerant R-22"
    assert technosphere[0]["amount"] == pytest.approx(0.25)
    assert technosphere[1]["product"] == "ammonia, liquid"
    assert technosphere[1]["amount"] == pytest.approx(9.75)


def test_methyl_bromide_qps_exchange_is_exempt():
    database = [
        dataset("market for phosphine", "phosphine", "GLO"),
        dataset(
            "quarantine pre-shipment fumigation service",
            "fumigation service",
            "CN",
            exchanges=[
                {
                    "name": "market for methyl bromide",
                    "product": "methyl bromide",
                    "location": "GLO",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "type": "technosphere",
                }
            ],
        ),
    ]

    transformer = build_transformer(database, year=2030)
    transformer.update_database()

    consumer = next(
        ds
        for ds in transformer.database
        if ds["name"] == "quarantine pre-shipment fumigation service"
    )
    technosphere = [
        exc for exc in consumer["exchanges"] if exc["type"] == "technosphere"
    ]

    assert technosphere[0]["product"] == "methyl bromide"
    assert technosphere[0]["amount"] == 1.0
    assert transformer.summary["exempt"] == 1


def test_biosphere_ods_emission_is_scaled_by_schedule():
    database = [
        dataset(
            "refrigerant venting",
            "used refrigerant treatment",
            "US",
            exchanges=[
                {
                    "name": "Dichlorodifluoromethane",
                    "categories": ("air", "unspecified"),
                    "unit": "kilogram",
                    "amount": 1.0,
                    "type": "biosphere",
                    "uncertainty type": 2,
                    "loc": 0.0,
                    "scale": 0.1,
                }
            ],
        ),
    ]

    transformer = build_transformer(database, year=2020)
    transformer.update_database()

    biosphere = [
        exc
        for exc in transformer.database[0]["exchanges"]
        if exc["type"] == "biosphere"
    ][0]

    assert biosphere["amount"] == 0.0
    assert biosphere["uncertainty type"] == 0
    assert "loc" not in biosphere
    assert "scale" not in biosphere
    assert transformer.summary["biosphere scaled"] == 1


def test_new_database_update_accepts_ozone_sector(monkeypatch):
    calls = []
    scenario = {
        "model": "image",
        "pathway": "SSP2-Base",
        "year": 2030,
        "database": [],
        "iam data": DummyIAMData(),
    }
    obj = object.__new__(NewDatabase)
    obj.version = "3.10"
    obj.system_model = "cutoff"
    obj.use_absolute_efficiency = False
    obj.gains_scenario = None
    obj.scenarios = [scenario]
    obj.database = None
    obj._database_is_complete = False

    def fake_update_ozone(scenario, version, system_model):
        calls.append((scenario["pathway"], version, system_model))
        return scenario

    monkeypatch.setattr(new_database_module, "_update_ozone", fake_update_ozone)
    monkeypatch.setattr(new_database_module, "dump_database", lambda scenario: None)
    monkeypatch.setattr(
        obj,
        "_load_scenario_database_for_update",
        lambda scenario, scenario_position: scenario,
    )
    monkeypatch.setattr(obj, "_clear_scenario_runtime_state", lambda scenario: None)

    obj.update("ozone")

    assert calls == [("SSP2-Base", "3.10", "cutoff")]
    assert scenario["applied functions"] == ["ozone"]
