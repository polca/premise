"""UVEK source extraction preserves linked quantities and explicit axes."""

from copy import deepcopy
from unittest.mock import Mock

import pytest

from premise.database.background import (
    extract_uvek_source,
    resolve_source_background,
    validate_uvek_scope,
)


@pytest.fixture
def source(monkeypatch):
    import bw2data as bd

    activity = {
        "name": "Native process",
        "reference product": "Native product",
        "location": "CH",
        "unit": "kilogram",
        "database": "native",
        "code": "process",
        "parameters": {"dose": -2.0},
        "exchanges": [
            {"type": "production", "input": ("native", "process"), "amount": 1.0},
            {
                "type": "technosphere",
                "input": ("native", "process"),
                "amount": -2.0,
                "formula": "dose",
                "uncertainty type": 2,
                "loc": 0.693147,
                "scale": 0.2,
                "negative": True,
            },
            {"type": "biosphere", "input": ("bio310", "flow"), "amount": 0.5},
        ],
    }
    biosphere = {
        ("bio310", "flow"): {
            "name": "Emission",
            "categories": ("air", "urban air close to ground"),
            "unit": "kilogram",
        }
    }
    metadata = {
        "background_family": "uvek",
        "background_version": "2026",
        "system_model": "cutoff",
        "biosphere_family": "ecoinvent",
        "biosphere_version": "3.10",
    }
    native = {("native", "process"): activity}
    monkeypatch.setattr(bd, "databases", {"native": metadata, "bio310": {}})
    monkeypatch.setattr(
        bd,
        "Database",
        lambda name: Mock(
            load=Mock(return_value=native if name == "native" else biosphere)
        ),
    )
    return native, biosphere, metadata


def test_uvek_axes_never_infer_biosphere_from_publisher_release():
    background = resolve_source_background("2026", family="UVEK")
    assert background.biosphere_version == "3.10"
    assert background.version == "2026"
    with pytest.raises(ValueError, match="Unsupported ecoinvent"):
        resolve_source_background("2026")
    with pytest.raises(ValueError, match="requires release 2026"):
        resolve_source_background("2026", family="uvek", system_model="consequential")


def test_native_extraction_resolves_labels_without_changing_source(source):
    native, biosphere, _ = source
    before = deepcopy((native, biosphere))
    records = extract_uvek_source("native", "bio310")
    assert (native, biosphere) == before
    edge = records[0]["exchanges"][1]
    for key in (
        "amount",
        "formula",
        "uncertainty type",
        "loc",
        "scale",
        "negative",
        "input",
    ):
        assert edge[key] == native[("native", "process")]["exchanges"][1][key]
    assert edge["product"] == "Native product"
    assert edge["location"] == "CH"
    assert (
        records[0]["exchanges"][2]["categories"]
        == biosphere[("bio310", "flow")]["categories"]
    )
    records[0]["parameters"]["dose"] = 99
    assert native[("native", "process")]["parameters"]["dose"] == -2


def test_native_extraction_refuses_wrong_declared_biosphere(source):
    source[2]["biosphere_version"] = "3.12"
    with pytest.raises(ValueError, match="biosphere_version='3.10'"):
        extract_uvek_source("native", "bio310")


def test_native_extraction_refuses_supplier_outside_selected_biosphere(source):
    source[0][("native", "process")]["exchanges"][2]["input"] = ("other", "flow")
    with pytest.raises(ValueError, match="no supplier in the declared destination"):
        extract_uvek_source("native", "bio310")


def test_uvek_scope_defaults_to_pilot_and_refuses_unverified_sectors():
    assert validate_uvek_scope(None) == ["electricity"]
    assert validate_uvek_scope("electricity") == ["electricity"]
    with pytest.raises(ValueError, match="unsupported sectors.*cement"):
        validate_uvek_scope(["electricity", "cement"])


def test_uvek_release_never_uses_ecoinvent_biosphere_fallback():
    from premise.export.export import biosphere_flows_dictionary
    from premise.inventory.inventory_imports import get_biosphere_code

    for lookup in (get_biosphere_code, biosphere_flows_dictionary):
        with pytest.raises(ValueError, match="explicit biosphere version 3.10"):
            lookup("2026")


def test_export_validation_uses_the_biosphere_axis(monkeypatch):
    from premise.export import export as module

    validator = Mock()
    validator.run_fast_export_session.return_value = ([], None)
    constructor = Mock(return_value=validator)
    monkeypatch.setattr(module, "BaseDatasetValidator", constructor)
    scenario = {
        "model": "remind",
        "pathway": "SSP2-NPi",
        "year": 2050,
        "database": [],
        "iam data": Mock(regions=["EUR"]),
        "background context": resolve_source_background(
            "2026", family="uvek"
        ).to_dict(),
    }
    module.prepare_db_for_fast_export(scenario, "export", "2026", "bio310")
    assert constructor.call_args.kwargs["version"] == "3.10"
    assert scenario["background context"]["version"] == "2026"
