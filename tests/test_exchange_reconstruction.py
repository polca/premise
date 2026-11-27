"""Keep legacy exchange amounts, flow-name precedence and errors explicit."""

from types import SimpleNamespace

import numpy as np
import pytest

import premise.export.export as module
from premise.validation.validation import DatasetNormalizer


@pytest.fixture
def flow_catalog(monkeypatch):
    flows = {
        ("native", "air", "urban", "kilogram"): "native-code",
        ("canonical", "air", "urban", "kilogram"): "canonical-code",
        ("single", "air", "unspecified", "kilogram"): "single-code",
    }
    monkeypatch.setattr(module, "biosphere_flows_dictionary", lambda version: flows)
    monkeypatch.setattr(
        module,
        "bio_flows_correspondence",
        {"air": {"native": "canonical", "legacy": "canonical", "dangling": "missing"}},
    )
    return flows


@pytest.mark.parametrize(
    "name,categories,expected",
    [
        ("native", ("air", "urban"), "native-code"),
        ("canonical", ("air", "urban"), "canonical-code"),
        ("legacy", ("air", "urban"), "canonical-code"),
        ("single", ("air",), "single-code"),
    ],
)
def test_biosphere_identity_and_alias_precedence(
    flow_catalog, name, categories, expected
):
    assert (
        module.correct_biosphere_flow(name, categories, "kilogram", "3.12") == expected
    )


def test_unmapped_biosphere_name_keeps_value_error(flow_catalog):
    with pytest.raises(ValueError) as caught:
        module.correct_biosphere_flow("unknown", ("air", "urban"), "kilogram", "3.12")
    assert caught.value.args == (
        "('unknown', ('air', 'urban'), 'kilogram', '3.12') not found in biosphere dictionary.",
    )
    assert caught.value.__context__ is None


def test_missing_alias_target_keeps_key_error(flow_catalog):
    with pytest.raises(KeyError) as caught:
        module.correct_biosphere_flow("dangling", ("air", "urban"), "kilogram", "3.12")
    assert caught.value.args == (("missing", "air", "urban", "kilogram"),)


def test_empty_biosphere_categories_are_not_silently_normalized(flow_catalog):
    with pytest.raises(IndexError):
        module.correct_biosphere_flow("native", (), "kilogram", "3.12")


@pytest.mark.parametrize("fast", [True, False])
@pytest.mark.parametrize(
    "version,name",
    [
        ("3.9", "Benzene, hexachloro-"),
        ("3.11", "Benzene, hexachloro-"),
        ("3.11", "Hexachlorobenzene"),
    ],
)
def test_brightway_export_reconstructs_biosphere_uuid_from_an_old_label(
    fast, version, name
):
    exchange = {
        "type": "biosphere",
        "name": name,
        "amount": 1.0,
        "unit": "kilogram",
        "categories": ("air", "urban air close to ground"),
    }
    activity = {
        "name": "test activity",
        "reference product": "test product",
        "code": "test",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [exchange],
    }
    scenario = {
        "model": "remind",
        "pathway": "SSP2-NPi",
        "year": 2040,
        "database": [activity],
        "iam data": SimpleNamespace(regions=["CAZ"]),
    }
    if fast:
        module.prepare_db_for_fast_export(scenario, "target", version, "test-biosphere")
    else:
        DatasetNormalizer(
            scenario["model"],
            scenario["pathway"],
            2040,
            ["CAZ"],
            [activity],
            db_name="target",
            biosphere_name="test-biosphere",
            version=version,
        ).assign_database_and_inputs()

    assert exchange["input"] == (
        "test-biosphere",
        "1af44724-172c-462b-b277-bb4b2fd32c33",
    )


@pytest.mark.parametrize(
    "flow_type,amount,replaces_zero",
    [
        ("production", 0.0, True),
        ("production", -0.0, True),
        ("production", 2.0, False),
        ("production", -2.0, False),
        ("production", np.float64(0.0), True),
        ("production", np.float64(2.5), False),
        ("production", float("nan"), False),
        ("technosphere", 0.0, False),
        ("technosphere", -2.0, False),
        ("biosphere", 0.0, False),
        ("biosphere", -2.0, False),
    ],
)
def test_only_zero_production_is_replaced(
    flow_type, amount, replaces_zero, monkeypatch
):
    monkeypatch.setattr(module, "correct_biosphere_flow", lambda *args: "bio-code")
    monkeypatch.setattr(module, "fetch_exchange_code", lambda *args: "activity-code")
    identity = ("name", "product", ("air", "urban"), "CH", "kilogram", flow_type)

    exchange = module.get_exchange(0, {0: identity}, "target", "3.12", amount=amount)

    if replaces_zero:
        assert exchange["amount"] == 1.0
        assert type(exchange["amount"]) is float
    else:
        assert exchange["amount"] is amount
    assert exchange["input"] == (
        ("biosphere3", "bio-code")
        if flow_type == "biosphere"
        else ("target", "activity-code")
    )
