"""Native cement selectors feed the shared transforms without changing quantities."""

import copy
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from premise.inventory.activity_maps import InventorySet
from premise.materials.cement import Cement

CONFIG = yaml.safe_load(
    (
        Path(__file__).parents[1] / "premise/iam_variables_mapping/cement_markets.yaml"
    ).read_text()
)["uvek"]


def activity(identity, exchanges=None):
    return {
        **identity,
        "unit": identity.get("unit", "kilogram"),
        "location": identity.get("location", "CH"),
        "database": "test",
        "code": "source",
        "exchanges": exchanges or [],
    }


def cement(database, mapping=None):
    obj = object.__new__(Cement)
    obj.database = database
    obj.source_mapping = CONFIG if mapping is None else mapping
    return obj


def test_native_producer_pairs_include_grades_and_exclude_false_matches():
    producers = [activity(pair) for pair in CONFIG["cement_production"]]
    decoys = [
        activity(
            {**CONFIG["cement_production"][0], "reference product": "other material"}
        ),
        activity({**CONFIG["cement_production"][0], "unit": "unit"}),
        activity({"name": "Cement factory", "reference product": "factory"}),
    ]
    obj = cement(producers + decoys)
    assert len(producers) == 14
    assert obj._cement_production_datasets() == producers
    original = activity(
        {"name": "cement production, Portland", "reference product": "cement, Portland"}
    )
    assert cement([original, *decoys], {})._cement_production_datasets() == [original]


@pytest.mark.parametrize("amount", [-0.8, 0, 0.8])
def test_clinker_relink_preserves_signed_grade_recipe_and_uncertainty(amount):
    edge = {
        "name": "Clinker, at plant",
        "product": "Clinker, at plant",
        "unit": "kilogram",
        "location": "CH",
        "type": "technosphere",
        "input": ("source", "old-clinker"),
        "amount": amount,
        "formula": "clinker_dose",
        "uncertainty type": 3,
        "loc": amount,
        "scale": 0.05,
    }
    other = {**edge, "name": "Gypsum, at plant", "product": "Gypsum, at plant"}
    wrong_basis = {**edge, "unit": "megajoule"}
    ds = activity(CONFIG["cement_production"][0], [edge, other, wrong_basis])
    before = copy.deepcopy(ds)
    cement([ds]).replace_clinker_production_with_markets()
    assert edge["name"] == "market for clinker" and edge["product"] == "clinker"
    assert "input" not in edge
    for key in ("amount", "formula", "uncertainty type", "loc", "scale", "location"):
        assert edge[key] == before["exchanges"][0][key]
    assert ds["exchanges"][1:] == before["exchanges"][1:]


def test_native_template_generates_standard_ccs_route_without_mutating_source():
    source = activity(
        CONFIG["clinker_template"],
        [
            {
                "name": "Clinker, at plant",
                "product": "Clinker, at plant",
                "type": "production",
                "amount": 1,
                "input": ("source", "source"),
            },
            {
                "name": "Hard coal supply mix",
                "product": "Hard coal supply mix",
                "type": "technosphere",
                "amount": 0.00937,
                "unit": "kilogram",
            },
        ],
    )
    before = copy.deepcopy(source)
    obj = cement([source])
    role = "cement, dry feed rotary kiln, efficient, with MEA CCS"
    obj.cement_map = {"cement, dry feed rotary kiln": [source], role: []}
    obj.add_to_index = Mock()
    obj.write_log = Mock()
    obj.create_clinker_technology_datasets()
    (new,) = obj.cement_map[role]
    assert source == before
    assert new["name"] == role.replace("cement", "clinker production")
    assert new["reference product"] == source["reference product"]
    assert new["code"] != source["code"]
    assert new["exchanges"][0]["amount"] == 1
    assert "input" not in new["exchanges"][0]
    assert new["exchanges"][1:] == source["exchanges"][1:]
    obj.add_to_index.assert_called_once_with(new)


def test_native_coal_classification_retains_original_coal_only_adjustment():
    obj = cement([])
    obj.fuels_specs = {"hard coal": {"lhv": {"value": 25}}}
    positive = {
        "name": "Hard coal supply mix",
        "product": "Hard coal supply mix",
        "type": "technosphere",
        "unit": "kilogram",
        "amount": 0.00937,
    }
    negative = {**positive, "amount": -1}
    energy = {**positive, "unit": "megajoule"}
    other = {**positive, "name": "Petroleum coke", "product": "Petroleum coke"}
    total, rows = obj._get_hard_coal_energy_and_exchanges(
        {"exchanges": [positive, negative, energy, other]}
    )
    assert rows == [positive]
    assert total == pytest.approx(0.00937 * 25)


def test_role_aliases_work_for_native_and_regional_clinker_without_fuzzy_matches():
    base = activity(CONFIG["clinker_template"])
    regional = {**base, "location": "EUR", "regionalized": True, "code": "regional"}
    wrong = {**base, "reference product": "clinker substitute", "code": "wrong"}
    mapping = InventorySet(
        [base, regional, wrong], version="2026", model="remind", family="uvek"
    )
    matches = mapping.generate_cement_map()
    assert matches["cement, dry feed rotary kiln"] == [base, regional]
    assert matches["cement, dry feed rotary kiln, efficient, with MEA CCS"] == []


@pytest.mark.parametrize("native", [False, True])
def test_clinker_validation_checks_mapped_native_and_standard_producers(native):
    from premise.validation.validation import CementValidation

    ds = activity(
        CONFIG["clinker_template"]
        if native
        else {"name": "clinker production", "reference product": "clinker"}
    )
    ds["location"] = "EUR"
    ds["log parameters"] = {"new accounted fuel energy per kg clinker": 2.0}
    validator = object.__new__(CementValidation)
    validator.database = [ds]
    validator.regions = ["EUR"]
    validator.clinker_production = (
        {(ds["name"], ds["reference product"])} if native else None
    )
    validator.log_issue = Mock()
    validator.check_clinker_energy_use()
    validator.log_issue.assert_called_once()
    assert (
        validator.log_issue.call_args.args[1]
        == "energy use for clinker production too low"
    )
    validator.log_issue.reset_mock()
    ds["log parameters"]["new accounted fuel energy per kg clinker"] = 3.1
    validator.check_clinker_energy_use()
    validator.log_issue.assert_not_called()
