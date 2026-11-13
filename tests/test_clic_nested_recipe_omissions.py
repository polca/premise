"""Nested audits authorize literal omissions at their own reference basis."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from premise.inventory.clic_rule_approvals import approved_recipe_source_omission


def case():
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    omission = {"source_exchange": {"amount": -0.1}, "original_index": 2}
    child = {
        "source": {"unit": "kilogram"},
        "source_profile": profile,
        "approved_recipe_source_omissions": {"child": [omission]},
    }
    inspection = {
        "source": {"unit": "megajoule"},
        "source_profile": profile,
        "nested_recipe_inspections": [child],
    }
    details = {
        "recipe_id": "child",
        "omission_index": 0,
        "omission": deepcopy(omission),
        "source_profile": deepcopy(profile),
        "basis": {"amount": 1, "unit": "kilogram"},
    }
    return details, {"premise_compatibility_reviews": [{"inspection": inspection}]}


def loss(details, code="migration.recipe_source_input_omission"):
    return SimpleNamespace(code=code, to_dict=lambda: {"details": details})


def test_explicit_child_audit_retains_its_own_basis():
    details, report = case()
    assert approved_recipe_source_omission(loss(details), report)


@pytest.mark.parametrize(
    "change",
    [
        "missing_child",
        "unmatched_parent",
        "index",
        "negative_index",
        "boolean_index",
        "literal",
        "profile",
        "recipe",
        "basis_unit",
        "basis_amount",
        "other_loss",
    ],
)
def test_child_audit_does_not_broaden_omission_permission(change):
    details, report = case()
    code = "migration.recipe_source_input_omission"
    if change == "missing_child":
        report["premise_compatibility_reviews"][0]["inspection"].pop(
            "nested_recipe_inspections"
        )
    elif change == "unmatched_parent":
        report["premise_compatibility_reviews"] = []
    elif change == "index":
        details["omission_index"] = 1
    elif change == "negative_index":
        details["omission_index"] = -1
    elif change == "boolean_index":
        details["omission_index"] = False
    elif change == "literal":
        details["omission"]["source_exchange"]["amount"] = -0.2
    elif change == "profile":
        details["source_profile"]["version"] = "3.10"
    elif change == "recipe":
        details["recipe_id"] = "other"
    elif change == "basis_unit":
        details["basis"]["unit"] = "megajoule"
    elif change == "basis_amount":
        details["basis"]["amount"] = 2
    else:
        code = "migration.other_loss"
    assert not approved_recipe_source_omission(loss(details, code), report)


def test_existing_top_level_audit_keeps_its_behavior():
    details, report = case()
    parent = report["premise_compatibility_reviews"][0]
    parent["inspection"] = parent["inspection"]["nested_recipe_inspections"][0]
    assert approved_recipe_source_omission(loss(details), report)
