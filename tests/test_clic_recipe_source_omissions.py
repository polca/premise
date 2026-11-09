"""A matched review permits only its exact declared recipe-source losses."""

from copy import deepcopy

import pytest
from brightpath.core.reports import Loss, StageKind
from premise.inventory.clic_rule_approvals import approved_recipe_source_omission


@pytest.mark.parametrize(
    "change",
    [
        "none",
        "amount",
        "recipe",
        "profile",
        "basis",
        "index",
        "missing_review",
        "biosphere_loss",
    ],
)
def test_recipe_source_loss_gate_is_exact(change):
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    omission = {"source_exchange": {"amount": 1e-13}, "original_index": 2}
    inspection = {
        "source_profile": profile,
        "source": {"unit": "kilogram"},
        "approved_recipe_source_omissions": {"reviewed-recipe": [omission]},
    }
    report = {
        "premise_compatibility_reviews": [
            {"fingerprint": "exact-matched-scope", "inspection": inspection}
        ]
    }
    details = {
        "recipe_id": "reviewed-recipe",
        "omission_index": 0,
        "omission": deepcopy(omission),
        "source_profile": deepcopy(profile),
        "basis": {"amount": 1, "unit": "kilogram"},
    }
    code = "migration.recipe_source_input_omission"
    if change == "amount":
        details["omission"]["source_exchange"]["amount"] = 2e-13
    elif change == "recipe":
        details["recipe_id"] = "other-recipe"
    elif change == "profile":
        details["source_profile"]["version"] = "3.12"
    elif change == "basis":
        details["basis"]["unit"] = "cubic meter"
    elif change == "index":
        details["omission_index"] = True
    elif change == "missing_review":
        report["premise_compatibility_reviews"] = []
    elif change == "biosphere_loss":
        code = "migration.recipe_input_omission"
    loss = Loss(
        code,
        "Explicit scoped omission",
        StageKind.BACKGROUND_MIGRATION,
        details=details,
    )
    assert approved_recipe_source_omission(loss, report) is (change == "none")
