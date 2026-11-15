"""Group repeated coverage gaps while retaining every preservation decision."""

import json
from copy import deepcopy
from types import SimpleNamespace

from premise.validation import MetalsValidation
from premise.validation.validation_framework import ValidationReport

YEAR_RULE = "LEGACY.METAL_MARKET_PRESERVED_YEAR_OUTSIDE_SUPPORT"


def year_decision(metal, requested=2050, first=2020, last=2030):
    return {
        "reason": "market_preserved_year_outside_support",
        "metal": metal,
        "requested_year": requested,
        "first_year": first,
        "last_year": last,
    }


def validator_for(decisions):
    validator = MetalsValidation(
        "image", "path", 2050, ["WEU"], [], SimpleNamespace(), "cutoff", "3.12"
    )
    validator.metals_list = []
    validator.market_diagnostics = decisions
    return validator


def test_shared_market_year_gap_keeps_all_decisions_in_serialized_certificate():
    decisions = [year_decision(f"metal {index}") for index in range(103)]
    before = deepcopy(decisions)
    validator = validator_for(decisions)

    report = validator.run_metals_checks()
    restored = ValidationReport.from_dict(json.loads(json.dumps(report.to_dict())))

    assert not restored.errors
    assert len(restored.warnings) == 1
    issue = restored.warnings[0]
    assert issue.rule_id == YEAR_RULE
    assert issue.checked_object_count == 103
    assert issue.expected == {"requested_year_within": (2020, 2030)}
    assert issue.actual["requested_year"] == 2050
    assert issue.actual["first_year"] == 2020
    assert issue.actual["last_year"] == 2030
    assert issue.actual["preserved_market_count"] == 103
    assert [dict(d) for d in issue.actual["market_decisions"]] == before
    assert "Preserved 103 metal markets" in issue.message
    assert "2050" in issue.message and "2020–2030" in issue.message
    (result,) = restored.get_phase("sector:metals").rule_results
    assert result.checked_object_count == 103
    assert result.actual == {"issue_count": 1}
    assert len(validator.minor_issues_log) == 1
    assert decisions == before


def test_distinct_requested_years_and_source_ranges_remain_separate_warnings():
    decisions = [
        year_decision("copper"),
        year_decision("zinc"),
        year_decision("lead", last=2025),
        year_decision("nickel", requested=2060),
    ]
    validator = validator_for(decisions)

    report = validator.run_metals_checks()

    assert len(report.warnings) == 3
    groups = {
        (i.actual["requested_year"], i.actual["first_year"], i.actual["last_year"]): {
            d["metal"] for d in i.actual["market_decisions"]
        }
        for i in report.warnings
    }
    assert groups == {
        (2050, 2020, 2030): {"copper", "zinc"},
        (2050, 2020, 2025): {"lead"},
        (2060, 2020, 2030): {"nickel"},
    }
    (result,) = report.get_phase("sector:metals").rule_results
    assert result.checked_object_count == 4


def test_other_market_failures_and_incomplete_year_diagnostics_stay_individual():
    decisions = [
        year_decision("copper"),
        year_decision("zinc"),
        {"reason": "market_preserved_supplier_missing", "metal": "lead"},
        {"reason": "market_preserved_transport_missing", "metal": "nickel"},
        {"reason": "market_preserved_year_outside_support", "metal": "silver"},
    ]
    validator = validator_for(decisions)

    report = validator.run_metals_checks()

    assert len(report.warnings) == 4
    assert {i.rule_id for i in report.warnings} == {
        YEAR_RULE,
        "LEGACY.METAL_MARKET_PRESERVED_SUPPLIER_MISSING",
        "LEGACY.METAL_MARKET_PRESERVED_TRANSPORT_MISSING",
    }
    individual = {i.activity_key[0] for i in report.warnings if i.actual is None}
    assert individual == {"lead", "nickel", "silver"}
    (summary,) = [i for i in report.warnings if i.actual is not None]
    assert {d["metal"] for d in summary.actual["market_decisions"]} == {
        "copper",
        "zinc",
    }
    assert sum(i.checked_object_count for i in report.warnings) == 5


def test_no_market_year_gap_adds_no_summary_warning():
    report = validator_for([]).run_metals_checks()

    assert not report.warnings
    assert not report.errors
