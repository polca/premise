"""Final exhaust results replace stage diagnostics without editing inventories."""

import copy
from types import SimpleNamespace

import pytest

import premise.validation.validation as validation_module
from premise.inventory.inventory_store import (
    CompactInventoryStore,
    LegacyInventoryStore,
)
from premise.database.new_database import NewDatabase
from premise.reporting.change_report import ReportScenario, _validation_rows
from premise.validation.transport_validation import validate_final_transport_emissions
from premise.validation.validation_framework import (
    ValidationIssue,
    ValidationPhaseResult,
    ValidationReport,
    ValidationRuleResult,
    inventory_store_fingerprint,
)

HOT = "LEGACY.TRANSPORT_HOT_EMISSION_DIAGNOSTIC"
OTHER = "LEGACY.VEHICLE_FUEL_CARBON_UNAVAILABLE"


def rule(rule_id, issues=()):
    return ValidationRuleResult(
        rule_id=rule_id,
        severity="warning",
        applicability="applicable",
        checked_object_count=2,
        issues=issues,
    )


def vehicle(kind, location, ratio):
    if kind == "car":
        name = "transport, passenger car, gasoline, Medium, EURO-6"
        fuel, energy = "market for petrol, low-sulfur", 42.4
    else:
        name = "transport, freight, lorry, diesel, 18 metric ton gross weight, EURO-VI"
        fuel, energy = "market for diesel, low-sulfur", 43.0
    return {
        "name": name,
        "reference product": "transport service",
        "location": location,
        "unit": "kilometer",
        "exchanges": [
            {
                "name": name,
                "product": "transport service",
                "location": location,
                "unit": "kilometer",
                "type": "production",
                "amount": 1.0,
            },
            {
                "name": fuel,
                "product": "fuel",
                "location": "GLO",
                "unit": "kilogram",
                "type": "technosphere",
                "amount": 1 / energy,
            },
            {
                "name": "Nitrogen oxides",
                "categories": ("air",),
                "unit": "kilogram",
                "type": "biosphere",
                "amount": ratio / 1000,
            },
        ],
    }


@pytest.fixture
def exhaust_references(monkeypatch):
    monkeypatch.setattr(
        validation_module,
        "load_car_exhaust_pollutants",
        lambda: {"gasoline": {"6.2": {"Nitrogen oxides": 1.0}}},
    )
    monkeypatch.setattr(
        validation_module,
        "load_truck_exhaust_pollutants",
        lambda: {"diesel": {"6": {"18 metric ton": {"Nitrogen oxides": 1.0}}}},
    )


@pytest.mark.parametrize("kind", ["car", "truck"])
@pytest.mark.parametrize("store_class", [CompactInventoryStore, LegacyInventoryStore])
def test_final_exhaust_clears_stale_and_finds_new_warnings(
    kind, store_class, exhaust_references, monkeypatch
):
    inventory = [
        vehicle(kind, "BRA", 1),
        vehicle(kind, "CHN", 3),
        vehicle(kind, "RER", 10),
    ]
    before = copy.deepcopy(inventory)
    store = store_class(inventory)
    fingerprint = inventory_store_fingerprint(store)
    monkeypatch.setattr(
        store,
        "iter_activities",
        lambda: pytest.fail("must read headers without exchanges"),
    )
    old = ValidationIssue(
        rule_id=HOT,
        severity="warning",
        message="old BRA NOx",
        activity_key=(inventory[0]["name"], "transport service", "BRA"),
    )
    other = ValidationIssue(
        rule_id=OTHER, severity="warning", message="fuel properties"
    )
    stage = ValidationPhaseResult(
        phase_id=f"sector:{kind}",
        kind="sector",
        rule_results=(rule(HOT, (old,)), rule(OTHER, (other,))),
    )
    scenario = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "iam data": SimpleNamespace(regions=["BRA", "CHN"]),
    }

    phases = validate_final_transport_emissions(scenario, store, [stage])
    report = ValidationReport(
        scenario_identity="scenario",
        store_generation=0,
        ruleset_version=8,
        certificate_key="key",
        rule_results=(),
        phase_results=(stage, *phases),
    )

    hot = [issue for issue in report.warnings if issue.rule_id == HOT]
    assert len(hot) == 1
    assert hot[0].activity_key[2] == "CHN"
    assert other in report.warnings
    assert old in report.get_phase(f"sector:{kind}").issues
    assert (
        report.superseded_rules[f"sector:{kind}", HOT] == f"sector:{kind}:final_exhaust"
    )
    assert all(result.checked_object_count == 2 for result in phases[0].rule_results)
    assert inventory == before
    assert inventory_store_fingerprint(store) == fingerprint
    restored = ValidationReport.from_dict(report.to_dict())
    assert restored.warnings == report.warnings
    assert restored.phase_results == report.phase_results
    findings, coverage = _validation_rows(
        [
            ReportScenario(
                identity=("image", "path", 2050),
                store=store,
                validation_report=restored,
            )
        ]
    )
    assert all(row["message"] != old.message for row in findings)
    assert any(row["message"] == hot[0].message for row in findings)
    historical = next(
        row
        for row in coverage
        if row["phase"] == f"sector:{kind}" and row["rule ID"] == HOT
    )
    assert historical["superseded by"] == f"sector:{kind}:final_exhaust"


def test_final_exhaust_returns_new_errors_without_mutating_inventory(
    exhaust_references,
):
    store = CompactInventoryStore([vehicle("car", "BRA", -1)])
    stage = ValidationPhaseResult(phase_id="sector:car", kind="sector")
    scenario = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "iam data": SimpleNamespace(regions=["BRA"]),
    }

    phases = validate_final_transport_emissions(scenario, store, [stage])

    assert [issue.rule_id for issue in phases[0].errors] == [
        "LEGACY.INVALID_TRANSPORT_EMISSION_AMOUNT"
    ]


def test_no_transport_stage_does_not_scan_inventory(exhaust_references, monkeypatch):
    store = CompactInventoryStore([])
    monkeypatch.setattr(
        store, "_iter_storage_activities", lambda: pytest.fail("unneeded scan")
    )
    scenario = {"iam data": SimpleNamespace(regions=["BRA"])}
    assert validate_final_transport_emissions(scenario, store, []) == ()


def test_certification_refreshes_exhaust_and_persists_history(
    exhaust_references, tmp_path, monkeypatch
):
    old = ValidationIssue(rule_id=HOT, severity="warning", message="old BRA NOx")
    stage = ValidationPhaseResult(
        phase_id="sector:car", kind="sector", rule_results=(rule(HOT, (old,)),)
    )
    scenario = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "iam data": SimpleNamespace(regions=["BRA"]),
        "_validation_phase_results": [stage.to_dict()],
    }
    store = CompactInventoryStore([vehicle("car", "BRA", 1)])
    database = object.__new__(NewDatabase)
    database._validation_enabled = True
    monkeypatch.setattr(database, "_validation_source_fingerprint", lambda: "source")
    monkeypatch.setattr(database, "_validation_iam_fingerprint", lambda scenario: "iam")

    certificate = database._certify_scenario_store(scenario, store)
    assert certificate.report.warnings == ()
    assert certificate.report.get_phase("sector:car").warnings == (old,)
    checkpoint = store.checkpoint(tmp_path / "scenario")
    restored = CompactInventoryStore.open(checkpoint)
    persisted = ValidationReport.from_dict(
        restored._validation_certificate_payload["report"]
    )
    assert persisted.warnings == ()
    assert persisted.get_phase("sector:car").warnings == (old,)

    # A later certification must refresh an existing final phase, not append a
    # duplicate or retain the earlier successful result after a mutation.
    scenario["_validation_phase_results"] = [
        p.to_dict() for p in certificate.report.phase_results
    ]
    with store.transaction("later emission change") as transaction:
        transaction.patch_exchange(2, {"amount": 0.003})
    latest = database._certify_scenario_store(scenario, store)
    assert (
        len(
            [
                p
                for p in latest.report.phase_results
                if p.phase_id == "sector:car:final_exhaust"
            ]
        )
        == 1
    )
    assert [issue.rule_id for issue in latest.report.warnings] == [HOT]


def test_empty_final_check_explicitly_replaces_previous_warning(exhaust_references):
    old = ValidationIssue(rule_id=HOT, severity="warning", message="old")
    stage = ValidationPhaseResult(
        phase_id="sector:car", kind="sector", rule_results=(rule(HOT, (old,)),)
    )
    scenario = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "iam data": SimpleNamespace(regions=["BRA"]),
    }
    phases = validate_final_transport_emissions(
        scenario, CompactInventoryStore([]), [stage]
    )
    report = ValidationReport(
        scenario_identity="scenario",
        store_generation=0,
        ruleset_version=8,
        certificate_key="key",
        rule_results=(),
        phase_results=(stage, *phases),
    )
    assert report.warnings == ()
    assert report.phase_results[0].warnings == (old,)


@pytest.mark.parametrize("replacement_rules", [(), (rule("OTHER"),)])
def test_phase_cannot_clear_a_rule_it_did_not_evaluate(replacement_rules):
    old = ValidationIssue(rule_id=HOT, severity="error", message="old error")
    stage = ValidationPhaseResult(
        phase_id="sector:car", kind="sector", rule_results=(rule(HOT, (old,)),)
    )
    final = ValidationPhaseResult(
        phase_id="sector:car:final_exhaust",
        kind="sector",
        rule_results=replacement_rules,
        supersedes=(("sector:car", HOT),),
    )
    report = ValidationReport(
        scenario_identity="scenario",
        store_generation=0,
        ruleset_version=8,
        certificate_key="key",
        rule_results=(),
        phase_results=(stage, final),
    )
    assert report.errors == (old,)
