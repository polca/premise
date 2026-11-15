"""Source comparisons explain quality flags without weakening validation."""

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from premise.validation import ElectricityValidation, ValidationIssue


def power_plant(efficiency=0.4, carbon_factor=1.0, location="GLO"):
    fuel = 3.6 / (efficiency * 36)
    return {
        "name": "electricity production, natural gas, conventional power plant",
        "reference product": "electricity, high voltage",
        "location": location,
        "unit": "kilowatt hour",
        "exchanges": [
            {
                "name": "market for natural gas, high pressure",
                "type": "technosphere",
                "unit": "cubic meter",
                "amount": fuel,
            },
            {
                "name": "Carbon dioxide, fossil",
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air",),
                "amount": fuel * 36 * 0.054 * carbon_factor,
            },
        ],
    }


def validator(database):
    obj = object.__new__(ElectricityValidation)
    obj.database = database
    obj.major_issues_log, obj.minor_issues_log, obj.validation_issues = [], [], []
    return obj


@pytest.mark.parametrize(
    "kind,before,after,origin,change",
    [
        ("efficiency", 0.20, 0.22, "inherited", "improved"),
        ("efficiency", 0.20, 0.20, "inherited", "unchanged"),
        ("efficiency", 0.22, 0.18, "inherited", "worsened"),
        ("efficiency", 0.40, 0.20, "new", "new discrepancy"),
        ("carbon_factor", 0.6, 0.7, "inherited", "improved"),
        ("carbon_factor", 0.6, 0.6, "inherited", "unchanged"),
        ("carbon_factor", 0.7, 0.5, "inherited", "worsened"),
        ("carbon_factor", 1.0, 0.6, "new", "new discrepancy"),
    ],
)
def test_quality_warnings_retain_source_and_scenario_measurements(
    kind, before, after, origin, change
):
    source = power_plant(**{kind: before})
    current = power_plant(**{kind: after})
    original = deepcopy([source, current])
    source_validator = validator([source])
    snapshot = source_validator.check_efficiency(capture_only=True)
    snapshot_copy = deepcopy(snapshot)
    assert source_validator.validation_issues == []
    assert source_validator.minor_issues_log == []
    obj = validator([current])
    obj.source_electricity_checks = snapshot

    obj.check_efficiency()

    (issue,) = obj.validation_issues
    restored = ValidationIssue.from_dict(json.loads(json.dumps(issue.to_dict())))
    assert restored.severity == "warning"
    assert restored.actual["origin"] == origin
    assert restored.actual["change"] == change
    assert restored.actual["source_match_count"] == 1
    assert restored.actual["source"]["failed"] == (origin == "inherited")
    assert restored.actual["scenario"]["failed"]
    assert (
        restored.actual["source"]["observed"] != restored.actual["scenario"]["observed"]
        or before == after
    )
    assert restored.expected == restored.actual["scenario"]["expected"]
    assert snapshot == snapshot_copy
    assert [source, current] == original


@pytest.mark.parametrize(
    "kind,before,after", [("efficiency", 0.2, 0.4), ("carbon_factor", 0.6, 1.0)]
)
def test_resolved_source_quality_warnings_are_not_reintroduced(kind, before, after):
    snapshot = validator([power_plant(**{kind: before})]).check_efficiency(
        capture_only=True
    )
    obj = validator([power_plant(**{kind: after})])
    obj.source_electricity_checks = snapshot

    obj.check_efficiency()

    assert obj.validation_issues == []


@pytest.mark.parametrize("ambiguous", [False, True])
def test_missing_or_ambiguous_source_counterparts_remain_unresolved(ambiguous):
    source = power_plant(carbon_factor=0.6)
    current = deepcopy(source)
    sources = [source, deepcopy(source)] if ambiguous else []
    obj = validator([current])
    obj.source_electricity_checks = validator(sources).check_efficiency(
        capture_only=True
    )

    obj.check_efficiency()

    (issue,) = obj.validation_issues
    assert issue.actual["origin"] == "unresolved"
    assert issue.actual["source"] is None
    assert issue.actual["source_match_count"] == (2 if ambiguous else 0)


def test_changed_combustion_boundary_is_not_treated_as_an_inherited_mismatch():
    source = power_plant()
    source["exchanges"] = [
        {
            "name": "Carbon dioxide, fossil",
            "type": "biosphere",
            "unit": "kilogram",
            "categories": ("air",),
            "amount": -0.4,
        },
        {
            "name": "carbon dioxide, captured",
            "type": "technosphere",
            "unit": "kilogram",
            "amount": 0.4,
        },
    ]
    obj = validator([power_plant(carbon_factor=0.6)])
    obj.source_electricity_checks = validator([source]).check_efficiency(
        capture_only=True
    )

    obj.check_efficiency()

    (issue,) = obj.validation_issues
    assert issue.actual["origin"] == "unresolved"
    assert issue.actual["change"] == "check boundary changed"


def test_invalid_capture_measurements_do_not_receive_an_improvement_label():
    source = power_plant(carbon_factor=0.6)
    source["exchanges"].append(
        {
            "name": "carbon dioxide, captured",
            "type": "technosphere",
            "unit": "kilogram",
            "amount": -0.1,
        }
    )
    obj = validator([power_plant(carbon_factor=0.7)])
    obj.source_electricity_checks = validator([source]).check_efficiency(
        capture_only=True
    )

    obj.check_efficiency()

    (issue,) = obj.validation_issues
    assert issue.actual["origin"] == "unresolved"
    assert issue.actual["change"] == "invalid measurements prevent comparison"


def test_electricity_update_captures_quality_before_changing_inventory(monkeypatch):
    import premise.electricity.electricity as module

    class MinimalValidation(ElectricityValidation):
        def __init__(self, **kwargs):
            self.database = kwargs["database"]
            self.model, self.scenario, self.year = "image", "path", 2050
            self.major_issues_log, self.minor_issues_log, self.validation_issues = (
                [],
                [],
                [],
            )

        def run_supplier_vector_checks(self):
            return None

        def run_electricity_checks(self, **kwargs):
            self.check_efficiency()
            return self._finalize_logs()

    class Transformation:
        def __init__(self, **kwargs):
            self.database = kwargs["database"]
            self.powerplant_map, self.index, self.cache = {}, {}, {}

        def update_electricity_efficiency(self):
            self.database[0]["exchanges"] = power_plant(efficiency=0.22)["exchanges"]

    for name in (
        "create_missing_power_plant_datasets",
        "adjust_coal_power_plant_emissions",
        "update_efficiency_of_solar_pv",
        "correct_hydropower_water_emissions",
        "create_region_specific_power_plants",
        "adjust_aluminium_electricity_markets",
        "update_electricity_markets",
        "clear_validation_provenance",
        "relink_datasets",
    ):
        setattr(Transformation, name, lambda self: None)
    reports = []
    monkeypatch.setattr(module, "ElectricityValidation", MinimalValidation)
    monkeypatch.setattr(module, "Electricity", Transformation)
    monkeypatch.setattr(module, "get_scenario_inventory", lambda s: s["database"])
    monkeypatch.setattr(
        module, "replace_scenario_inventory", lambda s, db: s.update(database=db)
    )
    monkeypatch.setattr(
        module,
        "record_validation_phase",
        lambda s, report: reports.append(report) if report is not None else None,
    )
    scenario = {
        "model": "image",
        "pathway": "path",
        "year": 2050,
        "database": [power_plant(efficiency=0.2)],
        "iam data": SimpleNamespace(
            regions=["GLO"],
            electricity_mix=object(),
            electricity_technology_efficiencies=object(),
        ),
    }

    module._update_electricity(scenario, "3.12", "cutoff", False)

    (report,) = reports
    (issue,) = report.warnings
    assert issue.actual["source"]["observed"] == pytest.approx(0.2)
    assert issue.actual["scenario"]["observed"] == pytest.approx(0.22)
    assert issue.actual["origin"] == "inherited"
    assert issue.actual["change"] == "improved"
