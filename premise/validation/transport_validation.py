"""Read-only exhaust checks against the completed scenario inventory."""

import time

from .validation import CarValidation, TruckValidation
from .validation_framework import (
    ValidationPhaseResult,
    ValidationRuleResult,
    _iter_activity_metadata,
)

# Keep successful outcomes explicit: a cleared warning must replace its earlier
# result even when the final check emits no issue for that rule.
EXHAUST_RULE_IDS = (
    "LEGACY.INVALID_TRANSPORT_EMISSION_AMOUNT",
    "LEGACY.INVALID_TRANSPORT_EMISSION_UNIT",
    "LEGACY.INVALID_TRANSPORT_FUEL_INPUT",
    "LEGACY.TRANSPORT_EMISSION_FUEL_REFERENCE_UNAVAILABLE",
    "LEGACY.TRANSPORT_EMISSION_REFERENCE_UNAVAILABLE",
    "LEGACY.TRANSPORT_EMISSION_SCOPE_UNAVAILABLE",
    "LEGACY.TRANSPORT_EMISSION_UNAVAILABLE",
    "LEGACY.TRANSPORT_HOT_EMISSION_DIAGNOSTIC",
)


def validate_final_transport_emissions(scenario, store, sector_phases):
    """Refresh only the pollutant rules previously checked for cars/trucks.

    Read activity headers first and restore exchanges only for the applicable
    regional vehicles. This avoids materializing the complete inventory graph.
    Fuel-carbon, energy, fleet and linking checks retain their sector results.
    """

    regions = tuple(getattr(scenario.get("iam data"), "regions", ()))
    if not regions:
        return ()
    phase_ids = {phase.phase_id for phase in sector_phases}
    checks = [
        (kind, prefix, validator_class)
        for kind, prefix, validator_class in (
            ("car", "transport, passenger car", CarValidation),
            ("truck", "transport, freight, lorry", TruckValidation),
        )
        if f"sector:{kind}" in phase_ids
    ]
    if not checks:
        return ()
    datasets = {kind: [] for kind, _, _ in checks}
    region_set = set(regions)
    started = time.perf_counter()
    for activity_id, activity in _iter_activity_metadata(store):
        if activity.get("location") not in region_set:
            continue
        for kind, prefix, _ in checks:
            if activity.get("name", "").startswith(prefix) and any(
                fuel in activity["name"]
                for fuel in ("gasoline", "diesel", "compressed gas")
            ):
                datasets[kind].append(store._report_activity_payload(activity_id))

    phases = []
    for kind, prefix, validator_class in checks:
        validator = validator_class(
            model=scenario["model"],
            scenario=scenario["pathway"],
            year=scenario["year"],
            regions=regions,
            database=datasets[kind],
            iam_data=scenario.get("iam data"),
        )
        validator.check_pollutant_emissions(vehicle_name=prefix)
        issues_by_rule = {}
        for issue in validator.validation_issues:
            issues_by_rule.setdefault(issue.rule_id, []).append(issue)
        results = tuple(
            ValidationRuleResult(
                rule_id=rule_id,
                severity=(
                    issues_by_rule[rule_id][0].severity
                    if rule_id in issues_by_rule
                    else (
                        "error" if rule_id.startswith("LEGACY.INVALID_") else "warning"
                    )
                ),
                applicability="applicable" if datasets[kind] else "not_applicable",
                checked_object_count=len(datasets[kind]),
                actual={"issue_count": len(issues_by_rule.get(rule_id, ()))},
                issues=tuple(issues_by_rule.get(rule_id, ())),
            )
            for rule_id in sorted(set(EXHAUST_RULE_IDS) | set(issues_by_rule))
        )
        phases.append(
            ValidationPhaseResult(
                phase_id=f"sector:{kind}:final_exhaust",
                kind="sector",
                rule_results=results,
                elapsed_seconds=time.perf_counter() - started,
                supersedes=tuple(
                    (f"sector:{kind}", rule_id) for rule_id in EXHAUST_RULE_IDS
                ),
            )
        )
        started = time.perf_counter()
    return tuple(phases)
