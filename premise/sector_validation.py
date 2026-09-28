"""Low-cost, read-only contracts shared by all sector transformations."""

from __future__ import annotations

import math
import time
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

import numpy as np

from .validation_framework import (
    ValidationIntent,
    ValidationIssue,
    ValidationPhaseResult,
    ValidationRuleResult,
    record_validation_phase,
)

SECTOR_DATA_ATTRIBUTES = {
    "biomass": ("biomass_mix",),
    "electricity": ("electricity_mix",),
    "cement": ("cement_technology_mix",),
    "steel": ("steel_technology_mix",),
    "fuels": (
        "petrol_blend",
        "diesel_blend",
        "natural_gas_blend",
        "hydrogen_blend",
    ),
    "metals": ("production_volumes",),
    "heat": (
        "buildings_heat_end_use",
        "industrial_heat_end_use",
        "secondary_heat_supply",
    ),
    "cdr": ("cdr_technology_mix",),
    "battery": ("battery_mobile_scenarios", "battery_stationary_scenarios"),
    "cars": ("passenger_car_fleet",),
    "two_wheelers": ("two_wheelers_fleet",),
    "trucks": ("road_freight_fleet",),
    "ships": ("sea_freight_fleet",),
    "buses": ("bus_fleet",),
    "trains": ("rail_freight_fleet",),
    "final energy": ("final_energy_use",),
    "emissions": ("gains_data_IAM",),
}

MAPPING_KEYS = {
    "cars": "car",
    "two_wheelers": "two-wheeler",
    "trucks": "truck",
    "ships": "ship",
    "buses": "bus",
    "trains": "train",
}

UNIT_SHARE_MARKET_SECTORS = frozenset(
    {"battery", "cdr", "external", "metals", "mining"}
)
_VALIDATION_RELINKED_TARGETS_KEY = "__premise_validation_relinked_targets_v1__"
_VALIDATION_ADDED_TARGETS_KEY = "__premise_validation_added_targets_v1__"


def _finite_scalar(value: Any) -> float | None:
    """Return a finite scalar without normalizing or mutating its owner."""

    if isinstance(value, (bool, np.bool_)):
        return None
    if isinstance(value, np.ndarray):
        if value.size != 1:
            return None
        value = value.reshape(-1)[0]
    if not isinstance(value, (int, float, np.number)):
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def _is_dataset(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and "name" in value
        and ("reference product" in value or "exchanges" in value)
    )


def _datasets(value: Any) -> Iterable[Mapping[str, Any]]:
    if _is_dataset(value):
        yield value
    elif isinstance(value, Mapping):
        for nested in value.values():
            yield from _datasets(nested)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for nested in value:
            yield from _datasets(nested)


def _mapping_targets(scenario: Mapping[str, Any], sector: str) -> tuple[dict, ...]:
    mapping = scenario.get("mapping", {})
    keys = [MAPPING_KEYS.get(sector, sector)]
    if sector == "external":
        keys = [key for key in mapping if str(key).startswith("external_")]
    targets = []
    seen = set()
    for key in keys:
        for dataset in _datasets(mapping.get(key, ())):
            marker = id(dataset)
            if marker not in seen:
                targets.append(dataset)
                seen.add(marker)
    return tuple(targets)


def _static_targets(
    database: Iterable[Mapping[str, Any]], sector: str
) -> tuple[dict, ...]:
    predicates = {
        "renewable": lambda ds: "direct drive" in str(ds.get("name", "")),
        "mining": lambda ds: "tailings" in str(ds.get("name", "")).lower(),
        "emissions": lambda ds: bool(ds.get("log parameters")),
    }
    predicate = predicates.get(sector)
    if predicate is None:
        return ()
    return tuple(dataset for dataset in database if predicate(dataset))


def _sector_applicable(scenario: Mapping[str, Any], sector: str) -> bool:
    if sector in {"renewable", "mining"}:
        return True
    if sector == "external":
        return bool(scenario.get("external scenarios"))
    iam_data = scenario.get("iam data")
    return iam_data is not None and any(
        getattr(iam_data, attribute, None) is not None
        for attribute in SECTOR_DATA_ATTRIBUTES.get(sector, ())
    )


def _result(
    rule_id: str,
    *,
    applicability: str,
    checked: int,
    issues: Iterable[ValidationIssue] = (),
    expected: Any = None,
    actual: Any = None,
    tolerance: float | None = None,
) -> ValidationRuleResult:
    issues = tuple(replace(issue, checked_object_count=checked) for issue in issues)
    return ValidationRuleResult(
        rule_id=rule_id,
        severity="error",
        applicability=applicability,
        checked_object_count=checked,
        expected=expected,
        actual=actual if actual is not None else {"issue_count": len(issues)},
        tolerance=tolerance,
        issues=issues,
    )


def validate_sector_contract(
    scenario: dict[str, Any], sector: str
) -> ValidationPhaseResult:
    """Evaluate the common target, vector, and physical contracts for a sector."""

    started = time.perf_counter()
    prefix = sector.upper().replace(" ", "_").replace("-", "_")
    applicable = _sector_applicable(scenario, sector)
    database = scenario.get("_inventory_working_copy")
    targets = _mapping_targets(scenario, sector)
    relinked_targets = tuple(
        scenario.get("cache", {}).get(_VALIDATION_RELINKED_TARGETS_KEY, {}).values()
    )
    added_targets = tuple(
        scenario.get("cache", {}).get(_VALIDATION_ADDED_TARGETS_KEY, {}).values()
    )
    if not targets and isinstance(database, list):
        targets = _static_targets(database, sector)
    scope_targets = tuple(
        {
            id(dataset): dataset
            for dataset in (*targets, *relinked_targets, *added_targets)
        }.values()
    )

    added_keys = frozenset(
        (
            dataset.get("name"),
            dataset.get("reference product", dataset.get("product")),
            dataset.get("location"),
        )
        for dataset in added_targets
    )

    affected_keys = {
        (
            dataset.get("name"),
            dataset.get("reference product", dataset.get("product")),
            dataset.get("location"),
        )
        for dataset in scope_targets
    }
    direct_targets = scenario.get("_validation_direct_targets", {}).get(sector, {})
    affected_keys.update(tuple(key) for key in direct_targets.get("activity_keys", ()))
    affected_keys = frozenset(affected_keys)
    affected_activity_ids = frozenset(direct_targets.get("activity_ids", ()))
    intent = ValidationIntent(
        transformation=sector,
        applicability="applicable" if applicable else "not_applicable",
        applicability_reason=(
            None if applicable else "no applicable IAM or external scenario data"
        ),
        targeted=True,
        scope_complete=True,
        affected_activity_ids=affected_activity_ids,
        affected_activity_keys=affected_keys,
        allowed_added_keys=added_keys,
        expected_match_count=len(affected_keys) + len(affected_activity_ids),
        algorithm=(
            "marginal mix"
            if sector in {"electricity", "fuels"}
            and getattr(scenario.get("iam data"), "system_model", None)
            == "consequential"
            else (
                "average production-volume mix"
                if sector in {"electricity", "fuels"}
                else None
            )
        ),
        computed_target_values={
            "declared_target_count": len(targets),
            "declared_scope_count": len(affected_keys),
            "observed_regions": sorted(
                {key[2] for key in affected_keys if key[2] is not None}
            ),
        },
    )
    intents = scenario.setdefault("_validation_intents", {})
    intents[sector] = intent.to_dict()

    if not applicable:
        results = (
            _result(
                f"METHOD.{prefix}.TARGET_COVERAGE",
                applicability="not_applicable",
                checked=0,
                expected="sector data available",
                actual="no applicable IAM or external scenario data",
            ),
        )
    else:
        coverage_issues = []
        if database is not None and not targets and sector != "emissions":
            coverage_issues.append(
                ValidationIssue(
                    rule_id=f"METHOD.{prefix}.TARGET_COVERAGE",
                    severity="error",
                    message="Applicable sector transformation declared no validation targets.",
                    expected=">= 1 target",
                    actual=0,
                )
            )
        results_list = [
            _result(
                f"METHOD.{prefix}.TARGET_COVERAGE",
                applicability="applicable",
                checked=len(targets),
                issues=coverage_issues,
                expected=">= 1 target",
                actual=len(targets),
            )
        ]

        physical_issues = []
        checked_exchanges = 0
        for dataset in targets:
            key = (
                dataset.get("name"),
                dataset.get("reference product"),
                dataset.get("location"),
            )
            for exchange in dataset.get("exchanges", ()):
                checked_exchanges += 1
                amount = exchange.get("amount")
                numeric_amount = _finite_scalar(amount)
                if numeric_amount is None:
                    physical_issues.append(
                        ValidationIssue(
                            rule_id=f"METHOD.{prefix}.PHYSICAL_BOUNDS",
                            severity="error",
                            message="Sector target contains a non-finite exchange amount.",
                            activity_key=key,
                            expected="finite numeric amount",
                            actual=amount,
                        )
                    )
                if (
                    exchange.get("type") == "production"
                    and exchange.get("name") == dataset.get("name")
                    and numeric_amount is not None
                    and numeric_amount == 0.0
                ):
                    physical_issues.append(
                        ValidationIssue(
                            rule_id=f"METHOD.{prefix}.PHYSICAL_BOUNDS",
                            severity="error",
                            message="Sector target has a zero reference-production amount.",
                            activity_key=key,
                            expected="non-zero production amount",
                            actual=amount,
                        )
                    )
        results_list.append(
            _result(
                f"METHOD.{prefix}.PHYSICAL_BOUNDS",
                applicability="applicable" if targets else "not_applicable",
                checked=checked_exchanges,
                issues=physical_issues,
            )
        )

        market_issues = []
        checked_markets = 0
        for dataset in targets if sector in UNIT_SHARE_MARKET_SECTORS else ():
            if not str(dataset.get("name", "")).startswith(
                ("market for ", "market group for ")
            ):
                continue
            text = (
                f"{dataset.get('name', '')} {dataset.get('reference product', '')}"
            ).lower()
            waste_market = any(
                token in text
                for token in (
                    "waste",
                    "treatment",
                    "scrap",
                    "residue",
                    "sewage",
                    "sludge",
                    "tailing",
                )
            )
            shares = [
                _finite_scalar(exchange.get("amount"))
                for exchange in dataset.get("exchanges", ())
                if exchange.get("type") == "technosphere"
                and exchange.get("unit") == dataset.get("unit")
                and _finite_scalar(exchange.get("amount")) is not None
            ]
            if not shares:
                continue
            checked_markets += 1
            total = sum(shares)
            valid_total = math.isclose(
                abs(total) if waste_market else total,
                1.0,
                rel_tol=1e-5,
                abs_tol=1e-8,
            )
            if not valid_total:
                market_issues.append(
                    ValidationIssue(
                        rule_id=f"METHOD.{prefix}.MARKET_COMPOSITION",
                        severity="error",
                        message="Sector market supplier shares do not sum to one.",
                        activity_key=(
                            dataset.get("name"),
                            dataset.get("reference product"),
                            dataset.get("location"),
                        ),
                        expected="unit magnitude" if waste_market else 1.0,
                        actual=total,
                        tolerance=1e-5,
                    )
                )
        results_list.append(
            _result(
                f"METHOD.{prefix}.MARKET_COMPOSITION",
                applicability="applicable" if checked_markets else "not_applicable",
                checked=checked_markets,
                issues=market_issues,
                expected=1.0 if checked_markets else None,
                tolerance=1e-5 if checked_markets else None,
            )
        )
        results = tuple(results_list)

    phase = ValidationPhaseResult(
        phase_id=f"sector:{sector}:contract",
        kind="sector",
        rule_results=results,
        elapsed_seconds=time.perf_counter() - started,
    )
    record_validation_phase(scenario, phase)
    return phase


__all__ = ["validate_sector_contract"]
