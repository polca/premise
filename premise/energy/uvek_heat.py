"""Apply reviewed native heat plans through the standard scenario-store bridge."""

from __future__ import annotations

import copy
import json
import math
import warnings
from collections import Counter
from types import SimpleNamespace

from ..core.transformation import (
    _VALIDATION_ADDED_TARGETS_KEY,
    _VALIDATION_RELINKED_TARGETS_KEY,
)
from ..iam.geomap import Geomap
from ..inventory.inventory_store import (
    get_scenario_inventory,
    replace_scenario_inventory,
)
from ..inventory.profiles import identity
from ..reporting.provenance import record_change_event
from ..validation.validation_framework import (
    ValidationIssue,
    ValidationPhaseResult,
    ValidationRuleResult,
    record_validation_phase,
)
from .uvek_heat_consumers import (
    AUDIT_FIELD,
    REVIEW_PATH,
    REVIEW_REFERENCE,
    _protected,
    _same,
    plan_native_heat_consumers,
)
from .uvek_heat_markets import plan_native_heat_markets

BACKGROUND_CONTEXT = {
    "family": "uvek",
    "version": "2026",
    "system_model": "cutoff",
    "biosphere_family": "ecoinvent",
    "biosphere_version": "3.10",
}


def _key(record):
    return record["database"], record["code"]


def _check_result(original, result, heat, consumers):
    """Verify complete links, planned records and unchanged physical inventories."""
    providers = {_key(a): a for a in result}
    if len(providers) != len(result):
        raise ValueError("Native heat result contains duplicate activity keys.")
    expected = {
        tuple(r["source_key"]): r["activity"] for r in consumers["replacements"]
    }
    originals = {_key(a): a for a in original}
    identity_fields = {"name", "product", "reference product", "location", "input"}
    for key, changed in expected.items():
        source = originals[key]
        restored = copy.deepcopy(changed)
        audit = restored.pop(AUDIT_FIELD)
        for row in audit["substitutions"]:
            index = row["index"]
            raw, actual = row["source_exchange"], changed["exchanges"][index]
            if not _same(raw, source["exchanges"][index]) or not _same(
                {k: v for k, v in raw.items() if k not in identity_fields},
                {k: v for k, v in actual.items() if k not in identity_fields},
            ):
                raise ValueError(
                    "Native heat changed caller quantity, uncertainty or audit."
                )
            restored["exchanges"][index] = raw
        if not _same(restored, source):
            raise ValueError("Native heat changed a consumer outside supplier fields.")
    protected = 0
    for record in original:
        target = providers.get(_key(record))
        if target is None:
            raise ValueError("Native heat removed an existing activity.")
        intended = expected.get(_key(record), record)
        if target is not intended and not _same(target, intended):
            raise ValueError("Native heat changed an activity outside its plan.")
        if _protected(record):
            protected += 1
            if _key(record) in expected:
                raise ValueError("Native heat rewrote a protected supply inventory.")
    for record in heat["activities"]:
        if not _same(providers[_key(record)], record):
            raise ValueError("Native heat owned inventory differs from its plan.")
    known_biosphere = {
        (tuple(e["input"]), e["name"], tuple(e.get("categories", ())), e["unit"])
        for a in original
        for e in a["exchanges"]
        if e["type"] == "biosphere"
    }
    checked = 0
    for record in result:
        production = [e for e in record["exchanges"] if e["type"] == "production"]
        if len(production) != 1 or production[0]["amount"] == 0:
            raise ValueError("Native heat result needs a nonzero production reference.")
        for exchange in record["exchanges"]:
            checked += 1
            if isinstance(exchange["amount"], bool) or not math.isfinite(
                exchange["amount"]
            ):
                raise ValueError("Native heat result contains a nonfinite exchange.")
            link = tuple(exchange["input"])
            if exchange["type"] in {"production", "technosphere"}:
                if link not in providers or identity(exchange) != identity(
                    providers[link]
                ):
                    raise ValueError(
                        "Native heat result has an incorrect supplier link."
                    )
                if exchange["type"] == "production" and link != _key(record):
                    raise ValueError(
                        "Native heat production is linked to another process."
                    )
            elif exchange["type"] == "biosphere":
                if (
                    link,
                    exchange["name"],
                    tuple(exchange.get("categories", ())),
                    exchange["unit"],
                ) not in known_biosphere:
                    raise ValueError(
                        "Native heat introduced an unverified biosphere input."
                    )
            else:
                raise ValueError("Unsupported exchange type in native heat result.")
    return protected, checked


def _not_applicable(scenario, diagnostics):
    diagnostics = copy.deepcopy(diagnostics)
    diagnostics["applicability"] = "not_applicable"
    scenario["heat diagnostics"] = diagnostics
    scenario.setdefault("background context", dict(BACKGROUND_CONTEXT))
    record_validation_phase(
        scenario,
        ValidationPhaseResult(
            "sector:heat:uvek",
            "sector",
            (
                ValidationRuleResult(
                    "heat.uvek.IAM_availability", "error", "not_applicable", 0
                ),
            ),
        ),
    )
    return scenario


def update_uvek_heat(
    scenario, version, system_model, *, family="uvek", biosphere_version=None
):
    """Plan, validate and commit native heat without invoking ecoinvent repairs.

    Preserve literal consumer audits and separately record historical retained inputs,
    including inside protected generation, CLIC and migration inventories.
    """
    expected_context = dict(BACKGROUND_CONTEXT)
    if (family, str(version), system_model, biosphere_version) != (
        "uvek",
        "2026",
        "cutoff",
        "3.10",
    ):
        raise ValueError("Native heat requires UVEK2026cutoff/biosphere3.10.")
    if scenario.get("background context", expected_context) != expected_context:
        raise ValueError(
            "Native heat arguments disagree with scenario background context."
        )
    iam = scenario["iam data"]
    if all(
        getattr(iam, a, None) is None
        for a in (
            "secondary_heat_supply",
            "buildings_heat_end_use",
            "industrial_heat_end_use",
        )
    ):
        return _not_applicable(
            scenario,
            {
                "IAM_heat_diagnostics": getattr(iam, "heat_diagnostics", {}),
                "reason": "no heat IAM arrays",
            },
        )
    database = get_scenario_inventory(scenario)
    heat = plan_native_heat_markets(
        database,
        iam,
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        family=family,
        version=version,
        system_model=system_model,
        biosphere_version=biosphere_version,
    )
    if not any(heat["markets"].values()):
        return _not_applicable(
            scenario,
            {
                **heat["diagnostics"],
                "reason": "validated IAM arrays have no positive regional heat supply",
            },
        )
    planned = [*database, *heat["activities"]]
    consumers = plan_native_heat_consumers(
        planned,
        heat,
        Geomap(scenario["model"]).ecoinvent_to_iam_location,
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        family=family,
        version=version,
        system_model=system_model,
        biosphere_version=biosphere_version,
    )
    replacements = {
        tuple(row["source_key"]): row["activity"] for row in consumers["replacements"]
    }
    result = [replacements.get(_key(a), a) for a in planned]
    protected, links = _check_result(database, result, heat, consumers)
    review = json.loads(REVIEW_PATH.read_text())
    limitations = list(review["limitations"])
    for supplier in heat["suppliers"].values():
        limitations.extend(supplier["limitations"])
    limitations = list(dict.fromkeys(limitations))
    changed = consumers["diagnostics"]["substituted_exchanges"]
    applied = changed + consumers["diagnostics"]["already_applied"]
    retained = dict(Counter(row["route"] for row in consumers["retained"]))
    phase = ValidationPhaseResult(
        "sector:heat:uvek",
        "sector",
        (
            ValidationRuleResult(
                "heat.uvek.IAM_availability",
                "error",
                "applicable",
                len(heat["diagnostics"]["positive_roles"]),
            ),
            ValidationRuleResult(
                "heat.uvek.layered_supply",
                "error",
                "applicable",
                sum(len(v) for v in heat["markets"].values()) + len(heat["deliveries"]),
            ),
            ValidationRuleResult(
                "heat.uvek.consumer_quantities", "error", "applicable", applied
            ),
            ValidationRuleResult(
                "heat.uvek.protected_inventories", "error", "applicable", protected
            ),
            ValidationRuleResult(
                "heat.uvek.complete_links", "error", "applicable", links
            ),
            ValidationRuleResult(
                "heat.uvek.screening_proxies",
                "warning",
                "applicable",
                len(heat["suppliers"]),
                issues=tuple(
                    ValidationIssue("heat.uvek.screening_proxies", "warning", message)
                    for message in limitations
                ),
            ),
            ValidationRuleResult(
                "heat.uvek.retained_boundaries",
                "warning",
                "applicable",
                len(consumers["retained"]),
                issues=(
                    ValidationIssue(
                        "heat.uvek.retained_boundaries",
                        "warning",
                        "Retained historical inputs and protected supply inventories remain explicit; this is not complete heat decarbonization.",
                        actual=retained,
                    ),
                ),
            ),
        ),
    )
    for message in limitations:
        warnings.warn(message, UserWarning, stacklevel=2)
    replace_scenario_inventory(scenario, result)
    scenario.pop("index", None)
    scenario["cache"] = {
        _VALIDATION_ADDED_TARGETS_KEY: {id(a): a for a in heat["activities"]},
        _VALIDATION_RELINKED_TARGETS_KEY: {id(a): a for a in replacements.values()},
    }
    scenario.setdefault("mapping", {})["heat"] = {
        **heat["markets"],
        "district_delivery": heat["deliveries"],
        "new_owned_records": heat["activities"],
    }
    scenario.setdefault("background context", expected_context)
    scenario.setdefault("native sector limitations", {})["heat"] = limitations
    scenario.setdefault("native sector audits", {})["heat"] = {
        "review": REVIEW_REFERENCE,
        "consumer_diagnostics": copy.deepcopy(consumers["diagnostics"]),
        "retained_inputs": copy.deepcopy(consumers["retained"]),
        "market_diagnostics": copy.deepcopy(heat["diagnostics"]),
    }
    scenario["heat diagnostics"] = {
        **heat["diagnostics"],
        "consumer_routing": copy.deepcopy(consumers["diagnostics"]),
        "applicability": "applicable",
    }
    record_validation_phase(scenario, phase)
    owner = SimpleNamespace(system_model=system_model)
    for record in heat["activities"]:
        record_change_event(
            owner,
            record,
            "created",
            sector="heat",
            reason_code="heat.uvek.reviewed_supply",
            configuration_reference=REVIEW_REFERENCE,
            explanation="Create reviewed native heat layer/delivery or retained boiler; preserve source inventories and nominal reference bases.",
        )
    for record in replacements.values():
        audit = record[AUDIT_FIELD]["substitutions"]
        record_change_event(
            owner,
            record,
            "modified",
            sector="heat",
            reason_code="heat.uvek.consumer_supplier_relink",
            configuration_reference=REVIEW_REFERENCE,
            explanation="Replace only reviewed useful-heat supplier identities; retain literal originals and all signed physical and uncertainty fields.",
            computed_target_values={
                "substitutions": len(audit),
                "routes": dict(Counter(row["selected_route"] for row in audit)),
                "geographic_proxies": sum(row["geographic_proxy"] for row in audit),
            },
        )
    return scenario
