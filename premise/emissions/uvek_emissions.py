"""Validate and apply reviewed native combustion through the scenario bridge."""

from __future__ import annotations

import copy
import json
import math
import warnings
from types import SimpleNamespace

import numpy as np

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
from . import uvek_combustion as combustion
from .emissions import Emissions
from .uvek_factors import plan_native_gains_reductions

BACKGROUND = {
    "family": "uvek",
    "version": "2026",
    "system_model": "cutoff",
    "biosphere_family": "ecoinvent",
    "biosphere_version": "3.10",
}
REFERENCE = "GAINS_emission_factors/uvek2026_ordinary_combustion_review.json"


def _key(record):
    return record["database"], record["code"]


def _contexts(mapper):
    """Independently bind reviewed input endpoints to sector and emitter region."""
    reviews = [json.loads(path.read_text()) for path in combustion.review_paths()]
    geography = {}
    for review in reviews:
        for location, proxy in review.get("emitting_location_aliases", {}).items():
            if location in geography and geography[location] != proxy:
                raise ValueError("Conflicting emissions geography reviews.")
            geography[location] = proxy
    result = {}
    for review in reviews:
        for row in review["bindings"]:
            location = row["supplier"]["location"]
            region = mapper(
                geography.get(location, {}).get("mapping_location", location)
            )
            scope = identity(row["caller"]), identity(row["supplier"])
            if scope in result:
                raise ValueError("Duplicate reviewed emissions input scope.")
            result[scope] = row["sector"], region
    return result


def _check_result(original, result, plan, factors, context, scopes):
    """Check every permitted input change and independently recompute air changes."""
    old = {_key(a): a for a in original}
    providers = {_key(a): a for a in result}
    added = {_key(a): a for a in plan["activities"]}
    changed = {
        tuple(row["source_key"]): row["activity"] for row in plan["replacements"]
    }
    if (
        len(old) != len(original)
        or len(providers) != len(result)
        or len(added) != len(plan["activities"])
        or len(changed) != len(plan["replacements"])
        or set(added) & set(old)
        or not set(changed) <= set(old)
        or set(providers) != set(old) | set(added)
    ):
        raise ValueError(
            "Native emissions contains duplicate, removed or undeclared activities."
        )
    by_identity = {}
    for record in original:
        by_identity.setdefault(identity(record), []).append(record)
    covered, used_variants = 0, set()
    for (caller_identity, supplier_identity), expected in scopes.items():
        callers = by_identity.get(caller_identity, [])
        suppliers = by_identity.get(supplier_identity, [])
        if len(callers) != 1 or len(suppliers) != 1:
            raise ValueError("Native emissions is missing a reviewed caller or source.")
        caller = callers[0]
        previous = {
            row["index"]: row["source_exchange"]
            for row in caller.get(combustion.CALLER, [])
        }
        count = 0
        for index, edge in enumerate(caller["exchanges"]):
            raw = previous.get(index, edge)
            if raw["type"] != "technosphere" or identity(raw) != supplier_identity:
                continue
            target_key = tuple(providers[_key(caller)]["exchanges"][index]["input"])
            marker = providers.get(target_key, {}).get(combustion.VARIANT, {})
            if (
                (marker.get("sector"), marker.get("region")) != expected
                or marker.get("scenario") != context
                or tuple(marker.get("source_key", ())) != _key(suppliers[0])
            ):
                raise ValueError("Native emissions did not cover every reviewed input.")
            used_variants.add(target_key)
            count += 1
        if not count:
            raise ValueError("Native emissions is missing a reviewed combustion input.")
        covered += count
    if not set(added) <= used_variants:
        raise ValueError("Native emissions created an unreviewed orphan variant.")
    checked_inputs = 0
    for key, actual in changed.items():
        source = old[key]
        restored = copy.deepcopy(actual)
        audits = restored.pop(combustion.CALLER)
        previous = source.get(combustion.CALLER, [])
        if len(audits) <= len(previous) or not combustion._same(
            audits[: len(previous)], previous
        ):
            raise ValueError("Native emissions changed a previous caller audit.")
        if previous:
            restored[combustion.CALLER] = previous
        seen = set()
        for row in audits[len(previous) :]:
            index, raw = row["index"], row["source_exchange"]
            if index in seen or not combustion._same(raw, source["exchanges"][index]):
                raise ValueError(
                    "Native emissions caller audit disagrees with its source."
                )
            seen.add(index)
            actual_edge = actual["exchanges"][index]
            fields = {"name", "product", "reference product", "location", "input"}
            if not combustion._same(
                {k: v for k, v in raw.items() if k not in fields},
                {k: v for k, v in actual_edge.items() if k not in fields},
            ):
                raise ValueError(
                    "Native emissions changed caller quantity, formula or uncertainty."
                )
            target = providers.get(tuple(actual_edge["input"]))
            marker = target.get(combustion.VARIANT, {}) if target else {}
            expected = scopes.get((identity(source), identity(raw)))
            if (
                raw["type"] != "technosphere"
                or expected != (row["sector"], row["region"])
                or marker.get("sector") != row["sector"]
                or marker.get("region") != row["region"]
                or tuple(marker.get("source_identity", ())) != identity(raw)
                or marker.get("scenario") != context
                or row["scenario"] != context
            ):
                raise ValueError(
                    "Native emissions changed an unreviewed caller/sector boundary."
                )
            restored["exchanges"][index] = raw
            checked_inputs += 1
        if not combustion._same(restored, source):
            raise ValueError(
                "Native emissions changed a caller outside its reviewed inputs."
            )
    for key, source in old.items():
        intended = changed.get(key, source)
        if providers[key] is not intended and not combustion._same(
            providers[key], intended
        ):
            raise ValueError("Native emissions changed an unreviewed existing record.")
    air_changes = 0
    for key, variant in added.items():
        if not combustion._same(providers[key], variant):
            raise ValueError("Native emissions added inventory differs from its plan.")
        marker = variant[combustion.VARIANT]
        source = old[tuple(marker["source_key"])]
        if (
            tuple(marker["source_identity"]) != identity(source)
            or marker["scenario"] != context
        ):
            raise ValueError("Native emissions variant has the wrong source/context.")
        fields = {"database", "code", "name", "exchanges", combustion.VARIANT}
        if not combustion._same(
            {k: v for k, v in variant.items() if k not in fields},
            {k: v for k, v in source.items() if k not in fields},
        ):
            raise ValueError(
                "Native emissions changed source metadata/reference basis."
            )
        changes = {row["index"]: row for row in marker["changes"]}
        if len(changes) != len(marker["changes"]) or len(variant["exchanges"]) != len(
            source["exchanges"]
        ):
            raise ValueError("Native emissions changed exchange multiplicity.")
        witnessed = set()
        for index, (raw, actual) in enumerate(
            zip(source["exchanges"], variant["exchanges"])
        ):
            expected = copy.deepcopy(raw)
            expected["output"] = key
            if raw["type"] == "production":
                expected.update(name=variant["name"], input=key)
            elif (
                raw["type"] == "biosphere"
                and raw["name"] in combustion.GASES
                and raw.get("categories")
                and raw["categories"][0] == "air"
            ):
                selection = dict(
                    region=marker["region"],
                    sector=marker["sector"],
                    pollutant=combustion.GASES[raw["name"]],
                )
                if factors["status"].sel(**selection).item() == "reduction":
                    factor = factors["factors"].sel(**selection).item()
                    witness = changes.get(index, {})
                    if (
                        witness.get("factor") != factor
                        or witness.get("pollutant") != selection["pollutant"]
                        or not combustion._same(witness.get("source_exchange"), raw)
                    ):
                        raise ValueError(
                            "Native emissions factor/audit disagrees with GAINS."
                        )
                    for field in ("amount", "loc", "scale", "minimum", "maximum"):
                        if isinstance(expected.get(field), np.floating):
                            expected[field] = float(expected[field])
                    expected.update(
                        Emissions._rescaled_exchange_updates(expected, factor)
                    )
                    witnessed.add(index)
                    air_changes += 1
            if not combustion._same(expected, actual):
                raise ValueError(
                    "Native emissions changed an unreviewed coordinate or uncertainty field."
                )
        if witnessed != set(changes):
            raise ValueError(
                "Native emissions declared an unreviewed biosphere change."
            )
    known_biosphere = {
        (tuple(e["input"]), e["name"], tuple(e.get("categories", ())), e["unit"])
        for a in original
        for e in a["exchanges"]
        if e["type"] == "biosphere"
    }
    links = 0
    for record in result:
        production = [e for e in record["exchanges"] if e["type"] == "production"]
        if len(production) != 1 or production[0]["amount"] == 0:
            raise ValueError("Native emissions requires nonzero production references.")
        for edge in record["exchanges"]:
            links += 1
            if isinstance(edge["amount"], bool) or not math.isfinite(edge["amount"]):
                raise ValueError("Native emissions has a nonfinite exchange.")
            key = tuple(edge["input"])
            if edge["type"] in {"production", "technosphere"}:
                if key not in providers or identity(edge) != identity(providers[key]):
                    raise ValueError("Native emissions has an incorrect supplier link.")
                if edge["type"] == "production" and key != _key(record):
                    raise ValueError("Native emissions changed production ownership.")
            elif (
                edge["type"] != "biosphere"
                or (key, edge["name"], tuple(edge.get("categories", ())), edge["unit"])
                not in known_biosphere
            ):
                raise ValueError(
                    "Native emissions introduced an unreviewed biosphere link."
                )
    return {
        "caller_inputs": checked_inputs,
        "reviewed_input_coverage": covered,
        "gaseous_air_changes": air_changes,
        "links": links,
    }


def _not_applicable(scenario, reason, diagnostics=None):
    scenario.setdefault("background context", dict(BACKGROUND))
    scenario["emissions diagnostics"] = {
        "applicability": "not_applicable",
        "reason": reason,
        "factor_diagnostics": copy.deepcopy(diagnostics or {}),
    }
    record_validation_phase(
        scenario,
        ValidationPhaseResult(
            "sector:emissions:uvek",
            "sector",
            (
                ValidationRuleResult(
                    "emissions.uvek.IAM_availability", "error", "not_applicable", 0
                ),
            ),
        ),
    )
    return scenario


def update_uvek_emissions(
    scenario,
    version,
    system_model,
    gains_scenario,
    *,
    family="uvek",
    biosphere_version=None,
):
    """Apply the reviewed component atomically; the public sector gate stays closed."""
    if (family, str(version), system_model, biosphere_version) != (
        "uvek",
        "2026",
        "cutoff",
        "3.10",
    ):
        raise ValueError("Native emissions requires UVEK2026cutoff/biosphere3.10.")
    if scenario.get("background context", BACKGROUND) != BACKGROUND:
        raise ValueError("Native emissions arguments disagree with background context.")
    if gains_scenario not in {"CLE", "MFR"}:
        raise ValueError("Native emissions requires a supported GAINS scenario.")
    data = getattr(scenario["iam data"], "gains_data_IAM", None)
    if data is None:
        return _not_applicable(scenario, "no GAINS IAM array")
    factors = plan_native_gains_reductions(data, scenario["year"])
    database = get_scenario_inventory(scenario)
    names = {
        e["input"][0]
        for a in database
        for e in a["exchanges"]
        if e["type"] == "biosphere"
    }
    if len(names) != 1:
        raise ValueError("Native emissions requires one selected canonical biosphere.")
    mapper = Geomap(scenario["model"]).ecoinvent_to_iam_location
    context = {
        "model": scenario["model"],
        "pathway": scenario["pathway"],
        "year": scenario["year"],
        "gains_scenario": gains_scenario,
    }
    plan = combustion.plan_reviewed_native_combustion(
        database, factors, context, mapper, biosphere_name=next(iter(names))
    )
    changes = {
        tuple(row["source_key"]): row["activity"] for row in plan["replacements"]
    }
    result = [changes.get(_key(a), a) for a in database] + plan["activities"]
    checked = _check_result(database, result, plan, factors, context, _contexts(mapper))
    if (
        not checked["gaseous_air_changes"]
        and not plan["diagnostics"]["already_applied"]
    ):
        return _not_applicable(
            scenario, "no reviewed positive GAINS reductions", factors["diagnostics"]
        )
    limitations = list(plan["diagnostics"]["limitations"])
    warnings.warn(
        "Native UVEK emissions applies reviewed combustion gases only; other emissions and explicitly retained cases remain unfinished. See native sector audits and limitations.",
        UserWarning,
        stacklevel=2,
    )
    replace_scenario_inventory(scenario, result)
    scenario.pop("index", None)
    scenario["cache"] = {
        _VALIDATION_ADDED_TARGETS_KEY: {id(a): a for a in plan["activities"]},
        _VALIDATION_RELINKED_TARGETS_KEY: {id(a): a for a in changes.values()},
    }
    scenario.setdefault("mapping", {})["emissions"] = {
        "owned_variants": [
            a
            for a in result
            if a.get(combustion.VARIANT, {}).get("scenario") == context
        ],
        "modified_callers": list(changes.values()),
    }
    scenario.setdefault("background context", dict(BACKGROUND))
    scenario.setdefault("native sector limitations", {})["emissions"] = limitations
    scenario.setdefault("native sector audits", {})["emissions"] = {
        "reviews": copy.deepcopy(plan["diagnostics"].get("reviews", [])),
        "combustion_diagnostics": copy.deepcopy(plan["diagnostics"]),
        "factor_diagnostics": copy.deepcopy(factors["diagnostics"]),
        "independent_checks": checked,
        "full_sector_mapping_complete": False,
    }
    scenario["emissions diagnostics"] = {
        **checked,
        "applicability": "applicable",
        "full_sector_mapping_complete": False,
    }
    rules = [
        ValidationRuleResult("emissions.uvek." + name, "error", "applicable", count)
        for name, count in checked.items()
    ]
    rules.append(
        ValidationRuleResult(
            "emissions.uvek.retained_scope",
            "warning",
            "applicable",
            len(limitations),
            issues=tuple(
                ValidationIssue("emissions.uvek.retained_scope", "warning", text)
                for text in limitations
            ),
        )
    )
    record_validation_phase(
        scenario, ValidationPhaseResult("sector:emissions:uvek", "sector", tuple(rules))
    )
    owner = SimpleNamespace(system_model=system_model)
    for record in plan["activities"]:
        record_change_event(
            owner,
            record,
            "created",
            sector="emissions",
            reason_code="emissions.uvek.reviewed_combustion",
            configuration_reference=record[combustion.VARIANT].get("review", REFERENCE),
            explanation="Fork reviewed native combustion by caller sector; preserve fuel inputs, allocation, source geography and literal air-emission audits.",
            computed_target_values={
                "gaseous_air_changes": len(record[combustion.VARIANT]["changes"]),
                "sector": record[combustion.VARIANT]["sector"],
                "region": record[combustion.VARIANT]["region"],
            },
        )
    providers = {_key(record): record for record in result}
    for record in changes.values():
        references = sorted(
            {
                providers[tuple(record["exchanges"][row["index"]]["input"])][
                    combustion.VARIANT
                ].get("review", REFERENCE)
                for row in record[combustion.CALLER]
            }
        )
        record_change_event(
            owner,
            record,
            "modified",
            sector="emissions",
            reason_code="emissions.uvek.caller_supplier",
            configuration_reference=(
                references[0] if len(references) == 1 else "GAINS_emission_factors"
            ),
            explanation="Relink reviewed combustion inputs only; signed caller quantities, formulas and uncertainty remain unchanged.",
            computed_target_values={
                "reviewed_combustion_inputs": len(record[combustion.CALLER]),
                "configuration_references": references,
            },
        )
    return scenario
