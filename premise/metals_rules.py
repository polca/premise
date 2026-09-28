"""Typed, validated runtime configuration for metal-intensity updates."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from math import isfinite
from pathlib import Path
from typing import Any, Mapping

import yaml

from .utils import DATA_DIR

MATERIAL_RULES_PATH = DATA_DIR / "metals" / "metal_products.yaml"
CONVERSION_FACTORS_PATH = DATA_DIR / "metals" / "technology_conversion_factors.yaml"


class MetalsConfigError(ValueError):
    """Raised when the metals runtime configuration is invalid."""


@dataclass(frozen=True, slots=True)
class ActivitySelector:
    kind: str
    name: str | None = None
    reference_product: str | None = None


@dataclass(frozen=True, slots=True)
class ProviderSelector:
    name: str
    reference_product: str

    def for_version(self, version: str | None) -> ProviderSelector:
        """Resolve the gallium rename recorded in the 3.10 -> 3.11 migration."""

        # See utils/import/migrations/cutoff/
        # ecoinvent-3.10-cutoff-ecoinvent-3.11-cutoff.json. The renamed market
        # also exists in 3.12; retain the configured provider in older sources.
        if str(version) in {"3.11", "3.12"} and (
            self.name,
            self.reference_product,
        ) == (
            "market for gallium, semiconductor-grade",
            "gallium, semiconductor-grade",
        ):
            return ProviderSelector(
                "market for gallium, high-grade", "gallium, high-grade"
            )
        return self


@dataclass(frozen=True, slots=True)
class MaterialRule:
    id: str
    enabled: bool
    technology: str | None
    target: ActivitySelector
    element: str | None
    provider: ProviderSelector | None
    exchange_amount_factor: float | None
    application: str
    ecoinvent_form: str | None = None
    intensity_form: str | None = None
    allocation_group: str | None = None
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class ActivityPolicy:
    id: str
    technology: str
    activity: ActivitySelector
    locations: tuple[str, ...]
    action: str
    reason: str

    def matches(self, dataset: Mapping[str, Any], technology: str) -> bool:
        if technology != self.technology:
            return False
        if dataset.get("name") != self.activity.name:
            return False
        if dataset.get("reference product") != self.activity.reference_product:
            return False
        return "*" in self.locations or dataset.get("location") in self.locations


@dataclass(frozen=True, slots=True)
class TechnologyConversion:
    id: str
    activity_name: str
    ecoinvent_unit: str
    intensity_unit: str
    factor: float
    description: str | None = None
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class MetalsRulesConfig:
    rules: tuple[MaterialRule, ...]
    policies: tuple[ActivityPolicy, ...]

    @property
    def enabled_rules(self) -> tuple[MaterialRule, ...]:
        return tuple(rule for rule in self.rules if rule.enabled)


def _load_yaml(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, Mapping):
        raise MetalsConfigError(f"{path.name}: expected a YAML mapping.")
    if payload.get("schema_version") != 1:
        raise MetalsConfigError(f"{path.name}: unsupported schema_version.")
    return payload


def _check_keys(
    value: Mapping[str, Any], *, allowed: set[str], required: set[str], context: str
) -> None:
    unknown = set(value) - allowed
    missing = required - set(value)
    if unknown:
        raise MetalsConfigError(f"{context}: unknown fields {sorted(unknown)}.")
    if missing:
        raise MetalsConfigError(f"{context}: missing fields {sorted(missing)}.")


def _text(value: Any, context: str, *, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value.strip():
        raise MetalsConfigError(f"{context}: expected non-empty text.")
    return value.strip()


def _number(value: Any, context: str, *, required: bool = True) -> float | None:
    if value is None and not required:
        return None
    if isinstance(value, bool):
        raise MetalsConfigError(f"{context}: expected a finite number.")
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise MetalsConfigError(f"{context}: expected a finite number.") from error
    if not isfinite(result) or result <= 0:
        raise MetalsConfigError(f"{context}: expected a positive finite number.")
    return result


def _selector(value: Any, context: str) -> ActivitySelector:
    if not isinstance(value, Mapping):
        raise MetalsConfigError(f"{context}: expected a mapping.")
    _check_keys(
        value,
        allowed={"kind", "name", "reference_product"},
        required={"kind"},
        context=context,
    )
    kind = _text(value.get("kind"), f"{context}.kind")
    if kind not in {"mapped_activity", "exact_activity"}:
        raise MetalsConfigError(f"{context}.kind: unsupported value {kind!r}.")
    name = _text(value.get("name"), f"{context}.name", required=False)
    product = _text(
        value.get("reference_product"),
        f"{context}.reference_product",
        required=False,
    )
    if kind == "exact_activity" and (name is None or product is None):
        raise MetalsConfigError(
            f"{context}: exact_activity requires name and reference_product."
        )
    if kind == "mapped_activity" and (name is not None or product is not None):
        raise MetalsConfigError(
            f"{context}: mapped_activity cannot declare name or reference_product."
        )
    return ActivitySelector(kind=kind, name=name, reference_product=product)


@lru_cache(maxsize=1)
def load_material_rules() -> MetalsRulesConfig:
    """Load and validate the authoritative material-rule YAML."""

    payload = _load_yaml(MATERIAL_RULES_PATH)
    _check_keys(
        payload,
        allowed={"schema_version", "rules", "activity_policies"},
        required={"schema_version", "rules", "activity_policies"},
        context=MATERIAL_RULES_PATH.name,
    )
    raw_rules = payload["rules"]
    raw_policies = payload["activity_policies"]
    if not isinstance(raw_rules, list) or not isinstance(raw_policies, list):
        raise MetalsConfigError("rules and activity_policies must be lists.")

    rules = []
    ids: set[str] = set()
    allowed_rule_keys = {
        "id",
        "enabled",
        "technology",
        "target",
        "element",
        "provider",
        "exchange_amount_factor",
        "application",
        "ecoinvent_form",
        "intensity_form",
        "allocation_group",
        "comment",
    }
    for index, raw in enumerate(raw_rules):
        context = f"rules[{index}]"
        if not isinstance(raw, Mapping):
            raise MetalsConfigError(f"{context}: expected a mapping.")
        _check_keys(
            raw,
            allowed=allowed_rule_keys,
            required={
                "id",
                "enabled",
                "technology",
                "target",
                "element",
                "provider",
                "exchange_amount_factor",
                "application",
            },
            context=context,
        )
        rule_id = _text(raw.get("id"), f"{context}.id")
        if rule_id in ids:
            raise MetalsConfigError(f"{context}: duplicate id {rule_id!r}.")
        ids.add(rule_id)
        enabled = raw.get("enabled")
        if not isinstance(enabled, bool):
            raise MetalsConfigError(f"{context}.enabled: expected true or false.")

        technology = _text(
            raw.get("technology"), f"{context}.technology", required=enabled
        )
        element = _text(raw.get("element"), f"{context}.element", required=enabled)
        provider_raw = raw.get("provider")
        provider = None
        if provider_raw is not None:
            if not isinstance(provider_raw, Mapping):
                raise MetalsConfigError(f"{context}.provider: expected a mapping.")
            _check_keys(
                provider_raw,
                allowed={"name", "reference_product"},
                required={"name", "reference_product"},
                context=f"{context}.provider",
            )
            provider_name = _text(
                provider_raw.get("name"),
                f"{context}.provider.name",
                required=enabled,
            )
            provider_product = _text(
                provider_raw.get("reference_product"),
                f"{context}.provider.reference_product",
                required=enabled,
            )
            if provider_name is not None and provider_product is not None:
                provider = ProviderSelector(provider_name, provider_product)
        elif enabled:
            raise MetalsConfigError(f"{context}.provider: required for enabled rule.")

        application = _text(raw.get("application"), f"{context}.application")
        if application != "set_direct_amount":
            raise MetalsConfigError(
                f"{context}.application: unsupported value {application!r}."
            )
        rules.append(
            MaterialRule(
                id=rule_id,
                enabled=enabled,
                technology=technology,
                target=_selector(raw.get("target"), f"{context}.target"),
                element=element,
                provider=provider,
                exchange_amount_factor=_number(
                    raw.get("exchange_amount_factor"),
                    f"{context}.exchange_amount_factor",
                    required=enabled,
                ),
                application=application,
                ecoinvent_form=_text(
                    raw.get("ecoinvent_form"),
                    f"{context}.ecoinvent_form",
                    required=False,
                ),
                intensity_form=_text(
                    raw.get("intensity_form"),
                    f"{context}.intensity_form",
                    required=False,
                ),
                allocation_group=_text(
                    raw.get("allocation_group"),
                    f"{context}.allocation_group",
                    required=False,
                ),
                comment=_text(raw.get("comment"), f"{context}.comment", required=False),
            )
        )

    policies = []
    allowed_policy_keys = {
        "id",
        "technology",
        "activity",
        "locations",
        "action",
        "reason",
    }
    for index, raw in enumerate(raw_policies):
        context = f"activity_policies[{index}]"
        if not isinstance(raw, Mapping):
            raise MetalsConfigError(f"{context}: expected a mapping.")
        _check_keys(
            raw,
            allowed=allowed_policy_keys,
            required=allowed_policy_keys,
            context=context,
        )
        policy_id = _text(raw.get("id"), f"{context}.id")
        if policy_id in ids:
            raise MetalsConfigError(f"{context}: duplicate id {policy_id!r}.")
        ids.add(policy_id)
        locations = raw.get("locations")
        if not isinstance(locations, list) or not locations:
            raise MetalsConfigError(f"{context}.locations: expected a non-empty list.")
        action = _text(raw.get("action"), f"{context}.action")
        if action != "preserve_source":
            raise MetalsConfigError(f"{context}.action: unsupported value {action!r}.")
        activity_raw = raw.get("activity")
        if not isinstance(activity_raw, Mapping):
            raise MetalsConfigError(f"{context}.activity: expected a mapping.")
        activity = _selector(
            {"kind": "exact_activity", **activity_raw}, f"{context}.activity"
        )
        policies.append(
            ActivityPolicy(
                id=policy_id,
                technology=_text(raw.get("technology"), f"{context}.technology"),
                activity=activity,
                locations=tuple(
                    _text(value, f"{context}.locations") for value in locations
                ),
                action=action,
                reason=_text(raw.get("reason"), f"{context}.reason"),
            )
        )

    allocation_groups: dict[tuple[str, str], list[float]] = {}
    for rule in rules:
        if rule.enabled and rule.allocation_group:
            key = (rule.technology or "", rule.allocation_group)
            allocation_groups.setdefault(key, []).append(
                rule.exchange_amount_factor or 0.0
            )
    for key, factors in allocation_groups.items():
        if len(factors) < 2 or abs(sum(factors) - 1.0) > 1e-12:
            raise MetalsConfigError(
                f"allocation group {key!r} must contain at least two enabled rules "
                "whose factors sum to one."
            )

    return MetalsRulesConfig(tuple(rules), tuple(policies))


@lru_cache(maxsize=1)
def load_technology_conversions() -> tuple[TechnologyConversion, ...]:
    """Load and validate technology unit conversions."""

    payload = _load_yaml(CONVERSION_FACTORS_PATH)
    _check_keys(
        payload,
        allowed={"schema_version", "conversions"},
        required={"schema_version", "conversions"},
        context=CONVERSION_FACTORS_PATH.name,
    )
    raw_conversions = payload["conversions"]
    if not isinstance(raw_conversions, list):
        raise MetalsConfigError("conversions must be a list.")
    conversions = []
    ids: set[str] = set()
    activities: set[str] = set()
    for index, raw in enumerate(raw_conversions):
        context = f"conversions[{index}]"
        if not isinstance(raw, Mapping):
            raise MetalsConfigError(f"{context}: expected a mapping.")
        _check_keys(
            raw,
            allowed={
                "id",
                "activity",
                "ecoinvent_unit",
                "intensity_unit",
                "factor",
                "description",
                "comment",
            },
            required={
                "id",
                "activity",
                "ecoinvent_unit",
                "intensity_unit",
                "factor",
            },
            context=context,
        )
        conversion_id = _text(raw.get("id"), f"{context}.id")
        if conversion_id in ids:
            raise MetalsConfigError(f"{context}: duplicate id {conversion_id!r}.")
        ids.add(conversion_id)
        activity = raw.get("activity")
        if not isinstance(activity, Mapping):
            raise MetalsConfigError(f"{context}.activity: expected a mapping.")
        _check_keys(
            activity,
            allowed={"name"},
            required={"name"},
            context=f"{context}.activity",
        )
        activity_name = _text(activity.get("name"), f"{context}.activity.name")
        if activity_name in activities:
            raise MetalsConfigError(
                f"{context}: duplicate activity selector {activity_name!r}."
            )
        activities.add(activity_name)
        conversions.append(
            TechnologyConversion(
                id=conversion_id,
                activity_name=activity_name,
                ecoinvent_unit=_text(
                    raw.get("ecoinvent_unit"), f"{context}.ecoinvent_unit"
                ),
                intensity_unit=_text(
                    raw.get("intensity_unit"), f"{context}.intensity_unit"
                ),
                factor=_number(raw.get("factor"), f"{context}.factor"),
                description=_text(
                    raw.get("description"),
                    f"{context}.description",
                    required=False,
                ),
                comment=_text(raw.get("comment"), f"{context}.comment", required=False),
            )
        )
    return tuple(conversions)


def material_rules_as_dataframe_records() -> list[dict[str, Any]]:
    """Return the legacy spreadsheet-shaped records for compatibility."""

    records = []
    for rule in load_material_rules().rules:
        records.append(
            {
                "technology": rule.technology,
                "demanding_process": (
                    rule.target.name if rule.target.kind == "exact_activity" else None
                ),
                "demanding_process_reference_product": (
                    rule.target.reference_product
                    if rule.target.kind == "exact_activity"
                    else None
                ),
                "Element": rule.element,
                "unit_convertor": rule.exchange_amount_factor,
                "Activity": rule.provider.name if rule.provider else None,
                "Reference product": (
                    rule.provider.reference_product if rule.provider else None
                ),
                "From (ecoinvent form)": rule.ecoinvent_form,
                "To (intensity form)": rule.intensity_form,
                "filter": "Yes" if rule.enabled else "No",
                "Comment": rule.comment,
            }
        )
    return records


def conversion_factors_as_dataframe_records() -> list[dict[str, Any]]:
    """Return the legacy spreadsheet-shaped conversion records."""

    return [
        {
            "Activity": item.activity_name,
            "EI_unit": item.ecoinvent_unit,
            "metals_db_unit": item.intensity_unit,
            "Conversion_factor": item.factor,
            "Description": item.description,
            "Comment": item.comment,
        }
        for item in load_technology_conversions()
    ]


__all__ = [
    "ActivityPolicy",
    "ActivitySelector",
    "MaterialRule",
    "MetalsConfigError",
    "MetalsRulesConfig",
    "ProviderSelector",
    "TechnologyConversion",
    "conversion_factors_as_dataframe_records",
    "load_material_rules",
    "load_technology_conversions",
    "material_rules_as_dataframe_records",
]
