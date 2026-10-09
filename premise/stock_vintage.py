"""Producer validation for opt-in annual stock-vintage resources.

This module changes timing only. It never changes an inventory coefficient,
normalises malformed weights, or infers an asset role from its supplier name.
"""

from copy import deepcopy
from collections.abc import Mapping
import hashlib
import json
import math
from numbers import Real
from pathlib import Path

IDENTITY_FIELDS = ("name", "reference product", "unit", "location")
ROLES = {
    "existing_asset_service",
    "new_asset_component",
    "replacement_component",
    "lifecycle_service",
}


def identity(record: dict, *, exchange: bool = False) -> tuple:
    if not isinstance(record, Mapping):
        raise ValueError("Stock-vintage identities must be objects")
    fields = tuple(
        (
            record.get("product", record.get(key))
            if exchange and key == "reference product"
            else record.get(key)
        )
        for key in IDENTITY_FIELDS
    )
    if any(not isinstance(value, str) or not value.strip() for value in fields):
        raise ValueError(f"Incomplete stock-vintage identity: {fields!r}")
    return fields


def calendar_year(value):
    if (
        isinstance(value, bool)
        or not isinstance(value, Real)
        or not math.isfinite(value)
        or int(value) != value
    ):
        raise ValueError(f"Expected an integer calendar year, got {value!r}")
    return int(value)


class StockVintageExport:
    """Validate annual records and apply exact caller/supplier overrides."""

    def __init__(self, payload: dict):
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError("Unsupported stock-vintage schema_version")
        self.payload = deepcopy(payload)
        payload = self.payload
        context = payload.get("inventory_context")
        if not isinstance(context, dict) or any(
            not isinstance(context.get(key), str) or not context[key].strip()
            for key in (
                "source_database",
                "source_version",
                "system_model",
                "model",
                "pathway",
            )
        ):
            raise ValueError("Stock-vintage profiles require inventory_context")
        self.context = context
        self.profiles = {}
        self.annual = {}
        profiles = payload.get("profiles")
        if not isinstance(profiles, list) or not profiles:
            raise ValueError("Stock-vintage profiles must be a nonempty list")
        for profile in profiles:
            if not isinstance(profile, dict):
                raise ValueError("A stock-vintage profile must be an object")
            profile_id = profile.get("id")
            if not isinstance(profile_id, str) or not profile_id.strip():
                raise ValueError("A stock-vintage profile needs an id")
            if profile_id in self.profiles:
                raise ValueError(f"Duplicate stock-vintage profile: {profile_id}")
            if profile.get("event_role") not in ROLES:
                raise ValueError("Unsupported stock-vintage event_role")
            if profile.get("allocation_basis") != "common_amortisation":
                raise ValueError("Stock-vintage v1 requires common_amortisation")
            for field in ("asset_unit", "service_unit"):
                if (
                    not isinstance(profile.get(field), str)
                    or not profile[field].strip()
                ):
                    raise ValueError(f"Profile {profile_id} requires {field}")
            if (
                not isinstance(profile.get("provenance"), dict)
                or not profile["provenance"]
            ):
                raise ValueError(f"Profile {profile_id} requires provenance")
            records = profile.get("years")
            if not isinstance(records, list) or not records:
                raise ValueError(f"Profile {profile_id} needs annual records")
            annual = {}
            for record in records:
                if not isinstance(record, dict):
                    raise ValueError("Annual records must be objects")
                year = calendar_year(record.get("service_year"))
                if year in annual:
                    raise ValueError(f"Duplicate service year {year}")
                events, weights = record.get("event_years"), record.get("weights")
                if not isinstance(events, list) or not isinstance(weights, list):
                    raise ValueError("Event years and weights must be lists")
                if not events or len(events) != len(weights):
                    raise ValueError(
                        "Event years and weights need equal nonzero length"
                    )
                events = [calendar_year(value) for value in events]
                if len(events) != len(set(events)):
                    raise ValueError("Aggregate duplicate event years first")
                if profile["event_role"] != "lifecycle_service" and max(events) > year:
                    raise ValueError("An asset cannot be manufactured after service")
                if any(
                    isinstance(weight, bool)
                    or not isinstance(weight, Real)
                    or not math.isfinite(weight)
                    or weight < 0
                    for weight in weights
                ):
                    raise ValueError("Weights must be finite and nonnegative")
                if not math.isclose(math.fsum(weights), 1, rel_tol=0, abs_tol=1e-10):
                    raise ValueError("Weights must sum to one")
                annual[year] = (events, weights)
            if sorted(annual) != list(range(min(annual), max(annual) + 1)):
                raise ValueError(f"Profile {profile_id} has missing annual records")
            self.annual[profile_id] = annual
            self.profiles[profile_id] = profile

        bindings = payload.get("bindings")
        if not isinstance(bindings, list) or not bindings:
            raise ValueError("Stock-vintage bindings must be a nonempty list")
        self.bindings = {}
        for binding in bindings:
            if not isinstance(binding, dict):
                raise ValueError("A stock-vintage binding must be an object")
            profile_id = binding.get("profile_id")
            if profile_id not in self.profiles:
                raise ValueError(f"Unknown stock-vintage profile: {profile_id}")
            caller, supplier = identity(binding.get("caller")), identity(
                binding.get("supplier")
            )
            if caller == supplier:
                raise ValueError("A stock-vintage binding cannot target production")
            profile = self.profiles[profile_id]
            if (
                caller[2] != profile["service_unit"]
                or supplier[2] != profile["asset_unit"]
            ):
                raise ValueError("Profile asset/service units do not match the binding")
            key = (caller, supplier)
            if key in self.bindings:
                raise ValueError(f"Conflicting stock-vintage bindings: {key}")
            self.bindings[key] = profile_id
        if set(self.profiles) != set(self.bindings.values()):
            raise ValueError("Every stock-vintage profile must have a binding")

    def validate_scenario(self, scenario):
        for key in ("model", "pathway"):
            if scenario.get(key) != self.context[key]:
                raise ValueError(
                    f"Stock-vintage {key} does not match inventory scenario"
                )
        year = calendar_year(scenario.get("year"))
        for profile_id, annual in self.annual.items():
            if year not in annual:
                raise ValueError(f"Profile {profile_id} has no service year {year}")

    def parameters(self, caller, supplier, year):
        key = (identity(caller), identity(supplier, exchange=True))
        profile_id = self.bindings.get(key)
        if profile_id is None:
            return None, None
        events, weights = self.annual[profile_id][calendar_year(year)]
        offsets = [event - year for event in events]
        return key, {
            "temporal_distribution": 6,
            "temporal_loc": None,
            "temporal_scale": None,
            "temporal_min": min(offsets),
            "temporal_max": max(offsets),
            "temporal_offsets": offsets,
            "temporal_weights": list(weights),
            "temporal_amount_source": "port",
        }

    def validate_matches(self, matched):
        missing = set(self.bindings) - matched
        if missing:
            raise ValueError(
                f"Unmatched stock-vintage inventory exchanges: {sorted(missing)}"
            )

    def write_resource(self, directory: Path):
        """Write deterministic JSON and return its package metadata."""
        path = directory / "stock_vintage.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = (
            json.dumps(self.payload, sort_keys=True, indent=2, allow_nan=False) + "\n"
        ).encode()
        path.write_bytes(raw)
        return {
            "schema_version": 1,
            "resource": "stock-vintage",
            "sha256": hashlib.sha256(raw).hexdigest(),
            "required_capability": "annual-stock-vintage-v1",
            "allocation_basis": "common_amortisation",
            "generator": "premise.stock_vintage.v1",
            "inventory_context": deepcopy(self.context),
        }
