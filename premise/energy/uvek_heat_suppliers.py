"""Resolve reviewed native heat suppliers before any scenario mutation."""

from __future__ import annotations

import copy
import json
from numbers import Real

from ..core.filesystem_constants import DATA_DIR
from ..inventory.activity_maps import HEAT_TECHS, act_fltr, get_mapping
from ..inventory.profiles import identity
from .uvek_heat_basis import resolve_heat_energy_basis

REVIEW_PATH = DATA_DIR / "heat" / "uvek2026_review.json"


def plan_native_heat_suppliers(database, roles, *, model):
    """Bind exact native identities and source-derived nominal heat factors.

    This is supplier preflight, not a sector transformation. Missing reviewed
    roles fail before inventory mutation; CLIC records, another geography or
    similarly named technologies cannot silently replace native counterparts.
    Actual supplier inventories remain untouched. Physical final-energy
    conversion must not be derived from an allocated CHP fuel requirement.
    """
    review = json.loads(REVIEW_PATH.read_text())
    if review.get("schema_version") != 1:
        raise ValueError("Unsupported native heat review schema.")
    bindings = {}
    for row in review["roles"]:
        if row["role"] in bindings:
            raise ValueError("Duplicate native heat role review.")
        bindings[row["role"]] = row
    definitions = get_mapping(HEAT_TECHS, "uvek_aliases", model=model)
    metadata = get_mapping(HEAT_TECHS, "energy_basis", model=model)
    conversions = get_mapping(HEAT_TECHS, "conversion", model=model)
    plans = {}
    for role in dict.fromkeys(roles):
        if role not in bindings or role not in definitions:
            raise ValueError(f"No reviewed native heat supplier for IAM role {role!r}.")
        row = bindings[role]
        definition = definitions[role]
        expected = identity(row["identity"])
        expected_filter = {
            key: [row["identity"][key]] if key != "unit" else "megajoule"
            for key in ("name", "reference product", "location", "unit")
        }
        if (
            definition.get("exact") is not True
            or definition.get("fltr") != expected_filter
            or definition.get("use_legacy_templates") is not False
            or definition.get("native_reference_amount") != 1
            or definition.get("native_limitations") != row["limitations"]
            or row["energy_basis"] != metadata[role]
            or row["conversion"] != conversions[role]
        ):
            raise ValueError(f"Native heat aliases disagree with review for {role!r}.")
        matches = act_fltr(database, fltr=expected_filter, exact=True)
        if len(matches) != 1 or identity(matches[0]) != expected:
            raise ValueError(
                f"Missing or ambiguous exact native heat supplier: {role!r}."
            )
        supplier = matches[0]
        if supplier.get("clic provenance") or supplier.get("brightpath_conversion"):
            raise ValueError(
                "A foreground/helper cannot replace a reviewed native heat supplier."
            )
        outputs = [e for e in supplier["exchanges"] if e["type"] == "production"]
        if (
            len(outputs) != 1
            or identity(outputs[0]) != expected
            or not isinstance(outputs[0]["amount"], Real)
            or isinstance(outputs[0]["amount"], bool)
            or outputs[0]["amount"] != 1
            or len(supplier["exchanges"]) <= 1
        ):
            raise ValueError(f"Native heat needs a nonempty +1MJ reference: {role!r}.")
        factor, witnesses = 1.0, ()
        if row["energy_basis"] == "final_energy":
            boundary = row["energy_boundary"]
            routes = {
                identity(route["supplier"]): tuple(
                    identity(i) for i in route["targets"]
                )
                for route in boundary["routes"]
            }
            carriers = {
                identity(carrier["identity"]): carrier["MJ_per_unit"]
                for carrier in boundary["carriers"]
            }
            evidence = resolve_heat_energy_basis(
                database, expected, routes=routes, carrier_MJ_per_unit=carriers
            )
            factor, witnesses = evidence.delivered_heat_factor, evidence.witnesses
            lower, upper = {
                "combustion": (0, 1.2),
                "heat_pump": (1, 6),
                "electric_boiler": (0.95, 1.05),
            }[row["conversion"]]
            valid_lower = (
                factor >= lower
                if row["conversion"] == "electric_boiler"
                else factor > lower
            )
            if not valid_lower or factor > upper:
                raise ValueError(
                    f"Unreviewed native physical heat factor for {role!r}."
                )
        elif row["energy_basis"] != "heat_output" or row["conversion"] != "none":
            raise ValueError(f"Unsupported native heat basis for {role!r}.")
        plans[role] = {
            "supplier": supplier,
            "identity": expected,
            "energy_basis": row["energy_basis"],
            "delivered_heat_factor": factor,
            "energy_witnesses": witnesses,
            "limitations": copy.deepcopy(row["limitations"]),
            "review": "premise/data/heat/uvek2026_review.json",
        }
    return plans
