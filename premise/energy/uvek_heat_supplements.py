"""Plan pinned CLIC and retained electric heat suppliers without source mutation."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from numbers import Real

from ..core.filesystem_constants import DATA_DIR
from ..inventory.activity_maps import HEAT_TECHS, get_mapping
from ..inventory.profiles import identity
from .uvek_heat_basis import resolve_heat_energy_basis

REVIEW_PATH = DATA_DIR / "heat" / "uvek2026_supplement_review.json"


def _positive_reference(dataset):
    outputs = [e for e in dataset["exchanges"] if e["type"] == "production"]
    if (
        len(outputs) != 1
        or identity(outputs[0]) != identity(dataset)
        or not isinstance(outputs[0]["amount"], Real)
        or isinstance(outputs[0]["amount"], bool)
        or outputs[0]["amount"] != 1
        or len(dataset["exchanges"]) <= 1
    ):
        raise ValueError("Supplemental heat requires a nonempty matching +1 reference.")


def _unique(database, key, *, slug=None):
    accepted = {key}
    if slug is not None:
        accepted.add((f"{key[0]} [CLIC {slug}]", *key[1:]))
    candidates = [
        a
        for a in database
        if identity(a) in accepted
        and (slug is None or a.get("clic provenance", {}).get("slug") == slug)
        and not (slug is not None and a.get("brightpath_conversion"))
    ]
    if len(candidates) != 1:
        label = "source pin or ownership" if slug is not None else "reviewed supplier"
        raise ValueError(f"Missing or ambiguous supplemental {label}: {key!r}.")
    _positive_reference(candidates[0])
    return candidates[0]


def _electric_boiler(database, row):
    workbook = DATA_DIR / row["workbook"]
    if hashlib.sha256(workbook.read_bytes()).hexdigest() != row["workbook_sha256"]:
        raise ValueError("Retained electric-boiler workbook changed; repeat review.")
    grid = _unique(database, identity(row["carrier"]))
    if grid.get("clic provenance") or grid.get("brightpath_conversion"):
        raise ValueError("A foreground/helper cannot impersonate the native grid.")
    code = "uvek-retained-electric-boiler-" + row["workbook_sha256"][:24]
    result = {
        **copy.deepcopy(row["identity"]),
        "database": "premise-UVEK-heat",
        "code": code,
        "production amount": 1,
        "type": "process",
        "source": "Retained industrial electric-boiler workbook; Danish Energy Agency (2026).",
        "premise heat": {
            "kind": "retained_workbook_electric_boiler",
            "workbook": row["workbook"],
            "workbook_sha256": row["workbook_sha256"],
            "review": "premise/data/heat/uvek2026_supplement_review.json",
            "limitations": copy.deepcopy(row["limitations"]),
        },
        "comment": " ".join(row["limitations"]),
        "exchanges": [],
    }
    result["exchanges"] = [
        {
            "name": result["name"],
            "product": result["reference product"],
            "location": result["location"],
            "unit": "megajoule",
            "type": "production",
            "amount": 1,
            "input": (result["database"], code),
        },
        {
            "name": grid["name"],
            "product": grid["reference product"],
            "location": grid["location"],
            "unit": "kilowatt hour",
            "type": "technosphere",
            "amount": row["reviewed_fuel_amount_per_MJ"],
            "uncertainty type": 0,
            "loc": row["reviewed_fuel_amount_per_MJ"],
            "input": (grid["database"], grid["code"]),
        },
    ]
    existing = [a for a in database if identity(a) == identity(result)]
    if existing:
        if (
            len(existing) != 1
            or existing[0].get("premise heat") != result["premise heat"]
            or json.dumps(existing[0].get("exchanges"), sort_keys=True)
            != json.dumps(result["exchanges"], sort_keys=True)
            or existing[0].get("code") != code
            or existing[0].get("database") != result["database"]
        ):
            raise ValueError(
                "Existing retained electric-boiler identity or inventory changed."
            )
        return existing[0], False
    if any(
        (a.get("database"), a.get("code")) == (result["database"], code)
        for a in database
    ):
        raise ValueError(
            "Retained electric-boiler code collides with another activity."
        )
    return result, True


def plan_heat_supplements(
    database,
    roles,
    *,
    model,
    family="uvek",
    version="2026",
    system_model="cutoff",
    biosphere_version="3.10",
):
    """Return reviewed role plans and owned additions; leave all inputs unchanged.

    CLIC records must retain the exact published pin and declared fuel quantity.
    Nominal energy bases follow the original source descriptions/properties,
    rather than generic carrier defaults or copied efficiency claims. Complete
    supplier inventories and all auxiliary inputs remain unchanged.
    """
    if (family, str(version), system_model, biosphere_version) != (
        "uvek",
        "2026",
        "cutoff",
        "3.10",
    ):
        raise ValueError("Heat supplements require UVEK2026cutoff/biosphere3.10.")
    review = json.loads(REVIEW_PATH.read_text())
    if review.get("schema_version") != 1:
        raise ValueError("Unsupported heat supplement review schema.")
    bindings = {}
    for row in review["records"]:
        for role in row["roles"]:
            if role in bindings:
                raise ValueError("Duplicate supplemental heat role.")
            bindings[role] = row
    aliases = get_mapping(HEAT_TECHS, "uvek_aliases", model=model)
    bases = get_mapping(HEAT_TECHS, "energy_basis", model=model)
    conversions = get_mapping(HEAT_TECHS, "conversion", model=model)
    plans, additions, generated = {}, [], {}
    for role in dict.fromkeys(roles):
        if role not in bindings or role not in aliases:
            raise ValueError(f"No reviewed supplemental heat supplier for {role!r}.")
        row = bindings[role]
        expected_filter = {
            k: value if k == "unit" else [value] for k, value in row["identity"].items()
        }
        definition = aliases[role]
        if (
            definition.get("use_legacy_templates") is not False
            or definition.get("exact") is not True
            or definition.get("fltr") != expected_filter
            or definition.get("native_reference_amount") != 1
            or definition.get("supplement_kind") != row["kind"]
            or definition.get("native_limitations") != row["limitations"]
            or row["role_bases"][role]
            != {"energy_basis": bases[role], "conversion": conversions[role]}
        ):
            raise ValueError(
                f"Heat supplement aliases/basis disagree with review: {role!r}."
            )
        if row["kind"] == "clic":
            pin = row["pin"]
            supplier = _unique(database, identity(row["identity"]), slug=pin["slug"])
            if (
                definition.get("clic_family_override") is not True
                or tuple(definition.get("clic_slugs", ())) != (pin["slug"],)
                or definition.get("clic_unit") != "megajoule"
                or definition.get("clic_reference_amount") != 1
                or definition.get("clic_limitations") != row["limitations"]
            ):
                raise ValueError(
                    "Supplemental CLIC root declaration disagrees with review."
                )
            provenance = supplier.get("clic provenance", {})
            if (
                supplier.get("brightpath_conversion")
                or provenance.get("pinned") is not True
                or provenance.get("slug") != pin["slug"]
                or provenance.get("semantic_version") != pin["version"]
                or provenance.get("version_id") != pin["version_id"]
                or provenance.get("sha256") != pin["sha256"]
                or provenance.get("background") != row["source_profile"]
            ):
                raise ValueError(
                    f"Supplemental heat source pin or ownership changed: {role!r}."
                )
        elif row["kind"] == "retained_electric_boiler":
            key = identity(row["identity"])
            if key not in generated:
                supplier, new = _electric_boiler(database, row)
                generated[key] = supplier
                if new:
                    additions.append(supplier)
            supplier = generated[key]
        else:
            raise ValueError("Unreviewed supplemental heat supplier kind.")
        carrier_pin = row.get("carrier_pin")
        carrier_record = _unique(
            [*database, *additions],
            identity(row["carrier"]),
            slug=carrier_pin["slug"] if carrier_pin else None,
        )
        carrier_provenance = carrier_record.get("clic provenance", {})
        if carrier_pin:
            if (
                carrier_provenance.get("semantic_version") != carrier_pin["version"]
                or carrier_provenance.get("version_id") != carrier_pin["version_id"]
                or carrier_provenance.get("sha256") != carrier_pin["sha256"]
                or carrier_record.get("brightpath_conversion")
            ):
                raise ValueError("Supplemental energy carrier source pin changed.")
        elif carrier_provenance or carrier_record.get("brightpath_conversion"):
            raise ValueError(
                "A foreground/helper cannot impersonate the native energy carrier."
            )
        carrier = identity(carrier_record)
        fuel = [
            e
            for e in supplier["exchanges"]
            if e["type"] == "technosphere" and identity(e) == carrier
        ]
        if (
            len(fuel) != 1
            or isinstance(fuel[0]["amount"], bool)
            or fuel[0]["amount"] != row["reviewed_fuel_amount_per_MJ"]
        ):
            raise ValueError(
                f"Reviewed supplemental fuel/electricity quantity changed: {role!r}."
            )
        evidence = resolve_heat_energy_basis(
            [*database, *additions],
            identity(supplier),
            routes={identity(supplier): (carrier,)},
            carrier_MJ_per_unit={carrier: row["carrier_MJ_per_unit"]},
        )
        factor = evidence.delivered_heat_factor
        if not math.isclose(factor, row["nominal_factor"], rel_tol=1e-12):
            raise ValueError("Supplemental nominal energy basis changed.")
        if bases[role] == "heat_output" and conversions[role] == "none":
            factor, witnesses = 1.0, ()
        elif bases[role] == "final_energy" and conversions[role] == row["conversion"]:
            witnesses = evidence.witnesses
        else:
            raise ValueError("Unsupported supplemental heat conversion basis.")
        plans[role] = {
            "supplier": supplier,
            "identity": identity(supplier),
            "energy_basis": bases[role],
            "delivered_heat_factor": factor,
            "energy_witnesses": witnesses,
            "limitations": copy.deepcopy(row["limitations"]),
            "review": "premise/data/heat/uvek2026_supplement_review.json",
        }
    return {"suppliers": plans, "activities": additions}
