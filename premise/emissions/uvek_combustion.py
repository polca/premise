"""Plan owned combustion variants for explicitly reviewed caller/sector pairs."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from collections import Counter

import numpy as np

from ..core.filesystem_constants import DATA_DIR
from ..core.utils import rescale_exchange
from ..inventory.activity_maps import GAINS_MAPPING, get_mapping
from ..inventory.profiles import identity

VARIANT = "premise emissions variant"
CALLER = "premise emissions caller"
REVIEW_PATH = DATA_DIR / "GAINS_emission_factors" / "uvek2026_gas_review.json"
ORDINARY_REVIEW_PATH = (
    DATA_DIR / "GAINS_emission_factors" / "uvek2026_ordinary_combustion_review.json"
)
GASES = {
    "Nitrogen oxides": "NOx",
    "Sulfur dioxide": "SO2",
    "Ammonia": "NH3",
    "Dinitrogen monoxide": "N2O",
    "Methane, fossil": "CH4",
    "Methane, non-fossil": "CH4",
    "NMVOC, non-methane volatile organic compounds": "VOC",
}


def _same(left, right):
    def scalar(value):
        if isinstance(value, np.generic):
            return value.item()
        raise TypeError(f"Unsupported emissions-audit value: {type(value).__name__}.")

    return json.dumps(
        left, sort_keys=True, allow_nan=False, default=scalar
    ) == json.dumps(right, sort_keys=True, allow_nan=False, default=scalar)


def _variant(source, sector, region, factors, scenario, biosphere_name):
    outputs = [e for e in source["exchanges"] if e["type"] == "production"]
    if (
        source.get("clic provenance")
        or source.get("brightpath_conversion")
        or source.get(VARIANT)
        or len(outputs) != 1
        or identity(outputs[0]) != identity(source)
        or source["unit"] != "megajoule"
        or isinstance(outputs[0]["amount"], bool)
        or outputs[0]["amount"] != 1
    ):
        raise ValueError(
            "Native combustion requires a reviewed non-helper +1MJ source."
        )
    token = [identity(source), sector, region, scenario]
    code = (
        "uvek-gains-"
        + hashlib.sha256(json.dumps(token, sort_keys=True).encode()).hexdigest()[:32]
    )
    result = copy.deepcopy(source)
    result.update(database="premise-UVEK-emissions", code=code)
    result["name"] = f"{source['name']} [GAINS {sector}, {region}]"
    key = (result["database"], code)
    changes, retained = [], Counter()
    for index, exchange in enumerate(result["exchanges"]):
        if isinstance(exchange["amount"], bool) or not math.isfinite(
            exchange["amount"]
        ):
            raise ValueError("Native combustion contains an invalid exchange amount.")
        exchange["output"] = key
        if exchange["type"] == "production":
            exchange["name"] = result["name"]
            exchange["input"] = key
            continue
        if exchange["type"] == "technosphere":
            if tuple(exchange["input"]) == (source["database"], source["code"]):
                raise ValueError("Self-supplying combustion needs a separate review.")
            continue
        if exchange["type"] != "biosphere" or exchange["input"][0] != biosphere_name:
            raise ValueError(
                "Native combustion needs the selected canonical biosphere."
            )
        pollutant = GASES.get(exchange["name"])
        categories = exchange.get("categories", ())
        if not isinstance(categories, (list, tuple)):
            raise ValueError("Native combustion needs explicit biosphere compartments.")
        if not pollutant or not categories or categories[0] != "air":
            retained["unreviewed_species_or_non_air"] += 1
            continue
        if exchange["unit"] != "kilogram":
            raise ValueError(
                "Reviewed native gaseous pollutants require kg references."
            )
        try:
            selection = dict(region=region, sector=sector, pollutant=pollutant)
            status = factors["status"].sel(**selection).item()
            factor = factors["factors"].sel(**selection).item()
        except (KeyError, ValueError) as error:
            raise ValueError(
                "Missing reviewed GAINS sector/region/pollutant axis."
            ) from error
        if status != "reduction":
            if status not in {
                "missing_data",
                "undefined_zero_baseline",
                "zero_projection_not_applied",
                "unchanged",
                "increase_not_applied",
            } or not math.isnan(factor):
                raise ValueError("A retained GAINS disposition cannot supply a factor.")
            retained[status] += 1
            continue
        if isinstance(factor, bool) or not math.isfinite(factor) or not 0 < factor < 1:
            raise ValueError(
                "Native combustion needs a finite positive reduction factor."
            )
        original = copy.deepcopy(source["exchanges"][index])
        rescale_exchange(exchange, factor, remove_uncertainty=False)
        changes.append(
            {
                "index": index,
                "pollutant": pollutant,
                "factor": factor,
                "source_exchange": original,
            }
        )
    result[VARIANT] = {
        "source_identity": identity(source),
        "source_key": (source["database"], source["code"]),
        "scenario": copy.deepcopy(scenario),
        "sector": sector,
        "region": region,
        "changes": changes,
        "retained": dict(retained),
        "upstream_inventory_unchanged": True,
    }
    return result


def plan_native_combustion_variants(
    database, bindings, factors, scenario, *, biosphere_name
):
    """Return owned variants and supplier-only caller copies, without mutation.

    Bindings declare exact ``caller``/``supplier`` identities and a reviewed
    ``sector``/``region``. They are decisions, never substring inference. Only
    reviewed gaseous air emissions change; PM, CO2, non-air releases, production
    references and all upstream inputs remain intact. Different GAINS sectors
    never mutate the shared original burner. Replays validate previous results
    rather than applying the correction a second time.
    """
    if (
        factors["diagnostics"].get("selected_year") != scenario["year"]
        or factors["diagnostics"].get("base_year") != 2020
        or factors["diagnostics"].get("reduction_only") is not True
    ):
        raise ValueError("Combustion factors disagree with the requested scenario.")
    by_identity = {}
    by_key = {}
    for record in database:
        by_identity.setdefault(identity(record), []).append(record)
        key = (record["database"], record["code"])
        if key in by_key:
            raise ValueError("Duplicate combustion graph activity key.")
        by_key[key] = record

    def unique(value):
        matches = by_identity.get(identity(value), [])
        if len(matches) != 1:
            raise ValueError("Missing or ambiguous exact native combustion binding.")
        return matches[0]

    additions, replacements, scopes = {}, {}, set()
    replayed = 0
    for binding in bindings:
        caller, source = unique(binding["caller"]), unique(binding["supplier"])
        if caller.get("clic provenance") or caller.get("brightpath_conversion"):
            raise ValueError(
                "Native combustion cannot rewrite an unreviewed foreground."
            )
        source_key = (source["database"], source["code"])
        variant = _variant(
            source,
            binding["sector"],
            binding["region"],
            factors,
            scenario,
            biosphere_name,
        )
        if binding.get("geography_proxy"):
            variant[VARIANT]["geography_proxy"] = copy.deepcopy(
                binding["geography_proxy"]
            )
        target_key = (variant["database"], variant["code"])
        existing = by_key.get(target_key)
        if existing is not None and not _same(existing, variant):
            raise ValueError(
                "Existing combustion variant disagrees with its source/factors."
            )
        caller_key = (caller["database"], caller["code"])
        changed = replacements.get(caller_key, copy.deepcopy(caller))
        audits = changed.setdefault(CALLER, [])
        found = 0
        for index, exchange in enumerate(caller["exchanges"]):
            if exchange["type"] != "technosphere":
                continue
            matching_audits = [
                row
                for row in audits
                if row["index"] == index
                and identity(row["source_exchange"]) == identity(source)
            ]
            original_match = identity(exchange) == identity(source)
            if not original_match and not matching_audits:
                continue
            scope = (caller_key, index)
            if scope in scopes:
                raise ValueError("Conflicting or duplicate combustion caller binding.")
            scopes.add(scope)
            found += 1
            expected = copy.deepcopy(
                exchange if original_match else matching_audits[0]["source_exchange"]
            )
            expected.update(
                name=variant["name"],
                product=variant["reference product"],
                location=variant["location"],
                input=target_key,
            )
            if "reference product" in expected:
                expected["reference product"] = variant["reference product"]
            if matching_audits:
                if (
                    len(matching_audits) != 1
                    or matching_audits[0]["scenario"] != scenario
                    or matching_audits[0]["sector"] != binding["sector"]
                    or matching_audits[0]["region"] != binding["region"]
                    or not _same(exchange, expected)
                    or existing is None
                ):
                    raise ValueError("Changed combustion replay or caller audit.")
                replayed += 1
                continue
            if tuple(exchange["input"]) != source_key:
                raise ValueError(
                    "Combustion caller link disagrees with source identity."
                )
            changed["exchanges"][index] = expected
            audits.append(
                {
                    "index": index,
                    "source_exchange": copy.deepcopy(exchange),
                    "sector": binding["sector"],
                    "region": binding["region"],
                    "scenario": copy.deepcopy(scenario),
                }
            )
            replacements[caller_key] = changed
        if not found:
            raise ValueError("Reviewed combustion input is absent from its caller.")
        if existing is None:
            additions[target_key] = variant
    return {
        "activities": list(additions.values()),
        "replacements": [
            {"source_key": key, "activity": record}
            for key, record in replacements.items()
        ],
        "diagnostics": {
            "reviewed_caller_inputs": len(scopes),
            "already_applied": replayed,
            "new_variants": len(additions),
            "modified_callers": len(replacements),
        },
    }


def plan_reviewed_gas_combustion(
    database, factors, scenario, location_mapper, *, biosphere_name
):
    """Bind the inspected native gas callers alongside unchanged ecoinvent aliases.

    The emitting burner's original location defines the GAINS region. The
    caller's function defines the sector; this does not relocate inventories or
    assign electricity projections to standalone heat or manufacturing use.
    Other native technologies and foreground services require their own reviews.
    """
    return _plan_reviews(
        database,
        factors,
        scenario,
        location_mapper,
        biosphere_name=biosphere_name,
        paths=(REVIEW_PATH,),
    )


def plan_reviewed_native_combustion(
    database, factors, scenario, location_mapper, *, biosphere_name
):
    """Plan all reviewed fuels together so multiple inputs on one caller survive."""
    return _plan_reviews(
        database,
        factors,
        scenario,
        location_mapper,
        biosphere_name=biosphere_name,
        paths=(REVIEW_PATH, ORDINARY_REVIEW_PATH),
    )


def _plan_reviews(
    database, factors, scenario, location_mapper, *, biosphere_name, paths
):
    reviews = [json.loads(path.read_text()) for path in paths]
    if any(
        row.get("schema_version") != 1 or not row.get("bindings") for row in reviews
    ):
        raise ValueError("Invalid reviewed native combustion bindings.")
    records = [row for review in reviews for row in review["bindings"]]
    aliases = get_mapping(GAINS_MAPPING, "uvek_aliases")
    sectors = {row["sector"] for row in records}
    for sector in sectors:
        names = sorted(
            {row["caller"]["name"] for row in records if row["sector"] == sector}
        )
        definition = aliases.get(sector, {})
        if (
            definition.get("exact") is not True
            or definition.get("use_legacy_templates") is not False
            or definition.get("mask")
            or definition.get("fltr") != {"name": names}
        ):
            raise ValueError(
                "Native GAINS aliases disagree with the exact caller review."
            )
    geography = {}
    for review in reviews:
        for location, proxy in review.get("emitting_location_aliases", {}).items():
            if location in geography and geography[location] != proxy:
                raise ValueError("Conflicting reviewed combustion geography proxies.")
            geography[location] = proxy
    bindings = []
    for row in records:
        location = row["supplier"]["location"]
        proxy = geography.get(location)
        binding = {
            **row,
            "region": location_mapper(proxy["mapping_location"] if proxy else location),
        }
        if proxy:
            binding["geography_proxy"] = {"source_location": location, **proxy}
        bindings.append(binding)
    result = plan_native_combustion_variants(
        database, bindings, factors, scenario, biosphere_name=biosphere_name
    )
    result["diagnostics"]["reviews"] = [
        str(path.relative_to(DATA_DIR)) if path.is_relative_to(DATA_DIR) else path.name
        for path in paths
    ]
    result["diagnostics"]["limitations"] = list(
        dict.fromkeys(text for review in reviews for text in review["limitations"])
    )
    result["diagnostics"]["retained_reviewed_cases"] = [
        row for review in reviews for row in review.get("retained_cases", [])
    ]
    result["diagnostics"]["emitting_location_aliases"] = geography
    return result
