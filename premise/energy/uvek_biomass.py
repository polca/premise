"""Native dry-mass biomass and pellet feedstocks, with explicit screening boundaries."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from collections import defaultdict

from ..core.transformation import BaseTransformation
from ..core.filesystem_constants import DATA_DIR
from ..inventory.activity_maps import BIOMASS_TYPES, InventorySet, get_mapping
from ..reporting.provenance import record_change_event
from ..validation.validation_framework import (
    ValidationIssue,
    ValidationPhaseResult,
    ValidationRuleResult,
)

MARKET_NAME = "market for lignocellulosic biomass, used as fuel"
MARKET_PRODUCT = "lignocellulosic biomass"
PELLET_NAME = "Wood pellet, measured as dry mass, at plant"
PROCEDURAL_MAPPING = DATA_DIR / "biomass" / "biomass_activities.yaml"
PELLET_LIMITATION = (
    "Inside native pellet manufacture only, the IAM dry-wood mix proxies original "
    "dry-kg chips, shavings and sawdust supplies. Particle size, moisture, milling, "
    "drying/pressing performance, origin and allocation differ. Original factory "
    "feedstock quantities, process energy, binder, infrastructure, freight, wastes "
    "and output remain; upstream feedstock burdens change. No guessed processing, "
    "yield, density or calorific multiplier or credit is added. Effects remain "
    "unquantified; neither feedstock-grade nor LCIA equivalence is claimed."
)
NATIVE_MIX = (
    "Wood chips, production mix, wet, measured as dry mass, at forest road & at sawmill"
)
PROXY_LIMITATION = (
    "UVEK biomass uses complete dry-kg forest-road wood chips as a screening proxy "
    "for purpose-grown woody/energy-crop supply, and complete clean sawmill chips "
    "as a proxy for forest residues. Cultivation, land use, species, collection, "
    "processing, coproduct allocation, delivery, geography and vintage differ. "
    "Native supplier inventories, their carbon accounting and infrastructure are "
    "retained; forestry is not relocated or synthesized. IAM fractions use a "
    "common woody-fuel basis; no density, moisture or calorific multiplier, "
    "biogenic uptake or recovery credit is invented. Native MJ/gross-energy "
    "residues, combustion services, glued/composite residues and material uses "
    "are not substituted. Source combustion emissions and efficiencies remain; "
    "this stage does not correct moisture-dependent combustion performance. "
    "Historical supplier weights are deterministic proxies, not future uncertainty estimates. "
    "Effects and the native upstream biosphere exclusions "
    "remain unquantified. Pellet feedstock substitution has separate limitations."
)
GEOGRAPHY_LIMITATION = (
    "Where an IAM region has no native positive historical portfolio for a "
    "biomass class, its suppliers follow the native RER wood-chip portfolio "
    "within that same class. This is a geographic screening proxy; source "
    "producer locations and complete inventories remain."
)


def identity(record):
    """Return a linked, case-sensitive supplier identity."""
    return (
        record["name"],
        record.get("reference product", record.get("product")),
        record["location"],
        record["unit"],
    )


def _at_year(array, year):
    if "year" not in array.coords:
        return array
    if year in array.coords["year"].values:
        return array.sel(year=year)
    return array.interp(year=year)


def _positive_shares(values, *, label):
    if any(not math.isfinite(v) or v < 0 for v in values.values()):
        raise ValueError(
            f"Invalid UVEK biomass {label}: finite nonnegative shares required."
        )
    total = math.fsum(values.values())
    return {k: v / total for k, v in values.items() if v > 0} if total else {}


class UVEKBiomass(BaseTransformation):
    """Plan native biomass mixes while preserving manufacturing quantities.

    Wood-chip suppliers and material/byproduct uses retain their inventories.
    Only reviewed raw feedstock links change inside native pellet manufacture.
    """

    def __init__(
        self,
        database,
        iam_data,
        model,
        pathway,
        year,
        version="2026",
        system_model="cutoff",
        cache=None,
        index=None,
        *,
        family="uvek",
        biosphere_version="3.10",
    ):
        if (family, str(version), system_model, biosphere_version) != (
            "uvek",
            "2026",
            "cutoff",
            "3.10",
        ):
            raise ValueError(
                "Native biomass requires UVEK2026cutoff and ecoinvent3.10biosphere."
            )
        super().__init__(
            database,
            iam_data,
            model,
            pathway,
            year,
            version,
            system_model,
            cache,
            index,
            family,
            biosphere_version,
        )
        self.providers = {}
        for dataset in database:
            key = identity(dataset)
            if key in self.providers:
                raise ValueError(f"Ambiguous native biomass provider: {key!r}.")
            self.providers[key] = dataset
        self.definitions = get_mapping(BIOMASS_TYPES, "uvek_aliases", model=model)
        mapping = InventorySet(database, version, model, family="uvek")
        self.suppliers = mapping.generate_map(self.definitions)
        self.native_keys = set()
        for role, suppliers in self.suppliers.items():
            for supplier in suppliers:
                refs = [e for e in supplier["exchanges"] if e["type"] == "production"]
                if (
                    len(refs) != 1
                    or refs[0]["amount"] != 1
                    or refs[0].get("unit", supplier["unit"]) != "kilogram"
                    or supplier["unit"] != "kilogram"
                    or len(supplier["exchanges"]) <= 1
                ):
                    raise ValueError(
                        f"Unverified positive dry-kg reference for {role!r}."
                    )
                self.native_keys.add(identity(supplier))
        mix_key = (NATIVE_MIX, NATIVE_MIX, "RER", "kilogram")
        if mix_key not in self.providers:
            raise ValueError("Native biomass needs the reviewed RER source portfolio.")
        self.source_mix = self.providers[mix_key]
        refs = [e for e in self.source_mix["exchanges"] if e["type"] == "production"]
        if len(refs) != 1 or refs[0]["amount"] != 1:
            raise ValueError(
                "Native biomass source portfolio must reference +1 dry kg."
            )
        self.weights = defaultdict(float)
        for exchange in self.source_mix["exchanges"]:
            if exchange["type"] != "technosphere":
                continue
            amount = float(exchange["amount"])
            if not math.isfinite(amount) or amount < 0:
                raise ValueError(
                    "Native biomass source weights must be finite and nonnegative."
                )
            if amount and identity(exchange) not in self.native_keys:
                raise ValueError(
                    "Unreviewed positive supplier in native biomass source portfolio."
                )
            if amount:
                self.weights[identity(exchange)] += amount
        self.producer_snapshots = {
            k: copy.deepcopy(self.providers[k]) for k in self.native_keys
        }
        procedural = mapping.generate_map(
            get_mapping(PROCEDURAL_MAPPING, "uvek_aliases")
        )
        self.pellet_keys = {
            identity(a) for a in procedural.get("pellet production", ())
        }
        self.pellet_feedstock_keys = {
            identity(a) for a in procedural.get("pellet woody feedstock", ())
        }
        self.pellet_feedstock_names = set(
            get_mapping(PROCEDURAL_MAPPING, "uvek_aliases")
            .get("pellet woody feedstock", {})
            .get("fltr", {})
            .get("name", [])
        )
        if self.pellet_keys and not self.pellet_feedstock_names:
            raise ValueError("Missing reviewed pellet feedstock aliases.")
        for key in self.pellet_keys | self.pellet_feedstock_keys:
            supplier = self.providers[key]
            refs = [e for e in supplier["exchanges"] if e["type"] == "production"]
            if (
                len(refs) != 1
                or refs[0]["amount"] != 1
                or refs[0].get("unit", supplier["unit"]) != "kilogram"
                or len(supplier["exchanges"]) <= 1
            ):
                raise ValueError(
                    "Pellet producer/feedstock needs a verified nonempty +1 dry-kg reference."
                )
        self.pellet_snapshots = {
            k: copy.deepcopy(self.providers[k]) for k in self.pellet_keys
        }
        self.producer_snapshots.update(
            {k: copy.deepcopy(self.providers[k]) for k in self.pellet_feedstock_keys}
        )
        self.relinked = []
        self.relinked_indices = []

    def get_ecoinvent_locs(self):
        """Map only reviewed suppliers and their fuel consumers.

        Unrelated native legacy regions (for example CPA in material production)
        do not acquire an invented geography alias or prevent this stage.
        """
        definitions = get_mapping(BIOMASS_TYPES, "uvek_aliases", model=self.model)
        names = {NATIVE_MIX}
        for entry in definitions.values():
            names.update(entry.get("fltr", {}).get("name", []))
        feed_names = set(
            get_mapping(PROCEDURAL_MAPPING, "uvek_aliases")
            .get("pellet woody feedstock", {})
            .get("fltr", {})
            .get("name", [])
        )
        all_suppliers = {identity(a): a for a in self.database}
        suppliers = {
            identity(a): a
            for a in self.database
            if a["name"] in names
            and a["reference product"] == a["name"]
            and a["unit"] == "kilogram"
        }
        locations = {a["location"] for a in suppliers.values()}
        for dataset in self.database:
            if not self._energy_consumer(dataset):
                continue
            for exchange in dataset["exchanges"]:
                if exchange["type"] != "technosphere":
                    continue
                supplier = suppliers.get(identity(exchange))
                if (
                    dataset["name"] == PELLET_NAME
                    and exchange.get("name") in feed_names
                ):
                    supplier = all_suppliers.get(identity(exchange))
                if supplier is not None and (
                    not exchange.get("input")
                    or tuple(exchange["input"])
                    == (supplier["database"], supplier["code"])
                ):
                    locations.add(dataset["location"])
        return sorted(locations)

    def _iam_shares(self):
        array = self.iam_data.biomass_mix
        if array is None:
            return {}
        array = _at_year(array, self.year)
        result = {}
        for region in map(str, array.coords["region"].values):
            values = {
                str(role): float(array.sel(region=region, variables=role).item())
                for role in array.coords["variables"].values
            }
            unknown = {
                k for k, v in values.items() if v != 0 and k not in self.definitions
            }
            if unknown:
                raise ValueError(
                    f"No reviewed UVEK biomass aliases for {sorted(unknown)!r}."
                )
            shares = _positive_shares(values, label=f"IAM mix in {region}")
            if shares:
                result[region] = shares
        return result

    def plan(self):
        """Resolve every supplier and consumer before changing scenario inventories."""
        mixes = self._iam_shares()
        if not mixes:
            raise ValueError(
                "No positive IAM biomass data; this stage is not applicable."
            )
        plans = {}
        for region, shares in mixes.items():
            vector = []
            for technology, share in shares.items():
                candidates = {identity(a) for a in self.suppliers.get(technology, ())}
                global_weights = {
                    k: v for k, v in self.weights.items() if k in candidates
                }
                if not global_weights:
                    raise ValueError(
                        f"No positive native supply for biomass {technology!r}."
                    )
                local = {
                    k: v
                    for k, v in global_weights.items()
                    if self.ecoinvent_to_iam_loc.get(k[2]) == region
                }
                weights = _positive_shares(local or global_weights, label=technology)
                for key, weight in weights.items():
                    supplier = self.providers[key]
                    vector.append(
                        {
                            "technology": technology,
                            "supplier": key,
                            "amount": share * weight,
                            "input": (supplier["database"], supplier["code"]),
                            "geographic_proxy": not bool(local),
                        }
                    )
            code = (
                "uvek2026-biomass-"
                + hashlib.sha256(
                    json.dumps([self.model, self.scenario, self.year, region]).encode()
                ).hexdigest()[:24]
            )
            key = (MARKET_NAME, MARKET_PRODUCT, region, "kilogram")
            existing = self.providers.get(key)
            if existing is not None:
                meta = existing.get("premise biomass", {})
                if meta.get("kind") != "uvek_iam_market" or meta.get("scenario") != [
                    self.model,
                    self.scenario,
                    self.year,
                ]:
                    raise ValueError(
                        "Cannot overwrite a foreground-owned or other-scenario biomass market."
                    )
                code = existing["code"]
            plans[region] = {
                "key": key,
                "code": code,
                "shares": shares,
                "vector": vector,
                "existing": existing,
                "consumers": [],
            }
        source_names = {k[0] for k in self.native_keys} | {NATIVE_MIX}
        for dataset in self.database:
            if (
                not self._energy_consumer(dataset)
                or identity(dataset) in self.native_keys
            ):
                continue
            pellet = identity(dataset) in self.pellet_keys
            allowed_names = source_names | (
                self.pellet_feedstock_names if pellet else set()
            )
            for original_index, exchange in enumerate(dataset["exchanges"]):
                if (
                    pellet
                    and exchange["type"] == "technosphere"
                    and exchange.get("name") in self.pellet_feedstock_names
                    and exchange.get("unit") != "kilogram"
                ):
                    raise ValueError(
                        "Pellet feedstock needs a reviewed dry-kg basis; no unit conversion inferred."
                    )
                if (
                    exchange["type"] != "technosphere"
                    or exchange.get("unit") != "kilogram"
                    or exchange.get("name") not in allowed_names
                ):
                    continue
                supplier = self.providers.get(identity(exchange))
                if supplier is None:
                    raise ValueError(
                        "Missing native wood-chip provider; no name-only fallback."
                    )
                reference = exchange.get("input")
                if reference and tuple(reference) != (
                    supplier["database"],
                    supplier["code"],
                ):
                    continue  # A foreground-owned supplier must retain its explicit link.
                if (
                    identity(supplier) not in self.native_keys
                    and supplier["name"] != NATIVE_MIX
                    and not (
                        pellet and identity(supplier) in self.pellet_feedstock_keys
                    )
                ):
                    continue
                location = dataset["location"]
                region = (
                    location
                    if location in plans
                    else self.ecoinvent_to_iam_loc.get(location)
                )
                if region not in plans:
                    region = "World" if "World" in plans else None
                if region is None:
                    raise ValueError(
                        f"No positive IAM biomass mix for consumer location {location!r}."
                    )
                if not math.isfinite(float(exchange["amount"])):
                    raise ValueError("Native biomass caller quantity must be finite.")
                plans[region]["consumers"].append((dataset, exchange, original_index))
        return plans

    @staticmethod
    def _energy_consumer(dataset):
        """Keep material processing and allocated byproduct outputs outside this stage."""
        if dataset["unit"] in {"megajoule", "kilowatt hour"}:
            return True
        if identity(dataset) == (PELLET_NAME, PELLET_NAME, "RER", "kilogram"):
            return True
        name = dataset["name"].lower().lstrip("x ")
        return name.startswith(
            (
                "synthetic gas,",
                "methane,",
                "ethanol,",
                "bioethanol,",
                "methanol,",
                "hydrogen production",
                "ethanol production",
                "methanol production",
                "methane production",
            )
        )

    def apply(self, plans):
        """Apply planned dry-mass markets and retain all original caller quantity metadata."""
        for region, plan in plans.items():
            market = plan["existing"]
            status = "modified" if market is not None else "created"
            if market is None:
                market = {
                    "name": MARKET_NAME,
                    "reference product": MARKET_PRODUCT,
                    "location": region,
                    "unit": "kilogram",
                    "database": self.source_mix["database"],
                    "code": plan["code"],
                }
            vector = plan["vector"]
            market["exchanges"] = [
                {
                    "name": MARKET_NAME,
                    "product": MARKET_PRODUCT,
                    "location": region,
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1,
                    "input": (market["database"], market["code"]),
                }
            ]
            for row in vector:
                name, product, location, unit = row["supplier"]
                market["exchanges"].append(
                    {
                        "name": name,
                        "product": product,
                        "location": location,
                        "unit": unit,
                        "type": "technosphere",
                        "amount": row["amount"],
                        "uncertainty type": 0,
                        "input": row["input"],
                        "premise biomass technology": row["technology"],
                    }
                )
            market["premise biomass"] = {
                "kind": "uvek_iam_market",
                "scenario": [self.model, self.scenario, self.year],
                "reference_basis": "one kilogram dry woody fuel; no unit conversion",
                "iam_shares": copy.deepcopy(plan["shares"]),
                "supplier_vector": copy.deepcopy(vector),
                "limitations": [PROXY_LIMITATION, GEOGRAPHY_LIMITATION]
                + ([PELLET_LIMITATION] if self.pellet_keys else []),
            }
            market["comment"] = PROXY_LIMITATION
            if status == "created":
                self.database.append(market)
                self.add_to_index(market)
            plan["market"] = market
            self._validation_relinked_targets[id(market)] = market
            record_change_event(
                self,
                market,
                status,
                sector="biomass",
                reason_code="biomass.uvek.native_dry_mass_mix",
                explanation=PROXY_LIMITATION,
            )
            for dataset, exchange, original_index in plan["consumers"]:
                original = copy.deepcopy(exchange)
                exchange.update(
                    name=MARKET_NAME,
                    product=MARKET_PRODUCT,
                    location=region,
                    input=(market["database"], market["code"]),
                )
                if "reference product" in exchange:
                    exchange["reference product"] = MARKET_PRODUCT
                self.relinked.append((dataset, exchange, original, market))
                self.relinked_indices.append(original_index)
                self._validation_relinked_targets[id(dataset)] = dataset
                record_change_event(
                    self,
                    dataset,
                    "modified",
                    sector="biomass",
                    reason_code=(
                        "biomass.uvek.pellet_feedstock_proxy"
                        if identity(dataset) in self.pellet_keys
                        else "biomass.uvek.native_dry_mass_relink"
                    ),
                    explanation=(
                        PELLET_LIMITATION
                        if identity(dataset) in self.pellet_keys
                        else "Original signed dry kg and all quantity/uncertainty metadata retained."
                    ),
                )
        return plans

    def validate(self, plans):
        """Check actual linked supplier vectors, native producers and unchanged quantities."""
        if not plans:
            raise ValueError(
                "Native biomass validation requires an actual nonempty IAM plan."
            )
        for region, plan in plans.items():
            market = plan["market"]
            expected = defaultdict(float)
            for row in plan["vector"]:
                expected[(row["technology"], tuple(row["supplier"]))] += row["amount"]
            actual = defaultdict(float)
            for exchange in market["exchanges"]:
                if exchange["type"] != "technosphere":
                    continue
                key = identity(exchange)
                provider = self.providers.get(key)
                if provider is None or tuple(exchange["input"]) != (
                    provider["database"],
                    provider["code"],
                ):
                    raise ValueError("Native biomass supplier ownership/link mismatch.")
                actual[(exchange["premise biomass technology"], key)] += exchange[
                    "amount"
                ]
            if set(actual) != set(expected) or any(
                not math.isclose(actual[k], v, rel_tol=1e-12, abs_tol=1e-14)
                for k, v in expected.items()
            ):
                raise ValueError(
                    "Native biomass supplier vector differs from the IAM plan."
                )
            if not math.isclose(math.fsum(actual.values()), 1, rel_tol=1e-12):
                raise ValueError(
                    "Native biomass dry-mass supply does not sum to one kg."
                )
            for technology, share in plan["shares"].items():
                if not math.isclose(
                    math.fsum(
                        v for (tech, _), v in actual.items() if tech == technology
                    ),
                    share,
                    rel_tol=1e-12,
                    abs_tol=1e-14,
                ):
                    raise ValueError(
                        "Native biomass technology share differs from IAM."
                    )
        quantity_fields = {"name", "product", "reference product", "location", "input"}
        for _, exchange, original, market in self.relinked:
            if {k: v for k, v in exchange.items() if k not in quantity_fields} != {
                k: v for k, v in original.items() if k not in quantity_fields
            } or tuple(exchange["input"]) != (market["database"], market["code"]):
                raise ValueError(
                    "Native biomass changed caller quantity/uncertainty or ownership."
                )
        if any(
            self.providers[k] != snapshot
            for k, snapshot in self.producer_snapshots.items()
        ):
            raise ValueError("Native biomass supplier inventory was changed.")
        for key, snapshot in self.pellet_snapshots.items():
            restored = copy.deepcopy(self.providers[key])
            for (dataset, _, original, _), index in zip(
                self.relinked, self.relinked_indices, strict=True
            ):
                if identity(dataset) == key:
                    restored["exchanges"][index] = original
            if restored != snapshot:
                raise ValueError(
                    "Pellet factory changed outside the reviewed dry-feedstock links."
                )
        return ValidationPhaseResult(
            phase_id="sector:biomass:uvek",
            kind="sector",
            rule_results=(
                ValidationRuleResult(
                    "biomass.uvek.supplier_vectors", "error", "applicable", len(plans)
                ),
                ValidationRuleResult(
                    "biomass.uvek.caller_quantities",
                    "error",
                    "applicable",
                    len(self.relinked),
                ),
                ValidationRuleResult(
                    "biomass.uvek.pellet_factory",
                    "error",
                    "applicable",
                    len(self.pellet_keys),
                ),
                ValidationRuleResult(
                    "biomass.uvek.native_supply_proxies",
                    "warning",
                    "applicable",
                    len(plans),
                    issues=(
                        ValidationIssue(
                            "biomass.uvek.native_supply_proxies",
                            "warning",
                            PROXY_LIMITATION,
                            checked_object_count=len(plans),
                        ),
                    ),
                ),
            ),
        )


def update_uvek_biomass(
    scenario, version, system_model, *, family="uvek", biosphere_version=None
):
    """Apply native biomass through the ordinary scenario-store update bridge."""
    import warnings

    from ..inventory.inventory_store import (
        get_scenario_inventory,
        replace_scenario_inventory,
    )
    from ..validation.validation_framework import record_validation_phase

    expected = {
        "family": "uvek",
        "version": "2026",
        "system_model": "cutoff",
        "biosphere_family": "ecoinvent",
        "biosphere_version": "3.10",
    }
    if (family, str(version), system_model, biosphere_version) != (
        "uvek",
        "2026",
        "cutoff",
        "3.10",
    ):
        raise ValueError(
            "Native biomass requires UVEK2026cutoff and ecoinvent3.10biosphere."
        )
    context = scenario.get("background context")
    if context is not None and context != expected:
        raise ValueError(
            "Native biomass arguments disagree with scenario background context."
        )
    mix = scenario["iam data"].biomass_mix
    if mix is not None:
        values = _at_year(mix, scenario["year"]).values.ravel()
        if any(not math.isfinite(float(v)) or float(v) < 0 for v in values):
            raise ValueError(
                "Invalid finite/nonnegative IAM biomass data for the selected year."
            )
        positive = any(float(v) > 0 for v in values)
    else:
        positive = False
    if not positive:
        record_validation_phase(
            scenario,
            ValidationPhaseResult(
                phase_id="sector:biomass:uvek",
                kind="sector",
                rule_results=(
                    ValidationRuleResult(
                        "biomass.uvek.IAM_availability", "error", "not_applicable", 0
                    ),
                ),
            ),
        )
        return scenario
    biomass = UVEKBiomass(
        get_scenario_inventory(scenario),
        scenario["iam data"],
        scenario["model"],
        scenario["pathway"],
        scenario["year"],
        version,
        system_model,
        scenario.get("cache"),
        scenario.get("index"),
        family=family,
        biosphere_version=biosphere_version,
    )
    plans = biomass.plan()
    biomass.apply(plans)
    record_validation_phase(scenario, biomass.validate(plans))
    limitations = [PROXY_LIMITATION]
    if any(
        row["geographic_proxy"] for plan in plans.values() for row in plan["vector"]
    ):
        limitations.append(GEOGRAPHY_LIMITATION)
    if biomass.pellet_keys:
        limitations.append(PELLET_LIMITATION)
    for message in limitations:
        warnings.warn(message, UserWarning, stacklevel=2)
    for key in biomass.pellet_keys:
        substitutions = []
        for (dataset, exchange, original, market), index in zip(
            biomass.relinked, biomass.relinked_indices, strict=True
        ):
            if identity(dataset) == key:
                substitutions.append(
                    {
                        "original_index": index,
                        "source_exchange": copy.deepcopy(original),
                        "target_market": identity(market),
                        "target_amount": exchange["amount"],
                        "basis": "unchanged signed dry kilograms; factor +1",
                        "limitations": PELLET_LIMITATION,
                    }
                )
        if substitutions:
            biomass.providers[key][
                "premise biomass feedstock substitutions"
            ] = substitutions
    for plan in plans.values():
        for exchange in plan["market"]["exchanges"]:
            exchange.pop("premise biomass technology", None)
    replace_scenario_inventory(scenario, biomass.database)
    scenario["index"] = biomass.index
    scenario["cache"] = biomass.cache
    scenario.setdefault("mapping", {})["biomass"] = biomass.suppliers
    scenario.setdefault("background context", expected)
    scenario.setdefault("native sector limitations", {})["biomass"] = limitations
    return scenario
