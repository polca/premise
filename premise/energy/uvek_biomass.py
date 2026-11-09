"""Native dry-mass wood-chip stage; the public biomass gate stays closed pending rollout."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from collections import defaultdict

from ..core.transformation import BaseTransformation
from ..inventory.activity_maps import BIOMASS_TYPES, InventorySet, get_mapping
from ..reporting.provenance import record_change_event
from ..validation.validation_framework import (
    ValidationIssue,
    ValidationPhaseResult,
    ValidationRuleResult,
)

MARKET_NAME = "market for lignocellulosic biomass, used as fuel"
MARKET_PRODUCT = "lignocellulosic biomass"
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
    "remain unquantified. This wood-chip stage does not establish pellet coverage."
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
    """Plan and validate a native wood-chip update without changing ECI code paths.

    Pellet/upstream feedstock coverage and public dispatch remain separate rollout
    gates. Native producers and material/byproduct users are never rewritten here.
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
        self.relinked = []

    def get_ecoinvent_locs(self):
        """Map only reviewed suppliers and their fuel consumers.

        Unrelated native legacy regions (for example CPA in material production)
        do not acquire an invented geography alias or prevent this stage.
        """
        definitions = get_mapping(BIOMASS_TYPES, "uvek_aliases", model=self.model)
        names = {NATIVE_MIX}
        for entry in definitions.values():
            names.update(entry.get("fltr", {}).get("name", []))
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
            for exchange in dataset["exchanges"]:
                if (
                    exchange["type"] != "technosphere"
                    or exchange.get("unit") != "kilogram"
                    or exchange.get("name") not in source_names
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
                plans[region]["consumers"].append((dataset, exchange))
        return plans

    @staticmethod
    def _energy_consumer(dataset):
        """Keep material processing and allocated byproduct outputs outside this stage."""
        if dataset["unit"] in {"megajoule", "kilowatt hour"}:
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
                "limitations": [PROXY_LIMITATION, GEOGRAPHY_LIMITATION],
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
            for dataset, exchange in plan["consumers"]:
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
                self._validation_relinked_targets[id(dataset)] = dataset
                record_change_event(
                    self,
                    dataset,
                    "modified",
                    sector="biomass",
                    reason_code="biomass.uvek.native_dry_mass_relink",
                    explanation="Original signed dry kg and all quantity/uncertainty metadata retained.",
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
