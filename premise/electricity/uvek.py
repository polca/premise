"""UVEK 2026 cut-off electricity pilot, retaining the native grid topology.

The approved pilot applies IAM generation shares to native busbar mixes,
including residential PV. Country voltage grids retain their original losses,
infrastructure, emissions and supplier identities. This differs from the ECI
topology, which inserts residential PV at low voltage. No native loss or
production-volume estimates are manufactured for this adapter.
"""

from __future__ import annotations

import copy
import math
import warnings
from collections import defaultdict

import numpy as np

from ..core.utils import get_efficiency_solar_photovoltaics, rescale_exchanges
from ..inventory.inventory_store import (
    get_scenario_inventory,
    replace_scenario_inventory,
)
from ..reporting.provenance import record_change_event
from ..validation.validation_framework import (
    ValidationIssue,
    ValidationPhaseResult,
    ValidationRuleResult,
    record_validation_phase,
)
from .electricity import Electricity
from .photovoltaic import identity as pv_identity, module_area

NATIVE_MIXES = frozenset({"Electricity mix", "Electricity, production mix"})
NATIVE_GRIDS = {
    "high": "Electricity, high voltage, at grid",
    "medium": "Electricity, medium voltage, at grid",
    "low": "Electricity, low voltage, at grid",
}
TOPOLOGY_LIMITATION = (
    "UVEK electricity pilot retains native country voltage-grid losses, "
    "infrastructure and direct emissions. IAM generation, including residential "
    "solar, is supplied at the native busbar; residential solar does not bypass "
    "transmission and distribution losses. Ecoinvent-specific baseline coal and "
    "hydropower corrections, proxy synthesis, regional plant cloning and "
    "industrial-market rewriting are not applied to native UVEK inventories."
    " Native PV installations without explicit capacity/module metadata retain "
    "their source inventories."
)
SIGNED_WEIGHT_LIMITATION = (
    "UVEK supplier weights use positive net coefficients after signed source "
    "exchange aggregation by exact supplier identity. Negative source import "
    "exchanges remain unchanged; excluded negative net weighting coefficients "
    "are retained in diagnostics and provenance. No individual edge is clipped."
)
GEOGRAPHIC_SUPPLIER_LIMITATION = (
    "Where a country has no positive historical weights for an IAM technology, "
    "UVEK's global portfolio for that same technology or its sole mapped native "
    "generator is used as an approved geographic screening proxy. Generator "
    "geography, vintage and upstream energy supply can differ. IAM technology "
    "shares and native grid quantities are unchanged."
)
IGCC_FUEL_LIMITATION = (
    "IGCC hard-coal/lignite allocation uses the positive net coal-generation "
    "weights in UVEK's baseline global electricity mix. This historical fuel "
    "split approximates the unavailable IAM IGCC fuel split. The complete "
    "reviewed CLIC generators, including any CCS chain, are retained."
)
GLOBAL_SOURCE_MIX = ("Electricity mix", "Electricity mix", "GLO", "kilowatt hour")
NATIVE_COAL_FUELS = {
    "Electricity, hard coal, at power plant": "hard coal",
    "Electricity, lignite, at power plant": "lignite",
}
IGCC_FUEL_SLUGS = {
    "Coal IGCC": {
        "hard coal": "electricity-production-at-hard-coal-fired-igcc-power-plant",
        "lignite": "electricity-production-at-lignite-fired-igcc-power-plant",
    },
    "Coal IGCC CCS": {
        "hard coal": "electricity-production-at-hard-coal-fired-igcc-power-plant-pre-pipeline-200km-storage-1000m",
        "lignite": "electricity-production-at-lignite-fired-igcc-power-plant-pre-pipeline-200km-storage-1000m",
    },
}


class UVEKSourceWeightError(ValueError):
    """A source allocation requiring scientific review, with signed evidence."""

    def __init__(self, message, diagnostics):
        super().__init__(message)
        self.diagnostics = diagnostics


def identity(record):
    """Return the full, case-sensitive supplier identity used by exporters."""

    return (
        record["name"],
        record.get("reference product", record.get("product")),
        record["location"],
        record["unit"],
    )


def is_native(record, names):
    """Recognize only reviewed native name/product pairs, never ECI aliases."""

    return (
        record.get("name") in names
        and record.get("reference product", record.get("product")) == record["name"]
        and record.get("unit") == "kilowatt hour"
        and not record.get("clic provenance")
    )


def native_grid_losses(database):
    """Read source grid multipliers without averaging or changing quantities.

    These are upstream kWh per native output kWh, not the loss convention used
    by ECI self-consuming markets. Zero production volumes are never weights.
    """

    result = defaultdict(dict)
    upstream_names = {
        "high": "Electricity mix",
        "medium": NATIVE_GRIDS["high"],
        "low": NATIVE_GRIDS["medium"],
    }
    for dataset in database:
        if not is_native(dataset, NATIVE_GRIDS.values()):
            continue
        voltage = next(v for v, name in NATIVE_GRIDS.items() if name == dataset["name"])
        outputs = [e for e in dataset["exchanges"] if e["type"] == "production"]
        output = sum(float(e["amount"]) for e in outputs)
        inputs = [
            e
            for e in dataset["exchanges"]
            if e["type"] == "technosphere" and is_native(e, {upstream_names[voltage]})
        ]
        amount = sum(float(e["amount"]) for e in inputs)
        if not math.isfinite(output) or output <= 0 or not inputs:
            raise ValueError(
                f"Invalid native UVEK grid output/upstream link: {identity(dataset)!r}."
            )
        if not math.isfinite(amount) or amount < output:
            raise ValueError(
                f"Invalid native UVEK grid multiplier: {identity(dataset)!r}."
            )
        result[dataset["location"]][voltage] = {
            "upstream kWh per kWh": amount / output,
            "loss kWh per kWh": amount / output - 1,
            "upstream suppliers": [identity(e) for e in inputs],
        }
    return dict(result)


class UVEKElectricity(Electricity):
    """Reweight native busbar mixes with exact identities and source weights."""

    def __init__(self, *args, family="uvek", biosphere_version=None, **kwargs):
        version = kwargs.get("version", args[5] if len(args) > 5 else None)
        system_model = kwargs.get("system_model", args[6] if len(args) > 6 else None)
        if (family, str(version), system_model, biosphere_version) != (
            "uvek",
            "2026",
            "cutoff",
            "3.10",
        ):
            raise ValueError(
                "The UVEK electricity pilot requires family='uvek', version='2026', "
                "system_model='cutoff', biosphere_version='3.10'."
            )
        super().__init__(
            *args, family=family, biosphere_version=biosphere_version, **kwargs
        )
        self.background_context = {
            "family": self.family,
            "version": self.version,
            "system_model": self.system_model,
            "biosphere_family": "ecoinvent",
            "biosphere_version": self.biosphere_version,
        }
        self.providers = {}
        for dataset in self.database:
            key = identity(dataset)
            if key in self.providers:
                raise ValueError(
                    f"Ambiguous UVEK electricity supplier identity: {key!r}."
                )
            self.providers[key] = dataset
        self.native_mixes = [d for d in self.database if is_native(d, NATIVE_MIXES)]
        self.native_grids = [
            d for d in self.database if is_native(d, NATIVE_GRIDS.values())
        ]
        if not self.native_mixes or not self.native_grids:
            raise ValueError(
                "UVEK electricity requires native busbar mixes and voltage grids."
            )
        self.grid_exchanges = {
            identity(d): copy.deepcopy(list(d["exchanges"])) for d in self.native_grids
        }
        for dataset in self.native_grids:
            for exc in dataset["exchanges"]:
                if exc["type"] != "technosphere":
                    continue
                supplier = self.providers.get(identity(exc))
                if supplier is None or (
                    exc.get("input")
                    and tuple(exc["input"])
                    != (supplier.get("database"), supplier.get("code"))
                ):
                    raise ValueError(
                        f"Native UVEK grid supplier identity mismatch: {identity(exc)!r}."
                    )
        self.source_weights = self._source_supplier_weights()
        self.efficiency_limitations = []
        self.supplier_proxy_decisions = {}

    @property
    def supplier_proxy_limitations(self):
        return sorted({d["limitation"] for d in self.supplier_proxy_decisions.values()})

    def get_network_losses(self):
        """Keep losses by native country; never apply ECI loss conventions."""

        return native_grid_losses(self.database)

    def get_ecoinvent_locs(self):
        """Map only transformed native mixes, not unrelated UVEK geographies.

        UVEK contains legacy/custom locations such as CPA outside this pilot's
        electricity mixes. No geography alias or World fallback is invented for
        those untouched activities. Generator efficiency locations are checked
        explicitly only when an update is actually possible.
        """

        return sorted(
            {d["location"] for d in self.database if is_native(d, NATIVE_MIXES)}
        )

    def _source_supplier_weights(self):
        """Expand native aggregate/traded mixes to positive provider coefficients.

        Solve the source mix graph before changing any exchanges. This supports
        source trade loops without arbitrary recursion cutoffs. Mapped generation
        providers are leaves; their own auxiliary electricity is not a weight.
        """

        leaves = {
            identity(d)
            for datasets in self.powerplant_map.values()
            for d in datasets
            if not d.get("clic provenance")
        }
        # Traverse source-only, pure kWh aggregators, including intermediate gas
        # and photovoltaic mixes. Do not turn an unmapped generator's auxiliary
        # grid inputs, fuel, infrastructure or emissions into technology weights.
        carriers = {}
        pending = list(self.native_mixes)
        while pending:
            dataset = pending.pop()
            key = identity(dataset)
            if key in carriers or key in leaves:
                continue
            exchanges = list(dataset["exchanges"])
            inputs = [e for e in exchanges if e["type"] == "technosphere"]
            if (
                dataset.get("clic provenance")
                or not inputs
                or any(e["unit"] != "kilowatt hour" for e in inputs)
                or any(
                    e["type"] not in {"production", "technosphere"} for e in exchanges
                )
            ):
                continue
            carriers[key] = dataset
            for exc in inputs:
                supplier = self.providers.get(identity(exc))
                if supplier is None:
                    raise ValueError(
                        f"Missing exact native UVEK source supplier: {identity(exc)!r}."
                    )
                if exc.get("input") and tuple(exc["input"]) != (
                    supplier.get("database"),
                    supplier.get("code"),
                ):
                    raise ValueError(
                        f"Native UVEK source supplier code disagrees with identity: {identity(exc)!r}."
                    )
                pending.append(supplier)
        carrier_positions = {key: i for i, key in enumerate(carriers)}
        leaf_positions = {key: i for i, key in enumerate(sorted(leaves))}
        matrix = np.eye(len(carriers))
        amounts = np.zeros((len(carriers), len(leaves)))
        source_edges = []
        for key, dataset in carriers.items():
            row = carrier_positions[key]
            output = sum(
                float(e["amount"])
                for e in dataset["exchanges"]
                if e["type"] == "production"
            )
            if not math.isfinite(output) or output <= 0:
                raise ValueError(f"Invalid native UVEK mix production amount: {key!r}.")
            for exc in dataset["exchanges"]:
                if exc["type"] != "technosphere" or exc["unit"] != "kilowatt hour":
                    continue
                supplier = identity(exc)
                amount = float(exc["amount"]) / output
                if not math.isfinite(amount):
                    raise ValueError(
                        f"Invalid native UVEK source mix weight in {key!r}."
                    )
                source_edges.append(
                    {"carrier": key, "supplier": supplier, "coefficient": amount}
                )
                if supplier in carrier_positions:
                    matrix[row, carrier_positions[supplier]] -= amount
                elif supplier in leaf_positions:
                    amounts[row, leaf_positions[supplier]] += amount
        try:
            weights = np.linalg.solve(matrix, amounts)
        except np.linalg.LinAlgError as error:
            raise ValueError(
                "UVEK native mix weights cannot resolve the source trade graph."
            ) from error
        if not np.all(np.isfinite(weights)):
            raise ValueError("UVEK native mix graph produces invalid supplier weights.")
        self.source_weight_diagnostics = {
            "source exchange coefficients": source_edges,
            "net supplier coefficients": [
                {
                    "carrier": key,
                    "supplier": supplier,
                    "coefficient": float(weights[row, col]),
                }
                for key, row in carrier_positions.items()
                for supplier, col in leaf_positions.items()
                if weights[row, col] != 0
            ],
        }
        return {
            key: {
                supplier: float(weights[row, col])
                for supplier, col in leaf_positions.items()
                if weights[row, col] != 0
            }
            for key, row in carrier_positions.items()
        }

    def _iam_mix(self, location):
        region = self.ecoinvent_to_iam_loc[location]
        mix = self.iam_data.electricity_mix.sel(region=region)
        mix = (
            mix.sel(year=self.year)
            if self.year in mix.year.values
            else mix.interp(year=self.year)
        )
        values = np.asarray(mix.values, dtype=float)
        if not np.all(np.isfinite(values)):
            raise ValueError(f"Non-finite UVEK IAM electricity shares in {region}.")
        values = np.maximum(values, 0)
        if values.sum() <= 0:
            raise ValueError(f"No positive IAM electricity shares in {region}.")
        return region, dict(
            zip(map(str, mix.coords["variables"].values), values / values.sum())
        )

    def _suppliers(self, dataset, technology, region):
        candidates = {identity(d): d for d in self.powerplant_map.get(technology, ())}
        source = self.source_weights.get(identity(dataset), {})
        weighted = [
            (d, source[key]) for key, d in candidates.items() if source.get(key, 0) > 0
        ]
        if weighted:
            total = sum(weight for _, weight in weighted)
            return [
                (d, weight / total) for d, weight in weighted
            ], "native positive source mix coefficients"
        global_source = self.source_weights.get(GLOBAL_SOURCE_MIX, {})
        weighted = [
            (d, global_source[key])
            for key, d in candidates.items()
            if global_source.get(key, 0) > 0
        ]
        if weighted:
            total = sum(weight for _, weight in weighted)
            self.supplier_proxy_decisions[(identity(dataset), technology)] = {
                "limitation": GEOGRAPHIC_SUPPLIER_LIMITATION,
                "source portfolio": GLOBAL_SOURCE_MIX,
                "positive net coefficients": [
                    {"supplier": identity(d), "coefficient": w} for d, w in weighted
                ],
            }
            return [
                (d, weight / total) for d, weight in weighted
            ], "global native technology portfolio (geographic proxy)"
        if len(candidates) == 1 and technology not in IGCC_FUEL_SLUGS:
            supplier = next(iter(candidates.values()))
            if not supplier.get("clic provenance"):
                self.supplier_proxy_decisions[(identity(dataset), technology)] = {
                    "limitation": GEOGRAPHIC_SUPPLIER_LIMITATION,
                    "unique native supplier": identity(supplier),
                }
                return [
                    (supplier, 1.0)
                ], "unique mapped native generator (geographic proxy)"
        if technology in IGCC_FUEL_SLUGS:
            slugs = IGCC_FUEL_SLUGS[technology]
            templates = {}
            for fuel, slug in slugs.items():
                matches = [
                    d
                    for d in candidates.values()
                    if d.get("clic provenance", {}).get("slug") == slug
                    and f"electricity.yaml:{technology}"
                    in d["clic provenance"].get("role_bindings", ())
                ]
                if len(matches) == 1:
                    templates[fuel] = matches[0]
            # No unknown variant, duplicate version or non-reviewed CLIC role
            # can inherit a fuel share from this exact two-template approval.
            if len(templates) == len(slugs) == len(candidates):
                fuels = defaultdict(float)
                for key, weight in global_source.items():
                    if (
                        key[0] in NATIVE_COAL_FUELS
                        and key[1] == key[0]
                        and key[3] == "kilowatt hour"
                        and weight > 0
                    ):
                        fuels[NATIVE_COAL_FUELS[key[0]]] += weight
                total = sum(fuels.values())
                if total > 0 and all(fuels.get(fuel, 0) > 0 for fuel in slugs):
                    weights = {fuel: fuels.get(fuel, 0) / total for fuel in slugs}
                    self.supplier_proxy_decisions[(identity(dataset), technology)] = {
                        "limitation": IGCC_FUEL_LIMITATION,
                        "source portfolio": GLOBAL_SOURCE_MIX,
                        "positive net fuel coefficients": dict(fuels),
                        "baseline fuel shares": weights,
                    }
                    return [
                        (templates[fuel], weight)
                        for fuel, weight in weights.items()
                        if weight > 0
                    ], "baseline global coal fuel split (IGCC proxy)"
        # Other new CLIC-only roles can use a unique reviewed provider. The
        # two-fuel IGCC approval cannot fall back to just one of its templates.
        imported = (
            [d for d in candidates.values() if d.get("clic provenance")]
            if technology not in IGCC_FUEL_SLUGS
            else []
        )
        if imported and len(imported) == len(candidates):
            for location in (dataset["location"], region, None):
                matches = [
                    d for d in imported if location is None or d["location"] == location
                ]
                if len(matches) == 1:
                    return [(matches[0], 1.0)], "unique reviewed CLIC role supplier"
                if matches:
                    break
        raise UVEKSourceWeightError(
            f"UVEK electricity {technology!r} in {dataset['location']!r} ({region}) "
            "has no positive native within-technology source mix weights or unique "
            "reviewed CLIC-only role supplier. Review uvek_aliases/CLIC role bindings "
            "and supply source weights; equal supplier/geographic shares are not assumed.",
            self.source_weight_diagnostics,
        )

    def plan_mixes(self):
        """Resolve all technologies before mutating any native activity."""

        self.supplier_proxy_decisions = {}
        plans = []
        missing = []
        for dataset in self.native_mixes:
            region, mix = self._iam_mix(dataset["location"])
            output = sum(
                float(e["amount"])
                for e in dataset["exchanges"]
                if e["type"] == "production"
            )
            exchanges, evidence = [], {}
            for technology, share in mix.items():
                if share <= 0:
                    continue
                try:
                    suppliers, basis = self._suppliers(dataset, technology, region)
                except UVEKSourceWeightError as error:
                    source = self.source_weights.get(identity(dataset), {})
                    missing.append(
                        {
                            "mix": identity(dataset),
                            "IAM region": region,
                            "technology": technology,
                            "IAM share": float(share),
                            "reason": str(error),
                            "candidates": [
                                {
                                    "identity": identity(d),
                                    "input": (d.get("database"), d.get("code")),
                                    "net source coefficient": source.get(identity(d)),
                                    "CLIC slug": d.get("clic provenance", {}).get(
                                        "slug"
                                    ),
                                    "CLIC role bindings": d.get(
                                        "clic provenance", {}
                                    ).get("role_bindings"),
                                }
                                for d in self.powerplant_map.get(technology, ())
                            ],
                        }
                    )
                    continue
                source = self.source_weights.get(identity(dataset), {})
                evidence[technology] = {
                    "basis": basis,
                    "suppliers": [],
                    "signed net source coefficients": [
                        {"identity": identity(d), "coefficient": source[identity(d)]}
                        for d in self.powerplant_map.get(technology, ())
                        if identity(d) in source
                    ],
                }
                evidence[technology].update(
                    self.supplier_proxy_decisions.get(
                        (identity(dataset), technology), {}
                    )
                )
                for supplier, weight in suppliers:
                    key = identity(supplier)
                    if key not in self.providers or supplier["unit"] != "kilowatt hour":
                        raise ValueError(
                            f"Invalid UVEK electricity role supplier: {key!r}."
                        )
                    exc = {
                        "name": supplier["name"],
                        "product": supplier["reference product"],
                        "location": supplier["location"],
                        "unit": supplier["unit"],
                        "type": "technosphere",
                        "amount": float(share * weight * output),
                        "uncertainty type": 0,
                        "premise electricity technology": technology,
                    }
                    if supplier.get("database") and supplier.get("code"):
                        exc["input"] = (supplier["database"], supplier["code"])
                    exchanges.append(exc)
                    evidence[technology]["suppliers"].append(
                        {
                            "identity": key,
                            "share": weight,
                            "source coefficient": self.source_weights.get(
                                identity(dataset), {}
                            ).get(key),
                        }
                    )
                if not math.isclose(
                    sum(weight for _, weight in suppliers), 1, abs_tol=1e-12
                ):
                    raise ValueError(
                        f"Incomplete UVEK electricity supplier allocation: {technology!r}."
                    )
            plans.append((dataset, exchanges, region, mix, evidence))
        self.source_weight_diagnostics["supplier proxy decisions"] = [
            {"mix": key[0], "technology": key[1], **value}
            for key, value in self.supplier_proxy_decisions.items()
        ]
        if missing:
            roles = sorted({item["technology"] for item in missing})
            countries = sorted({item["mix"][2] for item in missing})
            raise UVEKSourceWeightError(
                f"UVEK electricity has {len(missing)} unresolved supplier allocations "
                f"across {len(countries)} native locations. Required roles: {', '.join(roles)}. "
                "No mix changed. Inspect exception.diagnostics['missing supplier allocations'] "
                "for every mix, country, IAM share and exact candidate identity. "
                "Positive native within-technology weights or a unique reviewed CLIC-only "
                "role supplier are required; equal supplier/geographic shares are not assumed.",
                {
                    **self.source_weight_diagnostics,
                    "missing supplier allocations": missing,
                },
            )
        return plans

    def update_efficiency_of_solar_pv(self):
        """Update explicit PV installation metadata without ECI name parsing."""

        installations = [d for d in self.database if "pv capacity kwp" in d]
        if not installations:
            return
        original_areas = {pv_identity(d): module_area(d) for d in installations}
        module_efficiency = get_efficiency_solar_photovoltaics()
        for dataset in installations:
            self._update_pv_2026_efficiency(dataset, module_efficiency)
            self._validation_relinked_targets[id(dataset)] = dataset
        self._update_pv_cleaning_water(original_areas)

    def update_electricity_efficiency(self):
        """Use exact mapped fuel carriers; never infer nuclear energy from mass.

        UVEK frequently separates fuel combustion into an MJ supplier. Native
        nuclear assemblies have no reviewed enrichment/MOX energy specification,
        so their baseline inventories remain explicit, reported limitations.
        """

        data = self.iam_data.electricity_technology_efficiencies
        fuel_roles = self.mapping.generate_powerplant_fuels_map()
        technologies = set(map(str, data.coords["variables"].values))
        for technology in sorted(technologies.intersection(self.powerplant_map)):
            allowed = {identity(d) for d in fuel_roles.get(technology, ())}
            for dataset in self.powerplant_map[technology]:
                if "new efficiency" in dataset.get("log parameters", {}):
                    continue
                fuels = [
                    exc
                    for exc in dataset["exchanges"]
                    if exc["type"] == "technosphere"
                    and identity(exc) in allowed
                    and float(exc["amount"]) > 0
                ]
                energy = 0.0
                for exc in fuels:
                    amount, unit = float(exc["amount"]), exc["unit"]
                    if unit == "megajoule":
                        energy += amount
                    elif unit == "kilowatt hour":
                        energy += amount * 3.6
                    elif unit in {"kilogram", "cubic meter"}:
                        fuel = self.fuel_map_reverse.get(exc["product"])
                        spec = self.fuels_specs.get(fuel, {}).get("lhv", {})
                        if not spec or not math.isfinite(
                            float(spec.get("value", float("nan")))
                        ):
                            fuels = []
                            break
                        energy += amount * float(spec["value"])
                    else:
                        fuels = []
                        break
                if not fuels or energy <= 0 or not math.isfinite(energy):
                    message = (
                        f"UVEK {technology} efficiency unavailable for {identity(dataset)!r}: "
                        "no exact mapped fuel input with a reviewed energy specification; "
                        "native baseline retained. Nuclear enrichment/MOX energy "
                        "is not inferred from a generic LHV."
                    )
                    self.efficiency_limitations.append(message)
                    record_change_event(
                        self,
                        dataset,
                        "unchanged",
                        sector="electricity",
                        reason_code="electricity.uvek.efficiency_unavailable",
                        explanation=message,
                        algorithm="exact fuel identity and reviewed energy specification",
                    )
                    continue
                output = sum(
                    float(e["amount"])
                    for e in dataset["exchanges"]
                    if e["type"] == "production"
                )
                current = 3.6 * output / energy
                region = self.ecoinvent_to_iam_loc.get(dataset["location"])
                if region is None:
                    try:
                        region = self.geo.ecoinvent_to_iam_location(dataset["location"])
                    except (KeyError, ValueError):
                        message = (
                            f"UVEK {technology} efficiency unavailable for {identity(dataset)!r}: "
                            "no reviewed IAM geographic mapping; source inventory retained."
                        )
                        self.efficiency_limitations.append(message)
                        record_change_event(
                            self,
                            dataset,
                            "unchanged",
                            sector="electricity",
                            reason_code="electricity.uvek.efficiency_unavailable",
                            explanation=message,
                        )
                        continue
                if region not in data.coords["region"].values:
                    raise ValueError(f"Missing UVEK IAM efficiency region: {region!r}.")
                change = self.find_iam_efficiency_change(
                    data=data, variable=technology, location=region
                )
                target = (
                    float(change)
                    if self.use_absolute_efficiency
                    else float(
                        np.clip(
                            current * change,
                            self.powerplant_min_efficiency.get(technology, 0),
                            self.powerplant_max_efficiency.get(technology, 1.5),
                        )
                    )
                )
                if not math.isfinite(target) or target <= 0:
                    raise ValueError(
                        f"Invalid UVEK IAM efficiency for {technology!r} in {region!r}."
                    )
                factor = current / target
                if not math.isclose(factor, 1.0, abs_tol=1e-12):
                    rescale_exchanges(dataset, factor)
                    self._validation_relinked_targets[id(dataset)] = dataset
                    dataset.setdefault("parameters", {})["efficiency"] = target
                    dataset.setdefault("log parameters", {}).update(
                        {"old efficiency": current, "new efficiency": target}
                    )
                    record_change_event(
                        self,
                        dataset,
                        "updated",
                        sector="electricity",
                        reason_code="electricity.uvek.iam_efficiency",
                        algorithm="exact fuel identity and reviewed energy specification",
                        iam_variable=technology,
                        computed_target_values={
                            "old efficiency": current,
                            "new efficiency": target,
                            "fuel suppliers": [identity(e) for e in fuels],
                        },
                    )
        if self.efficiency_limitations:
            warnings.warn(
                f"UVEK electricity: {len(self.efficiency_limitations)} generator efficiency "
                "updates unavailable; source inventories retained. See electricity "
                "limitations and validation/provenance, including nuclear fuel specifications.",
                UserWarning,
                stacklevel=2,
            )

    def update_native_mixes(self, plans):
        """Replace generation coefficients and retain all source grid exchanges."""

        for dataset, exchanges, region, mix, evidence in plans:
            dataset["exchanges"] = [
                e for e in dataset["exchanges"] if e["type"] != "technosphere"
            ] + exchanges
            dataset["uvek electricity provenance"] = {
                "background context": dict(self.background_context),
                "IAM region": region,
                "IAM shares": mix,
                "supplier weights": evidence,
                "limitation": TOPOLOGY_LIMITATION,
            }
            self._validation_relinked_targets[id(dataset)] = dataset
            self.track_validation_provenance(dataset, "premise electricity technology")
            record_change_event(
                self,
                dataset,
                "updated",
                sector="electricity",
                reason_code="electricity.uvek.native_busbar_mix",
                explanation=TOPOLOGY_LIMITATION,
                algorithm="IAM busbar shares with native within-technology source coefficients",
                computed_target_values=dataset["uvek electricity provenance"],
            )
        for dataset in self.native_grids:
            voltage = next(
                v for v, name in NATIVE_GRIDS.items() if name == dataset["name"]
            )
            record_change_event(
                self,
                dataset,
                "unchanged",
                sector="electricity",
                reason_code="electricity.uvek.native_grid_retained",
                explanation="Native grid losses, network infrastructure, direct emissions and supplier codes retained.",
                algorithm="native voltage-grid topology",
                computed_target_values={
                    "background context": self.background_context,
                    "source grid": self.network_loss[dataset["location"]][voltage],
                },
            )
        if plans:
            record_change_event(
                self,
                plans[0][0],
                "inspected",
                sector="electricity",
                reason_code="electricity.uvek.signed_source_weights",
                explanation=SIGNED_WEIGHT_LIMITATION,
                algorithm="signed exact-supplier aggregation before positive net weighting",
                computed_target_values=self.source_weight_diagnostics,
            )

    def validate_native_mixes(self, plans):
        """Check every IAM technology, exact supplier link and native grid chain."""

        count = 0
        for dataset, _, _, _, _ in plans:
            _, expected = self._iam_mix(dataset["location"])
            actual = defaultdict(float)
            output = sum(
                float(e["amount"])
                for e in dataset["exchanges"]
                if e["type"] == "production"
            )
            for exc in dataset["exchanges"]:
                if exc["type"] != "technosphere":
                    continue
                provider = self.providers.get(identity(exc))
                if provider is None or (
                    exc.get("input")
                    and tuple(exc["input"])
                    != (provider.get("database"), provider.get("code"))
                ):
                    raise ValueError(
                        f"UVEK electricity export supplier identity mismatch: {identity(exc)!r}."
                    )
                actual[exc["premise electricity technology"]] += (
                    float(exc["amount"]) / output
                )
            if any(
                not math.isclose(
                    actual.get(tech, 0), share, rel_tol=1e-10, abs_tol=1e-12
                )
                for tech, share in expected.items()
            ) or set(actual) - set(expected):
                raise ValueError(
                    f"UVEK electricity supplier vector differs from IAM for {identity(dataset)!r}."
                )
            count += 1
        for dataset in self.native_grids:
            if list(dataset["exchanges"]) != self.grid_exchanges[identity(dataset)]:
                raise ValueError(
                    f"Native UVEK voltage-grid quantities changed: {identity(dataset)!r}."
                )
        efficiency_issues = tuple(
            ValidationIssue(
                "electricity.uvek.efficiency_unavailable",
                "warning",
                message,
                checked_object_count=1,
            )
            for message in self.efficiency_limitations
        )
        signed_count = sum(
            edge["coefficient"] < 0
            for edge in self.source_weight_diagnostics["source exchange coefficients"]
        )
        signed_issues = (
            (
                ValidationIssue(
                    "electricity.uvek.signed_source_weighting",
                    "warning",
                    SIGNED_WEIGHT_LIMITATION,
                    checked_object_count=signed_count,
                ),
            )
            if signed_count
            else ()
        )
        proxy_issues = tuple(
            ValidationIssue(
                "electricity.uvek.supplier_proxy",
                "warning",
                message,
                checked_object_count=len(self.supplier_proxy_decisions),
            )
            for message in self.supplier_proxy_limitations
        )
        return ValidationPhaseResult(
            phase_id="sector:electricity:uvek",
            kind="sector",
            rule_results=(
                ValidationRuleResult(
                    "electricity.uvek.supplier_vectors", "error", "applicable", count
                ),
                ValidationRuleResult(
                    "electricity.uvek.native_grids",
                    "error",
                    "applicable",
                    len(self.native_grids),
                ),
                ValidationRuleResult(
                    "electricity.uvek.native_topology_limitation",
                    "warning",
                    "applicable",
                    count,
                    issues=(
                        ValidationIssue(
                            "electricity.uvek.native_topology_limitation",
                            "warning",
                            TOPOLOGY_LIMITATION,
                            checked_object_count=count,
                        ),
                    ),
                ),
                ValidationRuleResult(
                    "electricity.uvek.efficiency_unavailable",
                    "warning",
                    "applicable",
                    len(efficiency_issues),
                    issues=efficiency_issues,
                ),
                ValidationRuleResult(
                    "electricity.uvek.signed_source_weighting",
                    "warning",
                    "applicable",
                    signed_count,
                    issues=signed_issues,
                ),
                ValidationRuleResult(
                    "electricity.uvek.supplier_proxy",
                    "warning",
                    "applicable",
                    len(self.supplier_proxy_decisions),
                    issues=proxy_issues,
                ),
            ),
        )


def update_uvek_electricity(
    scenario,
    version,
    system_model,
    use_absolute_efficiency=False,
    *,
    family="uvek",
    biosphere_version=None,
):
    """Run the native electricity pilot through the normal scenario entry point."""

    electricity = UVEKElectricity(
        database=get_scenario_inventory(scenario),
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        use_absolute_efficiency=use_absolute_efficiency,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
        family=family,
        biosphere_version=biosphere_version,
    )
    context = scenario.get("background context")
    if context is not None and context != electricity.background_context:
        raise ValueError(
            "UVEK electricity arguments disagree with scenario background context."
        )
    if scenario["iam data"].electricity_mix is None:
        return scenario
    plans = electricity.plan_mixes()
    warnings.warn(TOPOLOGY_LIMITATION, UserWarning, stacklevel=2)
    for limitation in electricity.supplier_proxy_limitations:
        warnings.warn(limitation, UserWarning, stacklevel=2)
    signed_edges = [
        e
        for e in electricity.source_weight_diagnostics["source exchange coefficients"]
        if e["coefficient"] < 0
    ]
    if signed_edges:
        warnings.warn(SIGNED_WEIGHT_LIMITATION, UserWarning, stacklevel=2)
    # Only metadata-driven PV installations and mapped IAM fuel efficiencies are
    # shared with the ECI path. Native topology needs none of its synthetic grids,
    # reference corrections, proxy synthesis or generic supplier-name fallbacks.
    electricity.update_efficiency_of_solar_pv()
    if scenario["iam data"].electricity_technology_efficiencies is not None:
        electricity.update_electricity_efficiency()
    electricity.update_native_mixes(plans)
    record_validation_phase(scenario, electricity.validate_native_mixes(plans))
    electricity.clear_validation_provenance()
    replace_scenario_inventory(scenario, electricity.database)
    scenario["index"] = electricity.index
    scenario["cache"] = electricity.cache
    scenario.setdefault("mapping", {})["electricity"] = electricity.powerplant_map
    scenario.setdefault("background context", dict(electricity.background_context))
    scenario["electricity limitations"] = [
        TOPOLOGY_LIMITATION,
        *([SIGNED_WEIGHT_LIMITATION] if signed_edges else []),
        *electricity.efficiency_limitations,
        *electricity.supplier_proxy_limitations,
    ]
    return scenario
