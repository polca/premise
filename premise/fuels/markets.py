import uuid
import copy
import numpy as np
import math
from collections import defaultdict

from ..inventory.activity_maps import FUELS_TECHS, InventorySet, act_fltr, get_mapping
from .utils import (
    fetch_mapping,
    update_dataset,
    calculate_fuel_properties,
)
from ..core.transformation import ws, get_shares_from_production_volume
from ..core.utils import get_fuel_properties
from ..inventory.inventory_store import clone_inventory_dataset
from .properties import (
    PROPERTIES_KEY,
    CarbonOriginResolver,
    activity_key,
    energy_per_unit,
    summarize_components,
)


class FuelMarketsMixin:

    @staticmethod
    def _prefer_non_recursive_market_suppliers(mapping, name, reference_product, unit):
        """Drop source markets that would relink to the market being created.

        Some ecoinvent fuel mappings contain both direct production datasets and
        a market with the same semantic key as the prospective regional market.
        Keeping that source market creates a regional -> World -> regional cycle
        once the original market is emptied and relinked.  Retain it only when a
        technology has no non-recursive provider to use instead.
        """
        filtered = {}
        for technology, suppliers in mapping.items():
            non_recursive = [
                supplier
                for supplier in suppliers
                if not (
                    supplier.get("name") == name
                    and supplier.get("reference product") == reference_product
                    and supplier.get("unit") == unit
                )
            ]
            filtered[technology] = non_recursive or suppliers
        return filtered

    def process_and_add_fuel_markets(self, reference_fuel, **kwargs):
        """Convert IAM energy shares to supplier quantities per reference MJ.

        Normalize eligible energy shares before converting quantities. Keep
        legacy market auxiliaries per reference output and record their separate
        boundary. Carbon properties describe full oxidation, not tailpipe flows.
        """
        template = get_mapping(
            FUELS_TECHS, var=f"{getattr(self, 'family', 'ecoinvent')}_market_template"
        ).get(reference_fuel)
        if template:
            kwargs["market_ancillaries"] = self._native_fuel_market_ancillaries(
                reference_fuel, template
            )
        kwargs["mapping"] = self._prefer_non_recursive_market_suppliers(
            kwargs["mapping"],
            kwargs["name"],
            kwargs["reference_product"],
            kwargs["unit"],
        )
        specifications = get_fuel_properties()
        reference = energy_per_unit(reference_fuel, kwargs["unit"], specifications)
        factors = {}
        for technology in kwargs["mapping"]:
            # Mappings can also include unused legacy providers in other units.
            # Validate the units of the providers actually selected below.
            basis = specifications[technology].get(
                "lhv reference unit",
                "cubic meter" if technology == "natural gas" else "kilogram",
            )
            factors[technology] = reference / energy_per_unit(
                technology, basis, specifications
            )
        resolver = CarbonOriginResolver(self.database)
        callback = kwargs.pop("additional_exchanges_fn", None)

        def annotate(dataset):
            if template:
                dataset["premise fuel delivery template"] = {
                    k: template[k]
                    for k in ("source", "fallback_location", "limitations")
                }
            components = []
            for exchange in dataset["exchanges"]:
                if exchange["type"] != "technosphere":
                    continue
                technology = exchange["premise market technology"]
                supplier = resolver.index[activity_key(exchange)]
                share, evidence = resolver.fuel_origin(
                    technology, supplier, specifications
                )
                components.append(
                    {
                        **{
                            key: exchange[key]
                            for key in ("name", "product", "location", "unit", "amount")
                        },
                        "technology": technology,
                        "energy_MJ_per_unit": energy_per_unit(
                            technology, exchange["unit"], specifications
                        ),
                        "CO2_kg_per_MJ": float(specifications[technology]["co2"]),
                        "non_fossil_share": share,
                        "carbon source": evidence,
                    }
                )
            properties = summarize_components(components, reference, kwargs["unit"])
            if not math.isclose(
                properties["energy MJ per unit"], reference, rel_tol=1e-10
            ):
                raise ValueError(
                    f"Fuel energy does not close for {activity_key(dataset)}"
                )
            dataset[PROPERTIES_KEY] = properties
            if callback:
                callback(dataset)

        start = len(self.database)
        kwargs["retain_validation_technology"] = True
        self.process_and_add_markets(
            **kwargs,
            conversion_factor=factors,
            normalize_before_conversion=True,
            additional_exchanges_fn=annotate,
        )
        # World markets purchase regional energy-equivalent outputs. Their
        # ledger must retain unresolved carbon separately from known origins.
        created = list(self.database[start:])
        index = {activity_key(ds): ds for ds in created}
        for dataset in created:
            if dataset.get("location") != "World" or PROPERTIES_KEY in dataset:
                continue
            totals = {
                key: 0.0
                for key in (
                    "energy MJ per unit",
                    "fossil CO2 kg per unit",
                    "non-fossil CO2 kg per unit",
                    "unresolved CO2 kg per unit",
                )
            }
            components = []
            for exchange in dataset["exchanges"]:
                if exchange["type"] != "technosphere":
                    continue
                properties = index[activity_key(exchange)][PROPERTIES_KEY]
                for key in totals:
                    totals[key] += exchange["amount"] * properties[key]
                components.append(
                    {
                        **{
                            key: exchange[key]
                            for key in ("name", "product", "location", "unit", "amount")
                        },
                        "energy_MJ_per_unit": properties["energy MJ per unit"],
                    }
                )
            if not math.isclose(totals["energy MJ per unit"], reference, rel_tol=1e-10):
                raise ValueError(
                    f"World fuel energy does not close for {activity_key(dataset)}"
                )
            dataset[PROPERTIES_KEY] = {
                "reference unit": kwargs["unit"],
                "reference energy MJ per unit": reference,
                **totals,
                "components": components,
                "carbon convention": "Production-weighted regional full-oxidation properties",
            }

        self._handover_fuel_market_aliases(kwargs)

    def _native_fuel_market_ancillaries(self, fuel, template):
        """Preserve declared delivery infrastructure separately from fuel shares."""
        cache = self.__dict__.setdefault("_fuel_market_ancillaries", {})
        if fuel not in cache:
            preserved = defaultdict(list)
            datasets = act_fltr(self.database, fltr=template["source"], exact=True)
            if not datasets:
                raise ValueError(
                    f"Missing reviewed fuel delivery template: {template['source']!r}"
                )
            for dataset in datasets:
                baseline = dataset.get("premise fuel ancillary baseline")
                if baseline is None:
                    if dataset.get("emptied"):
                        raise ValueError(
                            "An emptied fuel template lacks its delivery baseline; rebuild from source."
                        )
                    baseline = [
                        clone_inventory_dataset(e)
                        for e in dataset["exchanges"]
                        if e["type"] == "biosphere"
                        or (
                            e["type"] == "technosphere"
                            and e["name"] not in template["fuel_inputs"]
                        )
                    ]
                    dataset["premise fuel ancillary baseline"] = baseline
                preserved[dataset["location"]].extend(clone_inventory_dataset(baseline))
            fallback = template["fallback_location"]
            if fallback not in preserved:
                raise ValueError(
                    f"Missing reviewed fuel delivery fallback: {fallback!r}"
                )
            preserved["GLO"] = clone_inventory_dataset(preserved[fallback])
            cache[fuel] = dict(preserved)
        return cache[fuel]

    def _preserve_fuel_supply_networks(self):
        """Keep declared historical trade networks outside the future blend."""
        definitions = get_mapping(
            FUELS_TECHS, var=f"{getattr(self, 'family', 'ecoinvent')}_market_handovers"
        )
        for entries in definitions.values():
            for entry in entries:
                if not entry.get("preserve_supply_network", False):
                    continue
                required = entry.get("requires_iam_array")
                array = getattr(self.iam_data, required, None) if required else None
                if required and (array is None or float(array.sum()) <= 0):
                    continue
                originals = [
                    a
                    for a in act_fltr(self.database, fltr=entry["source"], exact=True)
                    if not a.get("regionalized", False)
                ]
                if originals and all(
                    a.get("premise fuel market handover") for a in originals
                ):
                    original_keys = {activity_key(a) for a in originals}
                    preserved = {
                        activity_key(a["premise preserved fuel supply"]["source"])
                        for a in self.database
                        if a.get("premise preserved fuel supply")
                    }
                    expected = {
                        a.get("premise preserved fuel supply count") for a in originals
                    }
                    if expected != {len(preserved & original_keys)}:
                        raise ValueError(
                            "Existing fuel handovers lack their fossil baseline; rebuild the scenario from source."
                        )
                    continue  # Incremental repetition retains the existing baseline.
                pool = {activity_key(a): a for a in originals}
                regions = [
                    r
                    for r in self.regions
                    if r != "World" or entry.get("preserve_world", True)
                ]
                roots = self.region_to_proxy_dataset_mapping(
                    originals, regions=regions
                ).values()
                pending = [
                    activity_key(e)
                    for a in roots
                    for e in ws.technosphere(a)
                    if activity_key(e) in pool
                ]
                needed = set()
                while pending:
                    key = pending.pop()
                    if key in needed:
                        continue
                    needed.add(key)
                    pending.extend(
                        activity_key(e)
                        for e in ws.technosphere(pool[key])
                        if activity_key(e) in pool
                    )
                for original in originals:
                    original["premise preserved fuel supply count"] = len(needed)
                copies = {}
                for original in originals:
                    if activity_key(original) not in needed:
                        continue
                    if original.get("emptied"):
                        raise ValueError(
                            "Cannot preserve an already emptied fossil supply network."
                        )
                    frozen = clone_inventory_dataset(original)
                    frozen[
                        "name"
                    ] += f" [fossil supply baseline, {original['location']}]"
                    frozen["code"] = uuid.uuid4().hex
                    frozen.pop("input", None)
                    frozen["premise preserved fuel supply"] = {
                        "source": {
                            k: original[k]
                            for k in ("name", "reference product", "location", "unit")
                        },
                        "purpose": "Historical fossil trade component, separate from IAM mixed delivery.",
                    }
                    for production in ws.production(frozen):
                        production["name"] = frozen["name"]
                        production.pop("input", None)
                    copies[activity_key(original)] = frozen
                # Country-specific names prevent geographical relinking from
                # reallocating an explicitly modelled trade edge to another country.
                for dataset in [*originals, *copies.values()]:
                    for exchange in ws.technosphere(dataset):
                        frozen = copies.get(activity_key(exchange))
                        if frozen is not None:
                            exchange["name"] = frozen["name"]
                            exchange.pop("input", None)
                self.database.extend(copies.values())
                self.add_to_index(list(copies.values()))

    def _handover_fuel_market_aliases(self, market):
        """Reuse geographic forwarding for declared source-family identities.

        Source forwarders are eligible by default; replacement of another
        complete supply needs an explicit declaration. Complete regional
        fossil inventories remain separate mixture components.
        """
        definitions = get_mapping(
            FUELS_TECHS, var=f"{getattr(self, 'family', 'ecoinvent')}_market_handovers"
        )
        for entries in definitions.values():
            for entry in entries:
                target = entry["target"]
                if (target["name"], target["reference product"], target["unit"]) != (
                    market["name"],
                    market["reference_product"],
                    market["unit"],
                ):
                    continue
                aliases = [
                    a
                    for a in act_fltr(self.database, fltr=entry["source"], exact=True)
                    if not a.get("regionalized", False)
                ]
                if not aliases:
                    if entry.get("optional_source", False):
                        continue
                    raise ValueError(
                        f"Missing reviewed fuel market handover: {entry['source']!r}"
                    )
                for alias in aliases:
                    production = list(ws.production(alias))
                    if (
                        (
                            not alias.get("emptied")
                            and not entry.get("replace_original_supply", False)
                        )
                        or len(production) != 1
                        or production[0]["amount"] != 1
                    ):
                        raise ValueError(
                            f"Fuel handover requires a unit-reference source forwarder: {alias['name']!r}"
                        )
                factor = float(entry["conversion_factor"])
                if not math.isfinite(factor) or factor <= 0:
                    raise ValueError(
                        "Fuel market handover needs a finite positive conversion factor."
                    )
                self.empty_original_datasets(
                    aliases,
                    production_shares={},
                    loc_map={
                        a["location"]: self.ecoinvent_to_iam_loc.get(
                            a["location"], "World"
                        )
                        for a in aliases
                    },
                    target=target,
                    conversion_factor=factor,
                )
                for alias in aliases:
                    alias.pop("has_downstream_consumer", None)
                    alias["premise fuel market handover"] = {
                        "target": target,
                        "conversion_factor": factor,
                        "limitations": entry["limitations"],
                    }
                # Keep these source identities visible: consumers retain their
                # original product/unit and uncertainty through the forwarding node.
                self.add_to_index(aliases)

    def generate_fuel_supply_chains(self):
        """Duplicate fuel chains and make them IAM region-specific."""
        self.generate_hydrogen_activities()
        self.generate_biogas_activities()
        self.generate_synthetic_fuel_activities()
        self.generate_biofuel_activities()

    def get_fuel_mapping(self) -> dict:
        """
        Define filter functions that decide which wurst datasets to modify.
        :return: dictionary that contains filters and functions
        :rtype: dict
        """

        return {
            fuel: {
                "find_share": self.fetch_fuel_share,
                "fuel filters": self.fuel_map[fuel],
            }
            for fuel in self.iam_fuel_markets.variables.values
        }

    def fetch_fuel_share(
        self, fuel: str, relevant_fuel_types: tuple[str], region: str, period: int
    ) -> float:
        """
        Return the percentage of a specific fuel type in the fuel mix for a specific region.
        :param fuel: the name of the fuel to fetch the percentage for
        :param relevant_fuel_types: a list of relevant fuel types to include in the calculation
        :param region: the IAM region to fetch the data for
        :param period: the period to fetch the data for
        :return: the percentage of the specified fuel type in the fuel mix for the region
        """

        relevant_variables = [
            v
            for v in self.iam_fuel_markets.variables.values
            if any(v.lower().startswith(x.lower()) for x in relevant_fuel_types)
        ]

        if period == 0:
            if self.year in self.iam_fuel_markets.coords["year"].values:
                fuel_share = (
                    self.iam_fuel_markets.sel(
                        region=region, variables=fuel, year=self.year
                    )
                    / self.iam_fuel_markets.sel(
                        region=region, variables=relevant_variables, year=self.year
                    ).sum(dim="variables")
                ).values

            else:
                fuel_share = (
                    (
                        self.iam_fuel_markets.sel(region=region, variables=fuel)
                        / self.iam_fuel_markets.sel(
                            region=region, variables=relevant_variables
                        ).sum(dim="variables")
                    )
                    .interp(
                        year=self.year,
                    )
                    .values
                )
        else:
            start_period = self.year
            end_period = self.year + period
            # make sure end_period is not greater than
            # the last year in the dataset
            end_period = min(
                end_period, self.iam_fuel_markets.coords["year"].values[-1]
            )
            fuel_share = (
                (
                    self.iam_fuel_markets.sel(region=region, variables=fuel)
                    / self.iam_fuel_markets.sel(
                        region=region, variables=relevant_variables
                    ).sum(dim="variables")
                )
                .fillna(0)
                .interp(
                    year=np.arange(start_period, end_period + 1),
                )
                .mean(dim="year")
                .values
            )

        if np.isnan(fuel_share):
            print(
                f"Warning: incorrect fuel share for {fuel} in {region} (-> set to 0%)."
            )
            fuel_share = 0.0

        return float(fuel_share)
