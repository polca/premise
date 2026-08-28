"""
Integrates projections regarding heat production and supply.
"""

import copy
import uuid

import xarray as xr

from .activity_maps import InventorySet
from .filesystem_constants import VARIABLES_DIR
from .heat_data import load_heat_mapping
from .inventory_imports import get_biosphere_code
from .logger import create_logger
from .provenance import record_change_event
from .inventory_store import get_scenario_inventory, replace_scenario_inventory
from .marginal_mixes import consequential_method
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    List,
    find_fuel_efficiency,
    get_shares_from_production_volume,
    ws,
)
from .validation import HeatValidation
from .validation_framework import record_validation_phase
from .utils import rescale_exchanges

logger = create_logger("heat")

SECONDARY_MARKET = {
    "name": "market for heat, secondary, district or industrial",
    "reference product": "heat, district or industrial",
}
BUILDINGS_MARKET = {
    "name": "market for heat, for buildings",
    "reference product": "heat, central or small-scale",
}
INDUSTRIAL_MARKET = {
    "name": "market for heat, district or industrial",
    "reference product": "heat, district or industrial",
}

BUILDING_LEGACY_INPUTS = [
    {
        "name": name,
        "reference product": product,
    }
    for name, product in [
        (
            "market for heat, central or small-scale, other than natural gas",
            "heat, central or small-scale, other than natural gas",
        ),
        (
            "market group for heat, central or small-scale, other than natural gas",
            "heat, central or small-scale, other than natural gas",
        ),
        (
            "market for heat, central or small-scale, biomethane",
            "heat, central or small-scale, biomethane",
        ),
        (
            "market group for heat, central or small-scale, biomethane",
            "heat, central or small-scale, biomethane",
        ),
        (
            "market for heat, central or small-scale, Jakobsberg",
            "heat, central or small-scale, Jakobsberg",
        ),
        (
            "market for heat, central or small-scale, natural gas",
            "heat, central or small-scale, natural gas",
        ),
        (
            "market group for heat, central or small-scale, natural gas",
            "heat, central or small-scale, natural gas",
        ),
        (
            "market for heat, central or small-scale, natural gas and heat pump, Jakobsberg",
            "heat, central or small-scale, natural gas and heat pump, Jakobsberg",
        ),
        (
            "market for heat, central or small-scale, natural gas, Jakobsberg",
            "heat, central or small-scale, natural gas, Jakobsberg",
        ),
    ]
]

INDUSTRIAL_LEGACY_INPUTS = [
    {
        "name": name,
        "reference product": product,
    }
    for name, product in [
        (
            "market for heat, district or industrial, natural gas",
            "heat, district or industrial, natural gas",
        ),
        (
            "market group for heat, district or industrial, natural gas",
            "heat, district or industrial, natural gas",
        ),
        (
            "market for heat, district or industrial, other than natural gas",
            "heat, district or industrial, other than natural gas",
        ),
        (
            "market group for heat, district or industrial, other than natural gas",
            "heat, district or industrial, other than natural gas",
        ),
        (
            "market for heat, from steam, in chemical industry",
            "heat, from steam, in chemical industry",
        ),
    ]
]


def normalize_cogeneration_heat_efficiency(database, regions) -> None:
    """Apply the historical cogeneration repair before read-only validation."""

    for dataset in database:
        if not (
            "heat" in dataset["name"]
            and dataset["unit"] == "megajoule"
            and "co-generation" in dataset["name"]
            and dataset["location"] in regions
            and not any(
                token in dataset["name"]
                for token in (
                    "heat pump",
                    "heat recovery",
                    "heat storage",
                    "treatment of",
                    "market for",
                    "market group for",
                    "frozen legacy mix",
                    "nuclear cogeneration",
                )
            )
        ):
            continue

        exchanges = dataset["exchanges"]
        energy_input = sum(
            exchange["amount"]
            for exchange in exchanges
            if exchange["unit"] == "megajoule" and exchange["type"] == "technosphere"
        )
        energy_input += sum(
            exchange["amount"]
            for exchange in exchanges
            if exchange["unit"] == "megajoule"
            and exchange["type"] == "biosphere"
            and exchange["name"].startswith("Energy")
        )
        energy_input += sum(
            exchange["amount"] * 26.4
            for exchange in exchanges
            if "hard coal" in exchange["name"]
            and exchange["type"] == "technosphere"
            and exchange["unit"] == "kilogram"
        )
        energy_input += sum(
            exchange["amount"]
            for exchange in exchanges
            if "briquettes" in exchange["name"]
            and exchange["type"] == "technosphere"
            and exchange["unit"] == "megajoule"
        )
        for token in ("natural gas", "liquefied petroleum gas", "methane"):
            energy_input += sum(
                exchange["amount"] * (36 if exchange["unit"] == "cubic meter" else 47.5)
                for exchange in exchanges
                if token in exchange["name"]
                and exchange["type"] == "technosphere"
                and exchange["unit"] in {"cubic meter", "kilogram"}
            )
        for token, heating_value in (
            ("diesel", 42.6),
            ("light fuel oil", 42.6),
            ("heavy fuel oil", 38.5),
            ("biogas", 22.7),
            ("hydrogen", 120),
            ("methanol", 20),
        ):
            unit = "cubic meter" if token == "biogas" else "kilogram"
            energy_input += sum(
                exchange["amount"] * heating_value
                for exchange in exchanges
                if token in exchange["name"]
                and exchange["type"] == "technosphere"
                and exchange["unit"] == unit
            )
        energy_input += sum(
            exchange["amount"] * 16.2
            for exchange in exchanges
            if any(token in exchange["name"] for token in ("biomass", "wood", "timber"))
            and "ethanol" not in exchange["name"]
            and exchange["type"] == "technosphere"
            and exchange["unit"] == "kilogram"
        )
        energy_input += sum(
            exchange["amount"] * 3.6
            for exchange in exchanges
            if "electricity" in exchange["name"]
            and exchange["type"] == "technosphere"
            and exchange["unit"] == "kilowatt hour"
        )
        if energy_input > 0:
            efficiency = 1 / energy_input
            if efficiency > 3.0:
                rescale_exchanges(dataset, efficiency / 3.0)


def _update_heat(scenario, version, system_model):

    heat_layers = (
        "buildings_heat_end_use",
        "industrial_heat_end_use",
        "secondary_heat_supply",
    )
    if all(getattr(scenario["iam data"], layer, None) is None for layer in heat_layers):
        print("No heat scenario data available -- skipping heat transformation")
        scenario["heat diagnostics"] = copy.deepcopy(
            getattr(scenario["iam data"], "heat_diagnostics", {})
        )
        return scenario

    heat = Heat(
        database=get_scenario_inventory(scenario),
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    heat.fetch_fuel_market_co2_emissions()
    heat.regionalize_activities()
    heat.adjust_carbon_dioxide_emissions()

    heat.create_layered_heat_markets()

    # Frozen proxies must retain their copied supply composition. Generated
    # markets themselves can participate in ordinary link resolution; only the
    # targeted legacy-link rewrite above excludes their dataset codes.
    heat.relink_datasets(excludes_datasets=["heat supply, frozen legacy mix"])
    heat.assert_no_heat_cycles()
    normalize_cogeneration_heat_efficiency(heat.database, scenario["iam data"].regions)

    validate = HeatValidation(
        model=scenario["model"],
        scenario=scenario["pathway"],
        year=scenario["year"],
        regions=scenario["iam data"].regions,
        database=heat.database,
        iam_data=scenario["iam data"],
    )

    record_validation_phase(scenario, validate.run_heat_checks())

    replace_scenario_inventory(scenario, heat.database)
    scenario["cache"] = heat.cache
    scenario["index"] = heat.index
    scenario["heat diagnostics"] = heat.diagnostics

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["heat"] = heat.heat_techs

    return scenario


class Heat(BaseTransformation):
    """
    Class that modifies fuel inventories and markets
    in ecoinvent based on IAM output data.
    """

    def __init__(
        self,
        database: List[dict],
        iam_data: IAMDataCollection,
        model: str,
        pathway: str,
        year: int,
        version: str,
        system_model: str,
        cache: dict = None,
        index: dict = None,
    ):
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
        )

        self.carbon_intensity_markets = {}
        self.mapping = InventorySet(self.database)
        self.heat_metadata = load_heat_mapping(VARIABLES_DIR / "heat.yaml", self.model)
        self.heat_techs = self.mapping.generate_heat_map(model=self.model)
        self.biosphere_flows = get_biosphere_code(self.version)
        self.created_dataset_codes = set()
        self.frozen_suppliers = {}
        self.diagnostics = copy.deepcopy(getattr(self.iam_data, "heat_diagnostics", {}))

        fuel_mapping = InventorySet(
            self.database, version=self.version, model=self.model
        ).generate_fuel_map()
        self.fuel_map_reverse = self._build_fuel_map_reverse(fuel_mapping)
        self.regionalizable_technologies = {
            technology
            for technology, metadata in self.heat_metadata.items()
            if metadata.get("supplier_type")
            not in {"secondary_market", "frozen_legacy"}
        }

    def _build_fuel_map_reverse(self, fuel_mapping: dict) -> dict:
        """Index fuel suppliers by activity, product, and linked supplier name."""
        reverse = {}
        for fuel, datasets in fuel_mapping.items():
            for dataset in datasets:
                # Heat inventories generally link to the supplying activity name,
                # while a number of other premise transformations link by its
                # reference product. Index both forms.
                reverse[dataset["name"]] = fuel
                reverse[dataset["reference product"]] = fuel

        # Some ecoinvent exchanges use a location- or pathway-specific supplier
        # name which is not itself selected by the broad fuel mapping. Their
        # product is nevertheless mapped unambiguously (for example sewage-sludge
        # biomethane with product ``biomethane, high pressure``). Add these names
        # as aliases so efficiency calculations identify the fuel input.
        for dataset in self.database:
            for exchange in ws.technosphere(dataset):
                product = exchange.get("product")
                if product in reverse and exchange.get("name"):
                    reverse.setdefault(exchange["name"], reverse[product])
                    continue

                # Residential log boilers use ``cleft timber`` as their fuel
                # product. It is the same dry-mass wood carrier represented by
                # the wood-chip LHV in premise's fuel specifications, but is not
                # selected by the generic fuel activity mapping.
                fuel_label = f"{exchange.get('name', '')} {product or ''}".lower()
                if "cleft timber" in fuel_label:
                    reverse.setdefault(exchange["name"], "wood chips")
                elif "methanol" in fuel_label:
                    # All premise methanol pathways use the same 19.9 MJ/kg
                    # LHV; the pathway remains represented by the linked
                    # inventory activity and only its energy content is shared.
                    reverse.setdefault(exchange["name"], "methanol, from wood")

        return reverse

    def fetch_fuel_market_co2_emissions(self):
        """
        Fetch CO2 emissions from fuel markets.
        """
        fuel_markets = [
            "market for diesel, low-sulfur",
            "market for petrol, low-sulfur",
            "market for natural gas, high pressure",
        ]

        for dataset in ws.get_many(
            self.database,
            ws.either(*[ws.equals("name", n) for n in fuel_markets]),
        ):
            if "log parameters" in dataset:
                if "fossil CO2 per kg fuel" in dataset["log parameters"]:
                    self.carbon_intensity_markets[
                        (dataset["name"], dataset["location"])
                    ] = {"fossil": dataset["log parameters"]["fossil CO2 per kg fuel"]}
                if "non-fossil CO2 per kg fuel" in dataset["log parameters"]:
                    self.carbon_intensity_markets[
                        (dataset["name"], dataset["location"])
                    ].update(
                        {
                            "non-fossil": dataset["log parameters"][
                                "non-fossil CO2 per kg fuel"
                            ]
                        }
                    )

        # add "market for diesel" to self.carbon_intensity_markets
        # by duplicating the "market for low-sulfur" entries
        # add "market for natural gas, low pressure" to self.carbon_intensity_markets
        # by duplicating the "market for natural gas, high pressure" entries

        new_keys = {}
        for key, value in self.carbon_intensity_markets.items():
            if key[0] == "market for diesel, low-sulfur":
                new_keys[("market for diesel", key[1])] = value
                new_keys[("market group for diesel", key[1])] = value
                new_keys[("market group for diesel, low-sulfur", key[1])] = value
            if key[0] == "market for petrol, low-sulfur":
                new_keys[("market for petrol", key[1])] = value
                new_keys[("market for petrol, unleaded", key[1])] = value
            if key[0] == "market for natural gas, high pressure":
                new_keys[("market group for natural gas, high pressure", key[1])] = (
                    value
                )
                new_keys[("market for natural gas, low pressure", key[1])] = value

        self.carbon_intensity_markets.update(new_keys)

    def regionalize_activities(self):

        production_volumes_vars = [
            v
            for v in self.regionalizable_technologies
            if v in self.iam_data.production_volumes.coords["variables"].values
        ]

        production_volumes = None
        if production_volumes_vars:
            production_volumes = self.iam_data.production_volumes.sel(
                variables=production_volumes_vars
            )

        self.process_and_add_activities(
            mapping={
                technology: self.heat_techs[technology]
                for technology in self.regionalizable_technologies
            },
            production_volumes=production_volumes,
        )
        self.heat_techs = self.mapping.generate_heat_map(model=self.model)

    def adjust_carbon_dioxide_emissions(self):
        """
        Regionalize heat production.

        """

        for heat_tech, heat_datasets in self.heat_techs.items():
            if heat_tech not in self.regionalizable_technologies:
                continue
            for dataset in heat_datasets:
                fossil_co2, non_fossil_co2 = 0.0, 0.0
                for exc in ws.technosphere(dataset):
                    if (
                        exc["name"],
                        exc["location"],
                    ) in self.carbon_intensity_markets:
                        fossil_co2 += (
                            exc["amount"]
                            * self.carbon_intensity_markets[
                                (exc["name"], exc["location"])
                            ]["fossil"]
                        )

                        non_fossil_co2 += (
                            exc["amount"]
                            * self.carbon_intensity_markets[
                                (exc["name"], exc["location"])
                            ]["non-fossil"]
                        )

                if fossil_co2 + non_fossil_co2 > 0:

                    initial_fossil_co2 = sum(
                        [
                            exc["amount"]
                            for exc in ws.biosphere(dataset)
                            if exc["name"] == "Carbon dioxide, fossil"
                        ]
                    )
                    initial_non_fossil_co2 = sum(
                        [
                            exc["amount"]
                            for exc in ws.biosphere(dataset)
                            if exc["name"] == "Carbon dioxide, non-fossil"
                        ]
                    )

                    dataset["exchanges"] = [
                        e
                        for e in dataset["exchanges"]
                        if e["name"]
                        not in (
                            "Carbon dioxide, fossil",
                            "Carbon dioxide, non-fossil",
                        )
                    ]

                    if fossil_co2 > 0:
                        dataset["exchanges"].append(
                            {
                                "uncertainty type": 0,
                                "loc": float(fossil_co2),
                                "amount": float(fossil_co2),
                                "name": "Carbon dioxide, fossil",
                                "categories": ("air",),
                                "type": "biosphere",
                                "unit": "kilogram",
                                "input": (
                                    "biosphere3",
                                    self.biosphere_flows[
                                        (
                                            "Carbon dioxide, fossil",
                                            "air",
                                            "unspecified",
                                            "kilogram",
                                        )
                                    ],
                                ),
                            }
                        )

                    if non_fossil_co2 > 0:

                        dataset["exchanges"].append(
                            {
                                "uncertainty type": 0,
                                "loc": float(non_fossil_co2),
                                "amount": float(non_fossil_co2),
                                "name": "Carbon dioxide, non-fossil",
                                "categories": ("air",),
                                "type": "biosphere",
                                "unit": "kilogram",
                                "input": (
                                    "biosphere3",
                                    self.biosphere_flows[
                                        (
                                            "Carbon dioxide, non-fossil",
                                            "air",
                                            "unspecified",
                                            "kilogram",
                                        )
                                    ],
                                ),
                            }
                        )

                    dataset.setdefault("log parameters", {})[
                        "initial amount of fossil CO2"
                    ] = initial_fossil_co2
                    dataset["log parameters"]["new amount of fossil CO2"] = float(
                        fossil_co2
                    )
                    dataset["log parameters"][
                        "initial amount of biogenic CO2"
                    ] = initial_non_fossil_co2
                    dataset["log parameters"]["new amount of biogenic CO2"] = float(
                        non_fossil_co2
                    )

    @staticmethod
    def _has_positive_values(array: xr.DataArray | None) -> bool:
        return array is not None and bool((array.fillna(0) > 0).any())

    @staticmethod
    def _select_year(array: xr.DataArray, year: int) -> xr.DataArray:
        if year in array.coords["year"]:
            return array.sel(year=year)
        return array.interp(year=year)

    def _select_suppliers(self, technology: str, region: str) -> tuple[list, str]:
        activities = self.heat_techs.get(technology, [])
        suppliers = [ds for ds in activities if ds["location"] == region]
        fallback = "IAM region"
        if not suppliers:
            suppliers = [
                ds
                for ds in activities
                if ds["location"] in self.iam_to_ecoinvent_loc[region]
            ]
            fallback = "contained ecoinvent location"
        if not suppliers:
            suppliers = [ds for ds in activities if ds["location"] == "RoW"]
            fallback = "RoW"
        if not suppliers:
            suppliers = [ds for ds in activities if ds["location"] == "GLO"]
            fallback = "GLO"
        suppliers = self.deduplicate_market_suppliers(suppliers)
        if len(suppliers) > 1:
            weighted = get_shares_from_production_volume(suppliers)
            shares = {
                (
                    supplier["name"],
                    supplier["reference product"],
                    supplier["location"],
                    supplier["unit"],
                ): supplier["share"]
                for supplier in weighted
            }
            suppliers = [
                {
                    **supplier,
                    "share": shares[
                        (
                            supplier["name"],
                            supplier["reference product"],
                            supplier["location"],
                            supplier["unit"],
                        )
                    ],
                }
                for supplier in suppliers
            ]
        elif suppliers:
            suppliers = [{**suppliers[0], "share": 1.0}]
        return suppliers, fallback

    def _source_for_region(self, datasets: list, region: str) -> dict:
        for location_group in (
            [region],
            self.iam_to_ecoinvent_loc[region],
            ["RoW"],
            ["GLO"],
        ):
            candidates = [ds for ds in datasets if ds["location"] in location_group]
            if candidates:
                return candidates[0]
        raise ValueError(
            f"No legacy heat market could be frozen for IAM region {region!r}."
        )

    def create_frozen_legacy_suppliers(self, layer: str) -> list:
        """Copy legacy market contents before any heat-market relinking."""

        if layer in self.frozen_suppliers:
            return self.frozen_suppliers[layer]

        if layer == "buildings_end_use":
            source_name = (
                "market for heat, central or small-scale, other than natural gas"
            )
            source_product = "heat, central or small-scale, other than natural gas"
            product = "heat, central or small-scale"
            suffix = "buildings"
        else:
            source_name = (
                "market for heat, district or industrial, other than natural gas"
            )
            source_product = "heat, district or industrial, other than natural gas"
            product = "heat, district or industrial"
            suffix = "district or industrial"

        sources = list(
            ws.get_many(
                self.database,
                ws.equals("name", source_name),
                ws.equals("reference product", source_product),
            )
        )
        if not sources:
            raise ValueError(
                f"Cannot create a frozen legacy heat supplier: {source_name!r} "
                "is absent from the inventory."
            )

        frozen = []
        for region in [region for region in self.regions if region != "World"]:
            dataset = copy.deepcopy(self._source_for_region(sources, region))
            dataset["name"] = f"heat supply, frozen legacy mix, {suffix}"
            dataset["reference product"] = product
            dataset["location"] = region
            dataset["code"] = uuid.uuid4().hex
            dataset["database"] = ""
            dataset["regionalized"] = True
            dataset["heat frozen proxy"] = True
            dataset["comment"] = (
                dataset.get("comment", "")
                + " Frozen before premise heat-market relinking and used only as "
                "an explicit residual or supply-composition proxy."
            )
            for exchange in ws.production(dataset):
                exchange["name"] = dataset["name"]
                exchange["product"] = product
                exchange["location"] = region
                exchange["amount"] = 1.0
                exchange.pop("input", None)

            self.database.append(dataset)
            self.add_to_index(dataset)
            self.created_dataset_codes.add(dataset["code"])
            frozen.append(dataset)

        self.frozen_suppliers[layer] = frozen
        return frozen

    def _electric_conversion_factor(self, dataset: dict) -> float:
        electricity = sum(
            exchange["amount"] * 3.6
            for exchange in ws.technosphere(dataset)
            if exchange.get("amount", 0) > 0
            and exchange.get("unit") == "kilowatt hour"
            and "electricity"
            in f"{exchange.get('name', '')} {exchange.get('product', '')}".lower()
        )
        if electricity <= 0:
            raise ValueError(
                f"No positive electricity input found in heat supplier "
                f"{dataset['name']!r} ({dataset['location']})."
            )
        return 1.0 / electricity

    def _supplier_conversion_factor(self, dataset: dict, conversion: str) -> float:
        if conversion in {"electric_boiler", "heat_pump"}:
            factor = self._electric_conversion_factor(dataset)
        elif conversion == "combustion":
            factor = find_fuel_efficiency(
                dataset=dataset,
                energy_out=1.0,
                fuel_specs=self.fuels_specs,
                fuel_map_reverse=self.fuel_map_reverse,
            )
        elif conversion == "none":
            factor = 1.0
        else:
            raise ValueError(f"Unknown heat conversion type {conversion!r}.")

        bounds = {
            # LHV-based efficiencies can exceed one for condensing boilers. The
            # 1.2 ceiling admits the unmodified ecoinvent LPG inventory (~1.155)
            # while still rejecting allocation/mapping errors by a wide margin.
            "combustion": (0.0, 1.2, False),
            "electric_boiler": (0.95, 1.05, True),
            "heat_pump": (1.0, 6.0, False),
        }
        if conversion in bounds:
            lower, upper, inclusive_lower = bounds[conversion]
            lower_valid = factor >= lower if inclusive_lower else factor > lower
            if not lower_valid or factor > upper:
                raise ValueError(
                    f"Implausible {conversion} delivered-heat factor {factor:.4g} "
                    f"for {dataset['name']!r} ({dataset['location']})."
                )
        return factor

    def convert_to_delivered_heat(
        self, array: xr.DataArray, layer: str
    ) -> xr.DataArray:
        """Convert all final-energy time series to delivered heat before mixing."""

        delivered = array.copy(deep=True)
        factor_log = []
        for technology in array.coords["variables"].values.tolist():
            metadata = self.heat_metadata.get(
                technology,
                {"energy_basis": "heat_output", "conversion": "none"},
            )
            if metadata["energy_basis"] == "heat_output":
                continue
            conversion = metadata.get("conversion", "none")
            for region in [region for region in self.regions if region != "World"]:
                raw = array.sel(variables=technology, region=region).fillna(0)
                if float(raw.max().values) <= 0:
                    continue
                suppliers, fallback = self._select_suppliers(technology, region)
                if not suppliers:
                    raise ValueError(
                        f"Positive IAM heat volume for {technology!r} in {region!r}, "
                        "but no inventory supplier is available."
                    )
                factor = sum(
                    self._supplier_conversion_factor(supplier, conversion)
                    * supplier.get("share", 1.0)
                    for supplier in suppliers
                )
                delivered.loc[dict(variables=technology, region=region)] = raw * factor
                factor_log.append(
                    {
                        "technology": technology,
                        "region": region,
                        "factor": float(factor),
                        "conversion": conversion,
                        "suppliers": [
                            (supplier["name"], supplier["location"])
                            for supplier in suppliers
                        ],
                        "fallback": fallback,
                    }
                )

        self.diagnostics.setdefault(layer, {})["conversion factors"] = factor_log
        self._record_volume_diagnostics(array, delivered, layer)
        return delivered

    def _record_volume_diagnostics(
        self, raw: xr.DataArray, delivered: xr.DataArray, layer: str
    ) -> None:
        raw_year = self._select_year(raw, self.year)
        delivered_year = self._select_year(delivered, self.year)
        raw_totals = raw_year.sum(dim="variables")
        totals = delivered_year.sum(dim="variables")
        records = []
        residual = delivered.attrs.get("residual", raw.attrs.get("residual", {}))
        for technology in raw.coords["variables"].values.tolist():
            for region in [region for region in self.regions if region != "World"]:
                raw_value = float(
                    raw_year.sel(variables=technology, region=region).values
                )
                delivered_value = float(
                    delivered_year.sel(variables=technology, region=region).values
                )
                total = float(totals.sel(region=region).values)
                raw_total = float(raw_totals.sel(region=region).values)
                records.append(
                    {
                        "technology": technology,
                        "region": region,
                        "raw IAM volume": raw_value,
                        "conversion factor": (
                            delivered_value / raw_value if raw_value > 0 else 1.0
                        ),
                        "delivered heat volume": delivered_value,
                        "raw share": raw_value / raw_total if raw_total > 0 else 0,
                        "normalized share": delivered_value / total if total > 0 else 0,
                        "residual": bool(residual.get(technology, False)),
                    }
                )
        self.diagnostics.setdefault(layer, {})["volumes"] = records

    def _secondary_fallback_array(self) -> xr.DataArray | None:
        arrays = [
            array.sum(dim="variables")
            for array in (
                self.iam_data.buildings_heat_end_use,
                self.iam_data.industrial_heat_end_use,
            )
            if array is not None
        ]
        if not arrays:
            return None
        volume = arrays[0]
        for other in arrays[1:]:
            volume, other = xr.align(volume, other, join="outer", fill_value=0)
            volume = volume + other
        volume = volume.expand_dims(variables=["heat, secondary, frozen legacy mix"])
        volume.attrs = {
            "unit": {"heat, secondary, frozen legacy mix": "EJ/yr"},
            "energy_basis": {"heat, secondary, frozen legacy mix": "heat_output"},
            "conversion": {"heat, secondary, frozen legacy mix": "none"},
            "residual": {"heat, secondary, frozen legacy mix": False},
        }
        self.diagnostics.setdefault("secondary_supply", {})[
            "fallback"
        ] = "Frozen legacy district-heat composition; IAM supply detail unavailable."
        return volume

    def _mapping_for_layer(self, array: xr.DataArray, layer: str) -> dict:
        mapping = {}
        for technology in array.coords["variables"].values.tolist():
            if technology == "heat, secondary, frozen legacy mix":
                mapping[technology] = self.create_frozen_legacy_suppliers(
                    "secondary_supply"
                )
                self.heat_techs[technology] = mapping[technology]
                continue
            metadata = self.heat_metadata[technology]
            supplier_type = metadata.get("supplier_type")
            if supplier_type == "frozen_legacy":
                mapping[technology] = self.create_frozen_legacy_suppliers(layer)
            elif supplier_type == "secondary_market":
                mapping[technology] = list(
                    ws.get_many(
                        self.database,
                        ws.equals("name", SECONDARY_MARKET["name"]),
                        ws.equals(
                            "reference product",
                            SECONDARY_MARKET["reference product"],
                        ),
                    )
                )
            else:
                mapping[technology] = self.heat_techs.get(technology, [])
            self.heat_techs[technology] = mapping[technology]
        return mapping

    def create_heat_market(self, array: xr.DataArray, layer: str, market: dict) -> None:
        delivered = self.convert_to_delivered_heat(array, layer)
        market_volumes = delivered
        if self.system_model == "consequential":
            market_volumes = consequential_method(
                delivered,
                self.year,
                self.iam_data.system_model_args,
                f"heat {layer}",
            )

        before = {dataset.get("code") for dataset in self.database}
        self.process_and_add_markets(
            name=market["name"],
            reference_product=market["reference product"],
            unit="megajoule",
            mapping=self._mapping_for_layer(market_volumes, layer),
            production_volumes=market_volumes,
            system_model=self.system_model,
        )
        self.created_dataset_codes.update(
            dataset["code"]
            for dataset in self.database
            if dataset.get("code") not in before
        )

    def create_layered_heat_markets(self) -> None:
        secondary = self.iam_data.secondary_heat_supply
        if secondary is None and (
            self._has_positive_values(self.iam_data.buildings_heat_end_use)
            or self._has_positive_values(self.iam_data.industrial_heat_end_use)
        ):
            secondary = self._secondary_fallback_array()

        secondary_created = self._has_positive_values(secondary)
        if secondary_created:
            self.create_heat_market(secondary, "secondary_supply", SECONDARY_MARKET)
        else:
            print("No secondary heat supply scenario data available -- skipping")

        buildings_created = self._has_positive_values(
            self.iam_data.buildings_heat_end_use
        )
        if buildings_created:
            self.create_heat_market(
                self.iam_data.buildings_heat_end_use,
                "buildings_end_use",
                BUILDINGS_MARKET,
            )
            self.relink_heat_markets(BUILDING_LEGACY_INPUTS, BUILDINGS_MARKET)
        else:
            print("No buildings heat scenario data available -- skipping")

        industrial_created = self._has_positive_values(
            self.iam_data.industrial_heat_end_use
        )
        if industrial_created:
            self.create_heat_market(
                self.iam_data.industrial_heat_end_use,
                "industrial_end_use",
                INDUSTRIAL_MARKET,
            )
            self.relink_heat_markets(INDUSTRIAL_LEGACY_INPUTS, INDUSTRIAL_MARKET)
        elif secondary_created:
            # Supply-only IAMs (currently TIAM-UCL) update purchased/district
            # heat links but leave onsite building and industrial fuel use alone.
            self.relink_heat_markets(INDUSTRIAL_LEGACY_INPUTS, SECONDARY_MARKET)
        else:
            print("No industrial heat scenario data available -- skipping")

    def _target_location(self, dataset: dict, new_input: dict) -> str | None:
        region = (
            dataset["location"]
            if dataset["location"] in self.regions
            else self.ecoinvent_to_iam_loc.get(dataset["location"])
        )
        candidate = {
            "name": new_input["name"],
            "reference product": new_input["reference product"],
            "unit": "megajoule",
        }
        if region and self.is_in_index(candidate, region):
            return region
        if self.is_in_index(candidate, "World"):
            return "World"
        return None

    def relink_heat_markets(self, current_input: list, new_input: dict) -> None:
        """Rewrite legacy heat consumers, excluding every newly created dataset."""

        names = {item["name"] for item in current_input}
        products = {item["reference product"] for item in current_input}
        for dataset in self.database:
            if dataset.get("code") in self.created_dataset_codes:
                continue
            target_location = self._target_location(dataset, new_input)
            if target_location is None:
                continue
            changed = False
            for exchange in ws.technosphere(dataset):
                if (
                    exchange.get("name") not in names
                    or exchange.get("product") not in products
                ):
                    continue
                exchange["name"] = new_input["name"]
                exchange["product"] = new_input["reference product"]
                exchange["location"] = target_location
                exchange.pop("input", None)
                changed = True
            if changed:
                dataset["exchanges"] = self.summarize_market_exchanges(
                    dataset["exchanges"]
                )

    def assert_no_heat_cycles(self) -> None:
        """Reject direct or indirect links among generated heat datasets."""

        generated = [
            dataset
            for dataset in self.database
            if dataset.get("code") in self.created_dataset_codes
        ]
        keys = {
            (
                dataset["name"],
                dataset["reference product"],
                dataset["location"],
            ): dataset["code"]
            for dataset in generated
        }
        graph = {dataset["code"]: set() for dataset in generated}
        for dataset in generated:
            for exchange in ws.technosphere(dataset):
                target = keys.get(
                    (
                        exchange.get("name"),
                        exchange.get("product"),
                        exchange.get("location"),
                    )
                )
                if target:
                    graph[dataset["code"]].add(target)

        visiting, visited = set(), set()

        def visit(node):
            if node in visiting:
                raise ValueError(
                    "Circular dependency detected in generated heat markets."
                )
            if node in visited:
                return
            visiting.add(node)
            for child in graph[node]:
                visit(child)
            visiting.remove(node)
            visited.add(node)

        for node in graph:
            visit(node)

    def write_log(self, dataset, status="created"):
        """Record a structured heat provenance event."""

        record_change_event(self, dataset, status, sector="heat")
