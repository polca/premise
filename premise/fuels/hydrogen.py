import math
from collections import defaultdict

import pandas as pd

from ..filesystem_constants import VARIABLES_DIR
from ..transformation import np, uuid, ws
from .config import (
    HYDROGEN_CONSUMER_ROUTING,
    HYDROGEN_DISTRIBUTION_SHARES,
    HYDROGEN_SOURCES,
)
from .utils import adjust_electrolysis_electricity_requirement, fetch_mapping

hydrogen_parameters = fetch_mapping(HYDROGEN_SOURCES)
hydrogen_distribution_rules = fetch_mapping(HYDROGEN_DISTRIBUTION_SHARES)
hydrogen_consumer_routing = fetch_mapping(HYDROGEN_CONSUMER_ROUTING)

H2_LHV_GJ_PER_TONNE = 120.0
TONNES_H2_PER_EJ = 1e9 / H2_LHV_GJ_PER_TONNE
STEEL_PLANT_SIZE_MT_PER_YEAR = 0.5
CEMENT_PLANT_SIZE_MT_PER_YEAR = 2.4
CEMENT_CAPACITY_FACTOR = 0.55
CHEMICAL_PLANT_H2_USE_T_PER_YEAR = 60_300
OTHER_DEMAND_NODE_H2_USE_T_PER_YEAR = 1_000
OTHER_DEMAND_NODE_LOAD_DAYS_PER_YEAR = 333
PASSENGER_CAR_AVERAGE_DISTANCE_KM_PER_YEAR = 10_900
FREIGHT_VEHICLE_AVERAGE_DISTANCE_KM_PER_YEAR = 23_900
PASSENGER_CARS_PER_STATION_PER_DAY = 1_500
FREIGHT_VEHICLES_PER_STATION_PER_DAY = 400
BILLION_KM_TO_KM = 1_000_000_000
VEHICLE_OCCUPANCY = 1.5
FREIGHT_LOAD = 15
HYDROGEN_END_USE_MARKETS = {
    sector: rules["market"]
    for sector, rules in hydrogen_consumer_routing["sectors"].items()
}
HYDROGEN_MARKET = "market for hydrogen, gaseous, low pressure"
HYDROGEN_PRODUCT = "hydrogen, gaseous, low pressure"
HYDROGEN_DISTRIBUTION_MODES = sorted(
    {
        mode
        for rule in hydrogen_distribution_rules.get("rules", [])
        for mode in rule.get("shares", {})
    }
)
HYDROGEN_TRANSPORT_ACTIVITIES = {
    "compressed_gaseous_truck": {
        "name": ("transport, hydrogen, gaseous, lorry, " "unspecified"),
        "reference product": (
            "transport, hydrogen, gaseous, lorry, " "unspecified"
        ),
        "unit": "ton kilometer",
    },
    "liquid_hydrogen_truck": {
        "name": ("transport, hydrogen, liquid, lorry, " "unspecified"),
        "reference product": (
            "transport, hydrogen, liquid, lorry, " "unspecified"
        ),
        "unit": "ton kilometer",
    },
    "compressed_gaseous_pipeline": {
        "name": "hydrogen supply, distributed by pipeline",
        "reference product": "hydrogen, gaseous, from pipeline",
        "unit": "kilogram",
    },
    "liquid_ammonia_ship": {
        "name": (
            "transport, freight, sea, tanker for liquefied ammonia, "
            "ammonia and mgo"
        ),
        "reference product": (
            "transport, freight, sea, tanker for liquefied ammonia, "
            "ammonia and mgo"
        ),
        "unit": "ton kilometer",
    },
    "liquid_hydrogen_ship": {
        "name": (
            "transport, freight, sea, tanker for liquefied hydrogen, "
            "heavy fuel oil"
        ),
        "reference product": (
            "transport, freight, sea, tanker for liquefied hydrogen, "
            "heavy fuel oil"
        ),
        "unit": "ton kilometer",
    },
}
HYDROGEN_TRANSPORT_DISTANCES_KM = {
    "compressed_gaseous_truck": 50,
    "liquid_hydrogen_truck": 100,
    "liquid_ammonia_ship": 2500,
    "liquid_hydrogen_ship": 2500,
}
HYDROGEN_PIPELINE_GENERAL_MARKET_AMOUNT = 1
KG_TO_TONNE = 0.001


class HydrogenMixin:
    # Workflow entry point: build regional hydrogen production, markets, and support datasets.
    def generate_hydrogen_activities(self):

        self._regionalize_hydrogen_activities()
        self._generate_supporting_hydrogen_datasets()

    # Consumer relinking workflow: build searchable text from a hydrogen-consuming dataset.
    @staticmethod
    def _hydrogen_consumer_text(dataset):
        values = [
            dataset.get("name", ""),
            dataset.get("reference product", ""),
            dataset.get("unit", ""),
        ]
        return " | ".join(values).lower()

    # Consumer relinking workflow: identify generic hydrogen market exchanges to redirect.
    @staticmethod
    def _is_plain_hydrogen_market_exchange(exchange):
        return (
            exchange.get("type") == "technosphere"
            and exchange.get("name") == HYDROGEN_MARKET
            and exchange.get("product") == HYDROGEN_PRODUCT
        )

    # Consumer relinking workflow: skip hydrogen suppliers so only consumers are relinked.
    @staticmethod
    def _is_hydrogen_supplier_dataset(dataset):
        return (
            dataset.get("reference product") == HYDROGEN_PRODUCT
            or dataset.get("name") == HYDROGEN_MARKET
            or dataset.get("name") in HYDROGEN_END_USE_MARKETS.values()
        )

    # Consumer relinking workflow: check whether a consumer should stay on the generic market.
    def _keep_general_hydrogen_market(self, dataset):
        rules = hydrogen_consumer_routing.get("keep_general_market", {})
        text = self._hydrogen_consumer_text(dataset)
        if any(keyword in text for keyword in rules.get("name_contains", [])):
            return True

        return any(
            self._isic_matches_rule(code, rules)
            for code in self._hydrogen_consumer_isic_codes(dataset)
        )

    # Consumer relinking workflow: extract ISIC rev.4 codes used for sector classification.
    @staticmethod
    def _hydrogen_consumer_isic_codes(dataset):
        codes = []
        for classification in dataset.get("classifications", []):
            if not isinstance(classification, (list, tuple)):
                continue
            if len(classification) < 2:
                continue
            system, value = classification[0], classification[1]
            if system != "ISIC rev.4 ecoinvent":
                continue
            code = str(value).split(":", 1)[0].strip()
            if code:
                codes.append(code)
        return codes

    # Consumer relinking workflow: evaluate exact, prefix, and exclusion-based ISIC rules.
    @staticmethod
    def _isic_matches_rule(code, rule):
        if code in {str(value) for value in rule.get("isic_exact", [])}:
            return True

        if any(
            code.startswith(str(prefix))
            for prefix in rule.get("isic_prefix", [])
        ):
            return True

        for prefix, excluded_codes in rule.get(
            "isic_prefix_excluding_exact", {}
        ).items():
            if code.startswith(str(prefix)) and code not in {
                str(value) for value in excluded_codes
            }:
                return True

        for prefix, excluded_prefixes in rule.get(
            "isic_prefix_excluding_prefix", {}
        ).items():
            if code.startswith(str(prefix)) and not any(
                code.startswith(str(excluded_prefix))
                for excluded_prefix in excluded_prefixes
            ):
                return True

        return False

    # Consumer relinking workflow: match a dataset to sector rules by name/product/unit text.
    def _hydrogen_sector_name_matches_consumer(self, dataset, sector_rules):
        text = self._hydrogen_consumer_text(dataset)
        return any(
            keyword in text
            for keyword in sector_rules.get("name_contains", [])
        )

    # Consumer relinking workflow: match a dataset to sector rules by ISIC classification.
    def _hydrogen_sector_isic_matches_consumer(self, dataset, sector_rules):
        return any(
            self._isic_matches_rule(code, sector_rules)
            for code in self._hydrogen_consumer_isic_codes(dataset)
        )

    # Consumer relinking workflow: assign one hydrogen end-use sector or return ambiguity.
    def _classify_hydrogen_consumer_sector(self, dataset):
        sector_rules = hydrogen_consumer_routing.get("sectors", {})
        matches = [
            sector
            for sector, rules in sector_rules.items()
            if self._hydrogen_sector_name_matches_consumer(dataset, rules)
        ]
        if len(matches) == 1:
            return matches[0], matches
        if len(matches) > 1:
            return None, matches

        matches = [
            sector
            for sector, rules in sector_rules.items()
            if self._hydrogen_sector_isic_matches_consumer(dataset, rules)
        ]
        if len(matches) == 1:
            return matches[0], matches

        return None, matches

    # Consumer relinking workflow: format unresolved or skipped relinking cases for logs.
    @staticmethod
    def _hydrogen_consumer_warning(dataset, exchange, matches):
        return {
            "name": dataset.get("name"),
            "reference product": dataset.get("reference product"),
            "location": dataset.get("location"),
            "hydrogen exchange location": exchange.get("location"),
            "hydrogen exchange amount": exchange.get("amount"),
            "candidate sectors": matches,
        }

    # Consumer relinking workflow: format successful relinking cases for traceability.
    @staticmethod
    def _matched_hydrogen_consumer_record(
        dataset, exchange, sector, new_market
    ):
        return {
            "name": dataset.get("name"),
            "reference product": dataset.get("reference product"),
            "location": dataset.get("location"),
            "hydrogen exchange location": exchange.get("location"),
            "hydrogen exchange amount": exchange.get("amount"),
            "sector": sector,
            "old generic hydrogen market": HYDROGEN_MARKET,
            "new sector specific hydrogen market": new_market,
        }

    # Sector-market workflow: find IAM regions with positive hydrogen demand by market type.
    def _available_hydrogen_sector_market_regions(self):
        final_energy = self._get_hydrogen_final_energy_by_subsector(
            year=self.year
        )
        if final_energy.empty:
            return {
                market_key: set() for market_key in HYDROGEN_END_USE_MARKETS
            }

        required_columns = {"region", "sector", "subsector"}
        if not required_columns.issubset(final_energy.columns):
            return {
                market_key: set() for market_key in HYDROGEN_END_USE_MARKETS
            }

        candidate_regions = {
            region
            for region in getattr(
                self, "regions", final_energy["region"].dropna().unique()
            )
            if region != "World"
        }
        available_markets = {}
        for market_key in HYDROGEN_END_USE_MARKETS:
            rows = final_energy.loc[
                self._hydrogen_sector_market_rows(final_energy, market_key)
            ]
            if rows.empty:
                available_markets[market_key] = set()
                continue

            available_markets[market_key] = {
                region
                for region in rows["region"].dropna().unique()
                if region in candidate_regions
            }

        return available_markets

    # Sector-market workflow: list sector markets that have at least one eligible region.
    def _available_hydrogen_sector_market_keys(self):
        return {
            market_key
            for market_key, regions in (
                self._available_hydrogen_sector_market_regions().items()
            )
            if regions
        }

    # Sector-market workflow: check whether a sector market exists for an exchange location.
    def _hydrogen_sector_market_is_available(self, sector, location=None):
        regions = self._available_hydrogen_sector_market_regions().get(
            sector, set()
        )
        if not regions:
            return False

        if location is None:
            return True

        if location in regions:
            return True

        try:
            iam_location = self.geo.ecoinvent_to_iam_location(location)
        except (AttributeError, ValueError):
            iam_location = location

        return iam_location in regions

    # Consumer relinking workflow: redirect generic hydrogen inputs to available sector markets.
    def relink_hydrogen_consumers_to_sector_markets(self):
        """
        Redirect plain hydrogen market inputs to sector-specific hydrogen markets.

        Consumers that cannot be classified unambiguously are kept linked to the
        plain hydrogen market and recorded in
        ``self.unmatched_hydrogen_consumers`` for later inspection.
        """

        self.unmatched_hydrogen_consumers = []
        self.skipped_hydrogen_consumers = []
        self.matched_hydrogen_consumers = []
        relinked = 0

        for dataset in self.database:
            if self._is_hydrogen_supplier_dataset(dataset):
                continue

            hydrogen_exchanges = [
                exchange
                for exchange in dataset.get("exchanges", [])
                if self._is_plain_hydrogen_market_exchange(exchange)
            ]
            if not hydrogen_exchanges:
                continue

            if self._keep_general_hydrogen_market(dataset):
                self.skipped_hydrogen_consumers.extend(
                    self._hydrogen_consumer_warning(dataset, exchange, [])
                    for exchange in hydrogen_exchanges
                )
                continue

            sector, matches = self._classify_hydrogen_consumer_sector(dataset)
            if sector is None:
                self.unmatched_hydrogen_consumers.extend(
                    self._hydrogen_consumer_warning(dataset, exchange, matches)
                    for exchange in hydrogen_exchanges
                )
                continue

            for exchange in hydrogen_exchanges:
                if not self._hydrogen_sector_market_is_available(
                    sector, exchange.get("location", dataset.get("location"))
                ):
                    self.skipped_hydrogen_consumers.append(
                        self._hydrogen_consumer_warning(
                            dataset, exchange, [sector]
                        )
                    )
                    continue

                new_market = HYDROGEN_END_USE_MARKETS[sector]
                self.matched_hydrogen_consumers.append(
                    self._matched_hydrogen_consumer_record(
                        dataset, exchange, sector, new_market
                    )
                )
                exchange["name"] = new_market
                relinked += 1

            if any(
                exchange.get("name") == HYDROGEN_END_USE_MARKETS[sector]
                for exchange in hydrogen_exchanges
            ):
                dataset.setdefault("log parameters", {})[
                    "hydrogen market sector"
                ] = sector

        if self.unmatched_hydrogen_consumers:
            print(
                "Could not classify "
                f"{len(self.unmatched_hydrogen_consumers)} hydrogen-consuming "
                "exchange(s) for sector-specific hydrogen market relinking."
            )

        if self.skipped_hydrogen_consumers:
            print(
                "Kept "
                f"{len(self.skipped_hydrogen_consumers)} hydrogen-consuming "
                "exchange(s) on the general hydrogen market because they are "
                "not included in sector-specific hydrogen market relinking or "
                "because the target sector has no positive hydrogen demand."
            )

        return relinked

    # Demand-node workflow: normalize YAML scalar/list values into a list.
    @staticmethod
    def _as_list(value):
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    # Demand-node workflow: map IAM final-energy variables to hydrogen end-use groups.
    @staticmethod
    def _hydrogen_end_use_group(variable):
        parts = str(variable).split(" - ")
        top_level = parts[0]

        if top_level == "Buildings":
            return pd.Series(
                {
                    "sector": "Residential and commercial buildings",
                    "subsector": parts[1] if len(parts) > 2 else "Buildings",
                }
            )

        if top_level == "Transport" or str(variable).startswith(
            "Transportation"
        ):
            return pd.Series({"sector": "Transport", "subsector": "Transport"})

        if top_level == "Industry":
            if len(parts) >= 3 and parts[1] == "Steel":
                subsector = "Steel"
            elif len(parts) >= 3 and parts[1] == "Chemicals":
                subsector = "Chemicals"
            elif len(parts) >= 2:
                subsector = parts[1]
            else:
                subsector = "Industry"
            return pd.Series(
                {"sector": "Industrial processes", "subsector": subsector}
            )

        if top_level == "CDR":
            return pd.Series(
                {
                    "sector": "Industrial processes",
                    "subsector": "Carbon dioxide removal",
                }
            )

        return pd.Series({"sector": "Other", "subsector": top_level})

    # Demand-node workflow: identify direct hydrogen final-energy IAM variables.
    @staticmethod
    def _is_direct_hydrogen_end_use(variable):
        variable = str(variable)
        return variable.endswith(" - H2") or variable.endswith(" - Hydrogen")

    # Demand-node workflow: detect hydrogen vehicle mappings in transport configuration files.
    @staticmethod
    def _is_hydrogen_vehicle_mapping(vehicle_name, mapping):
        fuel_aliases = mapping.get("ecoinvent_fuel_aliases", {}) or {}
        fuel_filters = " ".join(
            HydrogenMixin._as_list(fuel_aliases.get("fltr"))
        ).lower()
        if HydrogenMixin._contains_other_transport_fuel(
            f"{vehicle_name} {fuel_filters}"
        ):
            return False

        variable_text = " ".join(
            [
                str(vehicle_name),
                str(mapping.get("iam_aliases", {})),
                str(mapping.get("energy_use_aliases", {})),
                fuel_filters,
            ]
        ).lower()
        return any(
            token in variable_text
            for token in ["hydrogen", "fcev", "fuel cell"]
        )

    # Demand-node workflow: reject transport variables that refer to non-hydrogen fuels.
    @staticmethod
    def _contains_other_transport_fuel(value):
        value = str(value).lower()
        other_fuels = [
            "biofuel",
            "biofuels",
            "biomass",
            "biodiesel",
            "diesel",
            "ethanol",
            "fossil",
            "gasoline",
            "gases",
            "hybrid liquids",
            "liquids",
            "methanol",
            "natural gas",
            "|ng",
            "oil",
            "petrol",
        ]
        return any(fuel in value for fuel in other_fuels)

    # Demand-node workflow: keep only direct hydrogen transport service variables.
    @staticmethod
    def _is_direct_hydrogen_transport_service_variable(variable):
        variable = str(variable).lower()
        is_hydrogen = any(
            token in variable
            for token in [
                "hydrogen",
                "fcev",
                "fuel-cell-electric",
                "fuel cell electric",
                "fuel cell vehicle",
            ]
        )
        return is_hydrogen and not HydrogenMixin._contains_other_transport_fuel(
            variable
        )

    # Demand-node workflow: round positive node counts up while preserving empty/zero demand.
    @staticmethod
    def _ceil_positive(value):
        if pd.isna(value) or value <= 0:
            return 0
        return math.ceil(value)

    # Demand-node workflow: create the canonical empty logistics output table.
    def _empty_hydrogen_demand_nodes(self):
        return pd.DataFrame(
            columns=[
                "model",
                "pathway",
                "year",
                "region",
                "sector",
                "subsector",
                "hydrogen_final_energy_ej_per_year",
                "hydrogen_demand_t_per_year",
                "hydrogen_demand_t_per_day",
                "demand_nodes",
                "demand_nodes_rounded_up",
                "demand_node_type",
                "hydrogen_demand_t_per_node_per_year",
                "hydrogen_demand_t_per_node_per_day",
                *HYDROGEN_DISTRIBUTION_MODES,
                "availability_days_per_year",
                "activity_proxy_value",
                "activity_proxy_unit",
                "calculation_method",
                "source_variables",
                "validation_status",
                "validation_relative_difference",
            ]
        )

    # Demand-node workflow: aggregate IAM hydrogen final energy into region/sector demand rows.
    def _get_hydrogen_final_energy_by_subsector(self, year=None):
        final_energy = self.iam_data.production_volumes
        variables = [
            str(variable)
            for variable in final_energy.coords["variables"].values
            if self._is_direct_hydrogen_end_use(variable)
        ]

        if not variables:
            return self._empty_hydrogen_demand_nodes()

        final_energy = final_energy.sel(variables=variables)
        if year is not None and "year" in final_energy.coords:
            target_year = year
            if target_year < final_energy.year.values.min():
                target_year = final_energy.year.values.min()
            if target_year > final_energy.year.values.max():
                target_year = final_energy.year.values.max()

            if target_year in final_energy.year.values:
                final_energy = final_energy.sel(year=[target_year])
            else:
                final_energy = final_energy.interp(year=[target_year])

        table = final_energy.to_dataframe(
            name="hydrogen_final_energy_ej_per_year"
        ).reset_index()
        table = table.loc[table["hydrogen_final_energy_ej_per_year"] > 0]

        if table.empty:
            return self._empty_hydrogen_demand_nodes()

        table = table.join(
            table["variables"].apply(self._hydrogen_end_use_group)
        )
        table = (
            table.groupby(
                ["year", "region", "sector", "subsector"],
                as_index=False,
            )
            .agg(
                hydrogen_final_energy_ej_per_year=(
                    "hydrogen_final_energy_ej_per_year",
                    "sum",
                ),
                source_variables=(
                    "variables",
                    lambda values: "; ".join(sorted(set(map(str, values)))),
                ),
            )
            .assign(
                model=self.model,
                pathway=self.scenario,
                hydrogen_demand_t_per_year=lambda df: df[
                    "hydrogen_final_energy_ej_per_year"
                ]
                * TONNES_H2_PER_EJ,
                hydrogen_demand_t_per_day=lambda df: df[
                    "hydrogen_final_energy_ej_per_year"
                ]
                * TONNES_H2_PER_EJ
                / 365,
            )
        )

        return table[
            [
                "model",
                "pathway",
                "year",
                "region",
                "sector",
                "subsector",
                "hydrogen_final_energy_ej_per_year",
                "hydrogen_demand_t_per_year",
                "hydrogen_demand_t_per_day",
                "source_variables",
            ]
        ]

    # Demand-node workflow: collect IAM production volumes for one or more proxy variables.
    def _get_production_volume_table(self, variables, value_column):
        production_volumes = self.iam_data.production_volumes
        present_variables = [
            variable
            for variable in variables
            if variable in production_volumes.coords["variables"].values
        ]

        if not present_variables:
            return pd.DataFrame(columns=["region", "year", value_column])

        return (
            production_volumes.sel(variables=present_variables)
            .sum(dim="variables")
            .to_series()
            .rename(value_column)
            .reset_index()
        )

    # Demand-node workflow: estimate steel plant counts from H-DRI production volumes.
    def _add_steel_demand_nodes(self, demand):
        steel_production = self._get_production_volume_table(
            ["steel - primary - H-DRI"],
            "activity_proxy_value",
        )
        if steel_production.empty:
            return demand

        mask = demand["subsector"] == "Steel"
        demand.loc[mask, "demand_node_type"] = "steel_plants"
        demand.loc[mask, "activity_proxy_unit"] = "Mt steel/yr"
        demand.loc[mask, "availability_days_per_year"] = 333

        demand = demand.merge(
            steel_production,
            on=["region", "year"],
            how="left",
            suffixes=("", "_steel"),
        )
        steel_mask = demand["subsector"] == "Steel"
        demand.loc[steel_mask, "activity_proxy_value"] = demand.loc[
            steel_mask, "activity_proxy_value_steel"
        ]
        demand = demand.drop(columns=["activity_proxy_value_steel"])
        demand.loc[steel_mask, "demand_nodes"] = (
            demand.loc[steel_mask, "activity_proxy_value"]
            / STEEL_PLANT_SIZE_MT_PER_YEAR
        )
        return demand

    # Demand-node workflow: estimate cement plant counts from cement production volumes.
    def _add_cement_demand_nodes(self, demand):
        production_volumes = self.iam_data.production_volumes
        cement_variables = [
            str(variable)
            for variable in production_volumes.coords["variables"].values
            if str(variable).startswith("cement,")
        ]
        cement_production = self._get_production_volume_table(
            cement_variables,
            "activity_proxy_value",
        )
        if cement_production.empty:
            return demand

        mask = demand["subsector"] == "Cement"
        demand.loc[mask, "demand_node_type"] = "cement_plants"
        demand.loc[mask, "activity_proxy_unit"] = "Mt cement/yr"
        demand.loc[mask, "availability_days_per_year"] = (
            365 * CEMENT_CAPACITY_FACTOR
        )

        demand = demand.merge(
            cement_production,
            on=["region", "year"],
            how="left",
            suffixes=("", "_cement"),
        )
        cement_mask = demand["subsector"] == "Cement"
        demand.loc[cement_mask, "activity_proxy_value"] = demand.loc[
            cement_mask, "activity_proxy_value_cement"
        ]
        demand = demand.drop(columns=["activity_proxy_value_cement"])
        demand.loc[cement_mask, "demand_nodes"] = demand.loc[
            cement_mask, "activity_proxy_value"
        ] / (CEMENT_PLANT_SIZE_MT_PER_YEAR * CEMENT_CAPACITY_FACTOR)
        return demand

    # Demand-node workflow: estimate chemicals and other industrial nodes from final energy.
    def _add_final_energy_based_demand_nodes(self, demand):
        chemical_mask = demand["subsector"] == "Chemicals"
        demand.loc[chemical_mask, "demand_node_type"] = "chemical_plants"
        demand.loc[chemical_mask, "availability_days_per_year"] = 365
        demand.loc[chemical_mask, "demand_nodes"] = (
            demand.loc[chemical_mask, "hydrogen_demand_t_per_year"]
            / CHEMICAL_PLANT_H2_USE_T_PER_YEAR
        )

        fixed_node_subsectors = {"Steel", "Cement", "Chemicals"}
        other_mask = (demand["sector"] == "Industrial processes") & (
            ~demand["subsector"].isin(fixed_node_subsectors)
        )
        demand.loc[other_mask, "demand_node_type"] = "other_demand_nodes"
        demand.loc[other_mask, "availability_days_per_year"] = (
            OTHER_DEMAND_NODE_LOAD_DAYS_PER_YEAR
        )
        demand.loc[other_mask, "demand_nodes"] = (
            demand.loc[other_mask, "hydrogen_demand_t_per_year"]
            / OTHER_DEMAND_NODE_H2_USE_T_PER_YEAR
        )

        return demand

    # Demand-node workflow: derive transport fueling station counts from service demand.
    def _get_transport_fueling_stations(self):
        transport_mapping_files = {
            "passenger_car": VARIABLES_DIR / "transport_passenger_cars.yaml",
            "road_freight": VARIABLES_DIR / "transport_road_freight.yaml",
        }
        assumptions = {
            "passenger_car": {
                "average_distance_km_per_year": (
                    PASSENGER_CAR_AVERAGE_DISTANCE_KM_PER_YEAR
                ),
                "vehicles_per_station": PASSENGER_CARS_PER_STATION_PER_DAY,
                "service_divisor": VEHICLE_OCCUPANCY,
            },
            "road_freight": {
                "average_distance_km_per_year": (
                    FREIGHT_VEHICLE_AVERAGE_DISTANCE_KM_PER_YEAR
                ),
                "vehicles_per_station": FREIGHT_VEHICLES_PER_STATION_PER_DAY,
                "service_divisor": FREIGHT_LOAD,
            },
        }

        rows = []
        for vehicle_class, mapping_path in transport_mapping_files.items():
            transport_mapping = fetch_mapping(mapping_path)
            vehicle_assumptions = assumptions[vehicle_class]

            for vehicle_type, mapping in transport_mapping.items():
                if not self._is_hydrogen_vehicle_mapping(
                    vehicle_type, mapping
                ):
                    continue

                service_variables = self._as_list(
                    (mapping.get("iam_aliases", {}) or {}).get(self.model)
                )
                available_variables = [
                    variable
                    for variable in service_variables
                    if variable
                    in self.iam_data.data.coords["variables"].values
                    and self._is_direct_hydrogen_transport_service_variable(
                        variable
                    )
                ]
                if not available_variables:
                    continue

                service = (
                    self.iam_data.data.sel(variables=available_variables)
                    .sum(dim="variables")
                    .to_series()
                    .rename("transport_service_billion_km_per_year")
                    .reset_index()
                )
                service["vehicle_class"] = vehicle_class
                service["vehicle_type"] = vehicle_type
                service["transport_service_variables"] = "; ".join(
                    available_variables
                )
                service["hydrogen_transport_service_km_per_year"] = (
                    service["transport_service_billion_km_per_year"]
                    * BILLION_KM_TO_KM
                    / vehicle_assumptions["service_divisor"]
                )
                service["transport_vehicle_count"] = (
                    service["hydrogen_transport_service_km_per_year"]
                    / vehicle_assumptions["average_distance_km_per_year"]
                )
                service["demand_nodes"] = (
                    service["transport_vehicle_count"]
                    / vehicle_assumptions["vehicles_per_station"]
                )
                rows.append(service)

        if not rows:
            return pd.DataFrame(
                columns=[
                    "region",
                    "year",
                    "demand_nodes",
                    "activity_proxy_value",
                    "source_variables",
                ]
            )

        fueling_stations_by_region_and_vehicle_class = (
            pd.concat(rows, ignore_index=True)
            .groupby(["region", "year", "vehicle_class"], as_index=False)
            .agg(
                demand_nodes=("demand_nodes", "sum"),
                activity_proxy_value=("transport_vehicle_count", "sum"),
                source_variables=(
                    "transport_service_variables",
                    lambda values: "; ".join(sorted(set(values))),
                ),
            )
        )

        return (
            fueling_stations_by_region_and_vehicle_class.groupby(
                ["region", "year"], as_index=False
            )
            .agg(
                demand_nodes=("demand_nodes", "sum"),
                activity_proxy_value=("activity_proxy_value", "sum"),
                source_variables=(
                    "source_variables",
                    lambda values: "; ".join(sorted(set(values))),
                ),
            )
        )

    # Demand-node workflow: merge transport fueling station estimates into demand rows.
    def _add_transport_demand_nodes(self, demand):
        transport_nodes = self._get_transport_fueling_stations()
        if transport_nodes.empty:
            return demand

        demand = demand.merge(
            transport_nodes,
            on=["region", "year"],
            how="left",
            suffixes=("", "_transport"),
        )
        mask = demand["sector"] == "Transport"
        demand.loc[mask, "demand_node_type"] = "fueling_stations"
        demand.loc[mask, "activity_proxy_unit"] = "vehicles"
        demand.loc[mask, "availability_days_per_year"] = 365
        demand.loc[mask, "activity_proxy_value"] = demand.loc[
            mask, "activity_proxy_value_transport"
        ]
        demand.loc[mask, "demand_nodes"] = demand.loc[
            mask, "demand_nodes_transport"
        ]
        demand.loc[mask, "source_variables"] = (
            demand.loc[mask, "source_variables"].fillna("")
            + "; "
            + demand.loc[mask, "source_variables_transport"].fillna("")
        ).str.strip("; ")

        return demand.drop(
            columns=[
                "demand_nodes_transport",
                "activity_proxy_value_transport",
                "source_variables_transport",
            ]
        )

    # Demand-node workflow: compare regional hydrogen demand totals against World totals.
    def _validate_hydrogen_demand_nodes(self, demand):
        if demand.empty:
            return demand

        demand = demand.copy()
        world_labels = {"World", "WORLD"}
        regional_totals = (
            demand.loc[~demand["region"].isin(world_labels)]
            .groupby("year")["hydrogen_final_energy_ej_per_year"]
            .sum()
        )
        world_totals = (
            demand.loc[demand["region"].isin(world_labels)]
            .groupby("year")["hydrogen_final_energy_ej_per_year"]
            .sum()
        )

        relative_differences = {}
        for year, world_total in world_totals.items():
            regional_total = regional_totals.get(year, np.nan)
            if pd.isna(regional_total) or world_total == 0:
                relative_differences[year] = np.nan
            else:
                relative_differences[year] = (
                    world_total - regional_total
                ) / world_total

        demand["validation_relative_difference"] = demand["year"].map(
            relative_differences
        )
        demand["validation_status"] = "not_checked"
        checked = demand["validation_relative_difference"].notna()
        demand.loc[checked, "validation_status"] = np.where(
            demand.loc[checked, "validation_relative_difference"].abs()
            <= 1e-6,
            "ok",
            "world_sum_mismatch",
        )
        return demand

    # Logistics decision-tree workflow: match row attributes against a routing rule.
    @staticmethod
    def _distribution_rule_matches_row(row, match):
        for column, expected_value in match.items():
            if column not in row or pd.isna(row[column]):
                return False
            if row[column] != expected_value:
                return False
        return True

    # Logistics decision-tree workflow: evaluate numeric thresholds for a routing rule.
    @staticmethod
    def _distribution_condition_matches(value, condition):
        if not condition:
            return True
        if pd.isna(value):
            return False
        if "min" in condition and value < condition["min"]:
            return False
        if "max" in condition and value > condition["max"]:
            return False
        return True

    # Logistics decision-tree workflow: select the highest-priority distribution rule.
    def _select_hydrogen_distribution_rule(self, row):
        rules = sorted(
            hydrogen_distribution_rules.get("rules", []),
            key=lambda rule: rule.get("priority", 1000),
        )

        for rule in rules:
            if not self._distribution_rule_matches_row(
                row, rule.get("match", {})
            ):
                continue

            basis = rule.get("basis")
            if not basis:
                continue

            if self._distribution_condition_matches(
                row.get(basis, np.nan), rule.get("condition", {})
            ):
                return rule

        return None

    # Logistics decision-tree workflow: add distribution-mode shares to each demand row.
    def _add_hydrogen_distribution_shares(self, demand):
        demand = demand.copy()

        for mode in HYDROGEN_DISTRIBUTION_MODES:
            demand[mode] = 0.0

        for index, row in demand.iterrows():
            rule = self._select_hydrogen_distribution_rule(row)
            if rule is None:
                continue

            for mode, share in rule.get("shares", {}).items():
                demand.loc[index, mode] = share

        return demand

    # Demand-node workflow: build the full hydrogen logistics analysis output table.
    def set_hydrogen_logistics(self):
        demand = self._get_hydrogen_final_energy_by_subsector()

        if demand.empty:
            self.hydrogen_demand_nodes = demand
            return

        demand["demand_nodes"] = np.nan
        demand["demand_node_type"] = pd.NA
        demand["activity_proxy_value"] = np.nan
        demand["activity_proxy_unit"] = pd.NA
        demand["availability_days_per_year"] = np.nan
        demand["calculation_method"] = "hydrogen_final_energy"

        demand = self._add_steel_demand_nodes(demand)
        demand = self._add_cement_demand_nodes(demand)
        demand = self._add_final_energy_based_demand_nodes(demand)
        demand = self._add_transport_demand_nodes(demand)

        demand["demand_nodes_rounded_up"] = demand["demand_nodes"].apply(
            self._ceil_positive
        )
        demand["hydrogen_demand_t_per_node_per_year"] = (
            demand["hydrogen_demand_t_per_year"] / demand["demand_nodes"]
        )
        demand["hydrogen_demand_t_per_node_per_day"] = (
            demand["hydrogen_demand_t_per_year"]
            / demand["demand_nodes"]
            / demand["availability_days_per_year"]
        )

        demand.loc[
            demand["demand_node_type"] == "steel_plants",
            "calculation_method",
        ] = "production_volume_based"
        demand.loc[
            demand["demand_node_type"] == "cement_plants",
            "calculation_method",
        ] = "production_volume_based"
        demand.loc[
            demand["demand_node_type"].isin(
                ["chemical_plants", "other_demand_nodes"]
            ),
            "calculation_method",
        ] = "final_energy_based"
        demand.loc[
            demand["demand_node_type"] == "fueling_stations",
            "calculation_method",
        ] = "transport_service_based"

        demand = demand.replace([np.inf, -np.inf], np.nan)
        demand = self._validate_hydrogen_demand_nodes(demand)
        demand = self._add_hydrogen_distribution_shares(demand)

        columns = self._empty_hydrogen_demand_nodes().columns.tolist()
        self.hydrogen_demand_nodes = (
            demand.reindex(columns=columns)
            .sort_values(["year", "region", "sector", "subsector"])
            .reset_index(drop=True)
        )

    # Regionalization workflow: regionalize hydrogen production and create markets.
    def _regionalize_hydrogen_activities(self):

        hydrogen_map = {
            k: v for k, v in self.fuel_map.items() if k.startswith("hydrogen")
        }

        self.process_and_add_activities(
            mapping=hydrogen_map,
            production_volumes=self.iam_data.production_volumes,
            efficiency_adjustment_fn=self._adjust_hydrogen_efficiency,
        )

        # Create markets for hydrogen
        self.process_and_add_markets(
            name="market for hydrogen, gaseous, low pressure",
            reference_product="hydrogen, gaseous, low pressure",
            unit="kilogram",
            mapping={
                k: v
                for k, v in self.fuel_map.items()
                if k.startswith("hydrogen")
            },
            system_model=self.system_model,
            production_volumes=self.iam_data.production_volumes,
            additional_exchanges_fn=self._add_transport_to_hydrogen_datasets,
            flip_treatment_supplier_sign=True,
        )

        self._generate_sector_specific_hydrogen_markets(hydrogen_map)

    # Sector-market workflow: create end-use-specific hydrogen markets where demand exists.
    def _generate_sector_specific_hydrogen_markets(self, hydrogen_map):
        available_market_regions = (
            self._available_hydrogen_sector_market_regions()
        )
        available_markets = {
            market
            for market, regions in available_market_regions.items()
            if regions
        }
        self.generated_hydrogen_sector_markets = []
        self.generated_hydrogen_sector_market_regions = {
            market: sorted(regions)
            for market, regions in available_market_regions.items()
            if regions
        }
        self.skipped_hydrogen_sector_markets = [
            market
            for market in HYDROGEN_END_USE_MARKETS
            if market not in available_markets
        ]

        for market, market_name in HYDROGEN_END_USE_MARKETS.items():
            if market not in available_markets:
                continue

            production_volumes = self._filter_production_volumes_to_regions(
                self.iam_data.production_volumes,
                available_market_regions[market],
            )
            self.process_and_add_markets(
                name=market_name,
                reference_product="hydrogen, gaseous, low pressure",
                unit="kilogram",
                mapping=hydrogen_map,
                system_model=self.system_model,
                production_volumes=production_volumes,
                additional_exchanges_fn=(
                    self._add_transport_to_sector_specific_hydrogen_market
                ),
            )
            self.generated_hydrogen_sector_markets.append(market)

    # Sector-market workflow: zero IAM production outside regions served by a sector market.
    @staticmethod
    def _filter_production_volumes_to_regions(production_volumes, regions):
        production_volumes = production_volumes.copy(deep=True)
        if "region" not in production_volumes.coords:
            return production_volumes

        regions_to_zero = [
            region
            for region in production_volumes.coords["region"].values
            if region not in regions
        ]
        if regions_to_zero:
            production_volumes.loc[{"region": regions_to_zero}] = 0

        return production_volumes

    # Regionalization workflow: scale hydrogen production inputs to IAM efficiency assumptions.
    def _adjust_hydrogen_efficiency(self, dataset, technology):
        """
        Adjust the efficiency of hydrogen production datasets based on the technology.
        """
        params = hydrogen_parameters.get(technology)
        if not params:
            print(
                "Could not find efficiency parameters for technology:",
                technology,
            )
            return

        feedstock_name = params["feedstock name"]
        feedstock_unit = params["feedstock unit"]
        efficiency = params.get("efficiency")
        floor_value = params.get("floor value")

        initial_energy_use = sum(
            exc["amount"]
            for exc in dataset["exchanges"]
            if exc["unit"] == feedstock_unit
            and feedstock_name in exc["name"]
            and exc["type"] != "production"
        )
        dataset.setdefault("log parameters", {})[
            "initial energy input for hydrogen production"
        ] = initial_energy_use

        new_energy_use = None
        min_energy_use = None
        max_energy_use = None

        if technology in self.fuel_efficiencies.variables.values.tolist():
            scaling_factor = 1 / self.find_iam_efficiency_change(
                data=self.fuel_efficiencies,
                variable=technology,
                location=dataset["location"],
            )
            new_energy_use = max(
                scaling_factor * initial_energy_use, floor_value
            )
        elif "electrolysis" in technology:
            new_energy_use, min_energy_use, max_energy_use = (
                adjust_electrolysis_electricity_requirement(
                    self.year, efficiency
                )
            )
            scaling_factor = (
                new_energy_use / initial_energy_use
                if initial_energy_use
                else 1
            )
        else:
            scaling_factor = 1

        if scaling_factor == 1:
            return

        for exc in ws.technosphere(
            dataset,
            ws.contains("name", feedstock_name),
            ws.equals("unit", feedstock_unit),
        ):
            exc["amount"] *= scaling_factor
            exc["uncertainty type"] = 5
            exc["loc"] = exc["amount"]
            if min_energy_use:
                exc["minimum"] = exc["amount"] * (
                    min_energy_use / new_energy_use
                )
            else:
                exc["minimum"] = exc["loc"] * 0.9
            if max_energy_use:
                exc["maximum"] = exc["amount"] * (
                    max_energy_use / new_energy_use
                )
            else:
                exc["maximum"] = exc["loc"] * 1.1

        dataset["log parameters"][
            "new energy input for hydrogen production"
        ] = new_energy_use

    # Supporting-dataset workflow: regionalize pipeline supply datasets used by markets.
    def _generate_supporting_hydrogen_datasets(self):
        keywords = [
            "hydrogen supply, distributed by pipeline",
        ]

        hydrogen_distribution_map = {
            k: [ws.get_one(self.database, ws.contains("name", k))]
            for k in keywords
        }

        self.process_and_add_activities(
            mapping=hydrogen_distribution_map,
        )

    # Generic-market workflow: add pipeline distribution to non-sector hydrogen markets.
    def _add_transport_to_hydrogen_datasets(self, dataset):

        dataset["exchanges"].append(
            {
                "name": "hydrogen supply, distributed by pipeline",
                "product": "hydrogen, gaseous, from pipeline",
                "location": dataset["location"],
                "unit": "kilogram",
                "type": "technosphere",
                "uncertainty type": 0,
                "amount": 1,
            }
        )

    # Sector-market transport workflow: normalize labels before matching transport suppliers.
    @staticmethod
    def _normalize_hydrogen_transport_label(value):
        return " ".join(str(value).replace(" ,", ",").split()).lower()

    # Sector-market transport workflow: find database datasets for a configured transport mode.
    def _hydrogen_transport_supplier(self, activity):
        target_name = self._normalize_hydrogen_transport_label(
            activity["name"]
        )
        target_product = self._normalize_hydrogen_transport_label(
            activity["reference product"]
        )

        matches = [
            dataset
            for dataset in self.database
            if self._normalize_hydrogen_transport_label(dataset["name"])
            == target_name
            and self._normalize_hydrogen_transport_label(
                dataset["reference product"]
            )
            == target_product
            and dataset["unit"] == activity["unit"]
        ]

        if not matches:
            raise ValueError(
                "No hydrogen transport activity found for "
                f"{activity['name']} / {activity['reference product']}."
            )

        return matches

    # Sector-market transport workflow: choose the best regional transport supplier fallback.
    def _select_hydrogen_transport_supplier(self, activity, region):
        suppliers = self._hydrogen_transport_supplier(activity)

        location_preferences = [
            [region],
            self.iam_to_ecoinvent_loc.get(region, []),
            ["RoW"],
            ["GLO"],
        ]

        for locations in location_preferences:
            for supplier in suppliers:
                if supplier["location"] in locations:
                    return supplier

        return suppliers[0]

    # Sector-market transport workflow: map a market dataset name back to its sector key.
    @staticmethod
    def _hydrogen_sector_market_key(market_name):
        for key, sector_market_name in HYDROGEN_END_USE_MARKETS.items():
            if market_name == sector_market_name:
                return key
        return None

    # Sector-market transport workflow: select demand rows represented by a sector market.
    @staticmethod
    def _hydrogen_sector_market_rows(demand, market_key):
        if market_key == "Transport":
            return demand["sector"] == "Transport"
        if market_key in {"Chemicals", "Steel", "Cement"}:
            return demand["subsector"] == market_key
        if market_key == "Heating":
            return demand["sector"] == "Residential and commercial buildings"
        if market_key == "Other":
            return ~(
                (demand["sector"] == "Transport")
                | (demand["sector"] == "Residential and commercial buildings")
                | (demand["subsector"].isin(["Chemicals", "Steel", "Cement"]))
            )
        return pd.Series(False, index=demand.index)

    # Sector-market transport workflow: calculate demand-weighted transport shares for a market.
    def _hydrogen_transport_shares_for_market(self, dataset):
        demand = getattr(self, "hydrogen_demand_nodes", pd.DataFrame())
        if demand.empty:
            return {}

        market_key = self._hydrogen_sector_market_key(dataset["name"])
        if market_key is None:
            return {}

        mask = (
            (demand["year"] == self.year)
            & (demand["region"] == dataset["location"])
            & self._hydrogen_sector_market_rows(demand, market_key)
        )
        rows = demand.loc[mask]

        if rows.empty:
            return {}

        weights = rows["hydrogen_demand_t_per_year"].fillna(0)
        if weights.sum() <= 0:
            return {}

        shares = {}
        for mode in HYDROGEN_TRANSPORT_ACTIVITIES:
            if mode not in rows:
                continue
            shares[mode] = float(
                (rows[mode].fillna(0) * weights).sum() / weights.sum()
            )

        return shares

    # Sector-market transport workflow: create a technosphere exchange for a transport input.
    @staticmethod
    def _hydrogen_transport_exchange(activity, location, amount):
        return {
            "name": activity["name"],
            "product": activity["reference product"],
            "location": location,
            "unit": activity["unit"],
            "type": "technosphere",
            "uncertainty type": 0,
            "amount": amount,
        }

    # Sector-market transport workflow: convert a mode share into the exchange amount unit.
    @staticmethod
    def _hydrogen_transport_amount_for_sector_market(mode, share):
        if mode == "compressed_gaseous_pipeline":
            return share * HYDROGEN_PIPELINE_GENERAL_MARKET_AMOUNT

        distance = HYDROGEN_TRANSPORT_DISTANCES_KM[mode]
        return share * distance * KG_TO_TONNE

    # Sector-market transport workflow: attach configured transport inputs to sector markets.
    def _add_transport_to_sector_specific_hydrogen_market(self, dataset):
        shares = self._hydrogen_transport_shares_for_market(dataset)

        for mode, share in shares.items():
            if share <= 0:
                continue

            activity = HYDROGEN_TRANSPORT_ACTIVITIES[mode]
            amount = self._hydrogen_transport_amount_for_sector_market(
                mode, share
            )

            if mode == "compressed_gaseous_pipeline":
                dataset["exchanges"].append(
                    self._hydrogen_transport_exchange(
                        activity, dataset["location"], amount
                    )
                )
                continue

            supplier = self._select_hydrogen_transport_supplier(
                activity, dataset["location"]
            )
            dataset["exchanges"].append(
                self._hydrogen_transport_exchange(
                    activity=supplier,
                    location=supplier["location"],
                    amount=amount,
                )
            )
