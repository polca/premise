"""
Integrates projections regarding carbon dioxide removal.
"""

import copy

import yaml
import numpy as np
from collections import defaultdict
import xarray as xr

from .filesystem_constants import DATA_DIR, VARIABLES_DIR
from .logger import create_logger
from .transformation import (
    BaseTransformation,
    IAMDataCollection,
    InventorySet,
    List,
    uuid,
    ws,
    get_suppliers_of_a_region,
)
from .electricity import filter_technology
from .utils import rescale_exchanges

logger = create_logger("cdr")

CDR_ACTIVITIES = DATA_DIR / "cdr" / "cdr_activities.yaml"
CDR_TECHS = VARIABLES_DIR / "carbon_dioxide_removal.yaml"
REGION_BIOETHANOL_FEEDSTOCK_MAP = (
    VARIABLES_DIR / "iam_region_to_bioethanol_feedstock.yaml"
)
MEGAJOULE_PER_KILOWATT_HOUR = 3.6
HYDROGEN_LOWER_HEATING_VALUE = 120.0
HYDROGEN_HEAT_EFFICIENCY = 0.9
DAC_HEAT_PUMP_COP = 3.0
DAC_ENERGY_INPUT_LOWER_BOUNDS = {
    "sorbent": {
        "heat": 3.5,
        "electricity": 0.5,
        "total": 4.0,
    },
    "solvent": {
        "heat": 5.3,
        "electricity": 0.7,
        "total": 6.0,
    },
}


def fetch_mapping(filepath=CDR_ACTIVITIES) -> dict:
    """Returns a dictionary from a YML file"""

    with open(filepath, "r", encoding="utf-8") as stream:
        mapping = yaml.safe_load(stream)
    return mapping


def _update_cdr(scenario, version, system_model):

    if scenario["iam data"].cdr_technology_mix is None:
        print("No CDR scenario data available -- skipping")
        return scenario

    cdr = CarbonDioxideRemoval(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        version=version,
        system_model=system_model,
        cache=scenario.get("cache"),
        index=scenario.get("index"),
    )

    if scenario["iam data"].cdr_technology_mix is not None:
        cdr.regionalize_cdr_activities()
        cdr.create_cdr_markets()
        cdr.relink_datasets()
        scenario["database"] = cdr.database
        scenario["cache"] = cdr.cache
        scenario["index"] = cdr.index
    else:
        print("No CDR information found in IAM data. Skipping.")

    if "mapping" not in scenario:
        scenario["mapping"] = {}
    scenario["mapping"]["cdr"] = cdr.cdr_map

    return scenario


class CarbonDioxideRemoval(BaseTransformation):
    """
    Class that modifies CDR inventories and markets
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
        self.database = database
        self.iam_data = iam_data
        self.model = model
        self.pathway = pathway
        self.year = year
        self.version = version
        self.system_model = system_model
        self.mapping = InventorySet(self.database)

    def regionalize_cdr_activities(self) -> None:
        """
        Generates regional variants of mapped carbon dioxide removal activities.
        """

        cdr_map = self.mapping.generate_cdr_map(model=self.model)
        efficiency_adjusted_technologies = self._get_efficiency_adjusted_technologies(
            cdr_mapping=fetch_mapping(CDR_TECHS),
            model=self.model,
        )

        # regionalize support activities
        filters = fetch_mapping()

        self.cdr_support_activities = self.mapping.generate_sets_from_filters(filters)
        self.cdr_support_activities = self._exclude_mapped_cdr_activities_from_support(
            support_activities=self.cdr_support_activities,
            cdr_map=cdr_map,
            technologies=efficiency_adjusted_technologies,
        )

        cdr_support_activities = defaultdict(list)
        for activities in self.cdr_support_activities.values():
            for dataset in activities:
                cdr_support_activities[dataset["name"]].append(dataset)
        self.cdr_support_activities = dict(cdr_support_activities)

        if self.cdr_support_activities:
            self.process_and_add_activities(mapping=self.cdr_support_activities)

        self.cdr_map = self.mapping.generate_cdr_map(model=self.model)
        production_volumes = self._apply_cdr_regional_technology_constraints(
            self.iam_data.production_volumes
        )

        self.process_and_add_activities(
            efficiency_adjustment_fn=self.adjust_cdr_efficiency,
            mapping=self.cdr_map,
            production_volumes=production_volumes,
        )

    @staticmethod
    def _dataset_key(dataset):
        return dataset["name"], dataset["reference product"]

    @staticmethod
    def _as_list(value):
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    @classmethod
    def _model_aliases(cls, aliases, model):
        if not isinstance(aliases, dict):
            return cls._as_list(aliases)
        return cls._as_list(aliases.get(model))

    @staticmethod
    def _has_energy_alias_for_model(technology_mapping, model):
        for alias_group in ("energy_use_aliases", "efficiency_use_aliases"):
            energy_aliases = technology_mapping.get(alias_group, {})
            for carrier_aliases in energy_aliases.values():
                if isinstance(carrier_aliases, dict):
                    if carrier_aliases.get(model):
                        return True
                elif carrier_aliases:
                    return True
        return False

    @classmethod
    def _get_efficiency_adjusted_technologies(cls, cdr_mapping, model):
        return {
            technology
            for technology, technology_mapping in cdr_mapping.items()
            if cls._has_energy_alias_for_model(technology_mapping, model)
        }

    @classmethod
    def _exclude_mapped_cdr_activities_from_support(
        cls, support_activities, cdr_map, technologies
    ):
        """
        Keep efficiency-adjusted CDR foreground activities out of the support pass.

        These mapped activities need the second regionalization pass, where IAM-based
        energy scaling is applied. Mapped activities without energy or efficiency
        aliases stay in the support pass because some IAM technologies intentionally
        share the same inventory dataset.
        """

        mapped_datasets = {
            cls._dataset_key(dataset)
            for technology, datasets in cdr_map.items()
            if technology in technologies
            for dataset in datasets
        }

        return {
            technology: [
                dataset
                for dataset in datasets
                if cls._dataset_key(dataset) not in mapped_datasets
            ]
            for technology, datasets in support_activities.items()
            if any(
                cls._dataset_key(dataset) not in mapped_datasets for dataset in datasets
            )
        }

    def _get_cdr_mapping(self):
        return fetch_mapping(CDR_TECHS)

    def _get_carrier_energy_variables(self, cdr_mapping):
        energy_variables = {}
        for technology, technology_mapping in cdr_mapping.items():
            carrier_variables = {}
            for carrier, aliases in technology_mapping.get(
                "energy_use_aliases", {}
            ).items():
                model_aliases = self._model_aliases(aliases, self.model)
                if model_aliases:
                    carrier_variables[carrier] = model_aliases
            if carrier_variables:
                energy_variables[technology] = carrier_variables

        return energy_variables

    def _get_shared_production_alias_groups(self, cdr_mapping, production_volumes):
        energy_variables = self._get_carrier_energy_variables(cdr_mapping)
        grouped_technologies = defaultdict(list)
        available_variables = set(production_volumes.variables.values.tolist())

        for technology, technology_mapping in cdr_mapping.items():
            if (
                technology not in available_variables
                or technology not in energy_variables
            ):
                continue

            aliases = self._model_aliases(
                technology_mapping.get("iam_aliases", {}), self.model
            )
            if not aliases:
                continue

            grouped_technologies[tuple(sorted(aliases))].append(technology)

        return [
            technologies
            for technologies in grouped_technologies.values()
            if len(technologies) > 1
        ]

    def _sum_cdr_energy_use_for_technology(
        self, technology, energy_variables, production_volumes
    ):
        energy_use = getattr(self.iam_data, "cdr_energy_use", None)
        zero = production_volumes.sel(variables=technology) * 0

        if energy_use is None or "variables" not in energy_use.coords:
            return zero

        available_energy_variables = set(energy_use.variables.values.tolist())
        variables = [
            f"{technology} - {carrier}"
            for carrier in energy_variables.get(technology, {})
            if f"{technology} - {carrier}" in available_energy_variables
        ]

        if not variables:
            return zero

        energy = abs(energy_use.sel(variables=variables).sum(dim="variables"))
        return energy.reindex_like(zero, fill_value=0)

    def _split_cdr_production_volumes_by_carrier(self, production_volumes):
        """
        Split shared DAC production aliases across carrier-specific technologies.

        IAM models often report one CDR production variable and several final-energy
        variables for the same DAC family. The carrier-specific inventory routes
        should therefore share the production volume according to IAM final-energy
        shares instead of each receiving the full production volume.
        """

        if (
            production_volumes is None
            or "variables" not in production_volumes.coords
            or "region" not in production_volumes.coords
            or "year" not in production_volumes.coords
        ):
            return production_volumes

        cdr_mapping = self._get_cdr_mapping()
        energy_variables = self._get_carrier_energy_variables(cdr_mapping)
        technology_groups = self._get_shared_production_alias_groups(
            cdr_mapping=cdr_mapping,
            production_volumes=production_volumes,
        )
        if not technology_groups:
            return production_volumes

        constrained = production_volumes.copy(deep=True)

        for technologies in technology_groups:
            energy_by_technology = {
                technology: self._sum_cdr_energy_use_for_technology(
                    technology=technology,
                    energy_variables=energy_variables,
                    production_volumes=constrained,
                )
                for technology in technologies
            }
            total_energy = xr.zeros_like(next(iter(energy_by_technology.values())))
            for energy in energy_by_technology.values():
                total_energy += energy

            for technology, energy in energy_by_technology.items():
                share = xr.where(
                    total_energy > 0,
                    energy / total_energy,
                    1 / len(technologies),
                )
                constrained.loc[dict(variables=technology)] = (
                    constrained.sel(variables=technology) * share
                )

        return constrained

    def create_cdr_markets(
        self,
    ):
        production_volumes = self._apply_cdr_regional_technology_constraints(
            self.iam_data.production_volumes
        )

        self.process_and_add_markets(
            name="market for carbon dioxide removal",
            reference_product="carbon dioxide, captured and stored",
            unit="kilogram",
            mapping=self.cdr_map,
            production_volumes=production_volumes,
            system_model=self.system_model,
        )

    @staticmethod
    def _afforestation_feedstock_from_technology(technology):
        technology = technology.lower()
        if "afforestation" not in technology and "re/afforestation" not in technology:
            return None
        for feedstock in ("eucalyptus", "poplar"):
            if feedstock in technology:
                return feedstock
        return None

    def _get_afforestation_region_constraints(self):
        feedstock_mapping = fetch_mapping(REGION_BIOETHANOL_FEEDSTOCK_MAP).get(
            self.model, {}
        )
        wood_by_region = feedstock_mapping.get("wood", {})
        if not wood_by_region or not hasattr(self, "cdr_map"):
            return {}

        constraints = {}
        for technology in self.cdr_map:
            feedstock = self._afforestation_feedstock_from_technology(technology)
            if feedstock is None:
                continue
            regions = {
                region
                for region, region_feedstock in wood_by_region.items()
                if region_feedstock == feedstock
            }
            if regions:
                constraints[technology] = regions

        return constraints

    def _apply_cdr_regional_technology_constraints(self, production_volumes):
        """
        Split duplicate IAM CDR variables across constrained technologies.

        IMAGE reports one afforestation variable, while premise can expose separate
        inventory routes for eucalyptus and poplar/willow plantations. Each route
        initially receives the same IAM production volume label; here we keep that
        volume only in the regions where the route should be used.
        """

        production_volumes = self._split_cdr_production_volumes_by_carrier(
            production_volumes
        )
        constraints = self._get_afforestation_region_constraints()
        if (
            production_volumes is None
            or not constraints
            or "variables" not in production_volumes.coords
            or "region" not in production_volumes.coords
        ):
            return production_volumes

        constrained = production_volumes.copy(deep=True)
        regions = [str(region) for region in constrained.region.values]
        variables = set(constrained.variables.values.tolist())

        for technology, allowed_regions in constraints.items():
            if technology not in variables:
                continue

            disallowed_regions = [
                region
                for region in regions
                if region != "World" and region not in allowed_regions
            ]
            if disallowed_regions:
                constrained.loc[
                    dict(variables=technology, region=disallowed_regions)
                ] = 0

            if "World" in regions:
                non_world_regions = [region for region in regions if region != "World"]
                constrained.loc[dict(variables=technology, region="World")] = (
                    constrained.sel(variables=technology, region=non_world_regions).sum(
                        dim="region"
                    )
                )

        return constrained

    def _get_cdr_efficiency(self, technology, region, carrier):
        efficiencies = getattr(self.iam_data, "cdr_technology_efficiencies", None)
        if efficiencies is None:
            return None

        if technology not in efficiencies.coords["variables"].values:
            return None

        selector = {"variables": technology}

        if "carrier" in efficiencies.coords:
            if carrier not in efficiencies.coords["carrier"].values:
                return None
            selector["carrier"] = carrier

        if "region" in efficiencies.coords:
            if region not in efficiencies.coords["region"].values:
                return None
            selector["region"] = region

        if "year" in efficiencies.coords:
            if self.year in efficiencies.coords["year"].values:
                selector["year"] = self.year
                efficiency = efficiencies.sel(**selector)
            else:
                efficiency = efficiencies.sel(**selector).interp(year=self.year)
        else:
            efficiency = efficiencies.sel(**selector)

        efficiency = float(efficiency.values.item(0))
        if not np.isfinite(efficiency) or efficiency == 0:
            return None

        return efficiency

    @staticmethod
    def _bounded_scaling_factor(efficiency):
        if efficiency is None:
            return 1.0
        return max(0.5, min(1.5, float(1 / efficiency)))

    @staticmethod
    def _get_dac_family(technology, dataset):
        text = " ".join(
            [
                str(technology),
                str(dataset.get("name", "")),
                str(dataset.get("reference product", "")),
            ]
        ).lower()

        if "sorbent" in text:
            return "sorbent"
        if "solvent" in text:
            return "solvent"
        return None

    @staticmethod
    def _uses_heat_pump_heat(technology, dataset):
        text = " ".join(
            [
                str(technology),
                str(dataset.get("name", "")),
                str(dataset.get("reference product", "")),
                str(dataset.get("comment", "")),
            ]
        ).lower()
        return "heat pump" in text

    @staticmethod
    def _exchange_energy_amount_in_megajoule(exchange):
        try:
            amount = float(exchange.get("amount", 0))
        except (TypeError, ValueError):
            return None

        unit = str(exchange.get("unit", "")).lower()
        if unit == "megajoule":
            return amount
        if unit == "kilowatt hour":
            return amount * MEGAJOULE_PER_KILOWATT_HOUR

        text = " ".join(
            [str(exchange.get("name", "")), str(exchange.get("product", ""))]
        ).lower()
        if unit == "kilogram" and "hydrogen" in text:
            return amount * HYDROGEN_LOWER_HEATING_VALUE * HYDROGEN_HEAT_EFFICIENCY

        return None

    @classmethod
    def _sum_exchange_energy_in_megajoule(cls, dataset, filters):
        total = 0.0
        for exchange in ws.technosphere(dataset, *(filters or [])):
            energy = cls._exchange_energy_amount_in_megajoule(exchange)
            if energy is not None and energy > 0:
                total += energy
        return total

    def _rescale_to_energy_lower_bound(self, dataset, filters, lower_bound):
        if lower_bound is None:
            return 1.0

        current = self._sum_exchange_energy_in_megajoule(dataset, filters)
        if current <= 0 or current >= lower_bound:
            return 1.0

        scaling_factor = lower_bound / current
        rescale_exchanges(
            ds=dataset,
            value=scaling_factor,
            technosphere_filters=filters,
        )
        return scaling_factor

    def _get_dac_energy_lower_bounds(self, technology, dataset):
        family = self._get_dac_family(technology, dataset)
        if family not in DAC_ENERGY_INPUT_LOWER_BOUNDS:
            return None, None, None

        lower_bounds = DAC_ENERGY_INPUT_LOWER_BOUNDS[family]
        electricity_lower_bound = lower_bounds["electricity"]
        heat_lower_bound = lower_bounds["heat"]
        total_lower_bound = lower_bounds["total"]

        if self._uses_heat_pump_heat(technology, dataset):
            electricity_lower_bound += heat_lower_bound / DAC_HEAT_PUMP_COP
            heat_lower_bound = None

        return electricity_lower_bound, heat_lower_bound, total_lower_bound

    def adjust_cdr_efficiency(self, dataset, technology):
        """
        Scale energy exchanges using IAM CDR efficiency changes.
        """

        region = dataset["location"]

        electricity_scaling_factor = self._bounded_scaling_factor(
            self._get_cdr_efficiency(technology, region, "electricity")
        )
        heat_scaling_factor = self._bounded_scaling_factor(
            self._get_cdr_efficiency(technology, region, "heat")
        )

        electricity_filter = ws.either(
            ws.contains("name", "electricity"),
            ws.contains("product", "electricity"),
            ws.equals("unit", "kilowatt hour"),
        )
        heat_filter = ws.either(
            *[
                ws.contains(field, term)
                for field in ("name", "product")
                for term in (
                    "heat",
                    "steam",
                    "diesel",
                    "natural gas",
                    "hydrogen",
                    "fuel",
                )
            ]
        )
        no_biosphere_filter = [ws.equals("name", "__no_cdr_biosphere_scaling__")]
        electricity_filters = [
            ws.exclude(ws.contains("name", "carbon dioxide")),
            electricity_filter,
        ]
        heat_filters = [
            ws.exclude(ws.contains("name", "carbon dioxide")),
            ws.exclude(electricity_filter),
            heat_filter,
        ]

        if electricity_scaling_factor != 1:
            rescale_exchanges(
                ds=dataset,
                value=electricity_scaling_factor,
                technosphere_filters=electricity_filters,
                biosphere_filters=no_biosphere_filter,
            )

        if heat_scaling_factor != 1:
            rescale_exchanges(
                ds=dataset,
                value=heat_scaling_factor,
                technosphere_filters=heat_filters,
                biosphere_filters=no_biosphere_filter,
            )

        electricity_lower_bound, heat_lower_bound, total_lower_bound = (
            self._get_dac_energy_lower_bounds(technology, dataset)
        )
        electricity_lower_bound_scaling_factor = self._rescale_to_energy_lower_bound(
            dataset,
            electricity_filters,
            electricity_lower_bound,
        )
        heat_lower_bound_scaling_factor = self._rescale_to_energy_lower_bound(
            dataset,
            heat_filters,
            heat_lower_bound,
        )
        total_energy_filters = [
            ws.exclude(ws.contains("name", "carbon dioxide")),
            ws.either(electricity_filter, heat_filter),
        ]
        total_lower_bound_scaling_factor = self._rescale_to_energy_lower_bound(
            dataset,
            total_energy_filters,
            total_lower_bound,
        )

        if (
            electricity_scaling_factor != 1
            or heat_scaling_factor != 1
            or electricity_lower_bound_scaling_factor != 1
            or heat_lower_bound_scaling_factor != 1
            or total_lower_bound_scaling_factor != 1
        ):
            # add in comments the scaling factor applied
            if "comment" not in dataset:
                dataset["comment"] = (
                    f"The electricity and heat/fuel efficiency of the system has been "
                    f"adjusted to match the efficiency of the average CDR plant in "
                    f"{self.year}."
                )
            else:
                dataset["comment"] += (
                    f" The electricity and heat/fuel efficiency of the system has been "
                    f"adjusted to match the efficiency of the average CDR plant in "
                    f"{self.year}."
                )

            if (
                electricity_lower_bound_scaling_factor != 1
                or heat_lower_bound_scaling_factor != 1
                or total_lower_bound_scaling_factor != 1
            ):
                dataset[
                    "comment"
                ] += " DAC energy inputs have been kept above practical lower bounds."

        dataset.setdefault("log parameters", {}).update(
            {
                "electricity efficiency scaling factor": electricity_scaling_factor,
                "heat efficiency scaling factor": heat_scaling_factor,
                "electricity lower-bound scaling factor": (
                    electricity_lower_bound_scaling_factor
                ),
                "heat lower-bound scaling factor": heat_lower_bound_scaling_factor,
                "total lower-bound scaling factor": total_lower_bound_scaling_factor,
                "electricity lower bound (MJ/kg CO2)": electricity_lower_bound,
                "heat lower bound (MJ/kg CO2)": heat_lower_bound,
                "total lower bound (MJ/kg CO2)": total_lower_bound,
            }
        )

        return dataset

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """
        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
            f"{dataset.get('log parameters', {}).get('electricity efficiency scaling factor', '')}|"
            f"{dataset.get('log parameters', {}).get('heat efficiency scaling factor', '')}"
        )
