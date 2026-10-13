import uuid
import copy
import numpy as np
import math

from ..activity_maps import InventorySet, get_mapping
from .utils import (
    fetch_mapping,
    update_co2_emissions,
    update_dataset,
    calculate_fuel_properties,
)
from ..transformation import ws, get_shares_from_production_volume
from ..utils import get_fuel_properties
from .properties import (
    PROPERTIES_KEY,
    CarbonOriginResolver,
    activity_key,
    energy_per_unit,
    summarize_components,
)


class FuelMarketsMixin:

    def process_and_add_fuel_markets(self, reference_fuel, **kwargs):
        """Convert IAM energy shares to supplier quantities per reference MJ.

        Normalize eligible energy shares before converting quantities. Keep
        legacy market auxiliaries per reference output and record their separate
        boundary. Carbon properties describe full oxidation, not tailpipe flows.
        """
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
            dataset["comment"] += (
                f" One reference {kwargs['unit']} supplies {reference:g} MJ lower-heating-value fuel energy. "
                "IAM shares are energy shares; physical supplier quantities can sum to more or less than one. "
                "Ancillary processing energy is additional. Carbon origin is recorded separately from capture credits."
            )
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
