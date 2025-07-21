import uuid
import copy
import numpy as np

from ..activity_maps import InventorySet, get_mapping
from .utils import (
    fetch_mapping,
    update_co2_emissions,
    update_dataset,
    calculate_fuel_properties,
)
from .config import FUEL_MARKETS
from ..transformation import ws, get_shares_from_production_volume


class FuelMarketsMixin:

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
            fuel_share = 0

        return float(fuel_share)
