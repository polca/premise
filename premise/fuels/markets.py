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

    def generate_regional_fuel_market(
        self,
        dataset: dict,
        fuel_providers: dict,
        prod_vars: list,
        vars_map: dict,
        fuel_category: str,
        region: str,
        initial_lhv: float,
        period: int,
        # subset: list,
    ) -> dict:
        """
        Generate regional fuel market for a given dataset and fuel providers.

        :param dataset: The dataset for which to generate the regional fuel market.
        :param fuel_providers: A dictionary of fuel providers, keyed by product variable.
        :param prod_vars: A list of product variables.
        :param vars_map: A dictionary mapping product variables to fuel names.
        :param fuel_category: The fuel name.
        :param region: The region for which to generate the regional fuel market.
        :param activity: The activity dataset for the region.
        :param period: The period for which to generate the regional fuel market.
        :param subset: A list of filters to apply to the fuel providers.
        :return: A tuple containing the final LHV, fossil CO2, and biogenic CO2 emissions for the regional fuel market,
        as well as the updated dataset with the regional fuel market exchanges.

        """
        # Initialize variables
        fossil_co2, non_fossil_co2, final_lhv = [0, 0, 0]

        if period != 0:
            # this dataset is for a period of time
            dataset["name"] += f", {period}-year period"
            dataset["comment"] += (
                f" Average fuel mix over a {period}"
                f"-year period {self.year}-{self.year + period}."
            )
            for exc in ws.production(dataset):
                exc["name"] += f", {period}-year period"

        # Remove existing fuel providers
        dataset["exchanges"] = [
            exc
            for exc in dataset["exchanges"]
            if exc["type"] != "technosphere"
            or (
                exc["product"] != dataset["reference product"]
                and not any(
                    x in exc["name"] for x in ["production", "evaporation", "import"]
                )
            )
        ]

        string = ""

        # if the sum is zero, we need to select a provider
        if np.isclose(
            self.iam_fuel_markets.sel(region=region, variables=prod_vars)
            .interp(year=self.year)
            .sum(dim=["variables"]),
            0,
            rtol=1e-3,
        ):
            if "hydrogen" in dataset["name"].lower():
                prod_vars = [
                    "hydrogen, from natural gas",
                ]

            if "natural gas" in dataset["name"].lower():
                prod_vars = [
                    "natural gas",
                ]

        sum_share = 0
        shares = {}

        for prod_var in prod_vars:
            if len(prod_vars) > 1:
                share = fuel_providers[prod_var]["find_share"](
                    prod_var, tuple(vars_map[fuel_category]), region, period
                )
                sum_share += share
            else:
                share = 1.0
                sum_share = 1.0

            if np.isnan(share) or share <= 0:
                continue

            if isinstance(share, np.ndarray):
                share = share.item(0)

            shares[prod_var] = share

        # normalize shares
        shares = {k: v / sum_share for k, v in shares.items()}

        for prod_var, share in shares.items():

            blacklist = [
                "petroleum coke",
                "petroleum gas",
                "wax",
                # "low pressure",
                "pressure, vehicle grade",
                "burned",
                "market",
                "reduction",
            ]

            if "natural gas" in dataset["name"]:
                blacklist.remove("market")
                blacklist.append("market for natural gas, high pressure")
                blacklist.append("market for natural gas, low pressure")
                blacklist.append("market group for natural gas, high pressure")

            if "low-sulfur" in dataset["name"]:
                blacklist.append("unleaded")

            if "petroleum gas" in dataset["name"]:
                blacklist.remove("petroleum gas")

            candidate_suppliers = tuple(fuel_providers[prod_var]["fuel filters"])

            ecoinvent_regions = self.iam_to_ecoinvent_loc[dataset["location"]]

            possible_locations = [
                [dataset["location"]],
                [*ecoinvent_regions],
                ["RoW"],
                ["GLO"],
                ["Europe without Switzerland"],
                ["RER"],
            ]

            print(dataset["name"], dataset["location"])

            for n in candidate_suppliers:
                print(n["name"], n["location"])
            print()

            counter, suppliers = 0, []

            while len(suppliers) == 0:
                suppliers = [
                    ds
                    for ds in candidate_suppliers
                    if ds["location"] in possible_locations[counter]
                ]
                counter += 1

            if len(suppliers) == 0:
                print(
                    f"No suppliers found for {prod_var} in {region} "
                    f"for dataset in location {dataset['location']}"
                )
                continue

            suppliers = get_shares_from_production_volume(suppliers)

            for supplier_key, supplier_val in suppliers.items():
                # Convert m3 to kg
                conversion_factor = 1.0
                if supplier_key[-1] == "kilogram":
                    if dataset["unit"] == "cubic meter":
                        conversion_factor = 0.735

                supplier_share = share * supplier_val

                # Calculate amount of fuel input
                # Corrected by the LHV of the initial fuel
                # so that the overall composition maintains
                # the same average LHV
                amount = supplier_share * (
                    initial_lhv / self.fuels_specs[prod_var]["lhv"]
                )

                lhv = self.fuels_specs[prod_var]["lhv"]
                co2_factor = self.fuels_specs[prod_var]["co2"]
                biogenic_co2_share = self.fuels_specs[prod_var]["biogenic_share"]

                f_co2, nf_co2, weighted_lhv = calculate_fuel_properties(
                    amount, lhv, co2_factor, biogenic_co2_share
                )

                final_lhv += weighted_lhv
                fossil_co2 += f_co2
                non_fossil_co2 += nf_co2
                dataset = update_dataset(dataset, supplier_key, amount)

                text = (
                    f"{prod_var.capitalize()}: {(share * 100):.1f} pct @ "
                    f"{self.fuels_specs[prod_var]['lhv']} MJ/kg. "
                )
                if text not in string:
                    string += text

        if not np.isclose(sum(e for e in shares.values()), 1.0, rtol=1e-3):
            print(
                f"WARNING: sum of shares for {dataset['name']} in {region} is {sum_share} instead of 1.0"
            )

        if not np.isclose(final_lhv, initial_lhv, rtol=1e-2):
            print(
                f"WARNING: LHV for {dataset['name']} in {region} is {final_lhv} instead of {initial_lhv}"
            )

        dataset.setdefault("log parameters", {})["fossil CO2 per kg fuel"] = fossil_co2
        dataset["log parameters"]["non-fossil CO2 per kg fuel"] = non_fossil_co2
        dataset["log parameters"]["lower heating value"] = final_lhv
        if "natural gas" in dataset["name"]:
            string += f"Final average LHV of {final_lhv} MJ/m3."
        else:
            string += f"Final average LHV of {final_lhv} MJ/kg."

        # check validity of CO2 values
        sum_co2 = sum([fossil_co2, non_fossil_co2])
        if "diesel" in dataset["name"]:
            if sum_co2 < 3.1 or sum_co2 > 3.2:
                print(
                    f"WARNING: CO2 emission factor for {dataset['name']} is {sum_co2} instead of 3.1-3.2"
                )
                print()

        if "petrol" in dataset["name"]:
            if sum_co2 < 2.5 or sum_co2 > 3.2:
                print(
                    f"WARNING: CO2 emission factor for {dataset['name']} is {sum_co2} instead of 2.5-3.2"
                )
                print()

        if "natural gas" in dataset["name"]:
            if sum_co2 < 2.1 or sum_co2 > 2.3:
                print(
                    f"WARNING: CO2 emission factor for {dataset['name']} is {sum_co2} instead of 2.1-2.3"
                )
                print()

        if "comment" in dataset:
            dataset["comment"] += string
        else:
            dataset["comment"] = string

        # add two new fields: `fossil CO2` and `biogenic CO2`
        dataset["fossil CO2"] = fossil_co2
        dataset["non-fossil CO2"] = non_fossil_co2
        dataset["LHV"] = final_lhv

        return dataset

    def generate_fuel_markets(self):
        """
        Create new fuel supply chains and update existing fuel markets.
        """
        self.generate_fuel_supply_chains()

        self.fuel_map = self.mapping.generate_fuel_map()
        d_fuels = self.get_fuel_mapping()
        fuel_markets = self.mapping.generate_map(
            get_mapping(FUEL_MARKETS, "ecoinvent_aliases")
        )

        vars_map = {
            "petrol, low-sulfur": [
                "petrol",
                "ethanol",
                "methanol",
                "gasoline",
                "bioethanol",
            ],
            "diesel, low-sulfur": ["diesel", "biodiesel"],
            "natural gas": ["natural gas", "biomethane"],
            "hydrogen": ["hydrogen"],
            "kerosene": ["kerosene"],
            "liquefied petroleum gas": ["liquefied petroleum gas"],
        }

        lhvs = {
            "petrol, low-sulfur": 42.6,
            "diesel, low-sulfur": 43,
            "natural gas": 36,
            "hydrogen": 120,
            "kerosene": 42.7,
            "liquefied petroleum gas": 46.1,
        }

        new_datasets = []

        for fuel, datasets in fuel_markets.items():
            prod_vars = [
                v
                for v in self.iam_fuel_markets.variables.values
                if any(v.lower().startswith(i.lower()) for i in vars_map[fuel])
            ]

            regionalized_datasets = self.fetch_proxies(
                datasets=datasets, production_variable=prod_vars
            )

            periods = [0] if self.system_model == "consequential" else [0, 20, 40, 60]

            for period in periods:
                copied = copy.deepcopy(regionalized_datasets)
                for region, dataset in copied.items():
                    for exc in ws.production(dataset):
                        exc.pop("input", None)
                    dataset.pop("input", None)
                    dataset["code"] = str(uuid.uuid4().hex)

                    if region != "World":
                        dataset = self.generate_regional_fuel_market(
                            dataset=dataset,
                            fuel_providers=d_fuels,
                            prod_vars=prod_vars,
                            vars_map=vars_map,
                            fuel_category=fuel,
                            region=region,
                            initial_lhv=lhvs[fuel],
                            period=period,
                        )
                    else:
                        dataset = self.generate_world_fuel_market(
                            dataset=dataset,
                            d_act=copied,
                            prod_vars=prod_vars,
                            period=period,
                        )

                    if "log parameters" in dataset:
                        self.new_fuel_markets.update(
                            {
                                (dataset["name"], dataset["location"]): {
                                    "fossil CO2": dataset["log parameters"].get(
                                        "fossil CO2 per kg fuel", 0
                                    ),
                                    "non-fossil CO2": dataset["log parameters"].get(
                                        "non-fossil CO2 per kg fuel", 0
                                    ),
                                    "LHV": dataset["log parameters"].get(
                                        "lower heating value", 0
                                    ),
                                }
                            }
                        )

                        for loc in self.iam_to_ecoinvent_loc[dataset["location"]]:
                            self.new_fuel_markets[(dataset["name"], loc)] = (
                                self.new_fuel_markets[
                                    (dataset["name"], dataset["location"])
                                ]
                            )

                    self.write_log(dataset)
                    self.add_to_index(dataset)
                    new_datasets.append(dataset)

                    if (
                        "low-sulfur" in dataset["name"]
                        and "period" not in dataset["name"]
                    ):
                        new_dataset = copy.deepcopy(dataset)
                        new_dataset["name"] = (
                            new_dataset["name"].replace(", low-sulfur", "").strip()
                        )
                        new_dataset["reference product"] = (
                            new_dataset["reference product"]
                            .replace(", low-sulfur", "")
                            .strip()
                        )
                        for exc in ws.production(new_dataset):
                            exc["name"] = exc["name"].replace(", low-sulfur", "")
                            exc["product"] = exc["product"].replace(", low-sulfur", "")
                        new_dataset["code"] = str(uuid.uuid4().hex)
                        self.write_log(new_dataset)
                        self.add_to_index(new_dataset)
                        new_datasets.append(new_dataset)

        for dataset in ws.get_many(
            self.database,
            ws.either(
                *[ws.equals("name", x[0]) for x in list(self.new_fuel_markets.keys())]
            ),
        ):
            dataset.setdefault("log parameters", {}).update(
                {
                    "fossil CO2 per kg fuel": self.new_fuel_markets.get(
                        (dataset["name"], dataset["location"]), {}
                    ).get("fossil CO2", 0),
                    "non-fossil CO2 per kg fuel": self.new_fuel_markets.get(
                        (dataset["name"], dataset["location"]), {}
                    ).get("non-fossil CO2", 0),
                    "lower heating value": self.new_fuel_markets.get(
                        (dataset["name"], dataset["location"]), {}
                    ).get("LHV", 0),
                }
            )

        self.database.extend(new_datasets)

        datasets_to_empty = {
            "market group for diesel",
            "market group for diesel, low-sulfur",
            "market for petrol, unleaded",
            "market for diesel",
            "market for natural gas, high pressure",
            "market group for natural gas, high pressure",
            "market for kerosene",
            "market for liquefied petroleum gas",
        }

        for old_ds in datasets_to_empty:
            for ds in ws.get_many(
                self.database,
                ws.equals("name", old_ds),
                ws.doesnt_contain_any("location", self.regions),
            ):
                self.remove_from_index(ds)

        dataset = [
            ds
            for ds in self.database
            if ds["name"] == "hydrogen supply, distributed by pipeline"
        ]
        hydrogen_supply = self.fetch_proxies(datasets=dataset)

        for region, dataset in hydrogen_supply.items():
            for exc in ws.technosphere(dataset):
                if exc["name"].startswith("market for hydrogen, gaseous"):
                    exc["location"] = region

            self.write_log(dataset)
            self.add_to_index(dataset)
            self.database.append(dataset)

        self.relink_activities_to_new_markets()

    def generate_world_fuel_market(self, dataset, d_act, prod_vars, period):
        """
        Generate the world fuel market exchanges and compute weighted properties.
        """
        if period != 0:
            dataset["name"] += f", {period}-year period"

            dataset[
                "comment", ""
            ] += f" Average fuel mix over a {period}-year period {self.year}-{self.year + period}."
            for exc in ws.production(dataset):
                exc["name"] += f", {period}-year period"

        dataset["exchanges"] = [
            e for e in dataset["exchanges"] if e["type"] == "production"
        ]

        final_lhv, final_fossil_co2, final_biogenic_co2 = 0, 0, 0

        for r in d_act.keys():
            if r == "World" or (dataset["name"], r) not in self.new_fuel_markets:
                continue

            share = (
                (
                    self.iam_fuel_markets.sel(region=r, variables=prod_vars).sum(
                        dim="variables"
                    )
                    / self.iam_fuel_markets.sel(
                        variables=prod_vars,
                        region=[
                            x
                            for x in self.iam_fuel_markets.region.values
                            if x != "World"
                        ],
                    ).sum(dim=["variables", "region"])
                )
                .interp(
                    year=np.arange(self.year, self.year + period + 1),
                    kwargs={"fill_value": "extrapolate"},
                )
                .mean(dim="year")
                .values
            )

            if np.isnan(share):
                print("Incorrect market share for", dataset["name"], "in", r)
                continue

            key = (dataset["name"], r)
            if key not in self.new_fuel_markets or share <= 0:
                continue

            dataset["exchanges"].append(
                {
                    "uncertainty type": 0,
                    "amount": share,
                    "type": "technosphere",
                    "product": dataset["reference product"],
                    "name": dataset["name"],
                    "unit": dataset["unit"],
                    "location": r,
                }
            )

            props = self.new_fuel_markets[key]
            final_lhv += share * props["LHV"]
            final_fossil_co2 += share * props["fossil CO2"]
            final_biogenic_co2 += share * props["non-fossil CO2"]

        dataset.setdefault("log parameters", {}).update(
            {
                "fossil CO2 per kg fuel": final_fossil_co2,
                "non-fossil CO2 per kg fuel": final_biogenic_co2,
                "lower heating value": final_lhv,
            }
        )

        return dataset

    def relink_activities_to_new_markets(self):
        created_markets = list(set(x[0] for x in self.new_fuel_markets))
        created_markets.extend(
            [
                "market group for " + x.replace("market for ", "")
                for x in created_markets
            ]
        )
        created_markets += [
            "import from",
            "natural gas production",
            "petrol production",
            "diesel production",
            "transport, pipeline",
            "from methane pyrolysis",
        ]

        new_keys = {}
        for key, value in self.new_fuel_markets.items():
            if key[0] == "market for diesel, low-sulfur":
                new_keys.update(
                    {
                        ("market for diesel", key[1]): value,
                        ("market group for diesel", key[1]): value,
                        ("market group for diesel, low-sulfur", key[1]): value,
                    }
                )
            if key[0] == "market for petrol, low-sulfur":
                new_keys.update(
                    {
                        ("market for petrol", key[1]): value,
                        ("market for petrol, unleaded", key[1]): value,
                    }
                )
            if key[0] == "market for natural gas, high pressure":
                new_keys.update(
                    {
                        ("market group for natural gas, high pressure", key[1]): value,
                        ("market for natural gas, low pressure", key[1]): value,
                    }
                )

        self.new_fuel_markets.update(new_keys)

        old_fuel_inputs = [
            "market for " + x for x in list(fetch_mapping(FUEL_MARKETS).keys())
        ]
        old_fuel_inputs += [
            "market for petrol, unleaded",
            "market for diesel",
            "market group for petrol, unleaded",
            "market group for diesel",
            "market group for diesel, low-sulfur",
            "market group for natural gas, high pressure",
        ]

        for dataset in ws.get_many(
            self.database,
            ws.exclude(ws.either(*[ws.contains("name", x) for x in created_markets])),
        ):
            exchanges = list(
                ws.technosphere(
                    dataset,
                    ws.either(*[ws.contains("name", x) for x in old_fuel_inputs]),
                )
            )
            if not exchanges:
                continue

            supplier_loc = dataset["location"]
            if supplier_loc not in self.regions:
                supplier_loc = self.geo.ecoinvent_to_iam_location(supplier_loc)

            amount_non_fossil_co2 = sum(
                a["amount"]
                * self.new_fuel_markets.get((a["name"], supplier_loc), {}).get(
                    "non-fossil CO2", 0
                )
                for a in exchanges
            )

            if amount_non_fossil_co2 > 0 and not any(
                x in dataset["name"].lower()
                for x in [
                    "blending",
                    "lubricating oil production",
                    "petrol production",
                    "natural gas production",
                ]
            ):
                update_co2_emissions(
                    dataset, amount_non_fossil_co2, self.biosphere_flows
                )
                self.write_log(dataset, status="updated")
