"""
electricity.py contains the class `Electricity`, which inherits from `BaseTransformation`.
This class transforms the electricity markets and power plants of the wurst database,
based on projections from the IAM pathway.
It also creates electricity markets which mix is an weighted-average
over a certain period (e.g., 10, 20 years).
It eventually re-links all the electricity-consuming activities of the wurst database to
the newly created electricity markets.

"""

import csv
import os
import sys
from collections import defaultdict
from datetime import date

import numpy as np
import pandas as pd
import wurst
import xarray as xr

from premise import DATA_DIR
from premise.framework.logics import (
    contains,
    contains_any_from_list,
    does_not_contain,
    equals,
)

from .activity_maps import get_gains_to_ecoinvent_emissions
from .transformation import BaseTransformation
from .utils import c, create_hash, s, e

from premise.electricity_tools import (
    create_exchange_from_ref,
    apply_transformation_losses,
    calculate_energy_mix,
    reduce_database,
    create_new_energy_exchanges,
)

PRODUCTION_PER_TECH = DATA_DIR / "electricity" / "electricity_production_volumes_per_tech.csv"
LOSS_PER_COUNTRY = DATA_DIR / "electricity" / "losses_per_country.csv"


def get_losses_per_country_dict():
    """
    Create a dictionary with ISO country codes as keys and loss ratios as values.
    :return: ISO country code (keys) to loss ratio (values) dictionary
    :rtype: dict
    """

    if not LOSS_PER_COUNTRY.is_file():
        raise FileNotFoundError("The production per country dictionary file could not be found.")

    with open(LOSS_PER_COUNTRY, encoding="utf-8") as file:
        csv_list = [[val.strip() for val in r.split(";")] for r in file.readlines()]

    (_, *header), *data = csv_list
    csv_dict = {}
    for row in data:
        key, *values = row
        csv_dict[key] = {key: float(value) for key, value in zip(header, values)}

    return csv_dict


def get_production_per_tech_dict():
    """
    Create a dictionary with tuples (technology, country) as keys and production volumes as values.
    :return: technology to production volume dictionary
    :rtype: dict
    """

    if not PRODUCTION_PER_TECH.is_file():
        raise FileNotFoundError("The production per technology dictionary file could not be found.")
    csv_dict = {}
    with open(PRODUCTION_PER_TECH, encoding="utf-8") as file:
        input_dict = csv.reader(file, delimiter=";")
        for row in input_dict:
            csv_dict[(row[0], row[1])] = row[2]

    return csv_dict


class Electricity(BaseTransformation):
    """
    Class that modifies electricity markets in the database based on IAM output data.
    Inherits from `transformation.BaseTransformation`.

    :ivar database: wurst database, which is a list of dictionaries
    :vartype database: list
    :ivar iam_data: IAM data
    :vartype iam_data: xarray.DataArray
    :ivar model: name of the IAM model (e.g., "remind", "image")
    :vartype model: str
    :ivar pathway: name of the IAM pathway (e.g., "SSP2-Base")
    :vartype pathway: str
    :ivar year: year of the pathway (e.g., 2030)
    :vartype year: int

    """

    def __init__(self, database, iam_data, scenarios):
        super().__init__(database, iam_data, scenarios)
        self.losses = get_losses_per_country_dict()
        self.production_per_tech = get_production_per_tech_dict()
        self.gains_substances = get_gains_to_ecoinvent_emissions()

    def get_production_weighted_losses(self, voltage, region):
        """
        Return the transformation, transmission and distribution losses at a given voltage level for a given location.
        A weighted average is made of the locations contained in the IAM region.

        :param voltage: voltage level (high, medium or low)
        :type voltage: str
        :param region: IAM region
        :type region: str
        :return: tuple that contains transformation and distribution losses
        :rtype: tuple
        """

        # TODO: this could be improved: make a dict with all values only once instead of several times

        # Fetch locations contained in IAM region
        locations = self.geo.iam_to_ecoinvent_location(region)

        if voltage == "high":

            cumul_prod, transf_loss = 0, 0
            for loc in locations:
                dict_loss = self.losses.get(
                    loc,
                    {"Transformation loss, high voltage": 0, "Production volume": 0},
                )

                transf_loss += dict_loss["Transformation loss, high voltage"] * dict_loss["Production volume"]
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            return transf_loss

        if voltage == "medium":
            cumul_prod, transf_loss, distr_loss = 0, 0, 0
            for loc in locations:
                dict_loss = self.losses.get(
                    loc,
                    {
                        "Transformation loss, medium voltage": 0,
                        "Transmission loss to medium voltage": 0,
                        "Production volume": 0,
                    },
                )
                transf_loss += dict_loss["Transformation loss, medium voltage"] * dict_loss["Production volume"]
                distr_loss += dict_loss["Transmission loss to medium voltage"] * dict_loss["Production volume"]
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            distr_loss /= cumul_prod
            return transf_loss, distr_loss

        if voltage == "low":

            cumul_prod, transf_loss, distr_loss = 0, 0, 0

            for loc in locations:
                dict_loss = self.losses.get(
                    loc,
                    {
                        "Transformation loss, low voltage": 0,
                        "Transmission loss to low voltage": 0,
                        "Production volume": 0,
                    },
                )
                transf_loss += dict_loss["Transformation loss, low voltage"] * dict_loss["Production volume"]
                distr_loss += dict_loss["Transmission loss to low voltage"] * dict_loss["Production volume"]
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            distr_loss /= cumul_prod
            return transf_loss, distr_loss

    def create_new_markets_low_voltage(self):
        """
        Create low voltage market groups for electricity, by receiving medium voltage market groups as input
        and adding transformation and distribution losses. Transformation and distribution losses are taken from ei37.
        Contribution from solar power is added here as well, as most is delivered at low voltage,
        although CSP delivers at high voltage.
        Does not return anything. Modifies the database in place.
        """

        # we keep a log of created markets
        log_created_markets = []

        # Loop through IAM regions
        for region in self.regions:

            # `period` is a period of time considered to create time-weighted average mix
            # when `period` == 0, this is a market mix for the year `self.year`
            # when `period` == 10, this is a market mix for the period `self.year` + 10
            # this is useful for systems that consume electricity
            # over a long period of time (e.g., buildings, BEVs, etc.)
            for period in range(0, 60, 10):

                new_exchanges = []

                new_market = create_exchange_from_ref(
                    index=self.database.columns,
                    prod_equals_con=True,
                    overrides={
                        e.prod_name: "market group for electricity, low voltage",
                        e.prod_prod: "electricity, low voltage",
                        e.prod_loc: region,
                        e.type: "production",
                        (s.ecoinvent, c.cons_prod_vol): 0,
                        e.unit: "kilowatt hour",
                    },
                )

                new_exchanges.append(new_market)

                if period > 0:
                    new_market[(s.ecoinvent, c.comment)] += f", {period}-year period"


                exc = create_exchange_from_ref(
                    index=self.database.columns,
                    prod_equals_con=True,
                    overrides={
                        e.con_name: "market group for electricity, low voltage",
                        e.con_prod: "electricity, low voltage",
                        e.con_loc: region,
                        e.prod_name: "market for sulfur hexafluoride, liquid",
                        e.prod_prod: "sulfur hexafluoride, liquid",
                        e.prod_loc: "RoW",
                        e.type: "technosphere",
                        (s.ecoinvent, c.cons_prod_vol): 0,
                        e.unit: "kilogram",
                    },
                )
                new_exchanges.append(exc)

                exc = create_exchange_from_ref(
                    index=self.database.columns,
                    prod_equals_con=True,
                    overrides={
                        e.con_name: "market group for electricity, low voltage",
                        e.con_prod: "electricity, low voltage",
                        e.con_loc: region,
                        e.prod_name: "Sulfur hexafluoride",
                        e.prod_prod: "sulfur hexafluoride, liquid",
                        e.prod_loc: "RoW",
                        e.type: "biosphere3",
                        (s.ecoinvent, c.cons_prod_vol): 0,
                        e.unit: "kilogram",
                    },
                )
                new_exchanges.append(exc)
                # FIXME: currently we can not cover all biosphere info. We need to extend the dataframe or find a different interface.
                # original data:
                # {
                #     "uncertainty type": 0,
                #     "loc": 2.99e-9,
                #     "amount": 2.99e-9,
                #     "type": "biosphere",
                #     "input": (
                #         "biosphere3",
                #         "35d1dff5-b535-4628-9826-4a8fce08a1f2",
                #     ),
                #     "name": "Sulfur hexafluoride",
                #     "unit": "kilogram",
                #     "categories": ("air", "non-urban air or from high stacks"),
                # },

                exc = create_exchange_from_ref(
                    index=self.database.columns,
                    prod_equals_con=True,
                    overrides={
                        e.con_name: "market group for electricity, low voltage",
                        e.con_prod: "electricity, low voltage",
                        e.con_loc: region,
                        e.prod_name: "distribution network construction, electricity, low voltage",
                        e.prod_prod: "distribution network, electricity, low voltage",
                        e.prod_loc: "RoW",
                        (s.ecoinvent, c.amount): 8.74e-8,
                        e.type: "technosphere",
                        (s.ecoinvent, c.cons_prod_vol): 0,
                        e.unit: "kilometer",
                    },
                )
                new_exchanges.append(exc)

                electricity_mix, solar_share = calculate_energy_mix(
                    iam_data=self.iam_data,
                    region=region,
                    scenarios=self.scenario_labels,
                    period=period,
                    years=[int(i.split("::")[-1]) for i in self.scenario_labels],
                )

                reduced = reduce_database(region, electricity_mix, self.database, self.iam_to_eco_loc)

                new_exchanges.append(create_new_energy_exchanges(electricity_mix,
                                                            reduced,
                                                            solar_share,
                                                            cons_name="market group for electricity, low voltage",
                                                            cons_prod="electricity, low voltage",
                                                            cons_loc: region,
                                                            ))


                # TODO: double check, this looks as if the share is differently calculated then in the high voltage case.
                # TODO: the current implementation does it as in the high voltage case.
                # # Fourth, add the contribution of solar power
                # solar_amount = 0

                # for technology in (t for t in mix if "solar" in t.lower()):
                #     # If the solar power technology contributes to the mix
                #     if mix[technology] > 0:
                #         # Fetch ecoinvent regions contained in the IAM region
                #         ecoinvent_regions = self.geo.iam_to_ecoinvent_location(region)

                #         # Contribution in supply
                #         amount = mix[technology]
                #         solar_amount += amount

                #         # Get the possible names of ecoinvent datasets
                #         ecoinvent_technologies = self.powerplant_map[technology]

                #         # Fetch electricity-producing technologies contained in the IAM region
                #         # if they cannot be found for the ecoinvent locations concerned
                #         # we widen the scope to EU-based datasets, and RoW
                #         possible_locations = [ecoinvent_regions, ["RER"], ["RoW"]]
                #         suppliers, counter = [], 0

                #         while len(suppliers) == 0:
                #             suppliers = list(
                #                 get_suppliers_of_a_region(
                #                     database=self.database,
                #                     locations=possible_locations[counter],
                #                     names=ecoinvent_technologies,
                #                     reference_product="electricity",
                #                     unit="kilowatt hour",
                #                 )
                #             )
                #             counter += 1

                #         suppliers = get_shares_from_production_volume(suppliers)

                #         for supplier, share in suppliers.items():
                #             new_exchanges.append(
                #                 {
                #                     "uncertainty type": 0,
                #                     "loc": (amount * share),
                #                     "amount": (amount * share),
                #                     "type": "technosphere",
                #                     "production volume": 0,
                #                     "product": supplier[2],
                #                     "name": supplier[0],
                #                     "unit": supplier[-1],
                #                     "location": supplier[1],
                #                 }
                #             )

                #             if period == 0:
                #                 log_created_markets.append(
                #                     [
                #                         f"low voltage, {self.pathway}, {self.year}",
                #                         "n/a",
                #                         region,
                #                         0,
                #                         0,
                #                         supplier[0],
                #                         supplier[1],
                #                         share,
                #                         (share * amount),
                #                     ]
                #                 )
                #             else:
                #                 log_created_markets.append(
                #                     [
                #                         f"low voltage, {self.pathway}, {self.year}, {period}-year period",
                #                         "n/a",
                #                         region,
                #                         0,
                #                         0,
                #                         supplier[0],
                #                         supplier[1],
                #                         share,
                #                         (share * amount),
                #                     ]
                #                 )

                # Fifth, add:
                # * an input from the medium voltage market minus solar contribution, including distribution loss
                # * an self-consuming input for transformation loss

                transf_loss, distr_loss = self.get_production_weighted_losses("low", region)

                if period == 0:
                    name_suffix = f", {period}-year period"
                else:
                    name_suffix = ""

                exc = create_exchange_from_ref(
                    index=self.database.columns,
                    overrides={
                        e.con_name: "market group for electricity, low voltage",
                        e.con_prod: "electricity, low voltage",
                        e.con_loc: region,
                        e.prod_name: "market group for electricity, medium voltage"+name_suffix,
                        e.prod_prod: "electricity, medium voltage",
                        e.prod_loc: region,
                        e.type: "technosphere",
                        (s.ecoinvent, c.cons_prod_vol): 0,
                        e.unit: "kilowatt hour",
                    },
                )
                cols = [i for i in extensions.columns if "::" in str(i[0]) and i[1] == c.amount]
                exc[cols] = (1 - solar_amount) * (1 + distr_loss)
                new_exchanges.append(exc)

                exc = create_exchange_from_ref(
                    index=self.database.columns,
                    overrides={
                        e.con_name: "market group for electricity, low voltage",
                        e.con_prod: "electricity, low voltage",
                        e.con_loc: region,
                        e.prod_name: "market group for electricity, low voltage"+name_suffix,
                        e.prod_prod: "electricity, low voltage",
                        e.prod_loc: region,
                        e.type: "technosphere",
                        (s.ecoinvent, c.cons_prod_vol): 0,
                        (s.ecoinvent, c.amount): transf_loss,
                        e.unit: "kilowatt hour",
                    },
                )
                cols = [i for i in extensions.columns if "::" in str(i[0]) and i[1] == c.amount]
                exc[cols] = (1 - solar_amount) * (1 + distr_loss)
                new_exchanges.append(exc)

        extensions = pd.concat([new_exchanges, pd.DataFrame(new_exchanges).T])

        additional_exchanges.append(extensions)

        # update `self.list_datasets`
        # self.list_datasets.extend(
        #     [
        #         (
        #             "market group for electricity, low voltage",
        #             "electricity, low voltage",
        #             dataset[2],
        #         )
        #         for dataset in log_created_markets
        #     ]
        # )

        # with open(
        #     DATA_DIR / f"logs/log created electricity markets {self.pathway} {self.year}-{date.today()}.csv",
        #     "a",
        #     encoding="utf-8",
        # ) as csv_file:
        #     writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
        #     for line in log_created_markets:
        #         writer.writerow(line)

    def create_new_markets_medium_voltage(self):
        """
        Create medium voltage market groups for electricity, by receiving high voltage market groups as inputs
        and adding transformation and distribution losses.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """

        log_created_markets = []

        for region in self.regions:

            # `period` is a period of time considered to create time-weighted average mix
            # when `period` == 0, this is a market mix for the year `self.year`
            # when `period` == 10, this is a market mix for the period `self.year` + 10
            # this is useful for systems that consume electricity
            # over a long period of time (e.g., buildings, BEVs, etc.)
            for period in range(0, 60, 10):

                # Create an empty dataset

                if period == 0:
                    # this dataset is for year = `self.year`
                    new_dataset = {
                        "location": region,
                        "name": "market group for electricity, medium voltage",
                        "reference product": "electricity, medium voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.pathway} for the year {self.year}.",
                    }

                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": "market group for electricity, medium voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                else:
                    # this dataset is for a period of time
                    new_dataset = {
                        "location": region,
                        "name": f"market group for electricity, medium voltage, {period}-year period",
                        "reference product": "electricity, medium voltage",
                        "unit": "kilowatt hour",
                        "database": self.database[1]["database"],
                        "code": str(uuid.uuid4().hex),
                        "comment": f"Dataset created by `premise` from the IAM model {self.model.upper()}"
                        f" using the pathway {self.pathway}. Average electricity mix over a {period}"
                        f"-year period {self.year}-{self.year + period}.",
                    }

                    # First, add the reference product exchange
                    new_exchanges = [
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": f"market group for electricity, medium voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    ]

                # Second, add:
                # * an input from the high voltage market, including transmission loss
                # * an self-consuming input for transformation loss

                transf_loss, distr_loss = self.get_production_weighted_losses("medium", region)

                if period == 0:
                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": 1 + distr_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, high voltage",
                            "name": "market group for electricity, high voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": transf_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": "market group for electricity, medium voltage",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                else:

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": 1 + distr_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, high voltage",
                            "name": f"market group for electricity, high voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                    new_exchanges.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": transf_loss,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": "electricity, medium voltage",
                            "name": f"market group for electricity, medium voltage, {period}-year period",
                            "unit": "kilowatt hour",
                            "location": region,
                        }
                    )

                # Third, add an input to of sulfur hexafluoride emission to compensate the transformer's leakage
                # And an emission of a corresponding amount
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 5.4e-8,
                        "amount": 5.4e-8,
                        "type": "technosphere",
                        "production volume": 0,
                        "product": "sulfur hexafluoride, liquid",
                        "name": "market for sulfur hexafluoride, liquid",
                        "unit": "kilogram",
                        "location": "RoW",
                    }
                )
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 5.4e-8,
                        "amount": 5.4e-8,
                        "type": "biosphere",
                        "input": ("biosphere3", "35d1dff5-b535-4628-9826-4a8fce08a1f2"),
                        "name": "Sulfur hexafluoride",
                        "unit": "kilogram",
                        "categories": ("air", "non-urban air or from high stacks"),
                    }
                )

                # Fourth, transmission line
                new_exchanges.append(
                    {
                        "uncertainty type": 0,
                        "loc": 1.8628e-8,
                        "amount": 1.8628e-8,
                        "type": "technosphere",
                        "production volume": 0,
                        "product": "transmission network, electricity, medium voltage",
                        "name": "transmission network construction, electricity, medium voltage",
                        "unit": "kilometer",
                        "location": "RoW",
                    }
                )

                new_dataset["exchanges"] = new_exchanges

                if period == 0:

                    log_created_markets.append(
                        [
                            f"medium voltage, {self.pathway}, {self.year}",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            "medium voltage, {self.pathway}, {self.year}",
                            region,
                            1,
                            1 + distr_loss,
                        ]
                    )

                else:

                    log_created_markets.append(
                        [
                            f"medium voltage, {self.pathway}, {self.year}, {period}-year period",
                            "n/a",
                            region,
                            transf_loss,
                            distr_loss,
                            f"medium voltage, {self.pathway}, {self.year}",
                            region,
                            1,
                            1 + distr_loss,
                        ]
                    )

                self.database.append(new_dataset)

        # update `self.list_datasets`
        self.list_datasets.extend(
            [
                (
                    "market group for electricity, medium voltage",
                    "electricity, medium voltage",
                    dataset[2],
                )
                for dataset in log_created_markets
            ]
        )

        with open(
            DATA_DIR / f"logs/log created electricity markets {self.pathway} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for line in log_created_markets:
                writer.writerow(line)

    def create_new_markets_high_voltage(self):
        """
        Create high voltage market groups for electricity, based on electricity mixes given by the IAM pathway.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """

        # log_created_markets = []

        regions_set = []
        for _regs in self.regions.values():
            regions_set.extend(_regs)
        regions_set = set(regions_set)  # set of all regions from all IAM models

        additional_exchanges = []

        for region in regions_set:
            for period in range(0, 60, 10):

                new_market = create_exchange_from_ref(
                    index=self.database.columns,
                    overrides={
                        e.prod_name: "market group for electricity, high voltage",
                        e.prod_prod: "electricity, high voltage",
                        (s.ecoinvent, c.comment): "PREMISE created high energy voltage market",
                    },
                    prod_equals_con=True,
                )

                trans_loss_exc = apply_transformation_losses(
                    new_market, self.get_production_weighted_losses("high", region)
                )

                electricity_mix, solar_share = calculate_energy_mix(
                    iam_data=self.iam_data,
                    region=region,
                    scenarios=self.scenario_labels,
                    period=period,
                    years=[int(i.split("::")[-1]) for i in self.scenario_labels],
                )

                reduced = reduce_database(region, electricity_mix, self.database, self.iam_to_eco_loc)

                new_exchanges = create_new_energy_exchanges(electricity_mix,
                                                            reduced,
                                                            solar_share,
                                                            cons_name="market group for electricity, high voltage",
                                                            cons_prod="electricity, high voltage",
                                                            cons_loc: region,
                                                            )

                extensions = pd.concat([new_exchanges, pd.DataFrame([new_market, trans_loss_exc]).T])

                additional_exchanges.append(extensions)

        self.database = pd.concat([self.database, *additional_exchanges], ignore_index=True)

        # Writing log of created markets
        # with open(
        #     DATA_DIR / f"logs/log created electricity markets {self.pathway} {self.year}-{date.today()}.csv",
        #     "w",
        #     encoding="utf-8",
        # ) as csv_file:
        #     writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
        #     writer.writerow(
        #         [
        #             "dataset name",
        #             "energy type",
        #             "IAM location",
        #             "Transformation loss",
        #             "Distr./Transmission loss",
        #             "Supplier name",
        #             "Supplier location",
        #             "Contribution within energy type",
        #             "Final contribution",
        #         ]
        #     )
        #     for line in log_created_markets:
        #         writer.writerow(line)

    def update_electricity_efficiency(self):
        """
        This method modifies each ecoinvent coal, gas,
        oil and biomass dataset using data from the IAM pathway.
        Return a wurst database with modified datasets.

        :return: a wurst database, with rescaled electricity-producing datasets.
        :rtype: list
        """

        print("Adjusting efficiency of power plants...")

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        for scenario in self.scenario_labels:
            model, pathway, year = scenario.split("::")

            with open(
                DATA_DIR / f"logs/log power plant efficiencies change {model} {pathway} {year}-{date.today()}.csv",
                "w",
                encoding="utf-8",
            ) as csv_file:
                writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
                writer.writerow(
                    [
                        "dataset name",
                        "location",
                        "original efficiency",
                        "new efficiency",
                    ]
                )

            print(f"Log of changes in power plants efficiencies saved in {DATA_DIR}/logs")

        all_techs = [
            tech
            for tech in self.iam_data.efficiency.variables.values
            if tech in self.iam_data.electricity_markets.variables.values
        ]

        for technology in all_techs:

            _filter = self.database[(s.tag, technology)]
            subset = self.database.loc[_filter]

            self.database = self.database[~_filter]

            locs = [self.regions[scenario] for scenario in self.scenario_labels]
            locs = list(set([item for sublist in locs for item in sublist]))
            iam_years = [int(scenario.split("::")[-1]) for scenario in self.scenario_labels]

            for iam_loc in locs:

                scenarios = [scen for scen in self.scenario_labels if iam_loc in self.regions[scen]]
                ei_locs = [self.iam_to_ecoinvent_loc[l].get(iam_loc) for l in scenarios]
                ei_locs = list(set([item for sublist in ei_locs for item in sublist]))

                __filters = contains_any_from_list((s.exchange, c.cons_loc), ei_locs)

                scaling_factors = 1 / self.find_iam_efficiency_change(
                    variable=technology,
                    location=iam_loc,
                    year=iam_years,
                    scenario=scenarios,
                )

                scaling_factors = np.nan_to_num(scaling_factors, nan=1)

                if (scaling_factors == 1).all():
                    continue

                # we log changes in efficiency
                __filters_prod = __filters & equals((s.exchange, c.type), "production")

                new_eff_list = []
                new_comment_list = []

                for _, row in subset.loc[__filters_prod(subset)].iterrows():

                    ei_eff = row[(s.ecoinvent, c.efficiency)]

                    if np.isnan(ei_eff):
                        continue

                    new_eff = ei_eff * 1 / scaling_factors

                    # generate text for `comment` field
                    new_text = self.update_new_efficiency_in_comment(scenarios, iam_loc, ei_eff, new_eff)

                    new_eff_list.append(new_eff)
                    new_comment_list.append(new_text)

                if len(new_eff_list) > 0:
                    subset.loc[
                        __filters_prod(subset),
                        [(scenario, c.efficiency) for scenario in scenarios],
                    ] = new_eff_list
                    subset.loc[
                        __filters_prod(subset),
                        [(scenario, c.comment) for scenario in scenarios],
                    ] = new_comment_list

                # Rescale all the technosphere exchanges
                # according to the change in efficiency
                # between `year` and 2020
                # from the IAM efficiency values

                # filter out the production exchanges
                # we update technosphere exchanges
                # as well as emissions except those
                # covered by GAINS

                __filters_tech = (
                    __filters
                    & does_not_contain((s.exchange, c.type), "production")
                    & ~contains_any_from_list(
                        (s.exchange, c.prod_name),
                        list(self.gains_substances.keys()),
                    )
                )

                subset.loc[__filters_tech(subset), [(scenario, c.amount) for scenario in scenarios],] = (
                    subset.loc[__filters_tech(subset), (s.ecoinvent, c.amount)].values[..., None] * scaling_factors
                )

                if technology in self.iam_data.emissions.sector:
                    for ei_sub, gains_sub in self.gains_substances.items():

                        scaling_factor_gains = 1 / self.find_gains_emissions_change(
                            pollutant=gains_sub,
                            location=iam_loc,
                            sector=technology,
                            scenarios=scenarios,
                        )

                        __filters_bio = __filters & equals((s.exchange, c.prod_name), ei_sub)

                        # update location in the (scenario, c.cons_loc) column
                        subset.loc[__filters_bio(subset), [(scenario, c.amount) for scenario in scenarios],] = (
                            subset.loc[__filters_bio(subset), (s.ecoinvent, c.amount)].values[..., None]
                            * scaling_factor_gains
                        )

            self.database = pd.concat([self.database, subset])

    def create_region_specific_power_plants(self):
        """
        Some power plant inventories are not native to ecoinvent
        but imported. However, they are defined for a specific location
        (mostly European), but are used in many electricity markets
        (non-European). Hence, we create region-specific versions of these datasets,
        to align inputs providers with the geographical scope of the region.
        """
        print("Creating region-specific datasets...")

        techs = [
            "Biomass CHP",
            "Biomass IGCC CCS",
            "Biomass IGCC",
            "Coal PC",
            "Coal IGCC",
            "Coal PC CCS",
            "Coal CHP",
            "Gas OC",
            "Gas CC",
            "Gas CHP",
            "Gas CC CCS",
        ]

        for tech in techs:

            if tech in self.iam_data.production_volumes.variables:

                __filter_prod = (
                    contains_any_from_list((s.exchange, c.cons_name), self.powerplant_map[tech])
                    & equals((s.exchange, c.type), "production")
                )(self.database)
                subset = self.database.loc[_filter]
                __filter_prod = equals((s.exchange, c.type), "production")

                for group, ds in subset[__filter_prod(subset)].groupby(
                    [(s.exchange, c.cons_name), (s.exchange, c.cons_prod)]
                ):
                    existing_locs = self.producer_locs[(group[0], group[1], "kilowatt hour")].keys()

                    locs_to_copy = [k for k, v in self.iam_to_eco_loc.items() if not any(i in v for i in existing_locs)]

                    self.fetch_proxies(
                        name=group[0],
                        ref_prod=group[1],
                        production_variable=tech,
                        regions_to_copy_to=locs_to_copy,
                        relink=True,
                    )

            # scenario_cols = list(
            #     set(
            #         [
            #             col[0]
            #             for col in subset.columns
            #             if col[0] not in [s.exchange, s.tag, s.ecoinvent]
            #         ]
            #     )
            # )
            #
            # sel = equals((s.exchange, c.type), "production")
            # subset.loc[~sel(subset), [(col, c.amount) for col in scenario_cols]] = 0

            # if iam_loc != "World":
            #     new_exc = create_redirect_exchange(
            #         original, new_loc=iam_loc, cols=scenario_cols
            #     )
            #     self.exchange_stack.append(new_exc)
            # else:
            #     model = [m for m, v in self.regions.items() if iam_loc in v][0]
            #     for loc in self.regions[model]:
            #         prod_vol = (
            #             self.iam_data.production_volumes.sel(
            #                 scenario=self.scenario_labels,
            #                 region=loc,
            #                 variables=tech,
            #             )
            #                 .interp(year=iam_years)
            #                 .sum(dim="scenario")
            #                 .values
            #         )
            #
            #         prod_vol[prod_vol == 0] = 1
            #
            #         total_prod = (
            #             self.iam_data.production_volumes.sel(
            #                 scenario=self.scenario_labels,
            #                 region=self.regions[model],
            #                 variables=tech,
            #             )
            #                 .interp(year=iam_years)
            #                 .sum()
            #                 .values.item(0)
            #         )
            #
            #         new_exc[[(col, c.amount) for col in scenario_cols]] = new_exc[
            #                                                                   (s.ecoinvent, c.amount)
            #                                                               ] * (prod_vol / total_prod)
            #         self.exchange_stack.append(new_exc)

    def update_electricity_markets(self):
        """
        Delete electricity markets. Create high, medium and low voltage market groups for electricity.
        Link electricity-consuming datasets to newly created market groups for electricity.
        Return a wurst database with modified datasets.

        :return: a wurst database with new market groups for electricity
        :rtype: list
        """

        # We then need to create high voltage IAM electricity markets
        print("Create high voltage markets.")
        self.create_new_markets_high_voltage()
        # print("Create medium voltage markets.") # FIXME out commented for debugging
        self.create_new_markets_medium_voltage()
        # print("Create low voltage markets.") # FIXME out commented for debugging
        self.create_new_markets_low_voltage()

        # Finally, we need to relink all electricity-consuming activities to the new electricity markets
        print("Link activities to new electricity markets.")

        self.relink_datasets(
            excludes_datasets=["cobalt industry", "market group for electricity"],
            alternative_names=[
                "market group for electricity, high voltage",
                "market group for electricity, medium voltage",
                "market group for electricity, low voltage",
            ],
        )

        print(f"Log of deleted electricity markets saved in {DATA_DIR}/logs")
        print(f"Log of created electricity markets saved in {DATA_DIR}/logs")

        print("Done!")

        return self.database
