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
import yaml

from premise import DATA_DIR
from premise.electricity_tools import (
    apply_transformation_losses,
    calculate_energy_mix,
    create_exchange_from_ref,
    create_new_energy_exchanges,
    reduce_database,
)
from premise.framework.logics import (
    contains,
    contains_any_from_list,
    does_not_contain,
    equals,
)

from .activity_maps import get_gains_to_ecoinvent_emissions
from .transformation import *
from .transformation import BaseTransformation
from .utils import c, create_hash, e, get_efficiency_ratio_solar_PV, s

PRODUCTION_PER_TECH = (
    DATA_DIR / "electricity" / "electricity_production_volumes_per_tech.csv"
)
LOSS_PER_COUNTRY = DATA_DIR / "electricity" / "losses_per_country.csv"
INFRASTRUCTURE = DATA_DIR / "electricity" / "infrastructure.yaml"


def get_network_infrastructure():

    with open(INFRASTRUCTURE, "r") as stream:
        infras = yaml.safe_load(stream)

    for i, j in infras.items():
        for _k, k in enumerate(j):
            for l, m in k.items():
                if l == "categories":
                    infras[i][_k][l] = m.replace(", ", "::")

    return infras


def get_losses_per_country_dict():
    """
    Create a dictionary with ISO country codes as keys and loss ratios as values.
    :return: ISO country code (keys) to loss ratio (values) dictionary
    :rtype: dict
    """

    if not LOSS_PER_COUNTRY.is_file():
        raise FileNotFoundError(
            "The production per country dictionary file could not be found."
        )

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
        raise FileNotFoundError(
            "The production per technology dictionary file could not be found."
        )
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
        self.infras = get_network_infrastructure()

    @lru_cache()
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
        locations = self.iam_to_eco_loc[region]

        if voltage == "high":

            cumul_prod, transf_loss = 0, 0
            for loc in locations:
                dict_loss = self.losses.get(
                    loc,
                    {"Transformation loss, high voltage": 0, "Production volume": 0},
                )

                transf_loss += (
                    dict_loss["Transformation loss, high voltage"]
                    * dict_loss["Production volume"]
                )
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            return transf_loss, None

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
                transf_loss += (
                    dict_loss["Transformation loss, medium voltage"]
                    * dict_loss["Production volume"]
                )
                distr_loss += (
                    dict_loss["Transmission loss to medium voltage"]
                    * dict_loss["Production volume"]
                )
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
                transf_loss += (
                    dict_loss["Transformation loss, low voltage"]
                    * dict_loss["Production volume"]
                )
                distr_loss += (
                    dict_loss["Transmission loss to low voltage"]
                    * dict_loss["Production volume"]
                )
                cumul_prod += dict_loss["Production volume"]
            transf_loss /= cumul_prod
            distr_loss /= cumul_prod
            return transf_loss, distr_loss

    def create_new_electricity_markets(self, name, ref_prod, unit, voltage):
        """
        Create high voltage market groups for electricity, based on electricity mixes given by the IAM pathway.
        Contribution from solar power is added in low voltage market groups.
        Does not return anything. Modifies the database in place.
        """
        print(f"Creating {voltage} voltage markets...")

        additional_exchanges = []

        for region in self.iam_to_eco_loc.keys():

            for period in range(0, 60, 20):

                market_name = name
                if period > 0:
                    market_name += f", {period}-year period"

                new_market = create_exchange_from_ref(
                    index=self.database.columns,
                    overrides={
                        e.prod_name: market_name,
                        e.prod_prod: ref_prod,
                        e.prod_loc: region,
                        (
                            s.ecoinvent,
                            c.comment,
                        ): "PREMISE created high voltage electricity market",
                    },
                    prod_equals_con=True,
                )

                new_market[(s.exchange, c.unit)] = unit

                new_market[
                    [
                        (scenario, c.amount)
                        for scenario in [s.ecoinvent] + self.scenario_labels
                    ]
                ] = 1

                tuple_key = (market_name, ref_prod, unit)

                if tuple_key in self.producer_locs:
                    self.producer_locs[tuple_key][region] = {
                        "key": new_market[(s.exchange, c.cons_key)],
                        "pv": 0,
                    }
                else:
                    self.producer_locs[tuple_key] = {
                        region: {"key": new_market[(s.exchange, c.cons_key)], "pv": 0}
                    }

                transf_loss, distr_loss = self.get_production_weighted_losses(
                    voltage, region
                )

                scenario_cols = [
                    label
                    for label in self.scenario_labels
                    if region in self.regions[label]
                ]
                loss_excs = [
                    pd.Series(
                        apply_transformation_losses(
                            new_market, transf_loss, scenario_cols
                        )
                    ),
                ]

                distr_loss_exc = None

                if distr_loss:
                    distr_loss_exc = apply_transformation_losses(
                        new_market, distr_loss, scenario_cols
                    )
                    distr_loss_exc[
                        [
                            (col[0], c.amount)
                            for col in distr_loss_exc.index
                            if col[1] == c.amount
                        ]
                    ] += 1

                    txt = None
                    if voltage == "medium":
                        txt = "high"
                    if voltage == "low":
                        txt = "medium"

                    distr_loss_exc[
                        [(s.exchange, c.prod_name), (s.exchange, c.prod_prod)]
                    ] = [
                        market_name.replace(voltage, txt),
                        ref_prod.replace(voltage, txt),
                    ]

                    distr_loss_exc[(s.exchange, c.prod_key)] = create_hash(
                        distr_loss_exc[(s.exchange, c.prod_name)],
                        distr_loss_exc[(s.exchange, c.prod_prod)],
                        distr_loss_exc[(s.exchange, c.prod_loc)],
                    )

                    distr_loss_exc[(s.exchange, c.exc_key)] = create_hash(
                        distr_loss_exc[(s.exchange, c.prod_name)],
                        distr_loss_exc[(s.exchange, c.prod_prod)],
                        distr_loss_exc[(s.exchange, c.prod_loc)],
                        distr_loss_exc[(s.exchange, c.cons_name)],
                        distr_loss_exc[(s.exchange, c.cons_prod)],
                        distr_loss_exc[(s.exchange, c.cons_loc)],
                    )

                for infra_exc in self.infras[voltage]:

                    if infra_exc["type"] == "technosphere":

                        key = self.producer_locs[
                            (
                                infra_exc["name"],
                                infra_exc["reference product"],
                                infra_exc["unit"],
                            )
                        ][infra_exc["location"]]["key"]

                        extra_exc = create_exchange_from_ref(
                            index=self.database.columns,
                            overrides={
                                e.cons_name: market_name,
                                e.cons_prod: ref_prod,
                                e.cons_loc: region,
                                e.prod_name: infra_exc["name"],
                                e.prod_prod: infra_exc["reference product"],
                                e.prod_loc: infra_exc["location"],
                                e.prod_key: key,
                                e.ext_type: infra_exc["type"],
                            },
                            prod_equals_con=False,
                        )

                    else:

                        key = self.biosphere_dict[
                            (
                                infra_exc["name"],
                                infra_exc["categories"].split("::")[0],
                                infra_exc["categories"].split("::")[1]
                                if len(infra_exc["categories"].split("::")) > 1
                                else "unspecified",
                                infra_exc["unit"],
                            )
                        ]

                        extra_exc = create_exchange_from_ref(
                            index=self.database.columns,
                            overrides={
                                e.cons_name: market_name,
                                e.cons_prod: ref_prod,
                                e.cons_loc: region,
                                e.prod_name: infra_exc["name"],
                                e.prod_prod: "",
                                e.prod_loc: infra_exc["categories"],
                                e.prod_key: key,
                                e.ext_type: infra_exc["type"],
                            },
                            prod_equals_con=False,
                        )

                    extra_exc[(s.exchange, c.unit)] = infra_exc["unit"]
                    extra_exc[
                        [
                            (col[0], c.amount)
                            for col in extra_exc.index
                            if col[1] == c.amount and col[0] != s.ecoinvent
                        ]
                    ] = infra_exc["amount"] * np.array(
                        [
                            1
                            if extra_exc[(s.exchange, c.cons_loc)] in self.regions[col]
                            else 0
                            for col in self.scenario_labels
                        ]
                    )

                    loss_excs.append(extra_exc)

                new_exchanges = None
                if voltage in ["low", "high"]:

                    electricity_mix = calculate_energy_mix(
                        iam_data=self.iam_data,
                        region=region,
                        scenarios=self.scenario_labels,
                        period=period,
                        years=[int(i.split("::")[-1]) for i in self.scenario_labels],
                        voltage=voltage,
                    )

                    if not np.isnan(electricity_mix.values).all():

                        reduced = reduce_database(
                            region, electricity_mix, self.database, self.iam_to_eco_loc
                        )

                        new_exchanges = create_new_energy_exchanges(
                            electricity_mix,
                            reduced,
                            cons_name=market_name,
                            cons_prod=ref_prod,
                            cons_loc=region,
                            voltage=voltage,
                        )

                        # subtract sum of solar PV from medium electricity input
                        if voltage == "low":

                            distr_loss_exc[
                                [
                                    (col[0], c.amount)
                                    for col in distr_loss_exc.index
                                    if col[1] == c.amount
                                ]
                            ] -= new_exchanges[
                                [
                                    (col[0], c.amount)
                                    for col in new_exchanges.columns
                                    if col[1] == c.amount
                                ]
                            ].sum(
                                axis=0
                            )

                if isinstance(distr_loss_exc, pd.Series):
                    loss_excs.append(distr_loss_exc)

                concat_list = [
                    pd.DataFrame([new_market] + loss_excs, columns=new_market.index),
                ]

                if isinstance(new_exchanges, pd.DataFrame):
                    concat_list.append(new_exchanges)

                extensions = pd.concat(
                    concat_list,
                    axis=0,
                )

                cols = [col for col in extensions.columns if col[1] == c.amount]
                extensions.loc[:, cols] = extensions.loc[:, cols].fillna(0)

                additional_exchanges.append(extensions)

        self.database = pd.concat(
            [self.database, *additional_exchanges], ignore_index=True
        )

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

        all_techs = [
            tech
            for tech in self.iam_data.efficiency.variables.values
            if tech in self.iam_data.electricity_markets.variables.values
        ]

        for technology in all_techs:

            _filter = self.database[(s.tag, technology)]
            subset = self.database.loc[_filter]

            self.database = self.database[~_filter]

            locs = self.iam_to_eco_loc.keys()
            iam_years = [
                int(scenario.split("::")[-1]) for scenario in self.scenario_labels
            ]

            for iam_loc in locs:

                scenarios = [
                    scen
                    for scen in self.scenario_labels
                    if iam_loc in self.regions[scen]
                ]
                ei_locs = self.iam_to_eco_loc[iam_loc]

                __filters = contains_any_from_list(
                    (s.exchange, c.cons_loc), ei_locs
                ) | equals((s.exchange, c.cons_loc), iam_loc)

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
                    new_text = self.update_new_efficiency_in_comment(
                        scenarios, iam_loc, ei_eff, new_eff
                    )

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

                subset.loc[
                    __filters_tech(subset),
                    [(scenario, c.amount) for scenario in scenarios],
                ] = (
                    subset.loc[__filters_tech(subset), (s.ecoinvent, c.amount)].values[
                        ..., None
                    ]
                    * scaling_factors
                )

                if technology in self.iam_data.emissions.sector:
                    for ei_sub, gains_sub in self.gains_substances.items():

                        scaling_factor_gains = 1 / self.find_gains_emissions_change(
                            pollutant=gains_sub,
                            location=iam_loc,
                            sector=technology,
                            scenarios=scenarios,
                        )

                        __filters_bio = __filters & equals(
                            (s.exchange, c.prod_name), ei_sub
                        )

                        # update location in the (scenario, c.cons_loc) column
                        subset.loc[
                            __filters_bio(subset),
                            [(scenario, c.amount) for scenario in scenarios],
                        ] = (
                            subset.loc[
                                __filters_bio(subset), (s.ecoinvent, c.amount)
                            ].values[..., None]
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

                _filter = self.database[(s.tag, tech)] & equals(
                    (s.exchange, c.unit), "kilowatt hour"
                )(self.database)
                subset = self.database.loc[_filter]
                __filter_prod = equals((s.exchange, c.type), "production")

                for group, ds in subset[__filter_prod(subset)].groupby(
                    [(s.exchange, c.cons_name), (s.exchange, c.cons_prod)]
                ):

                    existing_locs = self.producer_locs[
                        (group[0], group[1], "kilowatt hour")
                    ].keys()

                    locs_to_copy = [
                        k
                        for k, v in self.iam_to_eco_loc.items()
                        if not any(i in v for i in existing_locs)
                    ]

                    if len(locs_to_copy) > 0:

                        self.fetch_proxies(
                            name=group[0],
                            ref_prod=group[1],
                            production_variable=tech,
                            regions_to_copy_to=locs_to_copy,
                            relink=True,
                        )

    def update_efficiency_of_solar_pv(self) -> None:
        """
        Update the efficiency of solar PV modules.
        We look at how many square meters are needed per kilowatt of installed capacity
        to obtain the current efficiency.
        Then we update the surface needed according to the projected efficiency.
        :return:
        """

        print("Updating efficiency of solar PV...")

        iam_years = [int(scenario.split("::")[-1]) for scenario in self.scenario_labels]

        # efficiency of modules in the future
        module_eff = get_efficiency_ratio_solar_PV()

        possible_techs = [
            "micro-Si",
            "single-Si",
            "multi-Si",
            "CIGS",
            "CIS",
            "CdTe",
        ]

        _filter = (
            contains((s.exchange, c.cons_name), "photovoltaic")
            & (
                contains((s.exchange, c.cons_name), "installation")
                | contains((s.exchange, c.cons_name), "construction")
            )
            & ~contains_any_from_list(
                (s.exchange, c.cons_name), ["market", "factory", "module"]
            )
            & contains((s.exchange, c.prod_name), "photovoltaic")
            & contains((s.exchange, c.type), "technosphere")
            & equals((s.exchange, c.unit), "square meter")
        )(self.database)

        _num_filter = (
            _filter
            & self.database[(s.exchange, c.cons_name)].str.contains(r"\d*\.\d+|\d+")
            & self.database[(s.exchange, c.cons_name)].str.contains(
                "|".join(possible_techs)
            )
        )

        power = (
            self.database.loc[_num_filter, (s.exchange, c.cons_name)]
            .str.findall(r"\d*\.\d+|\d+")
            .apply(lambda a: a[0])
            .astype(float)
            .values
        )

        mwp = (
            self.database.loc[_num_filter, (s.exchange, c.cons_name)]
            .str.contains("mwp", case=False, regex=False)
            .values
        )
        mwp = mwp * 1

        mwp[mwp == 1] = 1000
        mwp[mwp == 0] = 1

        power *= mwp

        # surface is also max_power in kW,
        # since we assume a constant 1,000W/m^2
        surface = self.database.loc[_num_filter, (s.ecoinvent, c.amount)].values
        current_eff = power / surface

        def like_function(x):
            for i in possible_techs:
                if i.lower() in x.lower():
                    return i
            return None

        techs = (
            self.database.loc[_num_filter, (s.exchange, c.cons_name)]
            .apply(like_function)
            .values
        )

        new_eff = np.clip(
            module_eff.sel(technology=techs).interp(year=iam_years).values, 0.1, 0.27
        )

        scaling = np.clip(current_eff[:, None] / new_eff, 0, 1)

        self.database.loc[
            _num_filter, [(scenario, c.amount) for scenario in self.scenario_labels]
        ] = (
            self.database.loc[_num_filter, (s.ecoinvent, c.amount)].values[:, None]
            * scaling
        )

        keys = self.database.loc[_num_filter, (s.exchange, c.cons_key)].values

        _filter_prod = (
            contains_any_from_list((s.exchange, c.cons_key), keys)
            & equals((s.exchange, c.type), "production")
        )(self.database)

        d_keys = dict(zip(keys, current_eff))
        d_new_keys = dict(zip(keys, new_eff))

        self.database.loc[_filter_prod, (s.ecoinvent, c.efficiency)] = d_keys.values()

        l_keys = list(d_new_keys.values())
        self.database.loc[
            _filter_prod,
            [(scenario, c.efficiency) for scenario in self.scenario_labels],
        ] = l_keys

    def update_electricity_markets(self):
        """
        Delete electricity markets. Create high, medium and low voltage market groups for electricity.
        Link electricity-consuming datasets to newly created market groups for electricity.
        Return a wurst database with modified datasets.

        :return: a wurst database with new market groups for electricity
        :rtype: list
        """

        # self.create_region_specific_power_plants()
        # self.update_electricity_efficiency()
        # self.update_efficiency_of_solar_pv()

        # We then need to create high voltage IAM electricity markets
        markets = [
            (
                f"market group for electricity, {voltage} voltage",
                f"electricity, {voltage} voltage",
                "kilowatt hour",
                voltage,
            )
            for voltage in ["high", "medium", "low"]
        ]

        tags = [
            (s.tag, f"{voltage} voltage electricity")
            for voltage in ["high", "medium", "low"]
        ]

        for m, market in enumerate(markets):
            self.create_new_electricity_markets(*market)
            self.relink_old_markets(tags[m], *market[:-1])

        print("Done!")

        return self.database
