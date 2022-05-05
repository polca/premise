"""
transformation.py contains the base class TransformationBase, used by other modules.
It provides basic methods usually used for electricity, cement, steel sectors transformation
on the wurst database.
"""
import enum
from collections import defaultdict
from copy import deepcopy
from functools import lru_cache
from itertools import product

import numpy as np
import pandas as pd
import xarray as xr
from wurst import searching as ws

from premise.framework.transformation_tools import *

from .exceptions import NoCandidateInDatabase
from .geomap import Geomap
from .inventory_imports import get_biosphere_code
from .utils import c, create_scenario_label, get_fuel_properties, s


def get_suppliers_of_a_region(database, locations, names, reference_product, unit):
    """
    Return a list of datasets, for which the location, name, reference production and unit correspond
    to the region and name given, respectively.

    :param database: database to search
    :type database: list of dictionaries
    :param locations: list of locations
    :type locations: list
    :param names: names of datasets
    :type names: list
    :param unit: unit of dataset
    :type unit: str
    :param reference_product: reference product of dataset
    :type reference_product: str
    :return: list of wurst datasets
    :rtype: list
    """

    return ws.get_many(
        database,
        *[
            ws.either(*[ws.contains("name", supplier) for supplier in names]),
            ws.either(*[ws.equals("location", loc) for loc in locations]),
            ws.contains("reference product", reference_product),
            ws.equals("unit", unit),
        ],
    )


def get_shares_from_production_volume(ds_list):
    """
    Return shares of supply of each datasets in `ds_list` based on respective production volumes
    :param ds_list: list of datasets
    :type ds_list: list
    :return: dictionary with (dataset name, dataset location, ref prod, unit) as keys, shares as values. Shares total 1.
    :rtype: dict
    """

    if not isinstance(ds_list, list):
        ds_list = [ds_list]

    dict_act = {}
    total_production_volume = 0
    for act in ds_list:
        for exc in ws.production(act):
            # even if non-existent, we set a minimum value of 1e-9
            # because if not, we risk dividing by zero!!!
            production_volume = max(float(exc.get("production volume", 1e-9)), 1e-9)

            dict_act[
                (
                    act["name"],
                    act["location"],
                    act["reference product"],
                    act["unit"],
                )
            ] = production_volume
            total_production_volume += production_volume

    for dataset in dict_act:
        dict_act[dataset] /= total_production_volume

    return dict_act


def get_tuples_from_database(database):
    """
    Return a list of tuples (name, reference product, location)
    for each dataset in database.
    :param database:
    :return: a list of tuples
    :rtype: list
    """
    return [
        (dataset["name"], dataset["reference product"], dataset["location"])
        for dataset in database
    ]


def format_dict(data):
    _data = [dict(zip(["loc", "pv", "key"], d)) for d in data]
    return {i["loc"]: {"pv": i["pv"], "key": i["key"]} for i in _data}


class BaseTransformation:
    """
    Base transformation class.
    """

    def __init__(
        self,
        database: pd.DataFrame,
        iam_data: xr.DataArray,
        scenarios: dict,
    ):
        self.database = database
        self.iam_data = iam_data
        self.scenarios = scenarios
        self.scenario_labels = [
            create_scenario_label(
                scenario["model"], scenario["pathway"], scenario["year"]
            )
            for scenario in self.scenarios
        ]
        self.regions = {}
        for label in self.scenario_labels:
            self.regions[label] = [
                r
                for r in self.iam_data.electricity_markets.sel(
                    scenario=label
                ).region.values
                if self.iam_data.electricity_markets.sel(scenario=label, region=r).sum()
                > 0
            ]

        self.fuels_lhv = get_fuel_properties()
        self.fuels_co2 = get_fuel_properties()

        self.ecoinvent_to_iam_loc = {label: {} for label in self.scenario_labels}
        for _s, scenario in enumerate(self.scenarios):
            geo = Geomap(model=scenario["model"])
            self.ecoinvent_to_iam_loc[self.scenario_labels[_s]] = {
                loc: geo.ecoinvent_to_iam_location(loc)
                for loc in get_dataframe_locs(self.database)
            }

        self.iam_to_gains = {}
        for _s, scenario in enumerate(self.scenarios):
            geo = Geomap(model=scenario["model"])
            self.iam_to_gains[self.scenario_labels[_s]] = {
                loc: geo.iam_to_GAINS_region(loc)
                for loc in self.regions[self.scenario_labels[_s]]
            }

        self.iam_to_ecoinvent_loc = {label: {} for label in self.scenario_labels}

        for _s, scenario in enumerate(self.scenarios):
            geo = Geomap(model=scenario["model"])
            self.iam_to_ecoinvent_loc[self.scenario_labels[_s]] = {
                loc: geo.iam_to_ecoinvent_location(loc)
                for loc in self.regions[self.scenario_labels[_s]]
            }

        self.producer_locs = (
            self.database.loc[self.database[(s.exchange, c.type)] == "production"]
            .groupby(
                [
                    (s.exchange, c.cons_name),
                    (s.exchange, c.cons_prod),
                    (s.exchange, c.unit),
                ]
            )[
                [
                    (s.exchange, c.cons_loc),
                    (s.ecoinvent, c.cons_prod_vol),
                    (s.exchange, c.cons_key),
                ]
            ]
            .apply(lambda g: format_dict(g.values.tolist()))
            .to_dict()
        )

        self.iam_to_eco_loc = {}
        for label in self.scenario_labels:
            self.iam_to_eco_loc = self.iam_to_eco_loc | self.iam_to_ecoinvent_loc[label]

        self.eco_to_iam_loc = {}
        for label in self.scenario_labels:
            for k, v in self.iam_to_ecoinvent_loc[label].items():
                for loc in v:
                    if loc not in self.eco_to_iam_loc:
                        self.eco_to_iam_loc[loc] = [k]
                    else:
                        if k not in self.eco_to_iam_loc[loc]:
                            self.eco_to_iam_loc[loc].append(k)

        self.exchange_stack = []

        self.cache = {}
        self.iam_eff_cache = {}
        self.gains_eff_cache = {}
        self.biosphere_dict = get_biosphere_code()

    def update_new_efficiency_in_comment(self, scenarios, iam_loc, old_ei_eff, new_eff):
        """
        Update the old efficiency value in the ecoinvent dataset by the newly calculated one.
        :param dataset: dataset
        :type dataset: dict
        :param scaling_factor: scaling factor (new efficiency / old efficiency)
        :type scaling_factor: float
        """
        # TODO REVIEW: is the argument scenarios here the same as self.scenarios? If so, self.scenarios could be used and the argument deleted.
        new_txt = [
            (
                f" 'premise' has modified the efficiency of this dataset, from an original "
                f"{int(old_ei_eff * 100)}% to {int(new_eff[i] * 100)}%, according to IAM model {scenario.split('::')[0].upper()}, "
                f"scenario {scenario.split('::')[1]} for the region {iam_loc} in {scenario.split('::')[-1]}."
            )
            for i, scenario in enumerate(scenarios)
        ]

        return new_txt

    def fetch_proxies(
        self, name, ref_prod, production_variable, regions_to_copy_to=None, relink=False
    ):
        """
        Fetch dataset proxies, given a dataset `name` and `reference product`.
        Store a copy for each IAM region.
        If a fitting ecoinvent location cannot be found for a given IAM region,
        fetch a dataset with a "RoW" location.
        Delete original datasets from the database.

        :param name: name of the datasets to find
        :type name: str
        :param ref_prod: reference product of the datasets to find
        :type ref_prod: str
        :param production_variable: name of variable in IAM data that refers to production volume
        :type production_variable: list or str
        :param relink: if `relink`, exchanges from the datasets will be relinked to
        the most geographically-appropriate providers from the database. This is computer-intensive.
        :type relink: bool
        :return:
        """

        exchange_stack = []

        if regions_to_copy_to:
            iam_ei_proxy_locs = {
                k: v for k, v in self.iam_to_eco_loc.items() if k in regions_to_copy_to
            }
        else:
            iam_ei_proxy_locs = self.iam_to_eco_loc

        iam_years = [int(scenario.split("::")[-1]) for scenario in self.scenario_labels]

        for iam_loc, ei_locs in iam_ei_proxy_locs.items():

            possible_locs = ei_locs + ["RoW", "GLO", "CH", "RER"]

            _filter = (
                equals((s.exchange, c.cons_name), name)
                & equals((s.exchange, c.cons_prod), ref_prod)
                & contains_any_from_list((s.exchange, c.cons_loc), possible_locs)
            )(self.database)
            original = self.database[_filter]

            if len(original.loc[:, (s.exchange, c.cons_loc)].unique()) == 0:
                raise NoCandidateInDatabase(
                    f"for locations {possible_locs} no candidate in database"
                )

            if len(original.loc[:, (s.exchange, c.cons_loc)].unique()) > 1:
                if any(
                    i not in ["RoW", "GLO"]
                    for i in original.loc[:, (s.exchange, c.cons_loc)].unique()
                ):
                    _filter_loc = ~contains_any_from_list(
                        (s.exchange, c.cons_loc), ["RoW", "GLO"]
                    )
                    original = original.loc[_filter_loc(original)]

            dataset = original.copy()

            dataset = rename_location(df=dataset, new_loc=iam_loc)

            # add new location in self.producer_locs
            self.add_activity_to_producer_locs(dataset)

            # Add `production volume` field
            prod_vol = (
                self.iam_data.production_volumes.sel(
                    scenario=self.scenario_labels,
                    region=iam_loc,
                    year=iam_years,
                    variables=production_variable,
                )
            ).values

            prod_vol *= np.identity(len(iam_years))
            prod_vol = prod_vol.sum(axis=1)

            scenario_cols = list(
                set(
                    [
                        col[0]
                        for col in original.columns
                        if col[0] not in [s.exchange, s.tag, s.ecoinvent]
                    ]
                )
            )

            dataset = change_production_volume(
                dataset,
                scenario_cols,
                prod_vol,
            )

            # move amounts to the scenario column
            dataset.loc[:, [(col, c.amount) for col in scenario_cols]] = np.repeat(
                dataset.loc[:, (s.ecoinvent, c.amount)].values[:, None],
                len(scenario_cols),
                1,
            )

            # relink technopshere exchanges
            if relink:
                dataset = self.relink_technosphere_exchanges(
                    dataset, scenario_cols, iam_ei_proxy_locs
                )

            exchange_stack.append(dataset)

        self.database = pd.concat(
            [self.database] + exchange_stack,
            axis=0,
            ignore_index=True,
        )

    def add_activity_to_producer_locs(self, ds, production_volume=None):

        exc = ds[equals((s.exchange, c.type), "production")(ds)].iloc[0]

        self.producer_locs[
            exc[(s.exchange, c.cons_name)],
            exc[(s.exchange, c.cons_prod)],
            exc[(s.exchange, c.unit)],
        ][exc[(s.exchange, c.cons_loc)]] = {
            "pv": production_volume or 0,
            "key": exc[(s.exchange, c.cons_key)],
        }

    def relink_technosphere_exchanges(self, ds, scenario_cols, iam_to_eco_loc):

        __filters_tech = equals((s.exchange, c.type), "technosphere")(ds)

        new_exchanges = []

        for _, exc in ds[__filters_tech].iterrows():

            lookup_key = (
                exc[(s.exchange, c.prod_name)],
                exc[(s.exchange, c.prod_prod)],
                exc[(s.exchange, c.prod_loc)],
                exc[(s.exchange, c.prod_key)],
            )

            if self.cache.get(exc[(s.exchange, c.cons_loc)], dict()).get(lookup_key):
                cached_exchanges = self.cache[exc[(s.exchange, c.cons_loc)]][lookup_key]
                allocated = self.write_cache_to_exchange(
                    exc, scenario_cols, cached_exchanges
                )

            else:

                prod_loc = self.producer_locs[
                    exc[(s.exchange, c.prod_name)],
                    exc[(s.exchange, c.prod_prod)],
                    exc[(s.exchange, c.unit)],
                ]

                included_locs = iam_to_eco_loc[exc[(s.exchange, c.cons_loc)]]

                if exc[(s.exchange, c.cons_loc)] in prod_loc.keys():
                    new_exc = exc.copy()

                    new_exc[(s.exchange, c.prod_loc)] = new_exc[
                        (s.exchange, c.cons_loc)
                    ]

                    # update producer key
                    new_exc[(s.exchange, c.prod_key)] = prod_loc[
                        new_exc[(s.exchange, c.prod_loc)]
                    ]["key"]

                    # update exchange key
                    new_exc[(s.exchange, c.exc_key)] = create_hash(
                        prod_loc[new_exc[(s.exchange, c.prod_loc)]]["key"],
                        new_exc[(s.exchange, c.cons_key)],
                    )

                    allocated, share = [new_exc], [1.0]
                else:
                    eligible_suppliers = list(
                        set(prod_loc.keys()).intersection(included_locs)
                    )

                    if not eligible_suppliers:
                        eligible_suppliers = [
                            i for i in prod_loc.keys() if i in ["RoW", "GLO"]
                        ]

                    if not eligible_suppliers:
                        new_exchanges.append(exc)
                        allocated, share = [exc], [1.0]
                        self.write_cache(exc, allocated, share)
                        continue

                    allocated, share = self.allocate_inputs(
                        exc,
                        eligible_suppliers,
                        prod_loc,
                        scenario_cols,
                    )
                self.write_cache(exc, allocated, share)
            new_exchanges.extend(allocated)

        __filters_tech = does_not_contain((s.exchange, c.type), "technosphere")(ds)
        ds = ds[__filters_tech]

        if len(new_exchanges) > 0:
            ds = pd.concat(
                [ds, pd.DataFrame(new_exchanges)],
                axis=0,
                ignore_index=True,
            )
        return ds

    def write_cache_to_exchange(self, exc, scenario_cols, cached_exchanges):
        exchanges = []
        for cached_exc in cached_exchanges:
            new_row = exc.copy()
            new_row[(s.exchange, c.prod_name)] = cached_exc[0]
            new_row[(s.exchange, c.prod_prod)] = cached_exc[1]
            new_row[(s.exchange, c.prod_loc)] = cached_exc[2]
            new_row[(s.exchange, c.prod_key)] = cached_exc[3]
            new_row[(s.exchange, c.exc_key)] = create_hash(
                cached_exc[3] + new_row[(s.exchange, c.cons_key)]
            )
            new_row[[(col, c.amount) for col in scenario_cols]] = (
                exc[(s.ecoinvent, c.amount)] * cached_exc[-1]
            )
            exchanges.append(new_row)

        return exchanges

    def write_cache(self, exc, allocated, share):

        if exc[(s.exchange, c.cons_loc)] not in self.cache:
            self.cache[exc[(s.exchange, c.cons_loc)]] = {}

        self.cache[exc[(s.exchange, c.cons_loc)]][
            (
                exc[(s.exchange, c.prod_name)],
                exc[(s.exchange, c.prod_prod)],
                exc[(s.exchange, c.prod_loc)],
                exc[(s.exchange, c.prod_key)],
            )
        ] = [
            (
                e[(s.exchange, c.prod_name)],
                e[(s.exchange, c.prod_prod)],
                e[(s.exchange, c.prod_loc)],
                e[(s.exchange, c.prod_key)],
                _s,
            )
            for e, _s in zip(allocated, share)
        ]

    def allocate_inputs(self, exc, lst, prods, scenario_cols):
        """Allocate the input exchanges in ``lst`` to ``exc``,
        using production volumes where possible, and equal splitting otherwise.
        Always uses equal splitting if ``RoW`` is present."""
        has_row = any((x in ("RoW", "GLO") for x in lst))
        pvs = [prods[i]["pv"] or 0 for i in lst]
        if all((x > 0 for x in pvs)) and not has_row:
            # Allocate using production volume
            total = sum(pvs)
        else:
            # Allocate evenly
            total = len(lst)
            pvs = [1 for _ in range(total)]

        # TODO: REVIEW: can we reformulate?
        #  This is very expensive, having a inner function
        #  and a loop calling it together
        #  with a very expensive copy operation
        def new_exchange(exc, location, factor, scenario_cols):
            cp = exc.copy()
            cp[(s.exchange, c.prod_loc)] = location
            cp[[(col, c.amount) for col in scenario_cols]] = (
                factor * cp[(s.ecoinvent, c.amount)]
            )
            # cp[(s.ecoinvent, c.amount)] = np.nan
            cp[(s.exchange, c.prod_key)] = prods[location]["key"]
            cp[(s.exchange, c.exc_key)] = create_hash(
                prods[location]["key"] + cp[(s.exchange, c.cons_key)]
            )

            return cp

        return (
            [
                new_exchange(exc, obj, factor / total, scenario_cols)
                for obj, factor in zip(lst, pvs)
            ],
            [p / total for p in pvs],
        )

    def get_carbon_capture_rate(self, loc, sector):
        """
        Returns the carbon capture rate as indicated by the IAM
        It is calculated as CO2 captured / (CO2 captured + CO2 emitted)

        :param loc: location of the dataset
        :return: rate of carbon capture
        :param sector: name of the sector to look capture rate for
        :type sector: str or list

        :rtype: float
        """

        if sector in self.iam_data.carbon_capture_rate.variables.values:
            rate = self.iam_data.carbon_capture_rate.sel(
                variables=sector,
                region=loc,
            ).values
        else:
            rate = 0

        return rate

    def find_gains_emissions_change(self, pollutant, location, sector, scenarios):
        """
        Return the relative change in emissions for a given pollutant, location and sector.
        :param pollutant: name of pollutant
        :param sector: name of technology/sector
        :param location: location of emitting dataset
        :return: a scaling factor
        :rtype: float
        """

        key = (pollutant, location, sector, tuple(scenarios))

        if key in self.gains_eff_cache:
            return self.gains_eff_cache[key]

        else:

            location = [
                self.iam_to_gains[s][location]
                if location not in self.iam_data.emissions.region
                else location
                for s in scenarios
            ]

            scaling_factor = self.iam_data.emissions.loc[
                dict(
                    region=location,
                    pollutant=pollutant,
                    sector=sector,
                    scenario=scenarios,
                )
            ]
            scaling_factor = scaling_factor.mean(dim=["year", "region"]).values

            self.gains_eff_cache[key] = scaling_factor

            return scaling_factor

    def find_iam_efficiency_change(self, variable, location, year, scenario):
        """
        Return the relative change in efficiency for `variable` in `location`
        relative to 2020.
        :param variable: IAM variable name
        :param location: IAM region
        :return: relative efficiency change (e.g., 1.05)
        :rtype: float
        """

        key = (variable, location, tuple(year), tuple(scenario))

        if key in self.iam_eff_cache:
            return self.iam_eff_cache[key]
        else:

            scaling_factor = self.iam_data.efficiency.sel(
                region=location, variables=variable, year=year, scenario=scenario
            )

            scaling_factor = scaling_factor.mean(dim="year").values

            self.iam_eff_cache[key] = scaling_factor

            return scaling_factor

    def relink_old_markets(self, tag: Tuple, name, ref_prod, unit):
        """
        Old electricity markets need to be emptied
        and relink to the new regional markets
        """

        sel = self.database[tag].copy(deep=False)
        # ignore mew markets in selector
        sel.loc[sel.isna()] = False
        sel.loc[sel == 0] = False
        sel.loc[sel > 0] = True

        self.database[sel] = emptying_datasets(
            df=self.database[sel],
            scenarios=self.scenario_labels,
        )

        _filter_prod = sel & equals((s.exchange, c.type), "production")(self.database)

        new_exchanges = []
        for _, ds in self.database[_filter_prod].iterrows():

            iam_locs = [
                loc
                for loc in self.eco_to_iam_loc[ds[(s.exchange, c.cons_loc)]]
                if loc != "World"
            ]

            for iam_loc in iam_locs:
                scenario_cols = [
                    col for col in self.scenario_labels if iam_loc in self.regions[col]
                ]

                supplier_key = self.producer_locs[(name, ref_prod, unit,)][
                    iam_loc
                ]["key"]
                new_exchanges.append(
                    create_redirect_exchange(
                        exc=ds,
                        new_loc=iam_loc,
                        new_name=name,
                        new_prod=ref_prod,
                        new_key=supplier_key,
                        cols=scenario_cols,
                    )
                )

        self.database = pd.concat(
            [self.database, pd.DataFrame(new_exchanges)], ignore_index=True
        )
