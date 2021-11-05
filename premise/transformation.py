"""
transformation.py contains the base class TransformationBase, used by other modules.
It provides basic methods usually used for electricity, cement, steel sectors transformation
on the wurst database.
"""

import csv
import uuid
from datetime import date
from itertools import product

import numpy as np
from wurst import searching as ws
from wurst import transformations as wt

from premise.transformation_tools import get_dataframe_locs

from . import DATA_DIR
from .activity_maps import InventorySet
from .geomap import Geomap
from .utils import (
    c,
    create_scenario_label,
    get_fuel_properties,
    relink_technosphere_exchanges,
)


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


class BaseTransformation:
    """
    Base transformation class.
    """

    def __init__(self, database, iam_data, model, pathway, year):
        self.database = database
        self.iam_data = iam_data
        self.model = model
        self.regions = iam_data.regions
        self.geo = Geomap(model=model, current_regions=self.regions)
        self.pathway = pathway
        self.year = year
        self.fuels_lhv = get_fuel_properties()
        mapping = InventorySet(self.database)
        self.emissions_map = mapping.get_remind_to_ecoinvent_emissions()
        self.fuel_map = mapping.generate_fuel_map()
        self.fuels_co2 = get_fuel_co2_emission_factors()
        self.list_datasets = get_tuples_from_database(self.database)
        self.ecoinvent_to_iam_loc = {
            loc: self.geo.ecoinvent_to_iam_location(loc)
            for loc in get_dataframe_locs(self.database)
        }

    def update_ecoinvent_efficiency_parameter(self, dataset, old_ei_eff, new_eff):
        """
        Update the old efficiency value in the ecoinvent dataset by the newly calculated one.
        :param dataset: dataset
        :type dataset: dict
        :param scaling_factor: scaling factor (new efficiency / old efficiency)
        :type scaling_factor: float
        """
        iam_region = self.ecoinvent_to_iam_loc[dataset[(c.cons_loc)]]

        new_txt = (
            f" 'premise' has modified the efficiency of this dataset, from an original "
            f"{int(old_ei_eff * 100)}% to {int(new_eff * 100)}%, according to IAM model {self.model.upper()}, "
            f"scenario {self.pathway} for the region {iam_region}."
        )

        scenario_label = create_scenario_label(
            model=self.model, pathway=self.pathway, year=self.year
        )

        dataset[(scenario_label, c.comment)] += new_txt

    def get_iam_mapping(self, activity_map, fuels_map, technologies):
        """
        Define filter functions that decide which wurst datasets to modify.
        :param activity_map: a dictionary that contains 'technologies' as keys and activity names as values.
        :param fuels_map: a dictionary that contains 'technologies' as keys and fuel names as values.
        :param technologies: a list of IAM technologies.
        :return: dictionary that contains filters and functions
        :rtype: dict
        """

        return {
            tech: {
                "IAM_eff_func": self.find_iam_efficiency_change,
                "current_eff_func": self.find_fuel_efficiency,
                "technology filters": activity_map[tech],
                "fuel filters": fuels_map[tech],
            }
            for tech in technologies
        }

    def fetch_proxies(self, name, ref_prod, production_variable, relink=False):
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

        d_map = {
            self.ecoinvent_to_iam_loc[d["location"]]: d["location"]
            for d in ws.get_many(
                self.database,
                ws.equals("name", name),
                ws.equals("reference product", ref_prod)
            )
        }

        d_iam_to_eco = {region: d_map.get(region, "RoW") for region in self.regions}

        d_act = {}

        for region in d_iam_to_eco:
            try:
                dataset = ws.get_one(
                    self.database,
                    ws.equals("name", name),
                    ws.equals("reference product", ref_prod),
                    ws.equals("location", d_iam_to_eco[region]),

                )

            except ws.NoResults:

                # trying with `GLO`
                dataset = ws.get_one(
                    self.database,
                    ws.equals("name", name),
                    ws.equals("reference product", ref_prod),
                    ws.equals("location", "GLO"),
                )

            d_act[region] = wt.copy_to_new_location(dataset, region)
            d_act[region]["code"] = str(uuid.uuid4().hex)

            for exc in ws.production(d_act[region]):
                if "input" in exc:
                    exc.pop("input")

            if "input" in d_act[region]:
                d_act[region].pop("input")

            # Add `production volume` field
            if isinstance(production_variable, str):
                production_variable = [production_variable]
            prod_vol = (
                self.iam_data.production_volumes.sel(
                    region=region, variables=production_variable
                )
                .interp(year=self.year)
                .sum(dim="variables")
                .values.item(0)
            )

            for prod in ws.production(d_act[region]):
                prod["location"] = region
                prod["production volume"] = prod_vol

            if relink:
                d_act[region] = relink_technosphere_exchanges(
                    d_act[region], self.database, self.model
                )

        deleted_markets = [
            (act["name"], act["reference product"], act["location"])
            for act in self.database
            if (act["name"], act["reference product"]) == (name, ref_prod)
        ]

        with open(
            DATA_DIR
            / f"logs/log deleted datasets {self.model} {self.pathway} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for line in deleted_markets:
                writer.writerow(line)

        # Remove old datasets
        self.database = [
            act
            for act in self.database
            if (act["name"], act["reference product"]) != (name, ref_prod)
        ]

        # Remove deleted datasets from `self.list_datasets`
        self.list_datasets = [
            dataset
            for dataset in self.list_datasets
            if (dataset[0], dataset[1]) != (name, ref_prod)
        ]

        # Add created datasets to `self.list_datasets`
        self.list_datasets.extend(
            [
                (dataset["name"], dataset["reference product"], dataset["location"])
                for dataset in d_act.values()
            ]
        )

        return d_act

    def relink_datasets(self, excludes_datasets, alternative_names=None):
        """
        For a given exchange name, product and unit, change its location to an IAM location,
        to effectively link to the newly built market(s)/activity(ies).

        :param name: dataset name
        :type name: str
        :param ref_product: reference product of the dataset
        :type ref_product: str
        :param unit: unit of the dataset
        :type unit: str
        :param excludes: list of terms that, if contained in the name of an exchange, should be ignored
        :type excludes: list
        :returns: does not return anything. Modifies in place.
        """

        # loop through the database
        # ignore datasets which name contains `name`
        for act in ws.get_many(
            self.database,
            ws.doesnt_contain_any("name", excludes_datasets),
        ):
            # and find exchanges of datasets to relink

            excs_to_relink = (
                exc
                for exc in act["exchanges"]
                if exc["type"] == "technosphere"
                and (exc["name"], exc["product"], exc["location"])
                not in self.list_datasets
            )

            unique_excs_to_relink = list(
                set(
                    (exc["name"], exc["product"], exc["unit"]) for exc in excs_to_relink
                )
            )

            for exc in unique_excs_to_relink:

                # print(f"searching alt. for {exc['name'], exc['location']} in {act['name'], act['location']}")

                alternative_names = [exc[0], *alternative_names]
                alternative_locations = (
                    [act["location"]]
                    if act["location"] in self.regions
                    else [self.ecoinvent_to_iam_loc[act["location"]]]
                )

                for alt_name, alt_loc in product(
                    alternative_names, alternative_locations
                ):

                    if (alt_name, exc[1], alt_loc) in self.list_datasets:
                        # print(f"found! {alt_name, alt_loc}")
                        break

                # summing up the amounts provided by the unwanted exchanges
                # and remove these unwanted exchanges from the dataset
                amount = sum(
                    e["amount"]
                    for e in excs_to_relink
                    if (e["name"], e["product"]) == exc
                )
                act["exchanges"] = [
                    e for e in act["exchanges"] if (e["name"], e.get("product")) != exc
                ]

                # create a new exchange, with the new provider
                try:
                    new_exc = {
                        "name": alt_name,
                        "product": exc[1],
                        "amount": amount,
                        "type": "technosphere",
                        "unit": exc[2],
                        "location": alt_loc,
                    }

                    act["exchanges"].append(new_exc)

                except:
                    print(
                        f"No alternative provider found for {exc[0], act['location']}."
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

    def find_gains_emissions_change(self, pollutant, location, sector):
        """
        Return the relative change in emissions for a given pollutant, location and sector.
        :param pollutant: name of pollutant
        :param sector: name of technology/sector
        :param location: location of emitting dataset
        :return: a scaling factor
        :rtype: float
        """

        scaling_factor = self.iam_data.emissions.loc[
            dict(
                region=location,
                pollutant=pollutant,
                sector=sector,
            )
        ].values.item(0)

        return scaling_factor

    def find_iam_efficiency_change(self, variable, location):
        """
        Return the relative change in efficiency for `variable` in `location`
        relative to 2020.
        :param variable: IAM variable name
        :param location: IAM region
        :return: relative efficiency change (e.g., 1.05)
        :rtype: float
        """

        scaling_factor = self.iam_data.efficiency.sel(
            region=location, variables=variable
        ).values.item(0)

        if scaling_factor in (np.nan, np.inf):
            scaling_factor = 1

        return scaling_factor
