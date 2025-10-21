"""
transformation.py contains the base class TransformationBase,
used by other classes (e.g. Transport, Electricity, Steel, Cement, etc.).
It provides basic methods usually used for electricity, cement, steel sector transformation
on the wurst database.
"""

import copy
import logging.config
import math
import uuid
from collections import defaultdict
from collections.abc import ValuesView
from copy import deepcopy
from functools import lru_cache
from itertools import groupby, product
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, Union

import numpy as np
import xarray as xr
import yaml
from _operator import itemgetter
from constructive_geometries import resolved_row
from wurst import reference_product, rescale_exchange
from wurst import searching as ws
from wurst import transformations as wt
from xarray import DataArray

from .activity_maps import InventorySet
from .data_collection import IAMDataCollection
from .filesystem_constants import DATA_DIR
from .geomap import Geomap
from .utils import get_fuel_properties

LOG_CONFIG = DATA_DIR / "utils" / "logging" / "logconfig.yaml"
# directory for log files
DIR_LOG_REPORT = Path.cwd() / "export" / "logs"
# if DIR_LOG_REPORT folder does not exist
# we create it
if not Path(DIR_LOG_REPORT).exists():
    Path(DIR_LOG_REPORT).mkdir(parents=True, exist_ok=True)

with open(LOG_CONFIG, encoding="utf-8") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger("module")


def redefine_uncertainty_params(old_exc, new_exc):
    """
    Returns "loc", "scale", "minimum" and "maximum" and "negative" values for a given exchange.
    """

    try:
        if old_exc.get("uncertainty type") in [
            0,
            1,
        ]:
            return (
                new_exc["amount"],
                None,
                None,
                None,
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 2:
            return (
                (
                    math.log(new_exc["amount"] * -1)
                    if new_exc["amount"] < 0
                    else math.log(new_exc["amount"])
                ),
                old_exc.get("scale"),
                None,
                None,
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 3:
            return (
                new_exc["amount"],
                old_exc.get("scale"),
                None,
                None,
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 4:
            return (
                None,
                None,
                old_exc.get("minimum", 0) * (new_exc["amount"] / old_exc["amount"]),
                old_exc.get("maximum") * (new_exc["amount"] / old_exc["amount"]),
                True if new_exc["amount"] < 0 else False,
            )

        elif old_exc.get("uncertainty type") == 5:
            return (
                new_exc["amount"],
                None,
                old_exc.get("minimum", 0) * (new_exc["amount"] / old_exc["amount"]),
                old_exc.get("maximum") * (new_exc["amount"] / old_exc["amount"]),
                True if new_exc["amount"] < 0 else False,
            )

        else:
            return None, None, None, None, None
    except:
        print("ERROR")
        print(old_exc)
        print(new_exc)
        return None, None, None, None, None


def group_dicts_by_keys(dicts: list, keys: list):
    groups = defaultdict(list)
    for d in dicts:
        group_key = tuple(d.get(k) for k in keys)
        groups[group_key].append(d)
    return list(groups.values())


def get_suppliers_of_a_region(
    database: List[dict],
    locations: List[str],
    names: List[str],
    reference_prod: str,
    unit: str,
    exclude: List[str] = None,
    exact_match: bool = False,
) -> filter:
    """
    Return a list of datasets, for which the location, name,
    reference product and unit correspond to the region and name
    given, respectively.

    :param database: database to search
    :param locations: list of locations
    :param names: names of datasets
    :param unit: unit of dataset
    :param reference_prod: reference product of dataset
    :return: list of wurst datasets
    :param exclude: list of terms to exclude
    """

    if exact_match is True:
        filters = [
            ws.either(*[ws.equals("name", supplier) for supplier in names]),
        ]
    else:
        filters = [
            ws.either(*[ws.contains("name", supplier) for supplier in names]),
        ]

    filters += [
        ws.either(*[ws.equals("location", loc) for loc in locations]),
        ws.contains("reference product", reference_prod),
        ws.equals("unit", unit),
    ]

    if exclude:
        filters.append(ws.doesnt_contain_any("name", exclude))

    return ws.get_many(
        database,
        *filters,
    )


def get_shares_from_production_volume(
    ds_list: Union[Dict[str, Any], List[Dict[str, Any]]],
) -> list:
    """
    Return shares of supply of each dataset in `ds_list`
    based on respective production volumes
    :param ds_list: list of datasets
    :return: dictionary with (dataset name, dataset location, ref prod, unit) as keys, shares as values. Shares total 1.
    """

    if not isinstance(ds_list, list):
        ds_list = [ds_list]

    suppliers = []
    total_production_volume = 0

    for act in ds_list:
        production_volume = 0

        if "production volume" in act:
            production_volume = max(float(act["production volume"]), 1e-9)
        else:
            for exc in ws.production(act):
                # even if non-existent, we set a minimum value of 1e-9
                # because if not, we risk dividing by zero!!!
                production_volume = max(float(exc.get("production volume", 1e-9)), 1e-9)

        suppliers.append(
            {
                "name": act["name"],
                "reference product": act["reference product"],
                "location": act["location"],
                "unit": act["unit"],
                "production volume": production_volume,
            }
        )
        total_production_volume += production_volume

    def nonzero(x):
        return x if x != 0.0 else 1.0

    for supplier in suppliers:
        supplier["share"] = supplier["production volume"] / nonzero(
            total_production_volume
        )

    return suppliers


def get_tuples_from_database(database: List[dict]) -> List[Tuple[str, str, str]]:
    """
    Return a list of tuples (name, reference product, location)
    for each dataset in database.
    :param database: wurst database
    :return: a list of tuples
    """
    return [
        (dataset["name"], dataset["reference product"], dataset["location"])
        for dataset in database
        if "has_downstream_consumer" not in dataset
    ]


def remove_exchanges(datasets_dict: Dict[str, dict], list_exc: List) -> Dict[str, dict]:
    """
    Returns the same `datasets_dict`, where the list of exchanges in these datasets
    has been filtered out: unwanted exchanges has been removed.

    :param datasets_dict: a dictionary with IAM regions as keys, datasets as value
    :param list_exc: list of names (e.g., ["coal", "lignite"]) which are checked against exchanges' names in the dataset
    :return: returns `datasets_dict` without the exchanges whose names check with `list_exc`
    """

    def keep(x):
        return {
            key: value
            for key, value in x.items()
            if not any(ele in x.get("product", []) for ele in list_exc)
        }

    for region in datasets_dict:
        datasets_dict[region]["exchanges"] = [
            keep(exc) for exc in datasets_dict[region]["exchanges"]
        ]

    return datasets_dict


def new_exchange(exc, location, factor):
    copied_exc = deepcopy(exc)
    copied_exc["location"] = location
    return rescale_exchange(copied_exc, factor, remove_uncertainty=False)


def allocate_inputs(exc, lst):
    """
    Allocate the input exchanges in ``lst`` to ``exc``,
    using production volumes where possible, and equal splitting otherwise.
    Always uses equal splitting if ``RoW`` is present.
    """
    pvs = [o.get("production volume", 0) for o in lst]

    if any((x > 0 for x in pvs)):
        # Allocate using production volume
        total = sum(pvs)
    else:
        # Allocate evenly
        total = len(lst)
        pvs = [1 for _ in range(total)]

    if lst[0]["name"] != exc["name"]:
        exc["name"] = lst[0]["name"]

    return (
        [
            new_exchange(exc, obj["location"], factor / total)
            for obj, factor in zip(lst, pvs)
            if factor > 0
        ],
        [p / total for p in pvs if p > 0],
    )


def filter_out_results(
    item_to_look_for: str, results: List[dict], field_to_look_at: str
) -> List[dict]:
    """Filters a list of results by a given field"""
    return [r for r in results if item_to_look_for not in r[field_to_look_at]]


def filter_technosphere_exchanges(exchanges: list):
    return filter(lambda x: x["type"] == "technosphere", exchanges)


def calculate_input_energy(
    fuel_name: str,
    fuel_amount: float,
    fuel_unit: str,
    fuels_specs: dict,
    fuel_map_reverse: dict,
) -> float:
    """
    Returns the amount of energy entering the conversion process, in MJ
    :param fuel_name: name of the liquid, gaseous or solid fuel
    :param fuel_amount: amount of fuel input
    :param fuel_unit: unit of fuel
    :return: amount of fuel energy, in MJ
    """

    def _sanitize_fuel_name(name: str) -> str:
        """Sanitize fuel name by removing market prefixes."""
        items_to_remove = [
            "market for ",
            "market group for ",
            ", high pressure",
            ", low pressure",
            ", used as fuel",
        ]
        for item in items_to_remove:
            name = name.replace(item, "")
        return name

    # if fuel input other than MJ
    if fuel_unit in ["kilogram", "cubic meter"]:
        fuel_name = _sanitize_fuel_name(fuel_name)
        if fuel_name in fuel_map_reverse:
            lhv = fuels_specs[fuel_map_reverse[fuel_name]]["lhv"]["value"]
        elif any(fuel_name.startswith(x) for x in fuels_specs.keys()):
            fuels = [x for x in fuels_specs.keys() if fuel_name.startswith(x)]
            lhv = fuels_specs[fuels[0]]["lhv"]["value"]
        elif any(fuel_name.startswith(x) for x in fuel_map_reverse.keys()):
            fuels = [x for x in fuel_map_reverse.keys() if fuel_name.startswith(x)]
            lhv = fuels_specs[fuel_map_reverse[fuels[0]]]["lhv"]["value"]
        else:
            print(f"Warning: LHV for {fuel_name} not found in fuel specifications.")
            print()
            print(f"Available fuel specs keys: {list(fuels_specs.keys())}.")
            print()
            print(f"Available fuel map reverse keys: {list(fuel_map_reverse.keys())}.")
            print()
            lhv = 0
    elif fuel_unit == "kilowatt hour":
        lhv = 3.6
    else:
        lhv = 1

    # if already in MJ
    return fuel_amount * lhv


def find_fuel_efficiency(
    dataset: dict,
    energy_out: float,
    fuel_specs: dict,
    fuel_map_reverse: dict,
    fuel_filters: List[str] = None,
) -> float:
    """
    This method calculates the efficiency value set initially, in case it is not specified in the parameter
    field of the dataset. In Carma datasets, fuel inputs are expressed in megajoules instead of kilograms.

    :param dataset: a wurst dataset of an electricity-producing technology
    :param fuel_filters: wurst filter to filter fuel input exchanges
    :param energy_out: the amount of energy expect as output, in MJ
    :return: the efficiency value set initially
    """

    def _sanitize_fuel_name(name: str) -> str:
        """Sanitize fuel name by removing market prefixes."""
        items_to_remove = [
            "market for ",
            "market group for ",
            ", high pressure",
            ", low pressure",
            ", used as fuel",
        ]
        for item in items_to_remove:
            name = name.replace(item, "")
        return name

    if fuel_filters is None:
        fuel_filters = list(fuel_map_reverse.keys())
        fuel_filters = [_sanitize_fuel_name(x) for x in fuel_filters]
    else:
        fuel_filters = [_sanitize_fuel_name(x) for x in fuel_filters]

    energy_input = np.sum(
        np.sum(
            np.asarray(
                [
                    calculate_input_energy(
                        exc["name"],
                        exc["amount"],
                        exc["unit"],
                        fuel_specs,
                        fuel_map_reverse,
                    )
                    for exc in dataset["exchanges"]
                    if any(
                        fuel.startswith(_sanitize_fuel_name(exc["name"]))
                        for fuel in fuel_filters
                    )
                    and exc["type"] == "technosphere"
                    and exc["amount"] > 0.0
                ]
            )
        )
    )

    if energy_input == 0:
        # try to see if we find instead direct energy flows in "megajoule" or "kilowatt hour"
        energy_input = np.sum(
            np.asarray(
                [
                    exc["amount"] if exc["unit"] == "megajoule" else exc["amount"] * 3.6
                    for exc in dataset["exchanges"]
                    if exc["type"] == "technosphere"
                    and exc["unit"] in ["megajoule", "kilowatt hour"]
                ]
            )
        )
        if energy_input == 0:
            if not any(x in dataset["name"] for x in ("waste", "treatment")):
                print()
                print(
                    [
                        _sanitize_fuel_name(e["name"])
                        for e in dataset["exchanges"]
                        if e["type"] == "technosphere"
                    ]
                )
                print(
                    f"Warning: {dataset['name'], dataset['location']} has no energy input"
                )
                for e in dataset["exchanges"]:
                    print(e["name"], e["amount"], e["unit"])
                print("----------------------------------------")

                print("fuel filters")
                print(fuel_filters)
                print("----------------------------------------")

    if energy_input != 0 and float(energy_out) != 0:
        current_efficiency = float(energy_out) / energy_input
    else:
        current_efficiency = np.nan

    if current_efficiency in (np.nan, np.inf):
        current_efficiency = 1

    if "parameters" in dataset:
        dataset["parameters"]["efficiency"] = current_efficiency
    else:
        dataset["parameters"] = {"efficiency": current_efficiency}

    return current_efficiency


class BaseTransformation:
    """
    Base transformation class.

    :ivar database: wurst database
    :ivar iam_data: IAMDataCollection object_
    :ivar model: IAM model
    :ivar year: database year
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
    ) -> None:
        self.mapping = None
        self.database: List[dict] = database
        self.iam_data: IAMDataCollection = iam_data
        self.model: str = model
        self.regions: List[str] = iam_data.regions
        self.geo: Geomap = Geomap(model=model)
        self.scenario: str = pathway
        self.year: int = year
        self.version: str = version
        self.fuels_specs: dict = get_fuel_properties()

        self.system_model: str = system_model
        self.cache: dict = cache or {}
        self.ecoinvent_to_iam_loc: Dict[str, str] = {
            loc: self.geo.ecoinvent_to_iam_location(loc)
            for loc in self.get_ecoinvent_locs()
        }
        self.iam_to_ecoinvent_loc = defaultdict(list)
        for key, value in self.ecoinvent_to_iam_loc.items():
            self.iam_to_ecoinvent_loc[value].append(key)

        self.index = index or self.create_index()

    def create_index(self):
        idx = defaultdict(list)
        for ds in self.database:
            key = (copy.deepcopy(ds["name"]), copy.deepcopy(ds["reference product"]))
            idx[key].append(
                {
                    "name": ds["name"],
                    "reference product": ds["reference product"],
                    "location": ds["location"],
                    "unit": ds["unit"],
                    "production volume": list(ws.production(ds))[0].get(
                        "production volume", 0
                    ),
                }
            )
        return idx

    def add_to_index(self, ds: [dict, list, ValuesView]):
        if isinstance(ds, ValuesView):
            ds = list(ds)

        if isinstance(ds, dict):
            ds = [ds]

        for d in ds:
            key = (copy.deepcopy(d["name"]), copy.deepcopy(d["reference product"]))
            self.index[key].append(
                {
                    "name": d["name"],
                    "reference product": d["reference product"],
                    "location": d["location"],
                    "unit": d["unit"],
                    "production volume": list(ws.production(d))[0].get(
                        "production volume", 0
                    ),
                }
            )

    def remove_from_index(self, ds):
        key = (copy.deepcopy(ds["name"]), copy.deepcopy(ds["reference product"]))
        available_locations = [k["location"] for k in self.index[key]]
        if ds["location"] in available_locations:
            ds_to_remove = [
                d for d in self.index[key] if d["location"] == ds["location"]
            ][0]
            self.index[key].remove(ds_to_remove)

    def is_in_index(self, ds, location=None):
        if not any(key in ds for key in ["reference product", "product"]):
            raise KeyError(
                f"Dataset {ds['name']} does not have neither 'reference product' nor 'product' keys."
            )
        if "reference product" in ds:
            key = (ds["name"], ds["reference product"])
        else:
            key = (ds["name"], ds["product"])

        if location is None:
            return ds["location"] in [k["location"] for k in self.index[key]]

        return location in [k["location"] for k in self.index[key]]

    def get_ecoinvent_locs(self) -> List[str]:
        """
        Rerun a list of unique locations in ecoinvent

        :return: list of locations
        :rtype: list
        """

        locs = list(set(a["location"] for a in self.database))

        # add Laos
        if "LA" not in locs:
            locs.append("LA")

        # add Fiji
        if "FJ" not in locs:
            locs.append("FJ")

        # add Guinea
        if "GN" not in locs:
            locs.append("GN")

        # add Guyana
        if "GY" not in locs:
            locs.append("GY")

        # add Sierra Leone
        if "SL" not in locs:
            locs.append("SL")

        # add Solomon Islands
        if "SB" not in locs:
            locs.append("SB")

        # add Uganda
        if "UG" not in locs:
            locs.append("UG")

        # add Afghanistan
        if "AF" not in locs:
            locs.append("AF")

        return locs

    def update_ecoinvent_efficiency_parameter(
        self, dataset: dict, old_ei_eff: float, new_eff: float
    ) -> None:
        """
        Update the old efficiency value in the ecoinvent dataset by the newly calculated one.
        :param dataset: dataset
        :param old_ei_eff: conversion efficiency of the original ecoinvent dataset
        :param new_eff: new conversion efficiency
        :return: nothing. Modifies the `comment` and `parameters` fields of the dataset.
        """
        parameters = dataset["parameters"]
        possibles = ["efficiency", "efficiency_oil_country", "efficiency_electrical"]

        if any(i in dataset for i in possibles):
            for key in possibles:
                if key in parameters:
                    dataset["parameters"][key] = new_eff
        else:
            dataset["parameters"]["efficiency"] = new_eff

        if dataset["location"] in self.regions:
            iam_region = dataset["location"]
        else:
            iam_region = self.ecoinvent_to_iam_loc[dataset["location"]]

        new_txt = (
            f" 'premise' has modified the efficiency of this dataset, from an original "
            f"{int(old_ei_eff * 100)}% to {int(new_eff * 100)}%, according to IAM model {self.model.upper()}, scenario {self.scenario} "
            f"for the region {iam_region}."
        )

        if "comment" in dataset:
            dataset["comment"] += new_txt
        else:
            dataset["comment"] = new_txt

    def get_iam_mapping(
        self, activity_map: dict, fuels_map: dict, technologies: list
    ) -> Dict[str, Any]:
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
                "current_eff_func": find_fuel_efficiency,
                "technology filters": activity_map[tech],
                "fuel filters": fuels_map[tech],
            }
            for tech in technologies
        }

    def get_technology_and_regional_production_shares(
        self, production_volumes: xr.DataArray, mapping: dict
    ) -> (
        tuple[None, dict[tuple[Any, str], float], dict[str, float]]
        | tuple[DataArray, dict[tuple[Any, Any], Any], dict[Any, Any]]
    ):

        regions = [region for region in self.regions if region != "World"]
        year = self.year
        if self.year < production_volumes.year.values.min():
            year = production_volumes.year.values.min()
        if self.year > production_volumes.year.values.max():
            year = production_volumes.year.values.max()

        if not any(v in production_volumes.variables.values for v in mapping.keys()):
            return (
                None,
                {(var, reg): 0.0 for var in mapping.keys() for reg in regions},
                {reg: 1 / len(regions) for reg in regions},
            )

        try:
            variables = [
                v
                for v in list(mapping.keys())
                if v in production_volumes.variables.values
            ]
            if year in production_volumes.year.values:
                production_volumes = production_volumes.sel(
                    variables=variables, region=regions, year=year
                )
            else:
                production_volumes = production_volumes.sel(
                    variables=variables, region=regions
                ).interp(year=year)
        except KeyError:
            raise KeyError(
                f"The variable(s) {[v for v in list(mapping.keys()) if v not in production_volumes.variables.values]} not "
                f"found in production volumes data. "
                f"Available variables: {sorted(list(production_volumes.variables.values))}."
            )
        regional_totals = production_volumes.sum(dim="variables")

        regional_shares = (production_volumes / regional_totals).fillna(0)
        world_shares = (regional_totals / regional_totals.sum()).fillna(0)

        technology_shares_dict = {
            (var, reg): regional_shares.sel(variables=var, region=reg).values.item(0)
            for var in regional_shares.variables.values
            for reg in regional_shares.region.values
        }

        regional_shares_dict = {
            reg: world_shares.sel(region=reg).item()
            for reg in world_shares.region.values
        }

        return production_volumes, technology_shares_dict, regional_shares_dict

    def process_and_add_markets(
        self,
        name,
        reference_product,
        unit,
        mapping,
        production_volumes=None,
        additional_exchanges_fn=None,
        system_model="cut-off",
        blacklist=None,
        conversion_factor=None,
    ):
        """
        Generalized method to create and add regionalized market datasets.

        Parameters
        ----------
        production_datasets : list of dict
            Datasets representing regionalized production activities.
        market_name : str
            Name of the market activity (e.g., "market for steel").
        reference_product : str
            The reference product of the market.
        production_shares : dict
            Dictionary with region as key and production share as value.
        region_list : list of str
            List of regions for which to create markets.
        additional_exchanges_fn : callable, optional
            Function to add extra exchanges to the market dataset (e.g., transport, losses).
        """

        conversion_factor = conversion_factor or {}
        blacklist = blacklist or {}

        if production_volumes is not None:
            regions = [region for region in self.regions if region != "World"]
            production_volumes, technology_shares_dict, regional_shares_dict = (
                self.get_technology_and_regional_production_shares(
                    production_volumes=production_volumes,
                    mapping=mapping,
                )
            )

        else:
            regions = self.regions
            technology_shares_dict = {
                (var, reg): 0.0 for var in mapping.keys() for reg in regions
            }
            regional_shares_dict = {reg: 1 / len(regions) for reg in regions}

        transport_operations = self.extract_market_logistics(
            name=name,
            reference_product=reference_product,
        )

        for region in regions:
            if production_volumes is not None:
                production_volume = float(
                    production_volumes.sel(
                        region=region,
                    )
                    .sum(dim="variables")
                    .values.item(0)
                )

                if production_volume == 0:
                    continue
            else:
                production_volume = 0.0

            production_exchange = {
                "name": name,
                "product": reference_product,
                "location": region,
                "amount": 1.0,
                "unit": unit,
                "uncertainty type": 0,
                "type": "production",
            }

            if production_volumes is not None:
                production_exchange["production volume"] = production_volume

            market_dataset = {
                "name": name,
                "reference product": reference_product,
                "location": region,
                "unit": unit,
                "regionalized": True,
                "code": str(uuid.uuid4().hex),
                "database": "",
                "comment": f"Market dataset for {name} in {region} for {self.year}.",
                "exchanges": [
                    production_exchange,
                ],
            }

            for technology, activities in mapping.items():
                if (technology, region) in technology_shares_dict:
                    suppliers = [ds for ds in activities if ds["location"] == region]
                    if len(suppliers) == 0:
                        suppliers = [
                            ds
                            for ds in activities
                            if ds["location"] in self.iam_to_ecoinvent_loc[region]
                        ]
                    if len(suppliers) == 0:
                        suppliers = [ds for ds in activities if ds["location"] == "RoW"]
                    if len(suppliers) == 0:
                        raise ValueError(
                            f"No activity found for technology {technology} in region {region}. "
                            f"Available activities: {[(a['name'], a['location']) for a in activities]}."
                        )

                    if len(suppliers) > 1:
                        suppliers = get_shares_from_production_volume(suppliers)

                    share = technology_shares_dict.get((technology, region), 0)

                    if share > 0:
                        if technology not in blacklist.get(system_model, []):
                            for supplier in suppliers:
                                market_dataset["exchanges"].append(
                                    {
                                        "name": supplier["name"],
                                        "product": supplier["reference product"],
                                        "location": supplier["location"],
                                        "amount": share
                                        * conversion_factor.get(technology, 1.0)
                                        * supplier.get("share", 1.0),
                                        "unit": supplier["unit"],
                                        "uncertainty type": 0,
                                        "type": "technosphere",
                                    }
                                )

            # normalize the shares
            total_share = sum(
                exc["amount"]
                for exc in market_dataset["exchanges"]
                if exc["type"] == "technosphere"
            )
            if total_share > 0:
                for exc in market_dataset["exchanges"]:
                    if exc["type"] == "technosphere":
                        exc["amount"] /= total_share
            else:
                continue

            if additional_exchanges_fn:
                additional_exchanges_fn(market_dataset)

            # add transport operations
            transport_location = [
                loc
                for loc in transport_operations.keys()
                if loc in self.iam_to_ecoinvent_loc[region]
            ]

            if len(transport_location) == 0:
                # check if RoW is available
                transport_location = [
                    loc for loc in transport_operations.keys() if loc == "RoW"
                ]

            if len(transport_location) == 0:
                # check if GLO is available
                transport_location = [
                    loc for loc in transport_operations.keys() if loc == "GLO"
                ]

            if len(transport_location) > 0:
                transport_location = transport_location[0]
            else:
                transport_location = None

            if transport_location:
                transport_op = transport_operations[transport_location]
                market_dataset["exchanges"].append(
                    {
                        "name": transport_op["name"],
                        "product": transport_op["reference product"],
                        "location": transport_op["location"],
                        "amount": transport_op["amount"],
                        "unit": transport_op["unit"],
                        "uncertainty type": 0,
                        "type": transport_op["type"],
                    }
                )

            self.database.append(market_dataset)
            self.add_to_index(market_dataset)
            self.write_log(market_dataset, "created")

        if "World" not in regions:
            # create the World market
            world_market = {
                "name": name,
                "reference product": reference_product,
                "location": "World",
                "unit": unit,
                "regionalized": True,
                "code": str(uuid.uuid4().hex),
                "database": "",
                "comment": f"Market dataset for {name} in World for {self.year}.",
                "exchanges": [
                    {
                        "name": name,
                        "product": reference_product,
                        "location": "World",
                        "amount": 1.0,
                        "unit": unit,
                        "uncertainty type": 0,
                        "type": "production",
                    }
                ],
            }

            candidate = {
                "name": name,
                "reference product": reference_product,
                "unit": unit,
            }
            for region in regions:
                share = regional_shares_dict.get(region, 0)

                if share > 0:
                    if self.is_in_index(candidate, region):
                        # add the regional market shares
                        world_market["exchanges"].append(
                            {
                                "name": name,
                                "product": reference_product,
                                "location": region,
                                "amount": share,
                                "unit": unit,
                                "uncertainty type": 0,
                                "type": "technosphere",
                            }
                        )

            self.database.append(world_market)
            self.add_to_index(world_market)
            self.write_log(world_market, "created")

        datasets = list(
            ws.get_many(
                self.database,
                ws.equals("name", name),
                ws.equals("reference product", reference_product),
            )
        )
        datasets = [ds for ds in datasets if ds.get("regionalized", False) is False]

        self.empty_original_datasets(
            datasets=datasets,
            loc_map={
                x["location"]: self.geo.ecoinvent_to_iam_location(x["location"])
                for x in datasets
            },
            production_shares=regional_shares_dict,
            regions=regions,
        )

    def extract_market_logistics(
        self,
        name: str,
        reference_product: str,
    ) -> Dict[Tuple[str, str, str], dict]:
        datasets = list(
            ws.get_many(
                self.database,
                ws.equals("name", name),
                ws.equals("reference product", reference_product),
            )
        )

        transport_operations = {}

        for dataset in datasets:
            for exc in ws.technosphere(dataset, ws.contains("unit", "kilometer")):
                transport_operations[dataset["location"]] = {
                    "name": exc["name"],
                    "reference product": exc["product"],
                    "location": exc["location"],
                    "amount": exc["amount"],
                    "unit": exc["unit"],
                    "type": exc["type"],
                }

        return transport_operations

    def process_and_add_activities(
        self,
        mapping,
        production_volumes=None,
        efficiency_adjustment_fn=None,
        regions=None,
        scaling_factors=None,
    ):
        """
        Generalized processing of activities and adding them to the database.

        Parameters
        ----------
        mapping : dict or pd.DataFrame
            Mapping containing the relevant activity data.
        proxy_selection_fn : callable
            Function to fetch proxies based on the mapping.
        efficiency_adjustment_fn : callable, optional
            Function to adjust process efficiency. If None, no adjustment is done.
        scaling_factors : dict, optional
            Dictionary with scaling factors for inputs.
        log_message : str, optional
            Message to log after processing.
        add_to_index : bool, optional
            Whether to add new activities to the index.
        """

        if production_volumes is not None:
            regions = regions or [
                region for region in self.regions if region != "World"
            ]
            production_volumes, _, regional_shares_dict = (
                self.get_technology_and_regional_production_shares(
                    production_volumes=production_volumes,
                    mapping=mapping,
                )
            )

        else:
            regions = regions or self.regions
            regional_shares_dict = {reg: 1 / len(regions) for reg in regions}

        processed_datasets, seen_datasets = [], []

        # resize production volumes to the keys available in mapping
        if production_volumes is not None:
            production_volumes = production_volumes.sel(
                variables=[
                    v
                    for v in list(mapping.keys())
                    if v in production_volumes.variables.values
                ]
            )

        for technology, grouped_activities in mapping.items():
            grouped_activities = [
                ds for ds in grouped_activities if ds["name"] not in seen_datasets
            ]

            if not grouped_activities:
                continue

            grouped_activities = group_dicts_by_keys(
                grouped_activities, ["name", "reference product"]
            )

            for activities in grouped_activities:
                if not activities:
                    continue

                if any(ds for ds in activities if ds.get("regionalized", False)):
                    # if any of the datasets in the activity is already regionalized, skip it
                    mapping[technology].extend(
                        [ds for ds in activities if ds.get("regionalized", True)]
                    )
                    continue

                prod_vol = None
                if production_volumes is not None:
                    if technology in production_volumes.coords["variables"].values:
                        prod_vol = production_volumes.sel(variables=technology)

                regionalized_datasets = self.fetch_proxies(
                    datasets=activities,
                    production_volumes=prod_vol,
                )

                # adjust efficiency of steel production
                if efficiency_adjustment_fn:
                    for dataset in regionalized_datasets.values():
                        if isinstance(efficiency_adjustment_fn, list):
                            for fn in efficiency_adjustment_fn:
                                fn(dataset, technology)
                        else:
                            efficiency_adjustment_fn(dataset, technology)

                processed_datasets.extend(regionalized_datasets.values())
                seen_datasets.extend([ds["name"] for ds in activities])
                mapping[technology].extend(regionalized_datasets.values())

                datasets = list(
                    ws.get_many(
                        self.database,
                        ws.equals("name", activities[0]["name"]),
                        ws.equals(
                            "reference product", activities[0]["reference product"]
                        ),
                    )
                )
                datasets = [
                    ds for ds in datasets if ds.get("regionalized", False) is False
                ]

                self.empty_original_datasets(
                    datasets=datasets,
                    loc_map={
                        x["location"]: self.geo.ecoinvent_to_iam_location(x["location"])
                        for x in datasets
                    },
                    production_shares=regional_shares_dict,
                    regions=regions,
                )

        for dataset in processed_datasets:
            self.add_to_index(dataset)
            self.write_log(dataset, "created")
            self.database.append(dataset)

    def region_to_proxy_dataset_mapping(
        self, datasets: list[dict], regions: List[str] = None
    ) -> Dict[str, dict]:
        d_map = {
            self.ecoinvent_to_iam_loc[d["location"]]: d
            for d in datasets
            if d["location"] not in self.regions
        }

        if not regions:
            regions = self.regions

        locs = {x["location"]: x for x in datasets}

        if "RoW" in locs:
            fallback_dataset = locs["RoW"]
        else:
            if "GLO" in locs:
                fallback_dataset = locs["GLO"]
            else:
                fallback_dataset = list(locs.values())[0]

        return {region: d_map.get(region, fallback_dataset) for region in regions}

    def fetch_proxies(
        self,
        datasets: List[dict],
        production_volumes: xr.DataArray = None,
        relink=True,
        regions=None,
        geo_mapping: dict = None,
        delete_original_datasets=False,
        unlist=True,
    ) -> Dict[str, dict]:
        """
        Fetch dataset proxies, given a dataset `name` and `reference product`.
        Store a copy for each IAM region.
        If a fitting ecoinvent location cannot be found for a given IAM region,
        fetch a dataset with a "RoW" location.
        Delete original datasets from the database.

        :param production_variable: name of variable in IAM data that refers to production volume
        :param relink: if `relink`, exchanges from the datasets will be relinked to
        the most geographically-appropriate providers from the database. This is computer-intensive.
        :param regions: regions to create proxy datasets for. if None, all regions are considered.
        :param delete_original_datasets: if True, delete original datasets from the database.
        :param empty_original_activity: if True, empty original activities from exchanges.
        :param unlist: if True, remove original datasets from the index.
        :return: dictionary with IAM regions as keys, proxy datasets as values.
        """

        if not isinstance(datasets, list):
            datasets = [datasets]

        d_iam_to_eco = geo_mapping or self.region_to_proxy_dataset_mapping(
            datasets=datasets, regions=regions
        )

        d_act = {}

        for region, dataset in d_iam_to_eco.items():

            if self.is_in_index(dataset, region):
                # delete original dataset from the database
                self.database = [
                    d
                    for d in self.database
                    if (d["name"], d["reference product"], d["location"])
                    != (dataset["name"], dataset["reference product"], region)
                ]

            dataset = copy.deepcopy(dataset)
            dataset["location"] = region
            dataset["code"] = str(uuid.uuid4().hex)
            dataset["regionalized"] = True

            for exc in ws.production(dataset):
                if "input" in exc:
                    del exc["input"]
                if "location" in exc:
                    exc["location"] = region

            if "input" in dataset:
                del dataset["input"]

            for prod in ws.production(dataset):
                prod["location"] = region
                if production_volumes is not None:
                    # Add `production volume` field
                    if region in production_volumes.region.values:
                        prod["production volume"] = float(
                            production_volumes.sel(region=prod["location"]).values.item(
                                0
                            )
                        )
                    else:
                        if region == "World":
                            # If the region is "World", use the total production volume
                            prod["production volume"] = float(
                                production_volumes.sum(dim="region").values.item(0)
                            )
                        else:
                            raise KeyError(
                                f"Region {region} not found in production volumes data."
                            )
                else:
                    prod["production volume"] = 0.0

            if relink:
                d_act[region] = self.relink_technosphere_exchanges(dataset)

        if unlist:
            for dataset in datasets:
                self.remove_from_index(dataset)

        if delete_original_datasets is True:
            # remove the dataset from `self.database`
            self.database = [ds for ds in self.database if ds not in datasets]

        return d_act

    def empty_original_datasets(
        self,
        datasets: list[dict],
        production_shares: dict,
        loc_map: Dict[str, str],
        regions: List[str] = None,
    ) -> None:
        """
        Empty original ecoinvent datasets and replace them with IAM-based inputs.
        """
        regions = regions or self.regions

        def build_exchange(dataset, location, amount):
            return {
                "name": dataset["name"],
                "product": dataset["reference product"],
                "amount": amount,
                "unit": dataset["unit"],
                "uncertainty type": 0,
                "location": location,
                "type": "technosphere",
            }

        for dataset in datasets:
            if dataset.get("regionalized", False) is True:
                print(
                    f"Skipping {dataset['name']} in {dataset['location']}, already regionalized."
                )
                continue

            ecoinvent_location = dataset["location"]
            iam_location = loc_map[ecoinvent_location]

            if not self.is_in_index(dataset, iam_location):
                iam_location = "World"

            if iam_location == "World":
                iam_location = [
                    r for r in regions if r != "World" and self.is_in_index(dataset, r)
                ]

            if not iam_location:
                continue

            if isinstance(iam_location, str):
                iam_location = [iam_location]

            # Clean dataset
            dataset["has_downstream_consumer"] = False
            dataset["exchanges"] = [
                e for e in dataset["exchanges"] if e["type"] == "production"
            ]

            # Empty production volume
            for prod in ws.production(dataset):
                prod["production volume"] = 0.0

            dataset["emptied"] = True
            dataset.pop("adjust efficiency", None)

            if not dataset["exchanges"]:
                print(
                    f"ISSUE: no exchanges found in {dataset['name']} in {ecoinvent_location}"
                )

            # Add new exchanges
            if len(iam_location) == 1:
                dataset["exchanges"].append(
                    build_exchange(dataset, iam_location[0], 1.0)
                )

            else:
                dataset["exchanges"].extend(
                    [
                        build_exchange(dataset, loc, production_shares.get(loc, 0))
                        for loc in iam_location
                    ]
                )

            self.write_log(dataset=dataset, status="empty")
            self.remove_from_index(dataset)

    def relink_datasets(self, excludes_datasets=None, alt_names=None):
        """
        For a given exchange name, product, and unit, change its location to an IAM location,
        to effectively link to the newly built market(s)/activity(ies).
        :param excludes_datasets: list of datasets to exclude from relinking
        :param alt_names: list of alternative names to use for relinking
        """

        # Simplify default arguments
        alt_names = alt_names or []
        excludes_datasets = excludes_datasets or []

        for act in ws.get_many(
            self.database, ws.doesnt_contain_any("name", excludes_datasets)
        ):
            # Filter out exchanges to relink
            excs_to_relink = [
                e
                for e in ws.technosphere(act)
                if (not self.is_in_index(e) and e["amount"] != 0)
            ]

            if len(excs_to_relink) == 0:
                continue

            old_uncertainty = {}

            for exc in excs_to_relink:
                if exc["type"] == "technosphere":
                    if exc.get("uncertainty type", 0) != 0:
                        old_uncertainty[
                            (exc["name"], exc.get("product"), exc["unit"])
                        ] = {
                            "uncertainty type": exc.get("uncertainty type", 0),
                            "amount": exc["amount"],
                            "loc": exc.get("loc"),
                            "scale": exc.get("scale"),
                            "minimum": exc.get("minimum", 0),
                            "maximum": exc.get("maximum", 0),
                        }

            # make a dictionary with the names and amounts
            # of the technosphere exchanges to relink
            # to compare with the new exchanges
            excs_to_relink_dict = defaultdict(float)
            for exc in excs_to_relink:
                try:
                    excs_to_relink_dict[exc["product"]] += exc["amount"]
                except:
                    print(exc)
                    raise

            # Create a set of unique exchanges to relink
            # turn this into a list of dictionaries
            unique_excs_to_relink = [
                {
                    "name": exc["name"],
                    "product": exc["product"],
                    "location": exc["location"],
                    "unit": exc["unit"],
                }
                for exc in excs_to_relink
            ]
            # remove duplicates items in the list
            unique_excs_to_relink = [
                dict(t) for t in {tuple(d.items()) for d in unique_excs_to_relink}
            ]

            # Process exchanges to relink
            new_exchanges = self.process_exchanges_to_relink(
                act, unique_excs_to_relink, alt_names
            )

            # apply uncertainties, if any
            if old_uncertainty:
                for exc in new_exchanges:
                    key = (exc["name"], exc["product"], exc["unit"])
                    if key in old_uncertainty:
                        exc["uncertainty type"] = old_uncertainty[key][
                            "uncertainty type"
                        ]
                        loc, scale, minimum, maximum, negative = (
                            redefine_uncertainty_params(old_uncertainty[key], exc)
                        )

                        if loc:
                            exc["loc"] = float(loc)

                        if scale:
                            exc["scale"] = float(scale)

                        if minimum:
                            exc["minimum"] = float(minimum)

                        if maximum:
                            exc["maximum"] = float(maximum)

                        if negative:
                            exc["negative"] = float(negative)

            # Update act["exchanges"] by removing the exchanges to relink
            act["exchanges"] = [e for e in act["exchanges"] if e not in excs_to_relink]
            # Update act["exchanges"] by adding new exchanges
            act["exchanges"].extend(new_exchanges)

            new_exchanges_dict = defaultdict(float)
            for exc in new_exchanges:
                new_exchanges_dict[exc["product"]] += exc["amount"]

            # compare with the original exchanges
            # if the amount is different, add a log
            for key in excs_to_relink_dict:
                assert (
                    key in new_exchanges_dict
                ), f"{key} not in {new_exchanges_dict} in dataset {act['name']}, {act['location']}"
                assert np.isclose(
                    excs_to_relink_dict[key],
                    new_exchanges_dict[key],
                    rtol=0.001,
                ), (
                    f"{excs_to_relink_dict[key]} != {new_exchanges_dict[key]} in dataset {act['name']}, {act['location']}."
                    f" Exchanges to relink: {excs_to_relink_dict}, new exchanges: {new_exchanges_dict}"
                )

    def process_exchanges_to_relink(self, act, unique_excs_to_relink, alt_names):
        new_exchanges = []
        for exc in unique_excs_to_relink:
            entries, amount = self.find_new_exchange_entries(act, exc, alt_names)
            if amount != 0:
                new_exchanges.extend(self.create_new_exchanges(entries, amount))
        # Make exchanges unique and sum amounts for duplicates
        return self.summarize_exchanges(new_exchanges)

    def get_exchange_from_cache(self, exc, loc):
        key = (
            exc["name"],
            exc["product"],
            exc["location"],
            exc["unit"],
        )

        return self.cache.get(loc, {}).get(self.model, {}).get(key)

    def find_alternative_locations(self, act, exc, alt_names):
        """
        Find alternative locations for an exchange, trying "market for" and "market group for"
        only if the initial search is unsuccessful.

        :param act: The activity dictionary.
        :param exc: A tuple representing the exchange (name, product, location, unit).
        :param alt_names: A list of alternative names to use for relinking.
        :return: A list of new exchange entries or an empty list if none are found.
        """
        names_to_look_for = [exc["name"]] + alt_names

        def allocate_exchanges(lst):
            """
            Allocate the input exchanges in ``lst`` to ``exc``,
            using production volumes where possible, and equal splitting otherwise.
            Always uses equal splitting if ``RoW`` is present.
            """

            pvs = []
            for o in lst:
                try:
                    ds = ws.get_one(
                        self.database,
                        ws.equals("name", o[0]),
                        ws.equals("reference product", o[1]),
                        ws.equals("location", o[2]),
                    )

                except ws.NoResults:
                    raise ws.NoResults(
                        f"Can't find {o[0]} {o[1]} {o[2]} in the database"
                    )

                for exc in ds["exchanges"]:
                    if exc["type"] == "production":
                        pvs.append(exc.get("production volume", 0))

            if any((x > 0 for x in pvs)):
                # Allocate using production volume
                total = sum(pvs)
            else:
                # Allocate evenly
                total = len(lst)
                pvs = [1 for _ in range(total)]

            return [p / total for p in pvs]

        # Function to search for new exchanges
        def search_for_new_exchanges(names):
            entries = []
            for name_to_look_for, alt_loc in product(
                set(names), set(alternative_locations)
            ):
                if (name_to_look_for, alt_loc) != (act["name"], act["location"]):
                    if self.is_in_index(
                        {
                            "name": name_to_look_for,
                            "product": exc["product"],
                            "location": alt_loc,
                            "unit": exc["unit"],
                        }
                    ):
                        entries.append(
                            (
                                name_to_look_for,
                                exc["product"],
                                alt_loc,
                                exc["unit"],
                                1.0,
                            )
                        )

            if len(entries) > 1 and any(
                x in ["World", "GLO", "RoW"] for x in [e[2] for e in entries]
            ):
                entries = [e for e in entries if e[2] not in ["World", "GLO", "RoW"]]

            if len(entries) > 1:
                shares = allocate_exchanges(entries)
                entries = [(e[0], e[1], e[2], e[3], s) for e, s in zip(entries, shares)]
                # remove entries that have a share of 0
                entries = [e for e in entries if e[-1] > 0]

            return entries

        # Start with the activity's location
        alternative_locations = [
            act["location"],
        ]

        # Add alternative locations based on mapping
        if act["location"] in self.ecoinvent_to_iam_loc:
            alternative_locations.append(self.ecoinvent_to_iam_loc[act["location"]])

        # Always include 'RoW', 'World', and 'GLO' as last resort options
        alternative_locations.extend(["RoW", "World", "GLO"])

        # Initial search with the provided names
        new_entries = search_for_new_exchanges(names_to_look_for)
        # check if the location of one of the entries matches with
        # the location of the activity to relink

        if len(new_entries) > 1:
            if any(e[2] == act["location"] for e in new_entries):
                new_entries = [e for e in new_entries if e[2] == act["location"]]
                # re-normalize the shares
                sum_shares = sum(e[-1] for e in new_entries)
                new_entries = [
                    (e[0], e[1], e[2], e[3], e[-1] / sum_shares) for e in new_entries
                ]

        if new_entries:
            return new_entries

        # If initial search fails, try with "market for" and "market group for"
        for prefix in ["market for", "market group for"]:
            if exc["name"].startswith(prefix):
                modified_name = exc["name"].replace(
                    prefix,
                    (
                        "market for"
                        if prefix == "market group for"
                        else "market group for"
                    ),
                )
                names_to_look_for.append(modified_name)

        # Second search with modified names
        return search_for_new_exchanges(names_to_look_for)

    def find_new_exchange_entries(self, act, exc, alt_names):
        entries = None

        if self.is_exchange_in_cache(exc, act["location"]):
            entries = self.get_exchange_from_cache(exc, act["location"])

        if not entries:
            entries = self.find_alternative_locations(act, exc, alt_names)

        if not entries:
            entries = [
                (exc["name"], exc["product"], exc["location"], exc["unit"]) + (1.0,)
            ]

        amount = sum(
            e["amount"]
            for e in ws.technosphere(act)
            if (e["name"], e["product"], e["location"], e["unit"])
            == (exc["name"], exc["product"], exc["location"], exc["unit"])
        )

        return entries, amount

    def create_new_exchanges(self, entries, amount):
        return [
            {
                "name": e[0],
                "product": e[1],
                "amount": amount * e[-1],
                "type": "technosphere",
                "unit": e[3],
                "location": e[2],
            }
            for e in entries
        ]

    def summarize_exchanges(self, new_exchanges):
        grouped_exchanges = groupby(
            sorted(
                new_exchanges, key=itemgetter("name", "product", "location", "unit")
            ),
            key=itemgetter("name", "product", "location", "unit"),
        )
        return [
            {
                "name": name,
                "product": prod,
                "location": loc,
                "unit": unit,
                "type": "technosphere",
                "amount": sum(e["amount"] for e in excs),
            }
            for (name, prod, loc, unit), excs in grouped_exchanges
        ]

    def find_iam_efficiency_change(
        self,
        data: xr.DataArray,
        variable: Union[str, list],
        location: str,
    ) -> float:
        """
        Return the relative change in efficiency for `variable` in `location`
        relative to 2020.
        :param variable: IAM variable name
        :param location: IAM region
        :return: relative efficiency change (e.g., 1.05)
        """

        if self.year in data.coords["year"].values:
            scaling_factor = data.sel(
                region=location, variables=variable, year=self.year
            ).values.item(0)
        else:
            scaling_factor = (
                data.sel(region=location, variables=variable)
                .interp(year=self.year)
                .values.item(0)
            )

        if scaling_factor in (np.nan, np.inf):
            scaling_factor = 1

        return scaling_factor

    def write_log(self, dataset, status="created"):
        """
        Write log file.
        """

        logger.info(
            f"{status}|{self.model}|{self.scenario}|{self.year}|"
            f"{dataset['name']}|{dataset['location']}|"
        )

    def add_new_entry_to_cache(
        self,
        location: str,
        exchange: dict,
        allocated: List[dict],
        shares: List[float],
    ) -> None:
        """
        Add an entry to the cache.
        :param location: The location to which the cache entry corresponds.
        :param exchange: The exchange dictionary containing the data to cache.
        :param allocated: A list of dictionaries containing allocated exchanges.
        :param shares: A list of floats representing the shares for each allocated exchange.
        """
        # Ensure 'product' key is present in exchange.
        exchange.setdefault("product", exchange.get("reference product"))

        # Create a key for the cache entry.
        exc_key = (
            exchange["name"],
            exchange["product"],
            exchange["location"],
            exchange["unit"],
        )

        # Create the cache entry.
        entry = [
            (
                e.get("name", e.get("reference product")),
                e.get("product", e.get("reference product")),
                e["location"],
                e["unit"],
                s,
            )
            for e, s in zip(allocated, shares)
        ]

        # Initialize cache dictionary levels with setdefault.
        location_cache = self.cache.setdefault(location, {})
        model_cache = location_cache.setdefault(self.model, {})

        # Add the new entry to the cache.
        model_cache[exc_key] = entry

    def is_exchange_in_cache(self, exchange: dict, dataset_location: str) -> bool:
        """
        Check if an exchange is in the cache.
        :param exchange: The exchange dictionary to check.
        :param dataset_location: The location of the dataset.
        :return: True if the exchange is in the cache, False otherwise.

        """
        return (
            exchange["name"],
            exchange["product"],
            exchange["location"],
            exchange["unit"],
        ) in self.cache.get(dataset_location, {}).get(self.model, {})

    def process_cached_exchange(
        self, exchange: dict, dataset: dict, new_exchanges: list
    ) -> None:
        """
        Process a cached exchange. Adds the new exchanges to the list of new exchanges.
        :param exchange: The exchange dictionary to process.
        :param dataset: The dataset dictionary.
        :param new_exchanges: The list of new exchanges to add to the dataset.

        """
        exchanges = self.cache[dataset["location"]][self.model][
            (
                exchange["name"],
                exchange["product"],
                exchange["location"],
                exchange["unit"],
            )
        ]

        if isinstance(exchanges, tuple):
            exchanges = [exchanges]

        _ = lambda x: 0 if x is None else x

        for i, e in enumerate(exchanges):

            new_exc = {
                "name": e[0],
                "product": e[1],
                "unit": exchange["unit"],
                "location": e[2],
                "type": "technosphere",
                "amount": exchange["amount"] * e[-1],
                "uncertainty type": exchange.get("uncertainty type", 0),
            }

            for key in ["loc", "scale", "negative", "minimum", "maximum"]:
                if key in exchange:
                    if isinstance(exchange[key], float):
                        new_exc[key] = exchange[key]

            new_exchanges.append(new_exc)

    def process_uncached_exchange(
        self,
        exchange: dict,
        dataset: dict,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ):
        """
        Process an uncached exchange. Adds the new exchanges to the list of new exchanges.
        :param exchange: The exchange dictionary to process.
        :param dataset: The dataset dictionary.
        :param new_exchanges: The list of new exchanges to add to the dataset.
        """

        # This function needs to handle the logic when
        # an exchange is not in the cache.
        key = (exchange["name"], exchange["product"])
        possible_datasets = self.index[key]

        if len(possible_datasets) == 0:
            if "market for" in exchange["name"]:
                key = (
                    exchange["name"].replace("market for", "market group for"),
                    exchange["product"],
                )
                possible_datasets = self.index[key]

        if len(possible_datasets) == 0:
            # search self.database for possible datasets
            possible_datasets = [
                ds
                for ds in self.database
                if ds["name"] == exchange["name"]
                and ds["reference product"] == exchange["product"]
            ]

            if len(possible_datasets) > 0:
                # repopulate self.index
                for ds in possible_datasets:
                    self.add_to_index(ds)

        if len(possible_datasets) == 0:
            print(
                f"No possible datasets found for {key} in {dataset['name']} {dataset['location']}"
            )

            exc = {
                "name": exchange["name"],
                "product": exchange["product"],
                "unit": exchange["unit"],
                "location": dataset["location"],
                "type": "technosphere",
                "amount": exchange["amount"],
                "uncertainty type": exchange.get("uncertainty type", 0),
            }

            for key in ["loc", "scale", "negative", "minimum", "maximum"]:
                if key in exchange:
                    exc[key] = exchange[key]

            return [exc]

        if len(possible_datasets) == 1:
            self.handle_single_possible_dataset(
                exchange, possible_datasets, new_exchanges
            )

        else:
            self.handle_multiple_possible_datasets(
                exchange,
                dataset,
                possible_datasets,
                new_exchanges,
                exclusive,
                biggest_first,
                contained,
            )

    def handle_single_possible_dataset(
        self, exchange, possible_datasets, new_exchanges
    ):
        # If there's only one possible dataset, we can just use it
        single_dataset = possible_datasets[0]

        assert (
            single_dataset.get("reference product") == exchange["product"]
        ), f"Candidate: {single_dataset}, exchange: {exchange}"

        new_exc = exchange.copy()
        new_exc["location"] = single_dataset["location"]
        new_exc["name"] = single_dataset["name"]
        new_exc["product"] = single_dataset["reference product"]

        new_exchanges.append(new_exc)

    def new_exchange(self, exchange, location, amount_multiplier):
        # Create a new exchange dictionary with the modified location and amount

        exc = {
            "name": exchange["name"],
            "product": exchange["product"],
            "unit": exchange["unit"],
            "location": location,
            "type": "technosphere",
            "amount": exchange["amount"] * amount_multiplier,
            "uncertainty type": exchange.get("uncertainty type", 0),
        }

        for key in ["loc", "scale", "negative", "minimum", "maximum"]:
            if key in exchange:
                exc[key] = exchange[key]

        return exc

    def handle_multiple_possible_datasets(
        self,
        exchange: dict,
        dataset: dict,
        possible_datasets: list,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ) -> None:
        # First, check if the dataset location itself is a possible match
        if dataset["location"] in [ds["location"] for ds in possible_datasets]:
            candidate = [
                ds for ds in possible_datasets if ds["location"] == dataset["location"]
            ][0]

            new_exc = exchange.copy()
            new_exc["location"] = candidate["location"]
            new_exc["name"] = candidate["name"]
            new_exc["product"] = candidate["reference product"]

            self.add_new_entry_to_cache(
                dataset["location"],
                exchange,
                [new_exc],
                [1.0],
            )

            new_exchanges.append(new_exc)
        else:
            # If more complex GIS matching or allocation is required,
            # we delegate to another function
            self.process_complex_matching_and_allocation(
                exchange,
                dataset,
                possible_datasets,
                new_exchanges,
                exclusive,
                biggest_first,
                contained,
            )

    def process_complex_matching_and_allocation(
        self,
        exchange: dict,
        dataset: dict,
        possible_datasets: list,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ) -> None:
        # Check if the location of the dataset is within IAM regions
        if dataset["location"] in self.geo.iam_regions:
            self.handle_iam_region(exchange, dataset, possible_datasets, new_exchanges)

        elif dataset["location"] in ["GLO", "RoW", "World"]:
            # Handle global or rest-of-world scenarios
            self.handle_global_and_row_scenarios(
                exchange, dataset, possible_datasets, new_exchanges
            )

        else:
            # After the above checks, perform GIS matching if necessary
            self.perform_gis_matching(
                exchange,
                dataset,
                possible_datasets,
                new_exchanges,
                exclusive,
                biggest_first,
                contained,
            )

        # If there's still no match found, consider the default option
        self.handle_default_option(exchange, dataset, new_exchanges, possible_datasets)

    def handle_iam_region(self, exchange, dataset, possible_datasets, new_exchanges):
        # In IAM regions, we need to look for possible local datasets
        locs = [
            iloc
            for iloc in self.iam_to_ecoinvent_loc[dataset["location"]]
            if iloc in [ds["location"] for ds in possible_datasets]
        ]

        if locs:
            kept = [ds for ds in possible_datasets if ds["location"] in locs]
            if dataset["location"] == "World" and "GLO" in locs:
                kept = [ds for ds in kept if ds["location"] == "GLO"]

            allocated, share = allocate_inputs(exchange, kept)

            new_exchanges.extend(allocated)
            self.add_new_entry_to_cache(dataset["location"], exchange, allocated, share)

    def handle_global_and_row_scenarios(
        self, exchange, dataset, possible_datasets, new_exchanges
    ):
        # Handle scenarios where the location is 'GLO' or 'RoW'
        possible_locations = [ds["location"] for ds in possible_datasets]
        if any(loc in possible_locations for loc in ["GLO", "RoW", "World"]):
            kept = [
                ds
                for ds in possible_datasets
                if ds["location"] in ["GLO", "RoW", "World"]
            ]
            allocated, share = allocate_inputs(exchange, kept)
            new_exchanges.extend(allocated)
            self.add_new_entry_to_cache(dataset["location"], exchange, allocated, share)

    def perform_gis_matching(
        self,
        exchange: dict,
        dataset: dict,
        possible_datasets: list,
        new_exchanges: list,
        exclusive: bool,
        biggest_first: bool,
        contained: bool,
    ) -> None:
        """
        Perform GIS matching for a dataset with a non-IAM location.

        :param exchange: The exchange dictionary to process.
        :param dataset: The dataset dictionary.
        :param possible_datasets: The list of possible datasets.
        :param new_exchanges: The list of new exchanges to add to the dataset.
        :param exclusive: Bool, default is ``True``. Don't allow overlapping locations in input providers.
        :param biggest_first: Bool, default is ``False``. Determines search order when selecting provider locations. Only relevant if ``exclusive`` is ``True``.
        :param contained: Bool, default is ``True``. If true, only use providers whose location is completely within the ``dataset`` location; otherwise use all intersecting locations.

        """
        # Perform GIS-based matching for location
        location = dataset["location"]
        # if regions contained in posisble location
        # we need to turn them into tuples (model, region)
        possible_locations = tuple([ds["location"] for ds in possible_datasets])

        gis_match = self.get_gis_match(
            location,
            possible_locations,
            contained,
            exclusive,
            biggest_first,
        )

        kept = [
            ds for loc in gis_match for ds in possible_datasets if ds["location"] == loc
        ]

        if kept:
            allocated, share = allocate_inputs(exchange, kept)
            new_exchanges.extend(allocated)
            self.add_new_entry_to_cache(dataset["location"], exchange, allocated, share)

    def handle_default_option(
        self, exchange, dataset, new_exchanges, possible_datasets
    ):
        new_exc = None
        # Handle the default case where no better candidate is found
        if not self.is_exchange_in_cache(exchange, dataset["location"]):
            for default_location in ["RoW", "GLO", "World"]:
                if default_location in [x["location"] for x in possible_datasets]:
                    default_dataset = [
                        x
                        for x in possible_datasets
                        if x["location"] == default_location
                    ][0]

                    new_exc = exchange.copy()
                    new_exc["name"] = default_dataset["name"]
                    new_exc["product"] = default_dataset["reference product"]
                    new_exc["location"] = default_dataset["location"]
                    new_exchanges.append(new_exc)

                    break

        if new_exc is None and not self.is_exchange_in_cache(
            exchange, dataset["location"]
        ):
            new_exchanges.append(exchange)

    def find_candidates(
        self,
        dataset: dict,
        exclusive=True,
        biggest_first=False,
        contained=False,
    ):
        new_exchanges = []

        for exchange in filter_technosphere_exchanges(dataset["exchanges"]):
            if self.is_exchange_in_cache(exchange, dataset["location"]):
                self.process_cached_exchange(exchange, dataset, new_exchanges)
            else:
                self.process_uncached_exchange(
                    exchange,
                    dataset,
                    new_exchanges,
                    exclusive,
                    biggest_first,
                    contained,
                )

        return new_exchanges

    def relink_technosphere_exchanges(
        self,
        dataset,
        exclusive=True,
        biggest_first=False,
        contained=False,
    ) -> dict:
        """Find new technosphere providers based on the location of the dataset.
        Designed to be used when the dataset's location changes, or when new datasets are added.
        Uses the name, reference product, and unit of the exchange to filter possible inputs. These must match exactly. Searches in the list of datasets ``data``.
        Will only search for providers contained within the location of ``dataset``, unless ``contained`` is set to ``False``, all providers whose location intersects the location of ``dataset`` will be used.
        A ``RoW`` provider will be added if there is a single topological face in the location of ``dataset`` which isn't covered by the location of any providing activity.
        If no providers can be found, `relink_technosphere_exchanes` will try to add a `RoW` or `GLO` providers, in that order, if available. If there are still no valid providers, a ``InvalidLink`` exception is raised, unless ``drop_invalid`` is ``True``, in which case the exchange will be deleted.
        Allocation between providers is done using ``allocate_inputs``; results seem strange if ``contained=False``, as production volumes for large regions would be used as allocation factors.
        Input arguments:
            * ``dataset``: The dataset whose technosphere exchanges will be modified.
            * ``data``: The list of datasets to search for technosphere product providers.
            * ``model``: The IAM model
            * ``exclusive``: Bool, default is ``True``. Don't allow overlapping locations in input providers.
            * ``drop_invalid``: Bool, default is ``False``. Delete exchanges for which no valid provider is available.
            * ``biggest_first``: Bool, default is ``False``. Determines search order when selecting provider locations. Only relevant if ``exclusive`` is ``True``.
            * ``contained``: Bool, default is ``True``. If true, only use providers whose location is completely within the ``dataset`` location; otherwise use all intersecting locations.
            * ``iam_regions``: List, lists IAM regions, if additional ones need to be defined.
        Modifies the dataset in place; returns the modified dataset."""

        sum_before = sum(exc["amount"] for exc in dataset["exchanges"])

        # collect the name of exchange and the sum of amounts
        # as a dictionary, for all technosphere exchanges
        # in the dataset

        exchanges_before = defaultdict(float)
        for exc in dataset["exchanges"]:
            if exc["type"] == "technosphere":
                exchanges_before[exc["product"]] += exc["amount"]

        old_uncertainty = {}

        for exc in dataset["exchanges"]:
            if exc["type"] == "technosphere":
                if exc.get("uncertainty type", 0) != 0:
                    old_uncertainty[(exc["name"], exc.get("product"), exc["unit"])] = {
                        "uncertainty type": exc.get("uncertainty type", 0),
                        "amount": exc["amount"],
                    }

                    for key in ["loc", "scale", "negative", "minimum", "maximum"]:
                        if key in exc:
                            old_uncertainty[
                                (exc["name"], exc.get("product"), exc["unit"])
                            ][key] = exc[key]

        new_exchanges = self.find_candidates(
            dataset,
            exclusive=exclusive,
            biggest_first=biggest_first,
            contained=contained,
        )

        # make unique list of exchanges from new_exchanges
        # and sum the amounts of exchanges with the same name,
        # product, location and unit

        new_exchanges = [
            {
                "name": name,
                "product": prod,
                "location": location,
                "unit": unit,
                "type": "technosphere",
                "amount": sum(exc["amount"] for exc in exchanges),
            }
            for (
                name,
                prod,
                location,
                unit,
            ), exchanges in groupby(
                sorted(
                    new_exchanges,
                    key=itemgetter(
                        "name",
                        "product",
                        "location",
                        "unit",
                    ),
                ),
                key=itemgetter(
                    "name",
                    "product",
                    "location",
                    "unit",
                ),
            )
        ]

        # apply uncertainties, if any
        if old_uncertainty:
            for exc in new_exchanges:
                key = (exc["name"], exc["product"], exc["unit"])
                if key in old_uncertainty:
                    exc["uncertainty type"] = old_uncertainty[key]["uncertainty type"]
                    loc, scale, minimum, maximum, negative = (
                        redefine_uncertainty_params(old_uncertainty[key], exc)
                    )

                    if loc:
                        exc["loc"] = float(loc)

                    if scale:
                        exc["scale"] = float(scale)

                    if negative:
                        exc["negative"] = float(negative)

                    if minimum:
                        exc["minimum"] = float(minimum)

                    if maximum:
                        exc["maximum"] = float(maximum)

        dataset["exchanges"] = [
            exc for exc in dataset["exchanges"] if exc["type"] != "technosphere"
        ] + new_exchanges

        sum_after = sum(exc["amount"] for exc in dataset["exchanges"])

        assert np.allclose(sum_before, sum_after, rtol=1e-3), (
            f"Sum of exchanges before and after relinking is not the same: {sum_before} != {sum_after}"
            f"\n{dataset['name']}|{dataset['location']}"
        )

        # compare new exchanges with exchanges before
        exchanges_after = defaultdict(float)
        for exc in dataset["exchanges"]:
            if exc["type"] == "technosphere":
                exchanges_after[exc["product"]] += exc["amount"]

        assert set(exchanges_before.keys()) == set(exchanges_after.keys()), (
            f"Exchanges before and after relinking are not the same: {set(exchanges_before.keys())} != {set(exchanges_after.keys())}"
            f"\n{dataset['name']}|{dataset['location']}"
        )

        return dataset

    @lru_cache()
    def get_gis_match(
        self,
        location,
        possible_locations,
        contained,
        exclusive,
        biggest_first,
    ):
        # prepare locations in possible_locations
        # all locations in possible_locations that are an IAM region
        # need to be converted to tuples with (model.upper(), location)
        # and other locations longer than 2 characters (other than GLO)
        # are converted to tuples with ("ecoinvent", location).

        possible_locations = [
            (
                (self.model.upper(), loc)
                if loc in self.regions
                else (
                    ("ecoinvent", loc)
                    if (len(loc) > 2 and loc not in ["GLO", "RoW"])
                    else loc
                )
            )
            for loc in possible_locations
        ]

        possible_locations = [loc for loc in possible_locations if loc in self.geo.geo]

        with resolved_row(possible_locations, self.geo.geo) as g:
            func = g.contained if contained else g.intersects

            gis_match = func(
                location,
                include_self=True,
                exclusive=exclusive,
                biggest_first=biggest_first,
                only=possible_locations,
            )

        return gis_match
