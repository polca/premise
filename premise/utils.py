"""
Various utils functions.
"""

import csv
import os
import sys
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import xarray as xr
import yaml
from constructive_geometries import resolved_row
from country_converter import CountryConverter
from prettytable import ALL, PrettyTable
from wurst.searching import equals, get_many, reference_product
from wurst.transformations.uncertainty import rescale_exchange

from . import DATA_DIR, __version__, geomap
from .geomap import Geomap

FUELS_PROPERTIES = DATA_DIR / "fuels" / "fuel_tech_vars.yml"
CROPS_PROPERTIES = DATA_DIR / "fuels" / "crops_properties.yml"
CLINKER_RATIO_ECOINVENT_36 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_36.csv"
CLINKER_RATIO_ECOINVENT_35 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_35.csv"
CLINKER_RATIO_REMIND = DATA_DIR / "cement" / "clinker_ratios.csv"
FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"

STEEL_RECYCLING_SHARES = DATA_DIR / "steel" / "steel_recycling_shares.csv"
METALS_RECYCLING_SHARES = DATA_DIR / "metals" / "metals_recycling_shares.csv"

REMIND_TO_FUELS = DATA_DIR / "steel" / "remind_fuels_correspondance.txt"
EFFICIENCY_RATIO_SOLAR_PV = DATA_DIR / "renewables" / "efficiency_solar_PV.csv"


class HiddenPrints:
    """
    From https://stackoverflow.com/questions/8391411/how-to-block-calls-to-print
    """

    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w", encoding="utf-8")

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


def eidb_label(model: str, scenario: str, year: int) -> str:
    """
    Return a label to name a scenario.
    :param model: IAM model
    :param scenario: SSP/RCP scenario
    :param year: year
    :return: scenario label, str.
    """
    return f"ecoinvent_{model}_{scenario}_{year}"


@lru_cache(maxsize=None)
def get_fuel_properties() -> dict:
    """
    Loads a yaml file into a dictionary.
    This dictionary contains lower heating values
    CO2 emission factors, biogenic carbon share, for a number of fuel types.
    Mostly taken from ecoinvent and
    https://www.engineeringtoolbox.com/fuels-higher-calorific-values-d_169.html
    :return: dictionary that contains lower heating values
    :rtype: dict
    """

    with open(FUELS_PROPERTIES, "r", encoding="utf-8") as stream:
        fuel_props = yaml.safe_load(stream)

    return fuel_props


def get_crops_properties() -> dict:
    """
    Return a dictionary with crop names as keys and IAM labels as values
    relating to land use change CO2 per crop type
    :return: dict
    """
    with open(CROPS_PROPERTIES, "r", encoding="utf-8") as stream:
        crop_props = yaml.safe_load(stream)

    return crop_props


def get_efficiency_ratio_solar_photovoltaics() -> xr.DataArray:
    """
    Return an array with PV module efficiencies in function of year and technology.
    :return: xr.DataArray with PV module efficiencies
    """

    dataframe = pd.read_csv(EFFICIENCY_RATIO_SOLAR_PV, sep=";")

    return dataframe.groupby(["technology", "year"]).mean()["efficiency"].to_xarray()


def get_clinker_ratio_ecoinvent(version: str) -> Dict[Tuple[str, str], float]:
    """
    Return a dictionary with (cement names, location) as keys
    and clinker-to-cement ratios as values,
    as found in ecoinvent.
    :return: dict
    """
    if version == "3.5":
        filepath = CLINKER_RATIO_ECOINVENT_35
    else:
        filepath = CLINKER_RATIO_ECOINVENT_36

    with open(filepath, encoding="utf-8") as file:
        clinker_ratios = {}
        for val in csv.reader(file, delimiter=","):
            clinker_ratios[(val[0], val[1])] = float(val[2])
    return clinker_ratios


def get_clinker_ratio_remind(year: int) -> xr.DataArray:
    """
    Return an array with the average clinker-to-cement ratio
    per year and per region, as given by REMIND.
    :return: xarray
    :return:
    """

    dataframe = pd.read_csv(CLINKER_RATIO_REMIND, sep=",")

    return (
        dataframe.groupby(["region", "year"])
        .mean()["value"]
        .to_xarray()
        .interp(year=year)
    )


def relink_technosphere_exchanges(
    dataset,
    data,
    model,
    cache,
    exclusive=True,
    drop_invalid=False,
    biggest_first=False,
    contained=True,
):
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
        * ``biggest_first``: Bool, default is ``False``. Determines search order when selecting provider locations. Only relevant is ``exclusive`` is ``True``.
        * ``contained``: Bool, default is ``True``. If true, only use providers whose location is completely within the ``dataset`` location; otherwise use all intersecting locations.
        * ``iam_regions``: List, lists IAM regions, if additional ones need to be defined.
    Modifies the dataset in place; returns the modified dataset."""

    new_exchanges = []
    technosphere = lambda x: x["type"] == "technosphere"

    geomatcher = geomap.Geomap(model=model)

    list_loc = [k if isinstance(k, str) else k[1] for k in geomatcher.geo.keys()]

    for exc in filter(technosphere, dataset["exchanges"]):

        try:
            exchange = cache[dataset["location"]][
                (exc["name"], exc["product"], exc["location"], exc["unit"])
            ]

            if isinstance(exchange, tuple):
                name, prod, unit, loc = exchange

                new_exchanges.append(
                    {
                        "name": name,
                        "product": prod,
                        "unit": unit,
                        "location": loc,
                        "type": "technosphere",
                        "amount": exc["amount"],
                    }
                )

            else:

                new_exchanges.extend(
                    [
                        {
                            "name": i[0],
                            "product": i[1],
                            "unit": i[3],
                            "location": i[2],
                            "type": "technosphere",
                            "amount": exc["amount"] * i[-1],
                        }
                        for i in exchange
                    ]
                )

        except KeyError:
            possible_datasets = [
                x for x in get_possibles(exc, data) if x["location"] in list_loc
            ]
            possible_locations = [obj["location"] for obj in possible_datasets]

            if dataset["location"] in possible_locations:
                exc["location"] = dataset["location"]
                new_exchanges.append(exc)
                continue

            possible_locations = [
                (model.upper(), p) if p in geomatcher.iam_regions else p
                for p in possible_locations
            ]

            if len(possible_datasets) > 0:

                with resolved_row(possible_locations, geomatcher.geo) as g:
                    func = g.contained if contained else g.intersects

                    if dataset["location"] in geomatcher.iam_regions:
                        location = (model.upper(), dataset["location"])
                    else:
                        location = dataset["location"]

                    gis_match = func(
                        location,
                        include_self=True,
                        exclusive=exclusive,
                        biggest_first=biggest_first,
                        only=possible_locations,
                    )

                kept = [
                    ds
                    for loc in gis_match
                    for ds in possible_datasets
                    if ds["location"] == loc
                ]

                if kept:
                    missing_faces = geomatcher.geo[location].difference(
                        set.union(*[geomatcher.geo[obj["location"]] for obj in kept])
                    )
                    if missing_faces and "RoW" in possible_locations:
                        kept.extend(
                            [
                                obj
                                for obj in possible_datasets
                                if obj["location"] == "RoW"
                            ]
                        )
                if not kept and "RoW" in possible_locations:
                    kept = [
                        obj for obj in possible_datasets if obj["location"] == "RoW"
                    ]

                if not kept and "GLO" in possible_locations:
                    kept = [
                        obj for obj in possible_datasets if obj["location"] == "GLO"
                    ]

                if not kept and any(
                    x in possible_locations for x in ["RER", "EUR", "WEU"]
                ):
                    kept = [
                        obj
                        for obj in possible_datasets
                        if obj["location"] in ["RER", "EUR", "WEU"]
                    ]

                if not kept:
                    if drop_invalid:
                        continue

                    new_exchanges.append(exc)
                    continue

                allocated, share = allocate_inputs(exc, kept)
                new_exchanges.extend(allocated)

                if dataset["location"] in cache:
                    cache[dataset["location"]][
                        (
                            exc["name"],
                            exc["product"],
                            exc["location"],
                            exc["unit"],
                        )
                    ] = [
                        (e["name"], e["product"], e["location"], e["unit"], s)
                        for e, s in zip(allocated, share)
                    ]

                else:

                    cache[dataset["location"]] = {
                        (exc["name"], exc["product"], exc["location"], exc["unit"],): [
                            (e["name"], e["product"], e["location"], e["unit"], s)
                            for e, s in zip(allocated, share)
                        ]
                    }
            else:
                new_exchanges.append(exc)
                # add to cache
                if dataset["location"] in cache:
                    cache[dataset["location"]][
                        (
                            exc["name"],
                            exc["product"],
                            exc["location"],
                            exc["unit"],
                        )
                    ] = (
                        exc["name"],
                        exc["product"],
                        exc["location"],
                        exc["unit"],
                    )

                else:
                    cache[dataset["location"]] = {
                        (exc["name"], exc["product"], exc["location"], exc["unit"],): (
                            exc["name"],
                            exc["product"],
                            exc["location"],
                            exc["unit"],
                        )
                    }

    dataset["exchanges"] = [
        exc for exc in dataset["exchanges"] if exc["type"] != "technosphere"
    ] + new_exchanges

    return cache, dataset


def allocate_inputs(exc, lst):
    """
    Allocate the input exchanges in ``lst`` to ``exc``,
    using production volumes where possible, and equal splitting otherwise.
    Always uses equal splitting if ``RoW`` is present.
    """
    has_row = any((x["location"] in ("RoW", "GLO") for x in lst))
    pvs = [reference_product(o).get("production volume") or 0 for o in lst]
    if all((x > 0 for x in pvs)) and not has_row:
        # Allocate using production volume
        total = sum(pvs)
    else:
        # Allocate evenly
        total = len(lst)
        pvs = [1 for _ in range(total)]

    def new_exchange(exc, location, factor):
        copied_exc = deepcopy(exc)
        copied_exc["location"] = location
        return rescale_exchange(copied_exc, factor)

    return [
        new_exchange(exc, obj["location"], factor / total)
        for obj, factor in zip(lst, pvs)
    ], [p / total for p in pvs]


def get_possibles(exchange, data):
    """Filter a list of datasets ``data``,
    returning those with the save name,
    reference product, and unit as in ``exchange``.
    Returns a generator."""
    key = (exchange["name"], exchange["product"], exchange["unit"])
    list_exc = []
    for dataset in data:
        if (dataset["name"], dataset["reference product"], dataset["unit"]) == key:
            list_exc.append(dataset)
    return list_exc


def default_global_location(database):

    """
    Set missing locations to ```GLO```
    for datasets in ``database``.
    Changes location if ``location``
    is missing or ``None``.
    Will add key ``location`` if missing.
    :param database: database to update
    """

    for dataset in get_many(database, *[equals("location", None)]):
        dataset["location"] = "GLO"
    return database


def get_regions_definition(model: str) -> None:
    """
    :param model: IAM model name, e.g., "remind", "image"

    Return a table containing the list of countries
    corresponding to each region of the model."""
    table = PrettyTable(["Region", "Countries"])

    geo = Geomap(model)
    country_converter = CountryConverter()

    for region in geo.iam_regions:

        list_countries = []
        for iso_2 in geo.iam_to_ecoinvent_location(region):
            if iso_2 in country_converter.ISO2["ISO2"].values:
                country_name = country_converter.convert(iso_2, to="name")
            else:
                country_name = iso_2

            list_countries.append(country_name)

        table.add_row([region, list_countries])

    table._max_width = {"Region": 50, "Countries": 125}
    table.hrules = ALL

    print(table)


# clear the cache folder
def clear_cache():
    [f.unlink() for f in Path(DATA_DIR / "cache").glob("*") if f.is_file()]
    print("Cache folder cleared!")


def print_version():
    print(f"premise v.{__version__}")


def info_on_utils_functions():
    """Display message to list utils functions"""

    table = PrettyTable(["Utils functions", "Description"])
    table.add_row(
        [
            "clear_cache()",
            (
                "Clears the cache folder. "
                "Useful when updating `premise`"
                "or encountering issues with "
                "inventories."
            ),
        ]
    )
    table.add_row(
        [
            "get_regions_definition(model)",
            "Retrieves the list of countries for each region of the model.",
        ]
    )
    table.add_row(
        [
            "ndb.NewDatabase(...)\nndb.generate_scenario_report()",
            "Generates a summary of the most important scenarios' variables.",
        ]
    )
    # align text to the left
    table.align = "l"
    table.hrules = ALL
    table._max_width = {"Utils functions": 50, "Description": 32}
    print(table)


def warning_about_biogenic_co2() -> None:
    """
    Prints a simple warning about characterizing biogenic CO2 flows.
    :return: Does not return anything.
    """
    table = PrettyTable(["Warning"])
    table.add_row(
        [
            "Because some of the scenarios can yield LCI databases\n"
            "containing net negative emission technologies (NET),\n"
            "it is advised to account for biogenic CO2 flows when calculating\n"
            "Global Warming potential indicators.\n"
            "`premise_gwp` provides characterization factors for such flows.\n"
            "It also provides factors for hydrogen emissions to air.\n\n"
            "Within your bw2 project:\n"
            "from premise_gwp import add_premise_gwp\n"
            "add_premise_gwp()"
        ]
    )
    # align text to the left
    table.align = "l"
    print(table)


def hide_messages():
    """
    Hide messages from the console.
    """

    print("Hide these messages?")
    print("NewDatabase(..., quiet=True)")
