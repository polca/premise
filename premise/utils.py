"""
Various utils functions.
"""

import csv
import enum
import hashlib
import os
import pprint
import sys
import warnings
from functools import lru_cache
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from country_converter import CountryConverter
from prettytable import ALL, PrettyTable
from wurst import searching as ws

from . import DATA_DIR, __version__
from .framework.tags import TagLibrary
from .geomap import Geomap

warnings.simplefilter(action="ignore", category=FutureWarning)

CLINKER_RATIO_ECOINVENT_36 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_36.csv"
CLINKER_RATIO_ECOINVENT_35 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_35.csv"
CLINKER_RATIO_REMIND = DATA_DIR / "cement" / "clinker_ratios.csv"

STEEL_RECYCLING_SHARES = DATA_DIR / "steel" / "steel_recycling_shares.csv"

EFFICIENCY_RATIO_SOLAR_PV = DATA_DIR / "renewables" / "efficiency_solar_PV.csv"

BLACK_WHITE_LISTS = DATA_DIR / "utils" / "black_white_list_efficiency.yml"
FUELS_PROPERTIES = DATA_DIR / "fuels" / "fuel_tech_vars.yml"
CROPS_PROPERTIES = DATA_DIR / "fuels" / "crops_properties.yml"

cache_match = {}


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


class c(enum.Enum):
    prod_name = "from activity"
    prod_prod = "from product"
    prod_loc = "from location"
    cons_name = "to activity"
    cons_prod = "to product"
    cons_loc = "to location"
    unit = "unit"
    type = "type"
    prod_key = "from key"
    cons_key = "to key"
    exc_key = "from/to key"
    new = "new"
    # the above are exchange identifiers

    # fields below are ecoinvent data
    cons_prod_vol = "production volume"
    amount = "amount"
    efficiency = "efficiency"
    comment = "comment"


class s(enum.Enum):
    exchange = "exchange"
    ecoinvent = "ecoinvent"
    tag = "tag"


class e:
    prod_name = (s.exchange, c.prod_name)
    prod_prod = (s.exchange, c.prod_prod)
    prod_loc = (s.exchange, c.prod_loc)
    cons_name = (s.exchange, c.cons_name)
    cons_prod = (s.exchange, c.cons_prod)
    cons_loc = (s.exchange, c.cons_loc)
    unit = (s.exchange, c.unit)
    ext_type = (s.exchange, c.type)
    prod_key = (s.exchange, c.prod_key)
    cons_key = (s.exchange, c.cons_key)
    exc_key = (s.exchange, c.exc_key)


def match(reference: str, candidates: List[str]) -> [str, None]:
    """
    Matches a string with a list of fuel candidates and returns the candidate whose label
    is contained in the string (or None).

    :param reference: the name of a potential fuel
    :param candidates: fuel names for which calorific values are known
    :return: str or None
    """

    reference = reference.replace(" ", "").strip().lower()

    if reference in cache_match:
        return cache_match[reference]

    else:
        for i in candidates:
            if i in reference:
                cache_match[reference] = i
                return i


def calculate_dataset_efficiency(
    exchanges: List[dict], ds_name: str, white_list: list
) -> float:
    """
    Returns the efficiency rate of the dataset by looking up the fuel inputs
    and calculating the fuel input-to-output ratio.

    :param exchanges: list of exchanges
    :param ds_name: str. the name of an activity
    :param white_list: a list of strings
    :return: efficiency rate
    :rtype: float
    """

    # variables to store the cumulative energy amount
    output_energy = 0.0
    input_energy = 0.0

    # loop through exchanges
    # energy exchanges can either be
    # an outgoing flow, with type `production`
    # or an incoming flow, with type `technosphere` or `biosphere`

    for iexc in exchanges:
        if iexc["type"] == "production":
            output_energy += extract_energy(iexc)
        if iexc["type"] in ("technosphere", "biosphere"):
            input_energy += extract_energy(iexc)

    # test in case no energy input is found
    # if an energy output is found
    # this is potentially an issue
    if input_energy < 1e-12:
        if output_energy > 1e-12:

            if not any(i in ds_name for i in white_list):
                print(
                    f"{pprint.pformat([(i['name'], i['type'], i['amount'], i['unit']) for i in exchanges])} {output_energy} {input_energy} in {ds_name}\n"
                )
                print(input_energy, output_energy)
        else:
            # no energy input
            # but no energy output either
            # so this might not be an energy conversion process
            # we exit here to prevent a division by zero
            return 0.0  # TODO: double-check whether this is really not an energy conversion process

    return output_energy / input_energy


def extract_energy(iexc: dict) -> float:
    """
    Extracts the energy input or output of the exchange.
    If the exchange has an energy unit like MJ or kWh, the `amount` is fetched directly.
    If not, it can be a solid or liquid fuels.
    In which case, its calorific value needs ot be found.

    :param iexc: a dictionary containing an exchange
    :return: an amount of energy, in MJ
    """
    fuels_lhv = get_fuel_properties()
    fuel_names = list(fuels_lhv.keys())

    try:
        amount = max((iexc["amount"], 0))
    except KeyError:
        print(iexc)

    if iexc["unit"] == "megajoule":
        input_energy = amount
    elif iexc["unit"] == "kilowatt hour":
        input_energy = amount * 3.6
    elif iexc["unit"] in ("kilogram", "cubic meter"):
        # if the calorific value of the exchange needs to be found
        # we look up the `product` of the exchange if it is of `type`
        # technosphere or production
        # otherwise, we look up the `name` if it is of type biosphere

        try:
            best_match = match(
                iexc["product"]
                if iexc["type"] in ("production", "technosphere")
                and iexc["unit"] != "unit"
                else iexc["name"],
                fuel_names,
            )
        except:
            best_match = None

        # if we do not find its calorific value, 0 is returned
        if not best_match:
            input_energy = 0.0
        else:
            input_energy = amount * fuels_lhv[best_match]["lhv"]

    else:
        input_energy = 0.0

    return input_energy


@lru_cache
def get_black_white_list():
    with open(BLACK_WHITE_LISTS, "r") as stream:
        out = yaml.safe_load(stream)
    black_list = out["blacklist"]
    white_list = out["whitelist"]

    return white_list, black_list


def find_efficiency(ids: dict) -> float:
    """
    Find the efficiency of a given wurst dataset
    :param ids: dictionary
    :return: energy efficiency (between 0 and 1)
    """

    # dictionary with fuels as keys, calorific values as values
    fuels_lhv = get_fuel_properties()
    # list of fuels names
    fuel_names = list(fuels_lhv.keys())

    # lists of strings that, if contained
    # in activity name, indicate that said
    # activity should either be skipped
    # or forgiven if an error is thrown

    white_list, black_list = get_black_white_list()

    if (
        ids["unit"] in ("megajoule", "kilowatt hour", "cubic meter")
        and not any(i.lower() in ids["name"].lower() for i in black_list)
    ) or (
        ids["unit"] == "kilogram"
        and any(i.lower() in ids["name"].lower() for i in fuel_names)
        and not any(i.lower() in ids["name"].lower() for i in black_list)
    ):
        try:
            energy_efficiency = calculate_dataset_efficiency(
                exchanges=ids["exchanges"], ds_name=ids["name"], white_list=white_list
            )
        except ZeroDivisionError:
            if not any(i.lower() in ids["name"].lower() for i in white_list):
                print(f"issue with {ids['name']}")
                energy_efficiency = np.nan
            else:
                energy_efficiency = np.nan
    else:
        energy_efficiency = np.nan

    return energy_efficiency


def create_hash(*items):

    hasher = hashlib.blake2b(digest_size=12)

    for item in items:
        hasher.update(str(item).encode())

    return int(hasher.hexdigest(), 16)


def create_hash_for_database(db):
    hash_func = lambda x: create_hash(*x)
    return db.apply(hash_func, axis=1)


def recalculate_hash(df):

    df[(s.exchange, c.prod_key)] = (
        create_hash(
            df[(s.exchange, c.prod_name)],
            df[(s.exchange, c.prod_prod)],
            df[(s.exchange, c.prod_loc)],
        ),
    )

    df[(s.exchange, c.cons_key)] = (
        create_hash(
            df[(s.exchange, c.cons_name)],
            df[(s.exchange, c.cons_prod)],
            df[(s.exchange, c.cons_loc)],
        ),
    )

    df[(s.exchange, c.exc_key)] = (
        create_hash(
            df[(s.exchange, c.prod_name)],
            df[(s.exchange, c.prod_prod)],
            df[(s.exchange, c.prod_loc)],
            df[(s.exchange, c.cons_name)],
            df[(s.exchange, c.cons_prod)],
            df[(s.exchange, c.cons_loc)],
        ),
    )

    return df


def convert_db_to_dataframe(database: List[dict]) -> pd.DataFrame:
    """
    Convert the wurst database into a pd.DataFrame.
    Does not extract uncertainty information.
    :param database: wurst database
    :type database: list
    :return: returns a dataframe
    :rtype: pd.DataFrame
    """

    data_to_ret = []

    for ids in database:
        consumer_name = ids["name"]
        consumer_product = ids["reference product"]
        consumer_loc = ids["location"]

        consumer_key = create_hash(consumer_name, consumer_product, consumer_loc)

        # calculate the efficiency of the dataset
        # check first if a `parameters` or `efficiency` key is present
        if any(i in ids for i in ["parameters", "efficiency"]):

            if "parameters" in ids:
                key = list(
                    key
                    for key in ids["parameters"]
                    if "efficiency" in key
                    and not any(item in key for item in ["thermal"])
                )

                if len(key) > 0:
                    energy_efficiency = ids["parameters"][key[0]]
                else:

                    energy_efficiency = find_efficiency(ids)

            else:
                energy_efficiency = ids["efficiency"]

        else:
            energy_efficiency = find_efficiency(ids)

        for iexc in ids["exchanges"]:
            producer_name = iexc["name"]
            producer_type = iexc["type"]

            if iexc["type"] == "biosphere":
                producer_product = ""
                producer_location = "::".join(iexc["categories"])
                producer_key = iexc["input"][1]
            else:
                producer_product = iexc["product"]
                producer_location = iexc["location"]
                producer_key = create_hash(
                    producer_name, producer_product, producer_location
                )

            comment = ""
            consumer_prod_vol = 0
            if iexc["type"] == "production":
                comment = ids.get("comment", "")

                if "production volume" in iexc:
                    consumer_prod_vol = iexc["production volume"]

            if iexc["type"] in ("technosphere", "biosphere"):
                comment = iexc.get("comment", "")

            exchange_key = create_hash(
                producer_name,
                producer_product,
                producer_location,
                consumer_name,
                consumer_product,
                consumer_loc,
            )
            data_to_ret.append(
                (
                    producer_name,
                    producer_product,
                    producer_location,
                    consumer_name,
                    consumer_product,
                    consumer_loc,
                    iexc["unit"],
                    producer_type,
                    producer_key,
                    consumer_key,
                    exchange_key,
                    True if "new" in ids else False,
                    consumer_prod_vol,
                    iexc["amount"],
                    energy_efficiency if producer_type == "production" else np.nan,
                    comment,
                )
            )

    tuples = []

    for idx, col in enumerate(list(c)):
        if idx < 12:
            tuples.append((s.exchange, col))
        else:
            tuples.append((s.ecoinvent, col))

    df = pd.DataFrame(
        data_to_ret,
        columns=pd.MultiIndex.from_tuples(tuples),
    )

    # create tag columns
    df = add_tags(TagLibrary().load(), df)

    # create flag column
    df = add_flag(df)

    return df


def add_flag(df: pd.DataFrame) -> pd.DataFrame:
    return df


def add_tags(tag_lib: TagLibrary, df: pd.DataFrame) -> pd.DataFrame:

    for tag in tag_lib.tags():
        df[(s.tag, tag)] = False

    for idx, row in df.iterrows():
        activity = row[(s.exchange, c.cons_name)]

        if activity in tag_lib:
            for tag in tag_lib.get_tag[activity]:
                df.loc[idx, (s.tag, tag)] = True

    return df


def extract_exc(row: pd.Series, col: str) -> dict:
    """
    Returns a dictionary that represents an exchange.
    :param row: pd.Series, containing an exchange.
    :return: dict., containing the items necessary to define an exchange.
    """

    if row[c.type] == "production":
        exc = {
            "name": row[c.prod_name],
            "product": row[c.prod_prod],
            "location": row[c.prod_loc],
            "type": row[c.type],
            "uncertainty type": 0,
            "unit": row[c.unit],
            "production volume": row[c.cons_prod_vol],
            "amount": row[c.amount],
            "input": (col, str(row[c.prod_key])),
        }

    elif row[c.type] == "technosphere":
        exc = {
            "name": row[c.prod_name],
            "product": row[c.prod_prod],
            "location": row[c.prod_loc],
            "type": row[c.type],
            "uncertainty type": 0,
            "unit": row[c.unit],
            "amount": row[c.amount],
            "input": (col, str(row[c.prod_key])),
        }

    else:
        exc = {
            "name": row[c.prod_name],
            "categories": tuple(row[c.prod_loc].split("::")),
            "type": row[c.type],
            "uncertainty type": 0,
            "unit": row[c.unit],
            "amount": row[c.amount],
            "input": ("biosphere3", str(row[c.prod_key])),
        }

    return exc


def transf(df: pd.DataFrame, col: str) -> dict:
    """
    Returns the first level items of the dictionary of a dataset.
    :param df: pd.DataFrame, containing the database in a tabular form.
    :param col: str. Scenario name, to locate the correct column in `df`.
    :return: dict. that contains the first level items of a dataset.
    """

    prod_exc = df.loc[df[c.type] == "production", :].iloc[0]

    outer = {
        "name": prod_exc[c.cons_name],
        "reference product": prod_exc[c.cons_prod],
        "location": prod_exc[c.cons_loc],
        "unit": prod_exc[c.unit],
        "comment": str(prod_exc[c.comment]),
        "database": col,
        "code": str(prod_exc[c.cons_key]),
        "exchanges": list(df.apply(extract_exc, col=col, axis=1)),
    }

    return outer


def convert_df_to_dict(df: pd.DataFrame, db_type: str = "single") -> List[dict]:
    """
    Converts ds into lists of dictionaries. Each list is a scenario database.
    :param df: pd.DataFrame. Contains the database in a tabular form.
    :return: List of dictionaries that can be consumed by `wurst`
    """

    scenarios = [
        col for col in df.columns if col[0] not in [s.exchange, s.ecoinvent, s.tag]
    ]

    cols = {col[0] for col in scenarios} if db_type == "single" else {s.ecoinvent}

    # if we are building a superstructure database
    # we want to append all non-empty comments to the comment column
    # of ecoinvent

    if db_type != "single":
        col_tuples = [
            (col, c.comment)
            for col in df.columns.levels[0]
            if col not in [s.exchange, s.tag]
        ]

        df.loc[:, (s.ecoinvent, c.comment)] = df[col_tuples].apply(
            lambda row: " ".join(row.values.astype(str)), axis=1
        )

    # else, we fill NaNs in scenario columns
    # with values from ecoinvent
    else:
        for col in scenarios:
            if col[1] == c.comment:
                df.loc[:, col] += df.loc[:, (s.ecoinvent, col[1])]

            else:
                sel = df[col].isna()
                df.loc[sel, col] = df.loc[sel, (s.ecoinvent, col[1])]

    for col in cols:
        temp = df.loc[:, ((s.exchange, col), slice(None))].droplevel(level=0, axis=1)
        yield [transf(group[1], col) for group in temp.groupby(c.cons_key)]


def eidb_label(model: str, scenario: str, year: int) -> str:

    return "ecoinvent_" + model + "_" + scenario + "_" + str(year)


def get_crops_properties():
    """
    Return a dictionary with crop names as keys and IAM labels as values
    relating to land use change CO2 per crop type

    :return: dict
    """
    with open(CROPS_PROPERTIES, "r") as stream:
        crop_props = yaml.safe_load(stream)

    return crop_props


@lru_cache
def get_fuel_properties():
    """
    Loads a yaml file into a dictionary.
    This dictionary contains lower heating values
    and CO2 emission factors for a number of fuel types.
    Mostly taken from ecoinvent and
    https://www.engineeringtoolbox.com/fuels-higher-calorific-values-d_169.html

    :return: dictionary that contains lower heating values
    :rtype: dict
    """

    with open(FUELS_PROPERTIES, "r") as stream:
        fuel_props = yaml.safe_load(stream)

    fuel_props = dict(
        (k.replace(" ", "").strip().lower(), v) for k, v in fuel_props.items()
    )
    return fuel_props


def get_efficiency_ratio_solar_PV() -> xr.DataArray:
    """
    Return an array with PV module efficiencies in function of year and technology.
    :return: xr.DataArray with PV module efficiencies
    """

    df = pd.read_csv(EFFICIENCY_RATIO_SOLAR_PV, sep=";")

    return df.groupby(["technology", "year"]).mean()["efficiency"].to_xarray()


def create_scenario_label(model: str, pathway: str, year: int) -> str:

    return f"{model}::{pathway}::{year}"


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

    print("Keep uncertainty data?")
    print("NewDatabase(..., keep_uncertainty_data=True)")
    print("")
    print("Hide these messages?")
    print("NewDatabase(..., quiet=True)")
