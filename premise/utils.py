import csv
import enum
import hashlib
import pprint
from functools import lru_cache
from typing import List

import numpy as np
import pandas as pd
import yaml
from wurst import searching as ws

from . import DATA_DIR
from .framework.tags import TagLibrary

CLINKER_RATIO_ECOINVENT_36 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_36.csv"
CLINKER_RATIO_ECOINVENT_35 = DATA_DIR / "cement" / "clinker_ratio_ecoinvent_35.csv"
CLINKER_RATIO_REMIND = DATA_DIR / "cement" / "clinker_ratios.csv"

STEEL_RECYCLING_SHARES = DATA_DIR / "steel" / "steel_recycling_shares.csv"

EFFICIENCY_RATIO_SOLAR_PV = DATA_DIR / "renewables" / "efficiency_solar_PV.csv"

BLACK_WHITE_LISTS = DATA_DIR / "utils" / "black_white_list_efficiency.yml"
FUELS_PROPERTIES = DATA_DIR / "fuels" / "fuel_tech_vars.yml"
CROPS_PROPERTIES = DATA_DIR / "fuels" / "crops_properties.yml"


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


def match(reference: str, candidates: List[str]) -> [str, None]:
    """
    Matches a string with a list of fuel candidates and returns the candidate whose label
    is contained in the string (or None).

    :param reference: the name of a potential fuel
    :param candidates: fuel names for which calorific values are known
    :return: str or None
    """

    reference = reference.replace(" ", "").strip().lower()

    for i in candidates:
        if i in reference:
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
                    f"{pprint.pformat([(i['name'], i['type'], i['amount'], i['unit']) for i in exchanges])} {output_energy} {input_energy}\n"
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
    amount = max((iexc["amount"], 0))

    if iexc["unit"].strip().lower() == "megajoule":
        input_energy = amount
    elif iexc["unit"].strip().lower() == "kilowatt hour":
        input_energy = amount * 3.6
    elif iexc["unit"] in ("kilogram", "cubic meter"):
        # if the calorific value of the exchange needs to be found
        # we look up the `product` of the exchange if it is of `type`
        # technosphere or production
        # otherwise, we look up the `name` if it is of type biosphere
        best_match = match(
            iexc["product"]
            if iexc["type"] in ("production", "technosphere") and iexc["unit"] != "unit"
            else iexc["name"],
            fuel_names,
        )
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
        except ZeroDivisionError as e:
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


def recalculate_hash(df):

    df[(s.exchange, c.prod_key)] = df.apply(
        lambda row: create_hash(
            row[(s.exchange, c.prod_name)],
            row[(s.exchange, c.prod_prod)],
            row[(s.exchange, c.prod_loc)],
        ),
        axis=1,
    )
    df[(s.exchange, c.cons_key)] = df.apply(
        lambda row: create_hash(
            row[(s.exchange, c.cons_name)],
            row[(s.exchange, c.cons_prod)],
            row[(s.exchange, c.cons_loc)],
        ),
        axis=1,
    )
    df[(s.exchange, c.exc_key)] = df.apply(
        lambda row: create_hash(
            row[(s.exchange, c.prod_name)],
            row[(s.exchange, c.prod_prod)],
            row[(s.exchange, c.prod_loc)],
            row[(s.exchange, c.cons_name)],
            row[(s.exchange, c.cons_prod)],
            row[(s.exchange, c.cons_loc)],
        ),
        axis=1,
    )


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
                    consumer_prod_vol,
                    iexc["amount"],
                    energy_efficiency if producer_type == "production" else np.nan,
                    comment,
                )
            )

    tuples = []

    for idx, col in enumerate(list(c)):
        if idx < 11:
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


def extract_exc(row: pd.Series) -> dict:
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
        }

    else:
        exc = {
            "name": row[c.prod_name],
            "categories": tuple(row[c.prod_loc].split("::")),
            "type": row[c.type],
            "uncertainty type": 0,
            "unit": row[c.unit],
            "amount": row[c.amount],
            "input": ("biosphere3", row[c.prod_key]),
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
        "comment": prod_exc[c.comment],
        "database": col,
        "code": str(prod_exc[c.cons_key]),
        "exchanges": list(df.apply(extract_exc, axis=1)),
    }

    return outer


def convert_df_to_dict(df: pd.DataFrame, db_type: str = "single") -> List[dict]:
    """
    Converts ds into lists of dictionaries. Each list is a scenario database.
    :param df: pd.DataFrame. Contains the database in a tabular form.
    :return: List of dictionaries that can be consumed by `wurst`
    """

    scenarios = [col for col in df.columns if col[0] not in [s.exchange, s.ecoinvent]]

    cols = {col[0] for col in scenarios} if db_type == "single" else {s.ecoinvent}

    # if we are building a superstructure database
    # we want to append all non-empty comments to the comment column
    # of ecoinvent

    if db_type != "single":
        col_tuples = [
            (col, c.comment) for col in df.columns.levels[0] if col != s.exchange
        ]
        df[(s.ecoinvent, c.comment)] = df[col_tuples].sum(axis=1)

    # else, we fill NaNs in scenario columns
    # with values from ecoinvent
    else:
        for col in scenarios:
            if col[1] == c.comment:
                sel = df[col]
                df.loc[sel, col] += df.loc[sel, (s.ecoinvent, col[1])]

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


def get_efficiency_ratio_solar_PV(year, power):
    """
    Return a dictionary with years as keys and efficiency ratios as values
    :return: dict
    """

    df = pd.read_csv(EFFICIENCY_RATIO_SOLAR_PV, sep=",")

    return (
        df.groupby(["power", "year"])
        .mean()["value"]
        .to_xarray()
        .interp(year=year, power=power, kwargs={"fill_value": "extrapolate"})
    )


def get_clinker_ratio_ecoinvent(version):
    """
    Return a dictionary with (cement names, location) as keys and clinker-to-cement ratios as values,
    as found in ecoinvent.
    :return: dict
    """
    if version == 3.5:
        fp = CLINKER_RATIO_ECOINVENT_35
    else:
        fp = CLINKER_RATIO_ECOINVENT_36

    with open(fp) as f:
        d = {}
        for val in csv.reader(f, delimiter=","):
            d[(val[0], val[1])] = float(val[2])
    return d


def get_clinker_ratio_remind(year):
    """
    Return an array with the average clinker-to-cement ratio per year and per region, as given by REMIND.
    :return: xarray
    :return:
    """
    df = pd.read_csv(CLINKER_RATIO_REMIND, sep=",")

    return df.groupby(["region", "year"]).mean()["value"].to_xarray().interp(year=year)


def get_steel_recycling_rates(year):
    """
    Return an array with the average shares for primary (Basic oxygen furnace) and secondary (Electric furnace)
    steel production per year and per region, as given by: https://www.bir.org/publications/facts-figures/download/643/175/36?method=view
    for 2015-2019, further linearly extrapolated to 2020, 2030, 2040 and 2050.
    :return: xarray
    :return:
    """
    df = pd.read_csv(STEEL_RECYCLING_SHARES, sep=";")

    return (
        df.groupby(["region", "year", "type"])
        .mean()[["share", "world_share"]]
        .to_xarray()
        .interp(year=year)
    )


def rev_index(inds):
    return {v: k for k, v in inds.items()}


def create_codes_and_names_of_A_matrix(db):
    """
    Create a dictionary a tuple (activity name, reference product,
    unit, location) as key, and its code as value.
    :return: a dictionary to map indices to activities
    :rtype: dict
    """
    return {
        (
            i["name"],
            i["reference product"],
            i["unit"],
            i["location"],
        ): i["code"]
        for i in db
    }


def create_scenario_label(model: str, pathway: str, year: int) -> str:

    return f"{model}::{pathway}::{year}"
