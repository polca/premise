import enum
import hashlib
import pprint
from functools import lru_cache
from typing import List

import numpy as np
import pandas as pd
import yaml
from wurst import searching as ws

from .export import *
from .geomap import Geomap

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
                producer_product = "-"
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

    return pd.DataFrame(
        data_to_ret,
        columns=pd.MultiIndex.from_tuples(tuples),
    )


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


def convert_df_to_dict(df: pd.DataFrame) -> List[dict]:
    """
    Converts ds into lists of dictionaries. Each list is a scenario database.
    :param df: pd.DataFrame. Contains the database in a tabular form.
    :return: List of dictionaries that can be consumed by `wurst`
    """

    scenarios = [col for col in df.columns if col[0] not in [s.exchange, s.ecoinvent]]

    for col in scenarios:
        if col[1] == c.comment:
            sel = df[col] == ""
        else:
            sel = df[col].isna()

        df.loc[sel, col] = df.loc[sel, (s.ecoinvent, col[1])]

    cols = {col[0] for col in scenarios}

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


def get_metals_recycling_rates(year):
    """
    Return an array with the average shares for some metals,
    as given by: https://static-content.springer.com/esm/art%3A10.1038%2Fs43246-020-00095-x/MediaObjects/43246_2020_95_MOESM1_ESM.pdf
    for 2025, 2035, 2045.
    :return: xarray
    :return:
    """
    df = pd.read_csv(METALS_RECYCLING_SHARES, sep=";")

    return (
        df.groupby(["metal", "year", "type"])
        .mean()["share"]
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


def add_modified_tags(original_db, scenarios):
    """
    Add a `modified` label to any activity that is new
    Also add a `modified` label to any exchange that has been added
    or that has a different value than the source database.
    :return:
    """

    # Class `Export` to which the original database is passed
    exp = Export(original_db)
    # Collect a dictionary of activities {row/col index in A matrix: code}
    rev_ind_A = rev_index(create_codes_index_of_A_matrix(original_db))
    # Retrieve list of coordinates [activity, activity, value]
    coords_A = exp.create_A_matrix_coordinates()
    # Turn it into a dictionary {(code of receiving activity, code of supplying activity): value}
    original = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}
    # Collect a dictionary with activities' names and correponding codes
    codes_names = create_codes_and_names_of_A_matrix(original_db)
    # Collect list of substances
    rev_ind_B = rev_index(create_codes_index_of_B_matrix())
    # Retrieve list of coordinates of the B matrix [activity index, substance index, value]
    coords_B = exp.create_B_matrix_coordinates()
    # Turn it into a dictionary {(activity code, substance code): value}
    original.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B})

    for s, scenario in enumerate(scenarios):
        print(f"Looking for differences in database {s + 1} ...")
        rev_ind_A = rev_index(create_codes_index_of_A_matrix(scenario["database"]))
        exp = Export(
            scenario["database"],
            scenario["model"],
            scenario["pathway"],
            scenario["year"],
            "",
        )
        coords_A = exp.create_A_matrix_coordinates()
        new = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}

        rev_ind_B = rev_index(create_codes_index_of_B_matrix())
        coords_B = exp.create_B_matrix_coordinates()
        new.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B})

        list_new = set(i[0] for i in original.keys()) ^ set(i[0] for i in new.keys())

        ds = (d for d in scenario["database"] if d["code"] in list_new)

        # Tag new activities
        for d in ds:
            d["modified"] = True

        # List codes that belong to activities that contain modified exchanges
        list_modified = (i[0] for i in new if i in original and new[i] != original[i])
        #
        # Filter for activities that have modified exchanges
        for ds in ws.get_many(
            scenario["database"],
            ws.either(*[ws.equals("code", c) for c in set(list_modified)]),
        ):
            # Loop through biosphere exchanges and check if
            # the exchange also exists in the original database
            # and if it has the same value
            # if any of these two conditions is False, we tag the exchange
            excs = (exc for exc in ds["exchanges"] if exc["type"] == "biosphere")
            for exc in excs:
                if (ds["code"], exc["input"][0]) not in original or new[
                    (ds["code"], exc["input"][0])
                ] != original[(ds["code"], exc["input"][0])]:
                    exc["modified"] = True
            # Same thing for technosphere exchanges,
            # except that we first need to look up the provider's code first
            excs = (exc for exc in ds["exchanges"] if exc["type"] == "technosphere")
            for exc in excs:
                if (
                    exc["name"],
                    exc["product"],
                    exc["unit"],
                    exc["location"],
                ) in codes_names:
                    exc_code = codes_names[
                        (exc["name"], exc["product"], exc["unit"], exc["location"])
                    ]
                    if new[(ds["code"], exc_code)] != original[(ds["code"], exc_code)]:
                        exc["modified"] = True
                else:
                    exc["modified"] = True

    return scenarios


def create_scenario_label(model: str, pathway: str, year: int) -> str:

    return f"{model}::{pathway}::{year}"


def relink_technosphere_exchanges(
    ds,
    data,
    model,
    exclusive=True,
    drop_invalid=False,
    biggest_first=False,
    contained=True,
    iam_regions=[],
):
    """Find new technosphere providers based on the location of the dataset.
    Designed to be used when the dataset's location changes, or when new datasets are added.
    Uses the name, reference product, and unit of the exchange to filter possible inputs. These must match exactly. Searches in the list of datasets ``data``.
    Will only search for providers contained within the location of ``ds``, unless ``contained`` is set to ``False``, all providers whose location intersects the location of ``ds`` will be used.
    A ``RoW`` provider will be added if there is a single topological face in the location of ``ds`` which isn't covered by the location of any providing activity.
    If no providers can be found, `relink_technosphere_exchanes` will try to add a `RoW` or `GLO` providers, in that order, if available. If there are still no valid providers, a ``InvalidLink`` exception is raised, unless ``drop_invalid`` is ``True``, in which case the exchange will be deleted.
    Allocation between providers is done using ``allocate_inputs``; results seem strange if ``contained=False``, as production volumes for large regions would be used as allocation factors.
    Input arguments:
        * ``ds``: The dataset whose technosphere exchanges will be modified.
        * ``data``: The list of datasets to search for technosphere product providers.
        * ``model``: The IAM model
        * ``exclusive``: Bool, default is ``True``. Don't allow overlapping locations in input providers.
        * ``drop_invalid``: Bool, default is ``False``. Delete exchanges for which no valid provider is available.
        * ``biggest_first``: Bool, default is ``False``. Determines search order when selecting provider locations. Only relevant is ``exclusive`` is ``True``.
        * ``contained``: Bool, default is ``True``. If true, only use providers whose location is completely within the ``ds`` location; otherwise use all intersecting locations.
        * ``iam_regions``: List, lists IAM regions, if additional ones need to be defined.
    Modifies the dataset in place; returns the modified dataset."""
    new_exchanges = []
    technosphere = lambda x: x["type"] == "technosphere"

    geomatcher = geomap.Geomap(model=model)

    list_loc = [k if isinstance(k, str) else k[1] for k in geomatcher.geo.keys()]

    for exc in filter(technosphere, ds["exchanges"]):

        possible_datasets = [
            x for x in get_possibles(exc, data) if x["location"] in list_loc
        ]
        possible_locations = [obj["location"] for obj in possible_datasets]

        if ds["location"] in possible_locations:
            exc["location"] = ds["location"]
            new_exchanges.append(exc)
            continue

        possible_locations = [
            (model.upper(), p) if p in geomatcher.iam_regions else p
            for p in possible_locations
        ]

        if len(possible_datasets) > 0:

            with resolved_row(possible_locations, geomatcher.geo) as g:
                func = g.contained if contained else g.intersects

                if ds["location"] in geomatcher.iam_regions:
                    location = (model.upper(), ds["location"])
                else:
                    location = ds["location"]

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
                        [obj for obj in possible_datasets if obj["location"] == "RoW"]
                    )
            elif "RoW" in possible_locations:
                kept = [obj for obj in possible_datasets if obj["location"] == "RoW"]

            if not kept and "GLO" in possible_locations:
                kept = [obj for obj in possible_datasets if obj["location"] == "GLO"]

            if not kept and any(x in possible_locations for x in ["RER", "EUR", "WEU"]):
                kept = [
                    obj
                    for obj in possible_datasets
                    if obj["location"] in ["RER", "EUR", "WEU"]
                ]

            if not kept:
                if drop_invalid:
                    continue
                else:
                    new_exchanges.append(exc)
                    continue

            allocated = allocate_inputs(exc, kept)

            new_exchanges.extend(allocated)

        else:
            new_exchanges.append(exc)

    ds["exchanges"] = [
        exc for exc in ds["exchanges"] if exc["type"] != "technosphere"
    ] + new_exchanges
    return ds


def allocate_inputs(exc, lst):
    """Allocate the input exchanges in ``lst`` to ``exc``, using production volumes where possible, and equal splitting otherwise.
    Always uses equal splitting if ``RoW`` is present."""
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
        cp = deepcopy(exc)
        cp["location"] = location
        return rescale_exchange(cp, factor)

    return [
        new_exchange(exc, obj["location"], factor / total)
        for obj, factor in zip(lst, pvs)
    ]


def get_possibles(exchange, data):
    """Filter a list of datasets ``data``, returning those with the save name, reference product, and unit as in ``exchange``.
    Returns a generator."""
    key = (exchange["name"], exchange["product"], exchange["unit"])
    list_exc = []
    for ds in data:
        if (ds["name"], ds["reference product"], ds["unit"]) == key:
            list_exc.append(ds)
    return list_exc


def default_global_location(database):
    """Set missing locations to ```GLO``` for datasets in ``database``.
    Changes location if ``location`` is missing or ``None``. Will add key ``location`` if missing."""
    for ds in get_many(database, *[equals("location", None)]):
        ds["location"] = "GLO"
    return database
