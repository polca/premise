"""
transformation_tools.py contains a number of small functions that help manipulating the Pandas dataframe `database`.
"""

from typing import Callable, List, Tuple

import numpy as np
import pandas as pd
from constructive_geometries import resolved_row

from . import geomap
from .utils import c, create_hash


def get_dataframe_locs(database) -> List[str]:
    """
    Return a list of unique locations in the dataframe

    :return: list of location (strings)
    :rtype: list
    """

    return database[("ecoinvent", c.cons_loc)].unique()


def does_not_contain(
    key: Tuple[str, c], value: str
) -> Callable[[pd.DataFrame], pd.Series]:
    """
    Return a function that selects rows that do NOT contain `value`
    in the column `key`
    :param key: column of dataframe
    :param value: string
    :return: lambda function
    """
    return lambda df: ~(df[key].str.contains(value, regex=False))


def contains(key: Tuple[str, c], value: str) -> Callable[[pd.DataFrame], pd.Series]:
    """
    Return a function that selects rows that contain `value`
    in the column `key`
    :param key: column of dataframe
    :param value: string
    :return: lambda function
    """
    return lambda df: df[key].str.contains(value, regex=False)


def equals(key: Tuple[str, c], value: str) -> Callable[[pd.DataFrame], pd.Series]:
    """
    Return a function that selects rows that equal `value`
    in the column `key`
    :param key: column of dataframe
    :param value: string
    :return: lambda function
    """
    return lambda df: df[key] == value


def get_many(
    df: pd.DataFrame, filtering_method: str = "and", *filters: Callable
) -> pd.DataFrame:
    """
    Return a list of pd.Series (rows, or exchanges) that satisfy
    the list of filters specified
    :param df: pd.Dataframe that contains the database
    :param filtering_method: can be "and" or "or". Determines whether filters multiply or sum up.
    :param filters: list of filters functions
    :return: list of pd.Series
    """

    selector = ~df[("ecoinvent", c.cons_name)].isnull()

    for ifilter in filters:
        if filtering_method.lower() == "and":
            selector *= ifilter(df)
        elif filtering_method.lower() == "or":
            # or
            selector += ifilter(df)
        else:
            raise ArithmeticError("Only `and` or `or` operations"
                                  "are implemented.")

    return df[selector]


def get_many_production_exchanges(
    df: pd.DataFrame, filtering_method: str = "and", *filters: Callable
) -> pd.DataFrame:
    """
    Return a list of pd.Series (rows, or exchanges) that satisfy
    the list of filters specified
    :param df: pd.Dataframe that contains the database
    :param filtering_method: can be "and" or "or". Determines whether filters multiply or sum up.
    :param filters: list of filters functions
    :return: list of pd.Series
    """

    selector = ~df[("ecoinvent", c.cons_name)].isnull()

    for ifilter in filters:
        if filtering_method == "and":
            selector *= ifilter(df)
        else:
            # or
            selector += ifilter(df)

    # filter for production exchanges
    filter_prod_exchanges = equals(("ecoinvent", c.type), "production")

    selector *= filter_prod_exchanges(df)

    return df[selector]


def get_many_single_column(
    df: pd.DataFrame,
    column: Tuple[str, c],
    filtering_method: str = "and",
    *filters: Callable
) -> List[int]:
    """
    Return a list of values contained in column `column` that satisfy
    the list of filters specified
    :param df: pd.Dataframe that contains the database
    :param column: column to return hash from
    :param filtering_method: can be "and" or "or". Determines whether filters multiply or sum up.
    :param filters: list of filters functions
    :return: list hashes
    """

    df = get_many(
        df,
        filtering_method,
        *filters
    )

    # return unique hashes from the specified column
    return df[column].unique()


def remove_datasets(df: pd.DataFrame, scenario, *filters: Callable):
    """
    Zero out exchanges of datasets that should not exist in `scenario`,
    based on the list of filters specified
    :param df: pd.Dataframe that contains the database
    :param: scenario column to return hash from
    :param filters: list of filters functions
    """

    # return hashes from the `c.cons_key` column
    hashes = get_many_single_column(df, ("ecoinvent", c.cons_key), "and", *filters)

    # zero out all exchanges contained in hash list
    df.loc[df[("ecoinvent", c.cons_key)].isin(hashes), (scenario, c.amount)] = 0


def empty_datasets(df: pd.DataFrame, scenario, *filters: Callable):
    """
    Zero out technosphere and biosphere exchanges of datasets in `scenario`,
    based on the list of filters specified, but preserves production exchanges,
    so that the dataset still exists, but is empty.
    :param df: pd.Dataframe that contains the database
    :param: scenario column to return hash from
    :param filters: list of filters functions
    """

    # filter for production exchanges
    filter_prod_exchanges = equals(("ecoinvent", c.type), "production")

    # return hashes of all exchanges that satisfy `filters`
    hashes_all_exc = get_many_single_column(df, ("ecoinvent", c.exc_key), "and", *filters)

    # return hashes of all exchanges of `production` type
    hashes_prod_exc = get_many_single_column(
        df, ("ecoinvent", c.exc_key), "and", filter_prod_exchanges
    )

    # subtract production exchange hashes to the hash list
    hashes = set(hashes_all_exc) - set(hashes_prod_exc)

    # zero out all exchanges contained in hash list
    df.loc[df[("ecoinvent", c.exc_key)].isin(hashes), (scenario, c.amount)] = 0


def redirect_datasets(df, scenario, redirect_to, *filters):
    """
    Empty a dataset, and make it point to another one
    :return:
    """

    # return hashes of all exchanges that satisfy `filters`
    hashes_all_exc = get_many_single_column(df, ("ecoinvent", c.exc_key), "and", *filters)

    # retrieve one row of the dataset
    hash = hashes_all_exc[-1]
    new_exc = df.loc[df[("ecoinvent", c.exc_key)].isin([hash]), :].copy()

    new_exc.loc[:, ("ecoinvent", c.cons_prod_vol)] = np.nan
    new_exc.loc[:, ("ecoinvent", c.comment)] = ""

    new_exc.loc[
        :, [("ecoinvent", c.prod_name), ("ecoinvent", c.prod_prod)]
    ] = new_exc.loc[:, [("ecoinvent", c.cons_name), ("ecoinvent", c.cons_prod)]]

    new_exc.loc[:, ("ecoinvent", c.prod_loc)] = redirect_to
    new_exc.loc[:, ("ecoinvent", c.type)] = "technosphere"

    new_exc.loc[:, ("ecoinvent", c.amount)] = 0
    new_exc.loc[:, (scenario, c.amount)] = 1

    new_exc[("ecoinvent", c.prod_key)] = new_exc.apply(
        lambda row: create_hash(
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.prod_prod)],
            row[("ecoinvent", c.prod_loc)],
        ),
        axis=1,
    )

    new_exc[("ecoinvent", c.exc_key)] = new_exc.apply(
        lambda row: create_hash(
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.cons_key)],
            row[("ecoinvent", c.cons_key)],
            row[("ecoinvent", c.cons_key)],
        ),
        axis=1,
    )

    return new_exc


def rename_location(
    df: pd.DataFrame, scenario: str, new_loc: str, *filters: Callable
) -> pd.DataFrame:
    """
    Change the location of datasets in `scenario`,
    based on the list of filters specified.
    :param df: pd.Dataframe that contains the database
    :param scenario: scenario column to return hash from
    :param new_loc: new location to change to
    :param filters: list of filters functions
    :return: pd.DataFrame with datasets with new locations
    """

    # return hashes of all exchanges that satisfy `filters`
    hashes_all_exc = get_many_single_column(df, ("ecoinvent", c.exc_key), "and", *filters)

    df.loc[
        df[("ecoinvent", c.exc_key)].isin(hashes_all_exc), (scenario, c.amount)
    ] = df.loc[
        df[("ecoinvent", c.exc_key)].isin(hashes_all_exc), ("ecoinvent", c.amount)
    ]
    df.loc[
        df[("ecoinvent", c.exc_key)].isin(hashes_all_exc), (scenario, c.prod_loc)
    ] = df.loc[
        df[("ecoinvent", c.exc_key)].isin(hashes_all_exc), ("ecoinvent", c.prod_loc)
    ]

    # filter for production exchanges
    filter_prod_exchanges = equals(("ecoinvent", c.type), "production")

    # return hashes of all exchanges of `production` type
    hashes_all_prod_exc = get_many_single_column(
        df, ("ecoinvent", c.exc_key), "and", filter_prod_exchanges
    )

    # return hashes of production exchanges within hashes_all_exc
    hashes = set(hashes_all_exc).intersection(hashes_all_prod_exc)

    # update location in the (ecoinvent, c.prod_loc) column
    df.loc[
        df[("ecoinvent", c.exc_key)].isin(hashes),
        [("ecoinvent", c.prod_loc), ("ecoinvent", c.prod_loc)],
    ] = new_loc

    df.loc[
        df[("ecoinvent", c.exc_key)].isin(hashes_all_exc), ("ecoinvent", c.amount)
    ] = 0

    df[("ecoinvent", c.prod_key)] = df.apply(
        lambda row: create_hash(
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.prod_prod)],
            row[("ecoinvent", c.prod_loc)],
        ),
        axis=1,
    )
    df[("ecoinvent", c.cons_key)] = df.apply(
        lambda row: create_hash(
            row[("ecoinvent", c.cons_key)],
            row[("ecoinvent", c.cons_key)],
            row[("ecoinvent", c.cons_key)],
        ),
        axis=1,
    )
    df[("ecoinvent", c.exc_key)] = df.apply(
        lambda row: create_hash(
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.prod_name)],
            row[("ecoinvent", c.cons_key)],
            row[("ecoinvent", c.cons_key)],
            row[("ecoinvent", c.cons_key)],
        ),
        axis=1,
    )

    return df


def change_production_volume(
    df: pd.DataFrame, scenario: str, new_prod_vol: float, *filters: Callable
) -> pd.DataFrame:
    """
    Change the production volume of a dataset in `scenario`,
    based on the list of filters specified.
    :param df: pd.Dataframe that contains the database
    :param scenario: scenario column to return hash from
    :param new_prod_vol: new production volume to change to
    :param filters: list of filters functions
    :return: pd.DataFrame with datasets with new production volume
    """

    # filter for production exchanges
    filter_prod_exchanges = equals(("ecoinvent", c.type), "production")

    # return hashes of all exchanges of `production` type
    hashes_prod_exc = get_many_single_column(
        df, ("ecoinvent", c.exc_key), "and", filter_prod_exchanges
    )

    # update production volume in the (scenario, c.cons_prod_loc) column
    df.loc[
        df[("ecoinvent", c.exc_key)].isin(hashes_prod_exc),
        (scenario, c.cons_prod_vol),
    ] = new_prod_vol

    return df


def scale_exchanges_by_constant_factor(
    df: pd.DataFrame, scenario: str, factor: float, *filters: Callable
) -> pd.DataFrame:
    """
    Change the exchange amounts of datasets in `scenario`,
    by a specified factor,
    based on the list of filters specified.
    :param df: pd.Dataframe that contains the database
    :param scenario: scenario column to return hash from
    :param factor: new location to change to
    :param filters: list of filters functions
    :return: pd.DataFrame with datasets with new locations
    """

    # return hashes of all exchanges that satisfy `filters`
    hashes_all_exc = get_many_single_column(df, ("ecoinvent", c.exc_key), "and", *filters)

    # filter for production exchanges
    filter_prod_exchanges = equals(("ecoinvent", c.type), "production")

    # return hashes of all exchanges of `production` type
    hashes_all_prod_exc = get_many_single_column(
        df, ("ecoinvent", c.exc_key), "and", filter_prod_exchanges
    )

    # subtract production exchange hashes to the hash list
    hashes = set(hashes_all_exc) - set(hashes_all_prod_exc)

    # update location in the (scenario, c.cons_loc) column
    df.loc[df[("ecoinvent", c.exc_key)].isin(hashes), (scenario, c.amount)] *= factor

    return df
