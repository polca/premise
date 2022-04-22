"""
transformation_tools.py contains a number of small functions that help manipulating the Pandas dataframe `database`.
"""

from typing import Callable, List, Tuple

import numpy as np
import pandas as pd
from constructive_geometries import resolved_row

from premise import geomap
from premise.framework.logics import (
    contains,
    contains_any_from_list,
    does_not_contain,
    equals,
)
from premise.utils import c, create_hash, recalculate_hash, s


def get_dataframe_locs(database) -> List[str]:
    """
    Return a list of unique locations in the dataframe

    :return: list of location (strings)
    :rtype: list
    """

    return database.loc[:, (s.exchange, c.cons_loc)].unique()


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

    selector = ~df[(s.exchange, c.cons_name)].isnull()

    for ifilter in filters:
        if filtering_method.lower() == "and":
            selector *= ifilter(df)
        elif filtering_method.lower() == "or":
            # or
            selector += ifilter(df)
        else:
            raise ArithmeticError("Only `and` or `or` operations" "are implemented.")

    return df[selector]


def emptying_datasets(df: pd.DataFrame, scenario, filters: Callable):
    """
    Zero out technosphere and biosphere exchanges of datasets in `scenario`,
    based on the list of filters specified, but preserves production exchanges,
    so that the dataset still exists, but is empty.
    :param df: pd.Dataframe that contains the database
    :param: scenario column to return hash from
    :param filters: list of filters functions
    """

    # filter for production exchanges
    filter_excluding_exchanges = (
        filters & does_not_contain((s.exchange, c.type), "production")
    )(df)

    # zero out all exchanges contained in the filter
    df.loc[filter_excluding_exchanges, (scenario, c.amount)] = 0


def create_redirect_exchange(df: pd.DataFrame, new_loc: str, cols: List[str]):
    """
    Empty a dataset, and make it point to another one
    :return:
    """

    new_exc = df.iloc[0].copy()

    new_exc[(s.ecoinvent, c.cons_prod_vol)] = np.nan
    new_exc[[(col, c.cons_prod_vol) for col in cols]] = np.nan

    new_exc[(s.ecoinvent, c.comment)] = ""
    new_exc[
        [(col, c.comment) for col in cols]
    ] = "redirect to new IAM-specific regional dataset"

    new_exc[(s.exchange, c.prod_name)] = new_exc[(s.exchange, c.cons_name)]
    new_exc[(s.exchange, c.prod_prod)] = new_exc[(s.exchange, c.cons_prod)]

    new_exc[(s.exchange, c.cons_loc)] = new_exc[(s.exchange, c.prod_loc)]
    new_exc[(s.exchange, c.prod_loc)] = new_loc

    new_exc[(s.exchange, c.type)] = "technosphere"

    new_exc[(s.ecoinvent, c.amount)] = 0
    new_exc[[(col, c.comment) for col in cols]] = 1

    return new_exc


def rename_location(df: pd.DataFrame, new_loc: str) -> pd.DataFrame:
    """
    Change the location of datasets in `scenario`,
    based on the list of filters specified.
    :param df: pd.Dataframe that contains the database
    :param scenario: scenario column to return hash from
    :param new_loc: new location to change to
    :return: pd.DataFrame with datasets with new locations
    """

    df.loc[:, (s.exchange, c.cons_loc)] = new_loc

    _filter = (equals((s.exchange, c.type), "production"))(df)

    df.loc[_filter, (s.exchange, c.prod_loc)] = new_loc

    # update consumer key
    df.loc[:, (s.exchange, c.cons_key)] = create_hash(
        df.loc[_filter, (s.exchange, c.cons_name)],
        df.loc[_filter, (s.exchange, c.cons_prod)],
        df.loc[_filter, (s.exchange, c.cons_loc)],
    )

    # update exchange key
    df.loc[:, (s.exchange, c.exc_key)] = create_hash(
        df.loc[_filter, (s.exchange, c.prod_name)],
        df.loc[_filter, (s.exchange, c.prod_prod)],
        df.loc[_filter, (s.exchange, c.prod_loc)],
        df.loc[_filter, (s.exchange, c.cons_name)],
        df.loc[_filter, (s.exchange, c.cons_prod)],
        df.loc[_filter, (s.exchange, c.cons_loc)],
    )

    return df


def change_production_volume(
    df: pd.DataFrame, cols: List[str], new_prod_vol: float
) -> pd.DataFrame:
    """
    Change the production volume of a dataset in `scenario`,
    based on the list of filters specified.
    :param df: pd.Dataframe that contains the database
    :param scenario: scenario column to return hash from
    :param new_prod_vol: new production volume to change to
    :return: pd.DataFrame with datasets with new production volume
    """

    _filter = (equals((s.exchange, c.type), "production"))(df)

    df.loc[_filter, [(col, c.cons_prod_vol) for col in cols]] = new_prod_vol

    return df


def scale_exchanges_by_constant_factor(
    df: pd.DataFrame, scenario: str, factor: float, filters
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

    # filter out the production exchanges
    _filters = (filters & does_not_contain((s.exchange, c.type), "production"))(df)

    # update location in the (scenario, c.cons_loc) column
    df.loc[_filters, (scenario, c.amount)] = (
        df.loc[_filters, (s.ecoinvent, c.amount)] * factor
    )

    return df