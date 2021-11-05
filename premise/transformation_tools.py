"""
transformation_tools.py contains a number of small functions that help manipulating the Pandas dataframe `database`.
"""

from typing import Callable, List, Tuple

import pandas as pd

from .utils import c


def get_dataframe_locs(database) -> List[str]:
    """
    Return a list of unique locations in the dataframe

    :return: list of location (strings)
    :rtype: list
    """

    return database[c.cons_loc].unique()


def get_dataframe_consumers_keys(database) -> List[int]:
    """
    Return a list of unique consumers keys

    :return: list of keys (int)
    :rtype: list
    """

    return database[c.cons_key].unique()


def contains(key: c, value: str) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df[key].str.contains(value)


def equals(key: c, value: str) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df[key] == value


def does_not_contain(key: c, value: str) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: ~(df[key].str.contains(value))


def get_dataframe_producers_keys(database) -> List[int]:
    """
    Return a list of unique producer keys

    :return: list of keys (int)
    :rtype: list
    """

    return database[c.prod_key].unique()


def get_many(df: pd.DataFrame, *filters: Tuple[Callable]) -> List[pd.Series]:

    selector = ~df[("ecoinvent", c.cons_name)].isnull()

    for ifilter in filters:
        selector *= ifilter(df)

    for _, data in df[selector].iterrows():
        yield data