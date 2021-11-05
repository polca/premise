"""
transformation_tools.py contains a number of small functions that help manipulating the Pandas dataframe `database`.
"""

from typing import List

from .utils import c
import pandas as pd


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


def contains(key: c, value: str):

def equals(key, value):

def excludes(key, value):


def get_dataframe_producers_keys(database) -> List[int]:
    """
    Return a list of unique producer keys

    :return: list of keys (int)
    :rtype: list
    """

    return database[c.prod_key].unique()

def get_many(df: pd.DataFrame, filters: dict) -> List[pd.Series]:

    selector = ~df[("ecoinvent", c.cons_name)].isnull()

    for col, name in filters.items():
        #FIXME: contains or equals?
        selector *= df[("ecoinvent", col)].str.contains(name)

    for _, data in df[selector].iterrows():
        yield data
