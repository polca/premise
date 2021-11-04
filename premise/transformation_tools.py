"""
transformation_tools.py contains a number of small functions that help manipulating the Pandas dataframe `database`.
"""

from typing import List

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


def get_dataframe_producers_keys(database) -> List[int]:
    """
    Return a list of unique producer keys

    :return: list of keys (int)
    :rtype: list
    """

    return database[c.prod_key].unique()
