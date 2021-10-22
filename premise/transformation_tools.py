from .utils import c


def get_ecoinvent_locs(database):
    """
    Return a list of unique locations in ecoinvent

    :return: list of location
    :rtype: list
    """

    return database[c.cons_loc].unique()
