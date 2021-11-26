import csv

import yaml

from premise.framework.logics import contains, does_not_contain

from . import DATA_DIR
from .transformation_tools import *
from .utils import c

GAINS_TO_ECOINVENT_EMISSION_FILEPATH = (
    DATA_DIR / "GAINS_emission_factors" / "ecoinvent_to_gains_emission_mappping.csv"
)
POWERPLANT_TECHS = DATA_DIR / "electricity" / "electricity_tech_vars.yml"
FUELS_TECHS = DATA_DIR / "fuels" / "fuel_tech_vars.yml"


def get_mapping(filepath, var):

    with open(filepath, "r") as stream:
        techs = yaml.safe_load(stream)

    mapping = {}
    for key, val in techs.items():
        if var in val:
            mapping[key] = val[var]

    return mapping


def get_gains_to_ecoinvent_emissions():
    """
    Retrieve the correspondence between GAINS and ecoinvent emission labels.
    :return: GAINS emission labels as keys and ecoinvent emission labels as values
    :rtype: dict
    """

    if not GAINS_TO_ECOINVENT_EMISSION_FILEPATH.is_file():
        raise FileNotFoundError(
            "The dictionary of emission labels correspondences could not be found."
        )

    csv_dict = {}

    with open(GAINS_TO_ECOINVENT_EMISSION_FILEPATH) as f:
        input_dict = csv.reader(f, delimiter=";")
        for row in input_dict:
            csv_dict[row[0]] = row[1]

    return csv_dict

class InventorySet:
    """
    Hosts different filter sets to for ecoinvent activities and exchanges.

    It stores:
    * material_filters: filters for activities related to materials.
    * powerplant_filters: filters for activities related to power generation technologies.
    * emissions_map: GAINS emission labels as keys, ecoinvent emission labels as values

    The functions :func:`generate_material_map` and :func:`generate_powerplant_map` can
    be used to extract the actual activity objects as dictionaries.
    These functions return the result of applying :func:`act_fltr` to the filter dictionaries.
    """

    def __init__(self, db):
        self.db = db
        self.powerplant_filters = get_mapping(
            filepath=POWERPLANT_TECHS, var="ecoinvent_aliases"
        )
        self.powerplant_fuels_filters = get_mapping(
            filepath=POWERPLANT_TECHS, var="ecoinvent_fuel_aliases"
        )
        self.fuels_filters = get_mapping(filepath=FUELS_TECHS, var="ecoinvent_aliases")

    def generate_powerplant_map(self):
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_filters)

    def generate_powerplant_fuels_map(self):
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_fuels_filters)

    def generate_fuel_map(self):
        """
        Filter ecoinvent processes related to fuel supply.

        :return: dictionary with fuel names as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.fuels_filters)

    @staticmethod
    def act_fltr(db, fltr=None, mask=None):
        """Filter `database` for activities matching field contents given by `fltr` excluding strings in `mask`.
        `fltr`: string, list of strings or dictionary.
        If a string is provided, it is used to match the name field from the start (*startswith*).
        If a list is provided, all strings in the lists are used and results are joined (*or*).
        A dict can be given in the form <fieldname>: <str> to filter for <str> in <fieldname>.
        `mask`: used in the same way as `fltr`, but filters add up with each other (*and*).
        `filter_exact` and `mask_exact`: boolean, set `True` to only allow for exact matches.

        :param db: A lice cycle inventory database
        :type db: brightway2 database object
        :param fltr: value(s) to filter with.
        :type fltr: Union[str, lst, dict]
        :param mask: value(s) to filter with.
        :type mask: Union[str, lst, dict]
        :param filter_exact: requires exact match when true.
        :type filter_exact: bool
        :param mask_exact: requires exact match when true.
        :type mask_exact: bool
        :return: list of activity data set names
        :rtype: list

        """
        if fltr is None:
            fltr = {}
        if mask is None:
            mask = {}

        # default field is name
        if type(fltr) == list or type(fltr) == str:
            fltr = {"name": fltr}
        if type(mask) == list or type(mask) == str:
            mask = {"name": mask}

        assert len(fltr) > 0, "Filter dict must not be empty."

        list_filters = []

        fields_map = {"name": c.cons_name, "reference product": c.cons_prod}

        for field in fltr:
            condition = fltr[field]
            if type(condition) == list:
                for el in condition:
                    list_filters.append(contains((s.exchange, fields_map[field]), el))
            else:
                list_filters.append(
                    contains((s.exchange, fields_map[field]), condition)
                )

        must_contain_filters = list_filters[0]

        for f in list_filters:
            must_contain_filters = must_contain_filters | f

        list_filters = []

        for field in mask:
            condition = mask[field]
            if type(condition) == list:
                for el in condition:
                    list_filters.append(
                        does_not_contain((s.exchange, fields_map[field]), el)
                    )
            else:
                list_filters.append(
                    does_not_contain((s.exchange, fields_map[field]), condition)
                )

        if len(list_filters) > 0:
            must_exclude_filters = list_filters[0]

            for f in list_filters:
                must_exclude_filters = must_exclude_filters & f

            filters = (must_contain_filters) & (must_exclude_filters)
        else:
            filters = must_contain_filters

        results = db.loc[filters(db), (s.exchange, c.cons_name)].unique()

        return results

    def generate_sets_from_filters(self, filtr):
        """
        Generate a dictionary with sets of activity names for
        technologies from the filter specifications.

        :param filtr:
        :func:`activity_maps.InventorySet.act_fltr`.
        :return: dictionary with the same keys as provided in filter
            and a set of activity data set names as values.
        :rtype: dict
        """
        techs = {tech: self.act_fltr(self.db, **fltr) for tech, fltr in filtr.items()}

        return {tech: set(actlst) for tech, actlst in techs.items()}
