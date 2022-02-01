"""
activity_maps.py contains InventorySet, which is a class that provides all necessary
mapping between ``premise`` and ``ecoinvent`` terminology.
"""

import csv
from pathlib import Path
from typing import List, Union

import yaml

from . import DATA_DIR

GAINS_TO_ECOINVENT_EMISSION_FILEPATH = (
    DATA_DIR / "GAINS_emission_factors" / "ecoinvent_to_gains_emission_mappping.csv"
)
POWERPLANT_TECHS = DATA_DIR / "electricity" / "electricity_tech_vars.yml"
FUELS_TECHS = DATA_DIR / "fuels" / "fuel_tech_vars.yml"
MATERIALS_TECHS = DATA_DIR / "utils" / "materials_vars.yml"


def get_mapping(filepath: Path, var: str) -> dict:
    """
    Loa a YAML file and return a dictionary given a variable.
    :param filepath: YAML file path
    :param var: variable to return the dictionary for.
    :return: a dictionary
    """

    with open(filepath, "r") as stream:
        techs = yaml.safe_load(stream)

    mapping = {}
    for key, val in techs.items():
        if var in val:
            mapping[key] = val[var]

    return mapping


def get_gains_to_ecoinvent_emissions() -> dict:
    """
    Retrieve the correspondence between GAINS and ecoinvent emission labels.
    :return: GAINS emission labels as keys and ecoinvent emission labels as values
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
    Hosts different filter sets to find equivalencies
    between ``premise`` terms and ``ecoinvent`` activities and exchanges.

    It stores:
    * material_filters: filters for activities related to materials.
    * powerplant_filters: filters for activities related to power generation technologies.
    * powerplant_fuel_filters: filters for fuel providers in power generation technologies.
    * fuel_filters: filters for fuel providers in general.
    * emissions_map: REMIND emission labels as keys, ecoinvent emission labels as values

    The functions :func:`generate_material_map`, :func:`generate_powerplant_map`
    and :func:`generate_fuel_map` can
    be used to extract the actual activity objects as dictionaries.
    These functions return the result of applying :func:`act_fltr` to the filter dictionaries.
    """

    def __init__(self, db: List[dict]) -> None:
        self.db = db
        self.powerplant_filters = get_mapping(
            filepath=POWERPLANT_TECHS, var="ecoinvent_aliases"
        )
        self.powerplant_fuels_filters = get_mapping(
            filepath=POWERPLANT_TECHS, var="ecoinvent_fuel_aliases"
        )
        self.fuels_filters = get_mapping(filepath=FUELS_TECHS, var="ecoinvent_aliases")
        self.materials_filters = get_mapping(
            filepath=MATERIALS_TECHS, var="ecoinvent_aliases"
        )

    def generate_powerplant_map(self) -> dict:
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_filters)

    def generate_powerplant_fuels_map(self) -> dict:
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_fuels_filters)

    def generate_fuel_map(self) -> dict:
        """
        Filter ecoinvent processes related to fuel supply.

        :return: dictionary with fuel names as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.fuels_filters)

    def generate_material_map(self) -> dict:
        return self.generate_sets_from_filters(self.materials_filters)

    @staticmethod
    def act_fltr(
        db: List[dict],
        fltr: Union[str, List[str]] = None,
        mask: Union[str, List[str]] = None,
        filter_exact: bool = False,
        mask_exact: bool = False,
    ) -> List[dict]:
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
        result = []

        # default field is name
        if type(fltr) == list or type(fltr) == str:
            fltr = {"name": fltr}
        if type(mask) == list or type(mask) == str:
            mask = {"name": mask}

        def like(a, b):
            if filter_exact:
                return a.lower() == b.lower()
            else:
                return a.lower().startswith(b.lower())

        def notlike(a, b):
            if mask_exact:
                return a.lower() != b.lower()
            else:
                return b.lower() not in a.lower()

        assert len(fltr) > 0, "Filter dict must not be empty."
        for field in fltr:
            condition = fltr[field]
            if type(condition) == list:
                for el in condition:
                    # this is effectively connecting the statements by *or*
                    result.extend([act for act in db if like(act[field], el)])
            else:
                result.extend([act for act in db if like(act[field], condition)])

        for field in mask:
            condition = mask[field]
            if type(condition) == list:
                for el in condition:
                    # this is effectively connecting the statements by *and*
                    result = [act for act in result if notlike(act[field], el)]
            else:
                result = [act for act in result if notlike(act[field], condition)]
        return result

    def generate_sets_from_filters(self, filtr: dict) -> dict:
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
        return {
            tech: set([act["name"] for act in actlst]) for tech, actlst in techs.items()
        }
