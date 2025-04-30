"""
activity_maps.py contains InventorySet, which is a class that provides all necessary
mapping between ``premise`` and ``ecoinvent`` terminology.
"""

from collections import defaultdict
from pathlib import Path
from typing import List, Union

import yaml
from charset_normalizer.models import CliDetectionResult
from wurst import searching as ws

from .filesystem_constants import DATA_DIR, VARIABLES_DIR

POWERPLANT_TECHS = VARIABLES_DIR / "electricity.yaml"
FUELS_TECHS = VARIABLES_DIR / "fuels.yaml"
BIOMASS_TYPES = VARIABLES_DIR / "biomass.yaml"
METALS_TECHS = DATA_DIR / "metals" / "activities_mapping.yml"
CDR_TECHS = VARIABLES_DIR / "carbon_dioxide_removal.yaml"
CARBON_STORAGE_TECHS = VARIABLES_DIR / "carbon_storage_variables.yaml"
CEMENT_TECHS = VARIABLES_DIR / "cement.yaml"
GAINS_MAPPING = (
    DATA_DIR / "GAINS_emission_factors" / "gains_ecoinvent_sectoral_mapping.yaml"
)
ACTIVITIES_METALS_MAPPING = DATA_DIR / "metals" / "activities_mapping.yml"
HEAT_TECHS = VARIABLES_DIR / "heat.yaml"
PASSENGER_CARS = VARIABLES_DIR / "transport_passenger_cars.yaml"
TWO_WHEELERS = VARIABLES_DIR / "transport_two_wheelers.yaml"
BUSES = VARIABLES_DIR / "transport_bus.yaml"
TRUCKS = VARIABLES_DIR / "transport_road_freight.yaml"
TRAINS = VARIABLES_DIR / "transport_railfreight.yaml"
SHIPS = VARIABLES_DIR / "transport_sea_freight.yaml"
FINAL_ENERGY = VARIABLES_DIR / "final_energy.yaml"
MINING_WASTE = DATA_DIR / "mining" / "tailing_activities.yaml"


def get_mapping(filepath: Path, var: str, model: str = None) -> dict:
    """
    Loa a YAML file and return a dictionary given a variable.
    :param filepath: YAML file path
    :param var: variable to return the dictionary for.
    :param model: if provided, only return the dictionary for this model.
    :return: a dictionary
    """

    with open(filepath, "r", encoding="utf-8") as stream:
        techs = yaml.full_load(stream)

    mapping = {}
    for key, val in techs.items():
        if var in val:
            if model is None:
                mapping[key] = val[var]
            else:
                if model in val.get("iam_aliases", {}):
                    mapping[key] = val[var]

    return mapping


def act_fltr(
    database: List[dict],
    fltr: Union[str, List[str]] = None,
    mask: Union[str, List[str]] = None,
) -> List[dict]:
    """Filter `database` for activities matching field contents given by `fltr` excluding strings in `mask`.
    `fltr`: string, list of strings or dictionary.
    If a string is provided, it is used to match the name field from the start (*startswith*).
    If a list is provided, all strings in the lists are used and results are joined (*or*).
    A dict can be given in the form <fieldname>: <str> to filter for <str> in <fieldname>.
    `mask`: used in the same way as `fltr`, but filters add up with each other (*and*).
    `filter_exact` and `mask_exact`: boolean, set `True` to only allow for exact matches.

    :param database: A lice cycle inventory database
    :type database: brightway2 database object
    :param fltr: value(s) to filter with.
    :type fltr: Union[str, lst, dict]
    :param mask: value(s) to filter with.
    :type mask: Union[str, lst, dict]
    :return: list of activity data set names
    :rtype: list

    """
    if fltr is None:
        fltr = {}
    if mask is None:
        mask = {}

    # default field is name
    if isinstance(fltr, (list, str)):
        fltr = {"name": fltr}
    if isinstance(mask, (list, str)):
        mask = {"name": mask}

    assert len(fltr) > 0, "Filter dict must not be empty."

    # find `act` in `database` that match `fltr`
    # and do not match `mask`
    filters = []
    for field, value in fltr.items():
        if isinstance(value, list):
            filters.extend([ws.either(*[ws.contains(field, v) for v in value])])
        else:
            filters.append(ws.contains(field, value))

    if mask:
        for field, value in mask.items():
            if isinstance(value, list):
                filters.extend([ws.exclude(ws.contains(field, v)) for v in value])
            else:
                filters.append(ws.exclude(ws.contains(field, value)))

    return list(ws.get_many(database, *filters))


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

    def __init__(
        self, database: List[dict], version: str = None, model: str = None
    ) -> None:
        self.database = database
        self.version = version
        self.model = model

        self.powerplant_filters = get_mapping(
            filepath=POWERPLANT_TECHS, var="ecoinvent_aliases", model=self.model
        )

        self.powerplant_max_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="max_efficiency", model=self.model
        )
        self.powerplant_min_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="min_efficiency", model=self.model
        )

        self.powerplant_fuels_filters = get_mapping(
            filepath=POWERPLANT_TECHS, var="ecoinvent_fuel_aliases"
        )

        self.fuels_filters = get_mapping(filepath=FUELS_TECHS, var="ecoinvent_aliases")

        self.cdr_filters = get_mapping(filepath=CDR_TECHS, var="ecoinvent_aliases")

        self.carbon_storage_filters = get_mapping(
            filepath=CARBON_STORAGE_TECHS, var="ecoinvent_aliases"
        )

        self.cement_fuel_filters = get_mapping(
            filepath=CEMENT_TECHS, var="ecoinvent_fuel_aliases"
        )

        self.gains_filters_EU = get_mapping(
            filepath=GAINS_MAPPING, var="ecoinvent_aliases"
        )
        self.heat_filters = get_mapping(filepath=HEAT_TECHS, var="ecoinvent_aliases")

        self.activity_metals_filters = get_mapping(
            filepath=ACTIVITIES_METALS_MAPPING, var="ecoinvent_aliases"
        )

        self.final_energy_filters = get_mapping(
            filepath=FINAL_ENERGY, var="ecoinvent_aliases"
        )

        self.biomass_filters = get_mapping(
            filepath=BIOMASS_TYPES, var="ecoinvent_aliases"
        )

        self.mining_waste_filters = get_mapping(
            filepath=MINING_WASTE, var="ecoinvent_aliases"
        )

    def generate_biomass_map(self) -> dict:
        """
        Filter ecoinvent processes related to biomass.
        Returns a dictionary with biomass names as keys (see below) and
        a set of related ecoinvent activities' names as values.
        """
        return self.generate_sets_from_filters(self.biomass_filters)

    def generate_heat_map(self) -> dict:
        """
        Filter ecoinvent processes related to heat production.

        :return: dictionary with heat prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.heat_filters)

    def generate_activities_using_metals_map(self) -> dict:
        """
        Filter ecoinvent processes related to metals.
        Returns a dictionary with metal names as keys (see below) and
        a set of related ecoinvent activities' names as values.
        """
        return self.generate_sets_from_filters(self.activity_metals_filters)

    def generate_gains_mapping_IAM(self, mapping):
        EU_to_IAM_var = get_mapping(filepath=GAINS_MAPPING, var="gains_aliases_IAM")
        new_map = defaultdict(set)
        for eu, iam in EU_to_IAM_var.items():
            new_map[iam].update(mapping[eu])

        return new_map

    def generate_gains_mapping(self):
        """
        Generate a dictionary with GAINS variables as keys and
        ecoinvent datasets as values.
        """
        return self.generate_sets_from_filters(self.gains_filters_EU)

    def generate_powerplant_map(self) -> dict:
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_filters)

    def generate_cdr_map(self) -> dict:
        """
        Filter ecoinvent processes related to direct air capture.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.cdr_filters)

    def generate_carbon_storage_map(self) -> dict:
        """
        Filter ecoinvent processes related to carbon storage.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.carbon_storage_filters)

    def generate_powerplant_fuels_map(self) -> dict:
        """
        Filter ecoinvent processes related to electricity production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.powerplant_fuels_filters)

    def generate_cement_fuels_map(self) -> dict:
        """
        Filter ecoinvent processes related to cement production.

        :return: dictionary with el. prod. techs as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.cement_fuel_filters)

    def generate_fuel_map(self) -> dict:
        """
        Filter ecoinvent processes related to fuel supply.

        :return: dictionary with fuel names as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.fuels_filters)

    def generate_mining_waste_map(self) -> dict:
        """
        Filter ecoinvent processes related to mining waste.

        :return: dictionary with mining waste names as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.mining_waste_filters)

    def generate_final_energy_map(self) -> dict:
        """
        Filter ecoinvent processes related to final energy consumption.

        :return: dictionary with final energy names as keys (see below) and
            sets of related ecoinvent activities as values.
        :rtype: dict

        """
        return self.generate_sets_from_filters(self.final_energy_filters)

    def generate_transport_map(self, transport_type: str) -> dict:
        """
        Filter ecoinvent processes related to transport.
        Rerurns a dictionary with transport type as keys (see below) and
        a set of related ecoinvent activities' names as values.
        """
        mapping = {}
        if transport_type == "car":
            mapping = self.generate_sets_from_filters(
                get_mapping(
                    filepath=PASSENGER_CARS, var="ecoinvent_aliases", model=self.model
                )
            )
        elif transport_type == "two-wheeler":
            mapping = self.generate_sets_from_filters(
                get_mapping(
                    filepath=TWO_WHEELERS, var="ecoinvent_aliases", model=self.model
                )
            )
        elif transport_type == "bus":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=BUSES, var="ecoinvent_aliases", model=self.model)
            )
        elif transport_type == "truck":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=TRUCKS, var="ecoinvent_aliases", model=self.model)
            )
        elif transport_type == "train":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=TRAINS, var="ecoinvent_aliases", model=self.model)
            )
        elif transport_type == "ship":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=SHIPS, var="ecoinvent_aliases", model=self.model)
            )

        # remove empty values
        mapping = {key: val for key, val in mapping.items() if len(val) > 0}

        return mapping

    def generate_vehicle_fuel_map(self, transport_type: str) -> dict:
        """
        Filter ecoinvent processes related to transport fuels.
        Rerurns a dictionary with transport type as keys (see below) and
        a set of related ecoinvent activities' names as values.
        """
        mapping = {}
        if transport_type == "car":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=PASSENGER_CARS, var="ecoinvent_fuel_aliases")
            )
        elif transport_type == "two-wheeler":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=TWO_WHEELERS, var="ecoinvent_fuel_aliases")
            )
        elif transport_type == "bus":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=BUSES, var="ecoinvent_fuel_aliases")
            )
        elif transport_type == "truck":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=TRUCKS, var="ecoinvent_fuel_aliases")
            )
        elif transport_type == "train":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=TRAINS, var="ecoinvent_fuel_aliases")
            )
        elif transport_type == "ship":
            mapping = self.generate_sets_from_filters(
                get_mapping(filepath=SHIPS, var="ecoinvent_fuel_aliases")
            )

        # remove empty values
        mapping = {key: val for key, val in mapping.items() if len(val) > 0}

        return mapping

    def generate_metals_activities_map(self) -> dict:
        """
        Filter ecoinvent processes related to metals.
        Rerurns a dictionary with material names as keys (see below) and
        a set of related ecoinvent activities' names as values.
        """
        return self.generate_sets_from_filters(self.activity_metals_filters)

    def generate_sets_from_filters(self, filtr: dict, database=None) -> dict:
        """
        Generate a dictionary with sets of activity names for
        technologies from the filter specifications.

        :param filtr:
        :func:`activity_maps.InventorySet.act_fltr`.
        :return: dictionary with the same keys as provided in filter
            and a set of activity data set names as values.
        :rtype: dict
        """

        database = database or self.database

        names = []

        for entry in filtr.values():
            if "fltr" in entry:
                if isinstance(entry["fltr"], dict):
                    if "name" in entry["fltr"]:
                        names.extend(entry["fltr"]["name"])
                elif isinstance(entry["fltr"], list):
                    names.extend(entry["fltr"])
                else:
                    names.append(entry["fltr"])

        subset = list(
            ws.get_many(
                database,
                ws.either(*[ws.contains("name", name) for name in names]),
            )
        )

        techs = {
            tech: act_fltr(subset, fltr.get("fltr"), fltr.get("mask"))
            for tech, fltr in filtr.items()
        }

        mapping = {
            tech: {act["name"] for act in actlst} for tech, actlst in techs.items()
        }

        # check if all keys have values
        # if not, print warning
        for key, val in mapping.items():
            if not val:
                print(f"Warning: No activities found for {key} -- revise mapping.")

        return mapping
