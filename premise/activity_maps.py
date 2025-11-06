"""Mappings between Premise activities and their ecoinvent counterparts."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import yaml
import pandas as pd
from wurst import searching as ws

from .filesystem_constants import DATA_DIR, VARIABLES_DIR
from .utils import load_database
from .logger import create_logger

logger = create_logger("mapping")

POWERPLANT_TECHS = VARIABLES_DIR / "electricity.yaml"
FUELS_TECHS = VARIABLES_DIR / "fuels.yaml"
BIOMASS_TYPES = VARIABLES_DIR / "biomass.yaml"
METALS_TECHS = DATA_DIR / "metals" / "activities_mapping.yml"
CDR_TECHS = VARIABLES_DIR / "carbon_dioxide_removal.yaml"
CEMENT_TECHS = VARIABLES_DIR / "cement.yaml"
GAINS_MAPPING = (
    DATA_DIR / "GAINS_emission_factors" / "gains_ecoinvent_sectoral_mapping.yaml"
)
STEEL_TECHS = VARIABLES_DIR / "steel.yaml"
ACTIVITIES_METALS_MAPPING = DATA_DIR / "metals" / "activities_mapping.yml"
HEAT_TECHS = VARIABLES_DIR / "heat.yaml"
PASSENGER_CARS = VARIABLES_DIR / "transport_passenger_cars.yaml"
TWO_WHEELERS = VARIABLES_DIR / "transport_two_wheelers.yaml"
BUSES = VARIABLES_DIR / "transport_bus.yaml"
TRUCKS = VARIABLES_DIR / "transport_road_freight.yaml"
TRAINS = VARIABLES_DIR / "transport_rail_freight.yaml"
SHIPS = VARIABLES_DIR / "transport_sea_freight.yaml"
FINAL_ENERGY = VARIABLES_DIR / "final_energy.yaml"
MINING_WASTE = DATA_DIR / "mining" / "tailings_activities.yaml"
CARBON_STORAGE_TECHS = VARIABLES_DIR / "carbon_dioxide_removal.yaml"


def get_mapping(
    filepath: Path, var: str, model: Optional[str] = None
) -> Dict[str, dict]:
    """Load a YAML mapping file and return the entries for ``var``.

    :param filepath: Path to the YAML file containing the mappings.
    :type filepath: pathlib.Path
    :param var: Variable to extract from the mapping.
    :type var: str
    :param model: Optional model identifier used to filter the mapping entries.
    :type model: Optional[str]
    :return: Dictionary where keys correspond to activity names and values to mapping metadata.
    :rtype: Dict[str, dict]
    """

    with open(filepath, "r", encoding="utf-8") as stream:
        techs = yaml.full_load(stream)

    mapping: Dict[str, dict] = {}
    for key, val in techs.items():
        if var in val:
            if model is None or model in val.get("iam_aliases", {}):
                mapping[key] = val[var]

    return mapping


FilterType = Union[str, List[str], Dict[str, Union[str, List[str]]]]
ActivityMapping = Dict[str, List[dict]]


def act_fltr(
    database: List[dict],
    fltr: Optional[FilterType] = None,
    mask: Optional[FilterType] = None,
) -> List[dict]:
    """Filter activities in ``database`` using inclusive and exclusive criteria.

    :param database: Life cycle inventory database to filter.
    :type database: List[dict]
    :param fltr: Filter values used to include activities. Strings apply to the ``name`` field by default.
    :type fltr: Optional[FilterType]
    :param mask: Filter values used to exclude activities.
    :type mask: Optional[FilterType]
    :return: List of activities matching the filter conditions.
    :rtype: List[dict]
    :raises AssertionError: If no filter values are provided.
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


def mapping_to_dataframe(
    scenario: Dict[str, Union[str, List[dict]]],
    original_database: Optional[List[dict]] = None,
) -> pd.DataFrame:
    """Convert mapping dictionaries into a grouped :class:`pandas.DataFrame`.

    :param scenario: Scenario dictionary containing a ``database`` key with the ecoinvent database.
    :type scenario: Dict[str, Union[str, List[dict]]]
    :param original_database: Optional original database used to reload the scenario.
    :type original_database: Optional[List[dict]]
    :return: DataFrame with aggregated mapping information and location lists.
    :rtype: pandas.DataFrame
    """

    temp_records: List[Tuple[str, str, str, str, str]] = []

    if "database" not in scenario:
        scenario = load_database(scenario, original_database=original_database)

    inv = InventorySet(
        database=scenario["database"],
        version=scenario.get("version", None),
        model=scenario.get("model", None),
    )
    for sector, mapping in [
        ("biomass", inv.generate_biomass_map()),
        ("heat", inv.generate_heat_map(model=scenario.get("model"))),
        ("cdr", inv.generate_cdr_map()),
        ("cement fuels", inv.generate_cement_fuels_map()),
        ("final energy", inv.generate_final_energy_map()),
        ("fuel", inv.generate_fuel_map()),
        ("gains", inv.generate_gains_mapping()),
        ("powerplant", inv.generate_powerplant_map()),
        ("powerplant fuels", inv.generate_powerplant_fuels_map()),
        ("steel", inv.generate_steel_map()),
        ("mining waste", inv.generate_mining_waste_map()),
        ("car", inv.generate_transport_map("car")),
        ("two-wheelers", inv.generate_transport_map("two-wheeler")),
        ("bus", inv.generate_transport_map("bus")),
        ("truck", inv.generate_transport_map("truck")),
        ("train", inv.generate_transport_map("train")),
        ("ship", inv.generate_transport_map("ship")),
    ]:
        for category, activities in mapping.items():
            for act in activities:
                temp_records.append(
                    (
                        sector,
                        category,
                        act.get("name"),
                        act.get("reference product"),
                        act.get("location"),
                    )
                )

    # Deduplicate and sort
    temp_records = list(set(temp_records))

    df = pd.DataFrame(
        temp_records,
        columns=["Sector", "Category", "Name", "Reference product", "Location"],
    )

    grouped_df = (
        df.groupby(["Sector", "Category", "Name", "Reference product"])["Location"]
        .unique()
        .reset_index()
    )

    # Optional: convert list of locations to string
    grouped_df["Location"] = grouped_df["Location"].apply(
        lambda x: ", ".join(sorted(x))
    )

    # Optional: visually hide duplicate sectors and categories
    grouped_df.loc[grouped_df["Sector"].duplicated(), "Sector"] = ""
    grouped_df.loc[grouped_df["Category"].duplicated(), "Category"] = ""

    return grouped_df


class InventorySet:
    """Generate activity mappings between Premise sectors and ecoinvent datasets."""

    def __init__(
        self,
        database: List[dict],
        version: Optional[str] = None,
        model: Optional[str] = None,
    ) -> None:
        """Initialise the inventory set with the source database and metadata.

        :param database: Life cycle inventory database represented as a list of datasets.
        :type database: List[dict]
        :param version: Version identifier of the ecoinvent database.
        :type version: Optional[str]
        :param model: IAM model identifier used to select specific mappings.
        :type model: Optional[str]
        """

        self.database = database
        self.version = version
        self.model = model

        self.powerplant_max_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="max_efficiency", model=self.model
        )
        self.powerplant_min_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="min_efficiency", model=self.model
        )

    def generate_map(self, filters: Dict[str, dict]) -> ActivityMapping:
        """Generate an activity mapping using the provided filter definitions.

        :param filters: Mapping specifications used to query the database.
        :type filters: Dict[str, dict]
        :return: Dictionary with technology keys and matching datasets.
        :rtype: ActivityMapping
        """

        return self.generate_sets_from_filters(filters)

    def generate_biomass_map(self) -> ActivityMapping:
        """Return mapping of biomass technologies to ecoinvent activities.

        :return: Mapping keyed by biomass technology name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(filepath=BIOMASS_TYPES, var="ecoinvent_aliases")

        lhv = get_mapping(filepath=BIOMASS_TYPES, var="lhv")

        sets = self.generate_sets_from_filters(filters)

        # add LHV info to datasets in sets
        for key, activities in sets.items():
            if key in lhv:
                for act in activities:
                    act["lhv"] = lhv[key]

        return sets

    def generate_heat_map(self, model: Optional[str]) -> ActivityMapping:
        """Return mapping of heat production technologies to activities.

        :param model: IAM model identifier used to filter the mapping.
        :type model: Optional[str]
        :return: Mapping keyed by heat technology name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(filepath=HEAT_TECHS, var="ecoinvent_aliases", model=model)
        return self.generate_sets_from_filters(filters)

    def generate_activities_using_metals_map(self) -> ActivityMapping:
        """Return mapping of metal-using activities to ecoinvent datasets.

        :return: Mapping keyed by metal name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(
            filepath=ACTIVITIES_METALS_MAPPING, var="ecoinvent_aliases"
        )
        return self.generate_sets_from_filters(filters)

    def generate_gains_mapping(self) -> ActivityMapping:
        """Return mapping between GAINS variables and ecoinvent datasets.

        :return: Mapping keyed by GAINS variable name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(filepath=GAINS_MAPPING, var="ecoinvent_aliases")
        return self.generate_sets_from_filters(filters)

    def generate_powerplant_map(self) -> ActivityMapping:
        """Return mapping of power plant technologies to activities.

        :return: Mapping keyed by electricity production technology.
        :rtype: ActivityMapping
        """
        filters = get_mapping(
            filepath=POWERPLANT_TECHS, var="ecoinvent_aliases", model=self.model
        )
        return self.generate_sets_from_filters(filters)

    def generate_cdr_map(self, model: Optional[str] = None) -> ActivityMapping:
        """Return mapping of carbon dioxide removal technologies to activities.

        :param model: IAM model identifier used to filter the mapping.
        :type model: Optional[str]
        :return: Mapping keyed by carbon removal technology.
        :rtype: ActivityMapping
        """
        filters = get_mapping(filepath=CDR_TECHS, var="ecoinvent_aliases", model=model)
        return self.generate_sets_from_filters(filters)

    def generate_powerplant_fuels_map(self) -> ActivityMapping:
        """Return mapping of power plant fuel supply chains to activities.

        :return: Mapping keyed by fuel technology.
        :rtype: ActivityMapping
        """
        filters = get_mapping(filepath=POWERPLANT_TECHS, var="ecoinvent_fuel_aliases")
        return self.generate_sets_from_filters(filters)

    def generate_powerplant_efficiency_bounds(
        self,
    ) -> Tuple[Dict[str, dict], Dict[str, dict]]:
        """Return minimum and maximum efficiency mappings for power plants.

        :return: Tuple ``(min_efficiency, max_efficiency)`` containing lookup dictionaries.
        :rtype: Tuple[Dict[str, dict], Dict[str, dict]]
        """
        min_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="min_efficiency", model=self.model
        )
        max_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="max_efficiency", model=self.model
        )

        return min_efficiency, max_efficiency

    def generate_cement_fuels_map(self) -> ActivityMapping:
        """Return mapping of cement production fuels to ecoinvent activities.

        :return: Mapping keyed by cement fuel category.
        :rtype: ActivityMapping
        """
        filters = get_mapping(filepath=CEMENT_TECHS, var="ecoinvent_fuel_aliases")
        return self.generate_sets_from_filters(filters)

    def generate_steel_map(self) -> ActivityMapping:
        """Return mapping of steel production routes to activities.

        :return: Mapping keyed by steel technology name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(
            filepath=STEEL_TECHS, var="ecoinvent_aliases", model=self.model
        )
        return self.generate_sets_from_filters(filters)

    def generate_cement_map(self) -> ActivityMapping:
        """Return mapping of cement production routes to activities.

        :return: Mapping keyed by cement technology name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(
            filepath=CEMENT_TECHS, var="ecoinvent_aliases", model=self.model
        )
        return self.generate_sets_from_filters(filters)

    def generate_fuel_map(self, model: Optional[str] = None) -> ActivityMapping:
        """Return mapping of fuel supply chains to activities.

        :param model: IAM model identifier used to filter the mapping.
        :type model: Optional[str]
        :return: Mapping keyed by fuel name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(
            filepath=FUELS_TECHS, var="ecoinvent_aliases", model=model
        )

        lhv = get_mapping(filepath=FUELS_TECHS, var="lhv")

        sets = self.generate_sets_from_filters(filters)

        # add LHV info to datasets in sets
        for key, activities in sets.items():
            if key in lhv:
                for act in activities:
                    act["lhv"] = lhv[key]

        return sets

    def generate_mining_waste_map(self) -> ActivityMapping:
        """Return mapping of mining waste management activities.

        :return: Mapping keyed by mining waste type.
        :rtype: ActivityMapping
        """
        filters = get_mapping(filepath=MINING_WASTE, var="ecoinvent_aliases")
        return self.generate_sets_from_filters(filters)

    def generate_final_energy_map(self) -> ActivityMapping:
        """Return mapping of final energy carriers to activities.

        :return: Mapping keyed by energy carrier name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(
            filepath=FINAL_ENERGY, var="ecoinvent_aliases", model=self.model
        )

        lhv = get_mapping(filepath=FINAL_ENERGY, var="lhv")

        sets = self.generate_sets_from_filters(filters)

        # add LHV info to datasets in sets
        for key, activities in sets.items():
            if key in lhv:
                for act in activities:
                    act["lhv"] = lhv[key]

        return sets

    def generate_transport_map(self, transport_type: str) -> ActivityMapping:
        """Return mapping of transport technologies to activities.

        :param transport_type: Transport mode to retrieve (e.g. ``"car"``).
        :type transport_type: str
        :return: Mapping keyed by transport technology.
        :rtype: ActivityMapping
        """

        mapping: ActivityMapping = {}
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

    def generate_vehicle_fuel_map(self, transport_type: str) -> ActivityMapping:
        """Return mapping of transport fuel supply chains to activities.

        :param transport_type: Transport mode to retrieve fuel mappings for.
        :type transport_type: str
        :return: Mapping keyed by transport fuel type.
        :rtype: ActivityMapping
        """

        mapping: ActivityMapping = {}
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

    def generate_metals_activities_map(self) -> ActivityMapping:
        """Return mapping of metal-related activities to datasets.

        :return: Mapping keyed by material name.
        :rtype: ActivityMapping
        """
        filters = get_mapping(
            filepath=ACTIVITIES_METALS_MAPPING, var="ecoinvent_aliases"
        )
        return self.generate_sets_from_filters(filters)

    def generate_sets_from_filters(
        self,
        filtr: Dict[str, Dict[str, FilterType]],
        database: Optional[List[dict]] = None,
    ) -> ActivityMapping:
        """Generate activity mappings using Wurst filter specifications.

        :param filtr: Filter configuration mapping technology names to filter definitions.
        :type filtr: Dict[str, Dict[str, FilterType]]
        :param database: Optional database subset to apply the filters on.
        :type database: Optional[List[dict]]
        :return: Mapping keyed by technology name with matching activities as values.
        :rtype: ActivityMapping
        """

        database = database or self.database

        names: List[str] = []

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

        mapping = techs

        # check if all keys have values
        # if not, print warning
        for key, val in mapping.items():
            if not val:
                logger.info(
                    f"{self.model}|{key}|No activities found for this technology.||"
                )
            # else:
            #    for v in val:
            #        logger.info(
            #            f"{self.model}|{key}|{v['name']}|{v['reference product']}|{v['location']}"
            #        )

        return mapping
