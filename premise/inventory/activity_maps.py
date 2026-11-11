"""Mappings between Premise activities and explicit background families."""

from __future__ import annotations

from collections import defaultdict
import math
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import yaml
import pandas as pd
from wurst import searching as ws

from ..core.filesystem_constants import DATA_DIR, VARIABLES_DIR
from .inventory_store import IndexedInventoryList
from ..core.logger import create_logger
from ..core.utils import load_database

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
_YAML_FULL_LOADER = getattr(yaml, "CFullLoader", yaml.FullLoader)


@lru_cache(maxsize=1)
def _load_mapping_file(filepath: Path) -> Dict[str, dict]:
    """Parse a mapping file once for all variable-specific lookups."""
    with open(filepath, "r", encoding="utf-8") as stream:
        return yaml.load(stream, Loader=_YAML_FULL_LOADER)


def expand_inventory_name_mapping(database, mapping):
    """Bind procedural name roles to reviewed CLIC identities after renaming.

    Legacy aliases come from the shipped identity ledger; they never infer a
    chemistry or physical boundary from substrings in an arbitrary dataset name.
    """
    result = dict(mapping)
    for activity in database:
        provenance = activity.get("clic provenance")
        if not provenance:
            continue
        names = {provenance.get("name")}
        names.update(
            identity[0] for identity in provenance.get("legacy_identities", ())
        )
        values = [
            mapping[name] for name in sorted(name for name in names if name in mapping)
        ]
        if values and any(value != values[0] for value in values[1:]):
            raise ValueError(
                f"Conflicting procedural roles for CLIC dataset {provenance.get('slug')!r}."
            )
        if values:
            result[activity["name"]] = values[0]
    return result


@lru_cache(maxsize=64)
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

    techs = _load_mapping_file(filepath)

    mapping: Dict[str, dict] = {}
    # Keep the shared role vocabulary even where a family has no native alias.
    # These empty definitions may bind reviewed CLIC slugs, never ECI names.
    is_uvek_alias = var in {"uvek_aliases", "uvek_fuel_aliases"}
    shared_var = var.replace("uvek_", "ecoinvent_", 1)
    for key, val in techs.items():
        if var in val or (is_uvek_alias and shared_var in val):
            if model is None or model in val.get("iam_aliases", {}):
                entry = val.get(var, {})
                if (
                    var in {"ecoinvent_aliases", "uvek_aliases"}
                    and isinstance(entry, dict)
                    and Path(filepath).parent.resolve() == VARIABLES_DIR.resolve()
                ):
                    from .clic_requirements import role_slugs

                    slugs = role_slugs(Path(filepath).name, key)
                    if var == "uvek_aliases" and (
                        "native_limitations" in entry
                        or "native_reference_amount" in entry
                    ):
                        limitations = entry.get("native_limitations")
                        reference = entry.get("native_reference_amount")
                        if (
                            not isinstance(limitations, list)
                            or not limitations
                            or any(
                                not isinstance(message, str) or not message
                                for message in limitations
                            )
                            or isinstance(reference, bool)
                            or not isinstance(reference, (int, float))
                            or not math.isfinite(reference)
                            or reference <= 0
                        ):
                            from .clic_archive import ClicInventoryError

                            raise ClicInventoryError(
                                f"Incomplete reviewed UVEK native role declaration for {key!r}."
                            )
                    family_override = var == "uvek_aliases" and "clic_slugs" in entry
                    if family_override:
                        from .clic_archive import ClicInventoryError

                        declared = entry["clic_slugs"]
                        limitations = entry.get("clic_limitations")
                        reference = entry.get("clic_reference_amount")
                        if (
                            not isinstance(declared, list)
                            or not declared
                            or any(
                                not isinstance(slug, str) or not slug
                                for slug in declared
                            )
                            or not isinstance(entry.get("clic_unit"), str)
                            or not entry["clic_unit"]
                            or isinstance(reference, bool)
                            or not isinstance(reference, (int, float))
                            or not math.isfinite(reference)
                            or reference <= 0
                            or not isinstance(limitations, list)
                            or not limitations
                            or any(
                                not isinstance(message, str) or not message
                                for message in limitations
                            )
                        ):
                            raise ClicInventoryError(
                                f"Incomplete reviewed UVEK CLIC role declaration for {key!r}."
                            )
                        slugs = tuple(sorted(set(declared)))
                    if slugs or var == "uvek_aliases":
                        entry = {
                            **entry,
                            "clic_slugs": slugs,
                            "clic_role_key": f"{Path(filepath).name}:{key}",
                            **(
                                {"clic_family_override": True}
                                if family_override
                                else {}
                            ),
                        }
                mapping[key] = entry

    return mapping


FilterType = Union[str, List[str], Dict[str, Union[str, List[str]]]]
ActivityMapping = Dict[str, List[dict]]


def act_fltr(
    database: List[dict],
    fltr: Optional[FilterType] = None,
    mask: Optional[FilterType] = None,
    exact: bool = False,
) -> List[dict]:
    """Filter activities in ``database`` using inclusive and exclusive criteria.

    :param database: Life cycle inventory database to filter.
    :type database: List[dict]
    :param fltr: Filter values used to include activities. Strings apply to the ``name`` field by default.
    :type fltr: Optional[FilterType]
    :param mask: Filter values used to exclude activities.
    :type mask: Optional[FilterType]
    :param exact: Match inclusive field values exactly instead of by substring.
    :type exact: bool
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

    filters = []
    match = ws.equals if exact else ws.contains
    for field, value in fltr.items():
        if isinstance(value, list):
            filters.append(ws.either(*[match(field, item) for item in value]))
        else:
            filters.append(match(field, value))

    for field, value in mask.items():
        values = value if isinstance(value, list) else [value]
        filters.extend(ws.exclude(ws.contains(field, item)) for item in values)

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
    """Generate sector mappings for an explicitly selected background family.

    ``missing_aliases`` records roles lacking native family filters, keyed by
    mapping filename and alias type. Such roles can still match reviewed CLIC
    slugs; they never inherit another family's native names.
    """

    def __init__(
        self,
        database: List[dict],
        version: Optional[str] = None,
        model: Optional[str] = None,
        family: str = "ecoinvent",
    ) -> None:
        """Initialise the inventory set with the source database and metadata.

        :param database: Life cycle inventory database represented as a list of datasets.
        :type database: List[dict]
        :param version: Version identifier of the selected background database.
        :type version: Optional[str]
        :param model: IAM model identifier used to select specific mappings.
        :type model: Optional[str]
        :param family: Background mapping family, either "ecoinvent" or "uvek".
        :type family: str
        :raises ValueError: If the background family is unsupported.
        """

        if family not in {"ecoinvent", "uvek"}:
            raise ValueError(f"Unsupported inventory mapping family: {family!r}.")
        self.family = family
        self.missing_aliases: Dict[str, Tuple[str, ...]] = {}
        self.database = database
        self.version = version
        self.model = model

        self.powerplant_max_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="max_efficiency", model=self.model
        )
        self.powerplant_min_efficiency = get_mapping(
            filepath=POWERPLANT_TECHS, var="min_efficiency", model=self.model
        )

    def _get_aliases(
        self, filepath: Path, model: Optional[str] = None, *, fuels: bool = False
    ) -> Dict[str, dict]:
        """Select native aliases without changing shared IAM or CLIC roles."""
        var = f"{self.family}_{'fuel_aliases' if fuels else 'aliases'}"
        filters = get_mapping(filepath=filepath, var=var, model=model)
        missing = tuple(key for key, entry in filters.items() if not entry.get("fltr"))
        if missing:
            self.missing_aliases[f"{Path(filepath).name}:{var}"] = missing
            for role in missing:
                logger.info(
                    f"{self.model}|{role}|Missing {var} in {Path(filepath).name}; "
                    "only explicit CLIC bindings can match.||"
                )
        return filters

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
        filters = self._get_aliases(filepath=BIOMASS_TYPES)

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
        filters = self._get_aliases(filepath=HEAT_TECHS, model=model)
        return self.generate_sets_from_filters(filters)

    def generate_activities_using_metals_map(self) -> ActivityMapping:
        """Return mapping of metal-using activities to ecoinvent datasets.

        :return: Mapping keyed by metal name.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=ACTIVITIES_METALS_MAPPING)
        return self.generate_sets_from_filters(filters)

    def generate_gains_mapping(self) -> ActivityMapping:
        """Return mapping between GAINS variables and ecoinvent datasets.

        :return: Mapping keyed by GAINS variable name.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=GAINS_MAPPING)
        return self.generate_sets_from_filters(filters)

    def generate_powerplant_map(self) -> ActivityMapping:
        """Return mapping of power plant technologies to activities.

        :return: Mapping keyed by electricity production technology.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=POWERPLANT_TECHS, model=self.model)
        result = self.generate_sets_from_filters(filters)
        # Native templates remain available before proxy creation. Afterwards,
        # bind the role's independent copies without dropping new templates.
        for technology, definition in filters.items():
            suffix = definition.get("technology_copy_suffix")
            if not suffix:
                continue
            sources = {
                (a["name"], a["reference product"], a["location"], a["unit"])
                for a in result[technology]
            }
            copies = [
                a
                for a in self.database
                if a.get("premise powerplant role copy", {}).get("role") == technology
                and tuple(a["premise powerplant role copy"]["source"]) in sources
            ]
            for activity in copies:
                source = activity["premise powerplant role copy"]["source"]
                if activity["name"] != source[0] + suffix:
                    raise ValueError(
                        "Powerplant copy mapping changed; rebuild the scenario."
                    )
            copied = {
                tuple(a["premise powerplant role copy"]["source"]) for a in copies
            }
            result[technology] = copies + [
                a
                for a in result[technology]
                if (a["name"], a["reference product"], a["location"], a["unit"])
                not in copied
            ]
        if self.family != "uvek" or self.model is None:
            return result
        for technology, definition in filters.items():
            additional = definition.get("model_native_aliases", {}).get(self.model)
            if additional is None:
                continue
            if (
                not isinstance(additional, dict)
                or not additional.get("fltr")
                or set(additional) - {"fltr", "mask", "exact"}
            ):
                raise ValueError(
                    f"Invalid model-specific native UVEK aliases for {technology!r}."
                )
            seen = {id(activity) for activity in result[technology]}
            # These filters express the IAM variable's native coverage. They
            # cannot establish or borrow another CLIC foreground's role.
            matches = self.generate_sets_from_filters({technology: additional})[
                technology
            ]
            for activity in matches:
                if id(activity) not in seen:
                    result[technology].append(activity)
                    seen.add(id(activity))
        return result

    def generate_cdr_map(self, model: Optional[str] = None) -> ActivityMapping:
        """Return mapping of carbon dioxide removal technologies to activities.

        :param model: IAM model identifier used to filter the mapping.
        :type model: Optional[str]
        :return: Mapping keyed by carbon removal technology.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=CDR_TECHS, model=model)
        return self.generate_sets_from_filters(filters)

    def generate_powerplant_fuels_map(self) -> ActivityMapping:
        """Return mapping of power plant fuel supply chains to activities.

        :return: Mapping keyed by fuel technology.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=POWERPLANT_TECHS, fuels=True)
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
        filters = self._get_aliases(filepath=CEMENT_TECHS, fuels=True)
        return self.generate_sets_from_filters(filters)

    def generate_steel_map(self) -> ActivityMapping:
        """Return mapping of steel production routes to activities.

        :return: Mapping keyed by steel technology name.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=STEEL_TECHS, model=self.model)
        return self.generate_sets_from_filters(filters)

    def generate_cement_map(self) -> ActivityMapping:
        """Return mapping of cement production routes to activities.

        :return: Mapping keyed by cement technology name.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=CEMENT_TECHS, model=self.model)
        return self.generate_sets_from_filters(filters)

    def generate_fuel_map(
        self, model: Optional[str] = None, *, for_markets: bool = False
    ) -> ActivityMapping:
        """Return mapping of fuel supply chains to activities.

        :param model: IAM model identifier used to filter the mapping.
        :type model: Optional[str]
        :param for_markets: Select declared supply references instead of classification carriers.
        :return: Mapping keyed by fuel name.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=FUELS_TECHS, model=model)
        # Market references can differ from the carriers used to classify heat
        # inputs. Select explicitly reviewed supply aliases without changing the
        # default classification map or any physical conversion equations.
        if for_markets:
            filters = {
                role: {**entry, **entry.get("market_suppliers", {})}
                for role, entry in filters.items()
            }

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
        filters = self._get_aliases(filepath=MINING_WASTE)
        return self.generate_sets_from_filters(filters)

    def generate_final_energy_map(self) -> ActivityMapping:
        """Return mapping of final energy carriers to activities.

        :return: Mapping keyed by energy carrier name.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=FINAL_ENERGY, model=self.model)

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
                self._get_aliases(filepath=PASSENGER_CARS, model=self.model)
            )
        elif transport_type == "two-wheeler":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=TWO_WHEELERS, model=self.model)
            )
        elif transport_type == "bus":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=BUSES, model=self.model)
            )
        elif transport_type == "truck":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=TRUCKS, model=self.model)
            )
        elif transport_type == "train":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=TRAINS, model=self.model)
            )
        elif transport_type == "ship":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=SHIPS, model=self.model)
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
                self._get_aliases(filepath=PASSENGER_CARS, fuels=True)
            )
        elif transport_type == "two-wheeler":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=TWO_WHEELERS, fuels=True)
            )
        elif transport_type == "bus":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=BUSES, fuels=True)
            )
        elif transport_type == "truck":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=TRUCKS, fuels=True)
            )
        elif transport_type == "train":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=TRAINS, fuels=True)
            )
        elif transport_type == "ship":
            mapping = self.generate_sets_from_filters(
                self._get_aliases(filepath=SHIPS, fuels=True)
            )

        # remove empty values
        mapping = {key: val for key, val in mapping.items() if len(val) > 0}

        return mapping

    def generate_metals_activities_map(self) -> ActivityMapping:
        """Return mapping of metal-related activities to datasets.

        :return: Mapping keyed by material name.
        :rtype: ActivityMapping
        """
        filters = self._get_aliases(filepath=ACTIVITIES_METALS_MAPPING)
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
        every_filter_has_name = True

        for entry in filtr.values():
            if "fltr" in entry:
                if isinstance(entry["fltr"], dict):
                    if "name" in entry["fltr"]:
                        name_filter = entry["fltr"]["name"]
                        if isinstance(name_filter, list):
                            names.extend(name_filter)
                        else:
                            names.append(name_filter)
                    else:
                        every_filter_has_name = False
                elif isinstance(entry["fltr"], list):
                    names.extend(entry["fltr"])
                else:
                    names.append(entry["fltr"])
            else:
                every_filter_has_name = False

        unique_names = tuple(dict.fromkeys(names))
        if unique_names and every_filter_has_name:
            matches = ws.get_many(
                database,
                ws.either(*[ws.contains("name", name) for name in unique_names]),
            )
            subset = (
                IndexedInventoryList(matches)
                if isinstance(database, IndexedInventoryList)
                else list(matches)
            )
        else:
            subset = database

        techs = {
            tech: (
                act_fltr(
                    subset,
                    fltr.get("fltr"),
                    fltr.get("mask"),
                    exact=fltr.get("exact", False),
                )
                if fltr.get("fltr")
                else []
            )
            for tech, fltr in filtr.items()
        }

        if self.family == "uvek":
            # Native UVEK identities cannot establish a CLIC scientific role.
            # Published records enter only through the reviewed bindings below.
            techs = {
                technology: [
                    activity
                    for activity in activities
                    if not activity.get("clic provenance")
                    or (
                        filtr[technology].get("allow_generated_helpers", False)
                        and activity.get("clic provenance", {}).get("role")
                        == "generated helper"
                    )
                ]
                for technology, activities in techs.items()
            }

        # Published slugs provide stable role identity when CLIC revises a
        # dataset's name/product or when colliding source identities need a
        # namespace. Native providers retain the existing filter semantics.
        clic_roles = defaultdict(list)
        for technology, definition in filtr.items():
            for slug in definition.get("clic_slugs", ()):
                clic_roles[slug].append(technology)
        if clic_roles or any("clic_role_key" in entry for entry in filtr.values()):

            def eligible(activity, technology):
                provenance = activity.get("clic provenance", {})
                bindings = provenance.get("role_bindings")
                key = filtr[technology].get("clic_role_key")
                if self.family == "uvek":
                    return key is not None and bindings is not None and key in bindings
                return bindings is None or key is None or key in bindings

            for technology, definition in filtr.items():
                if "clic_slugs" in definition:
                    allowed = set(definition["clic_slugs"])
                    techs[technology] = [
                        activity
                        for activity in techs[technology]
                        if not activity.get("clic provenance")
                        or (
                            definition.get("allow_generated_helpers", False)
                            and activity["clic provenance"].get("role")
                            == "generated helper"
                        )
                        or (
                            activity["clic provenance"].get("slug") in allowed
                            and eligible(activity, technology)
                        )
                    ]
            present = {
                technology: {id(item) for item in values}
                for technology, values in techs.items()
            }
            for activity in database:
                slug = activity.get("clic provenance", {}).get("slug")
                for technology in clic_roles.get(slug, ()):
                    if id(activity) not in present[technology] and eligible(
                        activity, technology
                    ):
                        techs[technology].append(activity)
                        present[technology].add(id(activity))

        mapping = techs

        # check if all keys have values
        # if not, log
        for key, val in mapping.items():
            if not val:
                logger.info(
                    f"{self.model}|{key}|No activities found for this technology "
                    f"(family={self.family}).||"
                )
            # else:
            #    for v in val:
            #        logger.info(
            #            f"{self.model}|{key}|{v['name']}|{v['reference product']}|{v['location']}"
            #        )

        return mapping
