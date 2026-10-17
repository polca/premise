"""Utility helpers to clean and normalise life cycle inventory datasets."""

from __future__ import annotations

import csv
import pprint
from numbers import Number
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import bw2io
import numpy as np
import wurst
from wurst.brightway.extract_database import extract_brightway2_databases

wurst.extract_brightway2_databases = extract_brightway2_databases
import yaml
from bw2data.database import DatabaseChooser
from tqdm import tqdm
from wurst import searching as ws

from ._bw2_backend_compat import ActivityDataset, ExchangeDataset, SQLiteBackend
from .data_collection import get_delimiter
from .filesystem_constants import DATA_DIR

CA_CEMENT_REFRACTORY_VERSIONS = frozenset({"3.11", "3.12"})
CA_CEMENT_REFRACTORY_CACHE_REVISION = 1
ICE_ALUMINIUM_VERSIONS = frozenset({"3.10", "3.11", "3.12"})
ICE_ALUMINIUM_CACHE_REVISION = 1

try:
    from bw2data.configuration import labels
except ImportError:

    class _LegacyLabels:
        biosphere_edge_types = {"biosphere"}

    labels = _LegacyLabels()


def load_methane_correction_list() -> List[str]:
    """Load the ``biomethane_correction.yaml`` file distributed with Premise.

    :return: Names of the biogas activities requiring methane corrections.
    :rtype: List[str]
    """

    with open(
        DATA_DIR / "fuels" / "biomethane_correction.yaml", encoding="utf-8"
    ) as file:
        methane_correction_list: List[str] = yaml.safe_load(file)
    return methane_correction_list


def remove_uncertainty(database: List[dict]) -> List[dict]:
    """Remove uncertainty information from database exchanges.

    :param database: Inventory database to clean.
    :type database: List[dict]
    :return: The same database with uncertainty metadata reset.
    :rtype: List[dict]
    """

    uncertainty_keys = ["scale", "shape", "minimum", "maximum"]
    nan_value = np.nan

    for dataset in database:
        for exchange in dataset["exchanges"]:
            exchange["uncertainty type"] = 0
            exchange["loc"] = float(exchange["amount"])
            for key in uncertainty_keys:
                exchange[key] = nan_value

    return database


def get_biosphere_flow_uuid(version: str) -> Dict[Tuple[str, str, str, str], str]:
    """Return a mapping between biosphere flow descriptors and their UUIDs.

    :param version: Ecoinvent version number used to select the appropriate
        lookup table.
    :type version: str
    :return: Mapping with keys ``(name, category, subcategory, unit)``.
    :rtype: Dict[Tuple[str, str, str, str], str]
    :raises FileNotFoundError: If the lookup table for the given version is missing.
    """

    if version == "3.12":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_312.csv"
    elif version == "3.11":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_311.csv"
    elif version == "3.10":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_310.csv"
    elif version == "3.9":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_39.csv"
    else:
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"

    if not Path(fp).is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(fp, encoding="utf-8") as file:
        input_dict = csv.reader(
            file,
            delimiter=get_delimiter(filepath=fp),
        )
        for row in input_dict:
            csv_dict[(row[0], row[1], row[2], row[3])] = row[-1]

    return csv_dict


def get_biosphere_flow_categories(
    version: str,
) -> Dict[str, Union[Tuple[str], Tuple[str, str]]]:
    """Return biosphere flow categories keyed by flow UUID.

    :param version: Ecoinvent version number used to select the lookup table.
    :type version: str
    :return: Mapping between flow UUIDs and their category tuples.
    :rtype: Dict[str, Union[Tuple[str], Tuple[str, str]]]
    """

    data = get_biosphere_flow_uuid(version)

    return {
        v: (k[1], k[2]) if k[2] != "unspecified" else (k[1],) for k, v in data.items()
    }


def remove_nones(database: List[dict]) -> List[dict]:
    """Remove exchanges with ``None`` values from the database.

    :param database: Wurst inventory database.
    :type database: List[dict]
    :return: Database with every exchange cleaned from ``None`` values.
    :rtype: List[dict]
    """

    def exists(exchange: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in exchange.items() if value is not None}

    for dataset in database:
        dataset["exchanges"] = [exists(exc) for exc in dataset["exchanges"]]

    return database


def remove_categories(database: List[dict]) -> List[dict]:
    """Remove category metadata from datasets and exchanges.

    :param database: Wurst inventory database.
    :type database: List[dict]
    :return: Database without category fields.
    :rtype: List[dict]
    """
    for dataset in database:
        if "categories" in dataset:
            del dataset["categories"]
        for exc in dataset["exchanges"]:
            try:
                if exc["type"] in ["production", "technosphere"]:
                    if "categories" in exc:
                        del exc["categories"]
            except KeyError:
                print(f"Exchange {exc['name']} in {dataset['name']} has no type")
                pass

    return database


def strip_string_from_spaces(database: List[dict]) -> List[dict]:
    """Strip whitespace and special spacing characters from text fields.

    :param database: Wurst inventory database.
    :type database: List[dict]
    :return: Database with normalised string fields.
    :rtype: List[dict]
    """
    for dataset in database:
        dataset["name"] = dataset["name"].strip()
        # also check for unicode characters like \xa0
        dataset["name"] = dataset["name"].replace("\xa0", "")

        dataset["reference product"] = dataset["reference product"].strip()
        dataset["location"] = dataset["location"].strip()
        for exc in dataset["exchanges"]:
            exc["name"] = exc["name"].strip()
            # also check for unicode characters like \xa0
            exc["name"] = exc["name"].replace("\xa0", "")
            if exc.get("product"):
                exc["product"] = exc["product"].strip()
                # also check for unicode characters like \xa0
                exc["product"] = exc["product"].replace("\xa0", "")
            if exc.get("reference product"):
                exc["reference product"] = exc["reference product"].strip()
                # also check for unicode characters like \xa0
                exc["reference product"] = exc["reference product"].replace("\xa0", "")
            if exc.get("location"):
                exc["location"] = exc["location"].strip()
            if exc.get("unit"):
                exc["unit"] = exc["unit"].strip()

    return database


def _extract_parameters(parameters: Any) -> Dict[str, Any]:
    if isinstance(parameters, dict):
        return {
            name: value["amount"] if isinstance(value, dict) else value
            for name, value in parameters.items()
            if not isinstance(value, dict) or "amount" in value
        }

    return {
        item["name"]: item["amount"]
        for item in parameters
        if isinstance(item, dict) and "name" in item and "amount" in item
    }


def _extract_activity_for_premise(
    proxy: ActivityDataset, add_identifiers: bool = False
) -> Dict[str, Any]:
    obj = {
        "location": proxy.location,
        "database": proxy.database,
        "code": proxy.code,
        "name": proxy.name,
        "reference product": proxy.product,
        "unit": proxy.data.get("unit", ""),
        "exchanges": [],
        "type": proxy.type,
    }

    classifications = proxy.data.get("classifications")
    if classifications:
        obj["classifications"] = classifications

    comment = proxy.data.get("comment")
    if comment:
        obj["comment"] = comment

    categories = proxy.data.get("categories")
    if categories:
        obj["categories"] = categories

    parameters = _extract_parameters(proxy.data.get("parameters", []))
    if parameters:
        obj["parameters"] = parameters

    if add_identifiers:
        obj["id"] = proxy.id

    return obj


def _extract_exchange_for_premise(
    proxy: ExchangeDataset, add_properties: bool = False
) -> Dict[str, Any]:
    uncertainty_fields = (
        "uncertainty type",
        "loc",
        "scale",
        "shape",
        "minimum",
        "maximum",
        "amount",
        "pedigree",
    )
    data = {key: proxy.data[key] for key in uncertainty_fields if key in proxy.data}
    assert "amount" in data, "Exchange has no `amount` field"

    if "uncertainty type" not in data:
        data["uncertainty type"] = 0
        data["loc"] = data["amount"]

    data["type"] = proxy.type

    production_volume = proxy.data.get("production volume")
    if production_volume is not None:
        data["production volume"] = production_volume

    data["input"] = (proxy.input_database, proxy.input_code)
    data["output"] = (proxy.output_database, proxy.output_code)

    if add_properties:
        properties = proxy.data.get("properties")
        if properties:
            data["properties"] = properties

    return data


def _add_exchanges_to_consumers_for_premise(
    activities: List[Dict[str, Any]],
    exchange_qs,
    exchange_count: Optional[int] = None,
    add_properties: bool = False,
) -> List[Dict[str, Any]]:
    lookup = {(o["database"], o["code"]): o for o in activities}

    with tqdm(total=exchange_count) as pbar:
        for exc_proxy in exchange_qs.iterator():
            exc = _extract_exchange_for_premise(
                exc_proxy, add_properties=add_properties
            )
            output = tuple(exc.pop("output"))
            lookup[output]["exchanges"].append(exc)
            pbar.update(1)

    return activities


def _add_input_info_for_indigenous_exchanges_for_premise(
    activities: List[Dict[str, Any]],
    names,
    add_identifiers: bool = False,
) -> None:
    names = set(names)
    lookup = {(o["database"], o["code"]): o for o in activities}

    for ds in activities:
        for exc in ds["exchanges"]:
            if "input" not in exc or exc["input"][0] not in names:
                continue

            obj = lookup[exc["input"]]
            exc["product"] = obj.get("reference product")
            exc["name"] = obj.get("name")
            exc["unit"] = obj.get("unit")
            exc["location"] = obj.get("location")

            if add_identifiers:
                exc["id"] = obj["id"]
                exc["code"] = obj["code"]

            if exc["type"] in labels.biosphere_edge_types and obj.get("categories"):
                exc["categories"] = obj["categories"]

            exc.pop("input")


def _add_input_info_for_external_exchanges_for_premise(
    activities: List[Dict[str, Any]],
    names,
    add_identifiers: bool = False,
) -> None:
    names = set(names)
    cache = {}

    for ds in tqdm(activities):
        for exc in ds["exchanges"]:
            if "input" not in exc or exc["input"][0] in names:
                continue

            if exc["input"] not in cache:
                cache[exc["input"]] = ActivityDataset.get(
                    ActivityDataset.database == exc["input"][0],
                    ActivityDataset.code == exc["input"][1],
                )

            obj = cache[exc["input"]]
            exc["name"] = obj.name
            exc["product"] = obj.product
            exc["unit"] = obj.data.get("unit")
            exc["location"] = obj.location

            if add_identifiers:
                exc["id"] = obj.id
                exc["code"] = obj.code

            categories = obj.data.get("categories")
            if exc["type"] in labels.biosphere_edge_types and categories:
                exc["categories"] = categories


def extract_brightway_databases_for_premise(
    database_names,
    add_properties: bool = False,
    add_identifiers: bool = False,
) -> List[Dict[str, Any]]:
    if isinstance(database_names, str):
        database_names = [database_names]

    error = "Must pass list of database names"
    assert isinstance(database_names, (list, tuple, set)), error

    databases = [DatabaseChooser(name) for name in database_names]
    error = "Wrong type of database object (must be SQLiteBackend)"
    assert all(isinstance(obj, SQLiteBackend) for obj in databases), error

    activity_qs = ActivityDataset.select().where(
        ActivityDataset.database << database_names
    )
    exchange_qs = ExchangeDataset.select().where(
        ExchangeDataset.output_database << database_names
    )
    activity_count = activity_qs.count()
    exchange_count = exchange_qs.count()

    print("Getting activity data")
    activities = [
        _extract_activity_for_premise(o, add_identifiers=add_identifiers)
        for o in tqdm(activity_qs.iterator(), total=activity_count)
    ]

    print("Adding exchange data to activities")
    _add_exchanges_to_consumers_for_premise(
        activities,
        exchange_qs,
        exchange_count=exchange_count,
        add_properties=add_properties,
    )

    print("Filling out exchange data")
    _add_input_info_for_indigenous_exchanges_for_premise(
        activities, database_names, add_identifiers=add_identifiers
    )
    _add_input_info_for_external_exchanges_for_premise(
        activities, database_names, add_identifiers=add_identifiers
    )

    return activities


class DatabaseCleaner:
    """Clean datasets contained in inventory databases for further processing."""

    def __init__(
        self,
        source_db: str,
        source_type: str,
        source_file_path: Optional[Path],
        version: str,
    ) -> None:
        """Create a cleaner for Brightway or EcoSpold data sources.

        :param source_db: Name of the source database or desired Brightway database key.
        :type source_db: str
        :param source_type: Type of the source database, either ``"brightway"`` or ``"ecospold"``.
        :type source_type: str
        :param source_file_path: Path to the EcoSpold directory when ``source_type`` is ``"ecospold"``.
        :type source_file_path: Optional[pathlib.Path]
        :param version: Version identifier of the ecoinvent data.
        :type version: str
        :raises NameError: If the Brightway database is empty.
        """

        if source_type == "brightway":
            # Check that database exists
            if len(DatabaseChooser(source_db)) == 0:
                raise NameError(
                    "The database selected is empty. Make sure the name is correct"
                )
            self.database = extract_brightway_databases_for_premise(source_db)
            self.database = remove_categories(self.database)
            # strip strings form spaces
            self.database = strip_string_from_spaces(self.database)

        if source_type == "ecospold":
            if source_file_path is None:
                raise ValueError(
                    "`source_file_path` must be provided when using EcoSpold data."
                )
            # The ecospold data needs to be formatted
            ecoinvent = bw2io.SingleOutputEcospold2Importer(
                str(source_file_path),
                source_db,
                use_mp=False,
            )

            ecoinvent.apply_strategies()
            self.database = ecoinvent.data
            # strip strings form spaces
            self.database = strip_string_from_spaces(self.database)

            # Location field is added to exchanges
            self.add_location_field_to_exchanges()
            # Product field is added to exchanges
            self.add_product_field_to_exchanges()
            # Parameter field is converted from a list to a dictionary
            self.transform_parameter_field()
        self.version = version

    def find_product_given_lookup_dict(self, lookup_dict: Dict[str, str]) -> List[str]:
        """Return products matching the filters in ``lookup_dict``.

        :param lookup_dict: Field/value pairs used to filter activities.
        :type lookup_dict: Dict[str, str]
        :return: List of product names corresponding to the filters.
        :rtype: List[str]
        """
        return [
            x["product"]
            for x in wurst.searching.get_many(
                self.database, *[ws.equals(k, v) for k, v in lookup_dict.items()]
            )
        ]

    def find_location_given_lookup_dict(self, lookup_dict: Dict[str, str]) -> List[str]:
        """Return locations matching the filters in ``lookup_dict``.

        :param lookup_dict: Field/value pairs used to filter activities.
        :type lookup_dict: Dict[str, str]
        :return: List of location identifiers corresponding to the filters.
        :rtype: List[str]
        """
        return [
            x["location"]
            for x in wurst.searching.get_many(
                self.database, *[ws.equals(k, v) for k, v in lookup_dict.items()]
            )
        ]

    def add_location_field_to_exchanges(self) -> None:
        """Add the ``location`` key to production and technosphere exchanges.

        :raises KeyError: If no matching activity can be found for an exchange input.
        """
        d_location = {(a["database"], a["code"]): a["location"] for a in self.database}
        for dataset in self.database:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "technosphere":
                    exc_input = exchange["input"]
                    exchange["location"] = d_location[exc_input]

    def add_product_field_to_exchanges(self) -> None:
        """Populate the ``product`` key on production and technosphere exchanges.

        :raises KeyError: If no corresponding activity can be found for an exchange input.
        """
        # Create a dictionary that contains the 'code' field as key and the 'product' field as value
        d_product = {
            a["code"]: (a["reference product"], a["name"]) for a in self.database
        }
        # Add a `product` field to the production exchange
        for dataset in self.database:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "production":
                    if "product" not in exchange:
                        exchange["product"] = dataset["reference product"]

                    if exchange["name"] != dataset["name"]:
                        exchange["name"] = dataset["name"]

        # Add a `product` field to technosphere exchanges
        for dataset in self.database:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "technosphere":
                    # Check if the field 'product' is present
                    if "product" not in exchange:
                        exchange["product"] = d_product[exchange["input"][1]][0]

                    # If a 'reference product' field is present,
                    # we make sure it matches with the new 'product' field
                    if "reference product" in exchange:
                        try:
                            assert exchange["product"] == exchange["reference product"]
                        except AssertionError:
                            exchange["product"] = d_product[exchange["input"][1]][0]

                    # Ensure the name is correct
                    exchange["name"] = d_product[exchange["input"][1]][1]

    def transform_parameter_field(self) -> None:
        """Transform the parameter field from lists to dictionaries."""
        # When handling ecospold files directly, the parameter field is a list.
        # It is here transformed into a dictionary
        for dataset in self.database:
            dataset["parameters"] = {
                k["name"]: k["amount"] for k in dataset["parameters"]
            }

    # Functions to clean up Wurst import and additional technologies
    def fix_unset_technosphere_and_production_exchange_locations(
        self, matching_fields: Tuple[str, str] = ("name", "unit")
    ) -> None:
        """Fill missing locations for production and technosphere exchanges.

        :param matching_fields: Fields used to look up potential location matches.
        :type matching_fields: Tuple[str, str]
        """
        for dataset in self.database:
            # collect production exchanges that simply do not have a location key and set it to
            # the location of the dataset
            for exc in wurst.production(dataset):
                if "location" not in exc:
                    exc["location"] = dataset["location"]

            for exc in wurst.technosphere(dataset):
                if "location" not in exc:
                    locs = self.find_location_given_lookup_dict(
                        {k: exc.get(k) for k in matching_fields}
                    )
                    if len(locs) == 1:
                        exc["location"] = locs[0]
                    else:
                        print(
                            f"No unique location found for exchange:\n{pprint.pformat(exc)}\nFound: {locs}"
                        )

    def fix_biosphere_flow_categories(self) -> None:
        """Ensure biosphere exchanges include category information."""

        dict_bio_cat = get_biosphere_flow_categories(self.version)
        dict_bio_uuid = get_biosphere_flow_uuid(self.version)

        for dataset in self.database:
            for exc in dataset["exchanges"]:
                if exc["type"] == "biosphere":
                    if "categories" not in exc:
                        # from the uuid, fetch the flow category
                        if "input" in exc:
                            if exc["input"][1] in dict_bio_cat:
                                key = exc["input"][1]
                                exc["categories"] = dict_bio_cat[key]
                            else:
                                print(f"no flow code for {exc['name']}")
                                exc["delete"] = True

                        elif "flow" in exc:
                            if exc["flow"] in dict_bio_cat:
                                key = exc["flow"]
                                exc["categories"] = dict_bio_cat[key]
                            else:
                                print(f"no flow code for {exc['name']}")
                                exc["delete"] = True

                        else:
                            print(f"no input or categories for {exc['name']}")
                            exc["delete"] = True

                    if "input" not in exc:
                        if "flow" in exc:
                            exc["input"] = ("biosphere3", exc["flow"])

                        elif "categories" in exc:
                            # from the category, fetch the uuid of that biosphere flow
                            cat = (
                                exc["categories"]
                                if len(exc["categories"]) > 1
                                else (exc["categories"][0], "unspecified")
                            )
                            uuid = dict_bio_uuid[
                                exc["name"], cat[0], cat[1], exc["unit"]
                            ]
                            exc["input"] = ("biosphere3", uuid)

                            if "delete" in exc:
                                del exc["delete"]
                        else:
                            print(f"no input or categories for {exc['name']}")
                            exc["delete"] = True

            dataset["exchanges"] = [
                exc for exc in dataset["exchanges"] if "delete" not in exc
            ]

    def correct_biogas_activities(self) -> None:
        """Balance carbon and energy flows for specific biogas activities."""

        list_biogas_activities = load_methane_correction_list()
        biosphere_codes = get_biosphere_flow_uuid(self.version)

        # find datasets that have a name in the list
        filters = [
            ws.either(*[ws.equals("name", name) for name in list_biogas_activities]),
        ]

        biogas_datasets = ws.get_many(
            self.database,
            *filters,
            ws.equals("reference product", "biogas"),
            ws.equals("unit", "cubic meter"),
        )

        # add a flow of "Carbon dioxide, in air" to the dataset
        # if not present. We add 1.96 kg CO2/m3 biogas.

        for ds in biogas_datasets:
            # Add CO2 uptake
            if not any(
                exc
                for exc in ws.biosphere(ds)
                if exc["name"] == "Carbon dioxide, in air"
            ):
                ds["exchanges"].append(
                    {
                        "uncertainty type": 0,
                        "amount": 1.96,
                        "type": "biosphere",
                        "name": "Carbon dioxide, in air",
                        "unit": "kilogram",
                        "categories": ("natural resource", "in air"),
                        "input": (
                            "biosphere3",
                            biosphere_codes[
                                (
                                    "Carbon dioxide, in air",
                                    "natural resource",
                                    "in air",
                                    "kilogram",
                                )
                            ],
                        ),
                    }
                )

            # Add primary energy flow
            if not any(
                exc
                for exc in ws.biosphere(ds)
                if exc["name"] == "Energy, gross calorific value, in biomass"
            ):
                ds["exchanges"].append(
                    {
                        "uncertainty type": 0,
                        "amount": 22.73,
                        "type": "biosphere",
                        "name": "Energy, gross calorific value, in biomass",
                        "unit": "megajoule",
                        "categories": ("natural resource", "biotic"),
                        "input": (
                            "biosphere3",
                            biosphere_codes[
                                (
                                    "Energy, gross calorific value, in biomass",
                                    "natural resource",
                                    "biotic",
                                    "megajoule",
                                )
                            ],
                        ),
                    }
                )

    def correct_canadian_cement_refractory(self) -> int:
        """Normalize the recognized 13-plant refractory numerator to one plant.

        The native Canadian plant is one unit, but its refractory input uses
        all 13 surveyed plants' lifetime clinker output. Guard the source
        identity and coefficient; leave other recipes and allocation unchanged.
        """
        if self.version not in CA_CEMENT_REFRACTORY_VERSIONS:
            return 0

        from .utils import rescale_exchange

        corrected = 0
        for dataset in self.database:
            if (
                dataset.get("name"),
                dataset.get("reference product"),
                dataset.get("location"),
                dataset.get("unit"),
            ) != (
                "integrated cement plant construction",
                "integrated cement plant",
                "CA",
                "unit",
            ):
                continue
            if dataset.get("activity") not in (
                None,
                "",
                "085620b0-f171-5666-8400-a7bcb0aacd6f",
            ):
                continue
            production = [e for e in dataset["exchanges"] if e["type"] == "production"]
            if (
                len(production) != 1
                or production[0].get("unit") != "unit"
                or not isinstance(production[0].get("amount"), Number)
                or not np.isclose(production[0]["amount"], 1, rtol=0, atol=1e-12)
            ):
                continue
            matches = [
                e
                for e in dataset["exchanges"]
                if (
                    e.get("type"),
                    e.get("name"),
                    e.get("product") or e.get("reference product"),
                    e.get("location"),
                    e.get("unit"),
                )
                == (
                    "technosphere",
                    "market for refractory, basic, packed",
                    "refractory, basic, packed",
                    "GLO",
                    "kilogram",
                )
            ]
            if len(matches) != 1:
                continue
            exchange = matches[0]
            amount = exchange.get("amount")
            if (
                not isinstance(amount, Number)
                # Allow float32 storage rounding, not a changed recipe.
                or not np.isclose(amount, 1492307607.08133, rtol=1e-7, atol=0)
                or exchange.get("formula")
                or exchange.get("uncertainty type") not in (None, 0, 1, 2, 3, 4, 5)
            ):
                continue
            rescale_exchange(exchange, 1 / 13, remove_uncertainty=False)
            if exchange.get("uncertainty type") in (None, 0) and "loc" in exchange:
                exchange["loc"] = exchange["amount"]
            note = (
                "Premise CA cement refractory normalization v1: divided this input "
                f"from {float(amount):.15g} kg by 13 to represent one average plant. "
                "The native rate is 0.00130904176059766 kg refractory/kg clinker; "
                "11.4 million tonnes/year is output from 13 plants, over 100 years. "
                "See ecoinvent v3.11 change report, section 10.1.1 (page 134). "
                "Other machinery and clinker/cement allocation are unchanged."
            )
            exchange["comment"] = (
                (exchange.get("comment") or "") + "\n" + note
            ).strip()
            dataset["comment"] = ((dataset.get("comment") or "") + "\n" + note).strip()
            corrected += 1
        return corrected

    def correct_passenger_car_engine_aluminium(self) -> int:
        """Restore source aluminium purchases, scrap and proportional recovery.

        Apply the three edits together only to the recognized native cutoff
        production/treatment chain. Preserve its nominal mass and inherited
        recovery fraction; this does not close the complete drivetrain budget.
        """
        if self.version not in ICE_ALUMINIUM_VERSIONS:
            return 0

        from .utils import rescale_exchange

        engine = "internal combustion engine, passenger car"
        used = f"used {engine}"
        production_name = "internal combustion engine production, passenger car"
        treatment_name = f"treatment of {used}, shredding"

        def dataset(name, product, output):
            matches = [
                d
                for d in self.database
                if (
                    d.get("name"),
                    d.get("reference product"),
                    d.get("location"),
                    d.get("unit"),
                )
                == (name, product, "GLO", "kilogram")
            ]
            if len(matches) != 1:
                return None
            ds = matches[0]
            outputs = [e for e in ds["exchanges"] if e.get("type") == "production"]
            if (
                len(outputs) != 1
                or outputs[0].get("unit") != "kilogram"
                or not isinstance(outputs[0].get("amount"), Number)
                or not np.isclose(outputs[0]["amount"], output, rtol=0, atol=1e-12)
                or outputs[0].get("formula")
            ):
                return None
            return ds

        def exchange(ds, name, product, amount):
            matches = [
                e
                for e in ds["exchanges"]
                if (
                    e.get("type"),
                    e.get("name"),
                    e.get("product") or e.get("reference product"),
                    e.get("location"),
                    e.get("unit"),
                )
                == ("technosphere", name, product, "GLO", "kilogram")
            ]
            if len(matches) != 1:
                return None
            exc = matches[0]
            if (
                not isinstance(exc.get("amount"), Number)
                or not np.isclose(exc["amount"], amount, rtol=1e-7, atol=0)
                or exc.get("formula")
                or exc.get("uncertainty type") not in (None, 0, 1, 2, 3, 4, 5)
            ):
                return None
            return exc

        production = dataset(production_name, engine, 1)
        treatment = dataset(treatment_name, used, -1)
        market = dataset(f"market for {used}", used, -1)
        if any(d is None for d in (production, treatment, market)):
            return 0
        if production.get("activity") not in (
            None,
            "",
            "8f58b3aa-a2d2-5bb6-a286-965d91f95bd7",
        ):
            return 0
        # The negative waste links and cutoff scrap identities also guard the
        # system model. Do not change an allocation/substitution recipe.
        links = [
            exchange(production, f"market for {used}", used, -1),
            exchange(market, treatment_name, used, -1),
        ]
        edits = [
            (
                production,
                exchange(
                    production,
                    "market for aluminium, cast alloy",
                    "aluminium, cast alloy",
                    170.07 / 275,
                ),
            ),
            (
                production,
                exchange(
                    production,
                    "aluminium scrap, new, Recycled Content cut-off",
                    "aluminium scrap, new",
                    -34.014 / 275,
                ),
            ),
            (
                treatment,
                exchange(
                    treatment,
                    "aluminium scrap, post-consumer, Recycled Content cut-off",
                    "aluminium scrap, post-consumer",
                    -0.408771,
                ),
            ),
        ]
        if any(e is None for e in links) or any(e is None for _, e in edits):
            return 0

        factor = 61.875 / 170.07
        note = (
            "Premise passenger-car ICE aluminium correction v1: source gross "
            "aluminium is 61.875 kg, with 12.375 kg manufacturing scrap and "
            "49.5 kg retained per nominal 275 kg drivetrain (Notter et al. 2010, "
            "supporting Table S2, https://doi.org/10.1021/es903729a; "
            "Althaus and Gauch 2010, Table 80). Replace the native 170.07 kg "
            "gross input and scale its manufacturing and post-consumer scrap "
            "by 61.875/170.07 together. Post-consumer recovery retains the native "
            "82.62188% of retained aluminium as an unvalidated treatment proxy. "
            "Other treatment flows/emissions and nominal masses are unchanged; "
            "the remaining steel, fluid/reference-mass and treatment mass gaps "
            "are unresolved. No balancing residue or new recovery rate is inferred."
        )
        for ds, exc in edits:
            rescale_exchange(exc, factor, remove_uncertainty=False)
            if exc.get("uncertainty type") in (None, 0) and "loc" in exc:
                exc["loc"] = exc["amount"]
            exc["comment"] = ((exc.get("comment") or "") + "\n" + note).strip()
        for ds in (production, treatment):
            ds["comment"] = ((ds.get("comment") or "") + "\n" + note).strip()
        return 1

    def prepare_datasets(self, keep_uncertainty_data: bool) -> List[dict]:
        """Run the standard cleaning pipeline on the loaded database.

        :param keep_uncertainty_data: Flag indicating whether to preserve uncertainty data.
        :type keep_uncertainty_data: bool
        :return: Cleaned database ready for further processing.
        :rtype: List[dict]
        """

        # Set missing locations to ```GLO``` for datasets in ``database``
        print("Set missing location of datasets to global scope.")
        wurst.default_global_location(self.database)

        # Set missing locations to ```GLO``` for exchanges in ``datasets``
        print("Set missing location of production exchanges to scope of dataset.")

        print("Correct missing location of technosphere exchanges.")
        self.fix_unset_technosphere_and_production_exchange_locations()

        print("Correct missing flow categories for biosphere exchanges")
        self.fix_biosphere_flow_categories()

        # Remove empty exchanges
        print("Remove empty exchanges.")
        remove_nones(self.database)

        corrected = self.correct_canadian_cement_refractory()
        if corrected:
            print(
                f"Normalized Canadian cement refractory input in {corrected} dataset(s)."
            )

        if self.correct_passenger_car_engine_aluminium():
            print("Corrected passenger-car engine aluminium input and scrap chain.")

        # correct carbon and energy balance
        self.correct_biogas_activities()

        # Remove uncertainty data
        if not keep_uncertainty_data:
            print("Remove uncertainty data.")
            self.database = remove_uncertainty(self.database)

        return self.database
