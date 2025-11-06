"""Utility helpers to clean and normalise life cycle inventory datasets."""

from __future__ import annotations

import csv
import pprint
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import bw2io
import numpy as np
import wurst
import yaml
from bw2data.database import DatabaseChooser
from bw2io.errors import MultiprocessingError
from wurst import searching as ws

from .data_collection import get_delimiter
from .filesystem_constants import DATA_DIR


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

    if version == "3.11":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_311.csv"
    elif version == "3.10":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_310.csv"
    elif version == "3.11":
        fp = DATA_DIR / "utils" / "export" / "flows_biosphere_311.csv"
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
            self.database = wurst.extract_brightway2_databases(source_db)
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

        # correct carbon and energy balance
        self.correct_biogas_activities()

        # Remove uncertainty data
        if not keep_uncertainty_data:
            print("Remove uncertainty data.")
            self.database = remove_uncertainty(self.database)

        return self.database
