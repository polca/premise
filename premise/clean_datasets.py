"""
clean_datasets.py contains a number of functions that clean a list of
datasets or databases. They perform operations like removing useless fields,
filling missing exchange information, etc.
"""

import csv
import pprint
from pathlib import Path
from typing import Dict, List, Tuple, Union

import bw2io
import numpy as np
import wurst
import yaml
from bw2data.database import DatabaseChooser
from bw2io.errors import MultiprocessingError
from wurst import searching as ws

from .data_collection import get_delimiter
from .filesystem_constants import DATA_DIR


def load_methane_correction_list():
    """
    Load biomethane_correction.yaml file and return a list
    """
    with open(DATA_DIR / "fuels" / "biomethane_correction.yaml", encoding="utf-8") as f:
        methane_correction_list = yaml.safe_load(f)
    return methane_correction_list


def remove_uncertainty(database):
    """
    Remove uncertainty information from database exchanges.
    :param database:
    :return:
    """
    uncertainty_keys = ["scale", "shape", "minimum", "maximum"]
    nan_value = np.nan

    for dataset in database:
        for exchange in dataset["exchanges"]:
            exchange["uncertainty type"] = 0
            exchange["loc"] = exchange["amount"]
            for key in uncertainty_keys:
                exchange[key] = nan_value

    return database


def get_biosphere_flow_uuid(version: str) -> Dict[Tuple[str, str, str, str], str]:
    """
    Retrieve a dictionary with biosphere flow (name, categories, unit) --> uuid.

    :returns: dictionary with biosphere flow (name, categories, unit) --> uuid
    :rtype: dict
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
    """
    Retrieve a dictionary with biosphere flow uuids and categories.

    :returns: dictionary with biosphere flow uuids as keys and categories as values
    :rtype: dict
    """

    data = get_biosphere_flow_uuid(version)

    return {
        v: (k[1], k[2]) if k[2] != "unspecified" else (k[1],) for k, v in data.items()
    }


def remove_nones(database: List[dict]) -> List[dict]:
    """
    Remove empty exchanges in the datasets of the wurst inventory database.
    Modifies in place (does not return anything).

    :param database: wurst inventory database
    :type database: list

    """

    def exists(x):
        return {k: v for k, v in x.items() if v is not None}

    for dataset in database:
        dataset["exchanges"] = [exists(exc) for exc in dataset["exchanges"]]

    return database


def remove_categories(database: List[dict]) -> List[dict]:
    """
    Remove categories from datasets in the wurst inventory database.
    Modifies in place (does not return anything).

    :param database: wurst inventory database
    :type database: list

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
    """
    Strip strings from spaces in the dataset of the wurst inventory database.
    Modifies in place (does not return anything).

    :param database: wurst inventory database
    :type database: list

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
    """
    Class that cleans the datasets contained in the inventory database for further processing.


    :ivar source_type: type of the database source. Can be ´brightway´ or 'ecospold'.
    :vartype source_type: str
    :ivar source_db: name of the source database if `source_type` == 'brightway'
    :vartype source_db: str
    :ivar source_file_path: filepath of the database if `source_type` == 'ecospold'.
    :vartype source_file_path: str

    """

    def __init__(
        self, source_db: str, source_type: str, source_file_path: Path, version: str
    ) -> None:
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
            # The ecospold data needs to be formatted
            ecoinvent = bw2io.SingleOutputEcospold2Importer(
                str(source_file_path), source_db, use_mp=False
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
        """
        Return a list of location names, given the filtering
        conditions given in `lookup_dict`.
        It is, for example, used to return a list of
        location names based on the name and the unit of a dataset.

        :param lookup_dict: a dictionary with filtering conditions
        :return: a list of location names
        """
        return [
            x["product"]
            for x in wurst.searching.get_many(
                self.database, *[ws.equals(k, v) for k, v in lookup_dict.items()]
            )
        ]

    def find_location_given_lookup_dict(self, lookup_dict: Dict[str, str]) -> List[str]:
        """
        Return a list of location names, given the
        filtering conditions given in `lookup_dict`.
        It is, for example, used to return a list
        of location names based on the name
        and the unit of a dataset.


        :param lookup_dict: a dictionary with filtering conditions
        :return: a list of location names
        """
        return [
            x["location"]
            for x in wurst.searching.get_many(
                self.database, *[ws.equals(k, v) for k, v in lookup_dict.items()]
            )
        ]

    def add_location_field_to_exchanges(self) -> None:
        """
        Add the `location` key to the production and
        technosphere exchanges in :attr:`database`.

        :raises IndexError: if no corresponding activity
            (and reference product) can be found.

        """
        d_location = {(a["database"], a["code"]): a["location"] for a in self.database}
        for dataset in self.database:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "technosphere":
                    exc_input = exchange["input"]
                    exchange["location"] = d_location[exc_input]

    def add_product_field_to_exchanges(self) -> None:
        """

        Add the `product` key to the production and
        technosphere exchanges in :attr:`database`.

        For production exchanges, use the value
        of the `reference_product` field.
        For technosphere exchanges, search the
        activities in :attr:`database` and
        use the reference product.

        :raises IndexError: if no corresponding
            activity (and reference product) can be found.

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
        """
        Transform the parameter field of the database to a dictionary.
        """
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
        """
        Give all the production and technopshere exchanges
        with a missing location name the location of the dataset
        they belong to.
        Modifies in place (does not return anything).

        :param matching_fields: filter conditions
        :type matching_fields: tuple

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
        """Add a `categories` for biosphere flows if missing.
        This happens when importing directly from ecospold files"""

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

    def correct_biogas_activities(self):
        """
        Some activities producing biogas are not given any
        biogenic CO2 or energy input, leading to imbalanced carbon and energy flows
        when combusted.
        """

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

    def prepare_datasets(self, keep_uncertainty_data) -> List[dict]:
        """
        Clean datasets for all databases listed in
        scenarios: fix location names, remove
        empty exchanges, etc.

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
