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
import wurst
from bw2data.database import DatabaseChooser
from wurst import searching as ws

from . import DATA_DIR

FILEPATH_FIX_NAMES = DATA_DIR / "fix_names.csv"
FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"


def get_fix_names_dict() -> Dict[str, str]:
    """
    Loads a csv file into a dictionary. This dictionary contains a few location names
    that need correction in the wurst inventory database.

    :return: dictionary that contains names equivalence
    :rtype: dict
    """
    with open(FILEPATH_FIX_NAMES) as f:
        return dict(filter(None, csv.reader(f, delimiter=";")))


def get_biosphere_flow_uuid() -> Dict[Tuple[str, str, str, str], str]:
    """
    Retrieve a dictionary with biosphere flow (name, categories, unit) --> uuid.

    :returns: dictionary with biosphere flow (name, categories, unit) --> uuid
    :rtype: dict
    """

    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(FILEPATH_BIOSPHERE_FLOWS) as f:
        input_dict = csv.reader(f, delimiter=";")
        for row in input_dict:
            csv_dict[(row[0], row[1], row[2], row[3])] = row[-1]

    return csv_dict


def get_biosphere_flow_categories() -> Dict[str, Union[Tuple[str], Tuple[str, str]]]:
    """
    Retrieve a dictionary with biosphere flow uuids and categories.

    :returns: dictionary with biosphere flow uuids as keys and categories as values
    :rtype: dict
    """

    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(FILEPATH_BIOSPHERE_FLOWS) as f:
        input_dict = csv.reader(f, delimiter=";")
        for row in input_dict:
            csv_dict[row[-1]] = (
                (row[1], row[2]) if row[2] != "unspecified" else (row[1],)
            )

    return csv_dict


def remove_nones(db: List[dict]) -> List[dict]:
    """
    Remove empty exchanges in the datasets of the wurst inventory database.
    Modifies in place (does not return anything).

    :param db: wurst inventory database
    :type db: list

    """
    exists = lambda x: {k: v for k, v in x.items() if v is not None}
    for ds in db:
        ds["exchanges"] = [exists(exc) for exc in ds["exchanges"]]

    return db


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
        self, source_db: str, source_type: str, source_file_path: Path
    ) -> None:

        if source_type == "brightway":
            # Check that database exists
            if len(DatabaseChooser(source_db)) == 0:
                raise NameError(
                    "The database selected is empty. Make sure the name is correct"
                )
            self.db = wurst.extract_brightway2_databases(source_db)

        if source_type == "ecospold":
            # The ecospold data needs to be formatted
            ei = bw2io.SingleOutputEcospold2Importer(source_file_path, source_db)
            ei.apply_strategies()
            self.db = ei.data
            # Location field is added to exchanges
            self.add_location_field_to_exchanges()
            # Product field is added to exchanges
            self.add_product_field_to_exchanges()
            # Parameter field is converted from a list to a dictionary
            self.transform_parameter_field()

    def get_rev_fix_names_dict(self) -> Dict[str, str]:
        """
        Reverse the fix_names dictionary.

        :return: dictionary that contains names equivalence
        :rtype: dict
        """
        return {v: k for k, v in get_fix_names_dict().items()}

    def find_product_given_lookup_dict(self, lookup_dict: Dict[str, str]) -> List[str]:
        """
        Return a list of location names, given the filtering conditions given in `lookup_dict`.
        It is, for example, used to return a list of location names based on the name and the unit of a dataset.


        :param lookup_dict: a dictionary with filtering conditions
        :return: a list of location names
        """
        return [
            x["product"]
            for x in wurst.searching.get_many(
                self.db, *[ws.equals(k, v) for k, v in lookup_dict.items()]
            )
        ]

    def find_location_given_lookup_dict(self, lookup_dict: Dict[str, str]) -> List[str]:
        """
        Return a list of location names, given the filtering conditions given in `lookup_dict`.
        It is, for example, used to return a list of location names based on the name and the unit of a dataset.


        :param lookup_dict: a dictionary with filtering conditions
        :return: a list of location names
        """
        return [
            x["location"]
            for x in wurst.searching.get_many(
                self.db, *[ws.equals(k, v) for k, v in lookup_dict.items()]
            )
        ]

    def add_location_field_to_exchanges(self) -> None:
        """Add the `location` key to the production and
        technosphere exchanges in :attr:`database`.

        :raises IndexError: if no corresponding activity (and reference product) can be found.

        """
        d_location = {(a["database"], a["code"]): a["location"] for a in self.db}
        for a in self.db:
            for e in a["exchanges"]:
                if e["type"] == "technosphere":
                    exc_input = e["input"]
                    e["location"] = d_location[exc_input]

    def add_product_field_to_exchanges(self) -> None:
        """Add the `product` key to the production and
        technosphere exchanges in :attr:`database`.

        For production exchanges, use the value of the `reference_product` field.
        For technosphere exchanges, search the activities in :attr:`database` and
        use the reference product.

        :raises IndexError: if no corresponding activity (and reference product) can be found.

        """
        # Create a dictionary that contains the 'code' field as key and the 'product' field as value
        d_product = {a["code"]: (a["reference product"], a["name"]) for a in self.db}
        # Add a `product` field to the production exchange
        for x in self.db:
            for y in x["exchanges"]:
                if y["type"] == "production":
                    if "product" not in y:
                        y["product"] = x["reference product"]

                    if y["name"] != x["name"]:
                        y["name"] = x["name"]

        # Add a `product` field to technosphere exchanges
        for x in self.db:
            for y in x["exchanges"]:
                if y["type"] == "technosphere":
                    # Check if the field 'product' is present
                    if "product" not in y:
                        y["product"] = d_product[y["input"][1]][0]

                    # If a 'reference product' field is present, we make sure it matches with the new 'product' field
                    if "reference product" in y:
                        try:
                            assert y["product"] == y["reference product"]
                        except AssertionError:
                            y["product"] = d_product[y["input"][1]][0]

                    # Ensure the name is correct
                    y["name"] = d_product[y["input"][1]][1]

    def transform_parameter_field(self) -> None:
        # When handling ecospold files directly, the parameter field is a list.
        # It is here transformed into a dictionary
        for x in self.db:
            x["parameters"] = {k["name"]: k["amount"] for k in x["parameters"]}

    # Functions to clean up Wurst import and additional technologies
    def fix_unset_technosphere_and_production_exchange_locations(
        self, matching_fields: Tuple[str, str] = ("name", "unit")
    ) -> None:
        """
        Give all the production and technopshere exchanges with a missing location name the location of the dataset
        they belong to.
        Modifies in place (does not return anything).

        :param matching_fields: filter conditions
        :type matching_fields: tuple

        """
        for ds in self.db:

            # collect production exchanges that simply do not have a location key and set it to
            # the location of the dataset
            for exc in wurst.production(ds):
                if "location" not in exc:
                    exc["location"] = ds["location"]

            for exc in wurst.technosphere(ds):
                if "location" not in exc:
                    locs = self.find_location_given_lookup_dict(
                        {k: exc.get(k) for k in matching_fields}
                    )
                    if len(locs) == 1:
                        exc["location"] = locs[0]
                    else:
                        print(
                            "No unique location found for exchange:\n{}\nFound: {}".format(
                                pprint.pformat(exc), locs
                            )
                        )

    def fix_biosphere_flow_categories(self) -> None:
        """Add a `categories` for biosphere flows if missing.
        This happens when importing directly from ecospold files"""

        dict_bio_cat = get_biosphere_flow_categories()
        dict_bio_uuid = get_biosphere_flow_uuid()

        for ds in self.db:
            for exc in ds["exchanges"]:
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

            ds["exchanges"] = [exc for exc in ds["exchanges"] if "delete" not in exc]

    def prepare_datasets(self) -> List[dict]:
        """
        Clean datasets for all databases listed in scenarios: fix location names, remove
        empty exchanges, etc.

        """

        # Set missing locations to ```GLO``` for datasets in ``database``
        print("Set missing location of datasets to global scope.")
        wurst.default_global_location(self.db)

        # Set missing locations to ```GLO``` for exchanges in ``datasets``
        print("Set missing location of production exchanges to scope of dataset.")

        print("Correct missing location of technosphere exchanges.")
        self.fix_unset_technosphere_and_production_exchange_locations()

        print("Correct missing flow categories for biosphere exchanges")
        self.fix_biosphere_flow_categories()

        # Remove empty exchanges
        print("Remove empty exchanges.")
        remove_nones(self.db)

        return self.db
