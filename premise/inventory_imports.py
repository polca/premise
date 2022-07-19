"""
Contains class and methods to imports inventories from ecoinvent, premise,
and those provided by the user.
"""

import csv
import itertools
import sys
import uuid
from pathlib import Path
from typing import Dict, List, Union

import bw2io
import yaml
from bw2io import ExcelImporter, Migration
from prettytable import PrettyTable
from wurst import searching as ws

from . import DATA_DIR, INVENTORY_DIR
from .geomap import Geomap

FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"
FILEPATH_MIGRATION_MAP = INVENTORY_DIR / "migration_map.csv"

OUTDATED_FLOWS = DATA_DIR / "utils" / "export" / "outdated_flows.yaml"


def get_outdated_flows():
    """
    Retrieve a list of outdated flows from the outdated flows file.
    """

    with open(OUTDATED_FLOWS, "r", encoding="utf-8") as stream:
        flows = yaml.safe_load(stream)

    return flows


def get_biosphere_code() -> dict:
    """
    Retrieve a dictionary with biosphere flow names and uuid codes.
    :returns: dictionary with biosphere flow names as keys and uuid codes as values

    """

    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(FILEPATH_BIOSPHERE_FLOWS, encoding="utf-8") as file:
        input_dict = csv.reader(file, delimiter=";")
        for row in input_dict:
            csv_dict[(row[0], row[1], row[2], row[3])] = row[4]

    return csv_dict


def generate_migration_maps(origin: str, destination: str) -> Dict[str, list]:
    """
    Generate mapping for ecoinvent datasets across different database versions.
    :param origin: ecoinvent database version to find equivalence from (e.g., "3.6")
    :param destination: ecoinvent database version to find equivalence for (e.g., "3.8")
    :return: a migration map dicitonary for bw2io
    """

    response = {"fields": ["name", "reference product", "location"], "data": []}

    with open(FILEPATH_MIGRATION_MAP, "r", encoding="utf-8") as read_obj:
        csv_reader = csv.reader(read_obj, delimiter=";")
        next(csv_reader)
        for row in csv_reader:
            if row[0] == origin and row[1] == destination:
                data = {}
                if row[5] != "":
                    data["name"] = row[5]
                if row[6] != "":
                    data["reference product"] = row[6]
                if row[7] != "":
                    data["location"] = row[7]
                response["data"].append(((row[2], row[3], row[4]), data))
    return response


class BaseInventoryImport:
    """
    Base class for inventories that are to be merged with the wurst database.

    :ivar database: the target database for the import (the ecoinvent database),
    unpacked to a list of dicts
    :ivar version_in: the ecoinvent database version of the inventory to import
    :ivar version_out: the ecoinvent database version the imported inventories
    should comply with
    :ivar path: the filepath of the inventories to import

    """

    def __init__(
        self,
        database: List[dict],
        version_in: str,
        version_out: str,
        path: Union[str, Path],
    ) -> None:
        """Create a :class:`BaseInventoryImport` instance."""
        self.database = database
        self.db_code = [x["code"] for x in self.database]
        self.db_names = [
            (x["name"], x["reference product"], x["location"]) for x in self.database
        ]
        self.version_in = version_in
        self.version_out = version_out
        self.biosphere_dict = get_biosphere_code()
        self.outdated_flows = get_outdated_flows()

        if not isinstance(path, Path):
            path = Path(path)
        self.path = path

        if self.path != Path("."):
            if not self.path.exists():
                raise FileNotFoundError(
                    f"The inventory file {self.path} could not be found."
                )

        self.import_db = self.load_inventory(path)

        # register migration maps
        # as imported inventories link to different ecoinvent versions
        ei_versions = ["35", "36", "37", "38"]

        for combination in itertools.product(ei_versions, ei_versions):
            if combination[0] != combination[1]:
                mapping = generate_migration_maps(combination[0], combination[1])
                if len(mapping["data"]) > 0:
                    Migration(f"migration_{combination[0]}_{combination[1]}").write(
                        mapping,
                        description=f"Change technosphere names due to change from {combination[0]} to {combination[1]}",
                    )

    def load_inventory(self, path: Union[str, Path]) -> None:
        """Load an inventory from a specified path.
        Sets the :attr:`import_db` attribute.
        :param str path: Path to the inventory file
        :returns: Nothing.
        """
        return None

    def prepare_inventory(self) -> None:
        """Prepare the inventory for the merger with Ecoinvent.
        Modifies :attr:`import_db` in-place.
        :returns: Nothing
        """

    def check_for_duplicates(self) -> None:
        """
        Check whether the inventories to be imported are not
        already in the source database.
        """

        # print if we find datasets that already exist
        already_exist = [
            (x["name"].lower(), x["reference product"].lower(), x["location"])
            for x in self.import_db.data
            if x["code"] in self.db_code
        ]

        already_exist.extend(
            [
                (x["name"].lower(), x["reference product"].lower(), x["location"])
                for x in self.import_db.data
                if (x["name"].lower(), x["reference product"].lower(), x["location"])
                in self.db_names
            ]
        )

        if len(already_exist) > 0:
            print(
                "The following datasets to import already exist "
                "in the source database. "
                "They will not be imported"
            )
            table = PrettyTable(["Name", "Reference product", "Location", "File"])
            for dataset in already_exist:
                table.add_row(
                    [dataset[0][:50], dataset[1][:30], dataset[2], self.path.name]
                )

            print(table)

        self.import_db.data = [
            x for x in self.import_db.data if x["code"] not in self.db_code
        ]
        self.import_db.data = [
            x
            for x in self.import_db.data
            if (x["name"], x["reference product"], x["location"]) not in self.db_names
        ]

    def merge_inventory(self) -> List[dict]:
        """Prepare :attr:`import_db` and merge the inventory to the ecoinvent :attr:`database`.
        Calls :meth:`prepare_inventory`. Changes the :attr:`database` attribute.
        :returns: Nothing
        """

        self.prepare_inventory()
        return self.import_db

    def search_missing_exchanges(self, label: str, value: str) -> List[dict]:
        """
        Return a list of activities for which
        a given exchange cannot be found
        :param label: the label of the field to look for
        :param value: the value of the field to look for
        :return:
        """

        results = []
        for act in self.import_db.data:
            if (
                len([a for a in act["exchanges"] if label in a and a[label] == value])
                == 0
            ):
                results.append(act)

        return results

    def search_missing_field(self, field: str, scope: str = "activity") -> List[dict]:
        """Find exchanges and activities that do not contain a specific field
        in :attr:`imort_db`
        :param str field: label of the field to search for.
        :param scope: "activity" or "all". whether to search in the activity
        or the activity and its exchanges
        :returns: a list of dictionaries, activities and exchanges
        :rtype: list
        """
        results = []
        for act in self.import_db.data:
            if field not in act:
                results.append(act)

            if scope == "all":
                for ex in act["exchanges"]:
                    if ex["type"] == "technosphere" and field not in ex:
                        results.append(ex)
        return results

    def add_product_field_to_exchanges(self) -> None:
        """Add the `product` key to the production and
        technosphere exchanges in :attr:`import_db`.
        Also add `code` field if missing.
        For production exchanges, use the value of the `reference_product` field.
        For technosphere exchanges, search the activities in :attr:`import_db` and
        use the reference product. If none is found, search the Ecoinvent :attr:`database`.
        Modifies the :attr:`import_db` attribute in place.
        :raises IndexError: if no corresponding activity (and reference product) can be found.
        """
        # Add a `product` field to the production exchange
        for dataset in self.import_db.data:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "production":
                    if "product" not in exchange:
                        exchange["product"] = dataset["reference product"]

                    if exchange["name"] != dataset["name"]:
                        exchange["name"] = dataset["name"]

        # Add a `product` field to technosphere exchanges
        for dataset in self.import_db.data:
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "technosphere":
                    # Check if the field 'product' is present
                    if not "product" in exchange:
                        exchange["product"] = self.correct_product_field(exchange)

                    # If a 'reference product' field is present, we make sure
                    # it matches with the new 'product' field
                    # if "reference product" in y:
                    #    try:
                    #        assert y["product"] == y["reference product"]
                    #    except AssertionError:
                    #        y["product"] = self.correct_product_field(y)

        # Add a `code` field if missing
        for dataset in self.import_db.data:
            if "code" not in dataset:
                dataset["code"] = str(uuid.uuid4().hex)

    def correct_product_field(self, exc: dict) -> str:
        """
        Find the correct name for the `product` field of the exchange
        :param exc: a dataset exchange
        :return: name of the product field of the exchange

        """
        # Look first in the imported inventories
        candidate = next(
            ws.get_many(
                self.import_db.data,
                ws.equals("name", exc["name"]),
                ws.equals("location", exc["location"]),
                ws.equals("unit", exc["unit"]),
            ),
            None,
        )

        # If not, look in the ecoinvent inventories
        if candidate is None:
            candidate = next(
                ws.get_many(
                    self.database,
                    ws.equals("name", exc["name"]),
                    ws.equals("location", exc["location"]),
                    ws.equals("unit", exc["unit"]),
                ),
                None,
            )

        if candidate is not None:
            return candidate["reference product"]

        print(
            f"An inventory exchange in {self.import_db.db_name} cannot be linked to the "
            f"biosphere or the ecoinvent database: {exc}"
        )

        return exc["reference product"]

    def add_biosphere_links(self, delete_missing: bool = False) -> None:
        """Add links for biosphere exchanges to :attr:`import_db`
        Modifies the :attr:`import_db` attribute in place.

        :param delete_missing: whether unlinked exchanges should be deleted or not.
        """
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "biosphere":
                    if isinstance(y["categories"], str):
                        y["categories"] = tuple(y["categories"].split("::"))
                    if len(y["categories"]) > 1:
                        try:
                            key = (
                                y["name"],
                                y["categories"][0],
                                y["categories"][1],
                                y["unit"],
                            )
                            if key in self.biosphere_dict:
                                y["input"] = (
                                    "biosphere3",
                                    self.biosphere_dict[key],
                                )
                            else:
                                if key[0] in self.outdated_flows:
                                    new_key = list(key)
                                    new_key[0] = self.outdated_flows[key[0]]
                                    y["input"] = (
                                        "biosphere3",
                                        self.biosphere_dict[tuple(new_key)],
                                    )
                                else:
                                    if delete_missing:
                                        y["flag_deletion"] = True
                                    else:
                                        print(
                                            f"Could not find a biosphere flow for {key}"
                                        )

                        except KeyError:
                            if delete_missing:
                                y["flag_deletion"] = True
                            else:
                                raise
                    else:
                        try:
                            y["input"] = (
                                "biosphere3",
                                self.biosphere_dict[
                                    (
                                        y["name"],
                                        y["categories"][0],
                                        "unspecified",
                                        y["unit"],
                                    )
                                ],
                            )
                        except KeyError:
                            if delete_missing:
                                print(
                                    f"The following biosphere exchange cannot be found and will be deleted: {y['name']}"
                                )
                                y["flag_deletion"] = True
                            else:
                                raise
            x["exchanges"] = [ex for ex in x["exchanges"] if "flag_deletion" not in ex]

    def remove_ds_and_modifiy_exchanges(self, name: str, ex_data: dict) -> None:
        """
        Remove an activity dataset from :attr:`import_db` and replace the corresponding
        technosphere exchanges by what is given as second argument.
        :param str name: name of activity to be removed
        :param dict ex_data: data to replace the corresponding exchanges
        :returns: Nothing
        """

        self.import_db.data = [
            act for act in self.import_db.data if not act["name"] == name
        ]

        for act in self.import_db.data:
            for ex in act["exchanges"]:
                if ex["type"] == "technosphere" and ex["name"] == name:
                    ex.update(ex_data)
                    # make sure there is no existing link
                    if "input" in ex:
                        del ex["input"]

            # Delete any field that does not have information
            for key in act:
                if act[key] is None:
                    act.pop(key)


class DefaultInventory(BaseInventoryImport):
    """
    Importing class. Inherits from :class:`BaseInventoryImport`.

    """

    def __init__(self, database, version_in, version_out, path):
        super().__init__(database, version_in, version_out, path)

    def load_inventory(self, path: Union[str, Path]) -> bw2io.ExcelImporter:
        return ExcelImporter(path)

    def prepare_inventory(self) -> None:

        if self.version_in != self.version_out:
            self.import_db.migrate(
                f"migration_{self.version_in.replace('.', '')}_{self.version_out.replace('.', '')}"
            )

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Check for duplicates
        self.check_for_duplicates()


class VariousVehicles(BaseInventoryImport):
    """
    Imports various future vehicles' inventories (two-wheelers, buses, trams, etc.).

    :ivar database: wurst database
    :ivar version_in: original ecoinvent version of the inventories
    :ivar version_out: ecoinvent version the inventories should comply with
    :ivar path: filepath of the inventories
    :ivar year: year of the database
    :ivar regions:
    :ivar model: IAM model
    :ivar scenario: IAM scenario
    :ivar vehicle_type: "two-wheeler", "car, "truck" or "bus"
    :ivar relink: whether suppliers within a dataset need to be relinked
    :ivar has_fleet: whether the `vehicle_type` has associated fleet information
    """

    def __init__(
        self,
        database: List[dict],
        version_in: str,
        version_out: str,
        path: Union[str, Path],
        year: int,
        regions: List[str],
        model: str,
        scenario: str,
        vehicle_type: str,
        relink: bool = False,
        has_fleet: bool = False,
    ) -> None:
        super().__init__(database, version_in, version_out, path)
        self.year = year
        self.regions = regions
        self.model = model
        self.scenario = scenario
        self.vehicle_type = vehicle_type
        self.relink = relink
        self.has_fleet = has_fleet
        self.geo = Geomap(model=model)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # initially links to ei37
        if self.version_in != self.version_out:
            self.import_db.migrate(
                f"migration_{self.version_in.replace('.', '')}_{self.version_out.replace('.', '')}"
            )

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()

    def merge_inventory(self):

        self.database.extend(self.import_db.data)

        print("Done!")

        return self.database


class AdditionalInventory(BaseInventoryImport):
    """
    Import additional inventories, if any.
    """

    def __init__(self, database, version_in, version_out, path):
        super().__init__(database, version_in, version_out, path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def remove_missing_fields(self):
        """
        Remove any field that does not have information.
        """

        for dataset in self.import_db.data:
            for key, value in list(dataset.items()):
                if not value:
                    del dataset[key]

    def prepare_inventory(self):

        if self.version_in != self.version_out:
            self.import_db.migrate(
                f"migration_{self.version_in.replace('.', '')}_{self.version_out.replace('.', '')}"
            )
            print(
                f"migration_{self.version_in.replace('.', '')}_{self.version_out.replace('.', '')}"
            )

        list_missing_prod = self.search_missing_exchanges(
            label="type", value="production"
        )

        if len(list_missing_prod) > 0:
            print("The following datasets are missing a `production` exchange.")
            print("You should fix those before proceeding further.\n")
            table = PrettyTable(
                ["Name", "Reference product", "Location", "Unit", "File"]
            )
            for dataset in list_missing_prod:
                table.add_row(
                    [
                        dataset.get("name", "XXXX"),
                        dataset.get("referece product", "XXXX"),
                        dataset.get("location", "XXXX"),
                        dataset.get("unit", "XXXX"),
                        self.path.name,
                    ]
                )

            print(table)

            sys.exit()

        self.add_biosphere_links(delete_missing=True)
        list_missing_ref = self.search_missing_field(field="name")
        list_missing_ref.extend(self.search_missing_field(field="reference product"))
        list_missing_ref.extend(self.search_missing_field(field="location"))
        list_missing_ref.extend(self.search_missing_field(field="unit"))

        if len(list_missing_ref) > 0:
            print(
                "The following datasets are missing an important field "
                "(`name`, `reference product`, `location` or `unit`).\n"
            )

            print("You should fix those before proceeding further.\n")
            table = PrettyTable(
                ["Name", "Reference product", "Location", "Unit", "File"]
            )
            for dataset in list_missing_ref:
                table.add_row(
                    [
                        dataset.get("name", "XXXX"),
                        dataset.get("referece product", "XXXX"),
                        dataset.get("location", "XXXX"),
                        dataset.get("unit", "XXXX"),
                        self.path.name,
                    ]
                )

            print(table)

        if len(list_missing_prod) > 0 or len(list_missing_ref) > 0:
            sys.exit()

        self.remove_missing_fields()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()
