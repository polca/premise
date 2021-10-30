import csv
import itertools
import pickle
import sys
import uuid
from pathlib import Path

import carculator
import carculator_truck
import numpy as np
from bw2io import ExcelImporter, Migration
from bw2io.importers.base_lci import LCIImporter
from prettytable import PrettyTable
from wurst import searching as ws
from wurst import transformations as wt

from . import DATA_DIR, INVENTORY_DIR
from .geomap import Geomap
from .utils import relink_technosphere_exchanges

FILEPATH_BIOSPHERE_FLOWS = (DATA_DIR / "utils" / "export" / "dict_biosphere.txt")
FILEPATH_MIGRATION_MAP = INVENTORY_DIR / "migration_map.csv"


def get_biosphere_code():
    """
    Retrieve a dictionary with biosphere flow names and uuid codes.

    :returns: dictionary with biosphere flow names as keys and uuid code as values
    :rtype: dict
    """

    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(FILEPATH_BIOSPHERE_FLOWS, encoding="utf-8") as f:
        input_dict = csv.reader(f, delimiter=";")
        for row in input_dict:
            csv_dict[(row[0], row[1], row[2], row[3])] = row[4]

    return csv_dict


def generate_migration_maps(origin, destination):

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
    Base class for inventories that are to be merged with the ecoinvent database.

    :ivar database: the target database for the import (the Ecoinvent database),
              unpacked to a list of dicts
    :vartype database: list
    :ivar version: the target Ecoinvent database version
    :vartype version: str
    """

    def __init__(self, database, version_in, version_out, path):
        """Create a :class:`BaseInventoryImport` instance.

        :param list database: the target database for the import (the Ecoinvent database),
                              unpacked to a list of dicts
        :param version: the version of the target database ("3.5", "3.6", "3.7", "3.7.1")
        :type version: str
        :param path: Path to the imported inventory.
        :type path: str or Path

        """
        self.database = database
        self.db_code = [x["code"] for x in self.database]
        self.db_names = [
            (x["name"], x["reference product"], x["location"]) for x in self.database
        ]
        self.version_in = version_in
        self.version_out = version_out
        self.biosphere_dict = get_biosphere_code()

        path = Path(path)

        if path != Path("."):
            if not path.is_file():
                raise FileNotFoundError(
                    f"The inventory file {path} could not be found."
                )
        self.path = path

        self.import_db = self.load_inventory(path)

        # register migration maps
        # as imported inventories link to different ecoinvent versions
        ei_versions = ["35", "36", "37", "38"]

        for r in itertools.product(ei_versions, ei_versions):
            if r[0] != r[1]:
                mapping = generate_migration_maps(r[0], r[1])
                if len(mapping["data"]) > 0:
                    Migration(f"migration_{r[0]}_{r[1]}").write(
                        mapping,
                        description=f"Change technosphere names due to change from {r[0]} to {r[1]}",
                    )

    def load_inventory(self, path):
        """Load an inventory from a specified path.

        Sets the :attr:`import_db` attribute.

        :param str path: Path to the inventory file
        :returns: Nothing.

        """

    def prepare_inventory(self):
        """Prepare the inventory for the merger with Ecoinvent.

        Modifies :attr:`import_db` in-place.

        :returns: Nothing

        """

    def check_for_duplicates(self):
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
                "The following datasets to import already exist in the source database. They will not be imported"
            )
            t = PrettyTable(["Name", "Reference product", "Location", "File"])
            for dataset in already_exist:
                t.add_row(
                    [dataset[0][:50], dataset[1][:30], dataset[2], self.path.name]
                )

            print(t)

        self.import_db.data = [
            x for x in self.import_db.data if x["code"] not in self.db_code
        ]
        self.import_db.data = [
            x
            for x in self.import_db.data
            if (x["name"], x["reference product"], x["location"]) not in self.db_names
        ]

    def merge_inventory(self):
        """Prepare :attr:`import_db` and merge the inventory to the ecoinvent :attr:`database`.

        Calls :meth:`prepare_inventory`. Changes the :attr:`database` attribute.

        :returns: Nothing

        """

        self.prepare_inventory()
        self.database.extend(self.import_db)

    def search_exchanges(self, srchdict):
        """Search :attr:`import_db` by field values.

        :param dict srchdict: dict with the name of the fields and the values.
        :returns: the activities with the exchanges that match the search.
        :rtype: dict

        """
        results = []
        for act in self.import_db.data:
            for ex in act["exchanges"]:
                if len(srchdict.items() - ex.items()) == 0:
                    results.append(act)
        return results

    def search_missing_exchanges(self, label, value):
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

    def search_missing_field(self, field, scope="activity"):
        """Find exchanges and activities that do not contain a specific field
        in :attr:`imort_db`

        :param scope:
        :param str field: label of the field to search for.
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

    def add_product_field_to_exchanges(self):
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
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "production":
                    if "product" not in y:
                        y["product"] = x["reference product"]

                    if y["name"] != x["name"]:
                        y["name"] = x["name"]

        # Add a `product` field to technosphere exchanges
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "technosphere":
                    # Check if the field 'product' is present
                    if not "product" in y:
                        y["product"] = self.correct_product_field(y)

                    # If a 'reference product' field is present, we make sure
                    # it matches with the new 'product' field
                    if "reference product" in y:
                        try:
                            assert y["product"] == y["reference product"]
                        except AssertionError:
                            y["product"] = self.correct_product_field(y)

        # Add a `code` field if missing
        for x in self.import_db.data:
            if "code" not in x:
                x["code"] = str(uuid.uuid4().hex)

    def correct_product_field(self, exc):
        """
        Find the correct name for the `product` field of the exchange
        :param exc: a dataset exchange
        :return: name of the product field of the exchange
        :rtype: str
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

        raise IndexError(
            f"An inventory exchange in {self.import_db.db_name} cannot be linked to the "
            f"biosphere or the ecoinvent database: {exc}"
        )

    def add_biosphere_links(self, delete_missing=False):
        """Add links for biosphere exchanges to :attr:`import_db`

        Modifies the :attr:`import_db` attribute in place.
        """
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "biosphere":
                    if isinstance(y["categories"], str):
                        y["categories"] = tuple(y["categories"].split("::"))
                    if len(y["categories"]) > 1:
                        try:
                            y["input"] = (
                                "biosphere3",
                                self.biosphere_dict[
                                    (
                                        y["name"],
                                        y["categories"][0],
                                        y["categories"][1],
                                        y["unit"],
                                    )
                                ],
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

    def remove_ds_and_modifiy_exchanges(self, name, ex_data):
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
    def __init__(self, database, version_in, version_out, path):
        super().__init__(database, version_in, version_out, path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):

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
    """

    def __init__(self, database, version_in, version_out, path, year, regions, model):
        super().__init__(database, version_in, version_out, path)
        self.year = year
        self.regions = regions
        self.model = model

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

        # We filter electric vehicles by year fo manufacture
        available_years = [2020, 2030, 2040, 2050]
        closest_year = min(available_years, key=lambda x: abs(x - self.year))

        list_vehicles = [
            "Bicycle,",
            "Kick-scooter,",
            "Moped,",
            "Scooter,",
            "Motorbike,",
        ]

        self.import_db.data = [
            x
            for x in self.import_db.data
            if not any(y in x["name"] for y in list_vehicles)
            or (
                any(y in x["name"] for y in list_vehicles)
                and str(closest_year) in x["name"]
                and "label-certified electricity" not in x["name"]
            )
        ]

        list_new_ds = []

        # create regional variants
        for dataset in self.import_db.data:
            if "transport, " in dataset["name"]:
                # change fuel supply exchanges for ecoinvent ones
                # so that it gets picked up by the fuels integration function
                for exc in dataset["exchanges"]:
                    if "fuel supply for gasoline" in exc["name"]:
                        exc["name"] = "market for petrol, low-sulfur"
                        exc["product"] = "petrol, low-sulfur"
                        exc["location"] = "RoW"
                        if "input" in exc:
                            exc.pop("input")

                for region in self.regions:
                    new_ds = wt.copy_to_new_location(dataset, region)

                    for exc in ws.production(new_ds):
                        if "input" in exc:
                            exc.pop("input")

                    if "input" in new_ds:
                        new_ds.pop("input")

                    new_ds = relink_technosphere_exchanges(
                        new_ds, self.database, self.model, iam_regions=self.regions
                    )

                    list_new_ds.append(new_ds)

        self.import_db.data.extend(list_new_ds)

        # remove empty fields
        for x in self.import_db.data:
            for k, v in list(x.items()):
                if not v:
                    del x[k]

    def merge_inventory(self):

        self.prepare_inventory()
        self.database.extend(self.import_db.data)

        print("Done!")

        return self.database


class PassengerCars(BaseInventoryImport):
    """
    Imports default inventories for passenger cars.
    """

    def __init__(
        self, database, version_in, version_out, model, year, regions, iam_data
    ):
        super().__init__(database, version_in, version_out, Path("."))

        self.db = database
        self.regions = regions
        self.model = model
        self.geomap = Geomap(
            model=model, current_regions=iam_data.coords["region"].values.tolist()
        )

        inventory_year = min(
            [2020, 2025, 2030, 2040, 2045, 2050], key=lambda x: abs(x - year)
        )

        filename = (
            model + "_pass_cars_inventory_data_ei_37_" + str(inventory_year) + ".pickle"
        )
        fp = INVENTORY_DIR / filename

        self.import_db = LCIImporter("passenger_cars")

        with open(fp, "rb") as handle:
            self.import_db.data = pickle.load(handle)

    def load_inventory(self, path):
        pass

    def prepare_inventory(self):
        # initially links to ei37
        if self.version_in != self.version_out:
            self.import_db.migrate(
                f"migration_{self.version_in.replace('.', '')}_{self.version_out.replace('.', '')}"
            )
            print(
                f"migration_{self.version_in.replace('.', '')}_{self.version_out.replace('.', '')}"
            )

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()

        for x in self.import_db.data:
            x["code"] = str(uuid.uuid4().hex)

    def merge_inventory(self):
        self.prepare_inventory()

        activities_to_remove = [
            "transport, passenger car",
            "market for passenger car",
            "market for transport, passenger car",
        ]

        self.db = [
            x
            for x in self.db
            if not any(y for y in activities_to_remove if y in x["name"])
        ]
        self.db.extend(self.import_db.data)

        exchanges_to_modify = [
            "market for transport, passenger car, large size, petrol, EURO 4",
            "market for transport, passenger car",
            "market for transport, passenger car, large size, petrol, EURO 3",
            "market for transport, passenger car, large size, diesel, EURO 4",
            "market for transport, passenger car, large size, diesel, EURO 5",
        ]
        for dataset in self.db:
            excs = (
                exc
                for exc in dataset["exchanges"]
                if exc["name"] in exchanges_to_modify and exc["type"] == "technosphere"
            )

            for exc in excs:

                try:

                    new_supplier = ws.get_one(
                        self.db,
                        *[
                            ws.contains(
                                "name",
                                "transport, passenger car, fleet average, all powertrains",
                            ),
                            ws.equals(
                                "location",
                                self.geomap.ecoinvent_to_iam_location(
                                    dataset["location"]
                                ),
                            ),
                            ws.contains("reference product", "transport"),
                        ],
                    )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

                except ws.NoResults:

                    new_supplier = ws.get_one(
                        self.db,
                        *[
                            ws.contains(
                                "name",
                                "transport, passenger car, fleet average, all powertrains",
                            ),
                            ws.equals("location", self.regions[0]),
                            ws.contains("reference product", "transport"),
                        ],
                    )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

                if "input" in exc:
                    exc.pop("input")

        return self.db


class Trucks(BaseInventoryImport):
    """
    Imports default inventories for trucks.
    """

    def __init__(
        self, database, version_in, version_out, model, year, regions, iam_data
    ):
        super().__init__(database, version_in, version_out, Path("."))

        self.db = database
        self.regions = regions
        self.geomap = Geomap(
            model=model, current_regions=iam_data.coords["region"].values.tolist()
        )

        inventory_year = min(
            [2020, 2025, 2030, 2040, 2045, 2050], key=lambda x: abs(x - year)
        )

        filename = (
            model + "_trucks_inventory_data_ei_37_" + str(inventory_year) + ".pickle"
        )
        fp = INVENTORY_DIR / filename

        self.import_db = LCIImporter("trucks")

        with open(fp, "rb") as handle:
            self.import_db.data = pickle.load(handle)

    def load_inventory(self, path):
        pass

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

        for x in self.import_db.data:
            x["code"] = str(uuid.uuid4().hex)

    def merge_inventory(self):
        self.prepare_inventory()

        # remove the old lorry transport datasets
        self.db = [x for x in self.db if "transport, freight, lorry" not in x["name"]]

        # add the new ones
        self.db.extend(self.import_db.data)

        # loop through datasets that use lorry transport
        for dataset in self.db:
            excs = (
                exc
                for exc in dataset["exchanges"]
                if "transport, freight, lorry" in exc["name"]
                and exc["type"] == "technosphere"
            )

            for exc in excs:

                if "3.5-7.5" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 3.5t"
                elif "7.5-16" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 7.5t"
                elif "16-32" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 26t"
                elif ">32" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 40t"
                elif "unspecified" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average"
                else:
                    search_for = "transport, freight, lorry, fleet average"

                try:
                    new_supplier = ws.get_one(
                        self.db,
                        *[
                            ws.equals("name", search_for),
                            ws.equals(
                                "location",
                                self.geomap.ecoinvent_to_iam_location(
                                    dataset["location"]
                                )
                                if dataset not in self.regions
                                else dataset["location"],
                            ),
                            ws.contains(
                                "reference product", "transport, freight, lorry"
                            ),
                        ],
                    )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

                except ws.NoResults:

                    new_supplier = ws.get_one(
                        self.db,
                        *[
                            ws.equals("name", search_for),
                            ws.equals("location", "World"),
                            ws.contains(
                                "reference product", "transport, freight, lorry"
                            ),
                        ],
                    )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

                if "input" in exc:
                    exc.pop("input")

        return self.db


class AdditionalInventory(BaseInventoryImport):
    """
    Import additional inventories, if any.
    """

    def __init__(self, database, version_in, version_out, path):
        super().__init__(database, version_in, version_out, path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def remove_missing_fields(self):

        for x in self.import_db.data:
            for k, v in list(x.items()):
                if not v:
                    del x[k]

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
            t = PrettyTable(["Name", "Reference product", "Location", "Unit", "File"])
            for dataset in list_missing_prod:
                t.add_row(
                    [
                        dataset.get("name", "XXXX"),
                        dataset.get("referece product", "XXXX"),
                        dataset.get("location", "XXXX"),
                        dataset.get("unit", "XXXX"),
                        self.path.name,
                    ]
                )

            print(t)

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
            t = PrettyTable(["Name", "Reference product", "Location", "Unit", "File"])
            for dataset in list_missing_ref:
                t.add_row(
                    [
                        dataset.get("name", "XXXX"),
                        dataset.get("referece product", "XXXX"),
                        dataset.get("location", "XXXX"),
                        dataset.get("unit", "XXXX"),
                        self.path.name,
                    ]
                )

            print(t)

        if len(list_missing_prod) > 0 or len(list_missing_ref) > 0:
            sys.exit()

        self.remove_missing_fields()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()


class CarculatorInventory(BaseInventoryImport):
    """
    Car models from the carculator project, https://github.com/romainsacchi/carculator
    """

    def __init__(
        self,
        database,
        version,
        fleet_file,
        model,
        year,
        regions,
        iam_data,
        filters=None,
    ):
        self.db_year = year
        self.model = model
        self.geomap = Geomap(
            model=model, current_regions=iam_data.coords["region"].values.tolist()
        )
        self.regions = regions
        self.fleet_file = fleet_file
        self.data = iam_data
        self.filter = ["fleet average"]

        if filters:
            self.filter.extend(filters)

        super().__init__(database, version, path=Path("."))

    def load_inventory(self, path):
        """Create `carculator` fleet average inventories for a given range of years."""

        cip = carculator.CarInputParameters()
        cip.static()
        _, array = carculator.fill_xarray_from_input_parameters(cip)

        array = array.interp(
            year=np.arange(1996, self.db_year + 1), kwargs={"fill_value": "extrapolate"}
        )
        cm = carculator.CarModel(array, cycle="WLTC 3.4")
        cm.set_all()

        fleet_array = carculator.create_fleet_composition_from_IAM_file(self.fleet_file)

        import_db = None

        for r, region in enumerate(self.regions):

            # The fleet file has REMIND region
            # Hence, if we use IMAGE, we need to convert
            # the region names
            # which is something `iam_to_GAINS_region()` does.
            if self.model == "remind":
                if region == "World":
                    reg_fleet = [r for r in self.regions if r != "World"]
                else:
                    reg_fleet = region
            if self.model == "image":
                if region == "World":
                    reg_fleet = [
                        self.geomap.iam_to_GAINS_region(r)
                        for r in self.regions
                        if r != "World"
                    ]
                else:
                    reg_fleet = self.geomap.iam_to_GAINS_region(region)

            fleet = fleet_array.sel(
                IAM_region=reg_fleet, vintage_year=np.arange(1996, self.db_year + 1)
            ).interp(variable=np.arange(1996, self.db_year + 1))

            years = []
            for y in np.arange(1996, self.db_year):
                if y in fleet.vintage_year:
                    if (
                        fleet.sel(vintage_year=y, variable=self.db_year)
                        .sum(dim=["size", "powertrain"])
                        .sum()
                        >= 0.01
                    ):
                        years.append(y)
            years.append(self.db_year)

            scope = {
                "powertrain": fleet.sel(vintage_year=years).powertrain.values,
                "size": fleet.sel(vintage_year=years).coords["size"].values,
                "year": years,
                "fu": {"fleet": fleet.sel(vintage_year=years), "unit": "vkm"},
            }

            background_configuration = {
                "country": region,
            }

            inventory = carculator.InventoryCalculation(
                cm.array, scope=scope, background_configuration=background_configuration
            )

            i = inventory.export_lci_to_bw(
                presamples=False,
                ecoinvent_version=str(self.version),
                create_vehicle_datasets=False,
            )

            # filter out cars if anything given in `self.filter`
            i.data = [
                ds_to_keep
                for ds_to_keep in i.data
                if "transport, passenger car" not in ds_to_keep["name"]
                or (
                    any(y.lower() in ds_to_keep["name"].lower() for y in self.filter)
                    and str(self.db_year) in ds_to_keep["name"]
                )
            ]

            # we want to remove all fuel and electricity supply datatsets
            # to only keep the one corresponding to the fleet year
            i.data = [
                ds_to_keep
                for ds_to_keep in i.data
                if not any(
                    forbidden_term in ds_to_keep["name"]
                    for forbidden_term in [
                        "fuel supply for",
                        "electricity supply for",
                        "electricity market for fuel preparation",
                    ]
                )
                or str(self.db_year) in ds_to_keep["name"]
            ]

            # we need to remove the electricity inputs in the fuel markets
            # that are typically added when synfuels are part of the blend
            for x in i.data:
                if "fuel supply for " in x["name"]:
                    for e in x["exchanges"]:
                        if "electricity market for " in e["name"]:
                            x["exchanges"].remove(e)

            # we want to rename the passenger car transport dataset
            # by removing the year in the name
            # we also want to change the names of fuel supply exchanges
            for x in i.data:
                if any(
                    y in x["name"]
                    for y in [
                        "transport, passenger car, fleet average",
                        "fuel supply for ",
                        "electricity supply for ",
                        "electricity market for fuel preparation",
                        "electricity market for energy storage",
                    ]
                ):
                    if "transport, passenger car, fleet average" in x["name"]:
                        x["name"] = x["name"][:-6]
                        for exc in ws.production(x):
                            exc["name"] = exc["name"][:-6]

                    if any(
                        d in x["name"]
                        for d in [
                            "fuel supply for ",
                            "electricity supply for ",
                            "electricity market for fuel preparation",
                        ]
                    ):
                        i.data.remove(x)

                    d_fuel_exchanges = {
                        "fuel supply for gasoline vehicles": {
                            "name": "market for petrol, low-sulfur",
                            "prod": "petrol, low-sulfur",
                            "loc": "RoW",
                        },
                        "fuel supply for diesel vehicles": {
                            "name": "market for diesel, low-sulfur",
                            "prod": "diesel, low-sulfur",
                            "loc": "RoW",
                        },
                        "fuel supply for gas vehicles": {
                            "name": "market for natural gas, low pressure, vehicle grade",
                            "prod": "natural gas, low pressure, vehicle grade",
                            "loc": "GLO",
                        },
                        "fuel supply for hydrogen vehicles": {
                            "name": "market for hydrogen, gaseous",
                            "prod": "hydrogen, gaseous",
                            "loc": "GLO",
                        },
                        "electricity supply for electric vehicles": {
                            "name": "market group for electricity, low voltage",
                            "prod": "electricity, low voltage",
                            "loc": "RER",
                        },
                        "electricity market for fuel preparation": {
                            "name": "market group for electricity, low voltage",
                            "prod": "electricity, low voltage",
                            "loc": "RER",
                        },
                        "electricity market for energy storage": {
                            "name": "market group for electricity, low voltage",
                            "prod": "electricity, low voltage",
                            "loc": "CN",
                        },
                    }

                    if "transport, passenger car, " in x["name"]:
                        for exc in ws.technosphere(x):
                            if any(d in exc["name"] for d in d_fuel_exchanges):
                                for key in (
                                    key
                                    for key in d_fuel_exchanges
                                    if key in exc["name"]
                                ):
                                    exc["name"] = d_fuel_exchanges[key]["name"]
                                    exc["product"] = d_fuel_exchanges[key]["prod"]
                                    exc["location"] = d_fuel_exchanges[key]["loc"]
                                    if "input" in exc:
                                        exc.pop("input")

            if r == 0:
                import_db = i
            else:
                # remove duplicate items if iterating over several regions
                i.data = [
                    x
                    for x in i.data
                    if (x["name"].lower(), x["location"])
                    not in [(z["name"].lower(), z["location"]) for z in import_db.data]
                ]
                import_db.data.extend(i.data)

        return import_db

    def prepare_inventory(self):
        self.add_biosphere_links(delete_missing=True)
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()

    def merge_inventory(self):
        self.prepare_inventory()

        activities_to_remove = [
            "transport, passenger car",
            "market for passenger car",
            "market for transport, passenger car",
        ]

        self.database = [
            dataset
            for dataset in self.database
            if not any(y for y in activities_to_remove if y in dataset["name"])
        ]
        self.database.extend(self.import_db.data)

        exchanges_to_modify = [
            "market for transport, passenger car, large size, petol, EURO 4",
            "market for transport, passenger car",
            "market for transport, passenger car, large size, petrol, EURO 3",
            "market for transport, passenger car, large size, diesel, EURO 4",
            "market for transport, passenger car, large size, diesel, EURO 5",
        ]
        for dataset in self.database:
            excs = (
                exc
                for exc in dataset["exchanges"]
                if exc["name"] in exchanges_to_modify and exc["type"] == "technosphere"
            )

            for exc in excs:

                try:

                    new_supplier = ws.get_one(
                        self.database,
                        *[
                            ws.contains(
                                "name",
                                "transport, passenger car, fleet average, all powertrains",
                            ),
                            ws.equals(
                                "location",
                                self.geomap.ecoinvent_to_iam_location(
                                    dataset["location"]
                                ),
                            ),
                            ws.contains("reference product", "transport"),
                        ],
                    )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

                except ws.NoResults:

                    new_supplier = ws.get_one(
                        self.database,
                        *[
                            ws.contains(
                                "name",
                                "transport, passenger car, fleet average, all powertrains",
                            ),
                            ws.equals("location", self.regions[0]),
                            ws.contains("reference product", "transport"),
                        ],
                    )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

        print("Done!")

        return self.database


class TruckInventory(BaseInventoryImport):
    """
    Car models from the carculator project, https://github.com/romainsacchi/carculator
    """

    def __init__(
        self,
        database,
        version_in,
        version_out,
        fleet_file,
        model,
        year,
        regions,
        iam_data,
        filters=None,
    ):

        self.db_year = year
        self.model = model
        self.geomap = Geomap(
            model=model, current_regions=iam_data.coords["region"].values.tolist()
        )
        self.regions = regions
        self.fleet_file = fleet_file
        self.filter = ["fleet average"]
        self.data = iam_data

        if filters:
            self.filter.extend(filters)

        super().__init__(database, version_in, version_out, Path("."))

    def load_inventory(self, path):
        """Create `carculator_truck` fleet average inventories for a given range of years."""

        fleet_array = carculator_truck.create_fleet_composition_from_IAM_file(
            self.fleet_file
        )

        fleet = fleet_array.sel(IAM_region="EUR").interp(
            variable=np.arange(1996, self.db_year + 1)
        )

        scope = {
            "powertrain": fleet.powertrain.values,
            "size": fleet.coords["size"].values,
            "fu": {"fleet": fleet.vintage_year.values, "unit": "tkm"},
        }

        tip = carculator_truck.TruckInputParameters()
        tip.static()
        _, array = carculator_truck.fill_xarray_from_input_parameters(tip, scope=scope)

        array = array.interp(
            year=np.arange(2010, self.db_year + 1), kwargs={"fill_value": "extrapolate"}
        )
        tm = carculator_truck.TruckModel(array, cycle="Regional delivery", country="CH")
        tm.set_all()

        import_db = None

        for r, region in enumerate(self.regions):

            if region == "World":
                fleet = fleet_array.sum(dim="IAM_region").interp(
                    variable=np.arange(1996, self.db_year + 1)
                )

            else:

                # The fleet file has REMIND region
                # Hence, if we use IMAGE, we need to convert
                # the region names
                # which is something `iam_to_GAINS_region()` does.
                if self.model == "remind":
                    reg_fleet = region
                if self.model == "image":
                    reg_fleet = self.geomap.iam_to_GAINS_region(region)

                fleet = fleet_array.sel(IAM_region=reg_fleet).interp(
                    variable=np.arange(1996, self.db_year + 1)
                )

            years = []
            for y in np.arange(2010, self.db_year):
                if y in fleet.vintage_year:
                    if (
                        fleet.sel(vintage_year=y, variable=self.db_year).sum(
                            dim=["size", "powertrain"]
                        )
                        >= 0.01
                    ):
                        years.append(y)
            years.append(self.db_year)

            scope = {
                "powertrain": fleet.sel(vintage_year=years).powertrain.values,
                "size": fleet.sel(vintage_year=years).coords["size"].values,
                "year": years,
                "fu": {"fleet": fleet.sel(vintage_year=years), "unit": "tkm"},
            }

            background_configuration = {
                "country": region,
            }

            inventory = carculator_truck.InventoryCalculation(
                tm,
                scope=scope,
                background_configuration=background_configuration,
            )

            i = inventory.export_lci_to_bw(
                presamples=False,
                ecoinvent_version=str(self.version),
                create_vehicle_datasets=False,
            )

            # filter out trucks if anything given in `self.filter`
            i.data = [
                dataset
                for dataset in i.data
                if "transport, " not in dataset["name"]
                or (
                    any(y.lower() in dataset["name"].lower() for y in self.filter)
                    and str(self.db_year) in dataset["name"]
                )
            ]

            # we want to remove all fuel and electricity supply datatsets
            # to only keep the one corresponding to the fleet year
            i.data = [
                dataset
                for dataset in i.data
                if not any(
                    forbidden_term in dataset["name"]
                    for forbidden_term in [
                        "fuel supply for",
                        "electricity supply for",
                        "electricity market for fuel preparation",
                        "electricity market for energy storage",
                    ]
                )
            ]

            # we want to rename the lorry transport datasets
            # by removing the year in the name
            # we also want to change the names of fuel supply exchanges
            for dataset in i.data:

                if "transport, freight, lorry, " in dataset["name"]:

                    dataset["name"] = dataset["name"][:-6]
                    for exc in ws.production(dataset):
                        exc["name"] = exc["name"][:-6]

                if dataset["name"] == "transport, freight, lorry, fleet average":
                    dataset[
                        "name"
                    ] = "market for transport, freight, lorry, unspecified"
                    dataset[
                        "reference product"
                    ] = "transport, freight, lorry, unspecified"
                    for exc in ws.production(dataset):
                        exc[
                            "name"
                        ] = "market for transport, freight, lorry, unspecified"
                        exc["product"] = "transport, freight, lorry, unspecified"

                d_fuel_exchanges = {
                    "fuel supply for gasoline vehicles": {
                        "name": "market for petrol, low-sulfur",
                        "prod": "petrol, low-sulfur",
                        "loc": "RoW",
                    },
                    "fuel supply for diesel vehicles": {
                        "name": "market for diesel, low-sulfur",
                        "prod": "diesel, low-sulfur",
                        "loc": "RoW",
                    },
                    "fuel supply for gas vehicles": {
                        "name": "market for natural gas, low pressure, vehicle grade",
                        "prod": "natural gas, low pressure, vehicle grade",
                        "loc": "GLO",
                    },
                    "fuel supply for hydrogen vehicles": {
                        "name": "market for hydrogen, gaseous",
                        "prod": "hydrogen, gaseous",
                        "loc": "GLO",
                    },
                    "electricity supply for electric vehicles": {
                        "name": "market group for electricity, low voltage",
                        "prod": "electricity, low voltage",
                        "loc": "RER",
                    },
                    "electricity market for fuel preparation": {
                        "name": "market group for electricity, low voltage",
                        "prod": "electricity, low voltage",
                        "loc": "RER",
                    },
                    "electricity market for energy storage": {
                        "name": "market group for electricity, low voltage",
                        "prod": "electricity, low voltage",
                        "loc": "CN",
                    },
                }

                for exc in ws.technosphere(dataset):
                    if any(d in exc["name"] for d in d_fuel_exchanges):
                        for key in (
                            key for key in d_fuel_exchanges if key in exc["name"]
                        ):

                            exc["name"] = d_fuel_exchanges[key]["name"]
                            exc["product"] = d_fuel_exchanges[key]["prod"]
                            exc["location"] = d_fuel_exchanges[key]["loc"]
                            if "input" in exc:
                                exc.pop("input")

            if r == 0:
                import_db = i
            else:
                # remove duplicate items if iterating over several regions
                i.data = [
                    dataset
                    for dataset in i.data
                    if (dataset["name"].lower(), dataset["location"])
                    not in [(z["name"].lower(), z["location"]) for z in import_db.data]
                ]
                import_db.data.extend(i.data)

        return import_db

    def prepare_inventory(self):
        self.add_biosphere_links(delete_missing=True)
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()

    def merge_inventory(self):
        self.prepare_inventory()

        activities_to_remove = [
            "transport, freight, lorry",
        ]

        self.database = [
            dataset
            for dataset in self.database
            if not any(y in dataset["name"] for y in activities_to_remove)
        ]
        self.database.extend(self.import_db.data)

        for dataset in self.database:
            excs = (
                exc
                for exc in dataset["exchanges"]
                if "transport, freight, lorry" in exc["name"]
                and exc["type"] == "technosphere"
            )

            for exc in excs:

                if "3.5-7.5" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 3.5t"
                if "7.5-16" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 7.5t"
                if "16-32" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 26t"
                if ">32" in exc["name"]:
                    search_for = "transport, freight, lorry, fleet average, 40t"
                if "unspecified" in exc["name"]:
                    search_for = "market for transport, freight, lorry, unspecified"

                if not any(
                    sub_string in ["3.5-7.5", "7.5-16", "16-32", ">32", "unspecified"]
                    for sub_string in exc["name"]
                ):
                    search_for = "market for transport, freight, lorry, unspecified"

                try:
                    new_supplier = ws.get_one(
                        self.database,
                        *[
                            ws.equals("name", search_for),
                            ws.equals(
                                "location",
                                self.geomap.ecoinvent_to_iam_location(
                                    dataset["location"]
                                ),
                            ),
                            ws.contains(
                                "reference product", "transport, freight, lorry"
                            ),
                        ],
                    )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

                except ws.NoResults:

                    search_for = "market for transport, freight, lorry, unspecified"

                    try:
                        new_supplier = ws.get_one(
                            self.database,
                            *[
                                ws.equals("name", search_for),
                                ws.equals(
                                    "location",
                                    self.geomap.ecoinvent_to_iam_location(
                                        dataset["location"]
                                    ),
                                ),
                                ws.contains(
                                    "reference product",
                                    "transport, freight, lorry, unspecified",
                                ),
                            ],
                        )

                    except ws.NoResults:

                        search_for = "market for transport, freight, lorry, unspecified"

                        try:
                            new_supplier = ws.get_one(
                                self.database,
                                *[
                                    ws.equals("name", search_for),
                                    ws.contains(
                                        "reference product",
                                        "transport, freight, lorry, unspecified",
                                    ),
                                ],
                            )

                        except ws.NoResults:
                            print(f"no results for {exc['name']} in {exc['location']}")

                            print("available trucks")
                            for available_dataset in self.database:
                                if (
                                    "transport, freight, lorry"
                                    in available_dataset["name"]
                                ):
                                    print(
                                        available_dataset["name"],
                                        available_dataset["location"],
                                    )

                        except ws.MultipleResults:
                            # If multiple trucks are available, but none of the correct region,
                            # we pick a a truck from the "World" region
                            print("found several suppliers")
                            new_supplier = ws.get_one(
                                self.database,
                                *[
                                    ws.equals("name", search_for),
                                    ws.equals("location", "World"),
                                    ws.contains(
                                        "reference product", "transport, freight, lorry"
                                    ),
                                ],
                            )

                    exc["name"] = new_supplier["name"]
                    exc["location"] = new_supplier["location"]
                    exc["product"] = new_supplier["reference product"]
                    exc["unit"] = new_supplier["unit"]

        print("Done!")

        return self.database
