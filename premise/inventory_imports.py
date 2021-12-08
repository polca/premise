import csv
import itertools
import pickle
import sys
import uuid
from pathlib import Path

import numpy as np
from bw2io import ExcelImporter, Migration
from bw2io.importers.base_lci import LCIImporter
from prettytable import PrettyTable
from wurst import searching as ws
from wurst import transformations as wt
import pandas as pd
import re

from . import DATA_DIR, INVENTORY_DIR
from .geomap import Geomap
from .utils import relink_technosphere_exchanges, eidb_label

FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "dict_biosphere.txt"
FILEPATH_MIGRATION_MAP = INVENTORY_DIR / "migration_map.csv"
FILEPATH_FLEET_COMP = (
    DATA_DIR / "iam_output_files" / "fleet_files" / "fleet_all_vehicles.csv"
)


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


def create_fleet_vehicles(
    datasets, regions_mapping, vehicle_type, year, model, scenario
):

    if not FILEPATH_FLEET_COMP.is_file():
        raise FileNotFoundError("The fleet composition file could not be found.")

    dataframe = pd.read_csv(FILEPATH_FLEET_COMP, sep=";")

    dataframe["region"] = dataframe["region"].map(regions_mapping)

    arr = (
        dataframe.groupby(["year", "region", "powertrain", "construction_year", "size"])
        .sum()["vintage_demand_vkm"]
        .to_xarray()
    )
    arr = arr.fillna(0)

    vehicles_names = {
        "bus": ["13m single deck urban bus"],
        "truck": ["18t", "26t", "3.5t", "7.5t", "40t"],
        "car": [
            "Mini",
            "Small",
            "Lower medium",
            "Medium",
            "Medium SUV",
            "Large",
            "Large SUV",
            "Van",
        ],
    }

    constr_year_map = {
        y: int(y.split("-")[-1]) for y in arr.coords["construction_year"].values
    }

    # We filter electric vehicles by year fo manufacture
    available_years = np.arange(2000, 2065, 5)
    ref_year = min(available_years, key=lambda x: abs(x - year))

    available_ds = []

    pwt_map = {
        "fuel cell electric": "FCEV",
        "battery electric - opportunity charging": "BEV-opp",
        "battery electric - overnight charging": "BEV-depot",
        "battery electric - battery-equipped trolleybus": "BEV-motion",
        "battery electric": "BEV",
        "diesel hybrid": "HEV-d",
        "plugin diesel hybrid": "PHEV-d",
        "diesel": "ICEV-d",
        "compressed gas": "ICEV-g",
    }

    d_names = {}

    for ds in datasets:
        if ds["name"].startswith("transport, "):

            if vehicle_type == "bus":
                if len(ds["name"].split(", ")) == 6:
                    if "battery electric" in ds["name"].split(", ")[2]:
                        _, _, pwt, _, size, y = ds["name"].split(", ")

                    else:
                        _, _, pwt, size, y, _ = ds["name"].split(", ")
                else:
                    _, _, pwt, size, y = ds["name"].split(", ")

            elif vehicle_type == "truck":
                if len(ds["name"].split(", ")) == 8:
                    if "battery electric" in ds["name"].split(", ")[3]:
                        _, _, _, pwt, _, size, y, type = ds["name"].split(", ")

                    else:
                        _, _, _, pwt, size, y, _, type = ds["name"].split(", ")
                else:
                    _, _, _, pwt, size, y, type = ds["name"].split(", ")

                size = size.replace(" gross weight", "")

            else:
                if len(ds["name"].split(", ")) == 6:
                    _, _, pwt, _, size, y = ds["name"].split(", ")
                else:
                    _, _, pwt, size, y = ds["name"].split(", ")

            if vehicle_type == "truck":
                d_names[(pwt_map[pwt], size, int(y), type)] = (
                    ds["name"],
                    ds["reference product"],
                    ds["unit"],
                )
                available_ds.append((pwt_map[pwt], size, int(y)))

            else:
                d_names[(pwt_map[pwt], size, int(y))] = (
                    ds["name"],
                    ds["reference product"],
                    ds["unit"],
                )
                available_ds.append((pwt_map[pwt], size, int(y)))

    name_map = {"bus": "transport, passenger bus", "truck": "transport, freight, lorry"}

    unit_map = {"bus": "passenger-kilometer", "truck": "ton kilometer"}

    list_act = []

    avg_load = {
        "urban delivery": {
            "3.5t": 0.26,
            "7.5t": 0.52,
            "18t": 1.35,
            "26t": 2.05,
            "32t": 6.1,
            "40t": 6.1,
        },
        "regional delivery": {
            "3.5t": 0.26,
            "7.5t": 0.52,
            "18t": 1.35,
            "26t": 2.05,
            "32t": 6.1,
            "40t": 6.1,
        },
        "long haul": {
            "3.5t": 0.8,
            "7.5t": 1.6,
            "18t": 4.1,
            "26t": 6.2,
            "32t": 9.1,
            "40t": 9.1,
        },
    }

    for region in arr.coords["region"].values:

        sel = arr.sel(region=region, size=vehicles_names[vehicle_type], year=ref_year)
        total_km = sel.sum()

        if total_km > 0:

            if vehicle_type == "truck":
                act_urban = {
                    "name": f"{name_map[vehicle_type]}, unspecified, urban delivery",
                    "reference product": name_map[vehicle_type],
                    "unit": unit_map[vehicle_type],
                    "location": region,
                    "exchanges": [
                        {
                            "name": f"{name_map[vehicle_type]}, unspecified, urban delivery",
                            "product": name_map[vehicle_type],
                            "unit": unit_map[vehicle_type],
                            "location": region,
                            "type": "production",
                            "amount": 1,
                        }
                    ],
                    "code": str(uuid.uuid4().hex),
                    "database": eidb_label(model, scenario, year),
                }

                act_regional = {
                    "name": f"{name_map[vehicle_type]}, unspecified, regional delivery",
                    "reference product": name_map[vehicle_type],
                    "unit": unit_map[vehicle_type],
                    "location": region,
                    "exchanges": [
                        {
                            "name": f"{name_map[vehicle_type]}, unspecified, regional delivery",
                            "product": name_map[vehicle_type],
                            "unit": unit_map[vehicle_type],
                            "location": region,
                            "type": "production",
                            "amount": 1,
                        }
                    ],
                    "code": str(uuid.uuid4().hex),
                    "database": eidb_label(model, scenario, year),
                }

                act_long_haul = {
                    "name": f"{name_map[vehicle_type]}, unspecified, long haul",
                    "reference product": name_map[vehicle_type],
                    "unit": unit_map[vehicle_type],
                    "location": region,
                    "exchanges": [
                        {
                            "name": f"{name_map[vehicle_type]}, unspecified, long haul",
                            "product": name_map[vehicle_type],
                            "unit": unit_map[vehicle_type],
                            "location": region,
                            "type": "production",
                            "amount": 1,
                        }
                    ],
                    "code": str(uuid.uuid4().hex),
                    "database": eidb_label(model, scenario, year),
                }

            else:
                act = {
                    "name": f"{name_map[vehicle_type]}, unspecified",
                    "reference product": name_map[vehicle_type],
                    "unit": unit_map[vehicle_type],
                    "location": region,
                    "exchanges": [
                        {
                            "name": f"{name_map[vehicle_type]}, unspecified",
                            "product": name_map[vehicle_type],
                            "unit": unit_map[vehicle_type],
                            "location": region,
                            "type": "production",
                            "amount": 1,
                        }
                    ],
                    "code": str(uuid.uuid4().hex),
                    "database": eidb_label(model, scenario, year),
                }

            for s in vehicles_names[vehicle_type]:
                for y in sel.coords["construction_year"].values:
                    for pt in sel.coords["powertrain"].values:
                        indiv_km = sel.sel(size=s, construction_year=y, powertrain=pt)
                        if indiv_km > 0 and (pt, s, constr_year_map[y]) in available_ds:
                            indiv_share = (indiv_km / total_km).values.item(0)

                            if vehicle_type == "truck":
                                if (
                                    pt,
                                    s,
                                    constr_year_map[y],
                                    "urban delivery",
                                ) in d_names:

                                    name, ref, unit = d_names[
                                        (pt, s, constr_year_map[y], "urban delivery")
                                    ]

                                    act_urban["exchanges"].append(
                                        {
                                            "name": name,
                                            "product": ref,
                                            "unit": unit,
                                            "location": region,
                                            "type": "technosphere",
                                            "amount": indiv_share
                                            * avg_load["urban delivery"][s],
                                        }
                                    )

                                if (
                                    pt,
                                    s,
                                    constr_year_map[y],
                                    "regional delivery",
                                ) in d_names:

                                    name, ref, unit = d_names[
                                        (pt, s, constr_year_map[y], "regional delivery")
                                    ]
                                    act_regional["exchanges"].append(
                                        {
                                            "name": name,
                                            "product": ref,
                                            "unit": unit,
                                            "location": region,
                                            "type": "technosphere",
                                            "amount": indiv_share
                                            * avg_load["regional delivery"][s],
                                        }
                                    )

                                if (pt, s, constr_year_map[y], "long haul") in d_names:

                                    name, ref, unit = d_names[
                                        (pt, s, constr_year_map[y], "long haul")
                                    ]
                                    act_long_haul["exchanges"].append(
                                        {
                                            "name": name,
                                            "product": ref,
                                            "unit": unit,
                                            "location": region,
                                            "type": "technosphere",
                                            "amount": indiv_share
                                            * avg_load["long haul"][s],
                                        }
                                    )

                            else:
                                name, ref, unit = d_names[(pt, s, constr_year_map[y])]

                                act["exchanges"].append(
                                    {
                                        "name": name,
                                        "product": ref,
                                        "unit": unit,
                                        "location": region,
                                        "type": "technosphere",
                                        "amount": indiv_share,
                                    }
                                )

            if vehicle_type == "truck":
                total = 0
                for exc in act_urban["exchanges"]:
                    if exc["type"] == "technosphere":
                        total += exc["amount"]

                for exc in act_urban["exchanges"]:
                    if exc["type"] == "technosphere":
                        exc["amount"] /= total

                total = 0
                for exc in act_regional["exchanges"]:
                    if exc["type"] == "technosphere":
                        total += exc["amount"]

                for exc in act_regional["exchanges"]:
                    if exc["type"] == "technosphere":
                        exc["amount"] /= total

                total = 0
                for exc in act_long_haul["exchanges"]:
                    if exc["type"] == "technosphere":
                        total += exc["amount"]

                for exc in act_long_haul["exchanges"]:
                    if exc["type"] == "technosphere":
                        exc["amount"] /= total

                if len(act_urban["exchanges"]) > 1:
                    list_act.append(act_urban)
                if len(act_regional["exchanges"]) > 1:
                    list_act.append(act_regional)
                if len(act_long_haul["exchanges"]) > 1:
                    list_act.append(act_long_haul)
            else:
                list_act.append(act)

    return list_act


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
        return self.import_db

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
                    # if "reference product" in y:
                    #    try:
                    #        assert y["product"] == y["reference product"]
                    #    except AssertionError:
                    #        y["product"] = self.correct_product_field(y)

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

        # raise IndexError(
        #    f"An inventory exchange in {self.import_db.db_name} cannot be linked to the "
        #    f"biosphere or the ecoinvent database: {exc}"
        # )
        print(
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

    def __init__(
        self,
        database,
        version_in,
        version_out,
        path,
        year,
        regions,
        model,
        scenario,
        vehicle_type,
        relink=False,
        has_fleet=False,
    ):
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

        list_vehicles = [
            "Bicycle,",
            "Kick-scooter,",
            "Moped,",
            "Scooter,",
            "Motorbike,",
            "urban delivery",
            "regional delivery",
            "long haul",
            "passenger bus",
        ]

        # We filter electric vehicles by year fo manufacture
        available_years = [2020, 2030, 2040, 2050]
        closest_year = min(available_years, key=lambda x: abs(x - self.year))

        if self.has_fleet:
            if self.model != "remind":
                region_map = {
                    self.geo.iam_to_iam_region(loc): loc for loc in self.regions
                }
            else:
                region_map = {loc: loc for loc in self.regions}

            self.import_db.data = [
                x
                for x in self.import_db.data
                if not any(y in x["name"].lower() for y in list_vehicles)
                or (
                    not any(
                        z for z in re.findall(r"\d+", x["name"]) if int(z) > self.year
                    )
                    and "label-certified electricity" not in x["name"]
                )
            ]

            fleet_act = create_fleet_vehicles(
                self.import_db.data,
                regions_mapping=region_map,
                vehicle_type=self.vehicle_type,
                year=self.year,
                model=self.model,
                scenario=self.scenario,
            )

            self.import_db.data.extend(fleet_act)

        else:

            self.import_db.data = [
                x
                for x in self.import_db.data
                if not any(y in x["name"] for y in list_vehicles)
                or (
                    str(closest_year) in x["name"]
                    and "label-certified electricity" not in x["name"]
                )
            ]

            # remove the year in the name
            str_to_replace = ", " + str(closest_year)
            for ds in self.import_db.data:
                if str_to_replace in ds["name"]:
                    ds["name"] = ds["name"].replace(str_to_replace, "")
                    for exc in ds["exchanges"]:
                        if str_to_replace in exc["name"]:
                            exc["name"] = exc["name"].replace(str_to_replace, "")

        list_new_ds = []

        # create regional variants
        for dataset in self.import_db.data:
            if (
                "transport, " in dataset["name"]
                and "unspecified" not in dataset["name"]
            ):
                for region in self.regions:
                    new_ds = wt.copy_to_new_location(dataset, region)

                    for exc in ws.production(new_ds):
                        if "input" in exc:
                            exc.pop("input")

                    if self.relink:
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

        # if trucks, need to reconnect everything
        # loop through datasets that use lorry transport
        if self.vehicle_type == "truck":
            for dataset in ws.get_many(
                self.database,
                ws.doesnt_contain_any("name", ["freight, lorry"]),
                ws.exclude(ws.equals("unit", "ton kilometer"))
            ):
                for exc in ws.technosphere(
                    dataset,
                    ws.contains("name", "transport, freight, lorry"),
                    ws.equals("unit", "ton kilometer")
                ):

                    if "input" in exc:
                        del exc["input"]

                    if dataset["unit"] == "kilogram":
                        if exc["amount"] * 1000 <= 150:
                            exc["name"] = "transport, freight, lorry, unspecified, urban delivery"

                        elif 150 < exc["amount"] * 1000 <= 450:
                            exc["name"] = "transport, freight, lorry, unspecified, regional delivery"

                        else:
                            exc["name"] = "transport, freight, lorry, unspecified, long haul"
                    else:
                        exc["name"] = "transport, freight, lorry, unspecified, long haul"

                    exc["product"] = "transport, freight, lorry"
                    exc["location"] = self.geo.ecoinvent_to_iam_location(dataset["location"])



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

