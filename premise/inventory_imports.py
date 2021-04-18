from . import DATA_DIR, INVENTORY_DIR
import wurst
from prettytable import PrettyTable
from wurst import searching as ws
from bw2io import ExcelImporter, Migration
from bw2io.importers.base_lci import LCIImporter
import carculator
import carculator_truck
from pathlib import Path
import csv
import uuid
import numpy as np
from .geomap import Geomap
import sys
import xarray as xr
import pickle

FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "dict_biosphere.txt"

FILEPATH_MIGRATION_MAP = INVENTORY_DIR / "migration_map.csv"


def generate_migration_maps(origin, destination):

    response = {"fields": ["name", "reference product", "location"], "data": []}

    with open(FILEPATH_MIGRATION_MAP, "r") as read_obj:
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


EI_37_35_MIGRATION_MAP = generate_migration_maps("37", "35")
EI_37_36_MIGRATION_MAP = generate_migration_maps("37", "36")
EI_35_37_MIGRATION_MAP = generate_migration_maps("35", "37")
EI_35_36_MIGRATION_MAP = generate_migration_maps("35", "36")
EI_36_37_MIGRATION_MAP = generate_migration_maps("36", "37")
EI_36_35_MIGRATION_MAP = generate_migration_maps("36", "35")


class BaseInventoryImport:
    """
    Base class for inventories that are to be merged with the ecoinvent database.

    :ivar db: the target database for the import (the Ecoinvent database),
              unpacked to a list of dicts
    :vartype db: list
    :ivar version: the target Ecoinvent database version
    :vartype version: str
    """

    def __init__(self, database, version, path):
        """Create a :class:`BaseInventoryImport` instance.

        :param list database: the target database for the import (the Ecoinvent database),
                              unpacked to a list of dicts
        :param version: the version of the target database ("3.5", "3.6", "3.7", "3.7.1")
        :type version: str
        :param path: Path to the imported inventory.
        :type path: str or Path

        """
        self.db = database
        self.db_code = [x["code"] for x in self.db]
        self.db_names = [
            (x["name"], x["reference product"], x["location"]) for x in self.db
        ]
        self.version = version
        self.biosphere_dict = self.get_biosphere_code()

        path = Path(path)

        if path != Path("."):
            if not path.is_file():
                raise FileNotFoundError(
                    "The inventory file {} could not be found.".format(path)
                )

        self.path = path
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        """Load an inventory from a specified path.

        Sets the :attr:`import_db` attribute.

        :param str path: Path to the inventory file
        :returns: Nothing.

        """
        pass

    def prepare_inventory(self):
        """Prepare the inventory for the merger with Ecoinvent.

        Modifies :attr:`import_db` in-place.

        :returns: Nothing

        """
        pass

    def check_for_duplicates(self):
        """
        Check whether the inventories to be imported are not
        already in the source database.
        """

        # print if we find datasets that already exist
        already_exist = [
            (x["name"], x["reference product"], x["location"])
            for x in self.import_db.data
            if x["code"] in self.db_code
        ]

        already_exist.extend(
            [
                (x["name"], x["reference product"], x["location"])
                for x in self.import_db.data
                if (x["name"], x["reference product"], x["location"]) in self.db_names
            ]
        )

        if len(already_exist) > 0:
            print(
                "The following datasets to import already exist in the source database. They will not be imported"
            )
            t = PrettyTable(["Name", "Reference product", "Location", "File"])
            for ds in already_exist:
                t.add_row([ds[0][:50], ds[1][:30], ds[2], self.path.name])

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
        """Prepare :attr:`import_db` and merge the inventory to the ecoinvent :attr:`db`.

        Calls :meth:`prepare_inventory`. Changes the :attr:`db` attribute.

        :returns: Nothing

        """
        self.prepare_inventory()
        self.db.extend(self.import_db)

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

    @staticmethod
    def get_biosphere_code():
        """
        Retrieve a dictionary with biosphere flow names and uuid codes.

        :returns: dictionary with biosphere flow names as keys and uuid code as values
        :rtype: dict
        """

        if not FILEPATH_BIOSPHERE_FLOWS.is_file():
            raise FileNotFoundError(
                "The dictionary of biosphere flows could not be found."
            )

        csv_dict = {}

        with open(FILEPATH_BIOSPHERE_FLOWS) as f:
            input_dict = csv.reader(f, delimiter=";")
            for row in input_dict:
                csv_dict[(row[0], row[1], row[2], row[3])] = row[4]

        return csv_dict

    def add_product_field_to_exchanges(self):
        """Add the `product` key to the production and
        technosphere exchanges in :attr:`import_db`.
        Also add `code` field if missing.

        For production exchanges, use the value of the `reference_product` field.
        For technosphere exchanges, search the activities in :attr:`import_db` and
        use the reference product. If none is found, search the Ecoinvent :attr:`db`.
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
                    self.db,
                    ws.equals("name", exc["name"]),
                    ws.equals("location", exc["location"]),
                    ws.equals("unit", exc["unit"]),
                ),
                None,
            )

        if candidate is not None:
            return candidate["reference product"]
        else:
            raise IndexError(
                "An inventory exchange in {} cannot be linked to the biosphere or the ecoinvent database: {}".format(
                    self.import_db.db_name, exc
                )
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


class CarmaCCSInventory(BaseInventoryImport):
    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # Carma inventories are originally made with ei 3.5
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_35_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")

        if self.version == "3.6":
            # apply some updates to comply with ei 3.6
            new_technosphere_data = EI_35_36_MIGRATION_MAP

            Migration("migration_36").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5 to 3.6",
            )
            self.import_db.migrate("migration_36")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Check for duplicates
        self.check_for_duplicates()


class DACInventory(BaseInventoryImport):
    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # Inventories initially made with ei 37
        if self.version == "3.6":
            # apply some updates to go from ei3.7 to ei3.6
            new_technosphere_data = EI_37_36_MIGRATION_MAP

            Migration("migration_36").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.7 to 3.5",
            )
            self.import_db.migrate("migration_36")

        if self.version == "3.5":
            # apply some updates to go from ei3.7 to ei3.5
            new_technosphere_data = EI_37_35_MIGRATION_MAP

            Migration("migration_35").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.7 to 3.5",
            )
            self.import_db.migrate("migration_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Add carbon storage for CCS technologies
        print("Add fossil carbon dioxide storage for CCS technologies.")
        self.add_negative_CO2_flows_for_biomass_CCS()

        # Check for duplicates
        self.check_for_duplicates()

    def add_negative_CO2_flows_for_biomass_CCS(self):
        """
        Rescale the amount of all exchanges of carbon dioxide, non-fossil by a factor -9 (.9/-.1),
        to account for sequestered CO2.

        All CO2 capture and storage in the Carma datasets is assumed to be 90% efficient.
        Thus, we can simply find out what the new CO2 emission is and then we know how much gets stored in the ground.
        It's very important that we ONLY do this for biomass CCS plants, as only they will have negative emissions!

        We also rename the emission to 'Carbon dioxide, from soil or biomass stock' so that it is properly
        characterized by IPCC's GWP100a method.

        Modifies in place (does not return anything).

        """
        for ds in ws.get_many(
            self.db, ws.contains("name", "storage"), ws.equals("database", "Carma CCS")
        ):
            for exc in ws.biosphere(
                ds, ws.equals("name", "Carbon dioxide, non-fossil")
            ):
                wurst.rescale_exchange(exc, (0.9 / -0.1), remove_uncertainty=True)


class BiofuelInventory(BaseInventoryImport):
    """
    Biofuel datasets from the master thesis of Francesco Cozzolino (2018).
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):

        # migration for ei 3.7
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_35_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")

        # Migrations for 3.6
        if self.version == "3.6":
            migrations = EI_35_36_MIGRATION_MAP

            Migration("biofuels_ecoinvent_36").write(
                migrations,
                description="Change technosphere names due to change from 3.5 to 3.6",
            )
            self.import_db.migrate("biofuels_ecoinvent_36")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Check for duplicates
        self.check_for_duplicates()


class HydrogenInventory(BaseInventoryImport):
    """
    Hydrogen datasets from the ELEGANCY project (2019).
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # inventories initially links to ei37

        # migration for ei 3.6
        if self.version == "3.6":
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_37_36_MIGRATION_MAP

            Migration("migration_36").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.7 to 3.6",
            )
            self.import_db.migrate("migration_36")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("hydrogen_ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.7 to 3.5",
            )
            self.import_db.migrate("hydrogen_ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Check for duplicates
        self.check_for_duplicates()


class HydrogenBiogasInventory(BaseInventoryImport):
    """
    Hydrogen datasets from the ELEGANCY project (2019).
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # migration for ei 3.7
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_36_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("hydrogen_ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.5 to 3.6",
            )
            self.import_db.migrate("hydrogen_ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Check for duplicates
        self.check_for_duplicates()


class BiogasInventory(BaseInventoryImport):
    """
    Biogas datasets from the SCCER project (2019).
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # migration for ei 3.7
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_36_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("biogas_ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.7 to 3.5",
            )
            self.import_db.migrate("biogas_ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()

        # Check for duplicates
        self.check_for_duplicates()


class SyngasInventory(BaseInventoryImport):
    """
    Synthetic fuel datasets from the PSI project (2019).
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # migration for ei 3.7
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_36_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")

        # migration for ei 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("syngas_ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.6 to 3.5",
            )
            self.import_db.migrate("syngas_ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()


class SynfuelInventory(BaseInventoryImport):
    """
    Synthetic fuel datasets from the PSI project (2019).
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # migration for ei 3.7
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_36_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")

        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("syngas_ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.6 to 3.5",
            )
            self.import_db.migrate("syngas_ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()


class GeothermalInventory(BaseInventoryImport):
    """
    Geothermal heat production, adapted from geothermal power production dataset from ecoinvent 3.6.
.
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # migration for ei 3.7
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_36_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")
        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()


class LPGInventory(BaseInventoryImport):
    """
    Liquified Petroleum Gas (LPG) from methanol distillation, the PSI project (2020), with hydrogen from electrolysis.
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        # migration for ei 3.7
        if self.version in ["3.7", "3.7.1"]:
            # apply some updates to comply with ei 3.7
            new_technosphere_data = EI_36_37_MIGRATION_MAP

            Migration("migration_37").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5/3.6 to 3.7",
            )
            self.import_db.migrate("migration_37")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("LPG_ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.5 to 3.6",
            )
            self.import_db.migrate("LPG_ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()


class VariousVehicles(BaseInventoryImport):
    """
    Imports various future vehicles' inventories (two-wheelers, buses, trams, etc.).
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):

        # Migrations for 3.6
        if self.version == "3.6":
            migrations = EI_37_36_MIGRATION_MAP

            Migration("ecoinvent_36").write(
                migrations,
                description="Change technosphere names due to change from 3.7 to 3.6",
            )
            self.import_db.migrate("ecoinvent_36")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.5 to 3.6",
            )
            self.import_db.migrate("ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()


class PassengerCars(BaseInventoryImport):
    """
    Imports default inventories for passenger cars.
    """

    def __init__(self, database, version, model, year, regions):
        super().__init__(database, version, Path("."))

        self.db = database
        self.regions = regions
        self.geomap = Geomap(model=model)

        inventory_year = min([2020, 2025, 2030, 2040, 2045, 2050],
                             key=lambda x: abs(x - year))
        ver = version.replace(".1", "")
        ver = ver.replace(".", "")
        filename = model \
                   + "_pass_cars_inventory_data_ei_" \
                   + ver + "_" + str(
            inventory_year) + ".pickle"
        fp = INVENTORY_DIR / filename

        self.import_db = LCIImporter("passenger_cars")

        with open(fp, 'rb') as handle:
            self.import_db.data = pickle.load(handle)

    def load_inventory(self, path):
        pass

    def prepare_inventory(self):

        # Migrations for 3.6
        if self.version == "3.6":
            migrations = EI_37_36_MIGRATION_MAP

            Migration("ecoinvent_36").write(
                migrations,
                description="Change technosphere names due to change from 3.7 to 3.6",
            )
            self.import_db.migrate("ecoinvent_36")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.5 to 3.6",
            )
            self.import_db.migrate("ecoinvent_35")

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
            "market for transport, passenger car, large size, petol, EURO 4",
            "market for transport, passenger car",
            "market for transport, passenger car, large size, petrol, EURO 3",
            "market for transport, passenger car, large size, diesel, EURO 4",
            "market for transport, passenger car, large size, diesel, EURO 5",
        ]
        for ds in self.db:
            excs = (
                exc
                for exc in ds["exchanges"]
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
                                self.geomap.ecoinvent_to_iam_location(ds["location"]),
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

        return self.db

class Trucks(BaseInventoryImport):
    """
    Imports default inventories for trucks.
    """

    def __init__(self, database, version, model, year, regions):
        super().__init__(database, version, Path("."))

        self.db = database
        self.regions = regions
        self.geomap = Geomap(model=model)

        inventory_year = min([2020, 2025, 2030, 2040, 2045, 2050],
                             key=lambda x: abs(x - year))
        ver = version.replace(".1", "")
        ver = ver.replace(".", "")
        filename = model \
                   + "_trucks_inventory_data_ei_" \
                   + ver + "_" + str(
            inventory_year) + ".pickle"
        fp = INVENTORY_DIR / filename

        self.import_db = LCIImporter("trucks")

        with open(fp, 'rb') as handle:
            self.import_db.data = pickle.load(handle)

    def load_inventory(self, path):
        pass

    def prepare_inventory(self):

        # Migrations for 3.6
        if self.version == "3.6":
            migrations = EI_37_36_MIGRATION_MAP

            Migration("ecoinvent_36").write(
                migrations,
                description="Change technosphere names due to change from 3.7 to 3.6",
            )
            self.import_db.migrate("ecoinvent_36")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("ecoinvent_35").write(
                migrations,
                description="Change technosphere names due to change from 3.5 to 3.6",
            )
            self.import_db.migrate("ecoinvent_35")

        self.add_biosphere_links()
        self.add_product_field_to_exchanges()
        # Check for duplicates
        self.check_for_duplicates()

        for x in self.import_db.data:
            x["code"] = str(uuid.uuid4().hex)

    def merge_inventory(self):
        self.prepare_inventory()

        activities_to_remove = [
            "transport, freight, lorry",
        ]

        self.db = [
            x
            for x in self.db
            if not any(y for y in activities_to_remove if y in x["name"])
        ]
        self.db.extend(self.import_db.data)

        for ds in self.db:
            excs = (
                exc
                for exc in ds["exchanges"]
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
                    search_for = "transport, freight, lorry, fleet average"

                if not any(
                        x
                        for x in ["3.5-7.5", "7.5-16", "16-32", ">32", "unspecified"]
                        if x in exc["name"]
                ):
                    search_for = "transport, freight, lorry, fleet average"

                try:
                    new_supplier = ws.get_one(
                        self.db,
                        *[
                            ws.equals("name", search_for),
                            ws.equals(
                                "location",
                                self.geomap.ecoinvent_to_iam_location(ds["location"]),
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

                    search_for = "transport, freight, lorry, fleet average"

                    try:
                        new_supplier = ws.get_one(
                            self.db,
                            *[
                                ws.equals("name", search_for),
                                ws.equals(
                                    "location",
                                    self.geomap.ecoinvent_to_iam_location(
                                        ds["location"]
                                    ),
                                ),
                                ws.contains(
                                    "reference product", "transport, freight, lorry"
                                ),
                            ],
                        )

                    except ws.NoResults:

                        search_for = "transport, freight, lorry, fleet average"

                        try:
                            new_supplier = ws.get_one(
                                self.db,
                                *[
                                    ws.equals("name", search_for),
                                    ws.contains(
                                        "reference product", "transport, freight, lorry"
                                    ),
                                ],
                            )

                        except ws.NoResults:
                            print(f"no results for {exc['name']} in {exc['location']}")

                            print("available trucks")
                            for dataset in self.db:
                                if "transport, freight, lorry" in dataset["name"]:
                                    print(dataset["name"], dataset["location"])

                        except ws.MultipleResults:
                            # If multiple trucks are available, but none of the correct region,
                            # we pick a a truck from the "World" region
                            print("found several suppliers")
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

        return self.db




class AdditionalInventory(BaseInventoryImport):
    """
    Import additional inventories, if any.
    """

    def __init__(self, database, version, path):
        super().__init__(database, version, path)
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        return ExcelImporter(path)

    def remove_missing_fields(self):

        for x in self.import_db.data:
            for k, v in list(x.items()):
                if not v:
                    del x[k]

    def prepare_inventory(self):
        # Initially links to ei37

        # Migrations for 3.6
        if self.version == "3.6":
            migrations = EI_37_36_MIGRATION_MAP

            Migration("ecoinvent_36").write(
                migrations,
                description="Change technosphere names due to change from 3.7 to 3.6",
            )
            self.import_db.migrate("ecoinvent_36")

        # Migrations for 3.5
        if self.version == "3.5":
            migrations = EI_37_35_MIGRATION_MAP

            Migration("migration_35").write(
                migrations,
                description="Change technosphere names due to change from 3.7 to 3.5",
            )
            self.import_db.migrate("migration_35")

        list_missing_prod = self.search_missing_exchanges(
            label="type", value="production"
        )

        if len(list_missing_prod) > 0:
            print("The following datasets are missing a `production` exchange.")
            print("You should fix those before proceeding further.\n")
            t = PrettyTable(["Name", "Reference product", "Location", "Unit", "File"])
            for ds in list_missing_prod:
                t.add_row(
                    [
                        ds.get("name", "XXXX"),
                        ds.get("referece product", "XXXX"),
                        ds.get("location", "XXXX"),
                        ds.get("unit", "XXXX"),
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
            for ds in list_missing_ref:
                t.add_row(
                    [
                        ds.get("name", "XXXX"),
                        ds.get("referece product", "XXXX"),
                        ds.get("location", "XXXX"),
                        ds.get("unit", "XXXX"),
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
        self.geomap = Geomap(model=self.model)
        self.regions = regions
        self.fleet_file = fleet_file
        self.data = iam_data
        self.filter = ["fleet average"]

        if filters:
            self.filter.extend(filters)

        super().__init__(database, version, Path("."))

    def get_liquid_fuel_blend(self, allocate_all_synfuels):

        if self.model == "remind":

            if allocate_all_synfuels:
                share_synfuel = self.data.sel(
                    variables=["SE|Liquids|Hydrogen"]
                ) / self.data.sel(variables="FE|Transport|Pass|Road|LDV|Liquids")

                share_liquids = self.data.sel(
                    variables=[
                        "FE|Transport|Liquids|Oil",
                        "FE|Transport|Liquids|Biomass",
                    ]
                ) / self.data.sel(
                    variables=[
                        "FE|Transport|Liquids|Oil",
                        "FE|Transport|Liquids|Biomass",
                    ]
                ).sum(
                    dim="variables"
                )
                share_liquids *= 1 - share_synfuel.values

                share_liquids = xr.concat(
                    [share_liquids, share_synfuel], dim="variables"
                )

                share_liquids = share_liquids.assign_coords(
                    {
                        "variables": [
                            "liquid - fossil",
                            "liquid - biomass",
                            "liquid - synfuel",
                        ]
                    }
                )

                share_liquids = np.clip(share_liquids.fillna(0), 0, 1)

            else:

                var = [
                    "FE|Transport|Liquids|Oil",
                    "FE|Transport|Liquids|Biomass",
                    "FE|Transport|Liquids|Hydrogen",
                ]

                share_liquids = self.data.sel(variables=var)
                share_liquids /= share_liquids.sum(dim="variables")

                share_liquids = share_liquids.assign_coords(
                    {
                        "variables": [
                            "liquid - fossil",
                            "liquid - biomass",
                            "liquid - synfuel",
                        ]
                    }
                )

                share_liquids = np.clip(share_liquids.fillna(0), 0, 1)

        if self.model == "image":
            var = [
                "Final Energy|Transportation|Freight|Liquids|Oil",
                "Final Energy|Transportation|Freight|Liquids|Biomass",
            ]

            share_liquids = self.data.sel(variables=var)
            share_liquids /= share_liquids.sum(dim="variables")

            share_liquids = share_liquids.assign_coords(
                {"variables": ["liquid - fossil", "liquid - biomass",]}
            )

            share_liquids = np.clip(share_liquids.fillna(0), 0, 1)

        return share_liquids

    def get_gas_fuel_blend(self):

        if self.model == "remind":

            var = ["FE|Transport|Gases|Non-Biomass", "FE|Transport|Gases|Biomass"]

            share_gas = self.data.sel(variables=var)
            share_gas /= share_gas.sum(dim="variables")

            share_gas = share_gas.assign_coords({"variables": ["gas - fossil", "gas - biomass"]})
            share_gas = np.clip(share_gas.fillna(0), 0, 1)

        if self.model == "image":

            share_gas = xr.DataArray(
                np.ones_like(
                self.data.sel(variables=["Final Energy|Transportation|Freight|Gases"])
                ),
                dims=["region", "variables", "year"],
                coords=[
                    self.data.region.values,
                    ["gas - fossil"],
                    self.data.year.values,
                ])



        return share_gas

    def load_inventory(self, path):
        """Create `carculator` fleet average inventories for a given range of years.
        """

        cip = carculator.CarInputParameters()
        cip.static()
        _, array = carculator.fill_xarray_from_input_parameters(cip)

        array = array.interp(
            year=np.arange(1996, self.db_year + 1), kwargs={"fill_value": "extrapolate"}
        )
        cm = carculator.CarModel(array, cycle="WLTC 3.4")
        cm.set_all()

        fleet_array = carculator.create_fleet_composition_from_IAM_file(self.fleet_file)

        liquid_fuel_blend = self.get_liquid_fuel_blend(allocate_all_synfuels=True)
        gas_fuel_blend = self.get_gas_fuel_blend()

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

            bc = {
                "country": region,
                "fuel blend": {
                    "petrol": {
                        "primary fuel": {
                            "type": "petrol",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - fossil", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values, 0, 1)
                            if "liquid - fossil" in liquid_fuel_blend.variables.values
                            else np.ones_like(years),
                        },
                        "secondary fuel": {
                            "type": "bioethanol - wheat straw",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - biomass", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - biomass" in liquid_fuel_blend.variables.values
                            else np.zeros_like(years), 0, 1)
                        },
                        "tertiary fuel": {
                            "type": "synthetic gasoline",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - synfuel", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - synfuel" in liquid_fuel_blend.variables.values
                            else np.zeros_like(years), 0, 1)
                        },
                    },
                    "diesel": {
                        "primary fuel": {
                            "type": "diesel",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - fossil", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - fossil" in liquid_fuel_blend.variables.values
                            else np.ones_like(years), 0, 1)
                        },
                        "secondary fuel": {
                            "type": "biodiesel - cooking oil",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - biomass", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - biomass" in liquid_fuel_blend.variables.values
                            else np.zeros_like(years),0, 1)
                        },
                        "tertiary fuel": {
                            "type": "synthetic diesel",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - synfuel", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - synfuel" in liquid_fuel_blend.variables.values
                            else np.zeros_like(years),0, 1)
                        },
                    },
                    "cng": {
                        "primary fuel": {
                            "type": "cng",
                            "share": np.clip(gas_fuel_blend.sel(
                                variables="gas - fossil", region=region
                                                        ).interp(year=scope["year"]
                                                                 , kwargs={"fill_value": "extrapolate"}).values
                            if "gas - fossil" in gas_fuel_blend.variables.values
                            else np.ones_like(years), 0, 1)
                        },
                        "secondary fuel": {
                            "type": "biogas - biowaste",
                            "share": np.clip(gas_fuel_blend.sel(
                                variables="gas - biomass", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "gas - biomass" in gas_fuel_blend.variables.values
                            else 1
                            - gas_fuel_blend.sel(variables="gas - fossil", region=region
                                                 ).interp(year=scope["year"],
                                                          kwargs={"fill_value": "extrapolate"}).values,0, 1)
                        },
                    },
                    "hydrogen": {
                        "primary fuel": {
                            "type": "electrolysis",
                            "share": np.ones_like(years),
                        }
                    },
                },
            }

            ic = carculator.InventoryCalculation(
                cm.array, scope=scope, background_configuration=bc
            )

            i = ic.export_lci_to_bw(
                presamples=False,
                ecoinvent_version=str(self.version),
                create_vehicle_datasets=False,
            )

            # filter out cars if anything given in `self.filter`
            i.data = [
                x
                for x in i.data
                if "transport, passenger car" not in x["name"]
                or (
                    any(y.lower() in x["name"].lower() for y in self.filter)
                    and str(self.db_year) in x["name"]
                )
            ]

            # we want to remove all fuel and electricity supply datatsets
            # to only keep the one corresponding to the fleet year
            i.data = [
                x
                for x in i.data
                if not any(
                    y in x["name"]
                    for y in [
                        "fuel supply for",
                        "electricity supply for",
                        "electricity market for fuel preparation",
                    ]
                )
                or str(self.db_year) in x["name"]
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
            for x in i.data:
                if any(
                    y in x["name"]
                    for y in [
                        "transport, passenger car, fleet average",
                        "fuel supply for ",
                        "electricity supply for ",
                        "electricity market for fuel preparation",
                    ]
                ):
                    x["name"] = x["name"][:-6]
                    for e in x["exchanges"]:
                        if e["type"] == "production":
                            e["name"] = x["name"]

                        if (
                            any(
                                f in e["name"]
                                for f in [
                                    "fuel supply for ",
                                    "electricity supply for ",
                                    "electricity market for fuel preparation",
                                ]
                            )
                            and e["type"] == "technosphere"
                        ):
                            e["name"] = e["name"][:-6]

            if r == 0:
                import_db = i
            else:
                # remove duplicate items if iterating over several regions
                i.data = [
                    x
                    for x in i.data
                    if (x["name"], x["location"])
                    not in [(z["name"], z["location"]) for z in import_db.data]
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

        self.db = [
            x
            for x in self.db
            if not any(y for y in activities_to_remove if y in x["name"])
        ]
        self.db.extend(self.import_db)

        exchanges_to_modify = [
            "market for transport, passenger car, large size, petol, EURO 4",
            "market for transport, passenger car",
            "market for transport, passenger car, large size, petrol, EURO 3",
            "market for transport, passenger car, large size, diesel, EURO 4",
            "market for transport, passenger car, large size, diesel, EURO 5",
        ]
        for ds in self.db:
            excs = (
                exc
                for exc in ds["exchanges"]
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
                                self.geomap.ecoinvent_to_iam_location(ds["location"]),
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

        return self.db


class TruckInventory(BaseInventoryImport):
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
        self.geomap = Geomap(model=self.model)
        self.regions = regions
        self.fleet_file = fleet_file
        self.filter = ["fleet average"]
        self.data = iam_data

        if filters:
            self.filter.extend(filters)

        super().__init__(database, version, Path("."))

    def get_liquid_fuel_blend(self, allocate_all_synfuels):

        if self.model == "remind":

            if allocate_all_synfuels:
                share_synfuel = self.data.sel(
                    variables=["SE|Liquids|Hydrogen"]
                ) / self.data.sel(variables="FE|Transport|Pass|Road|LDV|Liquids")

                share_liquids = self.data.sel(
                    variables=[
                        "FE|Transport|Liquids|Oil",
                        "FE|Transport|Liquids|Biomass",
                    ]
                ) / self.data.sel(
                    variables=[
                        "FE|Transport|Liquids|Oil",
                        "FE|Transport|Liquids|Biomass",
                    ]
                ).sum(
                    dim="variables"
                )
                share_liquids *= 1 - share_synfuel.values

                share_liquids = xr.concat(
                    [share_liquids, share_synfuel], dim="variables"
                )

                share_liquids = share_liquids.assign_coords(
                    {
                        "variables": [
                            "liquid - fossil",
                            "liquid - biomass",
                            "liquid - synfuel",
                        ]
                    }
                )

                share_liquids = np.clip(share_liquids.fillna(0), 0, 1)

            else:

                var = [
                    "FE|Transport|Liquids|Oil",
                    "FE|Transport|Liquids|Biomass",
                    "FE|Transport|Liquids|Hydrogen",
                ]

                share_liquids = self.data.sel(variables=var)
                share_liquids /= share_liquids.sum(dim="variables")

                share_liquids = share_liquids.assign_coords(
                    {
                        "variables": [
                            "liquid - fossil",
                            "liquid - biomass",
                            "liquid - synfuel",
                        ]
                    }
                )

                share_liquids = np.clip(share_liquids.fillna(0), 0, 1)

        if self.model == "image":
            var = [
                "Final Energy|Transportation|Freight|Liquids|Oil",
                "Final Energy|Transportation|Freight|Liquids|Biomass",
            ]

            share_liquids = self.data.sel(variables=var)
            share_liquids /= share_liquids.sum(dim="variables")

            share_liquids = share_liquids.assign_coords(
                {"variables": ["liquid - fossil", "liquid - biomass",]}
            )

            share_liquids = np.clip(share_liquids.fillna(0), 0, 1)

        return share_liquids

    def get_gas_fuel_blend(self):

        if self.model == "remind":

            var = ["FE|Transport|Gases|Non-Biomass", "FE|Transport|Gases|Biomass"]

            share_gas = self.data.sel(variables=var)
            share_gas /= share_gas.sum(dim="variables")

            share_gas = share_gas.assign_coords({"variables": ["gas - fossil", "gas - biomass"]})
            share_gas = np.clip(share_gas.fillna(0), 0, 1)

        if self.model == "image":

            share_gas = xr.DataArray(
                np.ones_like(
                self.data.sel(variables=["Final Energy|Transportation|Freight|Gases"])
                ),
                dims=["region", "variables", "year"],
                coords=[
                    self.data.region.values,
                    ["gas - fossil"],
                    self.data.year.values,
                ])



        return share_gas

    def load_inventory(self, path):
        """Create `carculator_truck` fleet average inventories for a given range of years.
        """

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

        liquid_fuel_blend = self.get_liquid_fuel_blend(allocate_all_synfuels=True)
        gas_fuel_blend = self.get_gas_fuel_blend()

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

            bc = {
                "country": region,
                "fuel blend": {
                    "petrol": {
                        "primary fuel": {
                            "type": "petrol",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - fossil", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values, 0, 1)
                            if "liquid - fossil" in liquid_fuel_blend.variables.values
                            else np.ones_like(years),
                        },
                        "secondary fuel": {
                            "type": "bioethanol - wheat straw",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - biomass", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - biomass" in liquid_fuel_blend.variables.values
                            else np.zeros_like(years), 0, 1)
                        },

                    },
                    "diesel": {
                        "primary fuel": {
                            "type": "diesel",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - fossil", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - fossil" in liquid_fuel_blend.variables.values
                            else np.ones_like(years), 0, 1)
                        },
                        "secondary fuel": {
                            "type": "biodiesel - cooking oil",
                            "share": np.clip(liquid_fuel_blend.sel(
                                variables="liquid - biomass", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "liquid - biomass" in liquid_fuel_blend.variables.values
                            else np.zeros_like(years),0, 1)
                        },

                    },
                    "cng": {
                        "primary fuel": {
                            "type": "cng",
                            "share": np.clip(gas_fuel_blend.sel(
                                variables="gas - fossil", region=region
                                                        ).interp(year=scope["year"]
                                                                 , kwargs={"fill_value": "extrapolate"}).values
                            if "gas - fossil" in gas_fuel_blend.variables.values
                            else np.ones_like(years), 0, 1)
                        },
                        "secondary fuel": {
                            "type": "biogas - biowaste",
                            "share": np.clip(gas_fuel_blend.sel(
                                variables="gas - biomass", region=region
                            ).interp(year=scope["year"], kwargs={"fill_value": "extrapolate"}).values
                            if "gas - biomass" in gas_fuel_blend.variables.values
                            else 1
                            - gas_fuel_blend.sel(variables="gas - fossil", region=region
                                                 ).interp(year=scope["year"],
                                                          kwargs={"fill_value": "extrapolate"}).values,0, 1)
                        },
                    },
                    "hydrogen": {
                        "primary fuel": {
                            "type": "electrolysis",
                            "share": np.ones_like(years),
                        }
                    },
                },
            }

            ic = carculator_truck.InventoryCalculation(
                tm, scope=scope, background_configuration=bc,
            )

            i = ic.export_lci_to_bw(
                presamples=False,
                ecoinvent_version=str(self.version),
                create_vehicle_datasets=False,
            )

            # filter out trucks if anything given in `self.filter`
            i.data = [
                x
                for x in i.data
                if "transport, " not in x["name"]
                or (
                    any(y.lower() in x["name"].lower() for y in self.filter)
                    and str(self.db_year) in x["name"]
                )
            ]

            # we want to remove all fuel and electricity supply datatsets
            # to only keep the one corresponding to the fleet year
            i.data = [
                x
                for x in i.data
                if not any(
                    y in x["name"]
                    for y in [
                        "fuel supply for",
                        "electricity supply for",
                        "electricity market for fuel preparation",
                    ]
                )
                or str(self.db_year) in x["name"]
            ]

            # we need to remove the electricity inputs in the fuel markets
            # that are typically added when synfuels are part of the blend
            for x in i.data:
                if "fuel supply for " in x["name"]:
                    for e in x["exchanges"]:
                        if "electricity market for " in e["name"]:
                            x["exchanges"].remove(e)

            # we want to rename the fuel supply and lorry transport dataset
            # by removing the year in the name
            for x in i.data:
                if any(
                    y in x["name"]
                    for y in [
                        "transport, freight, lorry, ",
                        "fuel supply for ",
                        "electricity supply for ",
                        "electricity market for fuel preparation",
                    ]
                ):
                    x["name"] = x["name"][:-6]
                    for e in x["exchanges"]:

                        if e["type"] == "production":
                            e["name"] = x["name"]

                        if (
                            any(
                                f in e["name"]
                                for f in [
                                    "fuel supply for ",
                                    "electricity supply for ",
                                    "electricity market for fuel preparation",
                                ]
                            )
                            and e["type"] == "technosphere"
                        ):
                            e["name"] = e["name"][:-6]

            if r == 0:
                import_db = i
            else:
                # remove duplicate items if iterating over several regions
                i.data = [
                    x
                    for x in i.data
                    if (x["name"], x["location"])
                    not in [(z["name"], z["location"]) for z in import_db.data]
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

        self.db = [
            x
            for x in self.db
            if not any(y for y in activities_to_remove if y in x["name"])
        ]
        self.db.extend(self.import_db)

        for ds in self.db:
            excs = (
                exc
                for exc in ds["exchanges"]
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
                    search_for = "transport, freight, lorry, fleet average"

                if not any(
                    x
                    for x in ["3.5-7.5", "7.5-16", "16-32", ">32", "unspecified"]
                    if x in exc["name"]
                ):
                    search_for = "transport, freight, lorry, fleet average"

                try:
                    new_supplier = ws.get_one(
                        self.db,
                        *[
                            ws.equals("name", search_for),
                            ws.equals(
                                "location",
                                self.geomap.ecoinvent_to_iam_location(ds["location"]),
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

                    search_for = "transport, freight, lorry, fleet average"

                    try:
                        new_supplier = ws.get_one(
                            self.db,
                            *[
                                ws.equals("name", search_for),
                                ws.equals(
                                    "location",
                                    self.geomap.ecoinvent_to_iam_location(
                                        ds["location"]
                                    ),
                                ),
                                ws.contains(
                                    "reference product", "transport, freight, lorry"
                                ),
                            ],
                        )

                    except ws.NoResults:

                        search_for = "transport, freight, lorry, fleet average"

                        try:
                            new_supplier = ws.get_one(
                                self.db,
                                *[
                                    ws.equals("name", search_for),
                                    ws.contains(
                                        "reference product", "transport, freight, lorry"
                                    ),
                                ],
                            )

                        except ws.NoResults:
                            print(f"no results for {exc['name']} in {exc['location']}")

                            print("available trucks")
                            for dataset in self.db:
                                if "transport, freight, lorry" in dataset["name"]:
                                    print(dataset["name"], dataset["location"])

                        except ws.MultipleResults:
                            # If multiple trucks are available, but none of the correct region,
                            # we pick a a truck from the "World" region
                            print("found several suppliers")
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

        return self.db
