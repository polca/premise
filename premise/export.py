"""
export.py contains all the functions to format, prepare and export databases.
"""

import csv
import datetime
import json
import os
import re
import uuid
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import sparse
import yaml
from scipy import sparse as nsp

from . import DATA_DIR, __version__
from .transformation import BaseTransformation

FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"
FILEPATH_SIMAPRO_UNITS = DATA_DIR / "utils" / "export" / "simapro_units.yml"
FILEPATH_SIMAPRO_COMPARTMENTS = (
    DATA_DIR / "utils" / "export" / "simapro_compartments.yml"
)
OUTDATED_FLOWS = DATA_DIR / "utils" / "export" / "outdated_flows.yaml"


def get_simapro_units() -> Dict[str, str]:
    """
    Load a dictionary that maps brightway2 unit to Simapro units.
    :return: a dictionary that maps brightway2 unit to Simapro units
    """

    with open(FILEPATH_SIMAPRO_UNITS, "r", encoding="utf-8") as stream:
        simapro_units = yaml.safe_load(stream)

    return simapro_units


def get_simapro_compartments() -> Dict[str, str]:
    """
    Load a dictionary that maps brightway2 unit to Simapro compartments.
    :return: a dictionary that maps brightway2 unit to Simapro compartments.
    """

    with open(FILEPATH_SIMAPRO_COMPARTMENTS, "r", encoding="utf-8") as stream:
        simapro_comps = yaml.safe_load(stream)

    return simapro_comps


def load_simapro_categories():
    """Load a dictionary with categories to use for Simapro export"""

    # Load the matching dictionary
    filename = "simapro_classification.csv"
    filepath = DATA_DIR / "utils" / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of Simapro categories could not be found."
        )
    with open(filepath, encoding="latin1") as f:
        csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
    _, *data = csv_list

    dict_cat = {}
    for row in data:
        _, cat_code, category_1, category_2, category_3 = row
        dict_cat[str(cat_code)] = {
            "category 1": category_1,
            "category 2": category_2,
            "category 3": category_3,
        }

    return dict_cat


def get_simapro_category_of_exchange():

    """Load a dictionary with categories to use for Simapro export based on ei 3.7"""

    # Load the matching dictionary
    filename = "simapro_categories.csv"
    filepath = DATA_DIR / "utils" / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of Simapro categories could not be found."
        )
    with open(filepath, encoding="utf-8") as file:
        csv_list = [[val.strip() for val in r.split(";")] for r in file.readlines()]
    _, *data = csv_list

    dict_cat = {}
    for row in data:
        name, category_1, category_2 = row
        dict_cat[name] = {
            "main category": category_1,
            "category": category_2,
        }

    return dict_cat


def load_references():
    """Load a dictionary with references of datasets"""

    # Load the matching dictionary
    filename = "references.csv"
    filepath = DATA_DIR / "utils" / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError("The dictionary of references could not be found.")
    with open(filepath, encoding="utf-8") as file:
        csv_list = [[val.strip() for val in r.split(";")] for r in file.readlines()]
    _, *data = csv_list

    dict_reference = {}
    for row in data:
        name, source, description = row
        dict_reference[name] = {"source": source, "description": description}

    return dict_reference


def get_simapro_biosphere_dictionnary():
    """
    Load a dictionary with biosphere flows to use for Simapro export.
    """

    # Load the matching dictionary between ecoinvent and Simapro biosphere flows
    filename = "simapro-biosphere.json"
    filepath = DATA_DIR / "utils" / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of biosphere flow match between ecoinvent "
            "and Simapro could not be found."
        )
    with open(filepath, encoding="utf-8") as json_file:
        data = json.load(json_file)
    dict_bio = {}
    for row in data:
        dict_bio[row[2]] = row[1]

    return dict_bio


def check_for_duplicates(database):
    """Check for the absence of duplicates before export"""

    db_names = [
        (x["name"].lower(), x["reference product"].lower(), x["location"])
        for x in database
    ]

    if len(db_names) == len(set(db_names)):
        return database

    print("One or multiple duplicates detected. Removing them...")

    seen = set()
    return [
        x
        for x in database
        if (x["name"].lower(), x["reference product"].lower(), x["location"])
        not in seen
        and not seen.add(
            (x["name"].lower(), x["reference product"].lower(), x["location"])
        )
    ]


def check_amount_format(database: list) -> list:
    """
    Check that the `amount` field is of type `float`.
    :param database: database to check
    :return: database with corrected amount field
    """

    for dataset in database:
        for exc in dataset["exchanges"]:
            if not isinstance(exc["amount"], float):
                exc["amount"] = float(exc["amount"])

    return database


def create_index_of_A_matrix(database):
    """
    Create a dictionary with row/column indices of the exchanges
    matrix as key and a tuple (activity name, reference product,
    unit, location) as value.
    :return: a dictionary to map indices to activities
    :rtype: dict
    """
    return {
        (
            database[i]["name"],
            database[i]["reference product"],
            database[i]["unit"],
            database[i]["location"],
        ): i
        for i in range(0, len(database))
    }


def rev_index(inds: dict) -> dict:
    """
    Reverse the index of the A matrix.
    """
    return {v: k for k, v in inds.items()}


def create_codes_index_of_exchanges_matrix(database):
    """
    Create a dictionary with row/column indices of the A matrix
    as key and the activity code as value.
    :return: a dictionary to map indices to activity codes
    :rtype: dict
    """

    return {database[i]["code"]: i for i in range(0, len(database))}


def create_codes_index_of_biosphere_flows_matrix():
    """
    Create a dictionary with row/column indices of the biosphere matrix
    """
    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(FILEPATH_BIOSPHERE_FLOWS, encoding="utf-8") as file:
        input_dict = csv.reader(file, delimiter=";")
        for i, row in enumerate(input_dict):
            csv_dict[row[-1]] = i

    return csv_dict


def create_index_of_biosphere_flows_matrix():
    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(FILEPATH_BIOSPHERE_FLOWS, encoding="utf-8") as file:
        input_dict = csv.reader(file, delimiter=";")
        for i, row in enumerate(input_dict):
            csv_dict[(row[0], row[1], row[2], row[3])] = i

    return csv_dict


def create_codes_and_names_of_tech_matrix(database: List[dict]):
    """
    Create a dictionary a tuple (activity name, reference product,
    unit, location) as key, and its code as value.
    :return: a dictionary to map indices to activities
    :rtype: dict
    """
    return {
        (
            i["name"],
            i["reference product"],
            i["unit"],
            i["location"],
        ): i["code"]
        for i in database
    }


def biosphere_flows_dictionary():
    """
    Create a dictionary with biosphere flows
    (name, category, sub-category, unit) -> code
    """
    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError("The dictionary of biosphere flows could not be found.")

    csv_dict = {}

    with open(FILEPATH_BIOSPHERE_FLOWS, encoding="utf-8") as file:
        input_dict = csv.reader(file, delimiter=";")
        for row in input_dict:
            csv_dict[(row[0], row[1], row[2], row[3])] = row[-1]

    return csv_dict


def get_list_unique_acts(scenarios):
    list_unique_acts = []
    for db in scenarios:
        for ds in db["database"]:
            list_unique_acts.extend(
                [
                    (a["name"], None, a.get("categories"), None, a["unit"])
                    if a["type"] == "biosphere"
                    else (
                        a["name"],
                        a.get("product"),
                        None,
                        a.get("location"),
                        a["unit"],
                    )
                    for a in ds["exchanges"]
                ]
            )
    return list(set(list_unique_acts))


def get_outdated_flows():

    with open(OUTDATED_FLOWS, "r", encoding="utf-8") as stream:
        outdated_flows = yaml.safe_load(stream)

    return outdated_flows


bio_dict = biosphere_flows_dictionary()
outdated_flows = get_outdated_flows()
exc_codes = {}


@lru_cache()
def fetch_exchange_code(name, ref, loc, unit):

    if (name, ref, loc, unit) not in exc_codes:
        code = str(uuid.uuid4().hex)
        exc_codes[(name, ref, loc, unit)] = code
    else:
        code = exc_codes[(name, ref, loc, unit)]

    return code


def get_act_dict_structure(ind, acts_ind, db_name) -> dict:
    name, ref, _, loc, unit = acts_ind[ind]
    code = fetch_exchange_code(name, ref, loc, unit)

    return {
        "name": name,
        "reference product": ref,
        "unit": unit,
        "location": loc,
        "database": db_name,
        "code": code,
        "exchanges": [],
    }


def get_exchange(ind, acts_ind, db_name, amount=1.0, production=False):
    name, ref, cat, loc, unit = acts_ind[ind]
    if cat:
        try:
            return {
                "name": name,
                "unit": unit,
                "categories": cat,
                "type": "biosphere",
                "amount": amount,
                "input": (
                    "biosphere3",
                    bio_dict[
                        name, cat[0], cat[1] if len(cat) > 1 else "unspecified", unit
                    ],
                ),
            }
        except KeyError:
            if name in outdated_flows:
                return {
                    "name": name,
                    "unit": unit,
                    "categories": cat,
                    "type": "biosphere",
                    "amount": amount,
                    "input": (
                        "biosphere3",
                        bio_dict[
                            outdated_flows[name],
                            cat[0],
                            cat[1] if len(cat) > 1 else "unspecified",
                            unit,
                        ],
                    ),
                }
    return {
        "name": name,
        "product": ref,
        "unit": unit,
        "location": loc,
        "type": "production" if production else "technosphere",
        "amount": amount,
        "input": (db_name, fetch_exchange_code(name, ref, loc, unit)),
    }


def build_superstructure_db(origin_db, scenarios, db_name, filepath) -> List[dict]:
    """
    Build a superstructure database from a list of databases
    :param origin_db: the original database
    :param scenarios: a list of modified databases
    :param db_name: the name of the new database
    :param filepath: the filepath of the new database
    :return: a superstructure database
    """

    print("Building superstructure database...")

    exc_codes.update(
        {
            (a["name"], a["reference product"], a["location"], a["unit"]): a["code"]
            for a in origin_db
        }
    )
    list_acts = get_list_unique_acts([{"database": origin_db}] + scenarios)
    acts_ind = dict(enumerate(list_acts))
    acts_ind_rev = {v: k for k, v in acts_ind.items()}
    list_scenarios = ["original"] + [
        f"{s['model']} - {s['pathway']} - {s['year']}" for s in scenarios
    ]
    list_dbs = [origin_db] + [a["database"] for a in scenarios]

    matrices = {
        a: nsp.lil_matrix((len(list_acts), len(list_acts)))
        for a, _ in enumerate(list_scenarios)
    }

    # store the metadata in a dictionary
    dict_meta = {
        (a["name"], a["reference product"], None, a["location"], a["unit"]): {
            b: c
            for b, c in a.items()
            if b
            not in [
                "exchanges",
                "code",
                "name",
                "reference product",
                "location",
                "unit",
                "database",
            ]
        }
        for db in list_dbs
        for a in db
    }

    for i, db in enumerate(list_dbs):
        for ds in db:
            for exc in ds["exchanges"]:
                if exc["type"] == "biosphere":
                    s = (exc["name"], None, exc.get("categories"), None, exc["unit"])
                else:
                    s = (
                        exc["name"],
                        exc["product"],
                        None,
                        exc["location"],
                        exc["unit"],
                    )
                c = (
                    ds["name"],
                    ds.get("reference product"),
                    ds.get("categories"),
                    ds.get("location"),
                    ds["unit"],
                )
                matrices[i][acts_ind_rev[s], acts_ind_rev[c]] += exc["amount"]

    m = sparse.stack([sparse.COO(x) for x in matrices.values()], axis=-1)
    inds = sparse.argwhere(m.sum(-1).T != 0)
    inds = list(map(tuple, inds))

    dataframe_rows = []

    inds_d = defaultdict(list)
    for ind in inds:
        inds_d[ind[0]].append(ind[1])

    new_db = []

    for k, v in inds_d.items():
        act = get_act_dict_structure(
            k,
            acts_ind,
            db_name,
        )
        act.update(dict_meta[acts_ind[k]])

        act["exchanges"].extend(
            get_exchange(i, acts_ind, db_name, amount=m[i, k, 0])
            if i != k
            else get_exchange(k, acts_ind, db_name, production=True)
            for i in v
        )

        new_db.append(act)

    inds_std = sparse.argwhere(m.std(-1).T > 1e-12)

    for i in inds_std:

        c_name, c_ref, c_cat, c_loc, c_unit = acts_ind[i[0]]
        s_name, s_ref, s_cat, s_loc, s_unit = acts_ind[i[1]]

        if s_cat:
            flow_type = "biosphere"
            database_name = "biosphere3"
            try:
                exc_key_supplier = (
                    database_name,
                    bio_dict[
                        s_name,
                        s_cat[0],
                        s_cat[1] if len(s_cat) > 1 else "unspecified",
                        s_unit,
                    ],
                )
            except KeyError:
                exc_key_supplier = (
                    database_name,
                    bio_dict[
                        outdated_flows[s_name],
                        s_cat[0],
                        s_cat[1] if len(s_cat) > 1 else "unspecified",
                        s_unit,
                    ],
                )

        elif i[0] == i[1]:
            flow_type = "production"
            database_name = db_name
            exc_key_supplier = (
                db_name,
                fetch_exchange_code(s_name, s_ref, s_loc, s_unit),
            )
        else:
            flow_type = "technosphere"
            database_name = db_name
            exc_key_supplier = (
                db_name,
                fetch_exchange_code(s_name, s_ref, s_loc, s_unit),
            )

        exc_key_consumer = (db_name, fetch_exchange_code(c_name, c_ref, c_loc, c_unit))

        row = [
            s_name,
            s_ref,
            s_loc,
            s_cat,
            database_name,
            exc_key_supplier,
            c_name,
            c_ref,
            c_loc,
            c_cat,
            db_name,
            exc_key_consumer,
            flow_type,
        ]

        if flow_type == "production":
            row.extend(np.ones_like(m[i[1], i[0], :]))
        else:
            row.extend(m[i[1], i[0], :])

        dataframe_rows.append(row)

    columns = [
        "from activity name",
        "from reference product",
        "from location",
        "from categories",
        "from database",
        "from key",
        "to activity name",
        "to reference product",
        "to location",
        "to categories",
        "to database",
        "to key",
        "flow type",
    ] + list_scenarios

    # create the dataframe
    df = pd.DataFrame(dataframe_rows, columns=columns)
    # remove the column `original`
    df = df.drop(columns=["original"])

    if filepath is not None:
        filepath = Path(filepath)
    else:
        filepath = DATA_DIR / "export" / "scenario diff files"

    if not os.path.exists(filepath):
        os.makedirs(filepath)

    filepath_sdf = filepath / f"scenario_diff_{db_name}.xlsx"

    # Drop duplicate rows
    # should not be any, but just in case
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"Dropped {before - after} duplicate(s).")

    try:
        df.to_excel(filepath_sdf, index=False)
    except ValueError:
        # from https://stackoverflow.com/questions/66356152/splitting-a-dataframe-into-multiple-sheets
        GROUP_LENGTH = 1000000  # set nr of rows to slice df
        with pd.ExcelWriter(filepath_sdf) as writer:
            for i in range(0, len(df), GROUP_LENGTH):
                df[i : i + GROUP_LENGTH].to_excel(
                    writer, sheet_name=f"Row {i}", index=False, header=True
                )

    print(f"Scenario difference file exported to {filepath}!")

    return new_db


def prepare_db_for_export(scenario, cache):

    base = BaseTransformation(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
        cache=cache,
    )

    # we ensure the absence of duplicate datasets
    print("- check for duplicates...")
    base.database = check_for_duplicates(base.database)
    # we check the format of numbers
    print("- check for values format...")
    base.database = check_amount_format(base.database)

    # we relink "dead" exchanges
    print("- relinking exchanges...")
    base.relink_datasets(
        excludes_datasets=["cobalt industry", "market group for electricity"],
        alt_names=[
            "market group for electricity, high voltage",
            "market group for electricity, medium voltage",
            "market group for electricity, low voltage",
            "carbon dioxide, captured from atmosphere, with heat pump heat, and grid electricity",
            "methane, from electrochemical methanation, with carbon from atmospheric CO2 capture, using heat pump heat",
            "Methane, synthetic, gaseous, 5 bar, from electrochemical methanation (H2 from electrolysis, CO2 from DAC using heat pump heat), at fuelling station, using heat pump heat",
        ],
    )

    print("Done!")

    return base.database, base.cache


class Export:
    """
    Class that exports the transformed data into matrices:
    * A matrix: contains products exchanges
    * B matrix: contains exchanges activities and the biosphere

    The A and B matrices are exported as csv files in a sparse representation (only non-zero values are listed), like so:
    - index row, index column, value of exchange

    Dictionaries to map row numbers to activities and products names are also exported.

    :ivar db: transformed database
    :vartype database: dict
    :ivar scenario: name of a Remind pathway
    :vartype pathway: str
    :ivar year: year of a Remind pathway
    :vartype year: int

    """

    def __init__(self, db, model=None, scenario=None, year=None, filepath=None):
        self.db = db
        self.model = model
        self.scenario = scenario
        self.year = year
        self.filepath = filepath
        self.bio_codes = self.rev_index(create_codes_index_of_biosphere_flows_matrix())

    def create_A_matrix_coordinates(self):
        index_A = create_index_of_A_matrix(self.db)
        list_rows = []

        try:
            for ds in self.db:
                for exc in ds["exchanges"]:
                    if exc["type"] == "production":
                        row = [
                            index_A[
                                (
                                    ds["name"],
                                    ds["reference product"],
                                    ds["unit"],
                                    ds["location"],
                                )
                            ],
                            index_A[
                                (
                                    exc["name"],
                                    exc["product"],
                                    exc["unit"],
                                    exc["location"],
                                )
                            ],
                            exc["amount"],
                        ]
                        list_rows.append(row)
                    if exc["type"] == "technosphere":
                        row = [
                            index_A[
                                (
                                    ds["name"],
                                    ds["reference product"],
                                    ds["unit"],
                                    ds["location"],
                                )
                            ],
                            index_A[
                                (
                                    exc["name"],
                                    exc["product"],
                                    exc["unit"],
                                    exc["location"],
                                )
                            ],
                            exc["amount"] * -1,
                        ]
                        list_rows.append(row)
        except KeyError:
            print(f"KeyError for {exc} in {ds['name']}")

        return list_rows

    def create_B_matrix_coordinates(self):

        index_B = create_index_of_biosphere_flows_matrix()
        rev_index_B = self.create_rev_index_of_B_matrix()
        index_A = create_index_of_A_matrix(self.db)
        list_rows = []

        for ds in self.db:
            for exc in ds["exchanges"]:
                if exc["type"] == "biosphere":
                    try:
                        code = exc["input"][1]
                        lookup = rev_index_B[code]
                        ind_B = index_B[lookup]

                        row = [
                            index_A[
                                (
                                    ds["name"],
                                    ds["reference product"],
                                    ds["unit"],
                                    ds["location"],
                                )
                            ],
                            ind_B,
                            exc["amount"] * -1,
                        ]
                    except KeyError:
                        print(
                            "Cannot find the biosphere flow",
                            exc["name"],
                            exc["categories"],
                        )
                        row = ()
                    list_rows.append(row)
        return list_rows

    def export_db_to_matrices(self):

        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)

        # Export A matrix
        with open(self.filepath / "A_matrix.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            writer.writerow(["index of activity", "index of product", "value"])
            rows = self.create_A_matrix_coordinates()
            for row in rows:
                writer.writerow(row)

        # Export A index
        with open(self.filepath / "A_matrix_index.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            index_A = create_index_of_A_matrix(self.db)
            for d in index_A:
                data = list(d) + [index_A[d]]
                writer.writerow(data)

        index_B = create_index_of_biosphere_flows_matrix()

        # Export B matrix
        with open(self.filepath / "B_matrix.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            writer.writerow(["index of activity", "index of biosphere flow", "value"])
            rows = self.create_B_matrix_coordinates()
            for row in rows:
                writer.writerow(row)

        # Export B index
        with open(self.filepath / "B_matrix_index.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(
                file,
                delimiter=";",
                lineterminator="\n",
            )
            for d in index_B:
                data = list(d) + [index_B[d]]
                writer.writerow(data)

        print("Matrices saved in {}.".format(self.filepath))

    @staticmethod
    def create_rev_index_of_B_matrix():
        if not FILEPATH_BIOSPHERE_FLOWS.is_file():
            raise FileNotFoundError(
                "The dictionary of biosphere flows could not be found."
            )

        csv_dict = {}

        with open(FILEPATH_BIOSPHERE_FLOWS, encoding="utf-8") as f:
            input_dict = csv.reader(f, delimiter=";")
            for row in input_dict:
                csv_dict[row[-1]] = (row[0], row[1], row[2], row[3])

        return csv_dict

    def get_category_of_exchange(self):
        """
        This function returns a dictionnary with (name, reference product) as keys,
        and {'main category': 'xxxxxx', 'category': 'yyyyyy'} as values.
        This is useful for Simapro export, to categorize datasets into a tree structure.
        :return: dict
        """

        dict_classifications = load_simapro_categories()

        dict_categories = {}

        for ds in self.db:
            if (ds["name"], ds["reference product"]) not in dict_categories:
                main_category, category = (None, None)

                if "classifications" in ds:
                    if len(ds["classifications"]) > 0:
                        for x in ds["classifications"]:
                            if x[0] == "ISIC rev.4 ecoinvent":

                                try:
                                    key = str(int(x[1].split(":")[0].strip()))
                                    main_category = dict_classifications[key][
                                        "category 1"
                                    ]

                                except KeyError:
                                    try:
                                        key = x[1].split(":")[0].strip()
                                        main_category = dict_classifications[key][
                                            "category 1"
                                        ]

                                    except KeyError:
                                        print(
                                            "missing class", x[1].split(":")[0].strip()
                                        )
                                        continue

                                if dict_classifications[key]["category 3"] != "":
                                    category = (
                                        dict_classifications[key]["category 2"]
                                        + "\ ".strip()
                                        + dict_classifications[key]["category 3"]
                                    )
                                else:
                                    category = dict_classifications[key]["category 2"]

                        if not main_category:
                            for x in ds["classifications"]:
                                if x[0] == "CPC":
                                    main_category = dict_classifications[
                                        x[1].split(":")[0].strip()
                                    ]["category 1"]

                                    if (
                                        dict_classifications[
                                            x[1].split(":")[0].strip()
                                        ]["category 3"]
                                        != ""
                                    ):
                                        category = (
                                            dict_classifications[
                                                x[1].split(":")[0].strip()
                                            ]["category 2"]
                                            + "\ ".strip()
                                            + dict_classifications[
                                                x[1].split(":")[0].strip()
                                            ]["category 3"]
                                        )
                                    else:
                                        category = dict_classifications[
                                            x[1].split(":")[0].strip()
                                        ]["category 2"]

                if not main_category or main_category == "":
                    main_category = "material"
                    category = "Others"

                dict_categories[(ds["name"], ds["reference product"])] = {
                    "main category": main_category,
                    "category": category,
                }

        return dict_categories

    def export_db_to_simapro(self):

        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)

        dict_bio = get_simapro_biosphere_dictionnary()

        headers = [
            "{SimaPro 9.1.1.7}",
            "{processes}",
            "{Project: premise import" + f"{datetime.datetime.today():%d.%m.%Y}" + "}",
            "{CSV Format version: 9.0.0}",
            "{CSV separator: Semicolon}",
            "{Decimal separator: .}",
            "{Date separator: .}",
            "{Short date format: dd.MM.yyyy}",
            "{Export platform IDs: No}",
            "{Skip empty fields: No}",
            "{Convert expressions to constants: No}",
            "{Related objects(system descriptions, substances, units, etc.): Yes}",
            "{Include sub product stages and processes: Yes}",
        ]

        fields = [
            "Process",
            "Category type",
            "Type",
            "Process name",
            "Time Period",
            "Geography",
            "Technology",
            "Representativeness",
            "Waste treatment allocation",
            "Cut off rules",
            "Capital goods",
            "Date",
            "Boundary with nature",
            "Infrastructure",
            "Record",
            "Generator",
            "Literature references",
            "External documents",
            "Comment",
            "Collection method",
            "Data treatment",
            "Verification",
            "System description",
            "Allocation rules",
            "Products",
            "Waste treatment",
            "Materials/fuels",
            "Resources",
            "Emissions to air",
            "Emissions to water",
            "Emissions to soil",
            "Final waste flows",
            "Non material emission",
            "Social issues",
            "Economic issues",
            "Waste to treatment",
            "End",
        ]

        # mapping between BW2 and Simapro units
        simapro_units = get_simapro_units()

        # mapping between BW2 and Simapro sub-compartments
        simapro_subs = get_simapro_compartments()

        filename = f"simapro_export_{self.model}_{self.scenario}_{self.year}.csv"

        dict_cat_simapro = get_simapro_category_of_exchange()
        dict_cat = self.get_category_of_exchange()
        dict_refs = load_references()

        with open(
            Path(self.filepath) / filename, "w", newline="", encoding="latin1"
        ) as csvFile:
            writer = csv.writer(csvFile, delimiter=";")
            for item in headers:
                writer.writerow([item])
            writer.writerow([])

            for ds in self.db:

                main_category, category = ("", "")

                if ds["name"] in dict_cat_simapro:

                    main_category, category = (
                        dict_cat_simapro[ds["name"]]["main category"],
                        dict_cat_simapro[ds["name"]]["category"],
                    )
                else:

                    if any(
                        i in ds["name"]
                        for i in (
                            "transport, passenger car",
                            "transport, heavy",
                            "transport, medium",
                        )
                    ):
                        main_category, category = ("transport", r"Road\Transformation")

                    if any(
                        i in ds["name"]
                        for i in ("Passenger car", "Heavy duty", "Medium duty")
                    ):
                        main_category, category = ("transport", r"Road\Infrastructure")

                    if main_category == "":

                        main_category, category = (
                            dict_cat[(ds["name"], ds["reference product"])][
                                "main category"
                            ],
                            dict_cat[(ds["name"], ds["reference product"])]["category"],
                        )

                for item in fields:

                    if (
                        main_category.lower() == "waste treatment"
                        and item == "Products"
                    ):
                        continue

                    if main_category.lower() != "waste treatment" and item in (
                        "Waste treatment",
                        "Waste treatment allocation",
                    ):
                        continue

                    writer.writerow([item])

                    if item == "Process name":

                        name = f"{ds['reference product']} {{{ds.get('location', 'GLO')}}}| {ds['name']} | Cut-off, U"

                        writer.writerow([name])

                    if item == "Type":
                        writer.writerow(["Unit process"])

                    if item == "Category type":
                        writer.writerow([main_category])

                    if item == "Generator":
                        writer.writerow(["premise " + str(__version__)])

                    if item == "Geography":
                        writer.writerow([ds["location"]])

                    if item == "Date":
                        writer.writerow([f"{datetime.datetime.today():%d.%m.%Y}"])

                    if item == "Comment":

                        if ds["name"] in dict_refs:
                            string = re.sub(
                                "[^a-zA-Z0-9 .,]", "", dict_refs[ds["name"]]["source"]
                            )

                            if dict_refs[ds["name"]]["description"] != "":
                                string += " " + re.sub(
                                    "[^a-zA-Z0-9 .,]",
                                    "",
                                    dict_refs[ds["name"]]["description"],
                                )

                            writer.writerow([string])
                        else:
                            if "comment" in ds:
                                string = re.sub("[^a-zA-Z0-9 .,]", "", ds["comment"])
                                writer.writerow([string])

                    if item in (
                        "Cut off rules",
                        "Capital goods",
                        "Technology",
                        "Representativeness",
                        "Waste treatment allocation",
                        "Boundary with nature",
                        "Allocation rules",
                        "Collection method",
                        "Verification",
                        "Time Period",
                        "Record",
                    ):
                        writer.writerow(["Unspecified"])
                    if item == "Literature references":
                        writer.writerow(["Ecoinvent 3"])
                    if item == "System description":
                        writer.writerow(["Ecoinvent v3"])
                    if item == "Infrastructure":
                        writer.writerow(["Yes"])
                    if item == "External documents":
                        writer.writerow(
                            [
                                "https://premise.readthedocs.io/en/latest/introduction.html"
                            ]
                        )
                    if item in ("Waste treatment", "Products"):
                        for e in ds["exchanges"]:
                            if e["type"] == "production":
                                name = (
                                    e["product"]
                                    + " {"
                                    + e.get("location", "GLO")
                                    + "}"
                                    + "| "
                                    + e["name"]
                                    + " "
                                    + "| Cut-off, U"
                                )

                                if item == "Waste treatment":
                                    writer.writerow(
                                        [
                                            name,
                                            simapro_units[e["unit"]],
                                            1.0,
                                            "not defined",
                                            category,
                                        ]
                                    )

                                else:
                                    writer.writerow(
                                        [
                                            name,
                                            simapro_units[e["unit"]],
                                            1.0,
                                            "100%",
                                            "not defined",
                                            category,
                                        ]
                                    )
                    if item == "Materials/fuels":
                        for e in ds["exchanges"]:
                            if e["type"] == "technosphere":

                                if e["name"] in dict_cat_simapro:
                                    exc_cat = dict_cat_simapro[e["name"]][
                                        "main category"
                                    ].lower()
                                else:
                                    exc_cat = dict_cat[e["name"], e["product"]][
                                        "main category"
                                    ].lower()

                                if exc_cat != "waste treatment":
                                    name = (
                                        e["product"]
                                        + " {"
                                        + e.get("location", "GLO")
                                        + "}"
                                        + "| "
                                        + e["name"]
                                        + " "
                                        + "| Cut-off, U"
                                    )

                                    writer.writerow(
                                        [
                                            name,
                                            simapro_units[e["unit"]],
                                            "{:.3E}".format(e["amount"]),
                                            "undefined",
                                            0,
                                            0,
                                            0,
                                        ]
                                    )
                    if item == "Resources":
                        for e in ds["exchanges"]:
                            if (
                                e["type"] == "biosphere"
                                and e["categories"][0] == "natural resource"
                            ):
                                writer.writerow(
                                    [
                                        dict_bio.get(e["name"], e["name"]),
                                        "",
                                        simapro_units[e["unit"]],
                                        "{:.3E}".format(e["amount"]),
                                        "undefined",
                                        0,
                                        0,
                                        0,
                                    ]
                                )
                    if item == "Emissions to air":
                        for e in ds["exchanges"]:
                            if e["type"] == "biosphere" and e["categories"][0] == "air":

                                if len(e["categories"]) > 1:
                                    sub_compartment = simapro_subs.get(
                                        e["categories"][1], ""
                                    )
                                else:
                                    sub_compartment = ""

                                if e["name"].lower() == "water":
                                    e["unit"] = "kilogram"
                                    e["amount"] /= 1000

                                writer.writerow(
                                    [
                                        dict_bio.get(e["name"], e["name"]),
                                        sub_compartment,
                                        simapro_units[e["unit"]],
                                        "{:.3E}".format(e["amount"]),
                                        "undefined",
                                        0,
                                        0,
                                        0,
                                    ]
                                )
                    if item == "Emissions to water":
                        for e in ds["exchanges"]:
                            if (
                                e["type"] == "biosphere"
                                and e["categories"][0] == "water"
                            ):
                                if len(e["categories"]) > 1:
                                    sub_compartment = simapro_subs.get(
                                        e["categories"][1], ""
                                    )
                                else:
                                    sub_compartment = ""

                                if e["name"].lower() == "water":
                                    e["unit"] = "kilogram"
                                    e["amount"] /= 1000

                                writer.writerow(
                                    [
                                        dict_bio.get(e["name"], e["name"]),
                                        sub_compartment,
                                        simapro_units[e["unit"]],
                                        "{:.3E}".format(e["amount"]),
                                        "undefined",
                                        0,
                                        0,
                                        0,
                                    ]
                                )
                    if item == "Emissions to soil":
                        for e in ds["exchanges"]:
                            if (
                                e["type"] == "biosphere"
                                and e["categories"][0] == "soil"
                            ):
                                if len(e["categories"]) > 1:
                                    sub_compartment = simapro_subs.get(
                                        e["categories"][1], ""
                                    )
                                else:
                                    sub_compartment = ""

                                writer.writerow(
                                    [
                                        dict_bio.get(e["name"], e["name"]),
                                        sub_compartment,
                                        simapro_units[e["unit"]],
                                        "{:.3E}".format(e["amount"]),
                                        "undefined",
                                        0,
                                        0,
                                        0,
                                    ]
                                )
                    if item == "Waste to treatment":
                        for e in ds["exchanges"]:
                            if e["type"] == "technosphere":

                                if e["name"] in dict_cat_simapro:
                                    exc_cat = dict_cat_simapro[e["name"]][
                                        "main category"
                                    ].lower()
                                else:
                                    exc_cat = dict_cat[e["name"], e["product"]][
                                        "main category"
                                    ].lower()

                                if exc_cat == "waste treatment":

                                    name = (
                                        e["product"]
                                        + " {"
                                        + e.get("location", "GLO")
                                        + "}"
                                        + "| "
                                        + e["name"]
                                        + " "
                                        + "| Cut-off, U"
                                    )

                                    writer.writerow(
                                        [
                                            name,
                                            simapro_units[e["unit"]],
                                            "{:.3E}".format(e["amount"] * -1),
                                            "undefined",
                                            0,
                                            0,
                                            0,
                                        ]
                                    )

                    writer.writerow([])

            # System description
            writer.writerow(["System description"])
            writer.writerow([])
            writer.writerow(["Name"])
            writer.writerow(["Ecoinvent v3"])
            writer.writerow([])
            writer.writerow(["Category"])
            writer.writerow(["Others"])
            writer.writerow([])
            writer.writerow(["Description"])
            writer.writerow([""])
            writer.writerow([])
            writer.writerow(["Cut-off rules"])
            writer.writerow([""])
            writer.writerow([])
            writer.writerow(["Energy model"])
            writer.writerow([])
            writer.writerow([])
            writer.writerow(["Transport model"])
            writer.writerow([])
            writer.writerow([])
            writer.writerow(["Allocation rules"])
            writer.writerow([])
            writer.writerow(["End"])
            writer.writerow([])

            # Literature reference
            writer.writerow(["Literature reference"])
            writer.writerow([])
            writer.writerow(["Name"])
            writer.writerow(["Ecoinvent"])
            writer.writerow([])
            writer.writerow(["Documentation link"])
            writer.writerow(["https://www.ecoinvent.org"])
            writer.writerow([])
            writer.writerow(["Comment"])
            writer.writerow(
                ["Pre-print available at: https://www.psi.ch/en/media/57994/download"]
            )
            writer.writerow([])
            writer.writerow(["Category"])
            writer.writerow(["Ecoinvent 3"])
            writer.writerow([])
            writer.writerow(["Description"])
            description = "modified by premise"

            writer.writerow([description])

        csvFile.close()

        print("Simapro CSV files saved in {}.".format(self.filepath))

    def create_names_and_indices_of_A_matrix(self):
        """
        Create a dictionary a tuple (activity name, reference product,
        database, location, unit) as key, and its indix in the
        matrix A as value.
        :return: a dictionary to map indices to activities
        :rtype: dict
        """
        return {
            (
                i["name"],
                i["reference product"],
                "ecoinvent",
                i["location"],
                i["unit"],
            ): x
            for x, i in enumerate(self.db)
        }

    def create_names_and_indices_of_B_matrix(self):
        if not FILEPATH_BIOSPHERE_FLOWS.is_file():
            raise FileNotFoundError(
                "The dictionary of biosphere flows could not be found."
            )

        csv_dict = {}

        with open(FILEPATH_BIOSPHERE_FLOWS, encoding="utf-8") as file:
            input_dict = csv.reader(file, delimiter=";")
            for i, row in enumerate(input_dict):
                if row[2] != "unspecified":
                    csv_dict[(row[0], (row[1], row[2]), "biosphere3", row[3])] = i
                else:
                    csv_dict[(row[0], (row[1],), "biosphere3", row[3])] = i

        return csv_dict

    def rev_index(self, inds):
        return {v: k for k, v in inds.items()}

    def get_bio_code(self, idx):

        return self.bio_codes[idx]
