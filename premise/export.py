"""
export.py contains all the functions to format, prepare and export databases.
"""

import csv
import datetime
import json
import os
import re
import uuid
from itertools import chain
from pathlib import Path
from typing import Dict, List

import pandas as pd
import yaml
from wurst import searching as ws

from . import DATA_DIR, __version__
from .transformation import BaseTransformation

FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "utils" / "export" / "flows_biosphere_38.csv"
FILEPATH_SIMAPRO_UNITS = DATA_DIR / "utils" / "export" / "simapro_units.yml"
FILEPATH_SIMAPRO_COMPARTMENTS = (
    DATA_DIR / "utils" / "export" / "simapro_compartments.yml"
)


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


def remove_uncertainty(database):
    """
    Remove uncertainty information from database exchanges.
    :param database:
    :return:
    """

    keys_to_remove = [
        "loc",
        "scale",
        "pedigree",
        "minimum",
        "maximum",
    ]

    for dataset in database:
        for exc in dataset["exchanges"]:
            if "uncertainty type" in exc:
                if exc["uncertainty type"] != 0:
                    exc["uncertainty type"] = 0
                    for key in keys_to_remove:
                        if key in exc:
                            del exc[key]
    return database


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


def add_modified_tags(original_db, scenarios):
    """
    Add a `modified` label to any activity that is new
    Also add a `modified` label to any exchange that has been added
    or that has a different value than the source database.
    :return:
    """

    # Class `Export` to which the original database is passed
    exp = Export(original_db)
    # Collect a dictionary of activities {row/col index in A matrix: code}
    rev_ind_A = rev_index(create_codes_index_of_exchanges_matrix(original_db))
    # Retrieve list of coordinates [activity, activity, value]
    coords_A = exp.create_A_matrix_coordinates()
    # Turn it into a dictionary {(code of receiving activity, code of supplying activity): value}
    original = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}
    # Collect a dictionary with activities' names and corresponding codes
    codes_names = create_codes_and_names_of_tech_matrix(original_db)
    # Collect list of substances
    rev_ind_B = rev_index(create_codes_index_of_biosphere_flows_matrix())
    # Retrieve list of coordinates of the B matrix [activity index, substance index, value]
    coords_B = exp.create_B_matrix_coordinates()
    # Turn it into a dictionary {(activity code, substance code): value}
    original.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B})

    for s, scenario in enumerate(scenarios):
        print(f"Looking for differences in database {s + 1} ...")
        rev_ind_A = rev_index(
            create_codes_index_of_exchanges_matrix(scenario["database"])
        )
        exp = Export(
            scenario["database"],
            scenario["model"],
            scenario["pathway"],
            scenario["year"],
            "",
        )
        coords_A = exp.create_A_matrix_coordinates()
        new = {(rev_ind_A[x[0]], rev_ind_A[x[1]]): x[2] for x in coords_A}

        rev_ind_B = rev_index(create_codes_index_of_biosphere_flows_matrix())
        coords_B = exp.create_B_matrix_coordinates()
        new.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] for x in coords_B})

        list_new = set(i[0] for i in original.keys()) ^ set(i[0] for i in new.keys())

        datasets = (d for d in scenario["database"] if d["code"] in list_new)

        # Tag new activities
        for dataset in datasets:
            dataset["modified"] = True

        # List codes that belong to activities that contain modified exchanges
        list_modified = (i[0] for i in new if i in original and new[i] != original[i])
        #
        # Filter for activities that have modified exchanges
        for datasets in ws.get_many(
            scenario["database"],
            ws.either(*[ws.equals("code", c) for c in set(list_modified)]),
        ):
            # Loop through biosphere exchanges and check if
            # the exchange also exists in the original database
            # and if it has the same value
            # if any of these two conditions is False, we tag the exchange
            excs = (exc for exc in datasets["exchanges"] if exc["type"] == "biosphere")
            for exc in excs:
                if (datasets["code"], exc["input"][0]) not in original or new[
                    (datasets["code"], exc["input"][0])
                ] != original[(datasets["code"], exc["input"][0])]:
                    exc["modified"] = True
            # Same thing for technosphere exchanges,
            # except that we first need to look up the provider's code first
            excs = (
                exc for exc in datasets["exchanges"] if exc["type"] == "technosphere"
            )
            for exc in excs:
                if (
                    exc["name"],
                    exc["product"],
                    exc["unit"],
                    exc["location"],
                ) in codes_names:
                    exc_code = codes_names[
                        (exc["name"], exc["product"], exc["unit"], exc["location"])
                    ]
                    if (
                        new[(datasets["code"], exc_code)]
                        != original[(datasets["code"], exc_code)]
                    ):
                        exc["modified"] = True
                else:
                    exc["modified"] = True

    return scenarios


def build_superstructure_db(origin_db, scenarios, db_name, filepath):
    # Class `Export` to which the original database is passed
    exp = Export(db=origin_db, filepath=filepath)

    # Collect a dictionary of activities
    # {(name, ref_prod, loc, database, unit):row/col index in A matrix}
    rev_ind_A = exp.rev_index(exp.create_names_and_indices_of_A_matrix())

    # Retrieve list of coordinates [activity, activity, value]
    coords_A = exp.create_A_matrix_coordinates()

    # Turn it into a dictionary {(code of receiving activity, code of supplying activity): value}
    original = dict()
    for x in coords_A:
        if (rev_ind_A[x[0]], rev_ind_A[x[1]]) in original:
            original[(rev_ind_A[x[0]], rev_ind_A[x[1]])] += x[2] * -1
        else:
            original[(rev_ind_A[x[0]], rev_ind_A[x[1]])] = x[2] * -1

    # Collect list of substances
    rev_ind_B = exp.rev_index(exp.create_names_and_indices_of_B_matrix())
    # Retrieve list of coordinates of the B matrix [activity index, substance index, value]
    coords_B = exp.create_B_matrix_coordinates()

    # Turn it into a dictionary {(activity name, ref prod, location, database, unit): value}
    original.update({(rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] * -1 for x in coords_B})

    modified = {}

    print("Looping through scenarios to detect changes...")

    for scenario in scenarios:

        exp = Export(
            db=scenario["database"],
            model=scenario["model"],
            scenario=scenario["pathway"],
            year=scenario["year"],
            filepath=filepath,
        )

        new_rev_ind_A = exp.rev_index(exp.create_names_and_indices_of_A_matrix())
        new_coords_A = exp.create_A_matrix_coordinates()

        new = dict()
        for x in new_coords_A:
            if (new_rev_ind_A[x[0]], new_rev_ind_A[x[1]]) in new:
                new[(new_rev_ind_A[x[0]], new_rev_ind_A[x[1]])] += x[2] * -1
            else:
                new[(new_rev_ind_A[x[0]], new_rev_ind_A[x[1]])] = x[2] * -1

        new_coords_B = exp.create_B_matrix_coordinates()
        new.update(
            {(new_rev_ind_A[x[0]], rev_ind_B[x[1]]): x[2] * -1 for x in new_coords_B}
        )
        # List activities that are in the new database
        # but not in the original one
        # As well as exchanges that are present
        # in both databases but with a different value
        list_modified = (i for i in new if i not in original or new[i] != original[i])
        # Also add activities from the original database that are not present in
        # the new one
        list_new = (i for i in original if i not in new)

        list_modified = chain(list_modified, list_new)

        for i in list_modified:
            if i not in modified:
                modified[i] = {"original": original.get(i, 0)}

            modified[i][
                f"{scenario['model']} - {scenario['pathway']} - {scenario['year']}"
            ] = new.get(i, 0)

    # some scenarios may have not been modified
    # and that means that exchanges might be absent
    # from `modified`
    # so we need to manually add them
    # and set the exchange value similar to that
    # of the original database

    list_scenarios = ["original"] + [
        f"{s['model']} - {s['pathway']} - {s['year']}" for s in scenarios
    ]

    for m in modified:
        for s in list_scenarios:
            if s not in modified[m].keys():
                # if it is a production exchange
                # the value should be -1
                if m[1] == m[0]:
                    modified[m][s] = -1
                else:
                    modified[m][s] = modified[m]["original"]

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
    ]
    columns.extend(list_scenarios)

    print("Export a scenario difference file.")

    l_modified = [columns]

    for m in modified:

        if m[1][2] == "biosphere3":
            d = [
                m[1][0],
                "",
                "",
                m[1][1],
                m[1][2],
                "",  # biosphere flow code
                m[0][0],
                m[0][1],
                m[0][3],
                "",
                db_name,
                "",  # activity code
                "biosphere",
            ]
        elif m[1] == m[0] and any(v < 0 for v in modified[m].values()):
            d = [
                m[1][0],
                m[1][1],
                m[1][3],
                "",
                db_name,
                "",  # activity code
                m[0][0],
                m[0][1],
                m[0][3],
                "",
                db_name,
                "",  # activity code
                "production",
            ]
        else:
            d = [
                m[1][0],
                m[1][1],
                m[1][3],
                "",
                db_name,
                "",  # activity code
                m[0][0],
                m[0][1],
                m[0][3],
                "",
                db_name,
                "",  # activity code
                "technosphere",
            ]

        for s in list_scenarios:
            # we do not want a zero here,
            # as it would render the matrix undetermined
            if m[1] == m[0] and modified[m][s] == 0:
                d.append(1)
            elif m[1] == m[0] and modified[m][s] < 0:
                d.append(modified[m][s] * -1)
            else:
                d.append(modified[m][s])
        l_modified.append(d)

    if filepath is not None:
        filepath = Path(filepath)
    else:
        filepath = DATA_DIR / "export" / "scenario diff files"

    if not os.path.exists(filepath):
        os.makedirs(filepath)

    filepath = filepath / f"scenario_diff_{db_name}.xlsx"

    df = pd.DataFrame(l_modified[1:], columns=l_modified[0])

    before = len(df)

    # Drop duplicate rows
    df = df.drop_duplicates()
    # Remove rows whose values across scenarios do not change
    df = df.loc[df.loc[:, "original":].std(axis=1) > 0, :]
    # Remove `original` column
    df = df.iloc[:, [j for j, c in enumerate(df.columns) if j != 13]]

    after = len(df)
    print(f"Dropped {before - after} duplicates.")

    df.to_excel(filepath, index=False)

    print(f"Scenario difference file exported to {filepath}!")

    list_modified_acts = list(
        set([e[0] for e, v in modified.items() if v["original"] == 0])
    )

    acts_to_extend = [
        act
        for act in origin_db
        if (
            act["name"],
            act["reference product"],
            "ecoinvent",
            act["location"],
            act["unit"],
        )
        in list_modified_acts
    ]

    print(f"Adding exchanges to {len(acts_to_extend)} activities.")

    dict_bio = exp.create_names_and_indices_of_B_matrix()

    for ds in acts_to_extend:
        exc_to_add = []
        for exc in [
            e
            for e in modified
            if e[0]
            == (
                ds["name"],
                ds["reference product"],
                "ecoinvent",
                ds["location"],
                ds["unit"],
            )
            and modified[e]["original"] == 0
        ]:
            # a biosphere flow
            if isinstance(exc[1][1], tuple):
                exc_to_add.append(
                    {
                        "amount": 0,
                        "input": (
                            "biosphere3",
                            exp.get_bio_code(
                                dict_bio[(exc[1][0], exc[1][1], exc[1][2], exc[1][3])]
                            ),
                        ),
                        "type": "biosphere",
                        "name": exc[1][0],
                        "unit": exc[1][3],
                        "categories": exc[1][1],
                    }
                )

            # a technosphere flow
            else:
                exc_to_add.append(
                    {
                        "amount": 0,
                        "type": "technosphere",
                        "product": exc[1][1],
                        "name": exc[1][0],
                        "unit": exc[1][4],
                        "location": exc[1][3],
                    }
                )

        if len(exc_to_add) > 0:
            ds["exchanges"].extend(exc_to_add)

    list_act = [
        (a["name"], a["reference product"], a["database"], a["location"], a["unit"])
        for a in origin_db
    ]
    list_to_add = [
        m[0] for m, v in modified.items() if v["original"] == 0 and m[0] not in list_act
    ]
    list_to_add = list(set(list_to_add))

    print(f"Adding {len(list_to_add)} extra activities to the original database...")

    acts_to_add = []
    for add in list_to_add:
        act_to_add = {
            "location": add[3],
            "name": add[0],
            "reference product": add[1],
            "unit": add[4],
            "database": add[2],
            "code": str(uuid.uuid4().hex),
            "exchanges": [],
        }

        acts = (act for act in modified if act[0] == add)
        excs_to_add = []
        for act in acts:
            if isinstance(act[1][1], tuple):
                # this is a biosphere flow
                excs_to_add.append(
                    {
                        "uncertainty type": 0,
                        "loc": 0,
                        "amount": 0,
                        "type": "biosphere",
                        "input": (
                            "biosphere3",
                            exp.get_bio_code(
                                dict_bio[(act[1][0], act[1][1], act[1][2], act[1][3])]
                            ),
                        ),
                        "name": act[1][0],
                        "unit": act[1][3],
                        "categories": act[1][1],
                    }
                )

            else:

                if act[1] == act[0]:
                    excs_to_add.append(
                        {
                            "uncertainty type": 0,
                            "loc": 1,
                            "amount": 1,
                            "type": "production",
                            "production volume": 0,
                            "product": act[1][1],
                            "name": act[1][0],
                            "unit": act[1][4],
                            "location": act[1][3],
                        }
                    )

                else:

                    excs_to_add.append(
                        {
                            "uncertainty type": 0,
                            "loc": 0,
                            "amount": 0,
                            "type": "technosphere",
                            "production volume": 0,
                            "product": act[1][1],
                            "name": act[1][0],
                            "unit": act[1][4],
                            "location": act[1][3],
                        }
                    )
        act_to_add["exchanges"].extend(excs_to_add)
        acts_to_add.append(act_to_add)

    origin_db.extend(acts_to_add)

    return origin_db


def prepare_db_for_export(scenario):

    base = BaseTransformation(
        database=scenario["database"],
        iam_data=scenario["iam data"],
        model=scenario["model"],
        pathway=scenario["pathway"],
        year=scenario["year"],
    )

    # we ensure the absence of duplicate datasets
    base.database = check_for_duplicates(base.database)
    # we remove uncertainty data
    base.database = remove_uncertainty(base.database)
    # we check the format of numbers
    base.database = check_amount_format(base.database)

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

    return base.database


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
