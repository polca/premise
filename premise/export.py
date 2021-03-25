import os
from . import DATA_DIR, __version__
import csv
from pathlib import Path
import datetime
import json
import re
import pandas as pd

FILEPATH_BIOSPHERE_FLOWS = DATA_DIR / "flows_biosphere_37.csv"



def create_index_of_A_matrix(db):
    """
    Create a dictionary with row/column indices of the A matrix as key and a tuple (activity name, reference product,
    unit, location) as value.
    :return: a dictionary to map indices to activities
    :rtype: dict
    """
    return {
        (
            db[i]["name"],
            db[i]["reference product"],
            db[i]["unit"],
            db[i]["location"],
        ): i
        for i in range(0, len(db))
    }

def create_codes_index_of_A_matrix(db):
    """
    Create a dictionary with row/column indices of the A matrix as key and the activity code as value.
    :return: a dictionary to map indices to activity codes
    :rtype: dict
    """
    return {db[i]["code"]: i for i in range(0, len(db))}

def create_codes_index_of_B_matrix():
    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError(
            "The dictionary of biosphere flows could not be found."
        )

    csv_dict = dict()

    with open(FILEPATH_BIOSPHERE_FLOWS) as f:
        input_dict = csv.reader(f, delimiter=";")
        for i, row in enumerate(input_dict):
            csv_dict[row[-1]] = i

    return csv_dict

def create_index_of_B_matrix():
    if not FILEPATH_BIOSPHERE_FLOWS.is_file():
        raise FileNotFoundError(
            "The dictionary of biosphere flows could not be found."
        )

    csv_dict = dict()

    with open(FILEPATH_BIOSPHERE_FLOWS) as f:
        input_dict = csv.reader(f, delimiter=";")
        for i, row in enumerate(input_dict):
            csv_dict[(row[0], row[1], row[2], row[3])] = i

    return csv_dict


class Export:
    """
    Class that exports the transformed data into matrices:
    * A matrix: contains products exchanges
    * B matrix: contains exchanges activities and the biosphere

    The A and B matrices are exported as csv files in a sparse representation (only non-zero values are listed), like so:
    - index row, index column, value of exchange

    Dictionaries to map row numbers to activities and products names are also exported.

    :ivar db: transformed database
    :vartype db: dict
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
        self.bio_codes = self.rev_index(create_codes_index_of_B_matrix())

    def create_A_matrix_coordinates(self):
        index_A = create_index_of_A_matrix(self.db)
        list_rows = []

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
        return list_rows

    def create_B_matrix_coordinates(self):

        index_B = create_index_of_B_matrix()
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
                    list_rows.append(row)
        return list_rows

    def export_db_to_matrices(self):

        if self.filepath is not None:
            self.filepath = Path(self.filepath) / self.model / self.scenario / str(self.year)
        else:
            self.filepath = (
                DATA_DIR / "export" / self.model / self.scenario / str(self.year)
            )


        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)

        # Export A matrix
        with open(self.filepath / "A_matrix.csv", "w") as f:
            writer = csv.writer(f, delimiter=";", lineterminator="\n",)
            writer.writerow(["index of activity", "index of product", "value"])
            rows = self.create_A_matrix_coordinates()
            for row in rows:
                writer.writerow(row)


        # Export A index
        with open(self.filepath / "A_matrix_index.csv", "w") as f:
            writer = csv.writer(f, delimiter=";", lineterminator="\n",)
            index_A = create_index_of_A_matrix(self.db)
            for d in index_A:
                data = list(d) + [index_A[d]]
                writer.writerow(data)

        index_B = create_index_of_B_matrix()

        # Export B matrix
        with open(self.filepath / "B_matrix.csv", "w") as f:
            writer = csv.writer(f, delimiter=";", lineterminator="\n",)
            writer.writerow(["index of activity", "index of biosphere flow", "value"])
            rows = self.create_B_matrix_coordinates()
            for row in rows:
                writer.writerow(row)


        # Export B index
        with open(self.filepath / "B_matrix_index.csv", "w") as f:
            writer = csv.writer(f, delimiter=";", lineterminator="\n",)
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

        with open(FILEPATH_BIOSPHERE_FLOWS) as f:
            input_dict = csv.reader(f, delimiter=";")
            for row in input_dict:
                csv_dict[row[-1]] = (row[0], row[1], row[2], row[3])

        return csv_dict

    @staticmethod
    def get_simapro_biosphere_dictionnary():
        # Load the matching dictionary between ecoinvent and Simapro biosphere flows
        filename = "simapro-biosphere.json"
        filepath = DATA_DIR / filename
        if not filepath.is_file():
            raise FileNotFoundError(
                "The dictionary of biosphere flow match between ecoinvent and Simapro could not be found."
            )
        with open(filepath) as json_file:
            data = json.load(json_file)
        dict_bio = {}
        for d in data:
            dict_bio[d[2]] = d[1]

        return dict_bio

    @staticmethod
    def load_simapro_categories():
        """Load a dictionary with categories to use for Simapro export"""

        # Load the matching dictionary
        filename = "simapro_classification.csv"
        filepath = DATA_DIR / filename
        if not filepath.is_file():
            raise FileNotFoundError(
                "The dictionary of Simapro categories could not be found."
            )
        with open(filepath) as f:
            csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
        header, *data = csv_list

        dict_cat = {}
        for row in data:
            _, cat_code, category_1, category_2, category_3 = row
            dict_cat[cat_code] = {
                "category 1": category_1,
                "category 2": category_2,
                "category 3": category_3
            }

        return dict_cat

    @staticmethod
    def get_simapro_category_of_exchange():

        """Load a dictionary with categories to use for Simapro export based on ei 3.7"""

        # Load the matching dictionary
        filename = "simapro_categories.csv"
        filepath = DATA_DIR / filename
        if not filepath.is_file():
            raise FileNotFoundError(
                "The dictionary of Simapro categories could not be found."
            )
        with open(filepath) as f:
            csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
        header, *data = csv_list

        dict_cat = {}
        for row in data:
            name, category_1, category_2 = row
            dict_cat[name] = {
                "main category": category_1,
                "category": category_2,
            }

        return dict_cat

    def get_category_of_exchange(self):
        """
        This function returns a dictionnary with (name, reference product) as keys,
        and {'main category': 'xxxxxx', 'category': 'yyyyyy'} as values.
        This is useful for Simapro export, to categorize datasets into a tree structure.
        :return: dict
        """

        dict_classifications = self.load_simapro_categories()
        dict_categories = {}

        for ds in self.db:
            if (ds["name"], ds["reference product"]) not in dict_categories:
                main_category, category = (None, None)

                if "classifications" in ds:
                    if len(ds["classifications"]) > 0:
                        for x in ds["classifications"]:
                            if x[0] == "ISIC rev.4 ecoinvent":
                                main_category = dict_classifications[x[1].split(":")[0].strip()]["category 1"]

                                if dict_classifications[x[1].split(":")[0].strip()]["category 3"] != "":
                                    category = (dict_classifications[x[1].split(":")[0].strip()]["category 2"] + "\ ".strip()
                                     + dict_classifications[x[1].split(":")[0].strip()]["category 3"])
                                else:
                                    category = dict_classifications[x[1].split(":")[0].strip()]["category 2"]

                        if not main_category:
                            for x in ds["classifications"]:
                                if x[0] == "CPC":
                                    main_category = dict_classifications[x[1].split(":")[0].strip()]["category 1"]

                                    if dict_classifications[x[1].split(":")[0].strip()]["category 3"] != "":
                                        category = (dict_classifications[x[1].split(":")[0].strip()][
                                                        "category 2"] + "\ ".strip()
                                                    + dict_classifications[x[1].split(":")[0].strip()]["category 3"])
                                    else:
                                        category = dict_classifications[x[1].split(":")[0].strip()]["category 2"]

                if not main_category:
                    main_category = "material"
                    category = "Others"

                dict_categories[(ds["name"], ds["reference product"])] = {"main category": main_category,
                                                                          "category": category}

        return dict_categories

    @staticmethod
    def load_references():
        """Load a dictionary with references of datasets"""

        # Load the matching dictionary
        filename = "references.csv"
        filepath = DATA_DIR / filename
        if not filepath.is_file():
            raise FileNotFoundError(
                "The dictionary of references could not be found."
            )
        with open(filepath) as f:
            csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
        header, *data = csv_list

        dict_reference = {}
        for row in data:
            name, source, description = row
            dict_reference[name] = {
                "source": source,
                "description": description
            }

        return dict_reference

    def export_db_to_simapro(self):

        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)

        dict_bio = self.get_simapro_biosphere_dictionnary()

        headers = [
            "{SimaPro 9.1.1.1}",
            "{processes}",
            "{Project: carculator import"
            + f"{datetime.datetime.today():%d.%m.%Y}"
            + "}",
            "{CSV Format version: 9.0.0}",
            "{CSV separator: Semicolon}",
            "{Decimal separator: .}",
            "{Date separator: .}",
            "{Short date format: dd.MM.yyyy}",
            "{Export platform IDs: No}",
            "{Skip empty fields: No}",
            "{Convert expressions to constants: No}",
            "{Selection: Selection(1)}",
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

        simapro_units = {
            "kilogram": "kg",
            "cubic meter": "m3",
            "cubic meter-year": "m3y",
            "kilowatt hour": "kWh",
            "kilometer": "km",
            "ton kilometer": "tkm",
            "megajoule": "MJ",
            "unit": "p",
            "square meter": "m2",
            "kilowatt": "p",
            "hour": "hr",
            "square meter-year": "m2a",
            "meter": "m",
            "vehicle-kilometer": "vkm",
            "person-kilometer": "personkm",
            "person kilometer": "personkm",
            "meter-year": "my",
            "kilo Becquerel": "kBq",
            "kg*day": "kg*day",
            "hectare": "ha",
            "kilometer-year": "kmy",
            "litre": "l",
            "guest night": "guestnight",
        }

        filename = (
            "simapro_export_"
            + self.model
            + "_"
            + self.scenario
            + "_"
            + str(self.year)
            + ".csv"
        )

        dict_cat_simapro = self.get_simapro_category_of_exchange()
        dict_cat = self.get_category_of_exchange()
        dict_refs = self.load_references()

        with open(self.filepath / filename, "w", newline="", encoding="utf-8") as csvFile:
            writer = csv.writer(csvFile, delimiter=";")
            for item in headers:
                writer.writerow([item])
            writer.writerow([])

            for ds in self.db:

                main_category, category = ("", "")

                if ds["name"] in dict_cat_simapro:

                    main_category, category = (dict_cat_simapro[ds["name"]]["main category"],
                                               dict_cat_simapro[ds["name"]]["category"])
                else:

                    if any(i in ds["name"] for i in ("transport, passenger car", "transport, heavy", "transport, medium")):
                        main_category, category = ("transport", r"Road\Transformation")

                    if any(i in ds["name"] for i in ("Passenger car", "Heavy duty", "Medium duty")):
                        main_category, category = ("transport", r"Road\Infrastructure")

                    if main_category == "":

                        main_category, category = (dict_cat[(ds["name"], ds["reference product"])]["main category"],
                                                   dict_cat[(ds["name"], ds["reference product"])]["category"])

                for item in fields:

                    if main_category.lower() == "waste treatment" and item == "Products":
                        continue

                    if main_category.lower() != "waste treatment"\
                            and item in ("Waste treatment",
                                         "Waste treatment allocation"
                                         ):
                        continue


                    writer.writerow([item])

                    if item == "Process name":

                        name = (
                            ds["reference product"]
                            + " {"
                            + ds.get("location", "GLO")
                            + "}"
                            + "| "
                            + ds["name"]
                            + " "
                            + "| Cut-off, U"
                        )
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
                            string = re.sub('[^a-zA-Z0-9 \.,]', '', dict_refs[ds["name"]]["source"])

                            if dict_refs[ds["name"]]["description"] != "":
                                string += " " + re.sub('[^a-zA-Z0-9 \.,]', '', dict_refs[ds["name"]]["description"])

                            writer.writerow([string])
                        else:
                            if "comment" in ds:
                                string = re.sub('[^a-zA-Z0-9 \.,]', '', ds["comment"])
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
                                    exc_cat = dict_cat_simapro[e["name"]]["main category"].lower()
                                else:
                                    exc_cat = dict_cat[e["name"], e["product"]]["main category"].lower()

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

                                if e["name"].lower() == "water":
                                    e["unit"] = "kilogram"
                                    e["amount"] /= 1000

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
                    if item == "Emissions to water":
                        for e in ds["exchanges"]:
                            if (
                                e["type"] == "biosphere"
                                and e["categories"][0] == "water"
                            ):
                                if e["name"].lower() == "water":
                                    e["unit"] = "kilogram"
                                    e["amount"] /= 1000

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
                    if item == "Emissions to soil":
                        for e in ds["exchanges"]:
                            if (
                                e["type"] == "biosphere"
                                and e["categories"][0] == "soil"
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
                    if item == "Waste to treatment":
                        for e in ds["exchanges"]:
                            if e["type"] == "technosphere":

                                if e["name"] in dict_cat_simapro:
                                    exc_cat = dict_cat_simapro[e["name"]]["main category"].lower()
                                else:
                                    exc_cat = dict_cat[e["name"], e["product"]]["main category"].lower()

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
                i["database"],
                i["location"],
                i["unit"]
            ): x
            for x, i in enumerate(self.db)
        }

    def create_names_and_indices_of_B_matrix(self):
        if not FILEPATH_BIOSPHERE_FLOWS.is_file():
            raise FileNotFoundError(
                "The dictionary of biosphere flows could not be found."
            )

        csv_dict = dict()

        with open(FILEPATH_BIOSPHERE_FLOWS) as f:
            input_dict = csv.reader(f, delimiter=";")
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

