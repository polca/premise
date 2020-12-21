import os
from . import DATA_DIR
import csv
from pathlib import Path

FILEPATH_BIOSPHERE_FLOWS = (DATA_DIR / "flows_biosphere_37.csv")


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
    :ivar scenario: name of a Remind scenario
    :vartype scenario: str
    :ivar year: year of a Remind scenario
    :vartype year: int

    """

    def __init__(self, db, model, scenario, year, filepath=None):
        self.db = db
        self.model = model
        self.scenario = scenario
        self.year = year
        self.filepath = Path(filepath) / self.model / self.scenario / str(self.year) \
                        or DATA_DIR / "matrices" / self.model / self.scenario / str(self.year)

    def export_db_to_matrices(self):
        index_A = self.create_index_of_A_matrix()


        if not os.path.exists(self.filepath):
            os.makedirs(self.filepath)

        # Export A matrix
        with open(self.filepath / 'A_matrix.csv', 'w') as f:
            writer = csv.writer(f, delimiter=';', lineterminator='\n', )
            writer.writerow(['index of activity', 'index of product', 'value'])
            for ds in self.db:
                for exc in ds['exchanges']:
                    if exc['type'] == 'production':
                        row = [index_A[(ds['name'], ds['reference product'], ds['unit'], ds['location'])],
                               index_A[(exc['name'], exc['product'], exc['unit'], exc['location'])],
                               exc['amount']]
                        writer.writerow(row)
                    if exc['type'] == 'technosphere':
                        row = [index_A[(ds['name'], ds['reference product'], ds['unit'], ds['location'])],
                               index_A[(exc['name'], exc['product'], exc['unit'], exc['location'])],
                               exc['amount'] * -1]
                        writer.writerow(row)

        # Export A index
        with open(self.filepath / 'A_matrix_index.csv', 'w') as f:
            writer = csv.writer(f, delimiter=';', lineterminator='\n', )
            for d in index_A:
                data = list(d) + [index_A[d]]
                writer.writerow(data)

        index_B = self.create_index_of_B_matrix()
        rev_index_B = self.create_rev_index_of_B_matrix()

        # Export B matrix
        with open(self.filepath / 'B_matrix.csv', 'w') as f:
            writer = csv.writer(f, delimiter=';', lineterminator='\n', )
            writer.writerow(['index of activity', 'index of biosphere flow', 'value'])
            for ds in self.db:
                for exc in ds['exchanges']:
                    if exc['type'] == 'biosphere':
                        try:

                            code = exc["input"][1]
                            lookup = rev_index_B[code]
                            ind_B = index_B[lookup]

                            row = [
                                index_A[(ds['name'], ds['reference product'], ds['unit'], ds['location'])],
                                ind_B,
                                exc['amount'] * -1
                            ]
                        except KeyError:
                            print("Cannot find the biosphere flow", exc["name"], exc["categories"])
                        writer.writerow(row)

        # Export B index
        with open(self.filepath / 'B_matrix_index.csv', 'w') as f:
            writer = csv.writer(f, delimiter=';', lineterminator='\n', )
            for d in index_B:
                data = list(d) + [index_B[d]]
                writer.writerow(data)


        print("Matrices saved in {}.".format(self.filepath))

    def create_index_of_A_matrix(self):
        """
        Create a dictionary with row/column indices of the A matrix as key and a tuple (activity name, reference product,
        unit, location) as value.
        :return: a dictionary to map indices to activities
        :rtype: dict
        """
        return {(self.db[i]['name'],
                 self.db[i]['reference product'],
                 self.db[i]['unit'],
                 self.db[i]['location'],): i
                for i in range(0, len(self.db))}

    @staticmethod
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
