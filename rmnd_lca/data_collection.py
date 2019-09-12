"""
.. ecoinvent_modification: model.py

"""
import os
from glob import glob


DEFAULT_DATA_DIR = "data"
DEFAULT_REMIND_DATA_DIR = os.path.join(DEFAULT_DATA_DIR, "Remind output files")
FILEPATH_FIX_NAMES = os.path.join(DEFAULT_DATA_DIR, "fix_names.csv")



class RemindDataCollection:
    """
    Class that extracts data from REMIND output files.
    """

    def __init__(self, scenario):
        self.scenario = scenario

    def get_remind_data(self, directory = DEFAULT_REMIND_DATA_DIR):
        """Read the REMIND csv result file and return a dataframe
        containing all the information.
        """

        file_name = os.path.join(directory, self.scenario + "_*.mif")
        files = glob(file_name)
        if len(files) != 1:
            raise FileExistsError("No or more then one file found for {}.".format(file_name))

        df = pd.read_csv(
            files[0], sep=';',
            index_col=['Region', 'Variable', 'Unit']
        ).drop(columns=['Model', 'Scenario', 'Unnamed: 24'])
        df.columns = df.columns.astype(int)

        return df









