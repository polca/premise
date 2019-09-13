"""
.. module: ecoinvent_modification.py

"""

import pyprind
from .clean_datasets import DatabaseCleaner
from .data_collection import RemindDataCollection




class NewDatabase:
    """
    Class that represents a new wurst inventory database, modified according to IAM data.

    :ivar database_dict: dictionary with scenarios to create
    :vartype database_dict: dict
    :ivar destination_db: name of the source database
    :vartype destination_db: str

    """

    def __init__(self, database_dict, destination_db):
        self.scenarios = database_dict
        self.destination = destination_db

    def modify_database(self):
        new_db = DatabaseCleaner(self.destination).prepare_datasets()

        for s in pyprind.prog_bar(self.scenarios.items()):
            scenario, year = s
            remind_data = RemindDataCollection(scenario).get_remind_data()








