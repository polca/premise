"""
.. module: clean_datasets.py

"""
import wurst
from wurst import searching as ws
import pprint
import csv
from pathlib import Path
from inspect import currentframe, getframeinfo
from bw2io import ExcelImporter

FILEPATH_FIX_NAMES = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'fix_names.csv')
FILEPATH_CARMA_INVENTORIES = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'lci-Carma-CCS.xlsx')
FILEPATH_BIOSPHERE_FLOWS = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'dict_biosphere.txt')

class DatabaseCleaner:
    """
    Class that cleans the datasets contained in the inventory database for further processing.

    :ivar destination_db: name of the source database
    :vartype destination_db: str

    """

    def __init__(self, destination_db):
        self.destination = destination_db
        self.db = wurst.extract_brightway2_databases(self.destination)
        self.biosphere_dict = self.get_biosphere_code()

    def add_negative_CO2_flows_for_biomass_CCS(self, db):
        """
        Rescale the amount of all exchanges of carbon dioxide, non-fossil by a factor -9 (.9/-.1),
        to account for sequestered CO2.

        All CO2 capture and storage in the Carma datasets is assumed to be 90% efficient.
        Thus, we can simply find out what the new CO2 emission is and then we know how much gets stored in the ground.
        It's very important that we ONLY do this for biomass CCS plants, as only they will have negative emissions!

        Modifies in place (does not return anything).

        :param db: wurst inventory database
        :type db: list

        """

        for ds in ws.get_many(db, ws.contains('name', 'storage')):
            for exc in ws.biosphere(ds, ws.equals('name', 'Carbon dioxide, non-fossil')):
                wurst.rescale_exchange(exc, (0.9 / -0.1), remove_uncertainty=True)


    def get_fix_names_dict(self):
        """
        Loads a csv file into a dictionary. This dictionary contains a few location names
        that need correction in the wurst inventory database.

        :return: dictionary that contains names equivalence
        :rtype: dict
        """
        with open(FILEPATH_FIX_NAMES) as f:
            return dict(filter(None, csv.reader(f, delimiter=';')))

    def get_rev_fix_names_dict(self):
        """
        Reverse the fix_names dicitonary.

        :return: dictionary that contains names equivalence
        :rtype: dict
        """

        return {v: k for k, v in self.get_fix_names_dict().items()}

    def remove_nones(self, db):
        """
        Remove empty exchanges in the datasets of the wurst inventory database.
        Modifies in place (does not return anything).

        :param db: wurst inventory database
        :type db: list

        """
        exists = lambda x: {k: v for k, v in x.items() if v is not None}
        for ds in db:
            ds['exchanges'] = [exists(exc) for exc in ds['exchanges']]

    def find_product_given_lookup_dict(self, db, lookup_dict):
        """
        Return a list of location names, given the filtering conditions given in `lookup_dict`.
        It is, for example, used to return a list of location names based on the name and the unit of a dataset.

        :param db: wurst inventory database
        :type db: list
        :param lookup_dict: a dictionary with filtering conditions
        :return: a list of location names
        :rtype: list
        """
        return [x['product'] for x in wurst.searching.get_many(self.db, *[ws.equals(k, v) for k, v in lookup_dict.items()])]

    def find_location_given_lookup_dict(self, db, lookup_dict):
        """
        Return a list of location names, given the filtering conditions given in `lookup_dict`.
        It is, for example, used to return a list of location names based on the name and the unit of a dataset.

        :param db: wurst inventory database
        :type db: list
        :param lookup_dict: a dictionary with filtering conditions
        :return: a list of location names
        :rtype: list
        """
        return [x['location'] for x in wurst.searching.get_many(self.db, *[ws.equals(k, v) for k, v in lookup_dict.items()])]

    # Functions to clean up Wurst import and additional technologies
    def fix_unset_technosphere_and_production_exchange_locations(self, matching_fields=('name', 'unit')):
        """
        Give all the production and technopshere exchanges with a missing location name the location of the dataset
        they belong to.
        Modifies in place (does not return anything).

        :param db: wurst inventory database
        :type db: list
        :param matching_fields: filter conditions
        :type matching_fields: tuple

        """
        for ds in self.db:

            # collect production exchanges that simply do not have a location key and set it to
            # the location of the dataset
            for exc in wurst.production(ds):
                if 'location' not in exc:
                    exc['location'] = ds['location']
                    print(exc)

            for exc in wurst.technosphere(ds):
                if 'location' not in exc:
                    locs = self.find_location_given_lookup_dict(self.db, {k: exc.get(k) for k in matching_fields})

                    if len(locs) == 1:
                        exc['location'] = locs[0]
                    else:
                        print("No unique location found for exchange:\n{}\nFound: {}".format(
                            pprint.pformat(exc), locs))
    def get_biosphere_code(self):
        """
        Retrieve biosphere uuid for biosphere flows imported from Excel inventory.
        :return: uuid
        :rtype: str
        """

        if not FILEPATH_BIOSPHERE_FLOWS.is_file():
            raise FileNotFoundError('The dictionary of biosphere flows could not be found.')

        csv_dict = {}

        with open(FILEPATH_BIOSPHERE_FLOWS) as f:
            input_dict = csv.reader(f, delimiter=';')
            for row in input_dict:
                csv_dict[(row[0], row[1], row[2], row[3])] = row[4]

        return csv_dict


    def add_carma_inventories(self):
        """
        Add CCS technologies from Carma project.
        The inventories are contained in an Excel file within the library `data` directory.
        Modifies the database in place. Does not return anything.

        """
        if not FILEPATH_CARMA_INVENTORIES.is_file():
            raise FileNotFoundError('The Carma inventory file could not be found.')

        i = ExcelImporter(FILEPATH_CARMA_INVENTORIES)

        # Add the `product` key to the production exchange and format the category field for biosphere exchanges
        for x in i.data:
            for y in x['exchanges']:
                if y['type'] == 'production' and "product" not in y:
                    y['product'] = x['reference product']

                if y['type'] == 'biosphere':
                    y['categories'] = tuple(y['categories'].split('::'))
                    if len(y['categories'])>1:
                        y['input'] = ('biosphere3',
                            self.biosphere_dict[(y['name'], y['categories'][0], y['categories'][1], y['unit'])])
                    else:
                        y['input'] = ('biosphere3',
                            self.biosphere_dict[(y['name'], y['categories'][0], 'unspecified', y['unit'])])

        # Add a `product` field to technosphere exchanges
        for x in i.data:
            for y in x['exchanges']:
                if y['type'] == 'technosphere':

                    # Look first in the Carma inventories
                    possibles = [a['reference product'] for a in i.data
                                    if a['name'] == y['name']
                                    and a['location'] == y['location']
                                    and a['unit'] == y['unit']]

                    # If not, look in the ecoinvent inventories
                    if len(possibles) == 0:
                        possibles = [a['reference product'] for a in self.db
                                    if a['name'] == y['name']
                                    and a['location'] == y['location']
                                    and a['unit'] == y['unit']]
                    y['product'] = possibles[0]


        self.db.extend(i)

    def prepare_datasets(self, write_changeset=False):
        """
        Clean datasets for all databases listed in scenarios: fix location names, remove
        empty exchanges, etc.

        :param emi_fname: dictionary that lists scenarios
        :type emi_fname: dict
        :param write_changeset: indicates if changes in datasets should be logged.
        :type write_changeset: bool

        """


        # Set missing locations to ```GLO``` for datasets in ``database``
        print('Set missing location of datasets to global scope.')
        wurst.default_global_location(self.db)
        # Set missing locations to ```GLO``` for exchanges in ``datasets``
        print('Set missing location of production exchanges to scope of dataset.')
        print('Correct missing location of technosphere exchanges.')
        self.fix_unset_technosphere_and_production_exchange_locations()
        # Remove empty exchanges
        print('Remove empty exchanges.')
        self.remove_nones(self.db)

        # Add Carma CCS inventories
        print('Add Carma CCS inventories')
        self.add_carma_inventories()

        # Add carbon storage for CCS technologies
        print('Add fossil carbon dioxide storage for CCS technologies.')
        self.add_negative_CO2_flows_for_biomass_CCS(self.db)



        return self.db







