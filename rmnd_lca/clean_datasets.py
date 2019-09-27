"""
.. module: clean_datasets.py

"""
import wurst
from wurst import searching as ws
import pprint
import csv
from pathlib import Path
from inspect import currentframe, getframeinfo

FILEPATH_FIX_NAMES = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'fix_names.csv')

class DatabaseCleaner:
    """
    Class that cleans the datasets contained in the inventory database for further processing.

    :ivar destination_db: name of the source database
    :vartype destination_db: str

    """

    def __init__(self, destination_db):
        self.destination = destination_db

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


    def rename_locations(self, db, name_dict):
        """
        This function loops through dataset and exchange location names and correct them if needed or if missing,
        based on a dictionary located in 'data/fix_names.csv'.

        :param db: wurst inventory database
        :type db: list
        :param name_dict: dictionary that contains names equivalence
        :type name_dict: dict
        :return: wurst inventory database
        :rtype: list
        """
        for ds in db:
            # If the dataset does not have a location defined
            ds['location'] = ds.get('location', 'GLO')

            # If the location name of the dataset is found in the dictionary
            ds['location'] = name_dict.get(ds['location'], ds['location'])

            for exc in ws.technosphere(ds):
                # If the exchange does not have a location defined
                exc['location'] = exc.get('location', 'GLO')

                # If the location name of the exchange is found in the dictionary
                exc['location'] = name_dict.get(exc['location'], exc['location'])

    def get_fix_names_dict(self):
        """
        Loads a csv file into a dictionary. This dictionary contains a few location names
        that need correction in the wurst inventory database.

        :return: dictionary that contains names equivalence
        :rtype: dict
        """
        with open(FILEPATH_FIX_NAMES) as f:
            return dict(filter(None, csv.reader(f, delimiter=';')))

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
        return [x['location'] for x in wurst.searching.get_many(db, *[ws.equals(k, v) for k, v in lookup_dict.items()])]

    # Functions to clean up Wurst import and additional technologies
    def fix_unset_technosphere_and_production_exchange_locations(self, db, matching_fields=('name', 'unit')):
        """
        Give all the production and technopshere exchanges with a missing location name the location of the dataset
        they belong to.
        Modifies in place (does not return anything).

        :param db: wurst inventory database
        :type db: list
        :param matching_fields: filter conditions
        :type matching_fields: tuple

        """
        for ds in db:
            # collect the production exchange that does not have a location
            exc_prod = wurst.production(ds, ws.equals('location', ''))

            # if such exists, it receives the location of the dataset
            if sum(1 for x in exc_prod)>0:
                next(exc_prod)['name'] = ds['location']

            # collect the technopshere exchange(s) that do not have a location
            exc_tech = wurst.technosphere(ds, ws.equals('location', ''))

            for e in exc_tech:
                # retrieve the REMIND location name(s)
                locs = self.find_location_given_lookup_dict(db, {k: e.get(k) for k in matching_fields})

                if len(locs) == 1:
                    e['location'] = locs[0]
                else:
                    print("No unique location found for exchange:\n{}\nFound: {}".format(
                        pprint.pformat(e), locs))

    def prepare_datasets(self, write_changeset=False):
        """
        Clean datasets for all databases listed in scenarios: fix location names, remove
        empty exchanges, etc.

        :param emi_fname: dictionary that lists scenarios
        :type emi_fname: dict
        :param write_changeset: indicates if changes in datasets should be logged.
        :type write_changeset: bool

        """

        # Load to ecoinvent database in wurst
        db = wurst.extract_brightway2_databases(self.destination)
        # Set missing locations to ```GLO``` for datasets in ``database``
        wurst.default_global_location(db)
        # Set missing locations to ```GLO``` for exchanges in ``datasets``
        self.fix_unset_technosphere_and_production_exchange_locations(db)
        # Remove empty exchanges
        self.remove_nones(db)
        # Change specific location names
        self.rename_locations(db, self.get_fix_names_dict())

        if 'carma' in self.destination:
            # Add negative CO2 exchanges for CCS using biomass
            self.add_negative_CO2_flows_for_biomass_CCS(db)

        return db







