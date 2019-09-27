"""
.. module: electricity.py

"""
import os
from pathlib import Path
from inspect import currentframe, getframeinfo
import pandas as pd
import xarray as xr
import numpy as np
import csv
from wurst.ecoinvent.electricity_markets import \
    empty_low_voltage_markets, empty_high_voltage_markets, empty_medium_voltage_markets
from wurst import searching as ws
from wurst.geo import geomatcher
#from constructive_geometries import geomatcher
from .clean_datasets import DatabaseCleaner as dbclean
from .activity_maps import InventorySet
import uuid
import pprint as pp



REGION_MAPPING_FILEPATH = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'regionmappingH12.csv')
POPULATION_PER_COUNTRY = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'population_per_country.csv')

class Electricity:
    """
    Class that modifies electricity markets in ecoinvent based on REMIND output data.

    :ivar scenario: name of a Remind scenario
    :vartype scenario: str

    """

    def __init__(self, db, rmd, scenario, year):
        self.db = db
        self.rmd = rmd
        self.geo = self.get_REMIND_geomatcher()
        self.population = self.get_population_dict()
        self.scenario = scenario
        self.year = year

        mapping = InventorySet(self.db)

        self.activities_map = mapping.activities_map
        self.powerplant_map = mapping.powerplants_map

    def get_REMIND_geomatcher(self):
        if not REGION_MAPPING_FILEPATH.is_file():
            raise FileNotFoundError('The mapping file for region names could not be found.')

        with open(REGION_MAPPING_FILEPATH) as f:
            f.readline()
            csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
            l = [(x[1], x[2]) for x in csv_list]

        # List of countries not found
        countries_not_found = ["CC", "CX", 'GG', 'JE', 'BL', 'MA']

        rmnd_to_iso = {}
        iso_to_rmnd = {}
        # Build a dictionary that maps region names (used by REMIND) to ISO country codes
        # And a reverse dictionary that maps ISO country codes to region names
        for ISO, region in l:
            if ISO not in countries_not_found:
                try:
                    rmnd_to_iso[region].append(ISO)
                except KeyError:
                    rmnd_to_iso[region] = [ISO]

                iso_to_rmnd[region] = ISO

        geo = geomatcher
        geo.add_definitions(rmnd_to_iso, "REMIND")

        return geo

    def remind_to_ecoinvent_location(self, location):

        if location != "World":
            location = ('REMIND', location)

            ecoinvent_locations =[]
            try:
                for r in self.geo.intersects(location):
                    if r!= 'GLO':
                        if not isinstance(r, tuple):
                            ecoinvent_locations.append(r)
                        #else:
                        #    if r[0] == 'ecoinvent':
                        #        ecoinvent_locations.append(r[1])
                return ecoinvent_locations
            except KeyError as e:
                print("Can't find location {} using the geomatcher.".format(location))

        else:
            return "GLO"

    def ecoinvent_to_remind_location(self, location):
        """
        Return a REMIND region name for a 2-digit ISO country code given.
        :param location: 2-digit ISO country code
        :type location: str
        :return: REMIND region name
        :rtype: str
        """

        if location == 'RoW':
            location = 'GLO'

        location = dbclean('').get_fix_names_dict().get(location, location)

        try:
            remind_location = [r[1] for r in self.geo.intersects(location) if r[0] == 'REMIND']
        except KeyError as e:
            print("Can't find location {} using the geomatcher.".format(remind_location))

        return remind_location


    def empty_electricity_markets(self, db):
        empty_high_voltage_markets(self.db)
        empty_medium_voltage_markets(self.db)
        empty_low_voltage_markets(self.db)

    def delete_electricity_inputs_from_market(self, ds):
        # This function reads through an electricity market dataset and deletes all electricity inputs that are not own consumption.
        ds['exchanges'] = [exc for exc in
                           ws.get_many(ds['exchanges'],
                                       *[ws.either(*[ws.exclude(ws.contains('unit', 'kilowatt hour')),
                                            ws.contains('name','market for electricity, high voltage'),
                                              ws.contains('name','market for electricity, medium voltage'),
                                              ws.contains('name','market for electricity, low voltage'),
                                              ws.contains('name','electricity voltage transformation')])])]

    def find_ecoinvent_electricity_datasets_in_same_ecoinvent_location(self, tech, location):
        # first try ecoinvent location code:
        try:
            return [x for x in
                    ws.get_many(self.db, *[ws.either(*[ws.equals('name', name) for name in self.powerplant_map[self.rmd.rev_market_labels[tech]]]),
                                      ws.equals('location', location), ws.equals('unit', 'kilowatt hour')])]

        # otherwise try remind location code (for new datasets)
        except:
            try:
                return [x for x in ws.get_many(self.db, *[
                    ws.either(*[ws.equals('name', name) for name in self.powerplant_map[self.rmd.rev_market_labels[tech]]]),
                    ws.equals('location', self.ecoinvent_to_remind_locations(location)),
                    ws.equals('unit', 'kilowatt hour')])]
            except:
                return []

    def find_other_ecoinvent_regions_in_remind_region(self, loc):
        if loc == 'RoW':
            loc = 'GLO'

        loc = dbclean('').get_fix_names_dict().get(loc, loc)
        remind_regions = [r for r in geomatcher.intersects(loc) if r[0] == 'REMIND']
        temp = [r for region in remind_regions for r in geomatcher.contained(region)]
        result = [t[1] if isinstance(t, tuple) else t for t in temp]

        return set(result)

    def find_ecoinvent_electricity_datasets_in_remind_location(self, tech, location):
        try:
            return [x for x in
                    ws.get_many(self.db,
                      *[ws.either(*[ws.equals('name', name) for name in self.powerplant_map[self.rmd.rev_market_labels[tech]]]),
                      ws.either(*[ws.equals('location', loc) for loc in self.find_other_ecoinvent_regions_in_remind_region(location)]),
                      ws.equals('unit', 'kilowatt hour')
                      ])]
        except:
            return []

    def add_new_datasets_to_electricity_market(self, ds):
        # This function adds new electricity datasets to a market based on remind results.
        # We pass not only a dataset to modify, but also a pandas dataframe containing the new electricity mix information,
        # and the db from which we should find the datasets
        # find out which remind regions correspond to our dataset:

        remind_locations = self.ecoinvent_to_remind_location(ds['location'])

        print(remind_locations, ds['location'])

        # here we find the mix of technologies in the new market and how much they contribute:
        mix = self.rmd.markets.loc[:,remind_locations,:].mean(axis=1)

        print(mix)

        # here we find the datasets that will make up the mix for each technology
        #datasets = {}
        #for i in mix.coords['variable'].values:
        #    if mix.loc[i] != 0:

                # First try to find a dataset that is from that location (or remind region for new datasets):
        #        datasets[i] = self.find_ecoinvent_electricity_datasets_in_same_ecoinvent_location(i, ds['location'])
        #
        #         #print('First round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])
        #
        #         # If this doesn't work, we try to take a dataset from another ecoinvent region within the same remind region
        #         if len(datasets[i]) == 0:
        #             datasets[i] = self.find_ecoinvent_electricity_datasets_in_remind_location(i, ds['location'])
        #             #print('Second round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])
        #         #
        #         # If even this doesn't work, try taking a global datasets
        #         if len(datasets[i]) == 0:
        #             datasets[i] = self.find_ecoinvent_electricity_datasets_in_same_ecoinvent_location(i, 'GLO')
        #             #print('Third round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])
        #         #
        #         #if no global dataset available, we just take the average of all datasets we have:
        #         #if len(datasets[i]) == 0:
        #         #    datasets[i] = self.find_ecoinvent_electricity_datasets_in_all_locations(i)
        #         #    print('Fourth round: ',i, [(ds['name'], ds['location']) for ds in datasets[i]])
        #         #
        #         # # If we still can't find a dataset, we just take the global market group
        #         # if len(datasets[i]) == 0:
        #         #     print('No match found for location: ', ds['location'], ' Technology: ', i,
        #         #           '. Taking global market group for electricity')
        #         #     datasets[i] = [x for x in ws.get_many(db, *[
        #         #         ws.equals('name', 'market group for electricity, high voltage'), ws.equals('location', 'GLO')])]
        #         print(i, [(ds['name'], ds['location']) for ds in datasets[i] if len(datasets[i]) == 0])

         # Now we add the new exchanges:
        #for i in mix.coords['variable'].values:
        #     if mix[i] != 0:
        #         total_amount = mix[i]
        #         amount = total_amount / len(datasets[i])
        #         for dataset in datasets[i]:
        #             ds['exchanges'].append({
        #                 'amount': amount,
        #                 'unit': dataset['unit'],
        #                 'input': (dataset['database'], dataset['code']),
        #                 'type': 'technosphere',
        #                 'name': dataset['name'],
        #                 'location': dataset['location']
        #             })
        #
        # # confirm that exchanges sum to 1!
        # sum = np.sum([exc['amount'] for exc in ws.technosphere(ds, *[ws.equals('unit', 'kilowatt hour'),
        #                                                              ws.doesnt_contain_any('name', [
        #                                                                  'market for electricity, high voltage'])])])
        # if round(sum, 4) != 1.00:  print(ds['location'], " New exchanges don't add to one! something is wrong!", sum)
        # return

    def get_suppliers_of_a_region(self, ecoinvent_regions, ecoinvent_technologies):

        return ws.get_many(self.db,
                                 *[ws.either(*[ws.equals('name', supplier) for supplier in ecoinvent_technologies]),
                                   ws.either(*[ws.equals('location', loc) for loc in ecoinvent_regions]),
                                   ws.equals('unit', 'kilowatt hour')])

    def get_population_dict(self):

        if not POPULATION_PER_COUNTRY.is_file():
            raise FileNotFoundError('The population per country dictionary file could not be found.')

        with open(POPULATION_PER_COUNTRY) as f:
            return dict(filter(None, csv.reader(f, delimiter=';')))

    def get_pop_weighted_share(self, supplier, suppliers):
        loc_population = int(self.population.get(supplier['location'],0))

        locs_population = 0

        for loc in suppliers:
            locs_population += int(self.population.get(loc['location'],0))

        return loc_population/locs_population


    def update_electricity_markets(self):
        # ## Functions for modifying ecoinvent electricity markets

        electricity_market_filter = [ws.either(ws.contains('name', 'market for electricity,'),
                                                ws.contains('name', 'market group for electricity')),
                                      ws.doesnt_contain_any('name', ['aluminium industry',
                                                                     'internal use in coal mining',
                                                                     'municipal'])]

        # We first need to delete 'market for electricity' and 'market group for electricity' datasets
        for ds in ws.get_many(self.db, *electricity_market_filter):
            del ds

        # We then need to create high voltage REMIND electricity markets

        # Loop through REMIND regions, except "World"
        gen = (region for region in self.rmd.markets.coords['region'].values if region != "World")

        for region in gen:

            # Fetch ecoinvent regions contained in the REMIND region
            ecoinvent_regions = self.remind_to_ecoinvent_location(region)

            # Create an empty dataset
            new_dataset = {}
            new_dataset['location'] = region
            new_dataset['name'] = 'market group for electricity, high voltage, ' + self.scenario + ', ' + str(self.year)
            new_dataset['reference product'] = 'electricity, high voltage'
            new_dataset['unit'] = 'kilowatt hour'
            new_dataset['database'] = self.db[1]['database']
            new_dataset['code'] = str(uuid.uuid1())
            new_dataset['comment'] = 'Dataset produced from REMIND scenario output results'

            new_exchanges = []

            # Loop through the REMIND technologies
            for technology in self.rmd.markets.coords['variable'].values:

                # If the given technology contributes to the mix
                if self.rmd.markets.loc[technology,region,0] != 0.0:

                    # Contribution in supply
                    amount = self.rmd.markets.loc[technology, region, 0].values


                    # Get the possible names of ecoinvent datasets
                    ecoinvent_technologies = self.powerplant_map[self.rmd.rev_market_labels[technology]]

                    # Fetch electricity-producing technologies contained in the REMIND region
                    #print(ecoinvent_regions, ecoinvent_technologies)

                    suppliers = list(self.get_suppliers_of_a_region(ecoinvent_regions, ecoinvent_technologies))
                    number_of_suppliers = len(suppliers)


                    # If no technology is available for the REMIND region
                    if number_of_suppliers == 0:
                        # We fetch European technologies instead
                        suppliers = list(self.get_suppliers_of_a_region(self.remind_to_ecoinvent_location('EUR'),
                                                                   ecoinvent_technologies))
                        number_of_suppliers = len(list(suppliers))

                    cumul_amount = 0
                    for supplier in suppliers:

                        cumul_amount += (amount * share)

                        share = self.get_pop_weighted_share(supplier, suppliers)

                        new_exchanges.append(
                            {
                                'uncertainty type': 0,
                                'loc': (amount * share),
                                'amount': (amount * share) ,
                                'type': 'technosphere',
                                'production volume': 0,
                                'product': supplier['reference product'],
                                'name': supplier['name'],
                                'unit': supplier['unit'],
                                'location': supplier['location']
                            }
                        )
                    print(region, technology, amount, cumul_amount)


            total = 0
            for e in new_exchanges:
                total += e['amount']
         

            new_dataset['exchanges'] = new_exchanges



            #pp.pprint(new_dataset)


















