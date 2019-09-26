"""
.. module: data_collection.py

"""
import os
from pathlib import Path
from inspect import currentframe, getframeinfo
import pandas as pd
import xarray as xr
import numpy as np
import csv

DEFAULT_REMIND_DATA_DIR = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'Remind output files')
REMIND_ELEC_MARKETS = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'remind_electricity_markets.csv')


class RemindDataCollection:
    """
    Class that extracts data from REMIND output files.

    :ivar scenario: name of a Remind scenario
    :vartype scenario: str

    """

    def __init__(self, scenario, year):
        self.scenario = scenario
        self.year = year
        self.data = self.get_remind_data()
        self.market_labels = self.get_remind_electricity_market_labels()
        self.rev_market_labels = self.get_rev_electricity_market_labels()
        self.markets = self.get_remind_markets()

    def get_remind_electricity_market_labels(self):
        """
        Loads a csv file into a dictionary. This dictionary contains labels of electricity markets
        in Remind.

        :return: dictionary that contains market names equivalence
        :rtype: dict
        """

        if not REMIND_ELEC_MARKETS.is_file():
            raise FileNotFoundError('The REMIND electricity markets dictionary file could not be found.')

        with open(REMIND_ELEC_MARKETS) as f:
            return dict(filter(None, csv.reader(f, delimiter=';')))

    def get_rev_electricity_market_labels(self):
        return {v: k for k, v in self.market_labels.items()}


    def get_remind_data(self, directory = DEFAULT_REMIND_DATA_DIR):
        """
        Read the REMIND csv result file and return an `xarray` with dimensions:
        * region
        * variable
        * year

        :return: an multi-dimensional array with Remind data
        :rtype: xarray.core.dataarray.DataArray

        """

        filename = self.scenario + ".mif"
        filepath = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath(directory, filename)
        if not filepath.is_file():
            raise FileNotFoundError('The REMIND data outputs file could not be found.')

        df = pd.read_csv(
            filepath, sep=';',
            index_col=['Region', 'Variable', 'Unit']
        ).drop(columns=['Model', 'Scenario', 'Unnamed: 24'])
        df.columns = df.columns.astype(int)

        df = df.loc[(df.index.get_level_values('Variable').str.contains('SE')) |
                    (df.index.get_level_values('Variable').str.contains('Tech'))]
        variables = df.index.get_level_values('Variable').unique()
        #variables = [','.join(v.split("|")[-2:]) for v in variables]
        #print(variables)
        regions = df.index.get_level_values('Region').unique()
        years = df.columns
        array = xr.DataArray(
            np.zeros(
                (
                    len(variables),
                    len(regions),
                    len(years),
                    1
                )
            ),
            coords=[
                variables,
                regions,
                years,
                np.arange(1)
            ],
            dims=["variable", "region", "year", "value"],
        )
        for r in regions:
            val = df.loc[(df.index.get_level_values('Region') == r), :]
            array.loc[dict(region=r, value=0)] = val

        return array



    def get_remind_markets(self, drop_hydrogen = True):
        """
        This method retrieves the market share for each electricity-producing technology, for a specified year,
        for each region provided by REMIND.
        Electricity production from hydrogen can be removed from the mix (unless specified, it is removed).

        :param drop_hydrogen: removes hydrogen from the region-specific electricity mix if `True`.
        :type drop_hydrogen: bool
        :return: an multi-dimensional array with electricity technologies market share for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """
        # If hydrogen is not to be considered, it is removed from the technologies labels list
        if drop_hydrogen:
            list_technologies = [l for l in list(self.market_labels.values()) if 'Hydrogen' not in l]
        else:
            list_technologies = list(self.market_labels.values())

        # If the year specified is not contained within the range of years given by REMIND
        if self.year < self.data.year.values.min() or self.year > self.data.year.values.max():
            raise KeyError('year not valid, must be between 2005 and 2150')

        # Otherwise, if the year specified corresponds exactly to a year given by REMIND
        elif self.year in self.data.coords['year']:
            # The contribution of each technologies, for a specified year, for a specified region is normalized to 1.
            return self.data.loc[list_technologies,:,self.year]\
                   / self.data.loc[list_technologies,:,self.year].groupby('region').sum(axis=0)

        # Finally, if the specified year falls in between two periods provided by REMIND
        else:
            # Interpolation between two periods
            data_to_interp_from = self.data.loc[list_technologies,:,:]\
                   / self.data.loc[list_technologies,:,:].groupby('region').sum(axis=0)
            return data_to_interp_from.interp(year=self.year)















