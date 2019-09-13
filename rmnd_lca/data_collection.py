"""
.. module: data_collection.py

"""
import os
from pathlib import Path
from inspect import currentframe, getframeinfo
import pandas as pd
import xarray as xr
import numpy as np

DEFAULT_REMIND_DATA_DIR = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'Remind output files')
REMIND_ELEC_MARKETS = Path(getframeinfo(currentframe()).filename).resolve().parent.joinpath('data/'+ 'remind_electricity_markets.csv')


class RemindDataCollection:
    """
    Class that extracts data from REMIND output files.

    :ivar scenario: name of a Remind scenario
    :vartype scenario: str

    """

    def __init__(self, scenario):
        self.scenario = scenario

    def get_remind_electricity_market_labels(self):
        """
        Loads a csv file into a dictionary. This dictionary contains labels of electricity markets
        in Remind.

        :return: dictionary that contains market names equivalence
        :rtype: dict
        """
        with open(REMIND_ELEC_MARKETS) as f:
            return dict(filter(None, csv.reader(f, delimiter=';')))


    def get_remind_data(self, directory = DEFAULT_REMIND_DATA_DIR):
        """Read the REMIND csv result file and return an `xarray` with dimensions:
        * region
        * variable
        * year


        :return: an multi-dimensional array with Remind data
        :rtype: xarray.core.dataarray.DataArray

        """

        file_name = os.path.join(directory, self.scenario + ".mif")

        df = pd.read_csv(
            file_name, sep=';',
            index_col=['Region', 'Variable', 'Unit']
        ).drop(columns=['Model', 'Scenario', 'Unnamed: 24'])
        df.columns = df.columns.astype(int)

        df = df.loc[(df.index.get_level_values('Variable').str.contains('SE')) |
                    (df.index.get_level_values('Variable').str.contains('Tech'))]
        variables = df.index.get_level_values('Variable').unique()
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

    def get_remind_markets(self, remind_data, year, drop_hydrogen = True):

        # This is a dictionary that contains simple energy sources has keys
        # and corresponding REMIND market names as values
        market_labels = self.get_remind_electricity_market_labels()
        # This is the inverse of the dictionary
        rev_market_labels = {v: k for k, v in market_labels.items()}

        if year < 2005 or year > 2150:
            print('year not valid, must be between 2005 and 2150')
            raise

        elif year in remind_data.coords['year']:
            result = remind_data.unstack(level=0)[year].loc[
                list(remind_electricity_market_labels.values())].reset_index(level=1, drop=True).rename(
                index=rename_remind_electricity_market_labels).divide(
                remind_data.unstack(level=0)[year].loc[list(remind_electricity_market_labels.values())].sum(
                    axis=0)).drop('World', axis=1)

        else:
            temp = remind_data.unstack(level=0).loc[list(remind_electricity_market_labels.values())].reset_index(
                level=1, drop=True).rename(index=rename_remind_electricity_market_labels).stack(level=1).T
            new = pd.DataFrame(index=temp.columns, columns=[year], data=np.nan).T

            result = pd.concat([temp, new]).sort_index().interpolate(method='values').loc[year].unstack(level=1)

        if drop_hydrogen == False:
            return result
        else:
            print('Excluding hydrogen from electricity markets.\nHydrogen had a maximum share of ' + str(
                round(result.loc['Hydrogen'].max() * 100, 2)) + ' %')
            return result.drop('Hydrogen', axis=0).divide(result.drop('Hydrogen', axis=0).sum())









