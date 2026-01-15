import pandas as pd
import numpy as np
from pathlib import Path
import os, re


def run_other(scenario_name):
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # read in four files
    population = pd.read_csv(DATA_DIR / 'population by region.csv')
    gdp = pd.read_csv(DATA_DIR / 'GDP per capita PPP by region.csv')
    gmst = pd.read_csv(DATA_DIR / 'global mean temperature.csv')
    co2 = pd.read_csv(DATA_DIR / 'CO2 emissions by region.csv')

    # process population
    temp_df = population.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    population = pd.concat([population, temp_df], axis=0)

    # gdp is good as is
    # gmst is mostly good as is,  just need to add world region and subset years
    gmst['region'] = 'World'
    gmst = gmst[gmst['Year'].isin(population['Year'].unique())]
    # process co2
    temp_df = co2.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    co2 = pd.concat([co2, temp_df], axis=0)

    # format to IAMC
    population = population.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    gdp = gdp.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    gmst = gmst.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    co2 = co2.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # add scenario names
    population['Scenario'] = scenario_name
    gdp['Scenario'] = scenario_name
    gmst['Scenario'] = scenario_name
    co2['Scenario'] = scenario_name

    # add model
    population['Model'] = 'GCAM'
    gdp['Model'] = 'GCAM'
    gmst['Model'] = 'GCAM'
    co2['Model'] = 'GCAM'

    # add units
    population['Unit'] = 'thousand people'
    gdp['Unit'] = 'thousand 1990 US$ per capita'
    gmst['Unit'] = 'degC'
    co2['Unit'] = 'MtC/yr'

    # define variable
    population['Variable'] = 'Population'
    gdp['Variable'] = 'GDP|PPP'
    gmst['Variable'] = 'Temperature|Global Mean'
    co2['Variable'] = 'Emissions|CO2'

    # select only relevant columns
    population = population[['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'Year', 'value']]
    gdp = gdp[['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'Year', 'value']]
    gmst = gmst[['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'Year', 'value']]
    co2 = co2[['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'Year', 'value']]

    # pivot tables

    population_pivot = pd.pivot_table(population,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    gdp_pivot = pd.pivot_table(gdp,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    gmst_pivot = pd.pivot_table(gmst,
                                        values=['value'],
                                        index=['Scenario', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    co2_pivot = pd.pivot_table(co2,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()

    out_df = pd.concat([population_pivot, gdp_pivot, gmst_pivot, co2_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_other.xlsx'), index=False)

run_other('ssp24p5tol5')