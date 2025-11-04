
import pandas as pd
import numpy as np
from pathlib import Path
import os
# import yaml

def run_cdr(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data from GCAM for CDR. one file: 
    cdr = pd.read_csv(DATA_DIR /'co2 sequestration.csv')

    # we need to reshape all of the data in a format premise can understand
    # first, reshape CDR
    # store in temp_df
    temp_df = cdr.copy()
    temp_df['technology'] = temp_df['technology'].replace({
      'hightemp DAC NG': 'Solvent',
      'hightemp DAC elec': 'Solvent',
      'lowtemp DAC heatpump': 'Sorbent'
    })

    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    cdr = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    cdr = pd.concat([cdr, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    cdr = cdr.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    cdr['Scenario'] = scenario_name

    # add GCAM as Model
    cdr['Model'] = 'GCAM'

    # replace Unit column with expected values (MtC/yr)
    cdr['Unit'] = 'MtC/yr'

    # define variable
    # 
    cdr['Variable'] = 'Carbon Sequestration|Direct Air Capture|' + cdr['technology']

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    cdr = cdr[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    cdr_pivot = pd.pivot_table(cdr,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    out_df = cdr_pivot.reset_index(drop=True)
    out_df = pd.concat([cdr_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    # 
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
      os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_cdr.xlsx'), index=False)