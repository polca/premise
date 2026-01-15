
import pandas as pd
import numpy as np
from pathlib import Path
import os
# import yaml

def run_rail_freight(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data from GCAM for bus. two files: one with physical output (activity in passenger-km) and one with energy use (in EJ)
    rail_freight_output = pd.read_csv(DATA_DIR / 'transport service output by tech.csv')
    rail_freight_input = pd.read_csv(DATA_DIR / 'transport final energy by tech and fuel.csv')

    # subset to only bus data
    rail_freight_output = rail_freight_output[rail_freight_output['subsector'] == 'Freight Rail']
    rail_freight_input = rail_freight_input[rail_freight_input['mode'] == 'Freight Rail']

    # we need to reshape all of the data in a format premise can understand
    # first, reshape rail_freight_output
    # store in temp_df
    temp_df = rail_freight_output.copy()
    temp_df= temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    rail_freight_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    rail_freight_output = pd.concat([rail_freight_output, temp_df], axis=0)


    # now reshape rail_freight_input
    temp_df = rail_freight_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    rail_freight_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    rail_freight_input = pd.concat([rail_freight_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    rail_freight_input = rail_freight_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    rail_freight_output = rail_freight_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    rail_freight_input['Scenario'] = scenario_name
    rail_freight_output['Scenario'] = scenario_name

    # add GCAM as Model
    rail_freight_input['Model'] = 'GCAM'
    rail_freight_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    rail_freight_input['Unit'] = 'EJ/yr'
    rail_freight_output['Unit'] = 'million tkm/yr'

    # define Variable
    # input: Final Energy|Transport|Pass|Road|{subsector}|{technology}
    # output: Distance|Transport|Pass|Road|{subsector}|{technology}
    rail_freight_input['Variable'] = 'Final Energy|Transport|Freight|Rail|' + rail_freight_input['mode'] + '|' + rail_freight_input['technology']
    rail_freight_output['Variable'] = 'Distance|Transport|Freight|Rail|' + rail_freight_output['subsector'] + '|' + rail_freight_output['technology'] 

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    rail_freight_input = rail_freight_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    rail_freight_output = rail_freight_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    rail_freight_input_pivot = pd.pivot_table(rail_freight_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    rail_freight_output_pivot = pd.pivot_table(rail_freight_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    out_df = pd.concat([rail_freight_input_pivot, rail_freight_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_transport_rail_freight.xlsx'), index=False)
