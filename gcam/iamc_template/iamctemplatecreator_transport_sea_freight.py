
import pandas as pd
import numpy as np
from pathlib import Path
import os
# import yaml

def run_sea_freight(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data from GCAM for bus. two files: one with physical output (activity in passenger-km) and one with energy use (in EJ)
    sea_freight_output = pd.read_csv(DATA_DIR / 'transport service output by tech.csv')
    sea_freight_input = pd.read_csv(DATA_DIR / 'transport final energy by tech and fuel.csv')

    # subset to only bus data
    sea_freight_output = sea_freight_output[sea_freight_output['subsector'].str.contains('Ship')]
    sea_freight_input = sea_freight_input[sea_freight_input['mode'].str.contains('Ship')]

    # we need to reshape all of the data in a format premise can understand
    # first, reshape sea_freight_output
    # store in temp_df
    temp_df = sea_freight_output.copy()
    temp_df= temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    sea_freight_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    sea_freight_output = pd.concat([sea_freight_output, temp_df], axis=0)


    # now reshape sea_freight_input
    temp_df = sea_freight_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    sea_freight_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    sea_freight_input = pd.concat([sea_freight_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    sea_freight_input = sea_freight_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    sea_freight_output = sea_freight_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    sea_freight_input['Scenario'] = scenario_name
    sea_freight_output['Scenario'] = scenario_name

    # add GCAM as Model
    sea_freight_input['Model'] = 'GCAM'
    sea_freight_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    sea_freight_input['Unit'] = 'EJ/yr'
    sea_freight_output['Unit'] = 'million tkm/yr'

    # define Variable
    # input: Final Energy|Transport|Pass|Road|{subsector}|{technology}
    # output: Distance|Transport|Pass|Road|{subsector}|{technology}
    sea_freight_input['Variable'] = 'Final Energy|Transport|Freight|Sea|' + sea_freight_input['mode'] + '|' + sea_freight_input['technology']
    sea_freight_output['Variable'] = 'Distance|Transport|Freight|Sea|' + sea_freight_output['subsector'] + '|' + sea_freight_output['technology'] 

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    sea_freight_input = sea_freight_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    sea_freight_output = sea_freight_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    sea_freight_input_pivot = pd.pivot_table(sea_freight_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    sea_freight_output_pivot = pd.pivot_table(sea_freight_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    out_df = pd.concat([sea_freight_input_pivot, sea_freight_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_transport_sea_freight.xlsx'), index=False)

run_sea_freight('ssp24p5tol5')