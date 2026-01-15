
import pandas as pd
import numpy as np
from pathlib import Path
import os
# import yaml

def run_two_wheelers(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data from GCAM for bus. two files: one with physical output (activity in passenger-km) and one with energy use (in EJ)
    two_wheelers_output = pd.read_csv(DATA_DIR / 'transport service output by tech.csv')
    two_wheelers_input = pd.read_csv(DATA_DIR / 'transport final energy by tech and fuel.csv')

    # subset to only bus data
    two_wheelers_output = two_wheelers_output[two_wheelers_output['subsector'].str.contains('2W')]
    two_wheelers_input = two_wheelers_input[two_wheelers_input['mode'].str.contains('2W')]

    # we need to reshape all of the data in a format premise can understand
    # first, reshape two_wheelers_output
    # store in temp_df
    temp_df = two_wheelers_output.copy()
    temp_df= temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    two_wheelers_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    two_wheelers_output = pd.concat([two_wheelers_output, temp_df], axis=0)


    # now reshape two_wheelers_input
    temp_df = two_wheelers_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    two_wheelers_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    two_wheelers_input = pd.concat([two_wheelers_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    two_wheelers_input = two_wheelers_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    two_wheelers_output = two_wheelers_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    two_wheelers_input['Scenario'] = scenario_name
    two_wheelers_output['Scenario'] = scenario_name

    # add GCAM as Model
    two_wheelers_input['Model'] = 'GCAM'
    two_wheelers_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    two_wheelers_input['Unit'] = 'EJ/yr'
    two_wheelers_output['Unit'] = 'million tkm/yr'

    # define Variable
    # input: Final Energy|Transport|Pass|Road|{subsector}|{technology}
    # output: Distance|Transport|Pass|Road|{subsector}|{technology}
    two_wheelers_input['Variable'] = 'Final Energy|Transport|Freight|Road|' + two_wheelers_input['mode'] + '|' + two_wheelers_input['technology']
    two_wheelers_output['Variable'] = 'Distance|Transport|Freight|Road|' + two_wheelers_output['subsector'] + '|' + two_wheelers_output['technology'] 

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    two_wheelers_input = two_wheelers_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    two_wheelers_output = two_wheelers_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    two_wheelers_input_pivot = pd.pivot_table(two_wheelers_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    two_wheelers_output_pivot = pd.pivot_table(two_wheelers_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    out_df = pd.concat([two_wheelers_input_pivot, two_wheelers_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_transport_two_wheelers.xlsx'), index=False)

run_two_wheelers('ssp24p5tol5')