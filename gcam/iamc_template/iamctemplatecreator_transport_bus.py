
import pandas as pd
import numpy as np
from pathlib import Path
import os
# import yaml

def run_bus(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data from GCAM for bus. two files: one with physical output (activity in passenger-km) and one with energy use (in EJ)
    bus_output = pd.read_csv(DATA_DIR / 'transport service output by tech.csv')
    bus_input = pd.read_csv(DATA_DIR / 'transport final energy by tech and fuel.csv')

    # subset to only bus data
    bus_output = bus_output[bus_output['subsector'] == 'Bus']
    bus_input = bus_input[bus_input['mode'] == 'Bus']


    # we need to reshape all of the data in a format premise can understand
    # first, reshape bus_output
    # store in temp_df
    temp_df = bus_output.copy()
    temp_df= temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    bus_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    bus_output = pd.concat([bus_output, temp_df], axis=0)


    # now reshape bus_input
    temp_df = bus_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    bus_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    bus_input = pd.concat([bus_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    bus_input = bus_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    bus_output = bus_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    bus_input['Scenario'] = scenario_name
    bus_output['Scenario'] = scenario_name

    # add GCAM as Model
    bus_input['Model'] = 'GCAM'
    bus_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    bus_input['Unit'] = 'EJ/yr'
    bus_output['Unit'] = 'million pkm/yr'

    # define Variable
    # input: Final Energy|Transport|Pass|Road|{subsector}|{technology}
    # output: Distance|Transport|Pass|Road|{subsector}|{technology}
    bus_input['Variable'] = 'Final Energy|Transport|Pass|Road|' + bus_input['mode'] + '|' + bus_input['technology']
    bus_output['Variable'] = 'Distance|Transport|Pass|Road|' + bus_output['subsector'] + '|' + bus_output['technology'] 

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    bus_input = bus_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    bus_output = bus_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    bus_input_pivot = pd.pivot_table(bus_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year'],
                                      aggfunc='sum').reset_index()
    bus_output_pivot = pd.pivot_table(bus_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year'],
                                       aggfunc='sum').reset_index()
    out_df = pd.concat([bus_input_pivot, bus_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_transport_bus.xlsx'), index=False)