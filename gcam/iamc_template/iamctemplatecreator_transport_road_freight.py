
import pandas as pd
import numpy as np
from pathlib import Path
import os
# import yaml

def run_road_freight(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data from GCAM for bus. two files: one with physical output (activity in passenger-km) and one with energy use (in EJ)
    road_freight_output = pd.read_csv(DATA_DIR / 'transport service output by tech.csv')
    road_freight_input = pd.read_csv(DATA_DIR / 'transport final energy by tech and fuel.csv')

    # subset to only bus data
    road_freight_output = road_freight_output[road_freight_output['subsector'].str.contains('truck')]
    road_freight_input = road_freight_input[road_freight_input['mode'].str.contains('truck')]

    # we need to reshape all of the data in a format premise can understand
    # first, reshape road_freight_output
    # store in temp_df
    temp_df = road_freight_output.copy()
    temp_df= temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    road_freight_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    road_freight_output = pd.concat([road_freight_output, temp_df], axis=0)


    # now reshape road_freight_input
    temp_df = road_freight_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    road_freight_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'mode', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    road_freight_input = pd.concat([road_freight_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    road_freight_input = road_freight_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    road_freight_output = road_freight_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    road_freight_input['Scenario'] = scenario_name
    road_freight_output['Scenario'] = scenario_name

    # add GCAM as Model
    road_freight_input['Model'] = 'GCAM'
    road_freight_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    road_freight_input['Unit'] = 'EJ/yr'
    road_freight_output['Unit'] = 'million tkm/yr'

    # define Variable
    # input: Final Energy|Transport|Pass|Road|{subsector}|{technology}
    # output: Distance|Transport|Pass|Road|{subsector}|{technology}
    road_freight_input['Variable'] = 'Final Energy|Transport|Freight|Road|' + road_freight_input['mode'] + '|' + road_freight_input['technology']
    road_freight_output['Variable'] = 'Distance|Transport|Freight|Road|' + road_freight_output['subsector'] + '|' + road_freight_output['technology'] 

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    road_freight_input = road_freight_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    road_freight_output = road_freight_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    road_freight_input_pivot = pd.pivot_table(road_freight_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    road_freight_output_pivot = pd.pivot_table(road_freight_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    out_df = pd.concat([road_freight_input_pivot, road_freight_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_transport_road_freight.xlsx'), index=False)

run_road_freight('ssp24p5tol5')