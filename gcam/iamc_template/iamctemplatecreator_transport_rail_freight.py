
import pandas as pd
import numpy as np
from pathlib import Path
# import yaml

def run_rail(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(r"../GCAM_queryresults_"+scenario_name)

    print(DATA_DIR)

    # load LCI data from GCAM for rail. two files: one with physical output (activity in ton-km) and one with energy use (in EJ)
    rail_output = pd.read_csv(DATA_DIR /'rail physical output by technology.csv')
    rail_input = pd.read_csv(DATA_DIR /'rail final energy by technology and fuel.csv')


    # we need to reshape all of the data in a format premise can understand
    # first, reshape rail_output
    # store in temp_df
    temp_df = rail_output.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    rail_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    rail_output = pd.concat([rail_output, temp_df], axis=0)


    # now reshape rail_input
    temp_df = rail_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    rail_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # print(df)
    rail_input = pd.concat([rail_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    rail_input = rail_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    rail_output = rail_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    rail_input['Scenario'] = scenario_name
    rail_output['Scenario'] = scenario_name

    # add GCAM as Model
    rail_input['Model'] = 'GCAM'
    rail_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    rail_input['Unit'] = 'EJ/yr'
    rail_output['Unit'] = 'million tkm/yr'

    # define variable
    # input: Final Energy|Transport|Freight|Rail|{technology}
    # output: Distance|Transport|Freight|Rail|{technology}
    rail_input['Variable'] = 'Final Energy|Transport|Freight|Rail|' + rail_input['technology']
    rail_output['Variable'] = 'Distance|Transport|Freight|Rail|' + rail_output['technology']

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    rail_input = rail_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    rail_output = rail_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    rail_input_pivot = pd.pivot_table(rail_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    rail_output_pivot = pd.pivot_table(rail_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    out_df = pd.concat([rail_input_pivot, rail_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # write to file
    out_df.to_excel('./iamc_template/'+scenario_name+'/iamc_template_gcam_rail_world.xlsx', index=False)



run_rail('SSP2 RCP26')