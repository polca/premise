
import pandas as pd
import numpy as np
from pathlib import Path
import os
# import yaml

def run_cement(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queryresults', scenario_name))

    # load LCI data from GCAM for cement. three files: 
    # - one with physical output (activity in Mt) 
    # - one with energy use (in EJ)
    # - one with process heat energy (in EJ)
    cement_output = pd.read_csv(DATA_DIR /'cement gen by gen tech.csv')
    cement_input = pd.read_csv(DATA_DIR /'cement final energy by tech and fuel.csv')
    cement_process_heat = pd.read_csv(DATA_DIR /'process heat cement final energy by tech and fuel.csv')

    # concatenate cement_input and cement_process_heat
    cement_input = pd.concat([cement_input, cement_process_heat], axis=0)

    # we need to reshape all of the data in a format premise can understand
    # first, reshape cement_output
    # store in temp_df
    temp_df = cement_output.copy()
    temp_df['technology'] = temp_df['technology'].replace({
        'cement': 'Cement',
        'cement CCS': 'Cement CCS'
    })
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    cement_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    cement_output = pd.concat([cement_output, temp_df], axis=0)

    # now reshape cement_input
    temp_df = cement_input.copy()
    temp_df['input'] = temp_df['input'].replace({
        'delivered coal': 'Coal',
        'elect_td_ind': 'Electricity',
        'refined liquids industrial': 'Refined Liquids',
        'wholesale gas': 'Gas',
        'delivered biomass': 'Biomass',
    })
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'input', 'Year'])['value'].agg('sum').reset_index()
    cement_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology', 'input', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # print(df)
    cement_input = pd.concat([cement_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    cement_input = cement_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    cement_output = cement_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    cement_input['Scenario'] = scenario_name
    cement_output['Scenario'] = scenario_name

    # add GCAM as Model
    cement_input['Model'] = 'GCAM'
    cement_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, Mt/yr)
    cement_input['Unit'] = 'EJ/yr'
    cement_output['Unit'] = 'Mt/yr'

    # define variable
    # output: Production|Industry|Cement|{technology}
    # input: Final Energy|Industry|Cement|{technology}|{input}
    cement_output['Variable'] = 'Production|Industry|Cement|' + cement_output['technology']
    cement_input['Variable'] = 'Final Energy|Industry|Cement|' + cement_input['input']


    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    cement_input = cement_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    cement_output = cement_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    cement_input_pivot = pd.pivot_table(cement_input,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    cement_output_pivot = pd.pivot_table(cement_output,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    out_df = pd.concat([cement_input_pivot, cement_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_cement.xlsx'), index=False)

