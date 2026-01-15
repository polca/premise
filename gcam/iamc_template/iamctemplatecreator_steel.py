
import pandas as pd
import numpy as np
from pathlib import Path
# import yaml

def run_steel(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(r"../GCAM_queryresults_"+scenario_name)

    print(DATA_DIR)

    # load LCI data from GCAM for steel. two files: 
    # - one with physical output (activity in MMT) 
    # - one with energy use (in EJ)
    steel_output = pd.read_csv(DATA_DIR /'steel gen by gen tech.csv')
    steel_input = pd.read_csv(DATA_DIR /'steel final energy by tech and fuel.csv')

    # we need to reshape all of the data in a format premise can understand
    # first, reshape steel_output
    # store in temp_df
    temp_df = steel_output.copy()
    # reclassify subsector to Primary/Secondary
    temp_df['subsector'] = np.where(temp_df['subsector'].str.contains('BLASTFUR'), 'Primary', 'Secondary')
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    steel_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    steel_output = pd.concat([steel_output, temp_df], axis=0)

    # now reshape steel_input
    temp_df = steel_input.copy()
    # reclassify subsector to Primary/Secondary
    temp_df['subsector'] = np.where(temp_df['subsector'].str.contains('BLASTFUR'), 'Primary', 'Secondary')
    # reclassify input to more descriptive fuel types
    temp_df['input'] = temp_df['input'].replace({
        'delivered coal': 'Coal',
        'elect_td_ind': 'Electricity',
        'refined liquids industrial': 'Refined Liquids',
        'wholesale gas': 'Gas',
        'delivered biomass': 'Biomass',
    })
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology', 'input', 'Year'])['value'].agg('sum').reset_index()
    steel_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology', 'input', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # print(df)
    steel_input = pd.concat([steel_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    steel_input = steel_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    steel_output = steel_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    steel_input['Scenario'] = scenario_name
    steel_output['Scenario'] = scenario_name

    # add GCAM as Model
    steel_input['Model'] = 'GCAM'
    steel_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, Mt/yr)
    steel_input['Unit'] = 'EJ/yr'
    steel_output['Unit'] = 'Mt/yr'

    # define variable
    # output: Production|Industry|Steel|{subsector}|{technology}
    # input: Final Energy|Industry|Steel|{subsector}|{input}
    steel_output['Variable'] = 'Production|Industry|Steel|' + steel_output['subsector'] + '|' + steel_output['technology']
    steel_input['Variable'] = 'Final Energy|Industry|Steel|' + steel_input['subsector'] + '|' + steel_input['input']


    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    steel_input = steel_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    steel_output = steel_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    steel_input_pivot = pd.pivot_table(steel_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    steel_output_pivot = pd.pivot_table(steel_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    out_df = pd.concat([steel_input_pivot, steel_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # write to file
    out_df.to_excel('./iamc_template/'+scenario_name+'/iamc_template_gcam_steel.xlsx', index=False)


