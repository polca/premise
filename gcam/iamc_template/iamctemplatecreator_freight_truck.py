
import pandas as pd
import numpy as np
from pathlib import Path
# import yaml

def run_freight_truck(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(r"../GCAM_queryresults_"+scenario_name)

    print(DATA_DIR)

    # load LCI data from GCAM for freight truck. two files: one with physical output (activity in ton-km) and one with energy use (in EJ)
    freight_truck_output = pd.read_csv(DATA_DIR /'freight truck physical output by technology.csv')
    freight_truck_input = pd.read_csv(DATA_DIR /'freight truck final energy by technology and fuel.csv')


    # we need to reshape all of the data in a format premise can understand
    # first, reshape freight_truck_output
    # store in temp_df
    temp_df = freight_truck_output.copy()
    temp_df= temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    freight_truck_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    freight_truck_output = pd.concat([freight_truck_output, temp_df], axis=0)


    # now reshape freight_truck_input
    temp_df = freight_truck_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    freight_truck_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    freight_truck_input = pd.concat([freight_truck_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    freight_truck_input = freight_truck_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    freight_truck_output = freight_truck_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    freight_truck_input['Scenario'] = scenario_name
    freight_truck_output['Scenario'] = scenario_name

    # add GCAM as Model
    freight_truck_input['Model'] = 'GCAM'
    freight_truck_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    freight_truck_input['Unit'] = 'EJ/yr'
    freight_truck_output['Unit'] = 'million tkm/yr'

    # define Variable
    # input: Final Energy|Transport|Freight|Road|{subsector}|{technology}
    # output: Distance|Transport|Freight|Road|{subsector}|{technology}
    freight_truck_input['Variable'] = 'Final Energy|Transport|Freight|Road|' + freight_truck_input['subsector'] + '|' + freight_truck_input['technology']
    freight_truck_output['Variable'] = 'Distance|Transport|Freight|Road|' + freight_truck_output['subsector'] + '|' + freight_truck_output['technology'] 

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    freight_truck_input = freight_truck_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    freight_truck_output = freight_truck_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    freight_truck_input_pivot = pd.pivot_table(freight_truck_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    freight_truck_output_pivot = pd.pivot_table(freight_truck_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    
    # list all year columns, loop through df columns and make sure column exists

    out_df = pd.concat([freight_truck_input_pivot, freight_truck_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # write to file
    out_df.to_excel('./iamc_template/'+scenario_name+'/iamc_template_gcam_freight_truck.xlsx', index=False)



run_freight_truck('SSP2 RCP26')