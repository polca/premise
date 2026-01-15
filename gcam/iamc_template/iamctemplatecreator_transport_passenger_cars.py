
import pandas as pd
import numpy as np
from pathlib import Path
# import yaml

def run_passenger_car(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(r"../GCAM_queryresults_"+scenario_name)

    print(DATA_DIR)

    # load LCI data from GCAM for passenger car. two files: one with physical output (activity in passenger-km) and one with energy use (in EJ)
    passenger_car_output = pd.read_csv(DATA_DIR /'passenger car physical output by technology.csv')
    passenger_car_input = pd.read_csv(DATA_DIR /'passenger car final energy by technology and fuel.csv')


    # we need to reshape all of the data in a format premise can understand
    # first, reshape passenger_car_output
    # store in temp_df
    temp_df = passenger_car_output.copy()
    temp_df= temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    # create out_df which will be written to file
    passenger_car_output = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    passenger_car_output = pd.concat([passenger_car_output, temp_df], axis=0)


    # now reshape passenger_car_input
    temp_df = passenger_car_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    passenger_car_input = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'subsector', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    passenger_car_input = pd.concat([passenger_car_input, temp_df], axis=0)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    passenger_car_input = passenger_car_input.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    passenger_car_output = passenger_car_output.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    passenger_car_input['Scenario'] = scenario_name
    passenger_car_output['Scenario'] = scenario_name

    # add GCAM as Model
    passenger_car_input['Model'] = 'GCAM'
    passenger_car_output['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, million tkm/yr)
    passenger_car_input['Unit'] = 'EJ/yr'
    passenger_car_output['Unit'] = 'million pkm/yr'

    # define Variable
    # input: Final Energy|Transport|Pass|Road|{subsector}|{technology}
    # output: Distance|Transport|Pass|Road|{subsector}|{technology}
    passenger_car_input['Variable'] = 'Final Energy|Transport|Pass|Road|LDV|' + passenger_car_input['subsector'] + '|' + passenger_car_input['technology']
    passenger_car_output['Variable'] = 'Distance|Transport|Pass|Road|LDV|' + passenger_car_output['subsector'] + '|' + passenger_car_output['technology'] 

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    passenger_car_input = passenger_car_input[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    passenger_car_output = passenger_car_output[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    passenger_car_input_pivot = pd.pivot_table(passenger_car_input,
                                      values=['value'],
                                      index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                      columns=['Year']).reset_index()
    passenger_car_output_pivot = pd.pivot_table(passenger_car_output,
                                       values=['value'],
                                       index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                       columns=['Year']).reset_index()
    out_df = pd.concat([passenger_car_input_pivot, passenger_car_output_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # write to file
    out_df.to_excel('./iamc_template/'+scenario_name+'/iamc_template_gcam_passenger_cars.xlsx', index=False)


