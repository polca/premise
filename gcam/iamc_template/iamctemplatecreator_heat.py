import pandas as pd
import numpy as np
from pathlib import Path
import os, re


def run_heat(scenario_name):
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # read in heat LCI data
    heat = pd.read_csv(DATA_DIR / 'heat by sector and fuel.csv')
    heat['sector'] = heat['sector'].map(lambda x: 'Industry' if re.search('process heat', x) else 'Buildings')
    # hard coded fuel mapping
    fuel_mapping = {
        'delivered coal': 'Coal',
        'refined liquids enduse': 'Oil',
        'H2 industrial': 'Hydrogen',
        'delivered biomass': 'Biomass',
        'refined liquids industrial': 'Oil',
        'wholesale gas': 'Gas',
        'elect_td_ind': 'Electricity',
        'global solar resource': 'Solar',
        'traditional biomass': 'Biomass',
        'H2 retail delivery': 'Hydrogen',
        'delivered gas': 'Gas',
        'elect_td_bld': 'Electricity',
        'district heat': 'District Heat'
    }
    heat['input'] = heat['input'].replace(fuel_mapping)

    temp_df = heat.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'input', 'Year'])['value'].agg('sum').reset_index()
    heat = temp_df.copy()

    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'input', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    heat = pd.concat([heat, temp_df])

    # format into IAMC template
    heat = heat.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    heat['Scenario'] = scenario_name
    heat['Model'] = 'GCAM'
    heat['Unit'] = 'EJ'
    heat['Variable'] = 'Final Energy|Heat|' + heat['sector'] + '|' + heat['input']

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    heat = heat[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    heat_pivot = pd.pivot_table(heat,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    out_df = heat_pivot
    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_heat.xlsx'), index=False)

run_heat('ssp24p5tol5')