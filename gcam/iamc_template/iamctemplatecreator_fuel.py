
import pandas as pd
import yaml
from pathlib import Path
import os

def run_fuel(scenario_name):
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data for fuels. five files
    liquids_production = pd.read_csv(DATA_DIR / 'refined liquids production by tech.csv')
    gas_production = pd.read_csv(DATA_DIR / 'gas production by tech.csv')
    hydrogen_production = pd.read_csv(DATA_DIR / 'hydrogen production by tech.csv')
    hydrogen_input = pd.read_csv(DATA_DIR / 'hydrogen inputs by tech.csv')

    # hard-coded tech mappings

    hydrogen_tech_mapping = {
        'biomass to H2': 'Biomass',
        'biomass to H2 CCS': 'Biomass CCS',
        'coal chemical CCS': 'Coal CCS',
        'electrolysis': 'Electrolysis',
        'gas ATR CCS': pd.NA,
        'natural gas steam reforming': 'Gas'
    }
    hydrogen_production['technology'] = hydrogen_production['technology'].replace(hydrogen_tech_mapping)
    hydrogen_input['technology'] = hydrogen_input['technology'].replace(hydrogen_tech_mapping)



    # we need to reshape all of the data in a format premise can understand
    # first, reshape liquids_production
    # store in temp_df
    temp_df = liquids_production.copy()
    temp_df = temp_df.dropna().groupby(['Units', 'scenario', 'region', 'sector', 'Year'])['value'].agg('sum').reset_index()
    liquids_production = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    liquids_production = pd.concat([liquids_production, temp_df], axis=0)

    # same with gas production
    temp_df = gas_production.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'Year'])['value'].agg('sum').reset_index()
    gas_production = temp_df.copy()
    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    gas_production = pd.concat([gas_production, temp_df], axis=0)

    # same with hydrogen production
    temp_df = hydrogen_production.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'technology', 'Year'])['value'].agg('sum').reset_index()
    hydrogen_production = temp_df.copy()
    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'technology', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    hydrogen_production = pd.concat([hydrogen_production, temp_df], axis=0)

    # same with hydrogen inputs
    temp_df = hydrogen_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'technology', 'Year'])['value'].agg('sum').reset_index()
    hydrogen_input = temp_df.copy()
    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'technology', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    hydrogen_input = pd.concat([hydrogen_input, temp_df], axis=0)
    # only keep technologies that are present in hydrogen_production
    hydrogen_input = hydrogen_input[hydrogen_input['technology'].isin(hydrogen_production['technology'].unique())]
    # only keep inputs that have EJ units
    hydrogen_input = hydrogen_input[hydrogen_input['Units'] == 'EJ']
    
    # calculate hydrogen production efficiency
    # merge dataframes
    hydrogen_eff = pd.merge(hydrogen_production, hydrogen_input, on=['scenario', 'region', 'technology', 'Year'], suffixes=('_prod', '_input'))
    hydrogen_eff['value'] = hydrogen_eff['value_prod']/hydrogen_eff['value_input']
    hydrogen_eff = hydrogen_eff.drop(['value_prod', 'value_input'], axis=1)


    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    liquids_production = liquids_production.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    gas_production = gas_production.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    hydrogen_production = hydrogen_production.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    hydrogen_eff = hydrogen_eff.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    liquids_production['Scenario'] = scenario_name
    gas_production['Scenario'] = scenario_name
    hydrogen_production['Scenario'] = scenario_name
    hydrogen_eff['Scenario'] = scenario_name

    # add GCAM as Model
    liquids_production['Model'] = 'GCAM'
    gas_production['Model'] = 'GCAM'
    hydrogen_production['Model'] = 'GCAM'
    hydrogen_eff['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, unitless)
    liquids_production['Unit'] = 'EJ/yr'
    gas_production['Unit'] = 'EJ/yr'
    hydrogen_production['Unit'] = 'EJ/yr'
    hydrogen_eff['Unit'] = 'unitless'

    # define variable
    liquids_production['Variable'] = 'Secondary Energy|Production|Refined Liquids|Oil'
    gas_production['Variable'] = 'Secondary Energy|Production|Natural Gas|Gas'
    hydrogen_production['Variable'] = 'Secondary Energy|Production|Hydrogen|' + hydrogen_production['technology']
    hydrogen_eff['Variable'] = 'Efficiency|Hydrogen|' + hydrogen_eff['technology']

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    liquids_production = liquids_production[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    gas_production = gas_production[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    hydrogen_production = hydrogen_production[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    hydrogen_eff = hydrogen_eff[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    liquids_production_pivot = pd.pivot_table(liquids_production,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    gas_production_pivot = pd.pivot_table(gas_production,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    hydrogen_production_pivot = pd.pivot_table(hydrogen_production,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    hydrogen_eff_pivot = pd.pivot_table(hydrogen_eff,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    out_df = pd.concat([liquids_production_pivot, gas_production_pivot, hydrogen_production_pivot, hydrogen_eff_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    # 
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
      os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_fuels.xlsx'), index=False)

run_fuel('ssp24p5tol5')