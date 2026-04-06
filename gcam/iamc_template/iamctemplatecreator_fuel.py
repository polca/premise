
import pandas as pd
import yaml
from pathlib import Path
import os

def run_fuel(scenario_name):
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data for fuels. five files
    liquids_production = pd.read_csv(DATA_DIR / 'refined liquids production by tech.csv')
    liquids_input = pd.read_csv(DATA_DIR / 'refined liquids inputs by tech.csv')
    gas_production = pd.read_csv(DATA_DIR / 'gas production by tech.csv')
    gas_input = pd.read_csv(DATA_DIR / 'gas inputs by tech.csv')
    hydrogen_production = pd.read_csv(DATA_DIR / 'hydrogen production by tech.csv')
    hydrogen_input = pd.read_csv(DATA_DIR / 'hydrogen inputs by tech.csv')

    # hard-coded tech mappings

    liquids_tech_mapping = {
      'BTL with hydrogen': 'BTL Hydrogen',
      'FT biofuels': 'FT Biodiesel',
      'FT biofuels CCS level 1': 'FT Biodiesel CCS',
      'FT biofuels CCS level 2': 'FT Biodiesel CCS',
      'biodiesel': 'Biodiesel',
      'cellulosic ethanol': 'Cellulosic Ethanol',
      'cellulosic ethanol CCS level 1': 'Cellulosic Ethanol CCS',
      'cellulosic ethanol CCS level 2': 'Cellulosic Ethanol CCS',
      'corn ethanol': 'Corn Ethanol',
      'coal to liquids': 'Coal to Liquids',
      'coal to liquids CCS level 1': 'Coal to Liquids CCS',
      'coal to liquids CCS level 2': 'Coal to Liquids CCS',
      'gas to liquids': 'Gas to Liquids',
      'oil refining': 'Oil',
      'sugar cane ethanol': 'Sugar Cane Ethanol'
    }
    liquids_production['technology'] = liquids_production['technology'].replace(liquids_tech_mapping)
    liquids_input['technology'] = liquids_input['technology'].replace(liquids_tech_mapping)

    gas_tech_mapping = {
      'biomass gasification': 'Biomass',
      'coal gasification': 'Coal',
      'natural gas': 'Gas'
    }
    gas_production['technology'] = gas_production['technology'].replace(gas_tech_mapping)
    gas_input['technology'] = gas_input['technology'].replace(gas_tech_mapping)

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
    temp_df = temp_df.dropna().groupby(['Units', 'scenario', 'region', 'sector', 'technology', 'Year'])['value'].agg('sum').reset_index()
    liquids_production = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector','technology', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    liquids_production = pd.concat([liquids_production, temp_df], axis=0)

    temp_df = liquids_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'technology', 'Year'])['value'].agg('sum').reset_index()
    liquids_input = temp_df.copy()
    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'technology', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    liquids_input = pd.concat([liquids_input, temp_df], axis=0)
    # only keep technologies that are present in hydrogen_production
    liquids_input = liquids_input[liquids_input['technology'].isin(liquids_production['technology'].unique())]
    # only keep inputs that have EJ units
    liquids_input = liquids_input[liquids_input['Units'] == 'EJ']

    
    # calculate hydrogen production efficiency
    # merge dataframes
    liquids_eff = pd.merge(liquids_production, liquids_input, on=['scenario', 'region', 'technology', 'Year'], suffixes=('_prod', '_input'))
    liquids_eff['value'] = liquids_eff['value_prod']/liquids_eff['value_input']
    liquids_eff = liquids_eff.drop(['value_prod', 'value_input'], axis=1)

    
    # same with gas production
    temp_df = gas_production.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'sector', 'technology', 'Year'])['value'].agg('sum').reset_index()
    gas_production = temp_df.copy()
    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'sector', 'technology', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    gas_production = pd.concat([gas_production, temp_df], axis=0)

    temp_df = gas_input.copy()
    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'technology', 'Year'])['value'].agg('sum').reset_index()
    gas_input = temp_df.copy()
    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'technology', 'Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    gas_input = pd.concat([gas_input, temp_df], axis=0)
    # only keep technologies that are present in hydrogen_production
    gas_input = gas_input[gas_input['technology'].isin(hydrogen_production['technology'].unique())]
    # only keep inputs that have EJ units
    gas_input = gas_input[gas_input['Units'] == 'EJ']
    
    # calculate hydrogen production efficiency
    # merge dataframes
    gas_eff = pd.merge(gas_production, gas_input, on=['scenario', 'region', 'technology', 'Year'], suffixes=('_prod', '_input'))
    gas_eff['value'] = gas_eff['value_prod']/gas_eff['value_input']
    gas_eff = gas_eff.drop(['value_prod', 'value_input'], axis=1)

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
    liquids_eff = liquids_eff.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    gas_production = gas_production.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    gas_eff = gas_eff.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    hydrogen_production = hydrogen_production.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    hydrogen_eff = hydrogen_eff.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    liquids_production['Scenario'] = scenario_name
    liquids_eff['Scenario'] = scenario_name
    gas_production['Scenario'] = scenario_name
    gas_eff['Scenario'] = scenario_name
    hydrogen_production['Scenario'] = scenario_name
    hydrogen_eff['Scenario'] = scenario_name

    # add GCAM as Model
    liquids_production['Model'] = 'GCAM'
    liquids_eff['Model'] = 'GCAM'
    gas_production['Model'] = 'GCAM'
    gas_eff['Model'] = 'GCAM'
    hydrogen_production['Model'] = 'GCAM'
    hydrogen_eff['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, unitless)
    liquids_production['Unit'] = 'EJ/yr'
    liquids_eff['Unit'] = 'unitless'
    gas_production['Unit'] = 'EJ/yr'
    gas_eff['Unit'] = 'unitless'
    hydrogen_production['Unit'] = 'EJ/yr'
    hydrogen_eff['Unit'] = 'unitless'

    # define variable
    liquids_production['Variable'] = 'Secondary Energy|Production|Refined Liquids|' + liquids_production['technology']
    liquids_eff['Variable'] = 'Efficiency|Refined Liquids|' + liquids_eff['technology']
    gas_production['Variable'] = 'Secondary Energy|Production|Natural Gas|' + gas_production['technology']
    gas_eff['Variable'] = 'Efficiency|Natural Gas|' + gas_eff['technology']
    hydrogen_production['Variable'] = 'Secondary Energy|Production|Hydrogen|' + hydrogen_production['technology']
    hydrogen_eff['Variable'] = 'Efficiency|Hydrogen|' + hydrogen_eff['technology']

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    liquids_production = liquids_production[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    liquids_eff = liquids_eff[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    gas_production = gas_production[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    gas_eff = gas_eff[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    hydrogen_production = hydrogen_production[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    hydrogen_eff = hydrogen_eff[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    liquids_production_pivot = pd.pivot_table(liquids_production,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    liquids_eff_pivot = pd.pivot_table(liquids_eff,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    gas_production_pivot = pd.pivot_table(gas_production,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    gas_eff_pivot = pd.pivot_table(gas_eff,
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
    out_df = pd.concat([liquids_production_pivot, liquids_eff_pivot, gas_production_pivot, gas_eff_pivot,hydrogen_production_pivot, hydrogen_eff_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    # 
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
      os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_fuels.xlsx'), index=False)
