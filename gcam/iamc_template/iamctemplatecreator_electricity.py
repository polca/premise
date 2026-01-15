
import pandas as pd
import yaml
from pathlib import Path
import os

def run_electricity(scenario_name):
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data for electricity. two files: 
    # - one with elec generation (in EJ)
    # - one with elec inputs (in EJ)
    elec_gen = pd.read_csv(DATA_DIR /'elec gen by gen tech.csv')
    elec_input = pd.read_csv(DATA_DIR /'elec energy input by elec gen tech.csv')


    # we need to reshape all of the data in a format premise can understand
    # first, reshape elec_gen
    # store in temp_df
    temp_df = elec_gen.copy()
    temp_df['technology'] = temp_df['technology'].replace({
      'biomass (IGCC CCS)': 'Biomass IGCC CCS',
      'biomass (IGCC)': 'Biomass IGCC',
      'biomass (conv CCS)': 'Biomass CHP CCS',
      'biomass (conv)': 'Biomass CHP',
      'coal (IGCC CCS)': 'Coal IGCC CCS',
      'coal (IGCC)': 'Coal IGCC',
      'coal (conv pul CCS)': 'Coal PC CCS',
      'coal (conv pul)': 'Coal PC',
      'gas (CC CCS)': 'Gas CC CCS',
      'gas (CC)': 'Gas CC',
      'gas (steam/CT)': 'Gas ST',
      'geothermal': 'Geothermal',
      'hydro': 'Hydro',
      'Gen_III': 'Nuclear EPR',
      'refined liquids (CC CCS)': 'Oil CC CCS',
      'refined liquids (CC)': 'Oil CC',
      'refined liquids (steam/CT)': 'Oil ST',
      'rooftop_pv': 'Solar PV Residential',
      'CSP_storage': pd.NA,
      'PV': 'Solar PV Centralized',
      'PV_storage': pd.NA,
      'wind': 'Wind Onshore',
      'wind_offshore': 'Wind Offshore',
      'wind_storage': pd.NA,
      'CSP': 'Solar CSP',
      'Gen_II_LWR': 'Nuclear'
    })

    temp_df = temp_df.dropna().groupby(['Units', 'scenario', 'region', 'technology', 'Year'])['value'].agg('sum').reset_index()
    elec_gen = temp_df.copy()
    # add world region by aggregating all data
    temp_df = temp_df.groupby(['Units', 'scenario', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    # concatenate dfs
    elec_gen = pd.concat([elec_gen, temp_df], axis=0)

    # now reshape elec_input
    temp_df = elec_input.copy()
    temp_df['technology'] = temp_df['technology'].replace({
      'biomass (IGCC CCS)': 'Biomass IGCC CCS',
      'biomass (IGCC)': 'Biomass IGCC',
      'biomass (conv CCS)': 'Biomass CHP CCS',
      'biomass (conv)': 'Biomass CHP',
      'coal (IGCC CCS)': 'Coal IGCC CCS',
      'coal (IGCC)': 'Coal IGCC',
      'coal (conv pul CCS)': 'Coal PC CCS',
      'coal (conv pul)': 'Coal PC',
      'gas (CC CCS)': 'Gas CC CCS',
      'gas (CC)': 'Gas CC',
      'gas (steam/CT)': 'Gas ST',
      'geothermal': 'Geothermal',
      'hydro': 'Hydro',
      'Gen_III': 'Nuclear EPR',
      'refined liquids (CC CCS)': 'Oil CC CCS',
      'refined liquids (CC)': 'Oil CC',
      'refined liquids (steam/CT)': 'Oil ST',
      'rooftop_pv': 'Solar PV Residential',
      'CSP_storage': pd.NA,
      'PV': 'Solar PV Centralized',
      'PV_storage': pd.NA,
      'wind': 'Wind Onshore',
      'wind_offshore': 'Wind Offshore',
      'wind_storage': pd.NA,
      'CSP': 'Solar CSP',
      'Gen_II_LWR': 'Nuclear'
    })

    temp_df = temp_df.groupby(['Units', 'scenario', 'region', 'technology','Year'])['value'].agg('sum').reset_index()
    elec_input = temp_df.copy()
    # add world region
    temp_df = temp_df.groupby(['Units', 'scenario', 'technology','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    elec_input = pd.concat([elec_input, temp_df], axis=0)

    # calculate electricity efficiency
    # merge dataframes
    elec_eff = pd.merge(elec_gen, elec_input, on=['scenario', 'region', 'technology', 'Year'], suffixes=('_gen', '_input'))
    elec_eff['value'] = elec_eff['value_gen']/elec_eff['value_input']
    elec_eff = elec_eff.drop(['value_gen', 'value_input'], axis=1)

    # now we need to format these dfs into IAMC format
    # first, rename existing columns to columns in IAMC format
    elec_gen = elec_gen.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    elec_eff = elec_eff.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})

    # replace Scenario with scenario_name
    elec_gen['Scenario'] = scenario_name
    elec_eff['Scenario'] = scenario_name
    # add GCAM as Model
    elec_gen['Model'] = 'GCAM'
    elec_eff['Model'] = 'GCAM'

    # replace Unit column with expected values (EJ/yr, unitless)
    elec_gen['Unit'] = 'EJ/yr'
    elec_eff['Unit'] = 'unitless'

    # define variable
    # 
    elec_gen['Variable'] = 'Secondary Energy|Electricity|' + elec_gen['technology']
    elec_eff['Variable'] = 'Efficiency|Electricity|' + elec_eff['technology']

    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    elec_gen = elec_gen[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    elec_eff = elec_eff[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    elec_gen_pivot = pd.pivot_table(elec_gen,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    elec_eff_pivot = pd.pivot_table(elec_eff,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    out_df = pd.concat([elec_gen_pivot, elec_eff_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    # 
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
      os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_electricity.xlsx'), index=False)
