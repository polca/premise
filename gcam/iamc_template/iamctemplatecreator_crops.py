import pandas as pd
import numpy as np
from pathlib import Path
import os


def run_crop(scenario_name):
    # Need to change path for each scenario
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data for crops. two files: 
    # - one with land allocation (in 1000km2)
    # - one with luc emissions (in Mt C)
    crop_land = pd.read_csv(DATA_DIR /'land allocation by crop.csv')
    crop_emis = pd.read_csv(DATA_DIR /'LUC emissions by LUT.csv')

    # truncate landleaf in crop_emis
    crop_emis['LandLeaf'] = crop_emis['LandLeaf'].str.split('_').str[0]

    # aggregate crop_emis

    crop_emis = crop_emis.groupby(['scenario', 'region','Year','LandLeaf'])['value'].agg('sum').reset_index()

    crop_mapping = {
        'CornC4': 'Maize',
        'SugarCropC4': 'Sugar',
        'OilCrop': 'Oilcrops',
        'biomassTree': 'Wood',
        'biomassGrass': 'Grass'
    }

    crop_land['LandLeaf'] = crop_land['LandLeaf'].replace(crop_mapping)
    crop_emis['LandLeaf'] = crop_emis['LandLeaf'].replace(crop_mapping)

    # filter only relevant crops
    crop_land = crop_land[crop_land['LandLeaf'].isin(crop_mapping.values())]
    crop_emis = crop_emis[crop_emis['LandLeaf'].isin(crop_mapping.values())]

    # convert land allocation from 1000km2 to ha
    crop_land['value'] = crop_land['value'] * 1000 * 100
    # convert LUC emissions from Mt C to kg CO2
    crop_emis['value'] = crop_emis['value'] * 1000 * 44/12

    # add world region by aggregating all values
    temp_df = crop_land.copy()
    temp_df = temp_df.groupby(['scenario', 'LandLeaf','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    crop_land = pd.concat([crop_land, temp_df], axis=0)

    temp_df = crop_emis.copy()
    temp_df = temp_df.groupby(['scenario', 'LandLeaf','Year'])['value'].agg('sum').reset_index()
    temp_df['region'] = 'World'
    crop_emis = pd.concat([crop_emis, temp_df], axis=0)

    crop = pd.merge(crop_land, crop_emis, how='inner', on=['scenario', 'region','Year','LandLeaf'], suffixes=('_land','_emis'))
    crop['ef'] = crop['value_emis'] / crop['value_land']  # kg CO2 / ha

    crop = crop[['scenario', 'region', 'LandLeaf', 'Year', 'ef']]

    # format to IAMC

    # first, rename existing columns to columns in IAMC format
    crop_land = crop_land.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit'})
    crop = crop.rename(columns={'region': 'Region', 'scenario': 'Scenario', 'Units': 'Unit', 'ef': 'value'})

    # replace Scenario with scenario_name
    crop_land['Scenario'] = scenario_name
    crop['Scenario'] = scenario_name
    # add GCAM as Model
    crop_land['Model'] = 'GCAM'
    crop['Model'] = 'GCAM'
    # replace Unit column with expected values (ha, kg CO2/ha)
    crop_land['Unit'] = 'ha'
    crop['Unit'] = 'kg CO2/ha'

    # define variable
    # land: Land Use|Average|Biomass|{LandLeaf}
    # ef: Emission Factor|CO2|Land Use Change|Average|Biomass|{LandLeaf}
    crop_land['Variable'] = 'Land Use|Average|Biomass|' + crop_land['LandLeaf']
    crop['Variable'] = 'Emission Factor|CO2|Land Use Change|Average|Biomass|' + crop['LandLeaf']


    # reorder columns and remove unnecessary columns (sector, subsector, technology)
    crop_land = crop_land[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]
    crop = crop[['Scenario', 'Region', 'Model', 'Variable', 'Unit', 'Year', 'value']]

    # pivot dfs, creating columns for each year

    crop_land_pivot = pd.pivot_table(crop_land,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    crop_pivot = pd.pivot_table(crop,
                                        values=['value'],
                                        index=['Scenario', 'Region', 'Model', 'Variable', 'Unit'],
                                        columns=['Year'],
                                        aggfunc='sum').reset_index()
    out_df = pd.concat([crop_land_pivot, crop_pivot]).reset_index(drop=True)

    # tidy up dataframe (fix multiple index column names in year columns)
    out_df.columns = ['Scenario', 'Region', 'Model', 'Variable', 'Unit'] + [str(x[1]) for x in out_df.columns[5:]]

    # create output directory if it doesn't exist
    if not os.path.exists(os.path.join('..', 'output', scenario_name)):
        os.mkdir(os.path.join('..', 'output', scenario_name))

    # write to file
    out_df.to_excel(os.path.join('..', 'output', scenario_name, 'iamc_template_gcam_crops.xlsx'), index=False)
